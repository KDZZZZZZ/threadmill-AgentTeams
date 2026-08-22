package runtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/artifacts"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/executionreceipt"
)

type sqliteRecoveryStateStore struct{ r *SQLiteRuntimeStateRepository }

func (s sqliteRecoveryStateStore) now() time.Time { return s.r.clock.Now().UTC() }

func validRecoveryKey(key WaitingKey, epoch ExecutionEpoch, owner string, ttl time.Duration) error {
	if key.TaskID == "" || key.InvocationID == "" || key.Generation <= 0 || epoch <= 0 || owner == "" || ttl <= 0 {
		return errors.New("recovery claim identity, observed epoch, owner, and lease are required")
	}
	return nil
}

func scanRecoveryClaim(scanner interface{ Scan(...any) error }) (RecoveryClaim, error) {
	var claim RecoveryClaim
	var observed int
	var expires, claimed, renewed string
	if err := scanner.Scan(&claim.Key.TaskID, &claim.Key.InvocationID, &claim.Key.Generation, &observed, &claim.OwnerID, &expires, &claim.Fence, &claim.Revision, &claimed, &renewed); err != nil {
		return RecoveryClaim{}, err
	}
	claim.ObservedExecutionEpoch = ExecutionEpoch(observed)
	var err error
	if claim.LeaseExpiresAt, err = time.Parse(time.RFC3339Nano, expires); err != nil {
		return RecoveryClaim{}, err
	}
	if claim.ClaimedAt, err = time.Parse(time.RFC3339Nano, claimed); err != nil {
		return RecoveryClaim{}, err
	}
	if claim.RenewedAt, err = time.Parse(time.RFC3339Nano, renewed); err != nil {
		return RecoveryClaim{}, err
	}
	return claim, nil
}

func recoveryClaimRow(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, key WaitingKey) (RecoveryClaim, bool, error) {
	claim, err := scanRecoveryClaim(q.QueryRowContext(ctx, "SELECT task_id,invocation_id,generation,observed_execution_epoch,owner_id,lease_expires_at,fence,revision,claimed_at,renewed_at FROM runtime_recovery_claims WHERE task_id=? AND invocation_id=? AND generation=?", key.TaskID, key.InvocationID, key.Generation))
	if errors.Is(err, sql.ErrNoRows) {
		return RecoveryClaim{}, false, nil
	}
	return claim, err == nil, err
}

func (s sqliteRecoveryStateStore) AcquireRecoveryClaim(ctx context.Context, key WaitingKey, epoch ExecutionEpoch, owner string, ttl time.Duration) (RecoveryClaim, error) {
	if err := validRecoveryKey(key, epoch, owner, ttl); err != nil {
		return RecoveryClaim{}, err
	}
	tx, err := s.r.db.BeginTx(ctx, nil)
	if err != nil {
		return RecoveryClaim{}, err
	}
	defer tx.Rollback()
	now := s.now()
	current, found, err := recoveryClaimRow(ctx, tx, key)
	if err != nil {
		return RecoveryClaim{}, err
	}
	if !found {
		claim := RecoveryClaim{Key: key, ObservedExecutionEpoch: epoch, OwnerID: owner, LeaseExpiresAt: now.Add(ttl), Fence: 1, Revision: 1, ClaimedAt: now, RenewedAt: now}
		if _, err = tx.ExecContext(ctx, "INSERT INTO runtime_recovery_claims VALUES(?,?,?,?,?,?,?,?,?,?)", key.TaskID, key.InvocationID, key.Generation, epoch, owner, claim.LeaseExpiresAt.Format(time.RFC3339Nano), claim.Fence, claim.Revision, claim.ClaimedAt.Format(time.RFC3339Nano), claim.RenewedAt.Format(time.RFC3339Nano)); err != nil {
			return RecoveryClaim{}, err
		}
		return claim, tx.Commit()
	}
	if current.OwnerID == owner && current.LeaseExpiresAt.After(now) {
		if current.ObservedExecutionEpoch != epoch {
			return RecoveryClaim{}, ErrRecoveryClaimLost
		}
		return current, tx.Commit()
	}
	if current.OwnerID != "" && current.LeaseExpiresAt.After(now) {
		return RecoveryClaim{}, ErrRecoveryClaimed
	}
	next := current
	next.ObservedExecutionEpoch, next.OwnerID = epoch, owner
	next.LeaseExpiresAt, next.ClaimedAt, next.RenewedAt = now.Add(ttl), now, now
	next.Fence, next.Revision = current.Fence+1, current.Revision+1
	result, err := tx.ExecContext(ctx, "UPDATE runtime_recovery_claims SET observed_execution_epoch=?,owner_id=?,lease_expires_at=?,fence=?,revision=?,claimed_at=?,renewed_at=? WHERE task_id=? AND invocation_id=? AND generation=? AND revision=?", epoch, owner, next.LeaseExpiresAt.Format(time.RFC3339Nano), next.Fence, next.Revision, next.ClaimedAt.Format(time.RFC3339Nano), next.RenewedAt.Format(time.RFC3339Nano), key.TaskID, key.InvocationID, key.Generation, current.Revision)
	if err != nil {
		return RecoveryClaim{}, err
	}
	changed, err := result.RowsAffected()
	if err != nil || changed != 1 {
		return RecoveryClaim{}, ErrRecoveryClaimed
	}
	return next, tx.Commit()
}

func (s sqliteRecoveryStateStore) RenewRecoveryClaim(ctx context.Context, claim RecoveryClaim, ttl time.Duration) (RecoveryClaim, error) {
	if err := validRecoveryKey(claim.Key, claim.ObservedExecutionEpoch, claim.OwnerID, ttl); err != nil || claim.Fence <= 0 || claim.Revision <= 0 {
		return RecoveryClaim{}, ErrRecoveryClaimLost
	}
	tx, err := s.r.db.BeginTx(ctx, nil)
	if err != nil {
		return RecoveryClaim{}, err
	}
	defer tx.Rollback()
	now := s.now()
	current, found, err := recoveryClaimRow(ctx, tx, claim.Key)
	if err != nil || !found || current.OwnerID != claim.OwnerID || current.Fence != claim.Fence || current.Revision != claim.Revision || !current.LeaseExpiresAt.After(now) {
		return RecoveryClaim{}, ErrRecoveryClaimLost
	}
	next := current
	next.LeaseExpiresAt, next.RenewedAt, next.Revision = now.Add(ttl), now, current.Revision+1
	result, err := tx.ExecContext(ctx, "UPDATE runtime_recovery_claims SET lease_expires_at=?,revision=?,renewed_at=? WHERE task_id=? AND invocation_id=? AND generation=? AND owner_id=? AND fence=? AND revision=?", next.LeaseExpiresAt.Format(time.RFC3339Nano), next.Revision, next.RenewedAt.Format(time.RFC3339Nano), next.Key.TaskID, next.Key.InvocationID, next.Key.Generation, next.OwnerID, next.Fence, current.Revision)
	if err != nil {
		return RecoveryClaim{}, err
	}
	changed, err := result.RowsAffected()
	if err != nil || changed != 1 {
		return RecoveryClaim{}, ErrRecoveryClaimLost
	}
	return next, tx.Commit()
}

func (s sqliteRecoveryStateStore) ReleaseRecoveryClaim(ctx context.Context, claim RecoveryClaim) error {
	if claim.Key.TaskID == "" || claim.OwnerID == "" || claim.Fence <= 0 || claim.Revision <= 0 {
		return ErrRecoveryClaimLost
	}
	tx, err := s.r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := s.now()
	result, err := tx.ExecContext(ctx, "UPDATE runtime_recovery_claims SET owner_id='',lease_expires_at=?,revision=?,renewed_at=? WHERE task_id=? AND invocation_id=? AND generation=? AND owner_id=? AND fence=? AND revision=?", now.Format(time.RFC3339Nano), claim.Revision+1, now.Format(time.RFC3339Nano), claim.Key.TaskID, claim.Key.InvocationID, claim.Key.Generation, claim.OwnerID, claim.Fence, claim.Revision)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil || changed != 1 {
		return ErrRecoveryClaimLost
	}
	return tx.Commit()
}

func (s sqliteRecoveryStateStore) GetRecoveryClaim(ctx context.Context, key WaitingKey) (RecoveryClaim, bool, error) {
	return recoveryClaimRow(ctx, s.r.db, key)
}

func (s sqliteRecoveryStateStore) AssertRecoveryClaim(ctx context.Context, claim RecoveryClaim) error {
	current, found, err := s.GetRecoveryClaim(ctx, claim.Key)
	if err != nil || !found || current.OwnerID != claim.OwnerID || current.Fence != claim.Fence || current.Revision != claim.Revision || !current.LeaseExpiresAt.After(s.now()) {
		return ErrRecoveryClaimLost
	}
	return nil
}

func (s sqliteRecoveryStateStore) LoadRecoverySnapshot(ctx context.Context, key WaitingKey) (RecoverySnapshot, error) {
	if key.TaskID == "" || key.InvocationID == "" || key.Generation <= 0 {
		return RecoverySnapshot{}, ErrRecoverySnapshotInconsistent
	}
	tx, err := s.r.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return RecoverySnapshot{}, err
	}
	defer tx.Rollback()
	snapshot := RecoverySnapshot{Key: key}
	if err = loadRecoveryWaiting(ctx, tx, &snapshot); err != nil {
		return RecoverySnapshot{}, err
	}
	if err = loadRecoveryInputs(ctx, tx, &snapshot); err != nil {
		return RecoverySnapshot{}, err
	}
	if err = loadRecoveryPhysical(ctx, tx, &snapshot); err != nil {
		return RecoverySnapshot{}, err
	}
	if err = loadRecoveryOutput(ctx, tx, &snapshot); err != nil {
		return RecoverySnapshot{}, err
	}
	if err = loadRecoveryContinuation(ctx, tx, &snapshot); err != nil {
		return RecoverySnapshot{}, err
	}
	if err = tx.Commit(); err != nil {
		return RecoverySnapshot{}, err
	}
	return snapshot, nil
}

func loadRecoveryWaiting(ctx context.Context, tx *sql.Tx, snapshot *RecoverySnapshot) error {
	var payload []byte
	err := tx.QueryRowContext(ctx, "SELECT payload FROM runtime_waiting WHERE task_id=? AND invocation_id=? AND generation=?", snapshot.Key.TaskID, snapshot.Key.InvocationID, snapshot.Key.Generation).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	var value WaitingRecord
	if err = json.Unmarshal(payload, &value); err != nil || value.Key != snapshot.Key {
		return ErrRecoverySnapshotInconsistent
	}
	snapshot.Waiting, snapshot.WaitingRevision = &value, value.Revision
	snapshot.CurrentExecutionEpoch = value.ExecutionEpoch
	snapshot.BindingRef = value.PreviousBindingRef
	snapshot.InputRevision = value.InputRevision
	return nil
}

func loadRecoveryInputs(ctx context.Context, tx *sql.Tx, snapshot *RecoverySnapshot) error {
	var payload []byte
	err := tx.QueryRowContext(ctx, "SELECT payload FROM runtime_inputs WHERE task_id=? AND invocation_id=? AND generation=? ORDER BY sequence DESC LIMIT 1", snapshot.Key.TaskID, snapshot.Key.InvocationID, snapshot.Key.Generation).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	var value StoredPhaseInputSet
	if err = json.Unmarshal(payload, &value); err != nil || value.Inputs.InputRevision == "" {
		return ErrRecoverySnapshotInconsistent
	}
	snapshot.LatestInputs = &value
	return nil
}

func loadRecoveryPhysical(ctx context.Context, tx *sql.Tx, snapshot *RecoverySnapshot) error {
	rows, err := tx.QueryContext(ctx, "SELECT payload FROM runtime_physical_executions WHERE task_id=? AND invocation_id=? AND generation=? ORDER BY execution_epoch", snapshot.Key.TaskID, snapshot.Key.InvocationID, snapshot.Key.Generation)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var payload []byte
		if err = rows.Scan(&payload); err != nil {
			return err
		}
		var value PhysicalExecution
		if err = json.Unmarshal(payload, &value); err != nil || value.Key().TaskID != snapshot.Key.TaskID || value.Key().InvocationID != snapshot.Key.InvocationID || value.Key().Generation != snapshot.Key.Generation {
			return ErrRecoverySnapshotInconsistent
		}
		snapshot.PhysicalHistory = append(snapshot.PhysicalHistory, value)
	}
	if err = rows.Err(); err != nil {
		return err
	}
	if len(snapshot.PhysicalHistory) == 0 {
		return nil
	}
	var selected *PhysicalExecution
	if snapshot.Waiting != nil {
		for index := range snapshot.PhysicalHistory {
			if snapshot.PhysicalHistory[index].ExecutionEpoch == snapshot.Waiting.ExecutionEpoch {
				selected = &snapshot.PhysicalHistory[index]
				break
			}
		}
	} else {
		selected = &snapshot.PhysicalHistory[len(snapshot.PhysicalHistory)-1]
	}
	if selected != nil {
		snapshot.CurrentPhysical, snapshot.PhysicalRevision = selected, selected.Revision
		if snapshot.CurrentExecutionEpoch == 0 {
			snapshot.CurrentExecutionEpoch = selected.ExecutionEpoch
		}
		if snapshot.BindingRef == "" {
			snapshot.BindingRef = selected.BindingRef
		}
		if snapshot.InputRevision == "" {
			snapshot.InputRevision = selected.InputRevision
		}
	}
	return nil
}

func loadRecoveryOutput(ctx context.Context, tx *sql.Tx, snapshot *RecoverySnapshot) error {
	var payload []byte
	err := tx.QueryRowContext(ctx, "SELECT payload FROM runtime_phase_outputs WHERE task_id=? AND invocation_id=? AND generation=?", snapshot.Key.TaskID, snapshot.Key.InvocationID, snapshot.Key.Generation).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	var value PhaseOutputRecord
	if err = json.Unmarshal(payload, &value); err != nil || value.Key.TaskID != snapshot.Key.TaskID || value.Key.InvocationID != snapshot.Key.InvocationID || value.Key.Generation != snapshot.Key.Generation {
		return ErrRecoverySnapshotInconsistent
	}
	snapshot.Output, snapshot.OutputRevision = &value, value.Revision
	return nil
}

func loadRecoveryContinuation(ctx context.Context, tx *sql.Tx, snapshot *RecoverySnapshot) error {
	if snapshot.Waiting == nil {
		return nil
	}
	var payload []byte
	err := tx.QueryRowContext(ctx, "SELECT payload FROM runtime_continuations WHERE ref=?", snapshot.Waiting.ContinuationRef).Scan(&payload)
	if err == nil {
		var continuation ContinuationMaterial
		if err = json.Unmarshal(payload, &continuation); err != nil {
			return ErrRecoverySnapshotInconsistent
		}
		snapshot.Continuation = &continuation
		snapshot.ArtifactRefs = append(snapshot.ArtifactRefs, continuation.ArtifactRefs...)
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if snapshot.Waiting.PreviousBindingRef != "" {
		err = tx.QueryRowContext(ctx, "SELECT payload FROM runtime_bindings WHERE binding_ref=?", snapshot.Waiting.PreviousBindingRef).Scan(&payload)
		if err == nil {
			var binding ContinuationBinding
			if err = json.Unmarshal(payload, &binding); err != nil {
				return ErrRecoverySnapshotInconsistent
			}
			snapshot.Binding = &binding
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
	}
	snapshot.ArtifactRefs = append(snapshot.ArtifactRefs, snapshot.Waiting.ArtifactRefs...)
	if snapshot.Output != nil {
		for _, ref := range snapshot.Output.Output.DeliveryRefs {
			snapshot.ArtifactRefs = append(snapshot.ArtifactRefs, artifacts.ArtifactRef(ref))
		}
		if snapshot.Output.Output.ReportRef != "" {
			snapshot.ArtifactRefs = append(snapshot.ArtifactRefs, artifacts.ArtifactRef(snapshot.Output.Output.ReportRef))
		}
		for _, ref := range snapshot.Output.Output.EvidenceRefs {
			snapshot.ArtifactRefs = append(snapshot.ArtifactRefs, artifacts.ArtifactRef(ref))
		}
	}
	if snapshot.CurrentPhysical != nil {
		key := executionreceipt.Key{TaskID: snapshot.Key.TaskID, InvocationID: snapshot.Key.InvocationID, Generation: snapshot.Key.Generation, ExecutionEpoch: int64(snapshot.CurrentPhysical.ExecutionEpoch)}
		err = tx.QueryRowContext(ctx, "SELECT payload FROM runtime_receipts WHERE task_id=? AND invocation_id=? AND generation=? AND execution_epoch=?", key.TaskID, key.InvocationID, key.Generation, key.ExecutionEpoch).Scan(&payload)
		if err == nil {
			var receipt executionreceipt.Receipt
			if err = json.Unmarshal(payload, &receipt); err != nil {
				return ErrRecoverySnapshotInconsistent
			}
			snapshot.Receipt = &receipt
		} else if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("load package receipt: %w", err)
		}
	}
	return nil
}
