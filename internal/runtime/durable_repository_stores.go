package runtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/executionreceipt"
)

type sqliteWaitingStore struct{ r *SQLiteRuntimeStateRepository }

func (s sqliteWaitingStore) Create(ctx context.Context, v WaitingRecord) (WaitingRecord, error) {
	if err := v.Validate(); err != nil {
		return WaitingRecord{}, err
	}
	v.Revision = 1
	v.CreatedAt = time.Now().UTC()
	v.UpdatedAt = v.CreatedAt
	b, _ := runtimeJSON(v)
	if err := noSecrets(b); err != nil {
		return WaitingRecord{}, err
	}
	tx, e := s.r.db.BeginTx(ctx, nil)
	if e != nil {
		return WaitingRecord{}, e
	}
	defer tx.Rollback()
	_, e = tx.ExecContext(ctx, "INSERT INTO runtime_waiting VALUES(?,?,?,?,?,?)", v.Key.TaskID, v.Key.InvocationID, v.Key.Generation, v.Revision, v.State, b)
	if e != nil {
		return WaitingRecord{}, mapUnique(e, ErrWaitingRecordExists)
	}
	if e = appendEvent(ctx, tx, "WaitingRecordCreated", v.Key, 0, "waiting", v.Revision, b); e != nil {
		return WaitingRecord{}, e
	}
	return v, tx.Commit()
}
func (s sqliteWaitingStore) Get(ctx context.Context, k WaitingKey) (WaitingRecord, bool, error) {
	var b []byte
	e := s.r.db.QueryRowContext(ctx, "SELECT payload FROM runtime_waiting WHERE task_id=? AND invocation_id=? AND generation=?", k.TaskID, k.InvocationID, k.Generation).Scan(&b)
	if errors.Is(e, sql.ErrNoRows) {
		return WaitingRecord{}, false, nil
	}
	if e != nil {
		return WaitingRecord{}, false, e
	}
	var v WaitingRecord
	e = json.Unmarshal(b, &v)
	return v, e == nil, e
}
func (s sqliteWaitingStore) CompareAndSwap(ctx context.Context, k WaitingKey, old int64, v WaitingRecord) (WaitingRecord, bool, error) {
	if e := v.Validate(); e != nil {
		return WaitingRecord{}, false, e
	}
	if v.Key != k {
		return WaitingRecord{}, false, errors.New("waiting record key cannot change")
	}
	v.Revision = old + 1
	v.UpdatedAt = time.Now().UTC()
	b, _ := runtimeJSON(v)
	if e := noSecrets(b); e != nil {
		return WaitingRecord{}, false, e
	}
	tx, e := s.r.db.BeginTx(ctx, nil)
	if e != nil {
		return WaitingRecord{}, false, e
	}
	defer tx.Rollback()
	res, e := tx.ExecContext(ctx, "UPDATE runtime_waiting SET revision=?,state=?,payload=? WHERE task_id=? AND invocation_id=? AND generation=? AND revision=?", v.Revision, v.State, b, k.TaskID, k.InvocationID, k.Generation, old)
	if e != nil {
		return WaitingRecord{}, false, e
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return WaitingRecord{}, false, tx.Rollback()
	}
	if e = appendEvent(ctx, tx, "WaitingRecordTransitioned", k, 0, "waiting", v.Revision, b); e != nil {
		return WaitingRecord{}, false, e
	}
	return v, true, tx.Commit()
}
func (s sqliteWaitingStore) Delete(ctx context.Context, k WaitingKey, old int64) (bool, error) {
	res, e := s.r.db.ExecContext(ctx, "DELETE FROM runtime_waiting WHERE task_id=? AND invocation_id=? AND generation=? AND revision=?", k.TaskID, k.InvocationID, k.Generation, old)
	if e != nil {
		return false, e
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

type sqliteContinuationStore struct{ r *SQLiteRuntimeStateRepository }

func (s sqliteContinuationStore) Put(ctx context.Context, ref ContinuationRef, v ContinuationMaterial) error {
	if ref == "" {
		return errors.New("continuation reference is required")
	}
	b, _ := runtimeJSON(v)
	if e := noSecrets(b); e != nil {
		return e
	}
	_, e := s.r.db.ExecContext(ctx, "INSERT INTO runtime_continuations VALUES(?,?)", ref, b)
	return e
}
func (s sqliteContinuationStore) ResolveContinuation(ctx context.Context, ref ContinuationRef) (ContinuationMaterial, error) {
	var b []byte
	e := s.r.db.QueryRowContext(ctx, "SELECT payload FROM runtime_continuations WHERE ref=?", ref).Scan(&b)
	if errors.Is(e, sql.ErrNoRows) {
		return ContinuationMaterial{}, ErrContinuationNotFound
	}
	var v ContinuationMaterial
	if e == nil {
		e = json.Unmarshal(b, &v)
	}
	return v, e
}

type sqliteInputStore struct{ r *SQLiteRuntimeStateRepository }

func (s sqliteInputStore) Put(ctx context.Context, k WaitingKey, v StoredPhaseInputSet) error {
	if k.TaskID == "" || k.InvocationID == "" || k.Generation <= 0 || v.Inputs.InputRevision == "" {
		return errors.New("input store key and input revision are required")
	}
	b, _ := runtimeJSON(v)
	if e := noSecrets(b); e != nil {
		return e
	}
	var n int
	e := s.r.db.QueryRowContext(ctx, "SELECT COALESCE(MAX(sequence),0)+1 FROM runtime_inputs WHERE task_id=? AND invocation_id=? AND generation=?", k.TaskID, k.InvocationID, k.Generation).Scan(&n)
	if e != nil {
		return e
	}
	_, e = s.r.db.ExecContext(ctx, "INSERT INTO runtime_inputs VALUES(?,?,?,?,?,?)", k.TaskID, k.InvocationID, k.Generation, v.Inputs.InputRevision, n, b)
	return e
}
func (s sqliteInputStore) ResolveRehydrationInputs(ctx context.Context, r WaitingRecord) (RehydrationInputSnapshot, error) {
	rows, e := s.r.db.QueryContext(ctx, "SELECT payload FROM runtime_inputs WHERE task_id=? AND invocation_id=? AND generation=? ORDER BY sequence", r.Key.TaskID, r.Key.InvocationID, r.Key.Generation)
	if e != nil {
		return RehydrationInputSnapshot{}, e
	}
	defer rows.Close()
	var all []StoredPhaseInputSet
	for rows.Next() {
		var b []byte
		rows.Scan(&b)
		var v StoredPhaseInputSet
		if e = json.Unmarshal(b, &v); e != nil {
			return RehydrationInputSnapshot{}, e
		}
		all = append(all, v)
	}
	if len(all) == 0 {
		return RehydrationInputSnapshot{}, errors.New("no authoritative input set exists for waiting invocation")
	}
	i := -1
	for x, v := range all {
		if v.Inputs.InputRevision == r.InputRevision {
			i = x
		}
	}
	if i < 0 {
		return RehydrationInputSnapshot{}, errors.New("waiting input revision is not in authoritative history")
	}
	last := all[len(all)-1]
	return RehydrationInputSnapshot{Inputs: last.Inputs, NewlyDelivered: newlyDelivered(all[i].Inputs.Delivered, last.Inputs.Delivered), RevisionIsNewer: i < len(all)-1, AwaitConditionSatisfied: last.AwaitConditionSatisfied, TerminalReason: last.TerminalReason}, nil
}
func (s sqliteInputStore) RebindInputsForContinuation(ctx context.Context, v ContinuationBinding) (ContinuationBinding, error) {
	if v.BindingRef == "" {
		// BindingRef is repository-owned just as it is in the in-memory
		// implementation. The timestamp is not a domain identity; the
		// immutable binding payload remains the authority and the UNIQUE key
		// fences a collision.
		v.BindingRef = fmt.Sprintf("binding-durable-%d", time.Now().UTC().UnixNano())
	}
	if e := ValidateContinuationRebind(v); e != nil {
		return ContinuationBinding{}, e
	}
	b, _ := runtimeJSON(v)
	_, e := s.r.db.ExecContext(ctx, "INSERT INTO runtime_bindings VALUES(?,?,?,?,?)", v.BindingRef, v.InvocationID, v.Generation, v.ExecutionEpoch, b)
	return v, e
}

func (s sqliteInputStore) ResolveContinuationBinding(ctx context.Context, bindingRef string) (ContinuationBinding, bool, error) {
	var b []byte
	e := s.r.db.QueryRowContext(ctx, "SELECT payload FROM runtime_bindings WHERE binding_ref=?", bindingRef).Scan(&b)
	if errors.Is(e, sql.ErrNoRows) {
		return ContinuationBinding{}, false, nil
	}
	if e != nil {
		return ContinuationBinding{}, false, e
	}
	var v ContinuationBinding
	if e = json.Unmarshal(b, &v); e != nil {
		return ContinuationBinding{}, false, e
	}
	return v, true, nil
}

type sqlitePhysicalStore struct{ r *SQLiteRuntimeStateRepository }

func (s sqlitePhysicalStore) Create(ctx context.Context, v PhysicalExecution) (PhysicalExecution, error) {
	if e := validatePhysicalExecution(v); e != nil {
		return PhysicalExecution{}, e
	}
	v.Revision = 1
	v.CreatedAt = time.Now().UTC()
	v.UpdatedAt = v.CreatedAt
	b, _ := runtimeJSON(v)
	if e := noSecrets(b); e != nil {
		return PhysicalExecution{}, e
	}
	tx, e := s.r.db.BeginTx(ctx, nil)
	if e != nil {
		return v, e
	}
	defer tx.Rollback()
	_, e = tx.ExecContext(ctx, "INSERT INTO runtime_physical_executions VALUES(?,?,?,?,?,?,?)", v.TaskID, v.InvocationID, v.Generation, v.ExecutionEpoch, v.Revision, v.State, b)
	if e != nil {
		return PhysicalExecution{}, e
	}
	k := WaitingKey{TaskID: v.TaskID, InvocationID: v.InvocationID, Generation: v.Generation}
	if e = appendEvent(ctx, tx, "PhysicalExecutionCreated", k, v.ExecutionEpoch, "physical", v.Revision, b); e != nil {
		return PhysicalExecution{}, e
	}
	return v, tx.Commit()
}
func (s sqlitePhysicalStore) Get(ctx context.Context, k PhysicalExecutionKey) (PhysicalExecution, bool, error) {
	var b []byte
	e := s.r.db.QueryRowContext(ctx, "SELECT payload FROM runtime_physical_executions WHERE task_id=? AND invocation_id=? AND generation=? AND execution_epoch=?", k.TaskID, k.InvocationID, k.Generation, k.ExecutionEpoch).Scan(&b)
	if errors.Is(e, sql.ErrNoRows) {
		return PhysicalExecution{}, false, nil
	}
	var v PhysicalExecution
	if e == nil {
		e = json.Unmarshal(b, &v)
	}
	return v, e == nil, e
}
func (s sqlitePhysicalStore) CompareAndSwap(ctx context.Context, k PhysicalExecutionKey, old int64, v PhysicalExecution) (PhysicalExecution, bool, error) {
	v.Revision = old + 1
	v.UpdatedAt = time.Now().UTC()
	b, _ := runtimeJSON(v)
	if e := noSecrets(b); e != nil {
		return PhysicalExecution{}, false, e
	}
	tx, e := s.r.db.BeginTx(ctx, nil)
	if e != nil {
		return PhysicalExecution{}, false, e
	}
	defer tx.Rollback()
	res, e := tx.ExecContext(ctx, "UPDATE runtime_physical_executions SET revision=?,state=?,payload=? WHERE task_id=? AND invocation_id=? AND generation=? AND execution_epoch=? AND revision=?", v.Revision, v.State, b, k.TaskID, k.InvocationID, k.Generation, k.ExecutionEpoch, old)
	if e != nil {
		return PhysicalExecution{}, false, e
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return PhysicalExecution{}, false, tx.Rollback()
	}
	wk := WaitingKey{TaskID: k.TaskID, InvocationID: k.InvocationID, Generation: k.Generation}
	if e = appendEvent(ctx, tx, "PhysicalExecutionTransitioned", wk, k.ExecutionEpoch, "physical", v.Revision, b); e != nil {
		return PhysicalExecution{}, false, e
	}
	return v, true, tx.Commit()
}
func (s sqlitePhysicalStore) ListByInvocation(ctx context.Context, t, i string, g int) ([]PhysicalExecution, error) {
	rows, e := s.r.db.QueryContext(ctx, "SELECT payload FROM runtime_physical_executions WHERE task_id=? AND invocation_id=? AND generation=? ORDER BY execution_epoch", t, i, g)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var out []PhysicalExecution
	for rows.Next() {
		var b []byte
		rows.Scan(&b)
		var v PhysicalExecution
		if e = json.Unmarshal(b, &v); e != nil {
			return nil, e
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

type sqliteReceiptStore struct{ r *SQLiteRuntimeStateRepository }

func (s sqliteReceiptStore) PutIfAbsent(ctx context.Context, v executionreceipt.Receipt) (executionreceipt.Receipt, bool, error) {
	if v.TaskID == "" || v.InvocationID == "" || v.Generation <= 0 || v.ExecutionEpoch <= 0 || v.BindingRef == "" || v.InputRevision == "" || v.PackageDigest == "" || v.SessionIdentity == "" || !v.Consumed {
		return executionreceipt.Receipt{}, false, errors.New("complete consumed package receipt is required")
	}
	v.RecordedAt = time.Now().UTC()
	v.Revision = 1
	b, _ := runtimeJSON(v)
	if e := noSecrets(b); e != nil {
		return executionreceipt.Receipt{}, false, e
	}
	tx, e := s.r.db.BeginTx(ctx, nil)
	if e != nil {
		return v, false, e
	}
	defer tx.Rollback()
	_, e = tx.ExecContext(ctx, "INSERT INTO runtime_receipts VALUES(?,?,?,?,?)", v.TaskID, v.InvocationID, v.Generation, v.ExecutionEpoch, b)
	if e == nil {
		wk := WaitingKey{TaskID: v.TaskID, InvocationID: v.InvocationID, Generation: v.Generation}
		if e = appendEvent(ctx, tx, "PackageConsumptionRecorded", wk, ExecutionEpoch(v.ExecutionEpoch), "receipt", v.Revision, b); e != nil {
			return v, false, e
		}
		return v, true, tx.Commit()
	}
	if !strings.Contains(e.Error(), "UNIQUE constraint failed") {
		return v, false, e
	}
	existing, found, e := s.Get(ctx, v.Key())
	if e != nil || !found {
		return v, false, e
	}
	if existing.BindingRef != v.BindingRef || existing.InputRevision != v.InputRevision || existing.PackageDigest != v.PackageDigest || existing.SessionIdentity != v.SessionIdentity || existing.Consumed != v.Consumed {
		return existing, false, errors.New("conflicting package consumption receipt")
	}
	return existing, false, nil
}
func (s sqliteReceiptStore) Get(ctx context.Context, k executionreceipt.Key) (executionreceipt.Receipt, bool, error) {
	var b []byte
	e := s.r.db.QueryRowContext(ctx, "SELECT payload FROM runtime_receipts WHERE task_id=? AND invocation_id=? AND generation=? AND execution_epoch=?", k.TaskID, k.InvocationID, k.Generation, k.ExecutionEpoch).Scan(&b)
	if errors.Is(e, sql.ErrNoRows) {
		return executionreceipt.Receipt{}, false, nil
	}
	var v executionreceipt.Receipt
	if e == nil {
		e = json.Unmarshal(b, &v)
	}
	return v, e == nil, e
}

type sqliteOutputStore struct{ r *SQLiteRuntimeStateRepository }

func (s sqliteOutputStore) PutIfAbsent(ctx context.Context, v PhaseOutputRecord) (PhaseOutputRecord, bool, error) {
	if v.Key.TaskID == "" || v.Key.InvocationID == "" || v.Key.Generation <= 0 || v.BindingRef == "" || v.InputRevision == "" || v.ExecutionEpoch <= 0 {
		return PhaseOutputRecord{}, false, errors.New("phase output identity is required")
	}
	v.Revision = 1
	v.AcceptedAt = time.Now().UTC()
	b, _ := runtimeJSON(v)
	if e := noSecrets(b); e != nil {
		return PhaseOutputRecord{}, false, e
	}
	tx, e := s.r.db.BeginTx(ctx, nil)
	if e != nil {
		return v, false, e
	}
	defer tx.Rollback()
	_, e = tx.ExecContext(ctx, "INSERT INTO runtime_phase_outputs VALUES(?,?,?,?,?)", v.Key.TaskID, v.Key.InvocationID, v.Key.Generation, v.Revision, b)
	if e == nil {
		wk := WaitingKey{TaskID: v.Key.TaskID, InvocationID: v.Key.InvocationID, Generation: v.Key.Generation}
		if e = appendEvent(ctx, tx, "PhaseOutputRecorded", wk, v.ExecutionEpoch, "phase_output", v.Revision, b); e != nil {
			return v, false, e
		}
		return v, true, tx.Commit()
	}
	if !strings.Contains(e.Error(), "UNIQUE constraint failed") {
		return v, false, e
	}
	existing, found, e := s.Get(ctx, v.Key)
	if e != nil || !found {
		return v, false, e
	}
	if existing.BindingRef != v.BindingRef || existing.InputRevision != v.InputRevision || existing.ExecutionEpoch != v.ExecutionEpoch || !reflect.DeepEqual(existing.Output, v.Output) {
		return existing, false, ErrConflictingOutput
	}
	return existing, false, nil
}
func (s sqliteOutputStore) Get(ctx context.Context, k PhaseOutputKey) (PhaseOutputRecord, bool, error) {
	var b []byte
	e := s.r.db.QueryRowContext(ctx, "SELECT payload FROM runtime_phase_outputs WHERE task_id=? AND invocation_id=? AND generation=?", k.TaskID, k.InvocationID, k.Generation).Scan(&b)
	if errors.Is(e, sql.ErrNoRows) {
		return PhaseOutputRecord{}, false, nil
	}
	var v PhaseOutputRecord
	if e == nil {
		e = json.Unmarshal(b, &v)
	}
	return v, e == nil, e
}
func (s sqliteOutputStore) CompareAndSwap(ctx context.Context, k PhaseOutputKey, old int64, v PhaseOutputRecord) (PhaseOutputRecord, bool, error) {
	v.Revision = old + 1
	b, _ := runtimeJSON(v)
	if e := noSecrets(b); e != nil {
		return PhaseOutputRecord{}, false, e
	}
	tx, e := s.r.db.BeginTx(ctx, nil)
	if e != nil {
		return v, false, e
	}
	defer tx.Rollback()
	res, e := tx.ExecContext(ctx, "UPDATE runtime_phase_outputs SET revision=?,payload=? WHERE task_id=? AND invocation_id=? AND generation=? AND revision=?", v.Revision, b, k.TaskID, k.InvocationID, k.Generation, old)
	if e != nil {
		return v, false, e
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return PhaseOutputRecord{}, false, tx.Rollback()
	}
	wk := WaitingKey{TaskID: k.TaskID, InvocationID: k.InvocationID, Generation: k.Generation}
	if e = appendEvent(ctx, tx, "PhaseOutputUpdated", wk, v.ExecutionEpoch, "phase_output", v.Revision, b); e != nil {
		return v, false, e
	}
	return v, true, tx.Commit()
}

func appendEvent(ctx context.Context, tx *sql.Tx, typ string, k WaitingKey, epoch ExecutionEpoch, aggregate string, rev int64, payload []byte) error {
	id := fmt.Sprintf("evt-%d", time.Now().UnixNano())
	var ep any = nil
	if epoch > 0 {
		ep = int(epoch)
	}
	_, e := tx.ExecContext(ctx, "INSERT INTO runtime_events VALUES(?,?,?,?,?,?,?,?,?,?,?)", id, typ, time.Now().UTC().Format(time.RFC3339Nano), k.TaskID, k.InvocationID, k.Generation, ep, aggregate, rev, 1, payload)
	return e
}
func (r *SQLiteRuntimeStateRepository) ListRuntimeEvents(ctx context.Context) ([]RuntimeEvent, error) {
	rows, e := r.db.QueryContext(ctx, "SELECT event_id,event_type,occurred_at,task_id,invocation_id,generation,execution_epoch,aggregate_key,result_revision,payload_version,payload FROM runtime_events ORDER BY occurred_at,event_id")
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var out []RuntimeEvent
	for rows.Next() {
		var v RuntimeEvent
		var at string
		var ep sql.NullInt64
		if e = rows.Scan(&v.EventID, &v.EventType, &at, &v.TaskID, &v.InvocationID, &v.Generation, &ep, &v.AggregateKey, &v.ResultRevision, &v.PayloadVersion, &v.Payload); e != nil {
			return nil, e
		}
		v.OccurredAt, _ = time.Parse(time.RFC3339Nano, at)
		if ep.Valid {
			x := ExecutionEpoch(ep.Int64)
			v.ExecutionEpoch = &x
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
func mapUnique(e, errorTarget error) error {
	if strings.Contains(e.Error(), "UNIQUE constraint failed") {
		return errorTarget
	}
	return e
}
