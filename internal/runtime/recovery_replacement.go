package runtime

import (
	"context"
	"errors"
	"fmt"
)

// CarrierRecoveryDecision keeps execution-plane observation separate from the
// sole C4-4 mutation that may replace a carrier.
type CarrierRecoveryDecision string

const (
	CarrierRecoveryContinueExisting CarrierRecoveryDecision = "continue_existing_carrier"
	CarrierRecoveryWaitObservation  CarrierRecoveryDecision = "wait_for_observation"
	CarrierRecoveryReconcileOld     CarrierRecoveryDecision = "reconcile_old_carrier"
	CarrierRecoveryReplaceLost      CarrierRecoveryDecision = "replace_lost_carrier"
	CarrierRecoveryTerminalNoOp     CarrierRecoveryDecision = "terminal_noop"
	CarrierRecoveryFailClosed       CarrierRecoveryDecision = "fail_closed"
)

// RecoveryReplacementPlan is a secret-free durable reservation for C4-5. It
// authorizes no token, Worker, credential, task, or session.
type RecoveryReplacementPlan struct {
	TaskID, InvocationID string
	Generation           int
	OldExecutionEpoch    ExecutionEpoch
	NewExecutionEpoch    ExecutionEpoch
	BindingRef           string
	InputRevision        string
	ContinuationRef      ContinuationRef
	ArtifactRefs         []string
	EventRefs            []string
	EvidenceRefs         []string
	RequiresFreshCarrier bool
	RequiresFreshReceipt bool
}

func replacementPlan(waiting WaitingRecord, old, next ExecutionEpoch) RecoveryReplacementPlan {
	refs := make([]string, 0, len(waiting.ArtifactRefs))
	for _, ref := range waiting.ArtifactRefs {
		refs = append(refs, string(ref))
	}
	return RecoveryReplacementPlan{
		TaskID: waiting.Key.TaskID, InvocationID: waiting.Key.InvocationID, Generation: waiting.Key.Generation,
		OldExecutionEpoch: old, NewExecutionEpoch: next, BindingRef: waiting.PreviousBindingRef,
		InputRevision: waiting.InputRevision, ContinuationRef: waiting.ContinuationRef,
		ArtifactRefs: refs, EventRefs: append([]string(nil), waiting.EventRefs...), EvidenceRefs: append([]string(nil), waiting.EvidenceRefs...),
		RequiresFreshCarrier: true, RequiresFreshReceipt: true,
	}
}

func addSnapshotArtifacts(plan *RecoveryReplacementPlan, snapshot RecoverySnapshot) {
	seen := make(map[string]struct{}, len(plan.ArtifactRefs))
	for _, ref := range plan.ArtifactRefs {
		seen[ref] = struct{}{}
	}
	for _, ref := range snapshot.ArtifactRefs {
		value := string(ref)
		if value == "" {
			continue
		}
		if _, exists := seen[value]; !exists {
			plan.ArtifactRefs = append(plan.ArtifactRefs, value)
			seen[value] = struct{}{}
		}
	}
}

// DecideCarrierRecovery freezes C4-4's conservative replacement policy.
func DecideCarrierRecovery(snapshot RecoverySnapshot, observation PhysicalExecutionObservation) CarrierRecoveryDecision {
	if snapshot.Output != nil || (snapshot.Waiting != nil && snapshot.Waiting.State == AwaitStateTerminal) {
		return CarrierRecoveryTerminalNoOp
	}
	if snapshot.CurrentPhysical != nil && snapshot.CurrentPhysical.State == PhysicalExecutionFailed {
		return CarrierRecoveryReplaceLost
	}
	if len(observation.SourceErrors) != 0 || observation.Worker == ObservedWorkerUnknown {
		return CarrierRecoveryWaitObservation
	}
	if observation.Worker == ObservedWorkerNotFound || observation.Worker == ObservedWorkerFailed {
		return CarrierRecoveryReplaceLost
	}
	if observation.Identity == ObservedCarrierIdentityMismatch || observation.Worker == ObservedWorkerTerminating {
		return CarrierRecoveryReconcileOld
	}
	if observation.Runtime == ObservedRuntimeGenerationPending || observation.MCP == ObservedMCPNotApplied {
		return CarrierRecoveryWaitObservation
	}
	if observation.Worker == ObservedWorkerReady && observation.Identity == ObservedCarrierIdentityVerified && observation.Runtime == ObservedRuntimeApplied && observation.MCP == ObservedMCPApplied {
		return CarrierRecoveryContinueExisting
	}
	return CarrierRecoveryFailClosed
}

// PrepareLostCarrierReplacement holds one RecoveryClaim across snapshot,
// observation and durable allocation. It performs no execution-plane write.
func (c *RecoveryCoordinator) PrepareLostCarrierReplacement(ctx context.Context, key WaitingKey) (RecoveryReplacementPlan, error) {
	if err := c.validateObservation(); err != nil {
		return RecoveryReplacementPlan{}, err
	}
	if c.Mutations == nil {
		return RecoveryReplacementPlan{}, errors.New("recovery lifecycle mutations are required")
	}
	seed, err := c.Repository.Recovery().LoadRecoverySnapshot(ctx, key)
	if err != nil {
		return RecoveryReplacementPlan{}, err
	}
	if seed.CurrentExecutionEpoch <= 0 {
		return RecoveryReplacementPlan{}, ErrRecoveryDispositionUnsupported
	}
	claim, err := c.Repository.Recovery().AcquireRecoveryClaim(ctx, key, seed.CurrentExecutionEpoch, c.OwnerID, c.ClaimTTL)
	if err != nil {
		return RecoveryReplacementPlan{}, err
	}
	defer func() { _ = c.Repository.Recovery().ReleaseRecoveryClaim(context.Background(), claim) }()
	snapshot, err := c.currentSnapshot(ctx, &claim, RecoverySnapshotFingerprint{})
	if err != nil {
		return RecoveryReplacementPlan{}, err
	}
	if snapshot.CurrentPhysical != nil && snapshot.CurrentPhysical.State == PhysicalExecutionReserved && snapshot.CurrentPhysical.ReplacesExecutionEpoch > 0 && snapshot.Waiting != nil && snapshot.Waiting.State == AwaitStateRehydrating {
		plan := replacementPlan(*snapshot.Waiting, snapshot.CurrentPhysical.ReplacesExecutionEpoch, snapshot.CurrentPhysical.ExecutionEpoch)
		addSnapshotArtifacts(&plan, snapshot)
		return plan, nil
	}
	disposition, err := ClassifyRecoverySnapshot(snapshot)
	if err != nil {
		return RecoveryReplacementPlan{}, err
	}
	var observation PhysicalExecutionObservation
	if disposition == RecoveryFailedPhysicalExecutionNeedsDecision {
		observation.Worker = ObservedWorkerFailed
	} else {
		if (disposition != RecoveryCarrierActiveNeedsObservation && disposition != RecoveryCarrierConsumedNoOutput) || snapshot.CurrentPhysical == nil {
			return RecoveryReplacementPlan{}, ErrRecoveryDispositionUnsupported
		}
		request := observationRequest(*snapshot.CurrentPhysical)
		if err = request.validate(); err != nil {
			return RecoveryReplacementPlan{}, err
		}
		observation, err = c.Observer.Observe(ctx, request)
		if err != nil {
			return RecoveryReplacementPlan{}, err
		}
		if _, err = c.currentSnapshot(ctx, &claim, snapshot.Fingerprint()); err != nil {
			return RecoveryReplacementPlan{}, err
		}
	}
	decision := DecideCarrierRecovery(snapshot, observation)
	if decision != CarrierRecoveryReplaceLost {
		return RecoveryReplacementPlan{}, fmt.Errorf("%w: %s", ErrRecoveryDispositionUnsupported, decision)
	}
	plan, _, err := c.Mutations.FenceAndAllocateReplacement(ctx, claim, snapshot.Fingerprint(), decision)
	if err == nil {
		addSnapshotArtifacts(&plan, snapshot)
	}
	return plan, err
}

func (s sqliteLifecycleMutations) FenceAndAllocateReplacement(ctx context.Context, claim RecoveryClaim, fingerprint RecoverySnapshotFingerprint, decision CarrierRecoveryDecision) (RecoveryReplacementPlan, bool, error) {
	if decision != CarrierRecoveryReplaceLost || claim.Key.TaskID == "" || claim.Key.InvocationID == "" || claim.Key.Generation <= 0 || claim.ObservedExecutionEpoch <= 0 {
		return RecoveryReplacementPlan{}, false, errors.New("lost-carrier replacement identity is required")
	}
	tx, err := s.r.db.BeginTx(ctx, nil)
	if err != nil {
		return RecoveryReplacementPlan{}, false, err
	}
	defer tx.Rollback()
	currentClaim, found, err := recoveryClaimRow(ctx, tx, claim.Key)
	if err != nil || !found || currentClaim.OwnerID != claim.OwnerID || currentClaim.Fence != claim.Fence || currentClaim.Revision != claim.Revision || !currentClaim.LeaseExpiresAt.After(s.r.clock.Now().UTC()) {
		return RecoveryReplacementPlan{}, false, ErrRecoveryClaimLost
	}
	var wb []byte
	err = tx.QueryRowContext(ctx, "SELECT payload FROM runtime_waiting WHERE task_id=? AND invocation_id=? AND generation=?", claim.Key.TaskID, claim.Key.InvocationID, claim.Key.Generation).Scan(&wb)
	if isNoRows(err) {
		return RecoveryReplacementPlan{}, false, ErrRecoverySnapshotStale
	}
	if err != nil {
		return RecoveryReplacementPlan{}, false, err
	}
	var waiting WaitingRecord
	if err = jsonUnmarshal(wb, &waiting); err != nil {
		return RecoveryReplacementPlan{}, false, err
	}
	if waiting.Key != claim.Key || waiting.Revision != fingerprint.WaitingRevision || waiting.ExecutionEpoch != fingerprint.ExecutionEpoch || waiting.PreviousBindingRef != fingerprint.BindingRef || waiting.InputRevision != fingerprint.InputRevision || waiting.State != AwaitStateRunning {
		return RecoveryReplacementPlan{}, false, ErrRecoverySnapshotStale
	}
	var outputCount int
	if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM runtime_phase_outputs WHERE task_id=? AND invocation_id=? AND generation=?", claim.Key.TaskID, claim.Key.InvocationID, claim.Key.Generation).Scan(&outputCount); err != nil {
		return RecoveryReplacementPlan{}, false, err
	}
	if outputCount != 0 {
		return RecoveryReplacementPlan{}, false, ErrRecoverySnapshotStale
	}
	rows, err := tx.QueryContext(ctx, "SELECT payload FROM runtime_physical_executions WHERE task_id=? AND invocation_id=? AND generation=? ORDER BY execution_epoch", claim.Key.TaskID, claim.Key.InvocationID, claim.Key.Generation)
	if err != nil {
		return RecoveryReplacementPlan{}, false, err
	}
	defer rows.Close()
	var old PhysicalExecution
	var maxEpoch ExecutionEpoch
	for rows.Next() {
		var b []byte
		if err = rows.Scan(&b); err != nil {
			return RecoveryReplacementPlan{}, false, err
		}
		var value PhysicalExecution
		if err = jsonUnmarshal(b, &value); err != nil {
			return RecoveryReplacementPlan{}, false, err
		}
		if value.Key().TaskID != claim.Key.TaskID || value.Key().InvocationID != claim.Key.InvocationID || value.Key().Generation != claim.Key.Generation {
			return RecoveryReplacementPlan{}, false, ErrRecoverySnapshotInconsistent
		}
		if value.ExecutionEpoch > maxEpoch {
			maxEpoch = value.ExecutionEpoch
		}
		if value.ExecutionEpoch == claim.ObservedExecutionEpoch {
			old = value
		}
	}
	if err = rows.Err(); err != nil {
		return RecoveryReplacementPlan{}, false, err
	}
	if old.Key() != (PhysicalExecutionKey{TaskID: claim.Key.TaskID, InvocationID: claim.Key.InvocationID, Generation: claim.Key.Generation, ExecutionEpoch: claim.ObservedExecutionEpoch}) || old.Revision != fingerprint.PhysicalRevision || (old.State != PhysicalExecutionRunning && old.State != PhysicalExecutionFailed) {
		return RecoveryReplacementPlan{}, false, ErrRecoverySnapshotStale
	}
	nextEpoch := maxEpoch + 1
	if nextEpoch <= claim.ObservedExecutionEpoch {
		return RecoveryReplacementPlan{}, false, ErrRecoverySnapshotInconsistent
	}
	now := nowUTC()
	old.State, old.Revision, old.UpdatedAt = PhysicalExecutionFenced, old.Revision+1, now
	ob, err := runtimeJSON(old)
	if err != nil {
		return RecoveryReplacementPlan{}, false, err
	}
	if err = noSecrets(ob); err != nil {
		return RecoveryReplacementPlan{}, false, err
	}
	reservation := PhysicalExecution{TaskID: claim.Key.TaskID, InvocationID: claim.Key.InvocationID, Generation: claim.Key.Generation, ExecutionEpoch: nextEpoch, BindingRef: waiting.PreviousBindingRef, InputRevision: waiting.InputRevision, ReplacesExecutionEpoch: claim.ObservedExecutionEpoch, RequiresFreshPackageReceipt: true, State: PhysicalExecutionReserved, Revision: 1, CreatedAt: now, UpdatedAt: now}
	rb, err := runtimeJSON(reservation)
	if err != nil {
		return RecoveryReplacementPlan{}, false, err
	}
	if err = noSecrets(rb); err != nil {
		return RecoveryReplacementPlan{}, false, err
	}
	waiting.State, waiting.ExecutionEpoch, waiting.Revision, waiting.UpdatedAt = AwaitStateRehydrating, nextEpoch, waiting.Revision+1, now
	wb, err = runtimeJSON(waiting)
	if err != nil {
		return RecoveryReplacementPlan{}, false, err
	}
	if err = noSecrets(wb); err != nil {
		return RecoveryReplacementPlan{}, false, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE runtime_physical_executions SET revision=?,state=?,payload=? WHERE task_id=? AND invocation_id=? AND generation=? AND execution_epoch=? AND revision=?", old.Revision, old.State, ob, old.TaskID, old.InvocationID, old.Generation, old.ExecutionEpoch, fingerprint.PhysicalRevision); err != nil {
		return RecoveryReplacementPlan{}, false, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO runtime_physical_executions VALUES(?,?,?,?,?,?,?)", reservation.TaskID, reservation.InvocationID, reservation.Generation, reservation.ExecutionEpoch, reservation.Revision, reservation.State, rb); err != nil {
		return RecoveryReplacementPlan{}, false, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE runtime_waiting SET revision=?,state=?,payload=? WHERE task_id=? AND invocation_id=? AND generation=? AND revision=?", waiting.Revision, waiting.State, wb, waiting.Key.TaskID, waiting.Key.InvocationID, waiting.Key.Generation, fingerprint.WaitingRevision); err != nil {
		return RecoveryReplacementPlan{}, false, err
	}
	if err = appendEvent(ctx, tx, "PhysicalExecutionFenced", claim.Key, old.ExecutionEpoch, "physical", old.Revision, ob); err != nil {
		return RecoveryReplacementPlan{}, false, err
	}
	if err = appendEvent(ctx, tx, "ReplacementEpochAllocated", claim.Key, reservation.ExecutionEpoch, "physical", reservation.Revision, rb); err != nil {
		return RecoveryReplacementPlan{}, false, err
	}
	return replacementPlan(waiting, old.ExecutionEpoch, reservation.ExecutionEpoch), true, tx.Commit()
}
