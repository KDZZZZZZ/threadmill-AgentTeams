package runtime

import (
	"context"
	"errors"
	"time"

	phasemcp "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/mcp/phase"
)

// TerminalRecoveryCleanupPorts are the restart-safe, physical cleanup
// capabilities used only after durable logical terminalization. Every input
// is reconstructed from an opaque PhysicalExecution field; no secret is read
// from RuntimeStateRepository. Implementations must treat a repeated request
// as success, because a process may stop after an external effect succeeds and
// before AdvanceTeardown records its completion.
type TerminalRecoveryCleanupPorts struct {
	Tasks       CompletedTeamHarnessTaskCleaner
	Workers     WorkerProvisioner
	MCP         MCPClientCleaner
	Credentials MCPCredentialProvisioner
	Leases      WorkspaceLeaseAcquirer
	Bindings    *phasemcp.BindingRegistry
}

func (p TerminalRecoveryCleanupPorts) validate() error {
	if p.Tasks == nil || p.Workers == nil || p.MCP == nil || p.Credentials == nil || p.Leases == nil || p.Bindings == nil {
		return errors.New("terminal recovery cleanup ports are required")
	}
	return nil
}

func (p TerminalRecoveryCleanupPorts) run(ctx context.Context, step TeardownStep, execution PhysicalExecution) error {
	switch step {
	case TeardownStepTask:
		return p.Tasks.CompleteTeamHarnessTask(ctx, TeamHarnessTask{ID: execution.TeamHarnessTaskID, AssignedTo: execution.TeamHarnessAssignedTo})
	case TeardownStepWorker:
		return p.Workers.DeleteWorker(ctx, ProvisionedWorker{ID: execution.WorkerID, Name: execution.WorkerName, MCPClientID: execution.MCPClientID, RuntimeGeneration: execution.DesiredRuntimeGeneration})
	case TeardownStepMCP:
		return p.MCP.CleanupWorkerMCP(ctx, ProvisionedWorker{ID: execution.WorkerID, Name: execution.WorkerName, MCPClientID: execution.MCPClientID, RuntimeGeneration: execution.DesiredRuntimeGeneration})
	case TeardownStepCredential:
		return p.Credentials.RevokeMCPCredential(ctx, MCPCredentialBinding{Ref: execution.CredentialBindingRef, WorkerName: execution.WorkerName})
	case TeardownStepToken:
		// BindingRegistry is the sole token resolver. A restarted process has a
		// fresh registry and therefore cannot resolve the old raw token; when a
		// registry is still present, revoke by the complete trusted identity.
		p.Bindings.RevokeBinding(phasemcp.InvocationBinding{
			TaskID: execution.TaskID, InvocationID: execution.InvocationID, Generation: execution.Generation,
			ExecutionEpoch: int64(execution.ExecutionEpoch), BindingRef: execution.BindingRef, InputRevision: execution.InputRevision,
		})
		return nil
	case TeardownStepLease:
		return p.Leases.ReleaseWorkspaceLease(ctx, WorkspaceLease{Ref: execution.WorkspaceLeaseRef, Epoch: execution.ExecutionEpoch})
	default:
		return errors.New("unknown terminal recovery teardown step")
	}
}

// RecoveryCoordinator is the first production restart reconciliation seam.
// C4-2 deliberately supports only terminal states: it never observes a
// carrier, provisions one, creates an epoch, or resumes an agent session.
type RecoveryCoordinator struct {
	Repository RuntimeStateRepository
	Mutations  LifecycleMutationStore
	Cleanup    TerminalRecoveryCleanupPorts
	Observer   PhysicalExecutionObserver
	// Reprovision consumes a C4-4 reservation only.  It is deliberately an
	// internal seam so reconciliation cannot recreate Worker/token/credential
	// plumbing or invent a second execution lifecycle.
	Reprovision ReplacementEpochReprovisioner
	OwnerID     string
	ClaimTTL    time.Duration

	// now is intentionally private and replaceable by package tests. Recovery
	// lease tests use a fake clock; production defaults to wall clock.
	now func() time.Time
}

// ReplacementEpochReprovisioner is satisfied by C4-5A's
// ReplacementReprovisionCoordinator.  The returned execution is redacted
// durable evidence, never a capability or a restored agent session.
type ReplacementEpochReprovisioner interface {
	Reprovision(context.Context, WaitingKey) (PhysicalExecution, error)
}

// RecoveryAction describes the single authoritative action chosen by one
// reconciliation attempt.  It is intentionally secret-free so callers may
// log or expose it as operational evidence.
type RecoveryAction string

const (
	RecoveryActionNoDurableInvocation RecoveryAction = "no_durable_invocation"
	RecoveryActionAwaitingInput       RecoveryAction = "awaiting_input"
	RecoveryActionTerminalNoOp        RecoveryAction = "terminal_noop"
	RecoveryActionTerminalCleanup     RecoveryAction = "terminal_cleanup"
	RecoveryActionCarrierActive       RecoveryAction = "carrier_still_active"
	RecoveryActionRetryRequired       RecoveryAction = "retry_required"
	RecoveryActionFailClosed          RecoveryAction = "fail_closed"
	RecoveryActionReplacementReserved RecoveryAction = "replacement_reserved"
	RecoveryActionReplacementActive   RecoveryAction = "replacement_reprovisioned"
)

// RecoveryReconciliationResult is the deterministic, secret-free outcome of
// one invocation recovery attempt.  Snapshot records remain authority; this
// result is only an operational explanation of the selected action.
type RecoveryReconciliationResult struct {
	Key            WaitingKey
	Disposition    RecoveryDisposition
	Action         RecoveryAction
	ExecutionEpoch ExecutionEpoch
	PhysicalState  PhysicalExecutionState
	Retryable      bool
	Reason         string
}

func (c *RecoveryCoordinator) validate() error {
	if c == nil || c.Repository == nil || c.Mutations == nil || c.OwnerID == "" {
		return errors.New("recovery coordinator dependencies are required")
	}
	if c.ClaimTTL <= 0 {
		return errors.New("recovery claim ttl is required")
	}
	return c.Cleanup.validate()
}

func (c *RecoveryCoordinator) clockNow() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

// Reconcile is retained for existing callers.  ReconcileInvocation is the C4-5
// production entrypoint and returns the deterministic action as well.
func (c *RecoveryCoordinator) Reconcile(ctx context.Context, key WaitingKey) error {
	_, err := c.ReconcileInvocation(ctx, key)
	return err
}

// ReconcileInvocation serializes one complete recovery decision for a durable
// logical invocation.  It uses Runtime SQLite records as truth; controller,
// taskflow and QwenPaw observations are evidence only.  A reservation is the
// hand-off between the claim-fenced decision and C4-5A reprovisioning, so a
// retry can never allocate another epoch.
func (c *RecoveryCoordinator) ReconcileInvocation(ctx context.Context, key WaitingKey) (RecoveryReconciliationResult, error) {
	if err := c.validate(); err != nil {
		return RecoveryReconciliationResult{Key: key, Action: RecoveryActionFailClosed}, err
	}
	seed, err := c.Repository.Recovery().LoadRecoverySnapshot(ctx, key)
	if err != nil {
		return RecoveryReconciliationResult{Key: key, Action: RecoveryActionFailClosed}, err
	}
	if seed.CurrentExecutionEpoch <= 0 {
		disposition, classifyErr := ClassifyRecoverySnapshot(seed)
		if classifyErr != nil {
			return RecoveryReconciliationResult{Key: key, Action: RecoveryActionFailClosed}, classifyErr
		}
		if disposition == RecoveryNoDurableInvocation {
			return RecoveryReconciliationResult{Key: key, Disposition: disposition, Action: RecoveryActionNoDurableInvocation}, nil
		}
		return RecoveryReconciliationResult{Key: key, Disposition: disposition, Action: RecoveryActionFailClosed}, ErrRecoveryDispositionUnsupported
	}
	claim, err := c.Repository.Recovery().AcquireRecoveryClaim(ctx, key, seed.CurrentExecutionEpoch, c.OwnerID, c.ClaimTTL)
	if err != nil {
		return RecoveryReconciliationResult{Key: key, Action: RecoveryActionRetryRequired, Retryable: true}, err
	}
	defer func() {
		if claim.OwnerID != "" {
			_ = c.Repository.Recovery().ReleaseRecoveryClaim(context.Background(), claim)
		}
	}()
	return c.reconcileInvocationClaimed(ctx, &claim)
}

func recoveryResult(snapshot RecoverySnapshot, disposition RecoveryDisposition, action RecoveryAction) RecoveryReconciliationResult {
	result := RecoveryReconciliationResult{Key: snapshot.Key, Disposition: disposition, Action: action, ExecutionEpoch: snapshot.CurrentExecutionEpoch}
	if snapshot.CurrentPhysical != nil {
		result.PhysicalState = snapshot.CurrentPhysical.State
	}
	return result
}

func (c *RecoveryCoordinator) reconcileInvocationClaimed(ctx context.Context, claim *RecoveryClaim) (RecoveryReconciliationResult, error) {
	for {
		snapshot, err := c.currentSnapshot(ctx, claim, RecoverySnapshotFingerprint{})
		if err != nil {
			return RecoveryReconciliationResult{Key: claim.Key, Action: RecoveryActionFailClosed}, err
		}
		disposition, err := ClassifyRecoverySnapshot(snapshot)
		if err != nil {
			return recoveryResult(snapshot, "", RecoveryActionFailClosed), err
		}
		switch disposition {
		case RecoveryTerminalNoOp:
			return recoveryResult(snapshot, disposition, RecoveryActionTerminalNoOp), nil
		case RecoveryContinueTerminalTeardown:
			if snapshot.CurrentPhysical == nil {
				return recoveryResult(snapshot, disposition, RecoveryActionFailClosed), ErrRecoverySnapshotInconsistent
			}
			if err = c.reconcileTerminalStep(ctx, claim, snapshot); err != nil {
				return recoveryResult(snapshot, disposition, RecoveryActionFailClosed), err
			}
			// Each durable teardown mutation changes the fingerprint; reload
			// before choosing the next (and only next) action.
			continue
		case RecoveryAwaitingInput:
			return recoveryResult(snapshot, disposition, RecoveryActionAwaitingInput), nil
		case RecoveryCarrierActiveNeedsObservation, RecoveryCarrierConsumedNoOutput, RecoveryFailedPhysicalExecutionNeedsDecision:
			return c.reconcileCarrier(ctx, claim, snapshot, disposition)
		case RecoveryReplacementProvisioningPending:
			return c.consumeReplacement(ctx, claim, snapshot, disposition)
		case RecoveryRelinquishmentIncomplete, RecoveryRehydrationIncomplete, RecoveryCarrierProvisioningIncomplete:
			return recoveryResult(snapshot, disposition, RecoveryActionRetryRequired), nil
		default:
			return recoveryResult(snapshot, disposition, RecoveryActionFailClosed), ErrRecoveryDispositionUnsupported
		}
	}
}

func (c *RecoveryCoordinator) reconcileCarrier(ctx context.Context, claim *RecoveryClaim, snapshot RecoverySnapshot, disposition RecoveryDisposition) (RecoveryReconciliationResult, error) {
	if snapshot.CurrentPhysical == nil {
		return recoveryResult(snapshot, disposition, RecoveryActionFailClosed), ErrRecoverySnapshotInconsistent
	}
	decision := CarrierRecoveryReplaceLost
	if disposition != RecoveryFailedPhysicalExecutionNeedsDecision {
		if c.Observer == nil {
			return recoveryResult(snapshot, disposition, RecoveryActionFailClosed), ErrRecoveryDispositionUnsupported
		}
		request := observationRequest(*snapshot.CurrentPhysical)
		if err := request.validate(); err != nil {
			return recoveryResult(snapshot, disposition, RecoveryActionFailClosed), err
		}
		observation, err := c.Observer.Observe(ctx, request)
		if err != nil {
			return recoveryResult(snapshot, disposition, RecoveryActionRetryRequired), err
		}
		if _, err = c.currentSnapshot(ctx, claim, snapshot.Fingerprint()); err != nil {
			return recoveryResult(snapshot, disposition, RecoveryActionFailClosed), err
		}
		decision = DecideCarrierRecovery(snapshot, observation)
	}
	switch decision {
	case CarrierRecoveryContinueExisting:
		return recoveryResult(snapshot, disposition, RecoveryActionCarrierActive), nil
	case CarrierRecoveryWaitObservation:
		result := recoveryResult(snapshot, disposition, RecoveryActionRetryRequired)
		result.Retryable = true
		return result, nil
	case CarrierRecoveryReplaceLost:
		if err := c.ensureClaim(ctx, claim); err != nil {
			return recoveryResult(snapshot, disposition, RecoveryActionFailClosed), err
		}
		plan, _, err := c.Mutations.FenceAndAllocateReplacement(ctx, *claim, snapshot.Fingerprint(), decision)
		if err != nil {
			return recoveryResult(snapshot, disposition, RecoveryActionFailClosed), err
		}
		// A fully composed production coordinator consumes the durable
		// reservation immediately through C4-5A.  A deliberately partial
		// composition may stop here; the immutable reservation makes that safe
		// and lets a later reconcile continue without allocating another epoch.
		if c.Reprovision != nil {
			reserved, reloadErr := c.currentSnapshot(ctx, claim, RecoverySnapshotFingerprint{})
			if reloadErr != nil {
				return recoveryResult(snapshot, disposition, RecoveryActionFailClosed), reloadErr
			}
			return c.consumeReplacement(ctx, claim, reserved, RecoveryReplacementProvisioningPending)
		}
		return RecoveryReconciliationResult{Key: claim.Key, Disposition: disposition, Action: RecoveryActionReplacementReserved, ExecutionEpoch: plan.NewExecutionEpoch, PhysicalState: PhysicalExecutionReserved}, nil
	default:
		return recoveryResult(snapshot, disposition, RecoveryActionFailClosed), ErrRecoveryDispositionUnsupported
	}
}

func (c *RecoveryCoordinator) consumeReplacement(ctx context.Context, claim *RecoveryClaim, snapshot RecoverySnapshot, disposition RecoveryDisposition) (RecoveryReconciliationResult, error) {
	if c.Reprovision == nil || snapshot.CurrentPhysical == nil || snapshot.CurrentPhysical.State != PhysicalExecutionReserved {
		return recoveryResult(snapshot, disposition, RecoveryActionFailClosed), ErrRecoveryDispositionUnsupported
	}
	// C4-5A reacquires a claim at the reserved epoch and fences every external
	// effect.  Release the old-epoch claim first: the durable reservation is the
	// immutable hand-off and prevents a second allocation during this boundary.
	if err := c.Repository.Recovery().ReleaseRecoveryClaim(ctx, *claim); err != nil {
		return recoveryResult(snapshot, disposition, RecoveryActionFailClosed), err
	}
	*claim = RecoveryClaim{}
	physical, err := c.Reprovision.Reprovision(ctx, snapshot.Key)
	if err != nil {
		return recoveryResult(snapshot, disposition, RecoveryActionRetryRequired), err
	}
	return RecoveryReconciliationResult{Key: snapshot.Key, Disposition: disposition, Action: RecoveryActionReplacementActive, ExecutionEpoch: physical.ExecutionEpoch, PhysicalState: physical.State}, nil
}

func (c *RecoveryCoordinator) reconcileClaimed(ctx context.Context, claim *RecoveryClaim) error {
	_, err := c.reconcileInvocationClaimed(ctx, claim)
	return err
}

// currentSnapshot renews only at a step boundary, asserts claim ownership,
// and detects a stale cached snapshot before an authoritative mutation.
func (c *RecoveryCoordinator) currentSnapshot(ctx context.Context, claim *RecoveryClaim, expected RecoverySnapshotFingerprint) (RecoverySnapshot, error) {
	if err := c.ensureClaim(ctx, claim); err != nil {
		return RecoverySnapshot{}, err
	}
	snapshot, err := c.Repository.Recovery().LoadRecoverySnapshot(ctx, claim.Key)
	if err != nil {
		return RecoverySnapshot{}, err
	}
	if expected != (RecoverySnapshotFingerprint{}) && snapshot.Fingerprint() != expected {
		return RecoverySnapshot{}, ErrRecoverySnapshotStale
	}
	return snapshot, nil
}

func (c *RecoveryCoordinator) ensureClaim(ctx context.Context, claim *RecoveryClaim) error {
	// Renew at a deterministic boundary before consuming half the lease. A
	// slow external cleanup may still outlive its lease; the post-effect assert
	// below is the authoritative fence in that case.
	if !claim.LeaseExpiresAt.After(c.clockNow().Add(c.ClaimTTL / 2)) {
		renewed, err := c.Repository.Recovery().RenewRecoveryClaim(ctx, *claim, c.ClaimTTL)
		if err != nil {
			return err
		}
		*claim = renewed
	}
	return c.Repository.Recovery().AssertRecoveryClaim(ctx, *claim)
}

func (c *RecoveryCoordinator) reconcileTerminalStep(ctx context.Context, claim *RecoveryClaim, snapshot RecoverySnapshot) error {
	execution := *snapshot.CurrentPhysical
	if execution.Key().TaskID != claim.Key.TaskID || execution.Key().InvocationID != claim.Key.InvocationID || execution.Generation != claim.Key.Generation || execution.ExecutionEpoch != claim.ObservedExecutionEpoch {
		return ErrRecoverySnapshotInconsistent
	}
	if execution.State == PhysicalExecutionRunning {
		return c.advance(ctx, claim, snapshot, TeardownStepBegin)
	}
	if execution.State != PhysicalExecutionTearingDown {
		return ErrRecoverySnapshotInconsistent
	}
	for _, step := range []TeardownStep{TeardownStepTask, TeardownStepWorker, TeardownStepMCP, TeardownStepCredential, TeardownStepToken, TeardownStepLease} {
		if teardownStepDone(execution.Teardown, step) {
			continue
		}
		// Assert immediately before effect, then again before the durable
		// completion. A stale actor may repeat an idempotent effect but can
		// never commit its progress after a fence takeover.
		if _, err := c.currentSnapshot(ctx, claim, snapshot.Fingerprint()); err != nil {
			return err
		}
		if err := c.Cleanup.run(ctx, step, execution); err != nil {
			return err
		}
		return c.advance(ctx, claim, snapshot, step)
	}
	return c.advance(ctx, claim, snapshot, TeardownStepTerminate)
}

func (c *RecoveryCoordinator) advance(ctx context.Context, claim *RecoveryClaim, snapshot RecoverySnapshot, step TeardownStep) error {
	current, err := c.currentSnapshot(ctx, claim, snapshot.Fingerprint())
	if err != nil {
		return err
	}
	if current.CurrentPhysical == nil {
		return ErrRecoverySnapshotInconsistent
	}
	_, _, err = c.Mutations.AdvanceTeardown(ctx, current.CurrentPhysical.Key(), current.CurrentPhysical.Revision, step)
	return err
}
