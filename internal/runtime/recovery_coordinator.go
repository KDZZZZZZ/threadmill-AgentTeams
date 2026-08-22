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
	OwnerID    string
	ClaimTTL   time.Duration

	// now is intentionally private and replaceable by package tests. Recovery
	// lease tests use a fake clock; production defaults to wall clock.
	now func() time.Time
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

// Reconcile claims one logical invocation and handles only terminal_noop or
// continue_terminal_teardown. All other classifier outcomes are intentionally
// left for later C4 slices and execute no external side effect.
func (c *RecoveryCoordinator) Reconcile(ctx context.Context, key WaitingKey) error {
	if err := c.validate(); err != nil {
		return err
	}
	seed, err := c.Repository.Recovery().LoadRecoverySnapshot(ctx, key)
	if err != nil {
		return err
	}
	if seed.CurrentExecutionEpoch <= 0 {
		return ErrRecoveryDispositionUnsupported
	}
	claim, err := c.Repository.Recovery().AcquireRecoveryClaim(ctx, key, seed.CurrentExecutionEpoch, c.OwnerID, c.ClaimTTL)
	if err != nil {
		return err
	}
	defer func() { _ = c.Repository.Recovery().ReleaseRecoveryClaim(context.Background(), claim) }()
	return c.reconcileClaimed(ctx, &claim)
}

func (c *RecoveryCoordinator) reconcileClaimed(ctx context.Context, claim *RecoveryClaim) error {
	for {
		snapshot, err := c.currentSnapshot(ctx, claim, RecoverySnapshotFingerprint{})
		if err != nil {
			return err
		}
		disposition, err := ClassifyRecoverySnapshot(snapshot)
		if err != nil {
			return err
		}
		switch disposition {
		case RecoveryTerminalNoOp:
			return nil
		case RecoveryContinueTerminalTeardown:
			if snapshot.CurrentPhysical == nil {
				return ErrRecoverySnapshotInconsistent
			}
			if err = c.reconcileTerminalStep(ctx, claim, snapshot); err != nil {
				return err
			}
			// Each successful authoritative mutation changes the snapshot
			// fingerprint. Reload and classify before the next step.
		default:
			return ErrRecoveryDispositionUnsupported
		}
	}
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
