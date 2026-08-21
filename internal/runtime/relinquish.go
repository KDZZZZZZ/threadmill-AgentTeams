package runtime

import (
	"context"
	"errors"
	"fmt"

	phasemcp "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/mcp/phase"
)

// AwaitEventRecorder records the Runtime await boundary without introducing a
// new Event Store implementation in M4-B1. Implementations must not persist
// transient credentials supplied in RelinquishRequest.
type AwaitEventRecorder interface {
	RecordAwaiting(context.Context, WaitingRecord) error
}

// Physical teardown ports are deliberately narrow and idempotency-friendly.
// They do not cancel the logical Invocation.
type WorkspaceLeaseReleaser interface {
	ReleaseWorkspaceLease(context.Context, WaitingRecord) error
}
type TeamHarnessTaskRelinquisher interface {
	CancelTeamHarnessTask(context.Context, string, string) error
}
type ExecutionTokenRevoker interface {
	RevokeExecutionToken(context.Context, string) error
}
type MCPExecutionCleaner interface {
	CleanupExecutionMCP(context.Context, WaitingRecord) error
}
type WorkerCredentialRevoker interface {
	RevokeWorkerCredential(context.Context, WaitingRecord) error
}
type ExecutionCarrierReleaser interface {
	ReleaseExecutionCarrier(context.Context, WaitingRecord) error
}

// RelinquishRequest combines durable non-sensitive state with transient
// carrier identifiers. ExecutionToken is used only during teardown and is
// intentionally never copied into WaitingRecord.
type RelinquishRequest struct {
	Record         WaitingRecord
	TeamHarnessID  string
	ExecutionToken string
	Reason         string
}

// ExecutionRelinquisher persists a logical wait before reclaiming the
// physical execution. A later rehydration obtains a new epoch and token; this
// type never invokes PhaseCommand.resume or creates a new InvocationID.
type ExecutionRelinquisher struct {
	Store              WaitingStore
	PhysicalExecutions PhysicalExecutionStore
	Mutations          LifecycleMutationStore
	Bindings           *phasemcp.BindingRegistry
	Events             AwaitEventRecorder
	Leases             WorkspaceLeaseReleaser
	Tasks              TeamHarnessTaskRelinquisher
	Tokens             ExecutionTokenRevoker
	MCP                MCPExecutionCleaner
	Credentials        WorkerCredentialRevoker
	Carriers           ExecutionCarrierReleaser
}

func (r ExecutionRelinquisher) Relinquish(ctx context.Context, request RelinquishRequest) (WaitingRecord, error) {
	if r.Store == nil {
		return WaitingRecord{}, errors.New("waiting store is required")
	}
	if request.Record.State != AwaitStatePreparingAwait {
		return WaitingRecord{}, errors.New("relinquish requires preparing_await record")
	}
	physical, physicalFound, err := r.currentPhysical(ctx, request.Record)
	if err != nil {
		return WaitingRecord{}, err
	}
	if r.PhysicalExecutions != nil && !physicalFound {
		return WaitingRecord{}, errors.New("current physical execution was not found")
	}
	if physicalFound && request.TeamHarnessID != "" && request.TeamHarnessID != physical.TeamHarnessTaskID {
		return WaitingRecord{}, errors.New("TeamHarness task does not match current physical execution")
	}
	created, err := r.Store.Create(ctx, request.Record)
	if err != nil && !errors.Is(err, ErrWaitingRecordExists) {
		return WaitingRecord{}, err
	}
	if errors.Is(err, ErrWaitingRecordExists) {
		current, found, getErr := r.Store.Get(ctx, request.Record.Key)
		if getErr != nil {
			return WaitingRecord{}, getErr
		}
		if !found {
			return WaitingRecord{}, errors.New("waiting record disappeared during relinquish")
		}
		if current.State == AwaitStateWaiting {
			return current, nil
		}
		return WaitingRecord{}, fmt.Errorf("relinquish already in progress for state %s", current.State)
	}

	releasing := created
	releasing.State = AwaitStateRelinquishing
	var swapped bool
	releasing, swapped, err = r.Store.CompareAndSwap(ctx, created.Key, created.Revision, releasing)
	if err != nil {
		return WaitingRecord{}, err
	}
	if !swapped {
		return WaitingRecord{}, errors.New("waiting record changed before relinquish")
	}
	if physicalFound {
		if r.Mutations != nil {
			physical, swapped, err = r.Mutations.AdvanceTeardown(ctx, physical.Key(), physical.Revision, TeardownStepBegin)
			if err != nil || !swapped {
				return WaitingRecord{}, errors.New("physical execution changed before await teardown")
			}
		} else {
			next := physical
			next.State = PhysicalExecutionTearingDown
			physical, swapped, err = r.PhysicalExecutions.CompareAndSwap(ctx, physical.Key(), physical.Revision, next)
			if err != nil || !swapped {
				return WaitingRecord{}, errors.New("physical execution changed before await teardown")
			}
		}
	}
	// Waiting state is durable before recording/releasing a carrier.
	if r.Events != nil {
		if err := r.Events.RecordAwaiting(ctx, releasing); err != nil {
			return WaitingRecord{}, err
		}
	}
	if r.Leases != nil {
		if err := r.Leases.ReleaseWorkspaceLease(ctx, releasing); err != nil {
			return WaitingRecord{}, err
		}
		physical, err = r.recordPhysicalTeardown(ctx, physical, physicalFound, TeardownStepLease, func(t *PhysicalExecutionTeardown) { t.LeaseReleased = true })
		if err != nil {
			return WaitingRecord{}, err
		}
	}
	if r.Tasks != nil && request.TeamHarnessID != "" {
		if err := r.Tasks.CancelTeamHarnessTask(ctx, request.TeamHarnessID, request.Reason); err != nil {
			return WaitingRecord{}, err
		}
		physical, err = r.recordPhysicalTeardown(ctx, physical, physicalFound, TeardownStepTask, func(t *PhysicalExecutionTeardown) { t.TeamHarnessTaskCancelled = true })
		if err != nil {
			return WaitingRecord{}, err
		}
	}
	// Revocation is deliberately before the transaction reports waiting.
	if r.Tokens != nil && request.ExecutionToken != "" {
		if err := r.Tokens.RevokeExecutionToken(ctx, request.ExecutionToken); err != nil {
			return WaitingRecord{}, err
		}
		physical, err = r.recordPhysicalTeardown(ctx, physical, physicalFound, TeardownStepToken, func(t *PhysicalExecutionTeardown) { t.TokenRevoked = true })
		if err != nil {
			return WaitingRecord{}, err
		}
	} else if r.Bindings != nil {
		r.Bindings.RevokeBinding(phasemcp.InvocationBinding{TaskID: request.Record.Key.TaskID, InvocationID: request.Record.Key.InvocationID, Generation: request.Record.Key.Generation, ExecutionEpoch: int64(request.Record.ExecutionEpoch), BindingRef: request.Record.PreviousBindingRef, InputRevision: request.Record.InputRevision})
		physical, err = r.recordPhysicalTeardown(ctx, physical, physicalFound, TeardownStepToken, func(t *PhysicalExecutionTeardown) { t.TokenRevoked = true })
		if err != nil {
			return WaitingRecord{}, err
		}
	}
	if r.MCP != nil {
		if err := r.MCP.CleanupExecutionMCP(ctx, releasing); err != nil {
			return WaitingRecord{}, err
		}
		physical, err = r.recordPhysicalTeardown(ctx, physical, physicalFound, TeardownStepMCP, func(t *PhysicalExecutionTeardown) { t.MCPCleaned = true })
		if err != nil {
			return WaitingRecord{}, err
		}
	}
	if r.Credentials != nil {
		if err := r.Credentials.RevokeWorkerCredential(ctx, releasing); err != nil {
			return WaitingRecord{}, err
		}
		physical, err = r.recordPhysicalTeardown(ctx, physical, physicalFound, TeardownStepCredential, func(t *PhysicalExecutionTeardown) { t.CredentialRevoked = true })
		if err != nil {
			return WaitingRecord{}, err
		}
	}
	if r.Carriers != nil {
		if err := r.Carriers.ReleaseExecutionCarrier(ctx, releasing); err != nil {
			return WaitingRecord{}, err
		}
		physical, err = r.recordPhysicalTeardown(ctx, physical, physicalFound, TeardownStepWorker, func(t *PhysicalExecutionTeardown) { t.WorkerDeleted = true })
		if err != nil {
			return WaitingRecord{}, err
		}
	}
	if physicalFound {
		if r.Mutations != nil {
			if _, swapped, err := r.Mutations.AdvanceTeardown(ctx, physical.Key(), physical.Revision, TeardownStepTerminate); err != nil || !swapped {
				return WaitingRecord{}, errors.New("physical execution changed before await termination")
			}
		} else {
			terminated := physical
			terminated.State = PhysicalExecutionTerminated
			if _, swapped, err := r.PhysicalExecutions.CompareAndSwap(ctx, physical.Key(), physical.Revision, terminated); err != nil || !swapped {
				return WaitingRecord{}, errors.New("physical execution changed before await termination")
			}
		}
	}
	waiting := releasing
	waiting.State = AwaitStateWaiting
	waiting, swapped, err = r.Store.CompareAndSwap(ctx, releasing.Key, releasing.Revision, waiting)
	if err != nil {
		return WaitingRecord{}, err
	}
	if !swapped {
		return WaitingRecord{}, errors.New("waiting record changed before wait completion")
	}
	return waiting, nil
}

func (r ExecutionRelinquisher) currentPhysical(ctx context.Context, record WaitingRecord) (PhysicalExecution, bool, error) {
	if r.PhysicalExecutions == nil {
		return PhysicalExecution{}, false, nil
	}
	execution, found, err := r.PhysicalExecutions.Get(ctx, PhysicalExecutionKey{TaskID: record.Key.TaskID, InvocationID: record.Key.InvocationID, Generation: record.Key.Generation, ExecutionEpoch: record.ExecutionEpoch})
	if err != nil || !found {
		return execution, found, err
	}
	if execution.State != PhysicalExecutionRunning || execution.BindingRef != record.PreviousBindingRef || execution.InputRevision != record.InputRevision {
		return PhysicalExecution{}, false, errors.New("physical execution does not match await binding")
	}
	return execution, true, nil
}

func (r ExecutionRelinquisher) recordPhysicalTeardown(ctx context.Context, execution PhysicalExecution, found bool, step TeardownStep, update func(*PhysicalExecutionTeardown)) (PhysicalExecution, error) {
	if !found {
		return execution, nil
	}
	if r.Mutations != nil {
		next, _, err := r.Mutations.AdvanceTeardown(ctx, execution.Key(), execution.Revision, step)
		return next, err
	}
	update(&execution.Teardown)
	next, swapped, err := r.PhysicalExecutions.CompareAndSwap(ctx, execution.Key(), execution.Revision, execution)
	if err != nil || !swapped {
		return PhysicalExecution{}, errors.New("physical execution teardown evidence changed concurrently")
	}
	return next, nil
}
