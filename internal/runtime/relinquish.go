package runtime

import (
	"context"
	"errors"
	"fmt"
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
	Store       WaitingStore
	Events      AwaitEventRecorder
	Leases      WorkspaceLeaseReleaser
	Tasks       TeamHarnessTaskRelinquisher
	Tokens      ExecutionTokenRevoker
	MCP         MCPExecutionCleaner
	Credentials WorkerCredentialRevoker
	Carriers    ExecutionCarrierReleaser
}

func (r ExecutionRelinquisher) Relinquish(ctx context.Context, request RelinquishRequest) (WaitingRecord, error) {
	if r.Store == nil {
		return WaitingRecord{}, errors.New("waiting store is required")
	}
	if request.Record.State != AwaitStatePreparingAwait {
		return WaitingRecord{}, errors.New("relinquish requires preparing_await record")
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
	}
	if r.Tasks != nil && request.TeamHarnessID != "" {
		if err := r.Tasks.CancelTeamHarnessTask(ctx, request.TeamHarnessID, request.Reason); err != nil {
			return WaitingRecord{}, err
		}
	}
	// Revocation is deliberately before the transaction reports waiting.
	if r.Tokens != nil && request.ExecutionToken != "" {
		if err := r.Tokens.RevokeExecutionToken(ctx, request.ExecutionToken); err != nil {
			return WaitingRecord{}, err
		}
	}
	if r.MCP != nil {
		if err := r.MCP.CleanupExecutionMCP(ctx, releasing); err != nil {
			return WaitingRecord{}, err
		}
	}
	if r.Credentials != nil {
		if err := r.Credentials.RevokeWorkerCredential(ctx, releasing); err != nil {
			return WaitingRecord{}, err
		}
	}
	if r.Carriers != nil {
		if err := r.Carriers.ReleaseExecutionCarrier(ctx, releasing); err != nil {
			return WaitingRecord{}, err
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
