package runtime

import (
	"context"
	"errors"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/artifacts"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/executionreceipt"
)

var (
	ErrRecoveryClaimed                = errors.New("logical invocation recovery is claimed by another owner")
	ErrRecoveryClaimLost              = errors.New("logical invocation recovery claim is stale or not held")
	ErrRecoverySnapshotInconsistent   = errors.New("durable recovery snapshot is inconsistent")
	ErrRecoverySnapshotStale          = errors.New("durable recovery snapshot changed before mutation")
	ErrRecoveryDispositionUnsupported = errors.New("recovery disposition is not supported by this coordinator")
)

// RecoveryClaim is a Runtime-internal lease for one logical invocation. It is
// deliberately not a Worker, workspace, or execution-token lease. Epoch is
// recorded as an observation/fence input, never as the claim's primary key.
type RecoveryClaim struct {
	Key                    WaitingKey
	ObservedExecutionEpoch ExecutionEpoch
	OwnerID                string
	LeaseExpiresAt         time.Time
	Fence                  int64
	Revision               int64
	ClaimedAt              time.Time
	RenewedAt              time.Time
}

// RecoverySnapshot is a read-only, secret-free view of one logical
// invocation. It is read from one SQLite transaction so a classifier never
// assembles unrelated revisions from separate store calls.
type RecoverySnapshot struct {
	Key                   WaitingKey
	CurrentExecutionEpoch ExecutionEpoch
	BindingRef            string
	InputRevision         string
	Waiting               *WaitingRecord
	LatestInputs          *StoredPhaseInputSet
	Binding               *ContinuationBinding
	Continuation          *ContinuationMaterial
	CurrentPhysical       *PhysicalExecution
	PhysicalHistory       []PhysicalExecution
	Receipt               *executionreceipt.Receipt
	Output                *PhaseOutputRecord
	ArtifactRefs          []artifacts.ArtifactRef
	WaitingRevision       int64
	PhysicalRevision      int64
	OutputRevision        int64
}

// RecoverySnapshotFingerprint is carried by a future coordinator beside its
// RecoveryClaim. It is not a lock: before every mutation the coordinator must
// reload the snapshot, compare relevant aggregate revisions, and assert its
// claim fence again.
type RecoverySnapshotFingerprint struct {
	ExecutionEpoch   ExecutionEpoch
	WaitingRevision  int64
	PhysicalRevision int64
	OutputRevision   int64
}

func (s RecoverySnapshot) Fingerprint() RecoverySnapshotFingerprint {
	return RecoverySnapshotFingerprint{ExecutionEpoch: s.CurrentExecutionEpoch, WaitingRevision: s.WaitingRevision, PhysicalRevision: s.PhysicalRevision, OutputRevision: s.OutputRevision}
}

// RecoveryStateStore owns recovery lease fencing and consistent snapshot
// reads. Future recovery actions must assert their claim again immediately
// before an authoritative mutation.
type RecoveryStateStore interface {
	AcquireRecoveryClaim(context.Context, WaitingKey, ExecutionEpoch, string, time.Duration) (RecoveryClaim, error)
	RenewRecoveryClaim(context.Context, RecoveryClaim, time.Duration) (RecoveryClaim, error)
	ReleaseRecoveryClaim(context.Context, RecoveryClaim) error
	GetRecoveryClaim(context.Context, WaitingKey) (RecoveryClaim, bool, error)
	AssertRecoveryClaim(context.Context, RecoveryClaim) error
	LoadRecoverySnapshot(context.Context, WaitingKey) (RecoverySnapshot, error)
}

type RecoveryDisposition string

const (
	RecoveryNoDurableInvocation                  RecoveryDisposition = "no_durable_invocation"
	RecoveryAwaitingInput                        RecoveryDisposition = "awaiting_input"
	RecoveryRelinquishmentIncomplete             RecoveryDisposition = "relinquishment_incomplete"
	RecoveryRehydrationIncomplete                RecoveryDisposition = "rehydration_incomplete"
	RecoveryCarrierProvisioningIncomplete        RecoveryDisposition = "carrier_provisioning_incomplete"
	RecoveryCarrierActiveNeedsObservation        RecoveryDisposition = "carrier_active_needs_observation"
	RecoveryCarrierConsumedNoOutput              RecoveryDisposition = "carrier_consumed_no_output"
	RecoveryContinueTerminalTeardown             RecoveryDisposition = "continue_terminal_teardown"
	RecoveryTerminalNoOp                         RecoveryDisposition = "terminal_noop"
	RecoveryFailedPhysicalExecutionNeedsDecision RecoveryDisposition = "failed_physical_execution_needs_recovery_decision"
)

// ClassifyRecoverySnapshot performs no I/O and no repair. In particular, a
// running carrier is not presumed lost until a later adapter observes it.
func ClassifyRecoverySnapshot(snapshot RecoverySnapshot) (RecoveryDisposition, error) {
	if snapshot.Key.TaskID == "" || snapshot.Key.InvocationID == "" || snapshot.Key.Generation <= 0 {
		return "", ErrRecoverySnapshotInconsistent
	}
	if snapshot.Waiting == nil {
		if snapshot.Output != nil {
			return "", ErrRecoverySnapshotInconsistent
		}
		if snapshot.CurrentPhysical == nil {
			return RecoveryNoDurableInvocation, nil
		}
		if snapshot.CurrentPhysical.State == PhysicalExecutionFailed {
			return RecoveryFailedPhysicalExecutionNeedsDecision, nil
		}
		if snapshot.CurrentPhysical.State == PhysicalExecutionTerminated {
			return RecoveryTerminalNoOp, nil
		}
		return "", ErrRecoverySnapshotInconsistent
	}
	if snapshot.Waiting.Key != snapshot.Key || snapshot.Waiting.ExecutionEpoch <= 0 {
		return "", ErrRecoverySnapshotInconsistent
	}
	if snapshot.LatestInputs == nil || snapshot.Continuation == nil || snapshot.LatestInputs.Inputs.InputRevision == "" {
		return "", ErrRecoverySnapshotInconsistent
	}
	if snapshot.Receipt != nil && (snapshot.CurrentPhysical == nil || snapshot.Receipt.TaskID != snapshot.Key.TaskID || snapshot.Receipt.InvocationID != snapshot.Key.InvocationID || snapshot.Receipt.Generation != snapshot.Key.Generation || snapshot.Receipt.ExecutionEpoch != int64(snapshot.CurrentPhysical.ExecutionEpoch)) {
		return "", ErrRecoverySnapshotInconsistent
	}
	if snapshot.Output != nil {
		if snapshot.Output.Key.TaskID != snapshot.Key.TaskID || snapshot.Output.Key.InvocationID != snapshot.Key.InvocationID || snapshot.Output.Key.Generation != snapshot.Key.Generation || snapshot.Waiting.State != AwaitStateTerminal {
			return "", ErrRecoverySnapshotInconsistent
		}
		if snapshot.CurrentPhysical == nil || snapshot.CurrentPhysical.State == PhysicalExecutionTerminated {
			return RecoveryTerminalNoOp, nil
		}
		return RecoveryContinueTerminalTeardown, nil
	}
	if snapshot.Waiting.State == AwaitStateTerminal {
		if snapshot.CurrentPhysical == nil || snapshot.CurrentPhysical.State == PhysicalExecutionTerminated {
			return RecoveryTerminalNoOp, nil
		}
		return RecoveryContinueTerminalTeardown, nil
	}
	if snapshot.CurrentPhysical != nil && snapshot.CurrentPhysical.State == PhysicalExecutionFailed {
		return RecoveryFailedPhysicalExecutionNeedsDecision, nil
	}
	switch snapshot.Waiting.State {
	case AwaitStatePreparingAwait, AwaitStateRelinquishing:
		return RecoveryRelinquishmentIncomplete, nil
	case AwaitStateWaiting:
		return RecoveryAwaitingInput, nil
	case AwaitStateRehydrating:
		if snapshot.CurrentPhysical == nil {
			return RecoveryRehydrationIncomplete, nil
		}
		if snapshot.CurrentPhysical.State == PhysicalExecutionProvisioning || snapshot.CurrentPhysical.State == PhysicalExecutionDelegated || snapshot.CurrentPhysical.State == PhysicalExecutionAccepted {
			return RecoveryCarrierProvisioningIncomplete, nil
		}
		return "", ErrRecoverySnapshotInconsistent
	case AwaitStateRunning:
		if snapshot.CurrentPhysical == nil || snapshot.CurrentPhysical.ExecutionEpoch != snapshot.Waiting.ExecutionEpoch {
			return "", ErrRecoverySnapshotInconsistent
		}
		switch snapshot.CurrentPhysical.State {
		case PhysicalExecutionRunning:
			if snapshot.Receipt != nil && snapshot.Receipt.Consumed {
				return RecoveryCarrierConsumedNoOutput, nil
			}
			return RecoveryCarrierActiveNeedsObservation, nil
		case PhysicalExecutionProvisioning, PhysicalExecutionDelegated, PhysicalExecutionAccepted:
			return RecoveryCarrierProvisioningIncomplete, nil
		case PhysicalExecutionTearingDown:
			return RecoveryRelinquishmentIncomplete, nil
		}
	}
	return "", ErrRecoverySnapshotInconsistent
}
