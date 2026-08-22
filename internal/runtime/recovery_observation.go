package runtime

import (
	"context"
	"errors"
	"time"
)

// PhysicalExecutionObserver reads execution-plane evidence for a durable
// carrier. It must not mutate Controller, TeamHarness, Runtime state, or any
// capability. Observation is deliberately separate from recovery decisions.
type PhysicalExecutionObserver interface {
	Observe(context.Context, PhysicalExecutionObservationRequest) (PhysicalExecutionObservation, error)
}

// PhysicalExecutionObservationRequest is reconstructed exclusively from the
// redacted PhysicalExecution identity and deployment-local client config.
// It intentionally has no execution token, credential value, private header,
// controller authorization, or provider/session content.
type PhysicalExecutionObservationRequest struct {
	TaskID, InvocationID string
	Generation           int
	ExecutionEpoch       ExecutionEpoch
	WorkerID, WorkerName string
	TeamHarnessTaskID    string
	DesiredGeneration    int64
	AppliedGeneration    int64
	MCPClientID          string
	AgentSessionRef      string
}

func observationRequest(value PhysicalExecution) PhysicalExecutionObservationRequest {
	return PhysicalExecutionObservationRequest{
		TaskID: value.TaskID, InvocationID: value.InvocationID, Generation: value.Generation, ExecutionEpoch: value.ExecutionEpoch,
		WorkerID: value.WorkerID, WorkerName: value.WorkerName, TeamHarnessTaskID: value.TeamHarnessTaskID,
		DesiredGeneration: value.DesiredRuntimeGeneration, AppliedGeneration: value.AppliedRuntimeGeneration,
		MCPClientID: value.MCPClientID, AgentSessionRef: value.AgentSessionRef,
	}
}

func (r PhysicalExecutionObservationRequest) validate() error {
	if r.TaskID == "" || r.InvocationID == "" || r.Generation <= 0 || r.ExecutionEpoch <= 0 || r.WorkerName == "" || r.TeamHarnessTaskID == "" || r.DesiredGeneration <= 0 || r.MCPClientID == "" {
		return errors.New("physical execution observation identity is required")
	}
	return nil
}

// ValidateForObservation permits adapter packages to validate an opaque
// request without exposing any recovery implementation detail publicly.
func (r PhysicalExecutionObservationRequest) ValidateForObservation() error { return r.validate() }

type ObservedWorkerState string

const (
	ObservedWorkerUnknown      ObservedWorkerState = "unknown"
	ObservedWorkerNotFound     ObservedWorkerState = "not_found"
	ObservedWorkerProvisioning ObservedWorkerState = "provisioning"
	ObservedWorkerReady        ObservedWorkerState = "ready"
	ObservedWorkerTerminating  ObservedWorkerState = "terminating"
	ObservedWorkerFailed       ObservedWorkerState = "failed"
)

type ObservedTaskState string

const (
	ObservedTaskUnknown    ObservedTaskState = "unknown"
	ObservedTaskNotFound   ObservedTaskState = "not_found"
	ObservedTaskAssigned   ObservedTaskState = "assigned"
	ObservedTaskInProgress ObservedTaskState = "in_progress"
	ObservedTaskCompleted  ObservedTaskState = "completed"
	ObservedTaskFailed     ObservedTaskState = "failed"
	ObservedTaskCancelled  ObservedTaskState = "cancelled"
)

type ObservedRuntimeState string

const (
	ObservedRuntimeUnknown            ObservedRuntimeState = "unknown"
	ObservedRuntimeGenerationPending  ObservedRuntimeState = "generation_pending"
	ObservedRuntimeApplied            ObservedRuntimeState = "applied"
	ObservedRuntimeGenerationMismatch ObservedRuntimeState = "generation_mismatch"
)

type ObservedMCPState string

const (
	ObservedMCPUnknown    ObservedMCPState = "unknown"
	ObservedMCPApplied    ObservedMCPState = "applied"
	ObservedMCPNotApplied ObservedMCPState = "not_applied"
)

type ObservedCarrierIdentity string

const (
	ObservedCarrierIdentityUnknown  ObservedCarrierIdentity = "unknown"
	ObservedCarrierIdentityVerified ObservedCarrierIdentity = "verified"
	ObservedCarrierIdentityMismatch ObservedCarrierIdentity = "mismatch"
)

// PhysicalExecutionObservation is a partial, non-transactional external view.
// Source errors are redacted categories, not transport credentials or raw
// external responses. It is never Runtime logical authority.
type PhysicalExecutionObservation struct {
	ObservedAt        time.Time
	Worker            ObservedWorkerState
	Task              ObservedTaskState
	Runtime           ObservedRuntimeState
	MCP               ObservedMCPState
	Identity          ObservedCarrierIdentity
	WorkerName        string
	TaskID            string
	RoomRef           string
	DesiredGeneration int64
	AppliedGeneration int64
	SourceErrors      []ObservationSourceError
}

type ObservationSourceError struct {
	Source string
	Kind   string
}

// RecoveryObservationResult is returned to a later recovery-decision slice.
// C4-3 deliberately does not turn any observation into a lifecycle mutation.
type RecoveryObservationResult struct {
	Disposition RecoveryDisposition
	Fingerprint RecoverySnapshotFingerprint
	Observation PhysicalExecutionObservation
}

// ObserveCarrier obtains a fenced, read-only execution-plane observation for
// the active carrier disposition. It does not clean up, provision, create an
// epoch, accept output, or persist an event.
func (c *RecoveryCoordinator) ObserveCarrier(ctx context.Context, key WaitingKey) (RecoveryObservationResult, error) {
	if err := c.validateObservation(); err != nil {
		return RecoveryObservationResult{}, err
	}
	seed, err := c.Repository.Recovery().LoadRecoverySnapshot(ctx, key)
	if err != nil {
		return RecoveryObservationResult{}, err
	}
	if seed.CurrentExecutionEpoch <= 0 {
		return RecoveryObservationResult{}, ErrRecoveryDispositionUnsupported
	}
	claim, err := c.Repository.Recovery().AcquireRecoveryClaim(ctx, key, seed.CurrentExecutionEpoch, c.OwnerID, c.ClaimTTL)
	if err != nil {
		return RecoveryObservationResult{}, err
	}
	defer func() { _ = c.Repository.Recovery().ReleaseRecoveryClaim(context.Background(), claim) }()
	snapshot, err := c.currentSnapshot(ctx, &claim, RecoverySnapshotFingerprint{})
	if err != nil {
		return RecoveryObservationResult{}, err
	}
	disposition, err := ClassifyRecoverySnapshot(snapshot)
	if err != nil {
		return RecoveryObservationResult{}, err
	}
	if disposition != RecoveryCarrierActiveNeedsObservation && disposition != RecoveryCarrierConsumedNoOutput {
		return RecoveryObservationResult{}, ErrRecoveryDispositionUnsupported
	}
	if snapshot.CurrentPhysical == nil || snapshot.CurrentPhysical.State != PhysicalExecutionRunning {
		return RecoveryObservationResult{}, ErrRecoverySnapshotInconsistent
	}
	request := observationRequest(*snapshot.CurrentPhysical)
	if err = request.validate(); err != nil {
		return RecoveryObservationResult{}, err
	}
	observation, err := c.Observer.Observe(ctx, request)
	if err != nil {
		return RecoveryObservationResult{}, err
	}
	// A successful external read cannot authorize a stale claim or a mutation
	// against changed durable state. Discard it rather than inferring a repair.
	if _, err = c.currentSnapshot(ctx, &claim, snapshot.Fingerprint()); err != nil {
		return RecoveryObservationResult{}, err
	}
	return RecoveryObservationResult{Disposition: disposition, Fingerprint: snapshot.Fingerprint(), Observation: observation}, nil
}

func (c *RecoveryCoordinator) validateObservation() error {
	if c == nil || c.Repository == nil || c.Observer == nil || c.OwnerID == "" || c.ClaimTTL <= 0 {
		return errors.New("recovery observation dependencies are required")
	}
	return nil
}
