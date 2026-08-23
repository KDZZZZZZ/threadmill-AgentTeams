package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/artifacts"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/phaseagent"
)

var ErrReplacementReservationInvalid = errors.New("replacement reservation is not consumable")

// ReplacementReprovisionCoordinator consumes, but never allocates, the C4-4
// successor reservation. All execution-plane effects remain delegated to the
// established PhysicalExecutionProvisioner ports.
type ReplacementReprovisionCoordinator struct {
	Repository  RuntimeStateRepository
	Provisioner *PhysicalExecutionProvisioner
	Surfaces    ExecutionSurfaces
	OwnerID     string
	ClaimTTL    time.Duration
}

func (c *ReplacementReprovisionCoordinator) Reprovision(ctx context.Context, key WaitingKey) (PhysicalExecution, error) {
	if c == nil || c.Repository == nil || c.Provisioner == nil || c.OwnerID == "" || c.ClaimTTL <= 0 || c.Surfaces.Runtime == nil || c.Surfaces.ContextReader == nil || c.Surfaces.ContextAgent == nil {
		return PhysicalExecution{}, errors.New("replacement reprovision coordinator dependencies are required")
	}
	seed, err := c.Repository.Recovery().LoadRecoverySnapshot(ctx, key)
	if err != nil {
		return PhysicalExecution{}, err
	}
	if seed.CurrentExecutionEpoch <= 0 {
		return PhysicalExecution{}, ErrRecoveryDispositionUnsupported
	}
	claim, err := c.Repository.Recovery().AcquireRecoveryClaim(ctx, key, seed.CurrentExecutionEpoch, c.OwnerID, c.ClaimTTL)
	if err != nil {
		return PhysicalExecution{}, err
	}
	defer func() { _ = c.Repository.Recovery().ReleaseRecoveryClaim(context.Background(), claim) }()
	if err = c.Repository.Recovery().AssertRecoveryClaim(ctx, claim); err != nil {
		return PhysicalExecution{}, err
	}
	snapshot, err := c.Repository.Recovery().LoadRecoverySnapshot(ctx, key)
	if err != nil {
		return PhysicalExecution{}, err
	}
	if disposition, classifyErr := ClassifyRecoverySnapshot(snapshot); classifyErr != nil || disposition != RecoveryReplacementProvisioningPending {
		if classifyErr != nil {
			return PhysicalExecution{}, classifyErr
		}
		return PhysicalExecution{}, ErrRecoveryDispositionUnsupported
	}
	if snapshot.Fingerprint() != seed.Fingerprint() {
		return PhysicalExecution{}, ErrRecoverySnapshotStale
	}
	plan, err := c.plan(snapshot, claim)
	if err != nil {
		return PhysicalExecution{}, err
	}
	// A shallow copy leaves all production ports shared while avoiding a
	// process-wide mutable claim field on the injected provisioner.
	provisioner := *c.Provisioner
	provisioner.Fence = replacementProvisioningFence{repository: c.Repository, claim: &claim, ttl: c.ClaimTTL, key: key, epoch: plan.NextExecutionEpoch, binding: plan.NewBindingRef, input: plan.NewInputRevision}
	return provisioner.Provision(ctx, plan)
}

func (c *ReplacementReprovisionCoordinator) plan(snapshot RecoverySnapshot, claim RecoveryClaim) (RehydrationPlan, error) {
	if snapshot.Waiting == nil || snapshot.CurrentPhysical == nil || snapshot.Continuation == nil || snapshot.LatestInputs == nil {
		return RehydrationPlan{}, ErrReplacementReservationInvalid
	}
	waiting, reserved := *snapshot.Waiting, *snapshot.CurrentPhysical
	if waiting.Key != claim.Key || waiting.State != AwaitStateRehydrating || waiting.ExecutionEpoch != claim.ObservedExecutionEpoch || reserved.Key() != (PhysicalExecutionKey{TaskID: claim.Key.TaskID, InvocationID: claim.Key.InvocationID, Generation: claim.Key.Generation, ExecutionEpoch: claim.ObservedExecutionEpoch}) || reserved.State != PhysicalExecutionReserved || reserved.BindingRef != waiting.PreviousBindingRef || reserved.InputRevision != waiting.InputRevision || !reserved.RequiresFreshPackageReceipt || reserved.ReplacesExecutionEpoch <= 0 {
		return RehydrationPlan{}, ErrReplacementReservationInvalid
	}
	if snapshot.LatestInputs.Inputs.InputRevision != waiting.InputRevision {
		return RehydrationPlan{}, ErrReplacementReservationInvalid
	}
	authority := DurableReconstructionAuthority{Store: c.Repository.Reconstruction()}
	workspace, err := authority.ReconstructWorkspace(context.Background(), waiting, *snapshot.Continuation)
	if err != nil {
		return RehydrationPlan{}, err
	}
	contextValue, err := authority.ReconstructContext(context.Background(), waiting, *snapshot.Continuation)
	if err != nil {
		return RehydrationPlan{}, err
	}
	memory, err := authority.ReconstructTaskMemory(context.Background(), waiting, *snapshot.Continuation)
	if err != nil {
		return RehydrationPlan{}, err
	}
	role, err := phaseagent.RoleForEndpoint(waiting.Endpoint)
	if err != nil {
		return RehydrationPlan{}, err
	}
	start := phaseagent.StartPhaseInput{InvocationID: waiting.Key.InvocationID, Endpoint: waiting.Endpoint, Generation: waiting.Key.Generation, BindingRef: waiting.PreviousBindingRef, Inputs: snapshot.LatestInputs.Inputs}
	invocation, err := phaseagent.NewInvocationContext(start)
	if err != nil {
		return RehydrationPlan{}, err
	}
	return RehydrationPlan{TaskID: waiting.Key.TaskID, InvocationID: waiting.Key.InvocationID, Generation: waiting.Key.Generation, NextExecutionEpoch: reserved.ExecutionEpoch, Endpoint: waiting.Endpoint, NewBindingRef: waiting.PreviousBindingRef, NewInputRevision: waiting.InputRevision, Inputs: snapshot.LatestInputs.Inputs, Execution: phaseagent.ExecutionContext{Invocation: invocation, Role: role, Runtime: c.Surfaces.Runtime, ContextReader: c.Surfaces.ContextReader, ContextAgent: c.Surfaces.ContextAgent}, Workspace: workspace, Context: contextValue, TaskMemory: memory, ArtifactRefs: append([]artifacts.ArtifactRef(nil), waiting.ArtifactRefs...), EventRefs: append([]string(nil), waiting.EventRefs...), EvidenceRefs: append([]string(nil), waiting.EvidenceRefs...), ContinuationRef: waiting.ContinuationRef, ExpectedWaitingRevision: waiting.Revision, ReservedReplacement: true}, nil
}

type replacementProvisioningFence struct {
	repository     RuntimeStateRepository
	claim          *RecoveryClaim
	ttl            time.Duration
	key            WaitingKey
	epoch          ExecutionEpoch
	binding, input string
}

func (f replacementProvisioningFence) AssertProvisioning(ctx context.Context) error {
	if f.repository == nil || f.claim == nil {
		return ErrRecoveryClaimLost
	}
	if !f.claim.LeaseExpiresAt.After(time.Now().Add(f.ttl / 2)) {
		renewed, err := f.repository.Recovery().RenewRecoveryClaim(ctx, *f.claim, f.ttl)
		if err != nil {
			return err
		}
		*f.claim = renewed
	}
	if err := f.repository.Recovery().AssertRecoveryClaim(ctx, *f.claim); err != nil {
		return err
	}
	snapshot, err := f.repository.Recovery().LoadRecoverySnapshot(ctx, f.key)
	if err != nil {
		return err
	}
	if snapshot.Waiting == nil || snapshot.CurrentPhysical == nil || snapshot.Waiting.State != AwaitStateRehydrating || snapshot.Waiting.ExecutionEpoch != f.epoch || snapshot.BindingRef != f.binding || snapshot.InputRevision != f.input || snapshot.CurrentPhysical.ExecutionEpoch != f.epoch {
		return ErrRecoverySnapshotStale
	}
	switch snapshot.CurrentPhysical.State {
	case PhysicalExecutionReserved, PhysicalExecutionProvisioning, PhysicalExecutionAccepted:
		return nil
	default:
		return fmt.Errorf("%w: physical state %s", ErrReplacementReservationInvalid, snapshot.CurrentPhysical.State)
	}
}
