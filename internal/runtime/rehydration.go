package runtime

import (
	"context"
	"errors"
	"fmt"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/artifacts"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/phaseagent"
)

// RehydrationInputSnapshot makes input ordering and await completion explicit
// because PhaseInputSet revisions are opaque strings, not comparable counters.
type RehydrationInputSnapshot struct {
	Inputs                  phaseagent.PhaseInputSet   `json:"inputs"`
	NewlyDelivered          []phaseagent.InputDelivery `json:"newly_delivered,omitempty"`
	RevisionIsNewer         bool                       `json:"revision_is_newer"`
	AwaitConditionSatisfied bool                       `json:"await_condition_satisfied"`
	TerminalReason          string                     `json:"terminal_reason,omitempty"`
}

type RehydrationInputResolver interface {
	ResolveRehydrationInputs(context.Context, WaitingRecord) (RehydrationInputSnapshot, error)
}

// RehydratedContext and WorkspaceBinding are logical bindings only. They do
// not acquire a workspace lease, mount files, start a worker, or retain a
// Context Graph subscription/session.
type RehydratedContext struct {
	SliceRef    string `json:"slice_ref"`
	BaselineRef string `json:"baseline_ref,omitempty"`
}

type ContextReconstructor interface {
	ReconstructContext(context.Context, WaitingRecord, ContinuationMaterial) (RehydratedContext, error)
}

type WorkspaceBinding struct {
	Ref            string   `json:"ref"`
	Revision       string   `json:"revision,omitempty"`
	AllowedDirs    []string `json:"allowed_dirs"`
	WriteLeaseHeld bool     `json:"write_lease_held"`
}

type WorkspaceReconstructor interface {
	ReconstructWorkspace(context.Context, WaitingRecord, ContinuationMaterial) (WorkspaceBinding, error)
}

type RehydratedTaskMemory struct {
	BufferRef string                          `json:"buffer_ref"`
	View      phaseagent.TaskMemoryBufferView `json:"view"`
}

type TaskMemoryReconstructor interface {
	ReconstructTaskMemory(context.Context, WaitingRecord, ContinuationMaterial) (RehydratedTaskMemory, error)
}

// ExecutionSurfaces are existing Phase Agent seams required to form a fresh
// transient ExecutionContext. They are not worker identity, MCP credentials,
// or QwenPaw state and are never serialized in a RehydrationPlan.
type ExecutionSurfaces struct {
	Runtime       phaseagent.Runtime
	ContextReader phaseagent.ContextGraphReader
	ContextAgent  phaseagent.ContextAgent
}

// RehydrationPlan is the Runtime-internal, non-physical result of rebuilding
// logical invocation state. M4-D may use ExpectedWaitingRevision to CAS the
// record from rehydrating to running only after provisioning succeeds.
type RehydrationPlan struct {
	TaskID                  string                      `json:"task_id"`
	InvocationID            string                      `json:"invocation_id"`
	Generation              int                         `json:"generation"`
	NextExecutionEpoch      ExecutionEpoch              `json:"next_execution_epoch"`
	Endpoint                phaseagent.PhaseEndpointRef `json:"endpoint"`
	NewBindingRef           string                      `json:"new_binding_ref"`
	NewInputRevision        string                      `json:"new_input_revision"`
	Inputs                  phaseagent.PhaseInputSet    `json:"inputs"`
	NewlyDelivered          []phaseagent.InputDelivery  `json:"newly_delivered,omitempty"`
	Execution               phaseagent.ExecutionContext `json:"-"`
	Workspace               WorkspaceBinding            `json:"workspace"`
	Context                 RehydratedContext           `json:"context"`
	TaskMemory              RehydratedTaskMemory        `json:"task_memory"`
	ArtifactRefs            []artifacts.ArtifactRef     `json:"artifact_refs"`
	EventRefs               []string                    `json:"event_refs"`
	EvidenceRefs            []string                    `json:"evidence_refs"`
	ContinuationRef         ContinuationRef             `json:"continuation_ref"`
	ExpectedWaitingRevision int64                       `json:"expected_waiting_revision"`
}

// RehydrationCoordinator rebuilds logical state and owns only the
// waiting -> rehydrating reservation plus rollback. It deliberately does not
// issue a token, create a worker/credential/task, acquire a lease, or call a
// Phase Agent Runner.
type RehydrationCoordinator struct {
	Store         WaitingStore
	Inputs        RehydrationInputResolver
	Bindings      InputContinuationRebinder
	Continuations ContinuationResolver
	Contexts      ContextReconstructor
	Workspaces    WorkspaceReconstructor
	TaskMemory    TaskMemoryReconstructor
	Surfaces      ExecutionSurfaces
}

type RehydrationRequest struct {
	Key                     WaitingKey
	ExpectedWaitingRevision int64
}

var (
	ErrWaitingRecordNotFound = errors.New("waiting record was not found")
	ErrNotWaiting            = errors.New("invocation is not waiting")
	ErrRehydrationConflict   = errors.New("waiting record changed before rehydration")
)

func (c RehydrationCoordinator) Prepare(ctx context.Context, request RehydrationRequest) (RehydrationPlan, error) {
	if err := c.validate(); err != nil {
		return RehydrationPlan{}, err
	}
	record, found, err := c.Store.Get(ctx, request.Key)
	if err != nil {
		return RehydrationPlan{}, err
	}
	if !found {
		return RehydrationPlan{}, ErrWaitingRecordNotFound
	}
	if record.State != AwaitStateWaiting {
		return RehydrationPlan{}, fmt.Errorf("%w: %s", ErrNotWaiting, record.State)
	}
	if request.ExpectedWaitingRevision != record.Revision {
		return RehydrationPlan{}, ErrRehydrationConflict
	}
	rehydrating := record
	rehydrating.State = AwaitStateRehydrating
	rehydrating, swapped, err := c.Store.CompareAndSwap(ctx, record.Key, record.Revision, rehydrating)
	if err != nil {
		return RehydrationPlan{}, err
	}
	if !swapped {
		return RehydrationPlan{}, ErrRehydrationConflict
	}

	plan, prepareErr := c.prepare(ctx, rehydrating)
	if prepareErr == nil {
		return plan, nil
	}
	if rollbackErr := c.rollbackRecord(ctx, rehydrating); rollbackErr != nil {
		return RehydrationPlan{}, fmt.Errorf("prepare rehydration: %w; rollback: %v", prepareErr, rollbackErr)
	}
	return RehydrationPlan{}, prepareErr
}

// Rollback returns an unprovisioned plan reservation to waiting. It is the
// M4-C retry contract for M4-D provisioning failure; no token existed to
// reuse, and the changed waiting revision makes each attempt auditable.
func (c RehydrationCoordinator) Rollback(ctx context.Context, plan RehydrationPlan) error {
	if c.Store == nil {
		return errors.New("waiting store is required")
	}
	record, found, err := c.Store.Get(ctx, WaitingKey{TaskID: plan.TaskID, InvocationID: plan.InvocationID, Generation: plan.Generation})
	if err != nil {
		return err
	}
	if !found {
		return ErrWaitingRecordNotFound
	}
	if record.State != AwaitStateRehydrating || record.Revision != plan.ExpectedWaitingRevision {
		return ErrRehydrationConflict
	}
	return c.rollbackRecord(ctx, record)
}

func (c RehydrationCoordinator) prepare(ctx context.Context, record WaitingRecord) (RehydrationPlan, error) {
	inputs, err := c.Inputs.ResolveRehydrationInputs(ctx, record)
	if err != nil {
		return RehydrationPlan{}, err
	}
	if inputs.Inputs.InputRevision == "" || !inputs.RevisionIsNewer || inputs.Inputs.InputRevision == record.InputRevision {
		return RehydrationPlan{}, errors.New("rehydration inputs must have a newer input revision")
	}
	if !inputs.AwaitConditionSatisfied && inputs.TerminalReason == "" {
		return RehydrationPlan{}, errors.New("await condition is not resolved")
	}
	if inputs.TerminalReason == "" && pendingInputsRemain(record.PendingInputIDs, inputs.Inputs.Pending) {
		return RehydrationPlan{}, errors.New("declared pending inputs remain pending")
	}
	material, err := c.Continuations.ResolveContinuation(ctx, record.ContinuationRef)
	if err != nil {
		return RehydrationPlan{}, err
	}
	if material.Endpoint != record.Endpoint || (material.WorkspaceRef != "" && material.WorkspaceRef != record.WorkspaceRef) || (material.ContextSliceRef != "" && material.ContextSliceRef != record.ContextSliceRef) || (material.TaskMemoryBufferRef != "" && material.TaskMemoryBufferRef != record.TaskMemoryBufferRef) {
		return RehydrationPlan{}, errors.New("continuation material is incompatible with waiting record")
	}
	nextEpoch := record.ExecutionEpoch + 1
	rebound, err := c.Bindings.RebindInputsForContinuation(ctx, ContinuationBinding{InvocationID: record.Key.InvocationID, Generation: record.Key.Generation, ExecutionEpoch: nextEpoch, PreviousBindingRef: record.PreviousBindingRef, PreviousRevision: record.InputRevision, InputRevision: inputs.Inputs.InputRevision})
	if err != nil {
		return RehydrationPlan{}, err
	}
	if err := ValidateContinuationRebind(rebound); err != nil {
		return RehydrationPlan{}, err
	}
	if rebound.InvocationID != record.Key.InvocationID || rebound.Generation != record.Key.Generation || rebound.ExecutionEpoch != nextEpoch || rebound.PreviousBindingRef != record.PreviousBindingRef || rebound.PreviousRevision != record.InputRevision || rebound.InputRevision != inputs.Inputs.InputRevision {
		return RehydrationPlan{}, errors.New("rebound binding does not match waiting invocation")
	}
	workspace, err := c.Workspaces.ReconstructWorkspace(ctx, record, material)
	if err != nil {
		return RehydrationPlan{}, err
	}
	if workspace.Ref != record.WorkspaceRef || workspace.WriteLeaseHeld || !allowedDirsWithin(workspace.AllowedDirs, record.AllowedDirs) {
		return RehydrationPlan{}, errors.New("rehydrated workspace is incompatible with waiting record")
	}
	contextBinding, err := c.Contexts.ReconstructContext(ctx, record, material)
	if err != nil {
		return RehydrationPlan{}, err
	}
	if contextBinding.SliceRef != record.ContextSliceRef {
		return RehydrationPlan{}, errors.New("rehydrated context does not match waiting record")
	}
	memory, err := c.TaskMemory.ReconstructTaskMemory(ctx, record, material)
	if err != nil {
		return RehydrationPlan{}, err
	}
	if memory.BufferRef != record.TaskMemoryBufferRef {
		return RehydrationPlan{}, errors.New("rehydrated task memory does not match waiting record")
	}
	role, err := phaseagent.RoleForEndpoint(record.Endpoint)
	if err != nil {
		return RehydrationPlan{}, err
	}
	start := phaseagent.StartPhaseInput{InvocationID: record.Key.InvocationID, Endpoint: record.Endpoint, Generation: record.Key.Generation, BindingRef: rebound.BindingRef, Inputs: inputs.Inputs}
	invocation, err := phaseagent.NewInvocationContext(start)
	if err != nil {
		return RehydrationPlan{}, err
	}
	return RehydrationPlan{
		TaskID: record.Key.TaskID, InvocationID: record.Key.InvocationID, Generation: record.Key.Generation,
		NextExecutionEpoch: nextEpoch, Endpoint: record.Endpoint, NewBindingRef: rebound.BindingRef,
		NewInputRevision: inputs.Inputs.InputRevision, Inputs: inputs.Inputs,
		NewlyDelivered: append([]phaseagent.InputDelivery(nil), inputs.NewlyDelivered...),
		Execution:      phaseagent.ExecutionContext{Invocation: invocation, Role: role, Runtime: c.Surfaces.Runtime, ContextReader: c.Surfaces.ContextReader, ContextAgent: c.Surfaces.ContextAgent},
		Workspace:      workspace, Context: contextBinding, TaskMemory: memory,
		ArtifactRefs:    mergeArtifactRefs(record.ArtifactRefs, material.ArtifactRefs),
		EventRefs:       mergeStrings(record.EventRefs, material.EventRefs),
		EvidenceRefs:    mergeStrings(record.EvidenceRefs, material.EvidenceRefs),
		ContinuationRef: record.ContinuationRef, ExpectedWaitingRevision: record.Revision,
	}, nil
}

func (c RehydrationCoordinator) rollbackRecord(ctx context.Context, rehydrating WaitingRecord) error {
	waiting := rehydrating
	waiting.State = AwaitStateWaiting
	_, swapped, err := c.Store.CompareAndSwap(ctx, rehydrating.Key, rehydrating.Revision, waiting)
	if err != nil {
		return err
	}
	if !swapped {
		return ErrRehydrationConflict
	}
	return nil
}

func (c RehydrationCoordinator) validate() error {
	if c.Store == nil || c.Inputs == nil || c.Bindings == nil || c.Continuations == nil || c.Contexts == nil || c.Workspaces == nil || c.TaskMemory == nil {
		return errors.New("rehydration coordinator dependencies are required")
	}
	if c.Surfaces.Runtime == nil || c.Surfaces.ContextReader == nil || c.Surfaces.ContextAgent == nil {
		return errors.New("rehydration execution surfaces are required")
	}
	return nil
}

func pendingInputsRemain(required []string, pending []phaseagent.PendingInput) bool {
	for _, inputID := range required {
		for _, item := range pending {
			if item.InputID == inputID {
				return true
			}
		}
	}
	return false
}

func allowedDirsWithin(current, previous []string) bool {
	if len(previous) == 0 {
		return false
	}
	for _, dir := range current {
		found := false
		for _, allowed := range previous {
			if dir == allowed {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func mergeArtifactRefs(values ...[]artifacts.ArtifactRef) []artifacts.ArtifactRef {
	out := make([]artifacts.ArtifactRef, 0)
	seen := make(map[artifacts.ArtifactRef]struct{})
	for _, refs := range values {
		for _, ref := range refs {
			if ref != "" {
				if _, exists := seen[ref]; !exists {
					out = append(out, ref)
					seen[ref] = struct{}{}
				}
			}
		}
	}
	return out
}

func mergeStrings(values ...[]string) []string {
	out := make([]string, 0)
	seen := make(map[string]struct{})
	for _, refs := range values {
		for _, ref := range refs {
			if ref != "" {
				if _, exists := seen[ref]; !exists {
					out = append(out, ref)
					seen[ref] = struct{}{}
				}
			}
		}
	}
	return out
}
