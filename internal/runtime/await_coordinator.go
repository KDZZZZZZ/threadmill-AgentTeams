package runtime

import (
	"context"
	"errors"
	"reflect"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/artifacts"
	phasemcp "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/mcp/phase"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/phaseagent"
)

// ContinuationPersister records controlled logical continuation material
// before the current physical carrier is relinquished.
type ContinuationPersister interface {
	Put(ContinuationRef, ContinuationMaterial) error
}

// AwaitContinuationCoordinator is the authenticated phaseagent.Runtime seam
// for runtime.awaitInputs. The trusted binding is supplied by the execution
// host; the agent can select only pending input IDs and cannot supply logical
// identity, carrier identity, continuation material, or teardown authority.
type AwaitContinuationCoordinator struct {
	Binding         phasemcp.InvocationBinding
	Delegate        phaseagent.Runtime
	Inputs          phaseagent.PhaseInputSet
	ContinuationRef ContinuationRef
	Continuation    ContinuationMaterial
	Continuations   ContinuationPersister
	Relinquisher    ExecutionRelinquisher
	ExecutionToken  string `json:"-"`
}

func (c *AwaitContinuationCoordinator) AwaitInputs(ctx context.Context, request phaseagent.AwaitInputsRequest) (phaseagent.InputWaitResult, error) {
	if err := c.validate(); err != nil {
		return phaseagent.InputWaitResult{}, err
	}
	pending, err := selectedPendingInputs(c.Inputs.Pending, request.InputIDs)
	if err != nil {
		return phaseagent.InputWaitResult{}, err
	}
	if err := c.Continuations.Put(c.ContinuationRef, c.Continuation); err != nil {
		if !errors.Is(err, ErrWaitingRecordExists) {
			return phaseagent.InputWaitResult{}, err
		}
		resolver, ok := c.Continuations.(ContinuationResolver)
		if !ok {
			return phaseagent.InputWaitResult{}, err
		}
		existing, resolveErr := resolver.ResolveContinuation(ctx, c.ContinuationRef)
		if resolveErr != nil || !reflect.DeepEqual(existing, c.Continuation) {
			return phaseagent.InputWaitResult{}, errors.New("continuation reference already contains different material")
		}
	}
	record := WaitingRecord{
		Key:            WaitingKey{TaskID: c.Binding.TaskID, InvocationID: c.Binding.InvocationID, Generation: c.Binding.Generation},
		ExecutionEpoch: ExecutionEpoch(c.Binding.ExecutionEpoch), Endpoint: c.Binding.Endpoint,
		PreviousBindingRef: c.Binding.BindingRef, InputRevision: c.Binding.InputRevision,
		PendingInputIDs: pending, ContinuationRef: c.ContinuationRef, State: AwaitStatePreparingAwait,
		WorkspaceRef: c.Continuation.WorkspaceRef, AllowedDirs: append([]string(nil), c.Binding.AllowedDirs...),
		ContextSliceRef: c.Continuation.ContextSliceRef, TaskMemoryBufferRef: c.Continuation.TaskMemoryBufferRef,
		ArtifactRefs: append([]artifacts.ArtifactRef(nil), c.Continuation.ArtifactRefs...),
		EventRefs:    append([]string(nil), c.Continuation.EventRefs...), EvidenceRefs: append([]string(nil), c.Continuation.EvidenceRefs...),
	}
	waiting, err := c.Relinquisher.Relinquish(ctx, RelinquishRequest{Record: record, TeamHarnessID: c.currentTaskID(ctx), ExecutionToken: c.ExecutionToken, Reason: "awaiting declared Phase inputs"})
	if err != nil {
		return phaseagent.InputWaitResult{}, err
	}
	return phaseagent.InputWaitResult{InputRevision: waiting.InputRevision, Pending: append([]phaseagent.PendingInput(nil), c.Inputs.Pending...)}, nil
}

func (c *AwaitContinuationCoordinator) SubmitPhaseOutput(ctx context.Context, output phaseagent.PhaseOutput) error {
	if c.Delegate == nil {
		return errors.New("runtime delegate is required")
	}
	return c.Delegate.SubmitPhaseOutput(ctx, output)
}
func (c *AwaitContinuationCoordinator) ProposeOrchestration(ctx context.Context, proposal phaseagent.OrchestrationProposal) error {
	if c.Delegate == nil {
		return errors.New("runtime delegate is required")
	}
	return c.Delegate.ProposeOrchestration(ctx, proposal)
}
func (c *AwaitContinuationCoordinator) SubmitRequirement(ctx context.Context, requirement phaseagent.Requirement) error {
	if c.Delegate == nil {
		return errors.New("runtime delegate is required")
	}
	return c.Delegate.SubmitRequirement(ctx, requirement)
}
func (c *AwaitContinuationCoordinator) ListTaskMemoryCandidates(ctx context.Context) (phaseagent.TaskMemoryBufferView, error) {
	if c.Delegate == nil {
		return phaseagent.TaskMemoryBufferView{}, errors.New("runtime delegate is required")
	}
	return c.Delegate.ListTaskMemoryCandidates(ctx)
}
func (c *AwaitContinuationCoordinator) SubmitMemoryCandidate(ctx context.Context, candidate phaseagent.MemoryCandidate) (phaseagent.CandidateBufferedReceipt, error) {
	if c.Delegate == nil {
		return phaseagent.CandidateBufferedReceipt{}, errors.New("runtime delegate is required")
	}
	return c.Delegate.SubmitMemoryCandidate(ctx, candidate)
}

func (c *AwaitContinuationCoordinator) validate() error {
	if c == nil || c.Continuations == nil || c.Relinquisher.Store == nil {
		return errors.New("await continuation dependencies are required")
	}
	if c.Binding.TaskID == "" || c.Binding.InvocationID == "" || c.Binding.Generation <= 0 || c.Binding.ExecutionEpoch <= 0 || c.Binding.BindingRef == "" || c.Binding.InputRevision == "" {
		return errors.New("trusted await binding is incomplete")
	}
	if c.ExecutionToken == "" {
		return errors.New("current execution token is required for await relinquish")
	}
	if c.Inputs.InputRevision != c.Binding.InputRevision || len(c.Inputs.Pending) == 0 || c.ContinuationRef == "" || c.Continuation.Endpoint != c.Binding.Endpoint {
		return errors.New("await inputs or continuation material do not match trusted binding")
	}
	return nil
}

func (c *AwaitContinuationCoordinator) currentTaskID(ctx context.Context) string {
	if c.Relinquisher.PhysicalExecutions == nil {
		return ""
	}
	execution, found, _ := c.Relinquisher.PhysicalExecutions.Get(ctx, PhysicalExecutionKey{TaskID: c.Binding.TaskID, InvocationID: c.Binding.InvocationID, Generation: c.Binding.Generation, ExecutionEpoch: ExecutionEpoch(c.Binding.ExecutionEpoch)})
	if !found {
		return ""
	}
	return execution.TeamHarnessTaskID
}

func selectedPendingInputs(pending []phaseagent.PendingInput, requested []string) ([]string, error) {
	if len(requested) == 0 {
		out := make([]string, 0, len(pending))
		for _, item := range pending {
			out = append(out, item.InputID)
		}
		return out, nil
	}
	seen := make(map[string]struct{}, len(requested))
	for _, id := range requested {
		if id == "" {
			return nil, errors.New("pending input ID is required")
		}
		found := false
		for _, item := range pending {
			if item.InputID == id {
				found = true
				break
			}
		}
		if !found {
			return nil, errors.New("await request contains an undeclared input")
		}
		if _, duplicate := seen[id]; !duplicate {
			seen[id] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for _, item := range pending {
		if _, ok := seen[item.InputID]; ok {
			out = append(out, item.InputID)
		}
	}
	return out, nil
}
