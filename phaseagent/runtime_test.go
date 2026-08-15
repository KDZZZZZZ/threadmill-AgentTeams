package phaseagent

import (
	"context"
	"testing"
)

// mockRuntime is a test double for the Phase Agent outbound port. It does not
// model Runtime behavior, graph mutations, or Context Graph persistence.
type mockRuntime struct {
	awaitResult InputWaitResult
	awaitErr    error
	memory      TaskMemoryBufferView

	awaitRequests    []AwaitInputsRequest
	outputs          []PhaseOutput
	proposals        []OrchestrationProposal
	requirements     []Requirement
	candidateSubmits []MemoryCandidate
	candidateReceipt CandidateBufferedReceipt
}

func (m *mockRuntime) AwaitInputs(_ context.Context, request AwaitInputsRequest) (InputWaitResult, error) {
	m.awaitRequests = append(m.awaitRequests, request)
	return m.awaitResult, m.awaitErr
}

func (m *mockRuntime) SubmitPhaseOutput(_ context.Context, output PhaseOutput) error {
	m.outputs = append(m.outputs, output)
	return nil
}

func (m *mockRuntime) ProposeOrchestration(_ context.Context, proposal OrchestrationProposal) error {
	m.proposals = append(m.proposals, proposal)
	return nil
}

func (m *mockRuntime) SubmitRequirement(_ context.Context, requirement Requirement) error {
	m.requirements = append(m.requirements, requirement)
	return nil
}

func (m *mockRuntime) ListTaskMemoryCandidates(_ context.Context) (TaskMemoryBufferView, error) {
	return m.memory, nil
}

func (m *mockRuntime) SubmitMemoryCandidate(_ context.Context, candidate MemoryCandidate) (CandidateBufferedReceipt, error) {
	m.candidateSubmits = append(m.candidateSubmits, candidate)
	return m.candidateReceipt, nil
}

var _ Runtime = (*mockRuntime)(nil)

func TestPhaseAgentDependsOnRuntimeInterface(t *testing.T) {
	t.Parallel()

	runtime := &mockRuntime{
		awaitResult:      InputWaitResult{InputRevision: "input-r3", Delivered: []InputDelivery{{InputID: "review", FromEndpoint: PhaseEndpointRef{TaskID: "review-task", EndpointID: "verify"}}}},
		memory:           TaskMemoryBufferView{Candidates: []TaskMemoryCandidateView{{CandidateID: "candidate-existing"}}},
		candidateReceipt: CandidateBufferedReceipt{CandidateID: "candidate-new"},
	}
	var agentRuntime Runtime = runtime
	ctx := context.Background()

	if _, err := agentRuntime.AwaitInputs(ctx, AwaitInputsRequest{InputIDs: []string{"review"}}); err != nil {
		t.Fatalf("AwaitInputs: %v", err)
	}
	if err := agentRuntime.SubmitPhaseOutput(ctx, PhaseOutput{Phase: PhaseVerify, ReportRef: "verify-report"}); err != nil {
		t.Fatalf("SubmitPhaseOutput: %v", err)
	}
	if err := agentRuntime.ProposeOrchestration(ctx, OrchestrationProposal{ProposalID: "proposal-1", FromEndpoint: PhaseEndpointRef{TaskID: "task-1", EndpointID: "verify"}}); err != nil {
		t.Fatalf("ProposeOrchestration: %v", err)
	}
	if err := agentRuntime.SubmitRequirement(ctx, Requirement{Text: "add coverage"}); err != nil {
		t.Fatalf("SubmitRequirement: %v", err)
	}
	if buffer, err := agentRuntime.ListTaskMemoryCandidates(ctx); err != nil || len(buffer.Candidates) != 1 {
		t.Fatalf("ListTaskMemoryCandidates: buffer=%#v err=%v", buffer, err)
	}
	receipt, err := agentRuntime.SubmitMemoryCandidate(ctx, MemoryCandidate{Statement: "new fact", Kind: "fact", SourceRefs: []string{"evidence"}})
	if err != nil {
		t.Fatalf("SubmitMemoryCandidate: %v", err)
	}

	if receipt.CandidateID != "candidate-new" || len(runtime.awaitRequests) != 1 || len(runtime.outputs) != 1 || len(runtime.proposals) != 1 || len(runtime.requirements) != 1 || len(runtime.candidateSubmits) != 1 {
		t.Fatalf("mock did not record Runtime calls: %#v", runtime)
	}
}
