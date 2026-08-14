package mcpapi

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime/phase"
)

type fakeProposalRuntime struct {
	principal auth.Principal
	scope     auth.BoundScope
	intent    phase.OrchestrationIntent
	calls     int
}

func (f *fakeProposalRuntime) SubmitOrchestrationIntent(_ context.Context, principal auth.Principal, scope auth.BoundScope, intent phase.OrchestrationIntent) (phase.OrchestrationProposal, error) {
	f.principal, f.scope, f.intent = principal, scope, intent
	f.calls++
	return phase.OrchestrationProposal{
		ProposalID:          "proposal-runtime",
		ClientRef:           "client-runtime",
		FromInvocationID:    scope.InvocationID,
		OrchestrationAdvice: intent.OrchestrationAdvice,
		DeliverySpecAdvice:  intent.DeliverySpecAdvice,
		ReportSpecAdvice:    intent.ReportSpecAdvice,
		Rationale:           intent.Rationale,
		EvidenceRefs:        intent.EvidenceRefs,
	}, nil
}

func TestOrchestrationProposalToolAcceptsIntentAndRuntimeFillsEnvelope(t *testing.T) {
	runtime := &fakeProposalRuntime{}
	registry, err := NewRegistry(OrchestrationProposalToolSpec(runtime))
	if err != nil {
		t.Fatal(err)
	}
	principal := principalWithTools(auth.RoleExecutor, auth.ToolAgentProposeOrchestration)
	intent := phase.OrchestrationIntent{
		OrchestrationAdvice: phase.OrchestrationDependency,
		DeliverySpecAdvice:  "add the missing API schema",
		ReportSpecAdvice:    "report compatibility evidence",
		Rationale:           "the declared input does not include the schema",
		EvidenceRefs:        []string{"artifact-1"},
	}
	result, err := registry.Invoke(context.Background(), principal, auth.ToolAgentProposeOrchestration, auth.Scope{ProjectID: "project-a", TaskID: "task-a"}, mustJSON(t, intent))
	if err != nil {
		t.Fatal(err)
	}
	proposal := result.(phase.OrchestrationProposal)
	if proposal.ProposalID != "proposal-runtime" || proposal.FromInvocationID != principal.InvocationID {
		t.Fatalf("proposal=%#v", proposal)
	}
	if runtime.calls != 1 || runtime.intent.OrchestrationAdvice != phase.OrchestrationDependency {
		t.Fatalf("runtime intent=%#v calls=%d", runtime.intent, runtime.calls)
	}
}

func TestOrchestrationProposalToolRejectsTrustedEnvelopeFields(t *testing.T) {
	runtime := &fakeProposalRuntime{}
	registry, err := NewRegistry(OrchestrationProposalToolSpec(runtime))
	if err != nil {
		t.Fatal(err)
	}
	principal := principalWithTools(auth.RoleExecutor, auth.ToolAgentProposeOrchestration)
	payloads := []json.RawMessage{
		json.RawMessage(`{"proposal_id":"forged","orchestration_advice":"retry","rationale":"x"}`),
		json.RawMessage(`{"from_invocation_id":"inv-other","orchestration_advice":"retry","rationale":"x"}`),
		json.RawMessage(`{"based_on_graph_revision":99,"orchestration_advice":"retry","rationale":"x"}`),
	}
	for _, payload := range payloads {
		if _, err := registry.Invoke(context.Background(), principal, auth.ToolAgentProposeOrchestration, auth.Scope{ProjectID: "project-a", TaskID: "task-a"}, payload); !kernel.IsCode(err, kernel.CodeInvalidRequest) {
			t.Errorf("spoof payload err=%v, want invalid_request", err)
		}
	}
	if runtime.calls != 0 {
		t.Fatalf("spoofed proposal reached runtime %d times", runtime.calls)
	}
}
