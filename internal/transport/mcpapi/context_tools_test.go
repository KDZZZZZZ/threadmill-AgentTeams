package mcpapi

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/contextagent"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/contextgraph"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime/phase"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/taskmanager"
)

type fakeContextReader struct {
	principal     auth.Principal
	listReq       contextgraph.ListSubgraphsRequest
	exploreReq    contextgraph.ExploreRequest
	subscribeReq  contextgraph.SubscribeRequest
	unsubscribeID string
}

func (f *fakeContextReader) ListSubgraphs(_ context.Context, principal auth.Principal, req contextgraph.ListSubgraphsRequest) ([]contextgraph.ContextSubgraph, error) {
	f.principal = principal
	f.listReq = req
	return []contextgraph.ContextSubgraph{{ID: "sg-1"}}, nil
}

func (f *fakeContextReader) Explore(_ context.Context, principal auth.Principal, req contextgraph.ExploreRequest) (contextgraph.ContextSliceDelta, error) {
	f.principal = principal
	f.exploreReq = req
	return contextgraph.ContextSliceDelta{Frontier: []string{"node:n1"}}, nil
}

func (f *fakeContextReader) Subscribe(_ context.Context, principal auth.Principal, req contextgraph.SubscribeRequest) (contextgraph.ContextSubscription, error) {
	f.principal = principal
	f.subscribeReq = req
	return contextgraph.ContextSubscription{ID: "sub-1"}, nil
}

func (f *fakeContextReader) Unsubscribe(_ context.Context, principal auth.Principal, subscriptionID string) error {
	f.principal = principal
	f.unsubscribeID = subscriptionID
	return nil
}

func TestContextReaderToolsForwardFormalRequestsAndPrincipal(t *testing.T) {
	reader := &fakeContextReader{}
	registry, err := NewRegistry(ContextReaderToolSpecs(reader)...)
	if err != nil {
		t.Fatal(err)
	}
	principal := principalWithTools(auth.RoleExecutor, auth.ToolContextListSubgraphs, auth.ToolContextExplore, auth.ToolContextSubscribe, auth.ToolContextUnsubscribe)
	if _, err := registry.Invoke(context.Background(), principal, auth.ToolContextListSubgraphs, auth.Scope{ProjectID: "project-a"}, mustJSON(t, contextgraph.ListSubgraphsRequest{Filter: "general"})); err != nil {
		t.Fatal(err)
	}
	if reader.principal.InvocationID != principal.InvocationID || reader.listReq.Filter != "general" {
		t.Fatalf("list forwarding principal=%#v req=%#v", reader.principal, reader.listReq)
	}
	if _, err := registry.Invoke(context.Background(), principal, auth.ToolContextExplore, auth.Scope{ProjectID: "project-a"}, mustJSON(t, contextgraph.ExploreRequest{AnchorRef: "node:n1", Depth: 2})); err != nil {
		t.Fatal(err)
	}
	if reader.exploreReq.AnchorRef != "node:n1" || reader.exploreReq.Depth != 2 {
		t.Fatalf("explore req = %#v", reader.exploreReq)
	}
	if _, err := registry.Invoke(context.Background(), principal, auth.ToolContextSubscribe, auth.Scope{ProjectID: "project-a"}, mustJSON(t, contextgraph.SubscribeRequest{SubgraphIDs: []string{"sg-1"}, EventKinds: []string{"node"}})); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(reader.subscribeReq.SubgraphIDs, []string{"sg-1"}) {
		t.Fatalf("subscribe req = %#v", reader.subscribeReq)
	}
	if _, err := registry.Invoke(context.Background(), principal, auth.ToolContextUnsubscribe, auth.Scope{ProjectID: "project-a"}, mustJSON(t, unsubscribeRequest{SubscriptionID: "sub-1"})); err != nil {
		t.Fatal(err)
	}
	if reader.unsubscribeID != "sub-1" {
		t.Fatalf("unsubscribe id = %q", reader.unsubscribeID)
	}
}

type fakeRetriever struct {
	principal auth.Principal
	req       contextagent.ContextRetrieveRequest
	calls     int
}

func (f *fakeRetriever) RetrieveForConsumer(_ context.Context, principal auth.Principal, req contextagent.ContextRetrieveRequest) (contextagent.ContextRetrieveResult, error) {
	f.calls++
	f.principal = principal
	f.req = req
	return contextagent.ContextRetrieveResult{SubscriptionIDs: []string{"sub-search"}}, nil
}

func TestContextAgentRetrieveToolPassesOriginalConsumerToRuntimeSeam(t *testing.T) {
	retriever := &fakeRetriever{}
	registry, err := NewRegistry(ContextAgentRetrieveToolSpec(retriever))
	if err != nil {
		t.Fatal(err)
	}
	principal := principalWithTools(auth.RoleExecutor, auth.ToolContextAgentRetrieve)
	if _, err := registry.Invoke(context.Background(), principal, auth.ToolContextAgentRetrieve, auth.Scope{ProjectID: "project-a"}, mustJSON(t, contextagent.ContextRetrieveRequest{Query: "release safety"})); err != nil {
		t.Fatal(err)
	}
	if retriever.principal.Role != auth.RoleExecutor || retriever.principal.InvocationID != principal.InvocationID {
		t.Fatalf("retrieve principal = %#v", retriever.principal)
	}
	if retriever.req.Query != "release safety" {
		t.Fatalf("retrieve req = %#v", retriever.req)
	}
}

func TestContextAgentRetrieveToolRejectsEmptyQueryBeforeDispatch(t *testing.T) {
	retriever := &fakeRetriever{}
	registry, err := NewRegistry(ContextAgentRetrieveToolSpec(retriever))
	if err != nil {
		t.Fatal(err)
	}
	principal := principalWithTools(auth.RoleExecutor, auth.ToolContextAgentRetrieve)
	for _, payload := range []json.RawMessage{nil, []byte(`null`), mustJSON(t, contextagent.ContextRetrieveRequest{Query: "  "})} {
		if _, err := registry.Invoke(context.Background(), principal, auth.ToolContextAgentRetrieve, auth.Scope{ProjectID: "project-a"}, payload); !kernel.IsCode(err, kernel.CodeInvalidRequest) {
			t.Fatalf("empty retrieve payload err = %v, want invalid_request", err)
		}
	}
	if retriever.calls != 0 {
		t.Fatalf("dispatcher called %d times for invalid retrieve payloads", retriever.calls)
	}
}

func TestContextAgentAggregateToolsExcludeSubscriptions(t *testing.T) {
	registry, err := NewRegistry(ContextAgentToolSpecs(&fakeContextReader{}, nil, nil, nil)...)
	if err != nil {
		t.Fatal(err)
	}
	tools := registry.AvailableTools()
	for _, forbidden := range []auth.Tool{auth.ToolContextSubscribe, auth.ToolContextUnsubscribe} {
		if _, ok := tools[forbidden]; ok {
			t.Fatalf("context agent aggregate exposed forbidden subscription tool %s", forbidden)
		}
	}
	for _, required := range []auth.Tool{auth.ToolContextListSubgraphs, auth.ToolContextExplore, auth.ToolContextSearch, auth.ToolContextSubmitReview} {
		if _, ok := tools[required]; !ok {
			t.Fatalf("context agent aggregate missing %s", required)
		}
	}
}

type fakeMemory struct {
	principal            auth.Principal
	submitReq            contextgraph.SubmitCandidateRequest
	projectReq           contextgraph.ProjectTaskContextRequest
	registerTaskID       kernel.TaskID
	finalizeTaskID       kernel.TaskID
	listTaskInvoked      bool
	awaitInvocation      kernel.InvocationID
	awaitReq             phase.AwaitInputsRequest
	outputInvocation     kernel.InvocationID
	outputReq            phase.PhaseOutput
	requirementPrincipal auth.Principal
	requirementReq       taskmanager.Requirement
}

func (f *fakeMemory) ListTaskCandidates(_ context.Context, principal auth.Principal) (contextgraph.TaskMemoryBufferView, error) {
	f.principal = principal
	f.listTaskInvoked = true
	return contextgraph.TaskMemoryBufferView{}, nil
}

func (f *fakeMemory) SubmitCandidate(_ context.Context, principal auth.Principal, req contextgraph.SubmitCandidateRequest) (contextgraph.TaskMemoryCandidateView, error) {
	f.principal = principal
	f.submitReq = req
	return contextgraph.TaskMemoryCandidateView{CandidateID: "candidate-1"}, nil
}

func (f *fakeMemory) RegisterTaskSubgraph(_ context.Context, principal auth.Principal, taskID kernel.TaskID) (contextgraph.TaskContextSubgraphBinding, error) {
	f.principal = principal
	f.registerTaskID = taskID
	return contextgraph.TaskContextSubgraphBinding{TaskID: string(taskID), SubgraphID: "task-sg"}, nil
}

func (f *fakeMemory) ProjectTaskContext(_ context.Context, principal auth.Principal, req contextgraph.ProjectTaskContextRequest) (contextgraph.ContextNodeRef, error) {
	f.principal = principal
	f.projectReq = req
	return "node-1", nil
}

func (f *fakeMemory) FinalizeTaskMemory(_ context.Context, principal auth.Principal, taskID kernel.TaskID) (contextgraph.FrozenCandidateBatch, error) {
	f.principal = principal
	f.finalizeTaskID = taskID
	return contextgraph.FrozenCandidateBatch{TaskID: string(taskID)}, nil
}

func (f *fakeMemory) AwaitInputs(_ context.Context, invocationID kernel.InvocationID, req phase.AwaitInputsRequest) (phase.InputWaitResult, error) {
	f.awaitInvocation = invocationID
	f.awaitReq = req
	return phase.InputWaitResult{InputRevision: "inputs-2"}, nil
}

func (f *fakeMemory) SubmitPhaseOutput(_ context.Context, invocationID kernel.InvocationID, req phase.PhaseOutput) (phase.OutputReceipt, error) {
	f.outputInvocation = invocationID
	f.outputReq = req
	return phase.OutputReceipt{InvocationID: invocationID, Output: req}, nil
}

func (f *fakeMemory) SubmitRequirement(_ context.Context, principal auth.Principal, req taskmanager.Requirement) (any, error) {
	f.requirementPrincipal = principal
	f.requirementReq = req
	return "accepted", nil
}

func TestMemoryAndTaskContextToolsUseFormalRequestTypes(t *testing.T) {
	memory := &fakeMemory{}
	specs := append(TaskMemoryToolSpecs(memory, memory), TaskContextToolSpecs(memory, memory)...)
	registry, err := NewRegistry(specs...)
	if err != nil {
		t.Fatal(err)
	}
	phase := principalWithTools(auth.RoleExecutor, auth.ToolAgentListTaskMemoryCandidates, auth.ToolAgentSubmitMemoryCandidate)
	if _, err := registry.Invoke(context.Background(), phase, auth.ToolAgentListTaskMemoryCandidates, auth.Scope{ProjectID: "project-a", TaskID: "task-a"}, nil); err != nil {
		t.Fatal(err)
	}
	if !memory.listTaskInvoked || memory.principal.TaskID != "task-a" {
		t.Fatalf("list memory principal=%#v invoked=%v", memory.principal, memory.listTaskInvoked)
	}
	candidateReq := contextgraph.SubmitCandidateRequest{Candidate: contextgraph.MemoryCandidate{Statement: "fact", Kind: "fact", SourceRefs: []string{"evidence:1"}}}
	if _, err := registry.Invoke(context.Background(), phase, auth.ToolAgentSubmitMemoryCandidate, auth.Scope{ProjectID: "project-a", TaskID: "task-a"}, mustJSON(t, candidateReq)); err != nil {
		t.Fatal(err)
	}
	if memory.submitReq.Candidate.Statement != "fact" {
		t.Fatalf("submit candidate req = %#v", memory.submitReq)
	}
	tm := principalWithTools(auth.RoleTaskManager, auth.ToolContextRegisterTaskSubgraph, auth.ToolContextProjectTaskContext, auth.ToolContextFinalizeTaskMemory)
	if _, err := registry.Invoke(context.Background(), tm, auth.ToolContextRegisterTaskSubgraph, auth.Scope{ProjectID: "project-a"}, mustJSON(t, registerTaskSubgraphRequest{TaskID: "task-b"})); err != nil {
		t.Fatal(err)
	}
	if memory.registerTaskID != "task-b" {
		t.Fatalf("register task id = %q", memory.registerTaskID)
	}
	projectReq := contextgraph.ProjectTaskContextRequest{Projection: contextgraph.TaskContextProjection{ProjectionID: "proj-1", SourceRevision: "rev-1"}}
	if _, err := registry.Invoke(context.Background(), tm, auth.ToolContextProjectTaskContext, auth.Scope{ProjectID: "project-a"}, mustJSON(t, projectReq)); err != nil {
		t.Fatal(err)
	}
	if memory.projectReq.Projection.ProjectionID != "proj-1" {
		t.Fatalf("project req = %#v", memory.projectReq)
	}
	if _, err := registry.Invoke(context.Background(), tm, auth.ToolContextFinalizeTaskMemory, auth.Scope{ProjectID: "project-a"}, mustJSON(t, finalizeTaskMemoryRequest{TaskID: "task-b"})); err != nil {
		t.Fatal(err)
	}
	if memory.finalizeTaskID != "task-b" {
		t.Fatalf("finalize task id = %q", memory.finalizeTaskID)
	}
}

func TestPhaseOutboundToolsUseFormalTypesAndTrustedBindings(t *testing.T) {
	runtime := &fakeMemory{}
	specs := append(PhaseRuntimeToolSpecs(runtime), RequirementToolSpec(runtime))
	registry, err := NewRegistry(specs...)
	if err != nil {
		t.Fatal(err)
	}
	principal := principalWithTools(auth.RoleExecutor, auth.ToolRuntimeAwaitInputs, auth.ToolAgentSubmitPhaseOutput, auth.ToolAgentSubmitRequirement)
	awaitReq := phase.AwaitInputsRequest{InputIDs: []string{"input-a"}}
	if _, err := registry.Invoke(context.Background(), principal, auth.ToolRuntimeAwaitInputs, auth.Scope{ProjectID: "project-a", TaskID: "task-a"}, mustJSON(t, awaitReq)); err != nil {
		t.Fatal(err)
	}
	if runtime.awaitInvocation != principal.InvocationID || !reflect.DeepEqual(runtime.awaitReq.InputIDs, []string{"input-a"}) {
		t.Fatalf("await invocation=%q req=%#v", runtime.awaitInvocation, runtime.awaitReq)
	}
	output := phase.PhaseOutput{Phase: "execute", ReportRef: "workspace/report.md", DeliveryRefs: []string{"workspace/out.txt"}}
	if _, err := registry.Invoke(context.Background(), principal, auth.ToolAgentSubmitPhaseOutput, auth.Scope{ProjectID: "project-a", TaskID: "task-a"}, mustJSON(t, output)); err != nil {
		t.Fatal(err)
	}
	if runtime.outputInvocation != principal.InvocationID || runtime.outputReq.ReportRef != "workspace/report.md" {
		t.Fatalf("output invocation=%q req=%#v", runtime.outputInvocation, runtime.outputReq)
	}
	requirement := taskmanager.Requirement{Text: "new work", Goal: "deliver"}
	if _, err := registry.Invoke(context.Background(), principal, auth.ToolAgentSubmitRequirement, auth.Scope{ProjectID: "project-a", TaskID: "task-a"}, mustJSON(t, requirement)); err != nil {
		t.Fatal(err)
	}
	if runtime.requirementPrincipal.InvocationID != principal.InvocationID || runtime.requirementReq.Text != "new work" {
		t.Fatalf("requirement principal=%#v req=%#v", runtime.requirementPrincipal, runtime.requirementReq)
	}
}

func principalWithTools(role auth.Role, tools ...auth.Tool) auth.Principal {
	principal := auth.Principal{
		ActorPrincipalID: "agent-a",
		Kind:             auth.PrincipalAgent,
		ProjectID:        "project-a",
		TaskID:           "task-a",
		InvocationID:     "inv-a",
		Role:             role,
		Tools:            auth.ToolSet(tools...),
	}
	if role == auth.RoleContext {
		principal.Operation = "retrieve"
	}
	return principal
}

func mustJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
