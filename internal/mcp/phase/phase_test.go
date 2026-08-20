package phasemcp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/artifacts"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/phaseagent"
)

type fakeRuntime struct {
	outputs      []phaseagent.PhaseOutput
	proposals    []phaseagent.OrchestrationProposal
	requirements []phaseagent.Requirement
	candidates   []phaseagent.MemoryCandidate
}

type eventOwningRuntime struct{ fakeRuntime }

func (*eventOwningRuntime) RecordsPhaseOutputSubmitted() bool { return true }

func (r *fakeRuntime) AwaitInputs(context.Context, phaseagent.AwaitInputsRequest) (phaseagent.InputWaitResult, error) {
	return phaseagent.InputWaitResult{InputRevision: "next"}, nil
}
func (r *fakeRuntime) SubmitPhaseOutput(_ context.Context, output phaseagent.PhaseOutput) error {
	r.outputs = append(r.outputs, output)
	return nil
}
func (r *fakeRuntime) ProposeOrchestration(_ context.Context, proposal phaseagent.OrchestrationProposal) error {
	r.proposals = append(r.proposals, proposal)
	return nil
}
func (r *fakeRuntime) SubmitRequirement(_ context.Context, requirement phaseagent.Requirement) error {
	r.requirements = append(r.requirements, requirement)
	return nil
}
func (r *fakeRuntime) ListTaskMemoryCandidates(context.Context) (phaseagent.TaskMemoryBufferView, error) {
	return phaseagent.TaskMemoryBufferView{Candidates: []phaseagent.TaskMemoryCandidateView{{CandidateID: "candidate-1"}}}, nil
}
func (r *fakeRuntime) SubmitMemoryCandidate(_ context.Context, candidate phaseagent.MemoryCandidate) (phaseagent.CandidateBufferedReceipt, error) {
	r.candidates = append(r.candidates, candidate)
	return phaseagent.CandidateBufferedReceipt{CandidateID: "buffered-1"}, nil
}

type fakeReader struct{}

func (fakeReader) ListSubgraphs(context.Context, phaseagent.ListSubgraphsRequest) ([]phaseagent.ContextSubgraph, error) {
	return []phaseagent.ContextSubgraph{{ID: "subgraph-1"}}, nil
}
func (fakeReader) Explore(context.Context, phaseagent.ExploreRequest) (phaseagent.ContextSliceDelta, error) {
	return phaseagent.ContextSliceDelta{}, nil
}
func (fakeReader) Subscribe(context.Context, phaseagent.SubscribeRequest) (phaseagent.ContextSubscription, error) {
	return phaseagent.ContextSubscription{ID: "subscription-1"}, nil
}
func (fakeReader) Unsubscribe(context.Context, string) error { return nil }

type fakeAgent struct{ queries []string }

func (a *fakeAgent) Retrieve(_ context.Context, request phaseagent.ContextRetrieveRequest) (phaseagent.ContextRetrieveResult, error) {
	a.queries = append(a.queries, request.Query)
	return phaseagent.ContextRetrieveResult{}, nil
}

func TestTokenRoutesOutputAndCandidateToBoundRuntime(t *testing.T) {
	t.Parallel()
	registry := NewBindingRegistry()
	runtimeA, runtimeB := &fakeRuntime{}, &fakeRuntime{}
	bindingA := mustIssue(t, registry, runtimeA, &fakeAgent{}, "invocation-a", "task-a", time.Time{})
	bindingB := mustIssue(t, registry, runtimeB, &fakeAgent{}, "invocation-b", "task-b", time.Time{})
	handler, _ := NewHandler(registry, artifacts.NewInMemoryRegistry(nil))
	if err := handler.SubmitPhaseOutput(context.Background(), bindingA.Token, phaseagent.PhaseOutput{}); err != nil {
		t.Fatal(err)
	}
	if _, err := handler.SubmitMemoryCandidate(context.Background(), bindingA.Token, phaseagent.MemoryCandidate{Statement: "fact"}); err != nil {
		t.Fatal(err)
	}
	if len(runtimeA.outputs) != 1 || len(runtimeA.candidates) != 1 || len(runtimeB.outputs) != 0 || bindingA.Binding.TaskID == bindingB.Binding.TaskID {
		t.Fatalf("token routing failed: A=%#v B=%#v", runtimeA, runtimeB)
	}
}

func TestInvalidAndExpiredTokensAreRejected(t *testing.T) {
	t.Parallel()
	registry := NewBindingRegistry()
	handler, _ := NewHandler(registry)
	if _, err := handler.Tools("missing"); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("invalid token: %v", err)
	}
	binding := mustIssue(t, registry, &fakeRuntime{}, &fakeAgent{}, "inv", "task", time.Now().Add(-time.Second))
	if _, err := handler.Tools(binding.Token); !errors.Is(err, ErrExpiredToken) {
		t.Fatalf("expired token: %v", err)
	}
}

func TestPhaseToolRegistryOmitsSearchAndRoutesRetrieve(t *testing.T) {
	t.Parallel()
	registry := NewBindingRegistry()
	agent := &fakeAgent{}
	binding := mustIssue(t, registry, &fakeRuntime{}, agent, "inv", "task", time.Time{})
	handler, _ := NewHandler(registry)
	tools, err := handler.Tools(binding.Token)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range tools {
		if name == "context.search" {
			t.Fatal("ordinary Phase Agent must not receive context.search")
		}
	}
	if _, err := handler.Retrieve(context.Background(), binding.Token, phaseagent.ContextRetrieveRequest{Query: "find the plan"}); err != nil {
		t.Fatal(err)
	}
	if len(agent.queries) != 1 || agent.queries[0] != "find the plan" {
		t.Fatalf("retrieve request mismatch: %#v", agent.queries)
	}
}

func TestIssueExecutionBindsRunnerIdentityInsteadOfAgentInput(t *testing.T) {
	t.Parallel()

	registry := NewBindingRegistry()
	runtime := &fakeRuntime{}
	role, _ := phaseagent.RoleForEndpoint(phaseagent.PhaseEndpointRef{TaskID: "trusted-task", EndpointID: "plan"})
	execution := phaseagent.ExecutionContext{
		Invocation: phaseagent.InvocationContext{Start: phaseagent.StartPhaseInput{InvocationID: "trusted-invocation", Endpoint: phaseagent.PhaseEndpointRef{TaskID: "trusted-task", EndpointID: "plan"}, Generation: 2, BindingRef: "trusted-binding"}},
		Role:       role, Runtime: runtime, ContextReader: fakeReader{}, ContextAgent: &fakeAgent{},
	}
	binding, err := registry.IssueExecution(execution, []string{"plan/"}, "permission-1", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if binding.Binding.InvocationID != "trusted-invocation" || binding.Binding.TaskID != "trusted-task" || binding.Binding.Generation != 2 || binding.Binding.BindingRef != "trusted-binding" {
		t.Fatalf("execution identity was not bound from Runtime context: %#v", binding.Binding)
	}
}

func TestArtifactRegistrationAndFormalOutputReferenceValidation(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "out"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "out", "report.md"), []byte("report"), 0644); err != nil {
		t.Fatal(err)
	}
	registry := NewBindingRegistry()
	runtime := &fakeRuntime{}
	binding := mustIssue(t, registry, runtime, &fakeAgent{}, "invocation-a", "task-a", time.Time{})
	services := registry.bindings[binding.Token]
	services.Binding.WorkspaceRoot, services.Binding.AllowedDirs = root, []string{"out"}
	registry.bindings[binding.Token] = services
	recorder := &mcpRecorder{}
	handler, _ := NewHandler(registry, artifacts.NewInMemoryRegistry(recorder), recorder)
	ref, err := handler.RegisterArtifact(context.Background(), binding.Token, "out/report.md", artifacts.ArtifactTypeGeneratedReport, "text/markdown")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := handler.RegisterArtifact(context.Background(), binding.Token, "../secret", artifacts.ArtifactTypeGeneratedReport, ""); err == nil {
		t.Fatal("path escape accepted")
	}
	if err := handler.SubmitPhaseOutput(context.Background(), binding.Token, phaseagent.PhaseOutput{ReportRef: string(ref)}); err != nil {
		t.Fatal(err)
	}
	if err := handler.SubmitPhaseOutput(context.Background(), binding.Token, phaseagent.PhaseOutput{ReportRef: "artifact-missing"}); err == nil {
		t.Fatal("unregistered output ref accepted")
	}
	if len(runtime.outputs) != 1 || !recorder.has(artifacts.EventPhaseOutputSubmitted) {
		t.Fatalf("output/event mismatch: %#v %#v", runtime.outputs, recorder.events)
	}
}

func TestCompletionRuntimeOwnsPhaseOutputSubmittedEvent(t *testing.T) {
	registry := NewBindingRegistry()
	runtime := &eventOwningRuntime{}
	binding := mustIssue(t, registry, runtime, &fakeAgent{}, "invocation-a", "task-a", time.Time{})
	recorder := &mcpRecorder{}
	handler, _ := NewHandler(registry, artifacts.NewInMemoryRegistry(nil), recorder)
	if err := handler.SubmitPhaseOutput(context.Background(), binding.Token, phaseagent.PhaseOutput{Phase: phaseagent.PhaseExecute}); err != nil {
		t.Fatal(err)
	}
	if len(runtime.outputs) != 1 || len(recorder.events) != 0 {
		t.Fatalf("completion-owned event was duplicated: outputs=%d events=%#v", len(runtime.outputs), recorder.events)
	}
}

type mcpRecorder struct{ events []artifacts.Event }

func (r *mcpRecorder) Record(_ context.Context, event artifacts.Event) error {
	r.events = append(r.events, event)
	return nil
}
func (r *mcpRecorder) has(kind string) bool {
	for _, event := range r.events {
		if event.Type == kind {
			return true
		}
	}
	return false
}

func mustIssue(t *testing.T, registry *BindingRegistry, runtime phaseagent.Runtime, agent phaseagent.ContextAgent, invocationID, taskID string, expires time.Time) ExecutionBinding {
	t.Helper()
	role, err := phaseagent.RoleForEndpoint(phaseagent.PhaseEndpointRef{TaskID: taskID, EndpointID: "execute"})
	if err != nil {
		t.Fatal(err)
	}
	binding, err := registry.Issue(BoundServices{Binding: InvocationBinding{InvocationID: invocationID, TaskID: taskID, Endpoint: phaseagent.PhaseEndpointRef{TaskID: taskID, EndpointID: "execute"}, Generation: 1, Role: role.Phase, BindingRef: "binding-" + invocationID, Capabilities: role.Capabilities}, Runtime: runtime, Reader: fakeReader{}, Agent: agent, Expires: expires})
	if err != nil {
		t.Fatal(err)
	}
	return binding
}
