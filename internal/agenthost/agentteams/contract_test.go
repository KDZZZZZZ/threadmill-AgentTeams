package agentteams

import (
	"context"
	"errors"
	"reflect"
	"testing"

	phasemcp "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/mcp/phase"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/phaseagent"
)

type fakeExecutionHost struct {
	requests []HostExecutionRequest
	outcome  HostExecutionOutcome
	err      error
}

func (h *fakeExecutionHost) Execute(_ context.Context, request HostExecutionRequest) (HostExecutionOutcome, error) {
	h.requests = append(h.requests, request)
	return h.outcome, h.err
}

type fakeEnvelopeResolver struct {
	envelope HostEnvelope
	err      error
}

func (r fakeEnvelopeResolver) ResolveHostEnvelope(_ context.Context, _ phaseagent.ExecutionContext) (HostEnvelope, error) {
	return r.envelope, r.err
}

var _ phaseagent.PhaseExecutor = (*AgentTeamsPhaseExecutor)(nil)
var _ ExecutionHost = (*fakeExecutionHost)(nil)

func TestExecutionContextConvertsToPrivateHostRequest(t *testing.T) {
	t.Parallel()

	execution := testExecution(t, phaseagent.PhasePlan)
	request, err := buildHostExecutionRequest(execution, testEnvelope(execution))
	if err != nil {
		t.Fatalf("buildHostExecutionRequest: %v", err)
	}
	if request.InvocationID != "invocation-1" || request.Endpoint.EndpointID != "plan" || request.Generation != 1 {
		t.Fatalf("identity conversion mismatch: %#v", request)
	}
	if request.Envelope.BindingRef != execution.Invocation.Start.BindingRef || request.Envelope.MCPBinding.Binding.TaskID != "task-1" {
		t.Fatalf("private envelope mismatch: %#v", request.Envelope)
	}
}

func TestCapabilityMappingPhaseWriteBoundaries(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		phase                   phaseagent.Phase
		implementationWrite     bool
		structuredArtifactWrite bool
		evidenceWrite           bool
	}{
		{phaseagent.PhasePlan, false, true, false},
		{phaseagent.PhaseExecute, true, true, true},
		{phaseagent.PhaseVerify, false, true, true},
	} {
		role, err := phaseagent.RoleForEndpoint(phaseagent.PhaseEndpointRef{TaskID: "task-1", EndpointID: string(tc.phase)})
		if err != nil {
			t.Fatalf("RoleForEndpoint(%s): %v", tc.phase, err)
		}
		policy := MapCapabilities(role)
		if policy.AllowImplementationWrite != tc.implementationWrite || policy.AllowStructuredArtifactWrite != tc.structuredArtifactWrite || policy.AllowEvidenceWrite != tc.evidenceWrite {
			t.Fatalf("wrong policy for %s: %#v", tc.phase, policy)
		}
		if len(policy.ToolPolicy.BlockedHostActions) == 0 {
			t.Fatalf("%s has no host action blacklist", tc.phase)
		}
	}
}

func TestExecutorCallsHostOnceAndCompletionReturnsNil(t *testing.T) {
	t.Parallel()

	execution := testExecution(t, phaseagent.PhaseExecute)
	host := &fakeExecutionHost{outcome: HostExecutionOutcome{ExecutionID: "host-1", Status: HostExecutionCompleted}}
	executor := newTestExecutor(t, host, testEnvelope(execution))
	if err := executor.Execute(context.Background(), execution); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(host.requests) != 1 {
		t.Fatalf("host calls = %d, want 1", len(host.requests))
	}
}

func TestHostTransportErrorIsReturned(t *testing.T) {
	t.Parallel()

	execution := testExecution(t, phaseagent.PhaseExecute)
	want := errors.New("worker unavailable")
	executor := newTestExecutor(t, &fakeExecutionHost{err: want}, testEnvelope(execution))
	var got *TransportError
	if !errors.As(executor.Execute(context.Background(), execution), &got) || !errors.Is(got, want) {
		t.Fatalf("expected wrapped transport error")
	}
}

func TestWaitingAndStoppedAreTypedControlOutcomes(t *testing.T) {
	t.Parallel()

	for _, status := range []HostExecutionStatus{HostExecutionWaiting, HostExecutionStopped} {
		execution := testExecution(t, phaseagent.PhasePlan)
		executor := newTestExecutor(t, &fakeExecutionHost{outcome: HostExecutionOutcome{Status: status}}, testEnvelope(execution))
		var got *ControlOutcomeError
		if !errors.As(executor.Execute(context.Background(), execution), &got) || got.Outcome.Status != status {
			t.Fatalf("expected typed control outcome %s", status)
		}
	}
}

func TestHostFailureIsNotPhaseOutput(t *testing.T) {
	t.Parallel()

	execution := testExecution(t, phaseagent.PhaseVerify)
	executor := newTestExecutor(t, &fakeExecutionHost{outcome: HostExecutionOutcome{Status: HostExecutionFailed, Summary: "check crashed"}}, testEnvelope(execution))
	var got *AgentExecutionError
	if !errors.As(executor.Execute(context.Background(), execution), &got) {
		t.Fatalf("expected AgentExecutionError")
	}
}

func TestHostOutcomeDoesNotContainPhaseOutput(t *testing.T) {
	t.Parallel()

	if _, exists := reflect.TypeOf(HostExecutionOutcome{}).FieldByName("PhaseOutput"); exists {
		t.Fatal("host execution outcome must not carry PhaseOutput")
	}
}

func testExecution(t *testing.T, phase phaseagent.Phase) phaseagent.ExecutionContext {
	t.Helper()
	role, err := phaseagent.RoleForEndpoint(phaseagent.PhaseEndpointRef{TaskID: "task-1", EndpointID: string(phase)})
	if err != nil {
		t.Fatal(err)
	}
	start := phaseagent.StartPhaseInput{
		InvocationID: "invocation-1",
		Endpoint:     phaseagent.PhaseEndpointRef{TaskID: "task-1", EndpointID: string(phase)},
		Generation:   1,
		BindingRef:   "binding-1",
		Inputs:       phaseagent.PhaseInputSet{InputRevision: "inputs-1"},
	}
	invocation, err := phaseagent.NewInvocationContext(start)
	if err != nil {
		t.Fatal(err)
	}
	return phaseagent.ExecutionContext{Invocation: invocation, Role: role}
}

func testEnvelope(execution phaseagent.ExecutionContext) HostEnvelope {
	return HostEnvelope{
		BindingRef: execution.Invocation.Start.BindingRef,
		TaskSpec:   "resolved phase contract projection",
		Workspace:  WorkspaceMount{Root: "/workspace/task-1", AllowedDirs: []string{"src/"}, ReadOnly: execution.Role.Phase != phaseagent.PhaseExecute},
		Context:    MaterializedContext{Content: "authorized context"},
		TaskMemory: phaseagent.TaskMemoryBufferView{},
		MCPBinding: TrustedMCPBinding{
			Token: "opaque-token",
			Binding: phasemcp.InvocationBinding{
				InvocationID:  execution.Invocation.Start.InvocationID,
				TaskID:        execution.Invocation.Start.Endpoint.TaskID,
				Endpoint:      execution.Invocation.Start.Endpoint,
				Generation:    execution.Invocation.Start.Generation,
				Role:          execution.Role.Phase,
				BindingRef:    execution.Invocation.Start.BindingRef,
				PermissionRef: "permission-1",
				Capabilities:  execution.Role.Capabilities,
			},
		},
	}
}

func newTestExecutor(t *testing.T, host ExecutionHost, envelope HostEnvelope) *AgentTeamsPhaseExecutor {
	t.Helper()
	executor, err := NewAgentTeamsPhaseExecutor(host, fakeEnvelopeResolver{envelope: envelope})
	if err != nil {
		t.Fatal(err)
	}
	return executor
}
