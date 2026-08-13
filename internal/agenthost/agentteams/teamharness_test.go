package agentteams

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/artifacts"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/phaseagent"
)

type fakeTaskflowClient struct {
	delegates   []TeamHarnessDelegateTaskRequest
	delegateErr error
	snapshots   []TeamHarnessTaskSnapshot
	checkErr    error
	checks      int
}

func (f *fakeTaskflowClient) DelegateTask(_ context.Context, request TeamHarnessDelegateTaskRequest) error {
	f.delegates = append(f.delegates, request)
	return f.delegateErr
}

func (f *fakeTaskflowClient) CheckTask(_ context.Context, _ string) (TeamHarnessTaskSnapshot, error) {
	f.checks++
	if f.checkErr != nil {
		return TeamHarnessTaskSnapshot{}, f.checkErr
	}
	if len(f.snapshots) == 0 {
		return TeamHarnessTaskSnapshot{Status: TeamHarnessTaskAssigned}, nil
	}
	snapshot := f.snapshots[0]
	if len(f.snapshots) > 1 {
		f.snapshots = f.snapshots[1:]
	}
	return snapshot, nil
}

type fixedRouteResolver struct{ route TaskflowRoute }

func (r fixedRouteResolver) ResolveTaskflowRoute(_ context.Context, _ HostExecutionRequest) (TaskflowRoute, error) {
	return r.route, nil
}

type fixedWorkerSelector struct{ worker string }

func (s fixedWorkerSelector) SelectWorker(_ context.Context, _ HostExecutionRequest) (string, error) {
	return s.worker, nil
}

type fakeMCPInjector struct {
	executionIDs []string
	bindings     []TrustedMCPBinding
	err          error
}

func (i *fakeMCPInjector) InjectPhaseMCP(_ context.Context, executionID string, binding TrustedMCPBinding) error {
	i.executionIDs = append(i.executionIDs, executionID)
	i.bindings = append(i.bindings, binding)
	return i.err
}

var _ TaskflowClient = (*fakeTaskflowClient)(nil)
var _ ExecutionHost = (*TeamHarnessExecutionHost)(nil)

func TestTeamHarnessHostFreshInvocationDelegatesAcknowledgesAndSubmits(t *testing.T) {
	t.Parallel()

	request := testHostRequest(t, phaseagent.PhaseExecute)
	client := &fakeTaskflowClient{snapshots: []TeamHarnessTaskSnapshot{
		{Status: TeamHarnessTaskAssigned},
		{Status: TeamHarnessTaskInProgress},
		{Status: TeamHarnessTaskSubmitted, Summary: "worker submission", ResultStatus: "SUCCESS", Deliverables: []string{"shared/tasks/x/result.md"}},
	}}
	taskMap := NewInMemoryInvocationTaskMap()
	injector := &fakeMCPInjector{}
	host := newTestTeamHarnessHost(t, client, taskMap, injector)
	outcome, err := host.Execute(context.Background(), request)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if outcome.Status != HostExecutionCompleted || !outcome.Acknowledged || outcome.Summary != "worker submission" {
		t.Fatalf("unexpected outcome: %#v", outcome)
	}
	if len(client.delegates) != 1 || client.delegates[0].Assignee != "worker-1" || client.delegates[0].ProjectID != "project-1" {
		t.Fatalf("delegate payload mismatch: %#v", client.delegates)
	}
	if len(injector.executionIDs) != 1 || injector.bindings[0].Token == "" {
		t.Fatalf("mcp binding was not injected: %#v", injector)
	}
	if taskID, found, err := taskMap.Lookup(context.Background(), request.InvocationID); err != nil || !found || taskID != outcome.ExecutionID {
		t.Fatalf("task mapping mismatch: taskID=%q found=%t err=%v", taskID, found, err)
	}
}

func TestDelegateRequestProjectsSpecWithoutTrustedBinding(t *testing.T) {
	t.Parallel()

	request := testHostRequest(t, phaseagent.PhasePlan)
	payload := BuildDelegateTaskRequest(request, TaskflowRoute{ProjectID: "project-1", RoomID: "room-1"}, "worker-1", "agentteams-task-1")
	if payload.TaskID != "agentteams-task-1" || payload.Title != "Threadmill plan phase" {
		t.Fatalf("payload identity mismatch: %#v", payload)
	}
	for _, forbidden := range []string{request.Envelope.BindingRef, request.Envelope.MCPBinding.Token, request.Envelope.MCPBinding.Binding.PermissionRef} {
		if strings.Contains(payload.Spec, forbidden) {
			t.Fatalf("spec contains trusted field %q: %s", forbidden, payload.Spec)
		}
	}
	if !strings.Contains(payload.Spec, "resolved phase contract projection") || !strings.Contains(payload.Spec, "Implementation write allowed: `false`") {
		t.Fatalf("spec lacks permitted projection: %s", payload.Spec)
	}
}

func TestDelegateFailureIsTransportFailure(t *testing.T) {
	t.Parallel()

	client := &fakeTaskflowClient{delegateErr: errors.New("task store unavailable")}
	host := newTestTeamHarnessHost(t, client, NewInMemoryInvocationTaskMap(), &fakeMCPInjector{})
	_, err := host.Execute(context.Background(), testHostRequest(t, phaseagent.PhaseExecute))
	if !errors.Is(err, client.delegateErr) {
		t.Fatalf("delegate error = %v, want wrapped %v", err, client.delegateErr)
	}
}

func TestContextCancellationReturnsCancelledOutcome(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	host := newTestTeamHarnessHost(t, &fakeTaskflowClient{}, NewInMemoryInvocationTaskMap(), &fakeMCPInjector{})
	outcome, err := host.Execute(ctx, testHostRequest(t, phaseagent.PhaseVerify))
	if err != nil || outcome.Status != HostExecutionCancelled {
		t.Fatalf("cancel mapping mismatch: outcome=%#v err=%v", outcome, err)
	}
}

func TestWaitingIsExplicitlyUnsupported(t *testing.T) {
	t.Parallel()

	client := &fakeTaskflowClient{snapshots: []TeamHarnessTaskSnapshot{{Status: TeamHarnessTaskWaiting}}}
	host := newTestTeamHarnessHost(t, client, NewInMemoryInvocationTaskMap(), &fakeMCPInjector{})
	_, err := host.Execute(context.Background(), testHostRequest(t, phaseagent.PhasePlan))
	var unsupported *UnsupportedControlFlowError
	if !errors.As(err, &unsupported) || unsupported.Flow != "waiting" {
		t.Fatalf("waiting must be explicit unsupported control flow: %v", err)
	}
}

func TestSubmitEvidenceDoesNotProducePhaseOutput(t *testing.T) {
	t.Parallel()

	client := &fakeTaskflowClient{snapshots: []TeamHarnessTaskSnapshot{{Status: TeamHarnessTaskSubmitted, Acknowledged: true, ResultStatus: "SUCCESS", Deliverables: []string{"shared/tasks/x/result.md"}}}}
	host := newTestTeamHarnessHost(t, client, NewInMemoryInvocationTaskMap(), &fakeMCPInjector{})
	outcome, err := host.Execute(context.Background(), testHostRequest(t, phaseagent.PhaseVerify))
	if err != nil || outcome.Status != HostExecutionCompleted {
		t.Fatalf("submission observation mismatch: outcome=%#v err=%v", outcome, err)
	}
	// HostExecutionOutcome has no PhaseOutput field; M1 only observes the
	// physical taskflow submission and leaves formal output to Runtime/M2 MCP.
}

func TestResultAndDeliverablesBecomeExecutionEvidenceOnly(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "out"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "out", "result.md"), []byte("human report"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "out", "delivery.txt"), []byte("candidate"), 0644); err != nil {
		t.Fatal(err)
	}
	request := testHostRequest(t, phaseagent.PhaseExecute)
	request.Envelope.Workspace = WorkspaceMount{Root: root, AllowedDirs: []string{"out"}}
	events := &evidenceRecorder{}
	ingestor := ArtifactEvidenceIngestor{Registrar: artifacts.NewInMemoryRegistry(events), Recorder: events}
	evidence, err := ingestor.IngestExecutionEvidence(context.Background(), request, TeamHarnessTaskSnapshot{ResultPath: "out/result.md", Deliverables: []string{"out/delivery.txt"}})
	if err != nil {
		t.Fatal(err)
	}
	if evidence.ResultRef == "" || len(evidence.DeliverableRefs) != 1 || !events.has(artifacts.EventAgentTeamsExecutionCompleted) {
		t.Fatalf("evidence/event mismatch: %#v %#v", evidence, events.events)
	}
	// ExecutionEvidence has no PhaseOutput, and no submission handler is called:
	// result.md and deliverables remain audit evidence until the agent explicitly
	// registers/references artifacts in agent.submitPhaseOutput.
}

type evidenceRecorder struct{ events []artifacts.Event }

func (r *evidenceRecorder) Record(_ context.Context, event artifacts.Event) error {
	r.events = append(r.events, event)
	return nil
}
func (r *evidenceRecorder) has(kind string) bool {
	for _, event := range r.events {
		if event.Type == kind {
			return true
		}
	}
	return false
}

func testHostRequest(t *testing.T, phase phaseagent.Phase) HostExecutionRequest {
	t.Helper()
	execution := testExecution(t, phase)
	request, err := buildHostExecutionRequest(execution, testEnvelope(execution))
	if err != nil {
		t.Fatal(err)
	}
	return request
}

func newTestTeamHarnessHost(t *testing.T, client TaskflowClient, taskMap InvocationTaskMap, injector MCPInjector) *TeamHarnessExecutionHost {
	t.Helper()
	host, err := NewTeamHarnessExecutionHost(
		client,
		fixedRouteResolver{route: TaskflowRoute{ProjectID: "project-1", RoomID: "room-1"}},
		fixedWorkerSelector{worker: "worker-1"},
		taskMap,
		DefaultTaskIDGenerator{},
		injector,
	)
	if err != nil {
		t.Fatal(err)
	}
	host.PollInterval = time.Nanosecond
	return host
}
