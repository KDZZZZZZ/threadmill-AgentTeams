package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/artifacts"
	phasemcp "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/mcp/phase"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/phaseagent"
)

type completionPorts struct {
	calls    []string
	failOnce string
}

func (p *completionPorts) CompleteTeamHarnessTask(context.Context, TeamHarnessTask) error {
	return p.call("task")
}
func (p *completionPorts) DeleteWorker(context.Context, ProvisionedWorker) error {
	return p.call("worker")
}
func (p *completionPorts) CleanupWorkerMCP(context.Context, ProvisionedWorker) error {
	return p.call("mcp")
}
func (p *completionPorts) RevokeMCPCredential(context.Context, MCPCredentialBinding) error {
	return p.call("credential")
}
func (p *completionPorts) ReleaseWorkspaceLease(context.Context, WorkspaceLease) error {
	return p.call("lease")
}
func (p *completionPorts) ProvisionWorker(context.Context, WorkerProvisionRequest) (ProvisionedWorker, error) {
	return ProvisionedWorker{}, errors.New("not used")
}
func (p *completionPorts) CreateMCPCredential(context.Context, MCPCredentialRequest) (MCPCredentialBinding, error) {
	return MCPCredentialBinding{}, errors.New("not used")
}
func (p *completionPorts) AcquireWorkspaceLease(context.Context, RehydrationPlan) (WorkspaceLease, error) {
	return WorkspaceLease{}, errors.New("not used")
}
func (p *completionPorts) call(name string) error {
	p.calls = append(p.calls, name)
	if p.failOnce == name {
		p.failOnce = ""
		return errors.New("test-only cleanup failure")
	}
	return nil
}

type completionEvents struct{ values []artifacts.Event }

func (r *completionEvents) Record(_ context.Context, event artifacts.Event) error {
	r.values = append(r.values, event)
	return nil
}

type conflictWaitingStore struct {
	WaitingStore
	conflict bool
}

func (s *conflictWaitingStore) CompareAndSwap(ctx context.Context, key WaitingKey, revision int64, next WaitingRecord) (WaitingRecord, bool, error) {
	if s.conflict {
		s.conflict = false
		return WaitingRecord{}, false, nil
	}
	return s.WaitingStore.CompareAndSwap(ctx, key, revision, next)
}

func completionFixture(t *testing.T) (*PhaseOutputCompletionCoordinator, *InMemoryWaitingStore, *InMemoryPhysicalExecutionStore, *InMemoryPhaseOutputStore, *phasemcp.BindingRegistry, *completionPorts, *completionEvents, string) {
	t.Helper()
	ctx := context.Background()
	waiting := NewInMemoryWaitingStore()
	endpoint := phaseagent.PhaseEndpointRef{TaskID: "task-a", EndpointID: "execute"}
	record, err := waiting.Create(ctx, WaitingRecord{Key: WaitingKey{TaskID: "task-a", InvocationID: "invocation-a", Generation: 3}, ExecutionEpoch: 2, Endpoint: endpoint, PreviousBindingRef: "binding-b2", InputRevision: "input-r5", ContinuationRef: "continuation-r4", State: AwaitStateRunning, WorkspaceRef: "workspace-a"})
	if err != nil {
		t.Fatal(err)
	}
	physical := NewInMemoryPhysicalExecutionStore()
	_, err = physical.Create(ctx, PhysicalExecution{TaskID: "task-a", InvocationID: "invocation-a", Generation: 3, ExecutionEpoch: 2, WorkerID: "worker-b", WorkerName: "worker-b", TeamHarnessTaskID: "task-b", TeamHarnessAssignedTo: "worker-b", BindingRef: "binding-b2", InputRevision: "input-r5", AgentSessionRef: "matrix:!room:test", AgentPackageDigest: "digest", PackageConsumed: true, MCPClientID: "threadmill", CredentialBindingRef: "credential-b", DesiredRuntimeGeneration: 1, AppliedRuntimeGeneration: 1, WorkspaceLeaseRef: "lease-b", ExecutionAuthorizationRef: "auth-b", State: PhysicalExecutionRunning, ObservedTaskStatus: "in_progress", TaskAcknowledged: true})
	if err != nil {
		t.Fatal(err)
	}
	binding := phasemcp.InvocationBinding{TaskID: record.Key.TaskID, InvocationID: record.Key.InvocationID, Generation: record.Key.Generation, ExecutionEpoch: 2, Endpoint: endpoint, BindingRef: "binding-b2", InputRevision: "input-r5", Role: phaseagent.PhaseExecute, Capabilities: phaseagent.PhaseCapabilities{AllowOutputSubmission: true}}
	registry := phasemcp.NewBindingRegistry()
	ports, events, outputs := &completionPorts{}, &completionEvents{}, NewInMemoryPhaseOutputStore()
	coordinator := &PhaseOutputCompletionCoordinator{Binding: binding, Delegate: noopRuntime{}, Outputs: outputs, Waiting: waiting, PhysicalExecutions: physical, Events: events, Tasks: ports, Workers: ports, MCP: ports, Credentials: ports, Bindings: registry, Leases: ports}
	token, err := registry.Issue(phasemcp.BoundServices{Binding: binding, Runtime: coordinator, Reader: noopReader{}, Agent: noopAgent{}, Expires: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	return coordinator, waiting, physical, outputs, registry, ports, events, token.Token
}

func TestPhaseOutputCompletionTerminalizesAndPreservesEvidence(t *testing.T) {
	c, waiting, physical, outputs, registry, ports, events, token := completionFixture(t)
	output := phaseagent.PhaseOutput{Phase: phaseagent.PhaseExecute, ReportRef: "artifact-report", EvidenceRefs: []string{"artifact-evidence"}}
	if err := c.SubmitPhaseOutput(context.Background(), output); err != nil {
		t.Fatal(err)
	}
	record, found, _ := waiting.Get(context.Background(), WaitingKey{TaskID: "task-a", InvocationID: "invocation-a", Generation: 3})
	if !found || record.State != AwaitStateTerminal {
		t.Fatalf("waiting record not terminal: %#v", record)
	}
	execution, found, _ := physical.Get(context.Background(), PhysicalExecutionKey{TaskID: "task-a", InvocationID: "invocation-a", Generation: 3, ExecutionEpoch: 2})
	if !found || execution.State != PhysicalExecutionTerminated || !execution.Teardown.TeamHarnessTaskCancelled || !execution.Teardown.WorkerDeleted || !execution.Teardown.MCPCleaned || !execution.Teardown.CredentialRevoked || !execution.Teardown.TokenRevoked || !execution.Teardown.LeaseReleased {
		t.Fatalf("physical execution not terminated: %#v", execution)
	}
	accepted, found, _ := outputs.Get(context.Background(), PhaseOutputKey{TaskID: "task-a", InvocationID: "invocation-a", Generation: 3})
	if !found || !accepted.EventRecorded || !reflect.DeepEqual(accepted.Output, output) {
		t.Fatalf("output not preserved: %#v", accepted)
	}
	if len(events.values) != 1 || events.values[0].Type != artifacts.EventPhaseOutputSubmitted {
		t.Fatalf("events=%#v", events.values)
	}
	if _, err := registry.Resolve(token); !errors.Is(err, phasemcp.ErrInvalidToken) {
		t.Fatalf("token remained resolvable: %v", err)
	}
	if !reflect.DeepEqual(ports.calls, []string{"task", "worker", "mcp", "credential", "lease"}) {
		t.Fatalf("cleanup order=%v", ports.calls)
	}
	if err := c.SubmitPhaseOutput(context.Background(), output); err != nil {
		t.Fatalf("duplicate: %v", err)
	}
	if len(events.values) != 1 || len(ports.calls) != 5 {
		t.Fatalf("duplicate repeated effects events=%d calls=%v", len(events.values), ports.calls)
	}
	if err := c.SubmitPhaseOutput(context.Background(), phaseagent.PhaseOutput{Phase: phaseagent.PhaseExecute, ReportRef: "different"}); !errors.Is(err, ErrConflictingOutput) {
		t.Fatalf("conflict=%v", err)
	}
}

func TestPhaseOutputCompletionRejectsStaleIdentityAndCASConflictBeforeTeardown(t *testing.T) {
	c, waiting, physical, _, _, ports, _, _ := completionFixture(t)
	c.Binding.InputRevision = "input-r4"
	if err := c.SubmitPhaseOutput(context.Background(), phaseagent.PhaseOutput{Phase: phaseagent.PhaseExecute}); !errors.Is(err, ErrStaleCompletion) {
		t.Fatalf("stale input=%v", err)
	}
	if len(ports.calls) != 0 {
		t.Fatalf("stale completion tore down: %v", ports.calls)
	}
	c.Binding.InputRevision = "input-r5"
	c.Waiting = &conflictWaitingStore{WaitingStore: waiting, conflict: true}
	if err := c.SubmitPhaseOutput(context.Background(), phaseagent.PhaseOutput{Phase: phaseagent.PhaseExecute, ReportRef: "report"}); !errors.Is(err, ErrCompletionConflict) {
		t.Fatalf("CAS conflict=%v", err)
	}
	if len(ports.calls) != 0 {
		t.Fatalf("CAS conflict tore down: %v", ports.calls)
	}
	execution, _, _ := physical.Get(context.Background(), PhysicalExecutionKey{TaskID: "task-a", InvocationID: "invocation-a", Generation: 3, ExecutionEpoch: 2})
	if execution.State != PhysicalExecutionRunning {
		t.Fatalf("physical state changed on CAS conflict: %s", execution.State)
	}
}

func TestPhaseOutputCompletionCleanupIsRetryable(t *testing.T) {
	c, waiting, physical, _, registry, ports, events, token := completionFixture(t)
	ports.failOnce = "mcp"
	output := phaseagent.PhaseOutput{Phase: phaseagent.PhaseExecute, ReportRef: "report"}
	if err := c.SubmitPhaseOutput(context.Background(), output); err == nil {
		t.Fatal("expected cleanup failure")
	}
	record, _, _ := waiting.Get(context.Background(), WaitingKey{TaskID: "task-a", InvocationID: "invocation-a", Generation: 3})
	if record.State != AwaitStateTerminal {
		t.Fatalf("cleanup failure rolled logical state back: %s", record.State)
	}
	execution, _, _ := physical.Get(context.Background(), PhysicalExecutionKey{TaskID: "task-a", InvocationID: "invocation-a", Generation: 3, ExecutionEpoch: 2})
	if execution.State != PhysicalExecutionTearingDown || !execution.Teardown.TeamHarnessTaskCancelled || !execution.Teardown.WorkerDeleted || execution.Teardown.MCPCleaned {
		t.Fatalf("retry evidence mismatch: %#v", execution)
	}
	if _, err := registry.Resolve(token); err != nil {
		t.Fatalf("token revoked before its cleanup step: %v", err)
	}
	if err := c.RetryCompletion(context.Background()); err != nil {
		t.Fatal(err)
	}
	execution, _, _ = physical.Get(context.Background(), execution.Key())
	if execution.State != PhysicalExecutionTerminated {
		t.Fatalf("retry did not terminate: %#v", execution)
	}
	if len(events.values) != 1 {
		t.Fatalf("retry repeated event: %d", len(events.values))
	}
}

func TestBindingRegistryRevokeBindingIsEpochAndRevisionScoped(t *testing.T) {
	registry := phasemcp.NewBindingRegistry()
	base := phasemcp.InvocationBinding{TaskID: "task", InvocationID: "invocation", Generation: 3, ExecutionEpoch: 2, BindingRef: "b2", InputRevision: "r5"}
	issue := func(binding phasemcp.InvocationBinding) string {
		value, err := registry.Issue(phasemcp.BoundServices{Binding: binding, Runtime: noopRuntime{}, Reader: noopReader{}, Agent: noopAgent{}})
		if err != nil {
			t.Fatal(err)
		}
		return value.Token
	}
	current := issue(base)
	old := base
	old.ExecutionEpoch, old.BindingRef, old.InputRevision = 1, "b1", "r4"
	oldToken := issue(old)
	if revoked := registry.RevokeBinding(base); revoked != 1 {
		t.Fatalf("revoked=%d", revoked)
	}
	if _, err := registry.Resolve(current); !errors.Is(err, phasemcp.ErrInvalidToken) {
		t.Fatalf("current token remained: %v", err)
	}
	if _, err := registry.Resolve(oldToken); err != nil {
		t.Fatalf("historical token was incorrectly selected: %v", err)
	}
}

func TestPhaseCompletionDurableEvidenceContainsNoCarrierSecrets(t *testing.T) {
	c, waiting, physical, outputs, _, _, events, _ := completionFixture(t)
	if err := c.SubmitPhaseOutput(context.Background(), phaseagent.PhaseOutput{Phase: phaseagent.PhaseExecute, ReportRef: "artifact-report"}); err != nil {
		t.Fatal(err)
	}
	waitingRecord, _, _ := waiting.Get(context.Background(), WaitingKey{TaskID: "task-a", InvocationID: "invocation-a", Generation: 3})
	physicalRecord, _, _ := physical.Get(context.Background(), PhysicalExecutionKey{TaskID: "task-a", InvocationID: "invocation-a", Generation: 3, ExecutionEpoch: 2})
	outputRecord, _, _ := outputs.Get(context.Background(), PhaseOutputKey{TaskID: "task-a", InvocationID: "invocation-a", Generation: 3})
	encoded, err := json.Marshal([]any{waitingRecord, physicalRecord, outputRecord, events.values})
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"test-only-raw-token", "test-only-credential-value", "Bearer test-only", "X-Private-Value", "hidden reasoning", "provider conversation"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("durable completion evidence leaked %q", forbidden)
		}
	}
}
