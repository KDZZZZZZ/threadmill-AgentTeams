package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/artifacts"
	phasemcp "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/mcp/phase"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/phaseagent"
)

func durableCompletionFixture(t *testing.T) (*PhaseOutputCompletionCoordinator, *SQLiteRuntimeStateRepository, *completionPorts, *completionEvents) {
	t.Helper()
	return durableCompletionFixtureAt(t, filepath.Join(t.TempDir(), "runtime.db"))
}

func durableCompletionFixtureAt(t *testing.T, databasePath string) (*PhaseOutputCompletionCoordinator, *SQLiteRuntimeStateRepository, *completionPorts, *completionEvents) {
	t.Helper()
	ctx := context.Background()
	repo, err := OpenSQLiteRuntimeStateRepository(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	state, err := NewDurableLifecycleState(repo)
	if err != nil {
		t.Fatal(err)
	}
	key := WaitingKey{TaskID: "task-d", InvocationID: "inv-d", Generation: 3}
	endpoint := phaseagent.PhaseEndpointRef{TaskID: key.TaskID, EndpointID: "execute"}
	if _, err = state.Waiting.Create(ctx, WaitingRecord{Key: key, ExecutionEpoch: 2, Endpoint: endpoint, PreviousBindingRef: "binding-b2", InputRevision: "input-r5", ContinuationRef: "c", State: AwaitStateRunning, WorkspaceRef: "workspace-a"}); err != nil {
		t.Fatal(err)
	}
	if _, err = state.PhysicalExecutions.Create(ctx, PhysicalExecution{TaskID: key.TaskID, InvocationID: key.InvocationID, Generation: key.Generation, ExecutionEpoch: 2, WorkerID: "worker-b", WorkerName: "worker-b", TeamHarnessTaskID: "task-b", TeamHarnessAssignedTo: "worker-b", BindingRef: "binding-b2", InputRevision: "input-r5", AgentSessionRef: "matrix:!room:test", AgentPackageDigest: "digest", PackageConsumed: true, MCPClientID: "threadmill", CredentialBindingRef: "credential-b", DesiredRuntimeGeneration: 1, AppliedRuntimeGeneration: 1, WorkspaceLeaseRef: "lease-b", ExecutionAuthorizationRef: "auth-b", State: PhysicalExecutionRunning, ObservedTaskStatus: "in_progress", TaskAcknowledged: true}); err != nil {
		t.Fatal(err)
	}
	ports, events := &completionPorts{}, &completionEvents{}
	return durableCompletionCoordinator(state, key, endpoint, ports, events), repo, ports, events
}

func durableCompletionCoordinator(state DurableLifecycleState, key WaitingKey, endpoint phaseagent.PhaseEndpointRef, ports *completionPorts, events *completionEvents) *PhaseOutputCompletionCoordinator {
	binding := phasemcp.InvocationBinding{TaskID: key.TaskID, InvocationID: key.InvocationID, Generation: key.Generation, ExecutionEpoch: 2, Endpoint: endpoint, BindingRef: "binding-b2", InputRevision: "input-r5", Role: phaseagent.PhaseExecute, Capabilities: phaseagent.PhaseCapabilities{AllowOutputSubmission: true}}
	return &PhaseOutputCompletionCoordinator{Binding: binding, Delegate: noopRuntime{}, Outputs: state.Outputs, Mutations: state.Mutations, Waiting: state.Waiting, PhysicalExecutions: state.PhysicalExecutions, Events: events, Tasks: ports, Workers: ports, MCP: ports, Credentials: ports, Bindings: phasemcp.NewBindingRegistry(), Leases: ports}
}

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

// staleSecondReadWaitingStore makes the coordinator obtain a stale expected
// revision after validateCurrentExecution has already observed the current
// record. The durable transaction must reject it without any teardown.
type staleSecondReadWaitingStore struct {
	WaitingStore
	reads int
}

func (s *staleSecondReadWaitingStore) Get(ctx context.Context, key WaitingKey) (WaitingRecord, bool, error) {
	record, found, err := s.WaitingStore.Get(ctx, key)
	if err != nil || !found {
		return record, found, err
	}
	s.reads++
	if s.reads == 2 && record.Revision > 0 {
		record.Revision--
	}
	return record, found, nil
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

func TestDurableCompletionSeamAcceptsOnceAndFencesLegacyEventPath(t *testing.T) {
	c, repo, ports, events := durableCompletionFixture(t)
	defer repo.Close()
	output := phaseagent.PhaseOutput{Phase: phaseagent.PhaseExecute, ReportRef: "report"}
	if err := c.SubmitPhaseOutput(context.Background(), output); err != nil {
		t.Fatal(err)
	}
	if len(events.values) != 0 {
		t.Fatalf("legacy event path invoked: %#v", events.values)
	}
	if len(ports.calls) != 5 {
		t.Fatalf("cleanup=%v", ports.calls)
	}
	if err := c.SubmitPhaseOutput(context.Background(), output); err != nil {
		t.Fatal(err)
	}
	if len(ports.calls) != 5 {
		t.Fatalf("duplicate cleanup=%v", ports.calls)
	}
	if err := c.SubmitPhaseOutput(context.Background(), phaseagent.PhaseOutput{Phase: phaseagent.PhaseExecute, ReportRef: "different"}); !errors.Is(err, ErrConflictingOutput) {
		t.Fatalf("conflict=%v", err)
	}
	eventsOut, err := repo.ListRuntimeEvents(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range eventsOut {
		if e.EventType == "PhaseOutputSubmitted" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("durable events=%d", n)
	}
}

func TestDurableCompletionSeamRejectsStaleAuthorityBeforeTeardown(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*PhaseOutputCompletionCoordinator)
	}{
		{"binding", func(c *PhaseOutputCompletionCoordinator) { c.Binding.BindingRef = "stale" }},
		{"input", func(c *PhaseOutputCompletionCoordinator) { c.Binding.InputRevision = "stale" }},
		{"generation", func(c *PhaseOutputCompletionCoordinator) { c.Binding.Generation++ }},
		{"epoch", func(c *PhaseOutputCompletionCoordinator) { c.Binding.ExecutionEpoch++ }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, repo, ports, _ := durableCompletionFixture(t)
			defer repo.Close()
			tc.mutate(c)
			if err := c.SubmitPhaseOutput(context.Background(), phaseagent.PhaseOutput{Phase: phaseagent.PhaseExecute, ReportRef: "report"}); err == nil {
				t.Fatal("stale authority accepted")
			}
			if len(ports.calls) != 0 {
				t.Fatalf("teardown=%v", ports.calls)
			}
			if _, found, err := repo.PhaseOutputStore().Get(context.Background(), PhaseOutputKey{TaskID: c.Binding.TaskID, InvocationID: c.Binding.InvocationID, Generation: c.Binding.Generation}); err != nil || found {
				t.Fatalf("stale authority persisted output found=%t err=%v", found, err)
			}
			waiting, found, err := repo.WaitingStore().Get(context.Background(), WaitingKey{TaskID: "task-d", InvocationID: "inv-d", Generation: 3})
			if err != nil || !found || waiting.State != AwaitStateRunning {
				t.Fatalf("stale authority changed waiting=%#v err=%v", waiting, err)
			}
			events, err := repo.ListRuntimeEvents(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			for _, e := range events {
				if e.EventType == "PhaseOutputSubmitted" {
					t.Fatal("success outbox recorded")
				}
			}
		})
	}
}

func TestDurableCompletionSeamRejectsStaleWaitingRevisionBeforeTeardown(t *testing.T) {
	c, repo, ports, _ := durableCompletionFixture(t)
	defer repo.Close()
	key := WaitingKey{TaskID: "task-d", InvocationID: "inv-d", Generation: 3}
	c.Waiting = &staleSecondReadWaitingStore{WaitingStore: c.Waiting}
	if err := c.SubmitPhaseOutput(context.Background(), phaseagent.PhaseOutput{Phase: phaseagent.PhaseExecute, ReportRef: "report"}); !errors.Is(err, ErrCompletionConflict) {
		t.Fatal("stale revision accepted")
	}
	if len(ports.calls) != 0 {
		t.Fatalf("teardown=%v", ports.calls)
	}
	waiting, found, err := repo.WaitingStore().Get(context.Background(), key)
	if err != nil || !found || waiting.State != AwaitStateRunning {
		t.Fatalf("stale revision changed waiting: %#v err=%v", waiting, err)
	}
	if _, found, err = repo.PhaseOutputStore().Get(context.Background(), PhaseOutputKey{TaskID: key.TaskID, InvocationID: key.InvocationID, Generation: key.Generation}); err != nil || found {
		t.Fatalf("stale revision persisted output found=%t err=%v", found, err)
	}
}

func TestDurableCompletionSeamOutboxFailureRollsBackAndDoesNotTeardown(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "runtime.db")
	c, repo, ports, events := durableCompletionFixtureAt(t, databasePath)
	if _, err := repo.db.Exec("CREATE TRIGGER reject_output BEFORE INSERT ON runtime_events WHEN NEW.event_type='PhaseOutputSubmitted' BEGIN SELECT RAISE(ABORT, 'outbox unavailable'); END"); err != nil {
		t.Fatal(err)
	}
	output := phaseagent.PhaseOutput{Phase: phaseagent.PhaseExecute, ReportRef: "report"}
	if err := c.SubmitPhaseOutput(context.Background(), output); err == nil {
		t.Fatal("outbox failure accepted output")
	}
	key := WaitingKey{TaskID: "task-d", InvocationID: "inv-d", Generation: 3}
	waiting, found, err := repo.WaitingStore().Get(context.Background(), key)
	if err != nil || !found || waiting.State != AwaitStateRunning {
		t.Fatalf("outbox failure left waiting=%#v err=%v", waiting, err)
	}
	if _, found, err = repo.PhaseOutputStore().Get(context.Background(), PhaseOutputKey{TaskID: key.TaskID, InvocationID: key.InvocationID, Generation: key.Generation}); err != nil || found {
		t.Fatalf("outbox failure left output found=%t err=%v", found, err)
	}
	if len(ports.calls) != 0 || len(events.values) != 0 {
		t.Fatalf("failure started teardown or legacy events: calls=%v events=%v", ports.calls, events.values)
	}
	runtimeEvents, err := repo.ListRuntimeEvents(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range runtimeEvents {
		if event.EventType == "PhaseOutputSubmitted" {
			t.Fatal("outbox failure left success event")
		}
	}
	if err = repo.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenSQLiteRuntimeStateRepository(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	waiting, found, err = reopened.WaitingStore().Get(context.Background(), key)
	if err != nil || !found || waiting.State != AwaitStateRunning {
		t.Fatalf("reopened rollback waiting=%#v err=%v", waiting, err)
	}
	if _, found, err = reopened.PhaseOutputStore().Get(context.Background(), PhaseOutputKey{TaskID: key.TaskID, InvocationID: key.InvocationID, Generation: key.Generation}); err != nil || found {
		t.Fatalf("reopened rollback output found=%t err=%v", found, err)
	}
	if _, err = reopened.db.Exec("DROP TRIGGER reject_output"); err != nil {
		t.Fatal(err)
	}
	state, err := NewDurableLifecycleState(reopened)
	if err != nil {
		t.Fatal(err)
	}
	retryPorts, retryEvents := &completionPorts{}, &completionEvents{}
	c = durableCompletionCoordinator(state, key, phaseagent.PhaseEndpointRef{TaskID: key.TaskID, EndpointID: "execute"}, retryPorts, retryEvents)
	if err = c.SubmitPhaseOutput(context.Background(), output); err != nil {
		t.Fatalf("retry after restored outbox: %v", err)
	}
	if len(retryPorts.calls) != 5 || len(retryEvents.values) != 0 {
		t.Fatalf("retry cleanup=%v legacy=%v", retryPorts.calls, retryEvents.values)
	}
}

func TestDurableCompletionAcceptanceSurvivesColdReopenWithoutReplay(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "runtime.db")
	c, repo, _, _ := durableCompletionFixtureAt(t, databasePath)
	output := phaseagent.PhaseOutput{Phase: phaseagent.PhaseExecute, ReportRef: "report"}
	if err := c.SubmitPhaseOutput(context.Background(), output); err != nil {
		t.Fatal(err)
	}
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenSQLiteRuntimeStateRepository(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	key := WaitingKey{TaskID: "task-d", InvocationID: "inv-d", Generation: 3}
	waiting, found, err := reopened.WaitingStore().Get(context.Background(), key)
	if err != nil || !found || waiting.State != AwaitStateTerminal {
		t.Fatalf("reopened waiting=%#v err=%v", waiting, err)
	}
	outputKey := PhaseOutputKey{TaskID: key.TaskID, InvocationID: key.InvocationID, Generation: key.Generation}
	recorded, found, err := reopened.PhaseOutputStore().Get(context.Background(), outputKey)
	if err != nil || !found || !reflect.DeepEqual(recorded.Output, output) {
		t.Fatalf("reopened output=%#v err=%v", recorded, err)
	}
	events, err := reopened.ListRuntimeEvents(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, event := range events {
		if event.EventType == "PhaseOutputSubmitted" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("reopened phase output events=%d", count)
	}
	_, _, created, err := reopened.LifecycleMutations().AcceptPhaseOutput(context.Background(), PhaseOutputRecord{Key: outputKey, BindingRef: "binding-b2", InputRevision: "input-r5", ExecutionEpoch: 2, Output: output}, key, waiting.Revision)
	if err != nil || created {
		t.Fatalf("reopened duplicate created=%t err=%v", created, err)
	}
}

func TestDurableCompletionSeamConcurrentAcceptanceHasOneEvent(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "runtime.db")
	c1, repo, ports1, events1 := durableCompletionFixtureAt(t, databasePath)
	defer repo.Close()
	state, err := NewDurableLifecycleState(repo)
	if err != nil {
		t.Fatal(err)
	}
	key := WaitingKey{TaskID: "task-d", InvocationID: "inv-d", Generation: 3}
	endpoint := phaseagent.PhaseEndpointRef{TaskID: key.TaskID, EndpointID: "execute"}
	ports2, events2 := &completionPorts{}, &completionEvents{}
	c2 := durableCompletionCoordinator(state, key, endpoint, ports2, events2)
	output := phaseagent.PhaseOutput{Phase: phaseagent.PhaseExecute, ReportRef: "report"}
	start := make(chan struct{})
	errs := make(chan error, 2)
	for _, coordinator := range []*PhaseOutputCompletionCoordinator{c1, c2} {
		go func(coordinator *PhaseOutputCompletionCoordinator) {
			<-start
			errs <- coordinator.SubmitPhaseOutput(context.Background(), output)
		}(coordinator)
	}
	close(start)
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("identical concurrent acceptance: %v", err)
		}
	}
	if len(ports1.calls)+len(ports2.calls) != 5 || len(events1.values)+len(events2.values) != 0 {
		t.Fatalf("duplicate effects ports=%v/%v legacy=%v/%v", ports1.calls, ports2.calls, events1.values, events2.values)
	}
	events, err := repo.ListRuntimeEvents(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, event := range events {
		if event.EventType == "PhaseOutputSubmitted" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("identical concurrent events=%d", count)
	}
}

func TestDurableCompletionSeamConcurrentConflictFailsClosed(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "runtime.db")
	c1, repo, ports1, _ := durableCompletionFixtureAt(t, databasePath)
	defer repo.Close()
	state, err := NewDurableLifecycleState(repo)
	if err != nil {
		t.Fatal(err)
	}
	key := WaitingKey{TaskID: "task-d", InvocationID: "inv-d", Generation: 3}
	ports2 := &completionPorts{}
	c2 := durableCompletionCoordinator(state, key, phaseagent.PhaseEndpointRef{TaskID: key.TaskID, EndpointID: "execute"}, ports2, &completionEvents{})
	start := make(chan struct{})
	type result struct{ err error }
	results := make(chan result, 2)
	go func() {
		<-start
		results <- result{c1.SubmitPhaseOutput(context.Background(), phaseagent.PhaseOutput{Phase: phaseagent.PhaseExecute, ReportRef: "one"})}
	}()
	go func() {
		<-start
		results <- result{c2.SubmitPhaseOutput(context.Background(), phaseagent.PhaseOutput{Phase: phaseagent.PhaseExecute, ReportRef: "two"})}
	}()
	close(start)
	var successes, conflicts int
	for range 2 {
		result := <-results
		if result.err == nil {
			successes++
		} else if errors.Is(result.err, ErrConflictingOutput) {
			conflicts++
		} else {
			t.Fatalf("unexpected concurrent conflict error: %v", result.err)
		}
	}
	if successes != 1 || conflicts != 1 || len(ports1.calls)+len(ports2.calls) != 5 {
		t.Fatalf("successes=%d conflicts=%d cleanup=%v/%v", successes, conflicts, ports1.calls, ports2.calls)
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
