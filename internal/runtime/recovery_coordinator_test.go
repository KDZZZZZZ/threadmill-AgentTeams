package runtime

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/executionreceipt"
	phasemcp "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/mcp/phase"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/phaseagent"
)

type recoveryCleanupPorts struct {
	mu       sync.Mutex
	calls    []TeardownStep
	failStep TeardownStep
	after    func(TeardownStep)
}

func (p *recoveryCleanupPorts) record(step TeardownStep) error {
	p.mu.Lock()
	p.calls = append(p.calls, step)
	fail := p.failStep == step
	if fail {
		p.failStep = ""
	}
	after := p.after
	p.mu.Unlock()
	if after != nil {
		after(step)
	}
	if fail {
		return errors.New("test-only cleanup failure")
	}
	return nil
}
func (p *recoveryCleanupPorts) CompleteTeamHarnessTask(context.Context, TeamHarnessTask) error {
	return p.record(TeardownStepTask)
}
func (p *recoveryCleanupPorts) DeleteWorker(context.Context, ProvisionedWorker) error {
	return p.record(TeardownStepWorker)
}
func (p *recoveryCleanupPorts) CleanupWorkerMCP(context.Context, ProvisionedWorker) error {
	return p.record(TeardownStepMCP)
}
func (p *recoveryCleanupPorts) RevokeMCPCredential(context.Context, MCPCredentialBinding) error {
	return p.record(TeardownStepCredential)
}
func (p *recoveryCleanupPorts) ReleaseWorkspaceLease(context.Context, WorkspaceLease) error {
	return p.record(TeardownStepLease)
}
func (p *recoveryCleanupPorts) CreateMCPCredential(context.Context, MCPCredentialRequest) (MCPCredentialBinding, error) {
	return MCPCredentialBinding{}, errors.New("not used")
}
func (p *recoveryCleanupPorts) ProvisionWorker(context.Context, WorkerProvisionRequest) (ProvisionedWorker, error) {
	return ProvisionedWorker{}, errors.New("not used")
}
func (p *recoveryCleanupPorts) AcquireWorkspaceLease(context.Context, RehydrationPlan) (WorkspaceLease, error) {
	return WorkspaceLease{}, errors.New("not used")
}
func (p *recoveryCleanupPorts) count(step TeardownStep) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, value := range p.calls {
		if value == step {
			n++
		}
	}
	return n
}

type failTeardownMutation struct {
	LifecycleMutationStore
	step TeardownStep
	once sync.Once
}

type observingPhysicalExecution struct {
	mu      sync.Mutex
	value   PhysicalExecutionObservation
	err     error
	calls   int
	after   func()
	request PhysicalExecutionObservationRequest
}

func (o *observingPhysicalExecution) Observe(_ context.Context, request PhysicalExecutionObservationRequest) (PhysicalExecutionObservation, error) {
	o.mu.Lock()
	o.calls++
	o.request = request
	after := o.after
	value, err := o.value, o.err
	o.mu.Unlock()
	if after != nil {
		after()
	}
	return value, err
}

func (s *failTeardownMutation) AdvanceTeardown(ctx context.Context, key PhysicalExecutionKey, revision int64, step TeardownStep) (PhysicalExecution, bool, error) {
	if step == s.step {
		failed := false
		s.once.Do(func() { failed = true })
		if failed {
			return PhysicalExecution{}, false, errors.New("test-only crash before teardown progress")
		}
	}
	return s.LifecycleMutationStore.AdvanceTeardown(ctx, key, revision, step)
}

func acceptedTerminalRecoveryFixture(t *testing.T, path string) (*SQLiteRuntimeStateRepository, WaitingKey, PhysicalExecution) {
	t.Helper()
	ctx := context.Background()
	r, key, waiting, physical := recoveryFixture(t, path, AwaitStateRunning, PhysicalExecutionRunning)
	output := PhaseOutputRecord{Key: PhaseOutputKey{TaskID: key.TaskID, InvocationID: key.InvocationID, Generation: key.Generation}, BindingRef: physical.BindingRef, InputRevision: physical.InputRevision, ExecutionEpoch: physical.ExecutionEpoch, Output: phaseagent.PhaseOutput{ReportRef: "artifact-report"}}
	if _, _, created, err := r.LifecycleMutations().AcceptPhaseOutput(ctx, output, key, waiting.Revision); err != nil || !created {
		t.Fatalf("accept terminal output created=%t err=%v", created, err)
	}
	return r, key, physical
}

func activeRecoveryObservationFixture(t *testing.T, path string) (*SQLiteRuntimeStateRepository, WaitingKey, PhysicalExecution) {
	t.Helper()
	ctx := context.Background()
	r, key, _, physical := recoveryFixture(t, path, AwaitStateRunning, PhysicalExecutionRunning)
	physical.WorkerID = "tm-invocation-g2-e2"
	physical.WorkerName = physical.WorkerID
	physical.TeamHarnessTaskID = "tm-phase-invocation-g2-e2"
	physical.TeamHarnessAssignedTo = physical.WorkerName
	physical.AgentSessionRef = "matrix:!room:test"
	physical.AgentPackageDigest = "digest"
	physical.PackageConsumed = true
	physical.MCPClientID = "threadmill"
	physical.CredentialBindingRef = "credential"
	physical.DesiredRuntimeGeneration = 7
	physical.AppliedRuntimeGeneration = 7
	physical.WorkspaceLeaseRef = "lease"
	physical.ExecutionAuthorizationRef = "authorization-ref"
	physical.ObservedTaskStatus = "in_progress"
	physical.TaskAcknowledged = true
	updated, swapped, err := r.PhysicalExecutionStore().CompareAndSwap(ctx, physical.Key(), physical.Revision, physical)
	if err != nil || !swapped {
		t.Fatalf("seed active physical swapped=%t err=%v", swapped, err)
	}
	return r, key, updated
}

func newRecoveryCoordinator(r RuntimeStateRepository, owner string, ports *recoveryCleanupPorts, mutations LifecycleMutationStore) *RecoveryCoordinator {
	if mutations == nil {
		mutations = r.LifecycleMutations()
	}
	return &RecoveryCoordinator{
		Repository: r, Mutations: mutations, OwnerID: owner, ClaimTTL: time.Minute,
		Cleanup: TerminalRecoveryCleanupPorts{Tasks: ports, Workers: ports, MCP: ports, Credentials: ports, Leases: ports, Bindings: phasemcp.NewBindingRegistry()},
	}
}

func currentPhysical(t *testing.T, r RuntimeStateRepository, key WaitingKey, epoch ExecutionEpoch) PhysicalExecution {
	t.Helper()
	v, found, err := r.PhysicalExecutionStore().Get(context.Background(), PhysicalExecutionKey{TaskID: key.TaskID, InvocationID: key.InvocationID, Generation: key.Generation, ExecutionEpoch: epoch})
	if err != nil || !found {
		t.Fatalf("physical found=%t err=%v", found, err)
	}
	return v
}

func countRuntimeEvents(t *testing.T, r RuntimeStateRepository, typ string) int {
	t.Helper()
	events, err := r.ListRuntimeEvents(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, event := range events {
		if event.EventType == typ {
			n++
		}
	}
	return n
}

func TestRecoveryCoordinatorContinuesAcceptedTerminalTeardownAfterColdReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "runtime.db")
	r, key, physical := acceptedTerminalRecoveryFixture(t, path)
	ports := &recoveryCleanupPorts{}
	failing := &failTeardownMutation{LifecycleMutationStore: r.LifecycleMutations(), step: TeardownStepTask}
	if err := newRecoveryCoordinator(r, "runtime-a", ports, failing).Reconcile(ctx, key); err == nil {
		t.Fatal("expected simulated crash before task progress")
	}
	if got := currentPhysical(t, r, key, physical.ExecutionEpoch); got.State != PhysicalExecutionTearingDown || got.Teardown.TeamHarnessTaskCancelled {
		t.Fatalf("partial physical=%#v", got)
	}
	if ports.count(TeardownStepTask) != 1 {
		t.Fatalf("task cleanup calls=%d", ports.count(TeardownStepTask))
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := OpenSQLiteRuntimeStateRepository(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err = newRecoveryCoordinator(r, "runtime-b", ports, nil).Reconcile(ctx, key); err != nil {
		t.Fatal(err)
	}
	if got := currentPhysical(t, r, key, physical.ExecutionEpoch); got.State != PhysicalExecutionTerminated || !allTeardownStepsDone(got.Teardown) {
		t.Fatalf("recovered physical=%#v", got)
	}
	if ports.count(TeardownStepTask) != 2 {
		t.Fatalf("task cleanup after crash should repeat idempotently, calls=%d", ports.count(TeardownStepTask))
	}
	if got := countRuntimeEvents(t, r, "PhaseOutputSubmitted"); got != 1 {
		t.Fatalf("output was replayed, events=%d", got)
	}
	if values, err := r.PhysicalExecutionStore().ListByInvocation(ctx, key.TaskID, key.InvocationID, key.Generation); err != nil || len(values) != 1 || values[0].ExecutionEpoch != physical.ExecutionEpoch {
		t.Fatalf("terminal recovery changed epoch history: %#v err=%v", values, err)
	}
}

func TestRecoveryCoordinatorSkipsCommittedStepsAndTerminalNoOp(t *testing.T) {
	ctx := context.Background()
	r, key, physical := acceptedTerminalRecoveryFixture(t, filepath.Join(t.TempDir(), "runtime.db"))
	defer r.Close()
	ports := &recoveryCleanupPorts{}
	coordinator := newRecoveryCoordinator(r, "runtime-a", ports, nil)
	if err := coordinator.Reconcile(ctx, key); err != nil {
		t.Fatal(err)
	}
	if got := currentPhysical(t, r, key, physical.ExecutionEpoch); got.State != PhysicalExecutionTerminated {
		t.Fatalf("physical=%#v", got)
	}
	firstTaskCalls := ports.count(TeardownStepTask)
	firstEvents := countRuntimeEvents(t, r, "PhysicalExecutionTerminated")
	if err := coordinator.Reconcile(ctx, key); err != nil {
		t.Fatal(err)
	}
	if ports.count(TeardownStepTask) != firstTaskCalls || countRuntimeEvents(t, r, "PhysicalExecutionTerminated") != firstEvents {
		t.Fatal("terminal no-op repeated cleanup or event")
	}
}

func TestRecoveryCoordinatorFailureOutboxAndSnapshotFenceDoNotAdvanceProgress(t *testing.T) {
	ctx := context.Background()
	t.Run("cleanup failure", func(t *testing.T) {
		r, key, physical := acceptedTerminalRecoveryFixture(t, filepath.Join(t.TempDir(), "runtime.db"))
		defer r.Close()
		ports := &recoveryCleanupPorts{failStep: TeardownStepTask}
		if err := newRecoveryCoordinator(r, "runtime-a", ports, nil).Reconcile(ctx, key); err == nil {
			t.Fatal("expected cleanup failure")
		}
		if got := currentPhysical(t, r, key, physical.ExecutionEpoch); got.Teardown.TeamHarnessTaskCancelled || got.State != PhysicalExecutionTearingDown {
			t.Fatalf("cleanup failure advanced durable progress: %#v", got)
		}
		if err := newRecoveryCoordinator(r, "runtime-b", ports, nil).Reconcile(ctx, key); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("outbox failure", func(t *testing.T) {
		r, key, physical := acceptedTerminalRecoveryFixture(t, filepath.Join(t.TempDir(), "runtime.db"))
		defer r.Close()
		if _, err := r.db.Exec("CREATE TRIGGER reject_terminal_recovery BEFORE INSERT ON runtime_events WHEN NEW.event_type='PhysicalExecutionTeardownStepCompleted' BEGIN SELECT RAISE(ABORT, 'outbox unavailable'); END"); err != nil {
			t.Fatal(err)
		}
		ports := &recoveryCleanupPorts{}
		if err := newRecoveryCoordinator(r, "runtime-a", ports, nil).Reconcile(ctx, key); err == nil {
			t.Fatal("expected outbox failure")
		}
		if got := currentPhysical(t, r, key, physical.ExecutionEpoch); got.Teardown.TeamHarnessTaskCancelled {
			t.Fatalf("outbox failure committed task progress: %#v", got)
		}
		if got := countRuntimeEvents(t, r, "PhysicalExecutionTeardownStepCompleted"); got != 0 {
			t.Fatalf("outbox failure left event=%d", got)
		}
	})
}

func TestRecoveryCoordinatorClaimAndSnapshotFences(t *testing.T) {
	ctx := context.Background()
	t.Run("expired claimant cannot commit after external success", func(t *testing.T) {
		r, key, physical := acceptedTerminalRecoveryFixture(t, filepath.Join(t.TempDir(), "runtime.db"))
		defer r.Close()
		clock := &fixedRecoveryClock{now: time.Date(2026, 8, 22, 0, 0, 0, 0, time.UTC)}
		r.clock = clock
		expiredOnce := false
		ports := &recoveryCleanupPorts{after: func(step TeardownStep) {
			if step == TeardownStepTask && !expiredOnce {
				expiredOnce = true
				clock.now = clock.now.Add(2 * time.Minute)
			}
		}}
		first := newRecoveryCoordinator(r, "runtime-a", ports, nil)
		first.now = clock.Now
		if err := first.Reconcile(ctx, key); !errors.Is(err, ErrRecoveryClaimLost) {
			t.Fatalf("expired owner err=%v", err)
		}
		if got := currentPhysical(t, r, key, physical.ExecutionEpoch); got.Teardown.TeamHarnessTaskCancelled {
			t.Fatalf("stale claimant committed progress: %#v", got)
		}
		second := newRecoveryCoordinator(r, "runtime-b", ports, nil)
		second.now = clock.Now
		if err := second.Reconcile(ctx, key); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("stale snapshot cannot commit", func(t *testing.T) {
		r, key, physical := acceptedTerminalRecoveryFixture(t, filepath.Join(t.TempDir(), "runtime.db"))
		defer r.Close()
		ports := &recoveryCleanupPorts{after: func(step TeardownStep) {
			if step != TeardownStepTask {
				return
			}
			value := currentPhysical(t, r, key, physical.ExecutionEpoch)
			if _, ok, err := r.PhysicalExecutionStore().CompareAndSwap(context.Background(), value.Key(), value.Revision, value); err != nil || !ok {
				t.Errorf("test stale mutation ok=%t err=%v", ok, err)
			}
		}}
		if err := newRecoveryCoordinator(r, "runtime-a", ports, nil).Reconcile(ctx, key); !errors.Is(err, ErrRecoverySnapshotStale) {
			t.Fatalf("stale snapshot err=%v", err)
		}
		if got := currentPhysical(t, r, key, physical.ExecutionEpoch); got.Teardown.TeamHarnessTaskCancelled {
			t.Fatalf("stale snapshot committed progress: %#v", got)
		}
	})
}

func TestRecoveryCoordinatorConcurrentClaimantAndUnsupportedDisposition(t *testing.T) {
	ctx := context.Background()
	t.Run("single claimant", func(t *testing.T) {
		r, key, _ := acceptedTerminalRecoveryFixture(t, filepath.Join(t.TempDir(), "runtime.db"))
		defer r.Close()
		started, release := make(chan struct{}), make(chan struct{})
		ports := &recoveryCleanupPorts{after: func(step TeardownStep) {
			if step == TeardownStepTask {
				close(started)
				<-release
			}
		}}
		first := newRecoveryCoordinator(r, "runtime-a", ports, nil)
		second := newRecoveryCoordinator(r, "runtime-b", ports, nil)
		result := make(chan error, 1)
		go func() { result <- first.Reconcile(ctx, key) }()
		<-started
		if err := second.Reconcile(ctx, key); !errors.Is(err, ErrRecoveryClaimed) {
			t.Fatalf("second claimant err=%v", err)
		}
		close(release)
		if err := <-result; err != nil {
			t.Fatal(err)
		}
	})
	t.Run("unsupported has no external effect", func(t *testing.T) {
		r, key, _, _ := recoveryFixture(t, filepath.Join(t.TempDir(), "runtime.db"), AwaitStateRunning, PhysicalExecutionRunning)
		defer r.Close()
		ports := &recoveryCleanupPorts{}
		if err := newRecoveryCoordinator(r, "runtime-a", ports, nil).Reconcile(ctx, key); !errors.Is(err, ErrRecoveryDispositionUnsupported) {
			t.Fatalf("unsupported err=%v", err)
		}
		if len(ports.calls) != 0 {
			t.Fatalf("unsupported disposition ran cleanup: %#v", ports.calls)
		}
	})
}

func TestRecoveryCoordinatorColdReopenRepeatsOnlyUncommittedExternalStep(t *testing.T) {
	ctx := context.Background()
	for _, target := range []TeardownStep{TeardownStepTask, TeardownStepWorker, TeardownStepMCP, TeardownStepCredential, TeardownStepToken, TeardownStepLease} {
		t.Run(string(target), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "runtime.db")
			r, key, physical := acceptedTerminalRecoveryFixture(t, path)
			ports := &recoveryCleanupPorts{}
			if err := newRecoveryCoordinator(r, "runtime-a", ports, &failTeardownMutation{LifecycleMutationStore: r.LifecycleMutations(), step: target}).Reconcile(ctx, key); err == nil {
				t.Fatalf("expected simulated crash before %s progress", target)
			}
			partial := currentPhysical(t, r, key, physical.ExecutionEpoch)
			if teardownStepDone(partial.Teardown, target) {
				t.Fatalf("%s progress committed across simulated crash: %#v", target, partial)
			}
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}
			r, err := OpenSQLiteRuntimeStateRepository(path)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			if err = newRecoveryCoordinator(r, "runtime-b", ports, nil).Reconcile(ctx, key); err != nil {
				t.Fatal(err)
			}
			if got := currentPhysical(t, r, key, physical.ExecutionEpoch); got.State != PhysicalExecutionTerminated || !allTeardownStepsDone(got.Teardown) {
				t.Fatalf("terminal physical=%#v", got)
			}
			// Token revocation has no retained token value, but it is still a
			// first-class durable step. All other side effects are observable by
			// these test ports and must be repeated only at the interrupted step.
			if target != TeardownStepToken && ports.count(target) != 2 {
				t.Fatalf("%s cleanup calls=%d want 2", target, ports.count(target))
			}
		})
	}
}

func TestRecoveryCoordinatorColdReopenSkipsCommittedProgressAndFinishesTermination(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "runtime.db")
	r, key, physical := acceptedTerminalRecoveryFixture(t, path)
	ports := &recoveryCleanupPorts{}
	// Simulate a crash after the task completion transaction has committed.
	value, _, err := r.LifecycleMutations().AdvanceTeardown(ctx, physical.Key(), physical.Revision, TeardownStepBegin)
	if err != nil {
		t.Fatal(err)
	}
	value, _, err = r.LifecycleMutations().AdvanceTeardown(ctx, physical.Key(), value.Revision, TeardownStepTask)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	r, err = OpenSQLiteRuntimeStateRepository(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err = newRecoveryCoordinator(r, "runtime-b", ports, nil).Reconcile(ctx, key); err != nil {
		t.Fatal(err)
	}
	if ports.count(TeardownStepTask) != 0 {
		t.Fatalf("committed task step repeated after reopen: %d", ports.count(TeardownStepTask))
	}
	if got := currentPhysical(t, r, key, physical.ExecutionEpoch); got.State != PhysicalExecutionTerminated {
		t.Fatalf("final termination missing: %#v", got)
	}
}

func TestRecoveryCoordinatorColdReopenFinishesOnlyFinalTermination(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "runtime.db")
	r, key, physical := acceptedTerminalRecoveryFixture(t, path)
	ports := &recoveryCleanupPorts{}
	value := physical
	var err error
	for _, step := range []TeardownStep{TeardownStepBegin, TeardownStepTask, TeardownStepWorker, TeardownStepMCP, TeardownStepCredential, TeardownStepToken, TeardownStepLease} {
		value, _, err = r.LifecycleMutations().AdvanceTeardown(ctx, value.Key(), value.Revision, step)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	r, err = OpenSQLiteRuntimeStateRepository(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err = newRecoveryCoordinator(r, "runtime-b", ports, nil).Reconcile(ctx, key); err != nil {
		t.Fatal(err)
	}
	if len(ports.calls) != 0 {
		t.Fatalf("recovery before final CAS reran cleanup: %#v", ports.calls)
	}
	if got := currentPhysical(t, r, key, physical.ExecutionEpoch); got.State != PhysicalExecutionTerminated {
		t.Fatalf("final termination missing: %#v", got)
	}
}

func TestRecoveryCoordinatorUsesBindingIdentityWithoutPersistingRawToken(t *testing.T) {
	ctx := context.Background()
	r, key, physical := acceptedTerminalRecoveryFixture(t, filepath.Join(t.TempDir(), "runtime.db"))
	defer r.Close()
	registry := phasemcp.NewBindingRegistry()
	issued, err := registry.Issue(phasemcp.BoundServices{Binding: phasemcp.InvocationBinding{
		TaskID: key.TaskID, InvocationID: key.InvocationID, Generation: key.Generation, ExecutionEpoch: int64(physical.ExecutionEpoch), BindingRef: physical.BindingRef, InputRevision: physical.InputRevision,
	}, Runtime: noopRuntime{}, Reader: noopReader{}, Agent: noopAgent{}})
	if err != nil {
		t.Fatal(err)
	}
	ports := &recoveryCleanupPorts{}
	coordinator := newRecoveryCoordinator(r, "runtime-a", ports, nil)
	coordinator.Cleanup.Bindings = registry
	if err = coordinator.Reconcile(ctx, key); err != nil {
		t.Fatal(err)
	}
	if _, err = registry.Resolve(issued.Token); !errors.Is(err, phasemcp.ErrInvalidToken) {
		t.Fatalf("trusted identity revoke left old token usable: %v", err)
	}
	var payload string
	if err = r.db.QueryRow("SELECT payload FROM runtime_physical_executions WHERE task_id=? AND invocation_id=? AND generation=? AND execution_epoch=?", key.TaskID, key.InvocationID, key.Generation, physical.ExecutionEpoch).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(payload, issued.Token) {
		t.Fatal("raw execution token was persisted")
	}
}

func TestRecoveryCoordinatorOutboxFailureLeavesNoProgressAfterColdReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "runtime.db")
	r, key, physical := acceptedTerminalRecoveryFixture(t, path)
	if _, err := r.db.Exec("CREATE TRIGGER reject_terminal_recovery_reopen BEFORE INSERT ON runtime_events WHEN NEW.event_type='PhysicalExecutionTeardownStepCompleted' BEGIN SELECT RAISE(ABORT, 'outbox unavailable'); END"); err != nil {
		t.Fatal(err)
	}
	ports := &recoveryCleanupPorts{}
	if err := newRecoveryCoordinator(r, "runtime-a", ports, nil).Reconcile(ctx, key); err == nil {
		t.Fatal("expected outbox failure")
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := OpenSQLiteRuntimeStateRepository(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if got := currentPhysical(t, r, key, physical.ExecutionEpoch); got.Teardown.TeamHarnessTaskCancelled || got.State != PhysicalExecutionTearingDown {
		t.Fatalf("outbox failure survived as progress: %#v", got)
	}
	if got := countRuntimeEvents(t, r, "PhysicalExecutionTeardownStepCompleted"); got != 0 {
		t.Fatalf("outbox failure survived event=%d", got)
	}
}

func TestRecoveryCoordinatorObservesActiveCarrierAfterColdReopenWithoutMutation(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "runtime.db")
	r, key, physical := activeRecoveryObservationFixture(t, path)
	beforeWaiting, found, err := r.WaitingStore().Get(ctx, key)
	if err != nil || !found {
		t.Fatal(err)
	}
	beforeEvents := countRuntimeEvents(t, r, "PhaseOutputSubmitted")
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	r, err = OpenSQLiteRuntimeStateRepository(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	observer := &observingPhysicalExecution{value: PhysicalExecutionObservation{ObservedAt: time.Now().UTC(), Worker: ObservedWorkerReady, Task: ObservedTaskInProgress, Runtime: ObservedRuntimeApplied, MCP: ObservedMCPApplied, Identity: ObservedCarrierIdentityVerified}}
	coordinator := newRecoveryCoordinator(r, "runtime-a", &recoveryCleanupPorts{}, nil)
	coordinator.Observer = observer
	result, err := coordinator.ObserveCarrier(ctx, key)
	if err != nil || result.Disposition != RecoveryCarrierActiveNeedsObservation || observer.calls != 1 {
		t.Fatalf("result=%#v calls=%d err=%v", result, observer.calls, err)
	}
	if observer.request.WorkerName != physical.WorkerName || observer.request.ExecutionEpoch != physical.ExecutionEpoch || observer.request.MCPClientID != physical.MCPClientID || observer.request.AgentSessionRef != physical.AgentSessionRef {
		t.Fatalf("request=%#v", observer.request)
	}
	afterWaiting, found, err := r.WaitingStore().Get(ctx, key)
	if err != nil || !found || !reflect.DeepEqual(afterWaiting, beforeWaiting) {
		t.Fatalf("observation mutated waiting before=%#v after=%#v err=%v", beforeWaiting, afterWaiting, err)
	}
	afterPhysical := currentPhysical(t, r, key, physical.ExecutionEpoch)
	if !reflect.DeepEqual(afterPhysical, physical) || countRuntimeEvents(t, r, "PhaseOutputSubmitted") != beforeEvents || observer.request.ExecutionEpoch != physical.ExecutionEpoch {
		t.Fatalf("observation mutated physical/events physical=%#v", afterPhysical)
	}
}

func TestRecoveryCoordinatorObservationFencesStaleClaimAndSnapshot(t *testing.T) {
	ctx := context.Background()
	t.Run("stale claim", func(t *testing.T) {
		r, key, _ := activeRecoveryObservationFixture(t, filepath.Join(t.TempDir(), "runtime.db"))
		defer r.Close()
		clock := &fixedRecoveryClock{now: time.Date(2026, 8, 22, 0, 0, 0, 0, time.UTC)}
		r.clock = clock
		observer := &observingPhysicalExecution{value: PhysicalExecutionObservation{}, after: func() { clock.now = clock.now.Add(2 * time.Minute) }}
		coordinator := newRecoveryCoordinator(r, "runtime-a", &recoveryCleanupPorts{}, nil)
		coordinator.Observer, coordinator.now = observer, clock.Now
		if _, err := coordinator.ObserveCarrier(ctx, key); !errors.Is(err, ErrRecoveryClaimLost) {
			t.Fatalf("stale claim err=%v", err)
		}
	})
	t.Run("stale snapshot", func(t *testing.T) {
		r, key, physical := activeRecoveryObservationFixture(t, filepath.Join(t.TempDir(), "runtime.db"))
		defer r.Close()
		observer := &observingPhysicalExecution{value: PhysicalExecutionObservation{}, after: func() {
			value := currentPhysical(t, r, key, physical.ExecutionEpoch)
			if _, swapped, err := r.PhysicalExecutionStore().CompareAndSwap(context.Background(), value.Key(), value.Revision, value); err != nil || !swapped {
				t.Errorf("mutate snapshot swapped=%t err=%v", swapped, err)
			}
		}}
		coordinator := newRecoveryCoordinator(r, "runtime-a", &recoveryCleanupPorts{}, nil)
		coordinator.Observer = observer
		if _, err := coordinator.ObserveCarrier(ctx, key); !errors.Is(err, ErrRecoverySnapshotStale) {
			t.Fatalf("stale snapshot err=%v", err)
		}
	})
}

func TestRecoveryCoordinatorObservationSingleClaimantAndUnsupportedNoEffect(t *testing.T) {
	ctx := context.Background()
	r, key, _ := activeRecoveryObservationFixture(t, filepath.Join(t.TempDir(), "runtime.db"))
	defer r.Close()
	started, release := make(chan struct{}), make(chan struct{})
	firstObserver := &observingPhysicalExecution{after: func() { close(started); <-release }}
	first := newRecoveryCoordinator(r, "runtime-a", &recoveryCleanupPorts{}, nil)
	first.Observer = firstObserver
	secondObserver := &observingPhysicalExecution{}
	second := newRecoveryCoordinator(r, "runtime-b", &recoveryCleanupPorts{}, nil)
	second.Observer = secondObserver
	result := make(chan error, 1)
	go func() { _, err := first.ObserveCarrier(ctx, key); result <- err }()
	<-started
	if _, err := second.ObserveCarrier(ctx, key); !errors.Is(err, ErrRecoveryClaimed) || secondObserver.calls != 0 {
		t.Fatalf("second observation err=%v calls=%d", err, secondObserver.calls)
	}
	close(release)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryCoordinatorObservesConsumedCarrierWithoutSynthesizingOutput(t *testing.T) {
	ctx := context.Background()
	r, key, physical := activeRecoveryObservationFixture(t, filepath.Join(t.TempDir(), "runtime.db"))
	defer r.Close()
	if _, _, err := r.ReceiptStore().PutIfAbsent(ctx, executionreceipt.Receipt{TaskID: key.TaskID, InvocationID: key.InvocationID, Generation: key.Generation, ExecutionEpoch: int64(physical.ExecutionEpoch), BindingRef: physical.BindingRef, InputRevision: physical.InputRevision, PackageDigest: "digest", SessionIdentity: physical.AgentSessionRef, Consumed: true}); err != nil {
		t.Fatal(err)
	}
	observer := &observingPhysicalExecution{value: PhysicalExecutionObservation{Worker: ObservedWorkerReady, Task: ObservedTaskCompleted, Runtime: ObservedRuntimeApplied, MCP: ObservedMCPApplied, Identity: ObservedCarrierIdentityVerified}}
	coordinator := newRecoveryCoordinator(r, "runtime-a", &recoveryCleanupPorts{}, nil)
	coordinator.Observer = observer
	result, err := coordinator.ObserveCarrier(ctx, key)
	if err != nil || result.Disposition != RecoveryCarrierConsumedNoOutput || result.Observation.Task != ObservedTaskCompleted {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	if _, found, err := r.PhaseOutputStore().Get(ctx, PhaseOutputKey{TaskID: key.TaskID, InvocationID: key.InvocationID, Generation: key.Generation}); err != nil || found {
		t.Fatalf("task observation synthesized phase output found=%t err=%v", found, err)
	}
}
