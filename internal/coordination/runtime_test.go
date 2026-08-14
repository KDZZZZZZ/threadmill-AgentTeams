package coordination

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

func TestGraphRuntimeExpiredLeaseStopsThroughPhaseController(t *testing.T) {
	graph, decisions, store := newGraphHarness()
	createTask(t, graph, decisions, "task-expired")
	now := time.Date(2026, 8, 11, 8, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	controller := &recordingController{}
	runtime := newGraphRuntime(projectID, store, controller)

	if err := runtime.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := controller.lastAction(); got != CommandStart {
		t.Fatalf("initial action = %s, want start", got)
	}

	now = now.Add(defaultPhaseLeaseTTL + time.Second)
	if err := runtime.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := controller.lastAction(); got != CommandStop {
		t.Fatalf("expired lease action = %s, want stop", got)
	}
	stop := controller.lastCommand()
	if stop.LeaseRef == "" || stop.Endpoint != ref("task-expired", EndpointPlan) {
		t.Fatalf("stop command = %#v, want expired plan lease", stop)
	}
}

func TestGraphRuntimeConcurrentReconcileClaimsOneRunAndLease(t *testing.T) {
	graph, decisions, store := newGraphHarness()
	revision := createTask(t, graph, decisions, "task-a")
	controller := &recordingController{}
	runtime := newGraphRuntime(projectID, store, controller)

	const workers = 100
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- runtime.reconcile(context.Background())
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("reconcile failed: %v", err)
		}
	}

	commands := store.runtimeCommands(context.Background(), projectID)
	if len(commands) != 1 {
		t.Fatalf("commands = %#v, want one run command", commands)
	}
	if commands[0].Action != CommandStart || commands[0].Endpoint != ref("task-a", EndpointPlan) || commands[0].Generation != 1 {
		t.Fatalf("command = %#v, want plan start generation 1", commands[0])
	}
	leases := store.runtimeLeases(context.Background(), projectID)
	active := 0
	for _, lease := range leases {
		if lease.State == "active" {
			active++
		}
	}
	if active != 1 {
		t.Fatalf("active leases = %d, want 1; all=%#v", active, leases)
	}
	if len(controller.commandsByID()) != 1 {
		t.Fatalf("controller commands = %#v, want same command id replays collapsed", controller.commandsByID())
	}
	if revision == 0 {
		t.Fatal("unused revision guard")
	}
}

func TestGraphRuntimeStopSuppressesPendingRunAndReleaseChoosesResume(t *testing.T) {
	graph, decisions, store := newGraphHarness()
	revision := createTask(t, graph, decisions, "task-a")
	controller := &recordingController{}
	runtime := newGraphRuntime(projectID, store, controller)

	if err := runtime.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	registerTransition(t, decisions, "hold-plan-runtime", GraphTransition{
		TargetKind: TargetPhaseEndpoint,
		Endpoint:   ref("task-a", EndpointPlan),
		Action:     "held",
		Generation: 1,
	})
	revision = mustTransition(t, graph, revision, "hold-plan-runtime")
	if err := runtime.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := controller.lastAction(); got != CommandStop {
		t.Fatalf("last action = %s, want stop", got)
	}
	stop := controller.lastCommand()
	if err := store.appendObservation(context.Background(), projectID, phaseObservation{
		ID:            "event-stopped",
		Kind:          "PhaseInvocationStopped",
		CommandID:     stop.ID,
		Endpoint:      stop.Endpoint,
		Generation:    stop.Generation,
		BindingRef:    stop.BindingRef,
		LeaseRef:      stop.LeaseRef,
		CheckpointRef: "checkpoint://task-a/plan/1",
	}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	registerTransition(t, decisions, "stopped-plan-runtime", GraphTransition{
		TargetKind:    TargetPhaseEndpoint,
		Endpoint:      ref("task-a", EndpointPlan),
		Action:        "stopped",
		Generation:    1,
		NewBindingRef: "binding://task-a/plan/2",
		CheckpointRef: "checkpoint://task-a/plan/1",
		EvidenceRefs:  []string{"event-stopped"},
	})
	revision = mustTransition(t, graph, revision, "stopped-plan-runtime")
	registerTransition(t, decisions, "release-plan-runtime", GraphTransition{
		TargetKind: TargetPhaseEndpoint,
		Endpoint:   ref("task-a", EndpointPlan),
		Action:     "released",
		Generation: 2,
	})
	mustTransition(t, graph, revision, "release-plan-runtime")
	if err := runtime.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := controller.lastAction(); got != CommandResume {
		t.Fatalf("last action after release = %s, want resume; commands=%#v", got, controller.commands)
	}
}

func TestGraphRuntimeDoesNotRedispatchStartedStartAfterStopCompletes(t *testing.T) {
	graph, decisions, store := newGraphHarness()
	revision := createTask(t, graph, decisions, "task-a")
	controller := &countingController{}
	runtime := newGraphRuntime(projectID, store, controller)

	if err := runtime.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	start := controller.lastCommand()
	if start.Action != CommandStart {
		t.Fatalf("initial command = %#v, want start", start)
	}
	appendStarted(t, store, "event-started-start", start)
	if err := runtime.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}

	registerTransition(t, decisions, "hold-started-start", GraphTransition{
		TargetKind: TargetPhaseEndpoint,
		Endpoint:   ref("task-a", EndpointPlan),
		Action:     "held",
		Generation: 1,
	})
	revision = mustTransition(t, graph, revision, "hold-started-start")
	if err := runtime.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	stop := controller.lastCommand()
	if stop.Action != CommandStop {
		t.Fatalf("command after hold = %#v, want stop; all=%#v", stop, controller.commands)
	}
	appendStopped(t, store, "event-stopped-start", stop, "checkpoint://task-a/plan/1")
	if err := runtime.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}

	if got := controller.count(start.ID); got != 1 {
		t.Fatalf("start command Apply count = %d, want 1; all=%#v", got, controller.commands)
	}
	if got := controller.count(stop.ID); got != 1 {
		t.Fatalf("stop command Apply count = %d, want 1; all=%#v", got, controller.commands)
	}
}

func TestGraphRuntimeDoesNotRedispatchStartedResumeAfterStopCompletes(t *testing.T) {
	graph, decisions, store := newGraphHarness()
	revision := createTask(t, graph, decisions, "task-a")
	controller := &countingController{}
	runtime := newGraphRuntime(projectID, store, controller)

	if err := runtime.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	start := controller.lastCommand()
	appendStarted(t, store, "event-started-before-resume", start)
	registerTransition(t, decisions, "hold-before-resume", GraphTransition{
		TargetKind: TargetPhaseEndpoint,
		Endpoint:   ref("task-a", EndpointPlan),
		Action:     "held",
		Generation: 1,
	})
	revision = mustTransition(t, graph, revision, "hold-before-resume")
	if err := runtime.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	firstStop := controller.lastCommand()
	appendStopped(t, store, "event-stopped-before-resume", firstStop, "checkpoint://task-a/plan/1")
	if err := runtime.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	registerTransition(t, decisions, "stopped-before-resume", GraphTransition{
		TargetKind:    TargetPhaseEndpoint,
		Endpoint:      ref("task-a", EndpointPlan),
		Action:        "stopped",
		Generation:    1,
		NewBindingRef: "binding://task-a/plan/2",
		CheckpointRef: "checkpoint://task-a/plan/1",
		EvidenceRefs:  []string{"event-stopped-before-resume"},
	})
	revision = mustTransition(t, graph, revision, "stopped-before-resume")
	registerTransition(t, decisions, "release-before-resume", GraphTransition{
		TargetKind: TargetPhaseEndpoint,
		Endpoint:   ref("task-a", EndpointPlan),
		Action:     "released",
		Generation: 2,
	})
	revision = mustTransition(t, graph, revision, "release-before-resume")
	if err := runtime.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	resume := controller.lastCommand()
	if resume.Action != CommandResume {
		t.Fatalf("command after release = %#v, want resume; all=%#v", resume, controller.commands)
	}
	appendStarted(t, store, "event-started-resume", resume)
	if err := runtime.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}

	registerTransition(t, decisions, "hold-started-resume", GraphTransition{
		TargetKind: TargetPhaseEndpoint,
		Endpoint:   ref("task-a", EndpointPlan),
		Action:     "held",
		Generation: 2,
	})
	revision = mustTransition(t, graph, revision, "hold-started-resume")
	if err := runtime.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	secondStop := controller.lastCommand()
	if secondStop.Action != CommandStop {
		t.Fatalf("command after holding resumed phase = %#v, want stop; all=%#v", secondStop, controller.commands)
	}
	appendStopped(t, store, "event-stopped-resume", secondStop, "checkpoint://task-a/plan/2")
	if err := runtime.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}

	if got := controller.count(resume.ID); got != 1 {
		t.Fatalf("resume command Apply count = %d, want 1; all=%#v", got, controller.commands)
	}
	if got := controller.count(secondStop.ID); got != 1 {
		t.Fatalf("second stop command Apply count = %d, want 1; all=%#v", got, controller.commands)
	}
}

func TestGraphRuntimeCheckpointFailClosedAndOrphanTerminalRejected(t *testing.T) {
	graph, decisions, store := newGraphHarness()
	revision := createTask(t, graph, decisions, "task-a")
	registerTransition(t, decisions, "hold-fail-closed", GraphTransition{
		TargetKind: TargetPhaseEndpoint,
		Endpoint:   ref("task-a", EndpointPlan),
		Action:     "held",
		Generation: 1,
	})
	revision = mustTransition(t, graph, revision, "hold-fail-closed")
	registerTransition(t, decisions, "stopped-nonresumable", GraphTransition{
		TargetKind:    TargetPhaseEndpoint,
		Endpoint:      ref("task-a", EndpointPlan),
		Action:        "stopped",
		Generation:    1,
		NewBindingRef: "binding://task-a/plan/2",
		NonResumable:  true,
		EvidenceRefs:  []string{"event-stop-hard"},
	})
	revision = mustTransition(t, graph, revision, "stopped-nonresumable")
	registerTransition(t, decisions, "release-nonresumable", GraphTransition{
		TargetKind: TargetPhaseEndpoint,
		Endpoint:   ref("task-a", EndpointPlan),
		Action:     "released",
		Generation: 2,
	})
	mustTransition(t, graph, revision, "release-nonresumable")
	controller := &recordingController{}
	runtime := newGraphRuntime(projectID, store, controller)
	if err := runtime.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(controller.commands) != 0 {
		t.Fatalf("nonresumable binding dispatched: %#v", controller.commands)
	}

	_ = createTask(t, graph, decisions, "task-b")
	err := store.appendObservation(context.Background(), projectID, phaseObservation{
		ID:         "event-terminal",
		Kind:       "PhaseInvocationFailed",
		CommandID:  "missing-command",
		Endpoint:   ref("task-b", EndpointPlan),
		Generation: 1,
		BindingRef: "binding://task-b/plan/1",
		LeaseRef:   "missing-lease",
	})
	if !kernel.IsCode(err, kernel.CodeStaleCommand) {
		t.Fatalf("orphan terminal observation error = %v, want stale_command", err)
	}
	if err := runtime.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(controller.commands) != 1 || controller.commands[0].Endpoint != ref("task-b", EndpointPlan) {
		t.Fatalf("orphan terminal observation suppressed runnable endpoint: %#v", controller.commands)
	}
}

func TestGraphRuntimeQuarantinesRejectedRunCommandsWithoutRedispatch(t *testing.T) {
	for _, code := range []kernel.ErrorCode{kernel.CodeStaleCommand, kernel.CodeLeaseConflict, kernel.CodeStaleCheckpoint} {
		t.Run(string(code), func(t *testing.T) {
			graph, decisions, store := newGraphHarness()
			_ = createTask(t, graph, decisions, "task-a")
			controller := &rejectingController{err: kernel.Error{Code: code, Message: "rejected", Recoverable: true}}
			runtime := newGraphRuntime(projectID, store, controller)

			if err := runtime.reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := runtime.reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			if controller.calls != 1 {
				t.Fatalf("Apply calls = %d, want 1", controller.calls)
			}
			store.mu.Lock()
			project := store.ensureProject(projectID)
			for _, record := range project.runtime.commands {
				if record.Command.Action == CommandStart && !record.NotExecutable {
					store.mu.Unlock()
					t.Fatalf("rejected command remained executable: %#v", record)
				}
			}
			for _, lease := range project.runtime.leases {
				if lease.State == "active" {
					store.mu.Unlock()
					t.Fatalf("pre-start rejected lease remained active: %#v", lease)
				}
			}
			store.mu.Unlock()
		})
	}
}

func TestGraphRuntimeRetriesExecutorUnavailableWithSameCommandAndLease(t *testing.T) {
	graph, decisions, store := newGraphHarness()
	_ = createTask(t, graph, decisions, "task-a")
	controller := &capacityRejectingController{}
	runtime := newGraphRuntime(projectID, store, controller)

	if err := runtime.reconcile(context.Background()); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	if err := runtime.reconcile(context.Background()); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if len(controller.commands) != 2 || controller.commands[0] != controller.commands[1] {
		t.Fatalf("capacity retry commands = %#v, want same command delivered twice", controller.commands)
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	project := store.ensureProject(projectID)
	if len(project.runtime.commands) != 1 {
		t.Fatalf("persisted commands = %d, want 1", len(project.runtime.commands))
	}
	for _, record := range project.runtime.commands {
		if record.Accepted || !record.RetryScheduled || record.NotExecutable || record.Quarantined {
			t.Fatalf("retry command state = %#v", record)
		}
	}
	activeLeases := 0
	for _, lease := range project.runtime.leases {
		if lease.State == "active" {
			activeLeases++
		}
	}
	if activeLeases != 1 {
		t.Fatalf("active leases = %d, want 1", activeLeases)
	}
}

func TestGraphRuntimeDoesNotRedispatchRejectedStopCommand(t *testing.T) {
	for _, code := range []kernel.ErrorCode{kernel.CodeStaleCommand, kernel.CodeLeaseConflict, kernel.CodeStaleCheckpoint} {
		t.Run(string(code), func(t *testing.T) {
			graph, decisions, store := newGraphHarness()
			revision := createTask(t, graph, decisions, "task-a")
			controller := &stopRejectingController{err: kernel.Error{Code: code, Message: "rejected", Recoverable: true}}
			runtime := newGraphRuntime(projectID, store, controller)
			if err := runtime.reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			registerTransition(t, decisions, "hold-rejected-stop-"+string(code), GraphTransition{
				TargetKind: TargetPhaseEndpoint,
				Endpoint:   ref("task-a", EndpointPlan),
				Action:     "held",
				Generation: 1,
			})
			_ = mustTransition(t, graph, revision, "hold-rejected-stop-"+string(code))
			if err := runtime.reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := runtime.reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			if controller.stopCalls != 1 {
				t.Fatalf("stop Apply calls = %d, want 1", controller.stopCalls)
			}
			store.mu.Lock()
			project := store.ensureProject(projectID)
			foundRejection := false
			for _, observation := range project.runtime.observations {
				if observation.Kind == "DispatchRejected" && observation.DispatchError == string(code) {
					foundRejection = true
				}
			}
			store.mu.Unlock()
			if !foundRejection {
				t.Fatal("rejected stop evidence was not retained")
			}
		})
	}
}

func TestReleasedTransitionCannotBypassActiveStopLease(t *testing.T) {
	graph, decisions, store := newGraphHarness()
	revision := createTask(t, graph, decisions, "task-a")
	controller := &recordingController{}
	runtime := newGraphRuntime(projectID, store, controller)
	if err := runtime.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	registerTransition(t, decisions, "hold-before-release", GraphTransition{
		TargetKind: TargetPhaseEndpoint,
		Endpoint:   ref("task-a", EndpointPlan),
		Action:     "held",
		Generation: 1,
	})
	revision = mustTransition(t, graph, revision, "hold-before-release")
	registerTransition(t, decisions, "release-with-active-lease", GraphTransition{
		TargetKind: TargetPhaseEndpoint,
		Endpoint:   ref("task-a", EndpointPlan),
		Action:     "released",
		Generation: 1,
	})
	if _, err := graph.Transition(context.Background(), revision, "release-with-active-lease"); !kernel.IsCode(err, kernel.CodeTransitionRejected) {
		t.Fatalf("release with active lease error = %v, want transition_rejected", err)
	}
	if err := runtime.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if controller.lastAction() != CommandStop {
		t.Fatalf("stop pressure disappeared after rejected release: %#v", controller.commands)
	}
}

func TestGraphRuntimeRepairsOrphanLeaseAndQuarantinesOrphanRunCommand(t *testing.T) {
	graph, decisions, store := newGraphHarness()
	_ = createTask(t, graph, decisions, "task-a")
	_, claimed, err := store.claimLeaseAndAppendCommand(context.Background(), projectID, latestRevision(t, graph), endpoint("task-a", EndpointPlan), CommandStart, "revision://2")
	if err != nil || !claimed {
		t.Fatalf("claim = %v/%v", claimed, err)
	}
	store.mu.Lock()
	project := store.ensureProject(projectID)
	for id := range project.runtime.commands {
		delete(project.runtime.commands, id)
	}
	store.mu.Unlock()

	controller := &recordingController{}
	runtime := newGraphRuntime(projectID, store, controller)
	runtime.schedulingStateProvider = fixedSchedulingStateProvider{state: RuntimeSchedulingState{Capacity: RuntimeCapacity{Desired: 0, Healthy: 0}}}
	if err := runtime.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, lease := range store.runtimeLeases(context.Background(), projectID) {
		if lease.State == "active" {
			t.Fatalf("orphan lease stayed active without remote observation: %#v", lease)
		}
	}

	_ = createTask(t, graph, decisions, "task-b")
	_, claimed, err = store.claimLeaseAndAppendCommand(context.Background(), projectID, latestRevision(t, graph), endpoint("task-b", EndpointPlan), CommandStart, "revision://3")
	if err != nil || !claimed {
		t.Fatalf("second claim = %v/%v", claimed, err)
	}
	store.mu.Lock()
	project = store.ensureProject(projectID)
	for id, lease := range project.runtime.leases {
		lease.State = "released"
		project.runtime.leases[id] = lease
	}
	store.mu.Unlock()
	if err := runtime.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(controller.commands) != 0 {
		t.Fatalf("orphan command was delivered: %#v", controller.commands)
	}
}

type recordingController struct {
	mu       sync.Mutex
	commands []PhaseCommand
}

type countingController struct {
	mu       sync.Mutex
	commands []PhaseCommand
}

type rejectingController struct {
	err   error
	calls int
}

type stopRejectingController struct {
	err       error
	stopCalls int
}

type capacityRejectingController struct {
	commands []PhaseCommand
}

func (c *capacityRejectingController) Apply(_ context.Context, command PhaseCommand) error {
	c.commands = append(c.commands, command)
	return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "capacity saturated", Recoverable: true}
}

func (c *stopRejectingController) Apply(_ context.Context, command PhaseCommand) error {
	if command.Action == CommandStop {
		c.stopCalls++
		return c.err
	}
	return nil
}

func (c *rejectingController) Apply(context.Context, PhaseCommand) error {
	c.calls++
	return c.err
}

func (c *recordingController) Apply(_ context.Context, command PhaseCommand) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, existing := range c.commands {
		if existing.ID == command.ID {
			if existing != command {
				return kernel.Error{Code: kernel.CodeCommandConflict, Message: "same command id with different payload"}
			}
			return nil
		}
	}
	c.commands = append(c.commands, command)
	return nil
}

func (c *recordingController) commandsByID() map[string]PhaseCommand {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]PhaseCommand, len(c.commands))
	for _, command := range c.commands {
		out[command.ID] = command
	}
	return out
}

func (c *recordingController) lastAction() CommandAction {
	return c.lastCommand().Action
}

func (c *recordingController) lastCommand() PhaseCommand {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.commands) == 0 {
		return PhaseCommand{}
	}
	return c.commands[len(c.commands)-1]
}

func (c *countingController) Apply(_ context.Context, command PhaseCommand) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.commands = append(c.commands, command)
	return nil
}

func (c *countingController) lastCommand() PhaseCommand {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.commands) == 0 {
		return PhaseCommand{}
	}
	return c.commands[len(c.commands)-1]
}

func (c *countingController) count(commandID string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	for _, command := range c.commands {
		if command.ID == commandID {
			count++
		}
	}
	return count
}

func appendStarted(t *testing.T, store *MemoryStore, eventID string, command PhaseCommand) {
	t.Helper()
	if err := store.appendObservation(context.Background(), projectID, phaseObservation{
		ID:         eventID,
		Kind:       "PhaseInvocationStarted",
		CommandID:  command.ID,
		Endpoint:   command.Endpoint,
		Generation: command.Generation,
		BindingRef: command.BindingRef,
		LeaseRef:   command.LeaseRef,
	}); err != nil {
		t.Fatal(err)
	}
}

func appendStopped(t *testing.T, store *MemoryStore, eventID string, command PhaseCommand, checkpointRef string) {
	t.Helper()
	if err := store.appendObservation(context.Background(), projectID, phaseObservation{
		ID:            eventID,
		Kind:          "PhaseInvocationStopped",
		CommandID:     command.ID,
		Endpoint:      command.Endpoint,
		Generation:    command.Generation,
		BindingRef:    command.BindingRef,
		LeaseRef:      command.LeaseRef,
		CheckpointRef: checkpointRef,
	}); err != nil {
		t.Fatal(err)
	}
}
