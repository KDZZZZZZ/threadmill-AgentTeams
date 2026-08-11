package coordination

import (
	"context"
	"sync"
	"testing"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

func TestPhaseObservationWriterAppendsOnlyAllowedKindsAndIsIdempotent(t *testing.T) {
	graph, decisions, store := newGraphHarness()
	revision := createTask(t, graph, decisions, "task-a")
	controller := &recordingController{}
	runtime := newGraphRuntime(projectID, store, controller)
	if err := runtime.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	start := controller.lastCommand()
	if err := store.RecordPhaseInvocationStarted(context.Background(), projectID, start); err != nil {
		t.Fatalf("record started: %v", err)
	}
	if err := store.RecordPhaseInvocationStarted(context.Background(), projectID, start); err != nil {
		t.Fatalf("idempotent started: %v", err)
	}
	if err := runtime.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordPhaseInvocationStarted(context.Background(), projectID, start); err != nil {
		t.Fatalf("folded started replay should remain idempotent: %v", err)
	}

	registerTransition(t, decisions, "hold-for-observation-writer", GraphTransition{
		TargetKind: TargetPhaseEndpoint,
		Endpoint:   ref("task-a", EndpointPlan),
		Action:     "held",
		Generation: 1,
	})
	revision = mustTransition(t, graph, revision, "hold-for-observation-writer")
	if revision == 0 {
		t.Fatal("unused revision guard")
	}
	if err := runtime.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	stop := controller.lastCommand()
	if stop.Action != CommandStop {
		t.Fatalf("last command = %#v, want stop", stop)
	}
	if err := store.RecordPhaseInvocationStopped(context.Background(), projectID, stop, "checkpoint://task-a/plan/1", false); err != nil {
		t.Fatalf("record stopped: %v", err)
	}
	if err := store.RecordPhaseInvocationStopped(context.Background(), projectID, stop, "checkpoint://task-a/plan/1", false); err != nil {
		t.Fatalf("idempotent stopped: %v", err)
	}
	const workers = 8
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- store.RecordPhaseInvocationStopped(context.Background(), projectID, stop, "checkpoint://task-a/plan/1", false)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent idempotent stopped: %v", err)
		}
	}
	if err := store.RecordPhaseInvocationStopped(context.Background(), projectID, stop, "checkpoint://task-a/plan/other", false); !kernel.IsCode(err, kernel.CodeIdempotencyConflict) {
		t.Fatalf("conflicting stopped observation = %v, want idempotency_conflict", err)
	}
}

func TestPhaseObservationWriterRejectsWrongCommandActionAndIncompleteStop(t *testing.T) {
	graph, decisions, store := newGraphHarness()
	_ = createTask(t, graph, decisions, "task-a")
	command := PhaseCommand{
		ID:         "cmd-stop-without-evidence",
		Endpoint:   ref("task-a", EndpointPlan),
		Generation: 1,
		BindingRef: "binding://task-a/plan/1",
		LeaseRef:   "lease:task-a:plan:1",
		Action:     CommandStop,
		CauseRef:   "test",
	}
	if err := store.RecordPhaseInvocationStarted(context.Background(), projectID, command); !kernel.IsCode(err, kernel.CodeStaleCommand) {
		t.Fatalf("started with stop command = %v, want stale_command", err)
	}
	if err := store.RecordPhaseInvocationStopped(context.Background(), projectID, command, "", false); !kernel.IsCode(err, kernel.CodeIncompleteStopEvidence) {
		t.Fatalf("stopped without evidence = %v, want incomplete_stop_evidence", err)
	}
}
