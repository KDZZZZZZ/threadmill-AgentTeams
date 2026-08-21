package runtime

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/phaseagent"
)

func TestActivatePhysicalExecutionAtomicallyTransitionsWaitingAndPhysical(t *testing.T) {
	ctx := context.Background()
	r, err := OpenSQLiteRuntimeStateRepository(filepath.Join(t.TempDir(), "runtime.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	key := WaitingKey{TaskID: "task", InvocationID: "inv", Generation: 1}
	w, err := r.WaitingStore().Create(ctx, WaitingRecord{Key: key, ExecutionEpoch: 1, Endpoint: phaseagent.PhaseEndpointRef{TaskID: "task", EndpointID: string(phaseagent.PhaseExecute)}, PreviousBindingRef: "b1", InputRevision: "r1", ContinuationRef: "c1", State: AwaitStateRehydrating})
	if err != nil {
		t.Fatal(err)
	}
	p, err := r.PhysicalExecutionStore().Create(ctx, PhysicalExecution{TaskID: "task", InvocationID: "inv", Generation: 1, ExecutionEpoch: 2, BindingRef: "b2", InputRevision: "r2", State: PhysicalExecutionAccepted, PackageConsumed: true})
	if err != nil {
		t.Fatal(err)
	}
	gotW, gotP, ok, err := r.LifecycleMutations().ActivatePhysicalExecution(ctx, key, w.Revision, p.Key(), p.Revision)
	if err != nil || !ok || gotW.State != AwaitStateRunning || gotW.ExecutionEpoch != 2 || gotW.PreviousBindingRef != "b2" || gotP.State != PhysicalExecutionRunning {
		t.Fatalf("activation waiting=%+v physical=%+v ok=%t err=%v", gotW, gotP, ok, err)
	}
	events, err := r.ListRuntimeEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range events {
		if e.EventType == "PhysicalExecutionActivated" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("activation events=%d", n)
	}
	_, _, duplicate, err := r.LifecycleMutations().ActivatePhysicalExecution(ctx, key, w.Revision, p.Key(), p.Revision)
	if err != nil || duplicate {
		t.Fatalf("duplicate ok=%t err=%v", duplicate, err)
	}
}

func TestActivatePhysicalExecutionOutboxFailureRollsBackBothRecords(t *testing.T) {
	ctx := context.Background()
	r, err := OpenSQLiteRuntimeStateRepository(filepath.Join(t.TempDir(), "runtime.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	key := WaitingKey{TaskID: "task", InvocationID: "inv", Generation: 1}
	w, err := r.WaitingStore().Create(ctx, WaitingRecord{Key: key, ExecutionEpoch: 1, Endpoint: phaseagent.PhaseEndpointRef{TaskID: "task", EndpointID: string(phaseagent.PhaseExecute)}, PreviousBindingRef: "b1", InputRevision: "r1", ContinuationRef: "c", State: AwaitStateRehydrating})
	if err != nil {
		t.Fatal(err)
	}
	p, err := r.PhysicalExecutionStore().Create(ctx, PhysicalExecution{TaskID: "task", InvocationID: "inv", Generation: 1, ExecutionEpoch: 2, BindingRef: "b2", InputRevision: "r2", State: PhysicalExecutionAccepted, PackageConsumed: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = r.db.Exec("CREATE TRIGGER reject_activation BEFORE INSERT ON runtime_events WHEN NEW.event_type='PhysicalExecutionActivated' BEGIN SELECT RAISE(ABORT, 'outbox unavailable'); END"); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err = r.LifecycleMutations().ActivatePhysicalExecution(ctx, key, w.Revision, p.Key(), p.Revision); err == nil {
		t.Fatal("activation committed despite outbox failure")
	}
	gw, _, _ := r.WaitingStore().Get(ctx, key)
	gp, _, _ := r.PhysicalExecutionStore().Get(ctx, p.Key())
	if gw.State != AwaitStateRehydrating || gp.State != PhysicalExecutionAccepted {
		t.Fatalf("partial commit waiting=%s physical=%s", gw.State, gp.State)
	}
}

func TestAcceptPhaseOutputAtomicallyTerminalsWaitingAndOutboxes(t *testing.T) {
	ctx := context.Background()
	r, err := OpenSQLiteRuntimeStateRepository(filepath.Join(t.TempDir(), "runtime.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	key := WaitingKey{TaskID: "task", InvocationID: "inv", Generation: 1}
	w, err := r.WaitingStore().Create(ctx, WaitingRecord{Key: key, ExecutionEpoch: 2, Endpoint: phaseagent.PhaseEndpointRef{TaskID: "task", EndpointID: string(phaseagent.PhaseExecute)}, PreviousBindingRef: "b2", InputRevision: "r2", ContinuationRef: "c", State: AwaitStateRunning})
	if err != nil {
		t.Fatal(err)
	}
	candidate := PhaseOutputRecord{Key: PhaseOutputKey{TaskID: "task", InvocationID: "inv", Generation: 1}, BindingRef: "b2", InputRevision: "r2", ExecutionEpoch: 2, Output: phaseagent.PhaseOutput{ReportRef: "done"}}
	output, waiting, created, err := r.LifecycleMutations().AcceptPhaseOutput(ctx, candidate, key, w.Revision)
	if err != nil || !created || waiting.State != AwaitStateTerminal || output.Output.ReportRef != "done" {
		t.Fatalf("accept output=%+v waiting=%+v created=%t err=%v", output, waiting, created, err)
	}
	_, _, created, err = r.LifecycleMutations().AcceptPhaseOutput(ctx, candidate, key, w.Revision)
	if err != nil || created {
		t.Fatalf("duplicate created=%t err=%v", created, err)
	}
	events, err := r.ListRuntimeEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range events {
		if e.EventType == "PhaseOutputSubmitted" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("events=%d", n)
	}
}

func TestAcceptPhaseOutputOutboxFailureRollsBack(t *testing.T) {
	ctx := context.Background()
	r, err := OpenSQLiteRuntimeStateRepository(filepath.Join(t.TempDir(), "runtime.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	key := WaitingKey{TaskID: "task", InvocationID: "inv", Generation: 1}
	w, err := r.WaitingStore().Create(ctx, WaitingRecord{Key: key, ExecutionEpoch: 2, Endpoint: phaseagent.PhaseEndpointRef{TaskID: "task", EndpointID: string(phaseagent.PhaseExecute)}, PreviousBindingRef: "b2", InputRevision: "r2", ContinuationRef: "c", State: AwaitStateRunning})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = r.db.Exec("CREATE TRIGGER reject_output BEFORE INSERT ON runtime_events WHEN NEW.event_type='PhaseOutputSubmitted' BEGIN SELECT RAISE(ABORT, 'outbox unavailable'); END"); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err = r.LifecycleMutations().AcceptPhaseOutput(ctx, PhaseOutputRecord{Key: PhaseOutputKey{TaskID: "task", InvocationID: "inv", Generation: 1}, BindingRef: "b2", InputRevision: "r2", ExecutionEpoch: 2, Output: phaseagent.PhaseOutput{ReportRef: "done"}}, key, w.Revision); err == nil {
		t.Fatal("accepted despite outbox failure")
	}
	got, _, _ := r.WaitingStore().Get(ctx, key)
	if got.State != AwaitStateRunning {
		t.Fatalf("waiting=%s", got.State)
	}
	if _, found, _ := r.PhaseOutputStore().Get(ctx, PhaseOutputKey{TaskID: "task", InvocationID: "inv", Generation: 1}); found {
		t.Fatal("output persisted")
	}
}

func TestAcceptPhaseOutputRejectsStaleWaitingCASWithoutStateOrEvent(t *testing.T) {
	ctx := context.Background()
	r, err := OpenSQLiteRuntimeStateRepository(filepath.Join(t.TempDir(), "runtime.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	key := WaitingKey{TaskID: "task", InvocationID: "inv", Generation: 1}
	w, err := r.WaitingStore().Create(ctx, WaitingRecord{Key: key, ExecutionEpoch: 2, Endpoint: phaseagent.PhaseEndpointRef{TaskID: "task", EndpointID: string(phaseagent.PhaseExecute)}, PreviousBindingRef: "b2", InputRevision: "r2", ContinuationRef: "c", State: AwaitStateRunning})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err = r.LifecycleMutations().AcceptPhaseOutput(ctx, PhaseOutputRecord{Key: PhaseOutputKey{TaskID: "task", InvocationID: "inv", Generation: 1}, BindingRef: "b2", InputRevision: "r2", ExecutionEpoch: 2, Output: phaseagent.PhaseOutput{ReportRef: "done"}}, key, w.Revision-1); !errors.Is(err, ErrCompletionConflict) {
		t.Fatalf("stale revision error=%v", err)
	}
	got, found, err := r.WaitingStore().Get(ctx, key)
	if err != nil || !found || got.State != AwaitStateRunning || got.Revision != w.Revision {
		t.Fatalf("stale revision changed waiting=%#v err=%v", got, err)
	}
	if _, found, err = r.PhaseOutputStore().Get(ctx, PhaseOutputKey{TaskID: "task", InvocationID: "inv", Generation: 1}); err != nil || found {
		t.Fatalf("stale revision inserted output found=%t err=%v", found, err)
	}
	events, err := r.ListRuntimeEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if event.EventType == "PhaseOutputSubmitted" {
			t.Fatal("stale revision wrote success event")
		}
	}
}

func TestAcceptPhaseOutputConcurrentCurrentAndStaleCASHasSingleWinner(t *testing.T) {
	ctx := context.Background()
	r, err := OpenSQLiteRuntimeStateRepository(filepath.Join(t.TempDir(), "runtime.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	key := WaitingKey{TaskID: "task", InvocationID: "inv", Generation: 1}
	w, err := r.WaitingStore().Create(ctx, WaitingRecord{Key: key, ExecutionEpoch: 2, Endpoint: phaseagent.PhaseEndpointRef{TaskID: "task", EndpointID: string(phaseagent.PhaseExecute)}, PreviousBindingRef: "b2", InputRevision: "r2", ContinuationRef: "c", State: AwaitStateRunning})
	if err != nil {
		t.Fatal(err)
	}
	candidate := PhaseOutputRecord{Key: PhaseOutputKey{TaskID: "task", InvocationID: "inv", Generation: 1}, BindingRef: "b2", InputRevision: "r2", ExecutionEpoch: 2, Output: phaseagent.PhaseOutput{ReportRef: "done"}}
	start := make(chan struct{})
	results := make(chan error, 2)
	var group sync.WaitGroup
	for _, revision := range []int64{w.Revision, w.Revision - 1} {
		group.Add(1)
		go func(revision int64) {
			defer group.Done()
			<-start
			_, _, _, err := r.LifecycleMutations().AcceptPhaseOutput(ctx, candidate, key, revision)
			results <- err
		}(revision)
	}
	close(start)
	group.Wait()
	close(results)
	var accepted, conflicts int
	for err := range results {
		if err == nil {
			accepted++
		} else if errors.Is(err, ErrCompletionConflict) {
			conflicts++
		} else {
			t.Fatalf("unexpected concurrent error: %v", err)
		}
	}
	if accepted != 1 || conflicts != 1 {
		t.Fatalf("accepted=%d conflicts=%d", accepted, conflicts)
	}
	events, err := r.ListRuntimeEvents(ctx)
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
		t.Fatalf("current/stale event count=%d", count)
	}
}

func TestAdvanceTeardownPersistsEachStepAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	r, err := OpenSQLiteRuntimeStateRepository(filepath.Join(t.TempDir(), "runtime.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	execution, err := r.PhysicalExecutionStore().Create(ctx, PhysicalExecution{TaskID: "task", InvocationID: "inv", Generation: 1, ExecutionEpoch: 2, BindingRef: "b2", InputRevision: "r2", State: PhysicalExecutionRunning})
	if err != nil {
		t.Fatal(err)
	}
	execution, changed, err := r.LifecycleMutations().AdvanceTeardown(ctx, execution.Key(), execution.Revision, TeardownStepBegin)
	if err != nil || !changed || execution.State != PhysicalExecutionTearingDown {
		t.Fatalf("begin execution=%#v changed=%t err=%v", execution, changed, err)
	}
	execution, changed, err = r.LifecycleMutations().AdvanceTeardown(ctx, execution.Key(), execution.Revision, TeardownStepTask)
	if err != nil || !changed || !execution.Teardown.TeamHarnessTaskCancelled {
		t.Fatalf("task execution=%#v changed=%t err=%v", execution, changed, err)
	}
	_, changed, err = r.LifecycleMutations().AdvanceTeardown(ctx, execution.Key(), execution.Revision, TeardownStepTask)
	if err != nil || changed {
		t.Fatalf("duplicate task changed=%t err=%v", changed, err)
	}
	for _, step := range []TeardownStep{TeardownStepWorker, TeardownStepMCP, TeardownStepCredential, TeardownStepToken, TeardownStepLease} {
		execution, changed, err = r.LifecycleMutations().AdvanceTeardown(ctx, execution.Key(), execution.Revision, step)
		if err != nil || !changed {
			t.Fatalf("step %s changed=%t err=%v", step, changed, err)
		}
	}
	execution, changed, err = r.LifecycleMutations().AdvanceTeardown(ctx, execution.Key(), execution.Revision, TeardownStepTerminate)
	if err != nil || !changed || execution.State != PhysicalExecutionTerminated {
		t.Fatalf("terminate execution=%#v changed=%t err=%v", execution, changed, err)
	}
	if _, changed, err = r.LifecycleMutations().AdvanceTeardown(ctx, execution.Key(), execution.Revision, TeardownStepTerminate); err != nil || changed {
		t.Fatalf("duplicate terminate changed=%t err=%v", changed, err)
	}
	events, err := r.ListRuntimeEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var started, steps, terminated int
	for _, event := range events {
		switch event.EventType {
		case "PhysicalExecutionTeardownStarted":
			started++
		case "PhysicalExecutionTeardownStepCompleted":
			steps++
		case "PhysicalExecutionTerminated":
			terminated++
		}
	}
	if started != 1 || steps != 6 || terminated != 1 {
		t.Fatalf("teardown events started=%d steps=%d terminated=%d", started, steps, terminated)
	}
}

func TestAdvanceTeardownOutboxFailureRollsBackProgress(t *testing.T) {
	ctx := context.Background()
	r, err := OpenSQLiteRuntimeStateRepository(filepath.Join(t.TempDir(), "runtime.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	execution, err := r.PhysicalExecutionStore().Create(ctx, PhysicalExecution{TaskID: "task", InvocationID: "inv", Generation: 1, ExecutionEpoch: 2, BindingRef: "b2", InputRevision: "r2", State: PhysicalExecutionRunning})
	if err != nil {
		t.Fatal(err)
	}
	execution, _, err = r.LifecycleMutations().AdvanceTeardown(ctx, execution.Key(), execution.Revision, TeardownStepBegin)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = r.db.Exec("CREATE TRIGGER reject_teardown_step BEFORE INSERT ON runtime_events WHEN NEW.event_type='PhysicalExecutionTeardownStepCompleted' BEGIN SELECT RAISE(ABORT, 'outbox unavailable'); END"); err != nil {
		t.Fatal(err)
	}
	if _, _, err = r.LifecycleMutations().AdvanceTeardown(ctx, execution.Key(), execution.Revision, TeardownStepTask); err == nil {
		t.Fatal("teardown progress committed despite outbox failure")
	}
	stored, found, err := r.PhysicalExecutionStore().Get(ctx, execution.Key())
	if err != nil || !found || stored.Revision != execution.Revision || stored.Teardown.TeamHarnessTaskCancelled {
		t.Fatalf("partial teardown progress=%#v err=%v", stored, err)
	}
}

func TestAdvanceTeardownConcurrentClaimHasSingleWinner(t *testing.T) {
	ctx := context.Background()
	r, err := OpenSQLiteRuntimeStateRepository(filepath.Join(t.TempDir(), "runtime.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	execution, err := r.PhysicalExecutionStore().Create(ctx, PhysicalExecution{TaskID: "task", InvocationID: "inv", Generation: 1, ExecutionEpoch: 2, BindingRef: "b2", InputRevision: "r2", State: PhysicalExecutionRunning})
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	var group sync.WaitGroup
	for range 2 {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			_, _, err := r.LifecycleMutations().AdvanceTeardown(ctx, execution.Key(), execution.Revision, TeardownStepBegin)
			results <- err
		}()
	}
	close(start)
	group.Wait()
	close(results)
	var winners, conflicts int
	for err := range results {
		if err == nil {
			winners++
		} else if errors.Is(err, ErrCompletionConflict) {
			conflicts++
		} else {
			t.Fatalf("unexpected claim error: %v", err)
		}
	}
	if winners != 1 || conflicts != 1 {
		t.Fatalf("winners=%d conflicts=%d", winners, conflicts)
	}
}
