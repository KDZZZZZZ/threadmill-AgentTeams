package runtime

import (
	"context"
	"path/filepath"
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
