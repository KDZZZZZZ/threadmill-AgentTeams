package runtime

import (
	"context"
	"strings"
	"testing"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/executionreceipt"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/phaseagent"
)

func TestSQLiteRuntimeStateRepositoryColdLoadsLifecycleAndCAS(t *testing.T) {
	ctx, path := context.Background(), t.TempDir()+"/runtime.db"
	r, err := OpenSQLiteRuntimeStateRepository(path)
	if err != nil {
		t.Fatal(err)
	}
	k := WaitingKey{TaskID: "task", InvocationID: "inv", Generation: 1}
	w, err := r.WaitingStore().Create(ctx, WaitingRecord{Key: k, ExecutionEpoch: 1, Endpoint: phaseagent.PhaseEndpointRef{TaskID: "task", EndpointID: string(phaseagent.PhaseExecute)}, PreviousBindingRef: "b1", InputRevision: "r1", ContinuationRef: "c1", State: AwaitStateWaiting})
	if err != nil {
		t.Fatal(err)
	}
	if err = r.ContinuationStore().Put(ctx, "c1", ContinuationMaterial{Endpoint: w.Endpoint, WorkspaceRef: "ws", ContextSliceRef: "ctx", TaskMemoryBufferRef: "mem"}); err != nil {
		t.Fatal(err)
	}
	if err = r.InputStore().Put(ctx, k, StoredPhaseInputSet{Inputs: phaseagent.PhaseInputSet{InputRevision: "r1"}}); err != nil {
		t.Fatal(err)
	}
	binding, err := r.InputStore().RebindInputsForContinuation(ctx, ContinuationBinding{InvocationID: "inv", Generation: 1, ExecutionEpoch: 2, PreviousBindingRef: "b1", PreviousRevision: "r1", InputRevision: "r2"})
	if err != nil || binding.BindingRef == "" {
		t.Fatalf("durable rebind=%+v err=%v", binding, err)
	}
	p, err := r.PhysicalExecutionStore().Create(ctx, PhysicalExecution{TaskID: "task", InvocationID: "inv", Generation: 1, ExecutionEpoch: 1, State: PhysicalExecutionRunning, BindingRef: "b1", InputRevision: "r1"})
	if err != nil {
		t.Fatal(err)
	}
	p.State = PhysicalExecutionTearingDown
	p.Teardown.TokenRevoked = true
	if _, ok, err := r.PhysicalExecutionStore().CompareAndSwap(ctx, p.Key(), p.Revision, p); err != nil || !ok {
		t.Fatalf("physical cas: ok=%t err=%v", ok, err)
	}
	if _, _, err = r.ReceiptStore().PutIfAbsent(ctx, executionreceipt.Receipt{TaskID: "task", InvocationID: "inv", Generation: 1, ExecutionEpoch: 1, BindingRef: "b1", InputRevision: "r1", PackageDigest: "digest", SessionIdentity: "matrix:room", Consumed: true}); err != nil {
		t.Fatal(err)
	}
	if _, _, err = r.PhaseOutputStore().PutIfAbsent(ctx, PhaseOutputRecord{Key: PhaseOutputKey{TaskID: "task", InvocationID: "inv", Generation: 1}, BindingRef: "b1", InputRevision: "r1", ExecutionEpoch: 1, Output: phaseagent.PhaseOutput{Phase: phaseagent.PhaseExecute, ReportRef: "artifact"}}); err != nil {
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
	got, found, err := r.WaitingStore().Get(ctx, k)
	if err != nil || !found || got.Revision != 1 {
		t.Fatalf("cold waiting=%+v found=%t err=%v", got, found, err)
	}
	got.State = AwaitStateRehydrating
	if _, ok, err := r.WaitingStore().CompareAndSwap(ctx, k, got.Revision, got); err != nil || !ok {
		t.Fatalf("cold cas ok=%t err=%v", ok, err)
	}
	if _, err = r.ContinuationStore().ResolveContinuation(ctx, "c1"); err != nil {
		t.Fatal(err)
	}
	if _, err = r.InputStore().ResolveRehydrationInputs(ctx, WaitingRecord{Key: k, InputRevision: "r1"}); err != nil {
		t.Fatal(err)
	}
	if history, err := r.PhysicalExecutionStore().ListByInvocation(ctx, "task", "inv", 1); err != nil || len(history) != 1 || !history[0].Teardown.TokenRevoked {
		t.Fatalf("history=%+v err=%v", history, err)
	}
	if _, found, err := r.ReceiptStore().Get(ctx, executionreceipt.Key{TaskID: "task", InvocationID: "inv", Generation: 1, ExecutionEpoch: 1}); err != nil || !found {
		t.Fatalf("receipt found=%t err=%v", found, err)
	}
	if _, found, err := r.PhaseOutputStore().Get(ctx, PhaseOutputKey{TaskID: "task", InvocationID: "inv", Generation: 1}); err != nil || !found {
		t.Fatalf("output found=%t err=%v", found, err)
	}
	if events, err := r.ListRuntimeEvents(ctx); err != nil || len(events) < 5 {
		t.Fatalf("events=%d err=%v", len(events), err)
	}
}

func TestSQLiteRuntimeStateRepositoryRejectsSecretAtRestAndNewerSchema(t *testing.T) {
	ctx, path := context.Background(), t.TempDir()+"/runtime.db"
	r, err := OpenSQLiteRuntimeStateRepository(path)
	if err != nil {
		t.Fatal(err)
	}
	k := WaitingKey{TaskID: "task", InvocationID: "inv", Generation: 1}
	_, err = r.WaitingStore().Create(ctx, WaitingRecord{Key: k, ExecutionEpoch: 1, Endpoint: phaseagent.PhaseEndpointRef{TaskID: "task", EndpointID: string(phaseagent.PhaseExecute)}, PreviousBindingRef: "b", InputRevision: "r", ContinuationRef: "c", State: AwaitStateWaiting})
	if err != nil {
		t.Fatal(err)
	}
	var all string
	for _, table := range []string{"runtime_waiting", "runtime_events"} {
		var v string
		if err = r.db.QueryRow("SELECT group_concat(CAST(payload AS TEXT),'') FROM " + table).Scan(&v); err != nil {
			t.Fatal(err)
		}
		all += v
	}
	if strings.Contains(all, "execution_token") || strings.Contains(all, "credential_value") {
		t.Fatal("secret field persisted")
	}
	if _, err = r.db.Exec("UPDATE runtime_schema_version SET version=99"); err != nil {
		t.Fatal(err)
	}
	r.Close()
	if _, err = OpenSQLiteRuntimeStateRepository(path); err == nil {
		t.Fatal("newer schema was accepted")
	}
}

func TestSQLiteRuntimeStateRepositoryRollsBackStateWhenOutboxFails(t *testing.T) {
	ctx, path := context.Background(), t.TempDir()+"/runtime.db"
	r, err := OpenSQLiteRuntimeStateRepository(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err = r.db.Exec("CREATE TRIGGER reject_runtime_event BEFORE INSERT ON runtime_events BEGIN SELECT RAISE(ABORT, 'outbox unavailable'); END"); err != nil {
		t.Fatal(err)
	}
	k := WaitingKey{TaskID: "task", InvocationID: "inv", Generation: 1}
	_, err = r.WaitingStore().Create(ctx, WaitingRecord{Key: k, ExecutionEpoch: 1, Endpoint: phaseagent.PhaseEndpointRef{TaskID: "task", EndpointID: string(phaseagent.PhaseExecute)}, PreviousBindingRef: "b", InputRevision: "r", ContinuationRef: "c", State: AwaitStateWaiting})
	if err == nil {
		t.Fatal("outbox failure unexpectedly committed state")
	}
	if _, found, getErr := r.WaitingStore().Get(ctx, k); getErr != nil || found {
		t.Fatalf("rolled-back state found=%t err=%v", found, getErr)
	}
	if events, listErr := r.ListRuntimeEvents(ctx); listErr != nil || len(events) != 0 {
		t.Fatalf("rolled-back events=%d err=%v", len(events), listErr)
	}
}
