package runtime

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/executionreceipt"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/phaseagent"
)

func TestDurableLifecycleStateColdReopenRetainsEveryM4LogicalAuthority(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "runtime.db")
	repository, err := OpenSQLiteRuntimeStateRepository(path)
	if err != nil {
		t.Fatal(err)
	}
	state, err := NewDurableLifecycleState(repository)
	if err != nil {
		t.Fatal(err)
	}

	key := WaitingKey{TaskID: "task-c1", InvocationID: "invocation-c1", Generation: 7}
	endpoint := phaseagent.PhaseEndpointRef{TaskID: key.TaskID, EndpointID: string(phaseagent.PhaseExecute)}
	waiting, err := state.Waiting.Create(ctx, WaitingRecord{
		Key: key, ExecutionEpoch: 1, Endpoint: endpoint, PreviousBindingRef: "binding-r4", InputRevision: "input-r4",
		ContinuationRef: "continuation-r4", State: AwaitStateWaiting, PendingInputIDs: []string{"upstream"},
		WorkspaceRef: "workspace-c1", AllowedDirs: []string{"out"}, ContextSliceRef: "context-r4", TaskMemoryBufferRef: "memory-r4",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := state.Continuations.Put(ctx, waiting.ContinuationRef, ContinuationMaterial{Endpoint: endpoint, WorkspaceRef: waiting.WorkspaceRef, ContextSliceRef: waiting.ContextSliceRef, TaskMemoryBufferRef: waiting.TaskMemoryBufferRef}); err != nil {
		t.Fatal(err)
	}
	inputs := phaseagent.PhaseInputSet{InputRevision: "input-r4", Pending: []phaseagent.PendingInput{{InputID: "upstream", FromEndpoint: phaseagent.PhaseEndpointRef{TaskID: key.TaskID, EndpointID: "plan"}}}}
	if err := state.Inputs.Put(ctx, key, StoredPhaseInputSet{Inputs: inputs}); err != nil {
		t.Fatal(err)
	}
	binding, err := state.Inputs.RebindInputsForContinuation(ctx, ContinuationBinding{
		InvocationID: key.InvocationID, Generation: key.Generation, ExecutionEpoch: 2,
		PreviousBindingRef: "binding-r4", PreviousRevision: "input-r4", InputRevision: "input-r5",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.PhysicalExecutions.Create(ctx, PhysicalExecution{TaskID: key.TaskID, InvocationID: key.InvocationID, Generation: key.Generation, ExecutionEpoch: 1, State: PhysicalExecutionTerminated, BindingRef: "binding-r4", InputRevision: "input-r4"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := state.Receipts.PutIfAbsent(ctx, executionreceipt.Receipt{TaskID: key.TaskID, InvocationID: key.InvocationID, Generation: key.Generation, ExecutionEpoch: 1, BindingRef: "binding-r4", InputRevision: "input-r4", PackageDigest: "digest-a", SessionIdentity: "matrix:room-a", Consumed: true}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := state.Outputs.PutIfAbsent(ctx, PhaseOutputRecord{Key: PhaseOutputKey{TaskID: key.TaskID, InvocationID: key.InvocationID, Generation: key.Generation}, BindingRef: "binding-r4", InputRevision: "input-r4", ExecutionEpoch: 1, Output: phaseagent.PhaseOutput{Summary: "accepted"}}); err != nil {
		t.Fatal(err)
	}
	if err := repository.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenSQLiteRuntimeStateRepository(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	recovered, err := NewDurableLifecycleState(reopened)
	if err != nil {
		t.Fatal(err)
	}
	if got, found, err := recovered.Waiting.Get(ctx, key); err != nil || !found || got.Revision != waiting.Revision || got.State != AwaitStateWaiting {
		t.Fatalf("waiting recovery = %#v, found=%t, err=%v", got, found, err)
	}
	if got, err := recovered.Continuations.ResolveContinuation(ctx, waiting.ContinuationRef); err != nil || got.Endpoint != endpoint {
		t.Fatalf("continuation recovery = %#v, err=%v", got, err)
	}
	if got, err := recovered.Inputs.ResolveRehydrationInputs(ctx, waiting); err != nil || got.Inputs.InputRevision != "input-r4" {
		t.Fatalf("input recovery = %#v, err=%v", got, err)
	}
	if got, found, err := recovered.Inputs.ResolveContinuationBinding(ctx, binding.BindingRef); err != nil || !found || got != binding {
		t.Fatalf("binding recovery = %#v, found=%t, err=%v", got, found, err)
	}
	physicalKey := PhysicalExecutionKey{TaskID: key.TaskID, InvocationID: key.InvocationID, Generation: key.Generation, ExecutionEpoch: 1}
	if got, found, err := recovered.PhysicalExecutions.Get(ctx, physicalKey); err != nil || !found || got.State != PhysicalExecutionTerminated {
		t.Fatalf("physical recovery = %#v, found=%t, err=%v", got, found, err)
	}
	if got, found, err := recovered.Receipts.Get(ctx, executionreceipt.Key{TaskID: key.TaskID, InvocationID: key.InvocationID, Generation: key.Generation, ExecutionEpoch: 1}); err != nil || !found || !got.Consumed {
		t.Fatalf("receipt recovery = %#v, found=%t, err=%v", got, found, err)
	}
	if got, found, err := recovered.Outputs.Get(ctx, PhaseOutputKey{TaskID: key.TaskID, InvocationID: key.InvocationID, Generation: key.Generation}); err != nil || !found || got.Output.Summary != "accepted" {
		t.Fatalf("output recovery = %#v, found=%t, err=%v", got, found, err)
	}
}

func TestNewDurableLifecycleStateRejectsNilRepository(t *testing.T) {
	t.Parallel()
	if _, err := NewDurableLifecycleState(nil); err == nil {
		t.Fatal("nil repository was accepted")
	}
}
