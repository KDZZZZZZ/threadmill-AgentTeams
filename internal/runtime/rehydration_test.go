package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/artifacts"
	phasemcp "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/mcp/phase"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/phaseagent"
)

type fakeRehydrationInputs struct {
	snapshot RehydrationInputSnapshot
	err      error
}

func (f fakeRehydrationInputs) ResolveRehydrationInputs(context.Context, WaitingRecord) (RehydrationInputSnapshot, error) {
	return f.snapshot, f.err
}

type fakeRebinder struct {
	binding ContinuationBinding
	err     error
	calls   []ContinuationBinding
}

func (f *fakeRebinder) RebindInputsForContinuation(_ context.Context, binding ContinuationBinding) (ContinuationBinding, error) {
	f.calls = append(f.calls, binding)
	if f.err != nil {
		return ContinuationBinding{}, f.err
	}
	result := f.binding
	result.InvocationID, result.Generation, result.ExecutionEpoch = binding.InvocationID, binding.Generation, binding.ExecutionEpoch
	result.PreviousBindingRef, result.PreviousRevision, result.InputRevision = binding.PreviousBindingRef, binding.PreviousRevision, binding.InputRevision
	return result, nil
}

type fakeContextReconstructor struct {
	result RehydratedContext
	err    error
}

func (f fakeContextReconstructor) ReconstructContext(context.Context, WaitingRecord, ContinuationMaterial) (RehydratedContext, error) {
	return f.result, f.err
}

type fakeWorkspaceReconstructor struct {
	result WorkspaceBinding
	err    error
}

func (f fakeWorkspaceReconstructor) ReconstructWorkspace(context.Context, WaitingRecord, ContinuationMaterial) (WorkspaceBinding, error) {
	return f.result, f.err
}

type fakeTaskMemoryReconstructor struct {
	result RehydratedTaskMemory
	err    error
}

func (f fakeTaskMemoryReconstructor) ReconstructTaskMemory(context.Context, WaitingRecord, ContinuationMaterial) (RehydratedTaskMemory, error) {
	return f.result, f.err
}

func rehydrationFixture(t *testing.T) (RehydrationCoordinator, WaitingRecord, *InMemoryWaitingStore, *fakeRebinder) {
	t.Helper()
	record := testWaitingRecord()
	record.State = AwaitStateWaiting
	record.AllowedDirs = []string{"src", "out"}
	store := NewInMemoryWaitingStore()
	created, err := store.Create(context.Background(), record)
	if err != nil {
		t.Fatal(err)
	}
	continuations := NewInMemoryContinuationStore()
	if err := continuations.Put(created.ContinuationRef, ContinuationMaterial{
		Endpoint: created.Endpoint, WorkspaceRef: created.WorkspaceRef, ContextSliceRef: created.ContextSliceRef,
		ContextBaselineRef: "context-baseline-r9", TaskMemoryBufferRef: created.TaskMemoryBufferRef,
		ArtifactRefs: []artifacts.ArtifactRef{"artifact-from-continuation"}, EventRefs: []string{"event-from-continuation"}, EvidenceRefs: []string{"evidence-from-continuation"},
	}); err != nil {
		t.Fatal(err)
	}
	rebinder := &fakeRebinder{binding: ContinuationBinding{BindingRef: "binding-r5"}}
	coordinator := RehydrationCoordinator{
		Store:    store,
		Inputs:   fakeRehydrationInputs{snapshot: RehydrationInputSnapshot{Inputs: phaseagent.PhaseInputSet{InputRevision: "input-r5", Delivered: []phaseagent.InputDelivery{{InputID: "review"}}}, RevisionIsNewer: true, AwaitConditionSatisfied: true}},
		Bindings: rebinder, Continuations: continuations,
		Contexts:   fakeContextReconstructor{result: RehydratedContext{SliceRef: created.ContextSliceRef, BaselineRef: "context-baseline-r9"}},
		Workspaces: fakeWorkspaceReconstructor{result: WorkspaceBinding{Ref: created.WorkspaceRef, Revision: "workspace-r7", AllowedDirs: []string{"src"}}},
		TaskMemory: fakeTaskMemoryReconstructor{result: RehydratedTaskMemory{BufferRef: created.TaskMemoryBufferRef, View: phaseagent.TaskMemoryBufferView{Candidates: []phaseagent.TaskMemoryCandidateView{{CandidateID: "memory-1"}}}}},
		Surfaces:   ExecutionSurfaces{Runtime: noopRuntime{}, ContextReader: noopReader{}, ContextAgent: noopAgent{}},
	}
	return coordinator, created, store, rebinder
}

func TestPrepareReconstructsLogicalInvocationWithoutPhysicalCredentials(t *testing.T) {
	t.Parallel()
	coordinator, record, store, rebinder := rehydrationFixture(t)
	record.ArtifactRefs = []artifacts.ArtifactRef{"artifact-existing"}
	if _, swapped, err := store.CompareAndSwap(context.Background(), record.Key, record.Revision, record); err != nil || !swapped {
		t.Fatal(err)
	}
	record, _, _ = store.Get(context.Background(), record.Key)
	plan, err := coordinator.Prepare(context.Background(), RehydrationRequest{Key: record.Key, ExpectedWaitingRevision: record.Revision})
	if err != nil {
		t.Fatal(err)
	}
	if plan.TaskID != "task-a" || plan.InvocationID != "invocation-a" || plan.Generation != 3 || plan.NextExecutionEpoch != 2 || plan.NewBindingRef != "binding-r5" || plan.NewInputRevision != "input-r5" {
		t.Fatalf("unexpected plan identity: %#v", plan)
	}
	if plan.Execution.Invocation.Start.BindingRef != "binding-r5" || plan.Execution.Invocation.Start.Inputs.InputRevision != "input-r5" || plan.Execution.Invocation.State != phaseagent.InvocationRunning {
		t.Fatalf("execution context not rebuilt from latest binding/inputs: %#v", plan.Execution)
	}
	if len(rebinder.calls) != 1 || rebinder.calls[0].PreviousBindingRef != "binding-r4" || rebinder.calls[0].BindingRef != "" {
		t.Fatalf("rebind input mismatch: %#v", rebinder.calls)
	}
	if strings.Join([]string{string(plan.ArtifactRefs[0]), string(plan.ArtifactRefs[1])}, ",") != "artifact-existing,artifact-from-continuation" || strings.Join(plan.EventRefs, ",") != "event-a,event-from-continuation" {
		t.Fatalf("logical references were not retained: %#v", plan)
	}
	current, found, err := store.Get(context.Background(), record.Key)
	if err != nil || !found || current.State != AwaitStateRehydrating || current.Revision != plan.ExpectedWaitingRevision {
		t.Fatalf("waiting reservation mismatch: %#v found=%t err=%v", current, found, err)
	}
	encoded, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"token-a", "credential-a", "X-Threadmill-Execution-Token", "hidden reasoning"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("plan leaked %q: %s", forbidden, encoded)
		}
	}
}

func TestPrepareRejectsStaleAndConcurrentRehydration(t *testing.T) {
	coordinator, record, store, _ := rehydrationFixture(t)
	if _, err := coordinator.Prepare(context.Background(), RehydrationRequest{Key: record.Key, ExpectedWaitingRevision: record.Revision - 1}); !errors.Is(err, ErrRehydrationConflict) {
		t.Fatalf("stale request error = %v", err)
	}
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := coordinator.Prepare(context.Background(), RehydrationRequest{Key: record.Key, ExpectedWaitingRevision: record.Revision})
			results <- err
		}()
	}
	wg.Wait()
	close(results)
	successes := 0
	for err := range results {
		if err == nil {
			successes++
		} else if !errors.Is(err, ErrNotWaiting) && !errors.Is(err, ErrRehydrationConflict) {
			t.Fatalf("unexpected concurrent error: %v", err)
		}
	}
	if successes != 1 {
		t.Fatalf("successful prepares = %d", successes)
	}
	current, _, _ := store.Get(context.Background(), record.Key)
	if current.State != AwaitStateRehydrating {
		t.Fatalf("state after concurrent prepare = %s", current.State)
	}
}

func TestPrepareFailureRollsBackAndRetryGetsNewWaitingRevision(t *testing.T) {
	coordinator, record, store, _ := rehydrationFixture(t)
	coordinator.Workspaces = fakeWorkspaceReconstructor{result: WorkspaceBinding{Ref: record.WorkspaceRef, AllowedDirs: []string{"src", "expanded"}}}
	if _, err := coordinator.Prepare(context.Background(), RehydrationRequest{Key: record.Key, ExpectedWaitingRevision: record.Revision}); err == nil {
		t.Fatal("expanded workspace permissions accepted")
	}
	afterFailure, _, _ := store.Get(context.Background(), record.Key)
	if afterFailure.State != AwaitStateWaiting || afterFailure.Revision <= record.Revision {
		t.Fatalf("prepare failure did not roll back: %#v", afterFailure)
	}
	coordinator.Workspaces = fakeWorkspaceReconstructor{result: WorkspaceBinding{Ref: record.WorkspaceRef, AllowedDirs: []string{"src"}}}
	plan, err := coordinator.Prepare(context.Background(), RehydrationRequest{Key: record.Key, ExpectedWaitingRevision: afterFailure.Revision})
	if err != nil {
		t.Fatal(err)
	}
	if plan.NextExecutionEpoch != 2 || plan.ExpectedWaitingRevision <= afterFailure.Revision {
		t.Fatalf("retry plan is not auditable: %#v", plan)
	}
	if err := coordinator.Rollback(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	afterRollback, _, _ := store.Get(context.Background(), record.Key)
	if afterRollback.State != AwaitStateWaiting || afterRollback.Revision <= plan.ExpectedWaitingRevision {
		t.Fatalf("rollback mismatch: %#v", afterRollback)
	}
}

func TestPrepareRequiresNewResolvedInputsAndRevokedTokenStaysInvalid(t *testing.T) {
	coordinator, record, store, _ := rehydrationFixture(t)
	coordinator.Inputs = fakeRehydrationInputs{snapshot: RehydrationInputSnapshot{Inputs: phaseagent.PhaseInputSet{InputRevision: record.InputRevision, Pending: []phaseagent.PendingInput{{InputID: "review"}}}, RevisionIsNewer: false}}
	if _, err := coordinator.Prepare(context.Background(), RehydrationRequest{Key: record.Key, ExpectedWaitingRevision: record.Revision}); err == nil {
		t.Fatal("old unresolved inputs accepted")
	}
	current, _, _ := store.Get(context.Background(), record.Key)
	if current.State != AwaitStateWaiting {
		t.Fatalf("failed precondition stranded state: %s", current.State)
	}
	coordinator.Inputs = fakeRehydrationInputs{snapshot: RehydrationInputSnapshot{Inputs: phaseagent.PhaseInputSet{InputRevision: "input-r5", Pending: []phaseagent.PendingInput{{InputID: "review"}}}, RevisionIsNewer: true, AwaitConditionSatisfied: true}}
	if _, err := coordinator.Prepare(context.Background(), RehydrationRequest{Key: record.Key, ExpectedWaitingRevision: current.Revision}); err == nil {
		t.Fatal("still-pending declared input accepted")
	}

	registry := phasemcp.NewBindingRegistry()
	binding, err := registry.Issue(phasemcp.BoundServices{Binding: phasemcp.InvocationBinding{TaskID: record.Key.TaskID, InvocationID: record.Key.InvocationID, Endpoint: record.Endpoint, Generation: record.Key.Generation, Role: phaseagent.PhaseExecute, BindingRef: record.PreviousBindingRef}, Runtime: noopRuntime{}, Reader: noopReader{}, Agent: noopAgent{}, Expires: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	registry.Revoke(binding.Token)
	if _, err := registry.Resolve(binding.Token); !errors.Is(err, phasemcp.ErrInvalidToken) {
		t.Fatalf("revoked old token resolved: %v", err)
	}
}

func TestContinuationStoreReturnsOnlyCopiedLogicalMaterial(t *testing.T) {
	t.Parallel()
	store := NewInMemoryContinuationStore()
	ref := ContinuationRef("continuation-a")
	material := ContinuationMaterial{Endpoint: phaseagent.PhaseEndpointRef{TaskID: "task-a", EndpointID: "execute"}, WorkspaceRef: "workspace-r7", ArtifactRefs: []artifacts.ArtifactRef{"artifact-a"}, EventRefs: []string{"event-a"}}
	if err := store.Put(ref, material); err != nil {
		t.Fatal(err)
	}
	material.ArtifactRefs[0] = "mutated"
	resolved, err := store.ResolveContinuation(context.Background(), ref)
	if err != nil || resolved.ArtifactRefs[0] != "artifact-a" {
		t.Fatalf("continuation material was not copied: %#v err=%v", resolved, err)
	}
	if _, err := store.ResolveContinuation(context.Background(), "missing"); !errors.Is(err, ErrContinuationNotFound) {
		t.Fatalf("missing continuation error = %v", err)
	}
}
