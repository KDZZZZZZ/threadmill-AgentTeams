package runtime

import (
	"context"
	"testing"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/phaseagent"
)

func TestInMemoryPhaseInputStoreReturnsLatestCompleteInputsAndDelta(t *testing.T) {
	store := NewInMemoryPhaseInputStore()
	key := WaitingKey{TaskID: "task-a", InvocationID: "invocation-a", Generation: 3}
	oldDelivery := phaseagent.InputDelivery{InputID: "design", PhaseOutputRef: "phase-output-design", SourceRevision: "source-4"}
	newDelivery := phaseagent.InputDelivery{InputID: "review", PhaseOutputRef: "phase-output-review", ArtifactRefs: []string{"artifact-review"}, SourceRevision: "source-5"}
	oldInputs := phaseagent.PhaseInputSet{InputRevision: "r4", Delivered: []phaseagent.InputDelivery{oldDelivery}, Pending: []phaseagent.PendingInput{{InputID: "review"}}}
	newInputs := phaseagent.PhaseInputSet{InputRevision: "r5", Delivered: []phaseagent.InputDelivery{oldDelivery, newDelivery}}
	if err := store.Put(key, StoredPhaseInputSet{Inputs: oldInputs}); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(key, StoredPhaseInputSet{Inputs: newInputs, AwaitConditionSatisfied: true}); err != nil {
		t.Fatal(err)
	}

	snapshot, err := store.ResolveRehydrationInputs(context.Background(), WaitingRecord{Key: key, InputRevision: "r4"})
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.RevisionIsNewer || snapshot.Inputs.InputRevision != "r5" || len(snapshot.Inputs.Delivered) != 2 || len(snapshot.NewlyDelivered) != 1 || snapshot.NewlyDelivered[0].InputID != "review" {
		t.Fatalf("unexpected input snapshot: %#v", snapshot)
	}
	newInputs.Delivered[1].ArtifactRefs[0] = "mutated"
	if snapshot.Inputs.Delivered[1].ArtifactRefs[0] != "artifact-review" {
		t.Fatal("authoritative input history was not copied")
	}
}

func TestInMemoryContinuationBindingStoreKeepsB1ImmutableAndCreatesB2(t *testing.T) {
	store := NewInMemoryContinuationBindingStore()
	b2, err := store.RebindInputsForContinuation(context.Background(), ContinuationBinding{
		InvocationID: "invocation-a", Generation: 3, ExecutionEpoch: 2,
		PreviousBindingRef: "B1", PreviousRevision: "r4", InputRevision: "r5",
	})
	if err != nil {
		t.Fatal(err)
	}
	if b2.BindingRef == "" || b2.BindingRef == "B1" || b2.PreviousBindingRef != "B1" || b2.PreviousRevision != "r4" || b2.InputRevision != "r5" {
		t.Fatalf("unexpected continuation binding: %#v", b2)
	}
	stored, ok := store.Resolve(b2.BindingRef)
	if !ok || stored != b2 {
		t.Fatalf("stored binding mismatch: %#v found=%t", stored, ok)
	}
}
