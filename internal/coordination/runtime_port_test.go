package coordination

import (
	"context"
	"testing"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

func TestNewRuntimeExposesOnlyReconcileAndUsesAuthoritativeStore(t *testing.T) {
	store := NewMemoryStore()
	controller := &recordingController{}
	runner, err := NewRuntime(RuntimeOptions{
		ProjectID:       kernel.ProjectID("project-runtime-port"),
		Store:           store,
		PhaseController: controller,
	})
	if err != nil {
		t.Fatalf("NewRuntime failed: %v", err)
	}
	if err := runner.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
}

func TestNewRuntimeFailsClosedWithoutRuntimeCapableStoreOrController(t *testing.T) {
	_, err := NewRuntime(RuntimeOptions{
		ProjectID:       kernel.ProjectID("project-runtime-port"),
		Store:           readOnlyCoordinationStore{},
		PhaseController: &recordingController{},
	})
	if err == nil {
		t.Fatal("NewRuntime accepted a store without runtime recovery state")
	}

	_, err = NewRuntime(RuntimeOptions{
		ProjectID: kernel.ProjectID("project-runtime-port"),
		Store:     NewMemoryStore(),
	})
	if err == nil {
		t.Fatal("NewRuntime accepted a missing phase controller")
	}
}

type readOnlyCoordinationStore struct{}

func (readOnlyCoordinationStore) Latest(context.Context, kernel.ProjectID) (GraphSnapshot, error) {
	return GraphSnapshot{}, nil
}

func (readOnlyCoordinationStore) Snapshot(context.Context, kernel.ProjectID, kernel.Revision) (GraphSnapshot, error) {
	return GraphSnapshot{}, nil
}

func (readOnlyCoordinationStore) ReplacePending(context.Context, kernel.ProjectID, PendingSubgraph) (GraphSnapshot, error) {
	return GraphSnapshot{}, nil
}

func (readOnlyCoordinationStore) Transition(context.Context, kernel.ProjectID, kernel.Revision, GraphTransition) (GraphSnapshot, error) {
	return GraphSnapshot{}, nil
}
