package contextgraph

import (
	"context"
	"testing"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

func TestProjectContextSnapshotCountsDistinctInjectedInitialSlicesAndUsesPersistedEdges(t *testing.T) {
	store := seededStore()
	ctxAgent := contextPrincipal(auth.ToolContextSubmitReview)
	first := createNode(t, store, ctxAgent, "hot-node", "creator-1", "", []string{"general-a"}).Nodes[0]
	second := createNode(t, store, ctxAgent, "cold-node", "creator-1", first.ID, []string{"general-a"}).Nodes[0]

	phaseA := principal(auth.RoleExecutor, "phase-a", "task-1", auth.ToolSet(auth.ToolContextSubscribe))
	phaseB := principal(auth.RoleVerifier, "phase-b", "task-1", auth.ToolSet(auth.ToolContextSubscribe))
	if _, err := store.EnsureInitialSlice(context.Background(), phaseA, []string{"general-a"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnsureInitialSlice(context.Background(), phaseB, []string{"general-a"}); err != nil {
		t.Fatal(err)
	}
	duplicate := store.initialSlices[invocationScopeKey{ProjectID: phaseA.ProjectID, InvocationID: phaseA.InvocationID}]
	duplicate.Nodes = append(duplicate.Nodes, duplicate.Nodes[0])
	store.initialSlices[invocationScopeKey{ProjectID: phaseA.ProjectID, InvocationID: phaseA.InvocationID}] = duplicate

	got, err := store.ProjectContextSnapshot(context.Background(), "project-a")
	if err != nil {
		t.Fatal(err)
	}
	if got.ProjectID != "project-a" || got.Revision != store.GraphRevision() {
		t.Fatalf("snapshot identity = %#v", got)
	}
	assertSnapshotUsage(t, got.Nodes, first.ID, 2)
	assertSnapshotUsage(t, got.Nodes, second.ID, 2)
	if len(got.Edges) != 3 {
		t.Fatalf("edges = %#v, want persisted logical and subgraph creation edges", got.Edges)
	}
	if _, err := store.ProjectContextSnapshot(context.Background(), kernel.ProjectID("")); !kernel.IsCode(err, kernel.CodeInvalidRequest) {
		t.Fatalf("empty project err = %v, want invalid_request", err)
	}
}

func assertSnapshotUsage(t *testing.T, nodes []ContextSnapshotNode, nodeID string, usage int) {
	t.Helper()
	for _, node := range nodes {
		if node.NodeID != nodeID {
			continue
		}
		if node.UsageCount != usage || node.LastUsedAt == nil {
			t.Fatalf("node %s usage = %d last=%v, want %d and timestamp", nodeID, node.UsageCount, node.LastUsedAt, usage)
		}
		return
	}
	t.Fatalf("node %s not found in snapshot: %#v", nodeID, nodes)
}
