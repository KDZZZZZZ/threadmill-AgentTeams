package contextgraph

import (
	"context"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

var (
	_ WritePort            = (*MemoryStore)(nil)
	_ ContextGraphReader   = (*MemoryStore)(nil)
	_ ContextGraphSearcher = (*MemoryStore)(nil)
	_ ContextGraphCurator  = (*MemoryStore)(nil)
)

func TestCreateNodesBuildsCanonicalObjectsMembershipEdgesRevisionAuditOutbox(t *testing.T) {
	store := seededStore()
	ctxAgent := contextPrincipal(auth.ToolContextSubmitReview, auth.ToolContextExplore)

	for _, kind := range []NodeKind{NodeKindDirective, NodeKindFact, NodeKindHypothesis} {
		nodeID := "node-" + string(kind)
		result, err := store.CreateNodes(context.Background(), ctxAgent, CreateNodesRequest{
			ExpectedGraphRevision: store.GraphRevision(),
			Nodes: []CreateNodeInput{{
				Node: ContextNode{
					ID:             nodeID,
					Kind:           string(kind),
					Statement:      "statement " + string(kind),
					Status:         string(NodeStatusAccepted),
					SubgraphIDs:    []string{"general-a"},
					SourceRefs:     []string{"source:" + string(kind)},
					CreatorAgentID: "creator-1",
				},
				CreationContext: NodeCreationContext{
					CreatorAgentID:        "creator-1",
					PreviousNodeID:        previousNodeFor(kind),
					SubscribedSubgraphIDs: []string{"general-a", "task-1-context"},
				},
			}},
		})
		if err != nil {
			t.Fatalf("%s create node: %v", kind, err)
		}
		if result.GraphRevision == 0 || len(result.Nodes) != 1 || len(result.AuditEvents) != 1 || len(result.OutboxEvents) != 1 {
			t.Fatalf("%s result = %#v", kind, result)
		}
	}

	explore, err := store.Explore(context.Background(), ctxAgent, ExploreRequest{AnchorRef: "subgraph:general-a"})
	if err != nil {
		t.Fatal(err)
	}
	if len(explore.Nodes) != 3 {
		t.Fatalf("expected three nodes in general membership, got %#v", explore.Nodes)
	}
	if got := store.subgraphs["general-a"].Subgraph.Revision; got != 4 {
		t.Fatalf("subgraph revision = %d, want 4", got)
	}
	if len(store.AuditEvents()) != 3 {
		t.Fatalf("audit events = %d, want 3", len(store.AuditEvents()))
	}
	if len(store.OutboxEvents()) != 3 {
		t.Fatalf("outbox events = %d, want 3", len(store.OutboxEvents()))
	}
}

func TestCreateNodeRecentAndSubscribedEdgesAreDeterministic(t *testing.T) {
	store := seededStore()
	ctxAgent := contextPrincipal(auth.ToolContextSubmitReview)

	createNode(t, store, ctxAgent, "n1", "creator-1", "", nil)
	result := createNode(t, store, ctxAgent, "n2", "creator-1", "n1", []string{"general-a", "general-a", "missing-subgraph"})

	want := map[ContextEdge]bool{
		{FromRef: "node:n1", ToNodeID: "n2", Kind: string(EdgeKindLogicalAdjacent)}:                false,
		{FromRef: "subgraph:general-a", ToNodeID: "n2", Kind: string(EdgeKindDerivesFromSubgraph)}: false,
	}
	if len(result.Edges) != len(want) {
		t.Fatalf("edges = %#v, want logical_adjacent and one derives_from_subgraph", result.Edges)
	}
	for _, edge := range result.Edges {
		if _, ok := want[edge]; !ok {
			t.Fatalf("unexpected edge %#v", edge)
		}
		want[edge] = true
	}
	for edge, seen := range want {
		if !seen {
			t.Fatalf("missing edge %#v", edge)
		}
	}
}

func TestCreateNodesRollsBackWhenAuditOrOutboxOrEdgeFails(t *testing.T) {
	for _, fault := range []Fault{FaultAudit, FaultOutbox, FaultEdge} {
		store := seededStore()
		ctxAgent := contextPrincipal(auth.ToolContextSubmitReview)
		beforeRevision := store.GraphRevision()
		beforeAudit := len(store.AuditEvents())
		beforeOutbox := len(store.OutboxEvents())
		store.SetNextFault(fault)

		_, err := store.CreateNodes(context.Background(), ctxAgent, CreateNodesRequest{
			ExpectedGraphRevision: beforeRevision,
			Nodes: []CreateNodeInput{{
				Node: ContextNode{
					ID:             "fault-node",
					Kind:           string(NodeKindFact),
					Statement:      "rollback candidate",
					Status:         string(NodeStatusAccepted),
					SubgraphIDs:    []string{"general-a"},
					SourceRefs:     []string{"source:rollback"},
					CreatorAgentID: "creator-1",
				},
				CreationContext: NodeCreationContext{
					CreatorAgentID:        "creator-1",
					PreviousNodeID:        "seed-node",
					SubscribedSubgraphIDs: []string{"general-a"},
				},
			}},
		})
		if err == nil {
			t.Fatalf("%s expected injected error", fault)
		}
		if store.GraphRevision() != beforeRevision {
			t.Fatalf("%s graph revision changed on rollback", fault)
		}
		if _, ok := store.nodes["fault-node"]; ok {
			t.Fatalf("%s left node after rollback", fault)
		}
		if len(store.AuditEvents()) != beforeAudit || len(store.OutboxEvents()) != beforeOutbox {
			t.Fatalf("%s audit/outbox changed after rollback", fault)
		}
	}
}

func TestCuratorMutationsAreAtomicWithAuditOutbox(t *testing.T) {
	for _, fault := range []Fault{FaultAudit, FaultOutbox} {
		store := seededStore()
		ctxAgent := contextPrincipal(auth.ToolContextCreateNode, auth.ToolContextUpdateNode)
		ref, err := store.CreateNode(context.Background(), ctxAgent, CreateGeneralNodeRequest{
			Statement:   "curator node",
			Kind:        string(NodeKindFact),
			SourceRefs:  []string{"source:curator"},
			SubgraphIDs: []string{"general-a"},
		})
		if err != nil {
			t.Fatal(err)
		}
		beforeRevision := store.GraphRevision()
		beforeAudit := len(store.AuditEvents())
		beforeOutbox := len(store.OutboxEvents())
		beforeNode := store.nodes[string(ref)].Node
		store.SetNextFault(fault)

		_, err = store.UpdateNode(context.Background(), ctxAgent, UpdateGeneralNodeRequest{
			NodeID:         string(ref),
			SourceRevision: "1",
			Statement:      "changed",
			Kind:           string(NodeKindFact),
			Status:         string(NodeStatusAccepted),
			SourceRefs:     []string{"source:changed"},
			SubgraphIDs:    []string{"general-a"},
		})
		if err == nil {
			t.Fatalf("%s expected injected error", fault)
		}
		if store.GraphRevision() != beforeRevision {
			t.Fatalf("%s graph revision changed on rollback", fault)
		}
		if got := store.nodes[string(ref)].Node; got.Statement != beforeNode.Statement || got.CreatorAgentID != beforeNode.CreatorAgentID {
			t.Fatalf("%s node changed after rollback: %#v", fault, got)
		}
		if len(store.AuditEvents()) != beforeAudit || len(store.OutboxEvents()) != beforeOutbox {
			t.Fatalf("%s audit/outbox changed after rollback", fault)
		}
	}
}

func TestReaderDoesNotMutateGraph(t *testing.T) {
	store := seededStore()
	phase := principal(auth.RoleExecutor, "phase-agent", "task-1", auth.ToolSet(auth.ToolContextListSubgraphs, auth.ToolContextExplore))
	beforeRevision := store.GraphRevision()
	beforeAudit := len(store.AuditEvents())
	beforeOutbox := len(store.OutboxEvents())

	if _, err := store.ListSubgraphs(context.Background(), phase, ListSubgraphsRequest{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Explore(context.Background(), phase, ExploreRequest{AnchorRef: "subgraph:general-a"}); err != nil {
		t.Fatal(err)
	}
	if store.GraphRevision() != beforeRevision || len(store.AuditEvents()) != beforeAudit || len(store.OutboxEvents()) != beforeOutbox {
		t.Fatal("read path mutated graph, audit, or outbox")
	}
}

func TestReturnedNodesAndEdgesDoNotLeakHiddenSubgraphs(t *testing.T) {
	store := seededStore()
	ctxAgent := contextPrincipal(auth.ToolContextExplore, auth.ToolContextSearch)
	store.nodes["mixed"] = NodeRecord{
		Node: ContextNode{
			ID:             "mixed",
			Kind:           string(NodeKindFact),
			Statement:      "needle mixed",
			Status:         string(NodeStatusAccepted),
			SubgraphIDs:    []string{"general-a", "task-1-context"},
			SourceRefs:     []string{"source:mixed"},
			CreatorAgentID: "creator-1",
		},
		Revision:  1,
		ProjectID: "project-a",
		Sequence:  2,
	}
	store.edges[edgeKey(ContextEdge{FromRef: "subgraph:task-1-context", ToNodeID: "mixed", Kind: string(EdgeKindDerivesFromSubgraph)})] = ContextEdge{
		FromRef:  "subgraph:task-1-context",
		ToNodeID: "mixed",
		Kind:     string(EdgeKindDerivesFromSubgraph),
	}

	explore, err := store.Explore(context.Background(), ctxAgent, ExploreRequest{AnchorRef: "node:mixed"})
	if err != nil {
		t.Fatal(err)
	}
	if len(explore.Nodes) != 1 || len(explore.Nodes[0].SubgraphIDs) != 1 || explore.Nodes[0].SubgraphIDs[0] != "general-a" {
		t.Fatalf("visible node leaked hidden subgraphs: %#v", explore.Nodes)
	}
	for _, frontier := range explore.Frontier {
		if frontier == "subgraph:task-1-context" {
			t.Fatalf("frontier leaked task subgraph: %#v", explore.Frontier)
		}
	}
}

func TestSearchAuthorizesScopeAndAnchorsBeforeMatching(t *testing.T) {
	store := seededStore()
	reviewer := contextPrincipal(auth.ToolContextSubmitReview)
	createNode(t, store, reviewer, "visible", "creator-1", "", nil)
	ctxAgent := contextPrincipal(auth.ToolContextSearch)
	ctxAgent.ConsumerInvocationID = "inv-search-caller"

	search, err := store.Search(context.Background(), ctxAgent, SearchRequest{Keywords: []string{"needle"}, Scope: []string{"subgraph:general-a"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(search.Slice.Nodes) != 1 || search.Slice.Nodes[0].ID != "visible" {
		t.Fatalf("search returned unauthorized or missing nodes: %#v", search.Slice.Nodes)
	}
	if len(search.SubscriptionIDs) != 1 {
		t.Fatalf("subscription IDs = %#v, want one search subscription", search.SubscriptionIDs)
	}
	if _, err := store.Search(context.Background(), ctxAgent, SearchRequest{Keywords: []string{"needle"}, Scope: []string{"task-1-context"}}); !kernel.IsCode(err, kernel.CodeNotFound) {
		t.Fatalf("hidden scope err = %v, want not_found", err)
	}
	if _, err := store.Search(context.Background(), ctxAgent, SearchRequest{AnchorRefs: []string{"node:missing"}}); !kernel.IsCode(err, kernel.CodeNotFound) {
		t.Fatalf("missing anchor err = %v, want not_found", err)
	}
}

func TestSearchIsContextAgentOnlyEvenIfCalledDirectly(t *testing.T) {
	store := seededStore()
	for _, role := range []auth.Role{auth.RolePlanner, auth.RoleExecutor, auth.RoleVerifier, auth.RoleTaskManager} {
		p := principal(role, "agent", "task-1", auth.ToolSet(auth.ToolContextSearch, auth.ToolContextExplore))
		_, err := store.Search(context.Background(), p, SearchRequest{Keywords: []string{"seed"}})
		if !kernel.IsCode(err, kernel.CodeForbidden) {
			t.Fatalf("%s direct Search err = %v, want forbidden", role, err)
		}
	}
}

func TestCuratorRejectsTaskTargetsAndPreservesCreator(t *testing.T) {
	store := seededStore()
	ctxAgent := contextPrincipal(
		auth.ToolContextCreateNode,
		auth.ToolContextUpdateNode,
		auth.ToolContextDeleteNode,
		auth.ToolContextUpdateSubgraph,
		auth.ToolContextDeleteSubgraph,
	)
	if _, err := store.CreateNode(context.Background(), ctxAgent, CreateGeneralNodeRequest{
		Statement:   "bad task write",
		Kind:        string(NodeKindFact),
		SourceRefs:  []string{"source:bad"},
		SubgraphIDs: []string{"task-1-context"},
	}); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("create task node err = %v, want forbidden", err)
	}
	if _, err := store.UpdateSubgraph(context.Background(), ctxAgent, UpdateGeneralSubgraphRequest{SubgraphID: "task-1-context", Revision: 1}); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("update task subgraph err = %v, want forbidden", err)
	}
	if err := store.DeleteSubgraph(context.Background(), ctxAgent, DeleteGeneralSubgraphRequest{SubgraphID: "task-1-context", Revision: 1}); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("delete task subgraph err = %v, want forbidden", err)
	}
	ref, err := store.CreateNode(context.Background(), ctxAgent, CreateGeneralNodeRequest{
		Statement:   "owned",
		Kind:        string(NodeKindFact),
		SourceRefs:  []string{"source:owned"},
		SubgraphIDs: []string{"general-a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	originalCreator := store.nodes[string(ref)].Node.CreatorAgentID
	if _, err := store.UpdateNode(context.Background(), ctxAgent, UpdateGeneralNodeRequest{
		NodeID:         string(ref),
		SourceRevision: "1",
		Statement:      "changed",
		Kind:           string(NodeKindFact),
		Status:         string(NodeStatusAccepted),
		SourceRefs:     []string{"source:changed"},
		SubgraphIDs:    []string{"general-a"},
	}); err != nil {
		t.Fatal(err)
	}
	if got := store.nodes[string(ref)].Node.CreatorAgentID; got != originalCreator {
		t.Fatalf("CreatorAgentID changed from %q to %q", originalCreator, got)
	}
}

func TestRevisionCAS(t *testing.T) {
	store := seededStore()
	curator := contextPrincipal(auth.ToolContextUpdateSubgraph, auth.ToolContextUpdateNode)
	reviewer := contextPrincipal(auth.ToolContextSubmitReview)
	if _, err := store.UpdateSubgraph(context.Background(), curator, UpdateGeneralSubgraphRequest{SubgraphID: "general-a", Name: "bad", Revision: 999}); !kernel.IsCode(err, kernel.CodeRevisionConflict) {
		t.Fatalf("update subgraph stale err = %v, want revision_conflict", err)
	}
	createNode(t, store, reviewer, "cas-node", "creator-1", "", nil)
	if _, err := store.UpdateNode(context.Background(), curator, UpdateGeneralNodeRequest{
		NodeID:         "cas-node",
		SourceRevision: "999",
		Statement:      "changed",
		Kind:           string(NodeKindFact),
		Status:         string(NodeStatusAccepted),
		SubgraphIDs:    []string{"general-a"},
		SourceRefs:     []string{"source:changed"},
	}); !kernel.IsCode(err, kernel.CodeRevisionConflict) {
		t.Fatalf("update node stale err = %v, want revision_conflict", err)
	}
}

func seededStore() *MemoryStore {
	now := time.Date(2026, 8, 11, 8, 0, 0, 0, time.UTC)
	store := NewMemoryStore(func() time.Time { return now })
	store.subgraphs["general-a"] = SubgraphRecord{
		Subgraph:  ContextSubgraph{ID: "general-a", Name: "General A", Summary: "general context", Revision: 1, Kind: string(SubgraphKindGeneral)},
		ProjectID: "project-a",
		CreatedAt: now,
		UpdatedAt: now,
	}
	store.subgraphs["task-1-context"] = SubgraphRecord{
		Subgraph:  ContextSubgraph{ID: "task-1-context", Name: "Task 1", Summary: "task context", Revision: 1, Kind: string(SubgraphKindTask)},
		ProjectID: "project-a",
		TaskID:    "task-1",
		CreatedAt: now,
		UpdatedAt: now,
	}
	store.subgraphs["general-b"] = SubgraphRecord{
		Subgraph:  ContextSubgraph{ID: "general-b", Name: "General B", Summary: "other project", Revision: 1, Kind: string(SubgraphKindGeneral)},
		ProjectID: "project-b",
		CreatedAt: now,
		UpdatedAt: now,
	}
	return store
}

func createNode(t *testing.T, store *MemoryStore, principal auth.Principal, id, creator, previous string, subscribed []string) CreateNodesResult {
	t.Helper()
	if subscribed == nil {
		subscribed = []string{"general-a"}
	}
	result, err := store.CreateNodes(context.Background(), principal, CreateNodesRequest{
		ExpectedGraphRevision: store.GraphRevision(),
		Nodes: []CreateNodeInput{{
			Node: ContextNode{
				ID:             id,
				Kind:           string(NodeKindFact),
				Statement:      "needle " + id,
				Status:         string(NodeStatusAccepted),
				SubgraphIDs:    []string{"general-a"},
				SourceRefs:     []string{"source:" + id},
				CreatorAgentID: creator,
			},
			CreationContext: NodeCreationContext{
				CreatorAgentID:        creator,
				PreviousNodeID:        previous,
				SubscribedSubgraphIDs: subscribed,
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func previousNodeFor(kind NodeKind) string {
	if kind == NodeKindDirective {
		return ""
	}
	return "node-directive"
}

func contextPrincipal(tools ...auth.Tool) auth.Principal {
	operation := "retrieve"
	for _, tool := range tools {
		switch tool {
		case auth.ToolContextCreateNode,
			auth.ToolContextUpdateNode,
			auth.ToolContextDeleteNode,
			auth.ToolContextCreateSubgraph,
			auth.ToolContextUpdateSubgraph,
			auth.ToolContextDeleteSubgraph:
			operation = "curate"
		case auth.ToolContextSubmitReview:
			if operation != "curate" {
				operation = "review"
			}
		}
	}
	p := principal(auth.RoleContext, "ctx-agent", "", auth.ToolSet(tools...))
	p.Operation = operation
	return p
}

func principal(role auth.Role, actor string, taskID kernel.TaskID, tools map[auth.Tool]struct{}) auth.Principal {
	return auth.Principal{
		ActorPrincipalID: kernel.ActorPrincipalID(actor),
		Kind:             auth.PrincipalAgent,
		ProjectID:        "project-a",
		Role:             role,
		TaskID:           taskID,
		InvocationID:     kernel.InvocationID("inv-" + actor),
		Tools:            tools,
		AuthenticatedAt:  time.Date(2026, 8, 11, 8, 0, 0, 0, time.UTC),
	}
}
