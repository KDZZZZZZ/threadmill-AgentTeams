package contextgraph

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

func TestPostgresContextGraphLargeFineGrainedLifecycle(t *testing.T) {
	ctx := context.Background()
	db := openContextGraphTestDB(t, ctx)
	defer db.Close()
	now := time.Date(2026, 8, 12, 2, 0, 0, 0, time.UTC)
	store := NewPostgresStore(db, func() time.Time { return now })
	resolver := &mutableTaskEndpointResolver{
		tasks: map[string]bool{"project-a/task-scale": true},
		done:  map[string]bool{"project-a/task-scale": true},
		endpoints: map[string]bool{
			"project-a/task-scale/plan": true,
		},
	}
	store.SetTaskEndpointResolver(resolver)
	curator := contextPrincipal(auth.ToolContextCreateSubgraph)
	reviewer := contextPrincipal(auth.ToolContextSubmitReview)

	const subgraphCount = 10
	const baseNodeCount = 1000
	subgraphs := make([]ContextSubgraph, 0, subgraphCount)
	for i := 0; i < subgraphCount; i++ {
		subgraph, err := store.CreateSubgraph(ctx, curator, CreateGeneralSubgraphRequest{
			Name: fmt.Sprintf("Scale domain %02d", i), Summary: "fine-grained reusable assertions",
		})
		if err != nil {
			t.Fatal(err)
		}
		subgraphs = append(subgraphs, subgraph)
	}
	revision, err := store.GraphRevision(ctx, "project-a")
	if err != nil {
		t.Fatal(err)
	}
	inputs := make([]CreateNodeInput, 0, baseNodeCount)
	for i := 0; i < baseNodeCount; i++ {
		subgraphID := subgraphs[i%subgraphCount].ID
		inputs = append(inputs, CreateNodeInput{
			Node: ContextNode{
				ID: fmt.Sprintf("scale-node-%04d", i), Kind: string(NodeKindFact),
				Statement: fmt.Sprintf("Domain %02d independently verified invariant critical-keyword-%04d.", i%subgraphCount, i),
				Status:    string(NodeStatusAccepted), SourceRefs: []string{fmt.Sprintf("evidence:scale:%04d", i)},
				SubgraphIDs: []string{subgraphID}, CreatorAgentID: string(reviewer.ActorPrincipalID),
			},
			CreationContext: NodeCreationContext{CreatorAgentID: string(reviewer.ActorPrincipalID), SubscribedSubgraphIDs: []string{subgraphID}},
		})
	}
	created, err := store.CreateNodes(ctx, reviewer, CreateNodesRequest{ExpectedGraphRevision: revision, Nodes: inputs})
	if err != nil {
		t.Fatal(err)
	}
	if len(created.Nodes) != baseNodeCount {
		t.Fatalf("created nodes = %d, want %d", len(created.Nodes), baseNodeCount)
	}

	const consumerCount = 20
	consumers := make([]auth.Principal, 0, consumerCount)
	for i := 0; i < consumerCount; i++ {
		consumer := principal(auth.RoleExecutor, fmt.Sprintf("scale-phase-%02d", i), "task-scale", auth.ToolSet(auth.ToolContextSubscribe, auth.ToolAgentSubmitMemoryCandidate))
		consumer.InvocationID = kernel.InvocationID(fmt.Sprintf("inv-scale-phase-%02d", i))
		first, second := subgraphs[i%subgraphCount].ID, subgraphs[(i+1)%subgraphCount].ID
		if _, err := store.Subscribe(ctx, consumer, SubscribeRequest{SubgraphIDs: []string{first, second}}); err != nil {
			t.Fatal(err)
		}
		slice, err := store.MaterializeRuntimeContext(ctx, consumer)
		if err != nil {
			t.Fatal(err)
		}
		if len(slice.Nodes) != 200 {
			t.Fatalf("consumer %d materialized nodes = %d, want 200", i, len(slice.Nodes))
		}
		for _, node := range slice.Nodes {
			if len(node.SubgraphIDs) != 1 || node.SubgraphIDs[0] != first && node.SubgraphIDs[0] != second {
				t.Fatalf("consumer %d leaked node %s memberships=%#v", i, node.ID, node.SubgraphIDs)
			}
		}
		consumers = append(consumers, consumer)
	}

	searcher := contextPrincipal(auth.ToolContextSearch)
	searcher.ConsumerInvocationID = consumers[0].InvocationID
	searcher.ConsumerTaskID = consumers[0].TaskID
	searcher.ConsumerRole = consumers[0].Role
	hit, err := store.Search(ctx, searcher, SearchRequest{Keywords: []string{"critical-keyword-0040"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(hit.Slice.Nodes) != 1 || len(hit.SubscriptionIDs) != 1 {
		t.Fatalf("scale search result = nodes:%d subscriptions:%#v", len(hit.Slice.Nodes), hit.SubscriptionIDs)
	}
	reusedNodeRef := "node:" + hit.Slice.Nodes[0].ID
	taskManager := principal(auth.RoleTaskManager, "scale-task-manager", "task-scale", auth.ToolSet(
		auth.ToolContextRegisterTaskSubgraph, auth.ToolContextProjectTaskContext, auth.ToolContextFinalizeTaskMemory,
	))
	binding, err := store.RegisterTaskSubgraph(ctx, taskManager, "task-scale")
	if err != nil {
		t.Fatal(err)
	}

	const candidateCount = 50
	candidateIDs := make([]string, 0, candidateCount)
	for i := 0; i < candidateCount; i++ {
		candidate, err := store.SubmitCandidate(ctx, consumers[0], SubmitCandidateRequest{Candidate: MemoryCandidate{
			Statement: fmt.Sprintf("Scale review assertion %04d captures an independently researched decision.", i),
			Kind:      string(NodeKindFact), SourceRefs: []string{fmt.Sprintf("evidence:scale-review:%04d", i), reusedNodeRef},
			SubgraphIDs: []string{subgraphs[0].ID},
		}})
		if err != nil {
			t.Fatal(err)
		}
		candidateIDs = append(candidateIDs, candidate.CandidateID)
	}

	projectionRefs := make(map[string]ContextNodeRef)
	for i := 0; i < 50; i++ {
		projection := TaskContextProjection{
			ProjectionID: fmt.Sprintf("scale-projection-%03d", i), SourceRevision: "1",
			Statement: fmt.Sprintf("Acceptance condition %03d remains independently addressable.", i), Kind: string(NodeKindDirective),
			SourceRefs: []string{fmt.Sprintf("contract:scale:%03d", i)}, SubgraphIDs: []string{binding.SubgraphID},
			Recipients: []TaskContextRecipient{{TaskID: "task-scale", EndpointRefs: []PhaseEndpointRef{{TaskID: "task-scale", EndpointID: "plan"}}}},
		}
		ref, err := store.ProjectTaskContext(ctx, taskManager, ProjectTaskContextRequest{Projection: projection})
		if err != nil {
			t.Fatal(err)
		}
		projectionRefs[projection.ProjectionID] = ref
		if i < 10 {
			projection.SourceRevision = "2"
			projection.SourceRefs = append(projection.SourceRefs, "evidence:projection-refresh")
			updated, err := store.ProjectTaskContext(ctx, taskManager, ProjectTaskContextRequest{Projection: projection})
			if err != nil {
				t.Fatal(err)
			}
			if updated != ref {
				t.Fatalf("projection %s changed node ref from %s to %s", projection.ProjectionID, ref, updated)
			}
		}
	}

	batch, err := store.FinalizeTaskMemory(ctx, taskManager, "task-scale")
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Candidates) != candidateCount {
		t.Fatalf("frozen candidates = %d, want %d", len(batch.Candidates), candidateCount)
	}
	decisions := make([]CandidateReviewDecision, 0, candidateCount)
	for _, candidateID := range candidateIDs {
		decisions = append(decisions, CandidateReviewDecision{CandidateID: candidateID, Action: "create"})
	}
	reviewer.TaskID = "task-scale"
	reviewReceipt, err := store.SubmitReview(ctx, reviewer, CandidateReviewSubmission{Decisions: decisions})
	if err != nil {
		t.Fatal(err)
	}
	if len(reviewReceipt.NodeIDs) != candidateCount {
		t.Fatalf("reviewed nodes = %d, want %d", len(reviewReceipt.NodeIDs), candidateCount)
	}

	restarted := NewPostgresStore(db, func() time.Time { return now.Add(time.Minute) })
	restarted.SetTaskEndpointResolver(resolver)
	materialized, err := restarted.MaterializeRuntimeContext(ctx, consumers[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(materialized.Nodes) != 250 {
		t.Fatalf("restart materialized nodes = %d, want 250 base+review nodes", len(materialized.Nodes))
	}
	searcher.ConsumerInvocationID = consumers[1].InvocationID
	searcher.ConsumerTaskID = consumers[1].TaskID
	reviewHit, err := restarted.Search(ctx, searcher, SearchRequest{Keywords: []string{"review assertion 0049"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(reviewHit.Slice.Nodes) != 1 {
		t.Fatalf("reviewed atomic node search hits = %d, want 1", len(reviewHit.Slice.Nodes))
	}
	projectionLeak, err := restarted.Search(ctx, searcher, SearchRequest{Keywords: []string{"Acceptance condition 042"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(projectionLeak.Slice.Nodes) != 0 {
		t.Fatalf("general search leaked %d task projection nodes", len(projectionLeak.Slice.Nodes))
	}

	var nodeRows, distinctNodeRows, projectionRows, candidateRows int
	if err := db.QueryRowContext(ctx, `SELECT count(*), count(DISTINCT id) FROM context_nodes WHERE project_id='project-a'`).Scan(&nodeRows, &distinctNodeRows); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM context_task_projections WHERE project_id='project-a'`).Scan(&projectionRows); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM context_task_memory_candidates WHERE project_id='project-a' AND task_id='task-scale'`).Scan(&candidateRows); err != nil {
		t.Fatal(err)
	}
	if nodeRows != 1100 || distinctNodeRows != nodeRows || projectionRows != 50 || candidateRows != candidateCount {
		t.Fatalf("scale rows nodes=%d distinct=%d projections=%d candidates=%d", nodeRows, distinctNodeRows, projectionRows, candidateRows)
	}
}
