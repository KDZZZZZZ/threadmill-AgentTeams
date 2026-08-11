package contextgraph

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/postgres"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestPostgresStoreRuntimeCriticalContextGraph(t *testing.T) {
	ctx := context.Background()
	db := openContextGraphTestDB(t, ctx)
	defer db.Close()

	resolver := &mutableTaskEndpointResolver{
		tasks: map[string]bool{
			"project-a/task-a": true,
			"project-a/task-b": true,
		},
		done: map[string]bool{},
		endpoints: map[string]bool{
			"project-a/task-a/plan": true,
			"project-a/task-b/plan": true,
		},
	}
	now := time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)
	store := NewPostgresStore(db, func() time.Time { return now })
	store.SetTaskEndpointResolver(resolver)

	curator := contextPrincipal(auth.ToolContextCreateSubgraph, auth.ToolContextCreateNode)
	reviewer := contextPrincipal(auth.ToolContextSubmitReview)
	sgA, err := store.CreateSubgraph(ctx, curator, CreateGeneralSubgraphRequest{Name: "General A", Summary: "runtime"})
	if err != nil {
		t.Fatal(err)
	}
	sgB, err := store.CreateSubgraph(ctx, curator, CreateGeneralSubgraphRequest{Name: "General B", Summary: "runtime"})
	if err != nil {
		t.Fatal(err)
	}
	projectBCurator := curator
	projectBCurator.ProjectID = "project-b"
	projectBSubgraph, err := store.CreateSubgraph(ctx, projectBCurator, CreateGeneralSubgraphRequest{Name: "Project B First", Summary: "isolation"})
	if err != nil {
		t.Fatal(err)
	}
	if projectBSubgraph.ID == sgA.ID || projectBSubgraph.ID == sgB.ID {
		t.Fatalf("cross-project first subgraph id collided: %s", projectBSubgraph.ID)
	}
	if _, err := store.CreateNode(ctx, projectBCurator, CreateGeneralNodeRequest{
		Statement:   "project b first node",
		Kind:        string(NodeKindFact),
		SourceRefs:  []string{"source:project-b"},
		SubgraphIDs: []string{projectBSubgraph.ID},
	}); err != nil {
		t.Fatal(err)
	}
	var projectAOutboxBefore, projectBOutbox int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM context_outbox_events WHERE project_id = 'project-a'`).Scan(&projectAOutboxBefore); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM context_outbox_events WHERE project_id = 'project-b'`).Scan(&projectBOutbox); err != nil {
		t.Fatal(err)
	}
	if projectAOutboxBefore == 0 || projectBOutbox == 0 {
		t.Fatalf("outbox project isolation counts project-a=%d project-b=%d", projectAOutboxBefore, projectBOutbox)
	}
	phase := principal(auth.RoleExecutor, "phase-pg", "task-a", auth.ToolSet(
		auth.ToolContextSubscribe,
		auth.ToolContextUnsubscribe,
		auth.ToolAgentSubmitMemoryCandidate,
		auth.ToolAgentListTaskMemoryCandidates,
	))
	phase.InvocationID = "inv-pg-phase"
	phaseOtherTask := phase
	phaseOtherTask.ActorPrincipalID = "phase-task-b"
	phaseOtherTask.TaskID = "task-b"
	phaseOtherTask.InvocationID = "inv-task-b"

	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs <- retryContextGraphMutation(func() error {
				_, err := store.Subscribe(ctx, phase, SubscribeRequest{SubgraphIDs: []string{sgA.ID}})
				if err != nil {
					return fmt.Errorf("subscribe %d: %w", i, err)
				}
				return nil
			})
		}(i)
	}
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs <- retryContextGraphMutation(func() error {
				revision, err := store.GraphRevision(ctx, "project-a")
				if err != nil {
					return err
				}
				_, err = store.CreateNodes(ctx, reviewer, CreateNodesRequest{
					ExpectedGraphRevision: revision,
					Nodes: []CreateNodeInput{{
						Node: ContextNode{
							ID:             fmt.Sprintf("pg-node-%d", i),
							Kind:           string(NodeKindFact),
							Statement:      fmt.Sprintf("postgres concurrent %d", i),
							Status:         string(NodeStatusAccepted),
							SubgraphIDs:    []string{sgA.ID},
							SourceRefs:     []string{fmt.Sprintf("source:%d", i)},
							CreatorAgentID: "ctx-agent",
						},
						CreationContext: NodeCreationContext{CreatorAgentID: "ctx-agent", SubscribedSubgraphIDs: []string{sgA.ID}},
					}},
				})
				if err != nil {
					return fmt.Errorf("create nodes %d: %w", i, err)
				}
				return nil
			})
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	restarted := NewPostgresStore(db, func() time.Time { return now })
	restarted.SetTaskEndpointResolver(resolver)
	slice, err := restarted.MaterializeRuntimeContext(ctx, phase)
	if err != nil {
		t.Fatal(err)
	}
	if len(slice.Nodes) != 2 {
		t.Fatalf("restart materialized nodes = %#v, want two", slice.Nodes)
	}

	overlap, err := restarted.Subscribe(ctx, phase, SubscribeRequest{SubgraphIDs: []string{sgA.ID}})
	if err != nil {
		t.Fatal(err)
	}
	exclusive, err := restarted.Subscribe(ctx, phase, SubscribeRequest{SubgraphIDs: []string{sgB.ID}})
	if err != nil {
		t.Fatal(err)
	}
	wantAB := joinStrings(uniqueStrings([]string{sgA.ID, sgB.ID}))
	if got, err := restarted.EffectiveSubgraphs(ctx, phase); err != nil || joinStrings(got) != wantAB {
		t.Fatalf("effective union = %#v, %v", got, err)
	}
	if err := restarted.Unsubscribe(ctx, phase, overlap.ID); err != nil {
		t.Fatal(err)
	}
	if got, err := restarted.EffectiveSubgraphs(ctx, phase); err != nil || joinStrings(got) != wantAB {
		t.Fatalf("overlap cancel union = %#v, %v", got, err)
	}
	for _, id := range slice.SubscriptionIDs {
		if err := restarted.Unsubscribe(ctx, phase, id); err != nil {
			t.Fatal(err)
		}
	}
	if got, err := restarted.EffectiveSubgraphs(ctx, phase); err != nil || joinStrings(got) != sgB.ID {
		t.Fatalf("exclusive remaining union = %#v, %v", got, err)
	}

	if _, err := restarted.CreateNode(ctx, curator, CreateGeneralNodeRequest{
		Statement:   "delta after cancel",
		Kind:        string(NodeKindFact),
		SourceRefs:  []string{"source:delta"},
		SubgraphIDs: []string{sgB.ID},
	}); err != nil {
		t.Fatal(err)
	}
	pending, err := restarted.PendingDeltas(ctx, phase)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 {
		t.Fatalf("pending deltas = %#v, want one", pending)
	}
	if err := restarted.AckDelta(ctx, phase, pending[0].ID); err != nil {
		t.Fatal(err)
	}
	if pending, err = restarted.PendingDeltas(ctx, phase); err != nil || len(pending) != 0 {
		t.Fatalf("acked pending = %#v, %v", pending, err)
	}
	if err := restarted.Unsubscribe(ctx, phase, exclusive.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.CreateNode(ctx, curator, CreateGeneralNodeRequest{
		Statement:   "no delta after cancel",
		Kind:        string(NodeKindFact),
		SourceRefs:  []string{"source:no-delta"},
		SubgraphIDs: []string{sgB.ID},
	}); err != nil {
		t.Fatal(err)
	}
	if pending, err = restarted.PendingDeltas(ctx, phase); err != nil || len(pending) != 0 {
		t.Fatalf("canceled subscription delivered = %#v, %v", pending, err)
	}

	projectB := phase
	projectB.ProjectID = "project-b"
	if _, err := restarted.Subscribe(ctx, projectB, SubscribeRequest{SubgraphIDs: []string{sgA.ID}}); !kernel.IsCode(err, kernel.CodeNotFound) {
		t.Fatalf("cross-project subscribe err = %v, want not_found", err)
	}

	tm := principal(auth.RoleTaskManager, "tm-pg", "task-a", auth.ToolSet(
		auth.ToolContextRegisterTaskSubgraph,
		auth.ToolContextProjectTaskContext,
		auth.ToolContextFinalizeTaskMemory,
		auth.ToolContextSubscribe,
	))
	bindingA, err := restarted.RegisterTaskSubgraph(ctx, tm, "task-a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.RegisterTaskSubgraph(ctx, tm, "task-b"); err != nil {
		t.Fatal(err)
	}
	projection := TaskContextProjection{
		ProjectionID:   "pg-projection",
		SourceRevision: "1",
		Statement:      "task-a directive",
		Kind:           string(NodeKindDirective),
		SourceRefs:     []string{"contract:task-a"},
		SubgraphIDs:    []string{bindingA.SubgraphID},
		Recipients: []TaskContextRecipient{{
			TaskID:       "task-a",
			EndpointRefs: []PhaseEndpointRef{{TaskID: "task-a", EndpointID: "plan"}},
		}},
	}
	if _, err := restarted.ProjectTaskContext(ctx, tm, ProjectTaskContextRequest{Projection: projection}); err != nil {
		t.Fatal(err)
	}
	cross := projection
	cross.ProjectionID = "pg-cross"
	cross.Recipients[0].TaskID = "task-b"
	if _, err := restarted.ProjectTaskContext(ctx, tm, ProjectTaskContextRequest{Projection: cross}); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("cross-task projection err = %v, want forbidden", err)
	}
	if _, err := restarted.Subscribe(ctx, phaseOtherTask, SubscribeRequest{SubgraphIDs: []string{bindingA.SubgraphID}}); !kernel.IsCode(err, kernel.CodeNotFound) {
		t.Fatalf("cross-task subscribe err = %v, want not_found", err)
	}

	phaseContextSub, err := restarted.Subscribe(ctx, phase, SubscribeRequest{SubgraphIDs: []string{sgA.ID}})
	if err != nil {
		t.Fatal(err)
	}
	previousRevision, err := restarted.GraphRevision(ctx, "project-a")
	if err != nil {
		t.Fatal(err)
	}
	previousForCandidate, err := restarted.CreateNodes(ctx, reviewer, CreateNodesRequest{
		ExpectedGraphRevision: previousRevision,
		Nodes: []CreateNodeInput{{
			Node: ContextNode{
				ID:             "candidate-previous",
				Kind:           string(NodeKindFact),
				Statement:      "previous candidate context",
				Status:         string(NodeStatusAccepted),
				SubgraphIDs:    []string{sgA.ID},
				SourceRefs:     []string{"source:previous"},
				CreatorAgentID: string(phase.ActorPrincipalID),
			},
			CreationContext: NodeCreationContext{CreatorAgentID: string(phase.ActorPrincipalID), SubscribedSubgraphIDs: []string{sgA.ID}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := restarted.SubmitCandidate(ctx, phase, SubmitCandidateRequest{Candidate: MemoryCandidate{
		Statement:   "candidate fact",
		Kind:        string(NodeKindFact),
		SourceRefs:  []string{"artifact:candidate"},
		SubgraphIDs: []string{sgA.ID},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if view, err := restarted.ListTaskCandidates(ctx, phase); err != nil || len(view.Candidates) != 1 {
		t.Fatalf("candidate view = %#v, %v", view, err)
	}
	var creationRaw string
	if err := db.QueryRowContext(ctx, `SELECT creation_context::text FROM context_task_memory_candidates WHERE project_id = $1 AND task_id = $2 AND candidate_id = $3`, phase.ProjectID, phase.TaskID, candidate.CandidateID).Scan(&creationRaw); err != nil {
		t.Fatal(err)
	}
	var creation NodeCreationContext
	if err := json.Unmarshal([]byte(creationRaw), &creation); err != nil {
		t.Fatal(err)
	}
	if creation.CreatorAgentID != string(phase.ActorPrincipalID) || creation.PreviousNodeID != previousForCandidate.Nodes[0].ID || joinStrings(creation.SubscribedSubgraphIDs) != sgA.ID {
		t.Fatalf("persisted creation context = %#v, previous=%s subscription=%s", creation, previousForCandidate.Nodes[0].ID, phaseContextSub.ID)
	}
	restartedAfterCandidate := NewPostgresStore(db, func() time.Time { return now })
	restartedAfterCandidate.SetTaskEndpointResolver(resolver)
	if _, err := restarted.FinalizeTaskMemory(ctx, tm, "task-a"); !kernel.IsCode(err, kernel.CodeTransitionRejected) {
		t.Fatalf("finalize before done = %v, want transition_rejected", err)
	}
	resolver.done["project-a/task-a"] = true
	if batch, err := restartedAfterCandidate.FinalizeTaskMemory(ctx, tm, "task-a"); err != nil || len(batch.Candidates) != 1 {
		t.Fatalf("frozen batch = %#v, %v", batch, err)
	}
	if _, err := restartedAfterCandidate.SubmitCandidate(ctx, phase, SubmitCandidateRequest{Candidate: MemoryCandidate{Statement: "late", Kind: string(NodeKindFact), SourceRefs: []string{"late"}, SubgraphIDs: []string{sgA.ID}}}); !kernel.IsCode(err, kernel.CodeTransitionRejected) {
		t.Fatalf("late candidate = %v, want transition_rejected", err)
	}
	taskReviewer := contextPrincipal(auth.ToolContextSubmitReview)
	taskReviewer.TaskID = "task-a"
	receipt, err := restartedAfterCandidate.SubmitReview(ctx, taskReviewer, CandidateReviewSubmission{Decisions: []CandidateReviewDecision{{
		CandidateID: candidate.CandidateID,
		Action:      "create",
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(receipt.NodeIDs) != 1 {
		t.Fatalf("review receipt = %#v", receipt)
	}
	var creationEdgeCount int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM context_edges WHERE to_node_id = $1 AND from_ref IN ($2, $3)`, receipt.NodeIDs[0], "node:"+previousForCandidate.Nodes[0].ID, "subgraph:"+sgA.ID).Scan(&creationEdgeCount); err != nil {
		t.Fatal(err)
	}
	if creationEdgeCount != 2 {
		t.Fatalf("review did not reuse persisted creation context edges; count=%d", creationEdgeCount)
	}
	var projectAOutboxAfter int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM context_outbox_events WHERE project_id = 'project-a'`).Scan(&projectAOutboxAfter); err != nil {
		t.Fatal(err)
	}
	if projectAOutboxAfter <= projectAOutboxBefore {
		t.Fatalf("project-a outbox did not advance independently: before=%d after=%d", projectAOutboxBefore, projectAOutboxAfter)
	}
}

func TestPostgresEnsureInitialSliceIsAtomicAcrossStores(t *testing.T) {
	ctx := context.Background()
	db := openContextGraphTestDB(t, ctx)
	defer db.Close()

	now := time.Date(2026, 8, 11, 12, 30, 0, 0, time.UTC)
	storeA := NewPostgresStore(db, func() time.Time { return now })
	storeB := NewPostgresStore(db, func() time.Time { return now })
	curator := contextPrincipal(auth.ToolContextCreateSubgraph, auth.ToolContextCreateNode)
	sg, err := storeA.CreateSubgraph(ctx, curator, CreateGeneralSubgraphRequest{Name: "Initial Race", Summary: "runtime"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := storeA.CreateNode(ctx, curator, CreateGeneralNodeRequest{
		Statement:   "initial race node",
		Kind:        string(NodeKindFact),
		SourceRefs:  []string{"source:initial-race"},
		SubgraphIDs: []string{sg.ID},
	}); err != nil {
		t.Fatal(err)
	}
	phase := principal(auth.RoleExecutor, "phase-initial-race", "task-initial-race", auth.ToolSet(auth.ToolContextSubscribe))
	phase.InvocationID = "inv-initial-race"
	runContextGraphRace(t, func(i int) error {
		store := storeA
		if i%2 == 1 {
			store = storeB
		}
		return retryContextGraphMutation(func() error {
			_, err := store.EnsureInitialSlice(ctx, phase, []string{sg.ID})
			return err
		})
	})
	var activeInitial int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM context_subscriptions WHERE project_id = $1 AND consumer_invocation_id = $2 AND source = 'initial_slice' AND active`, phase.ProjectID, phase.InvocationID).Scan(&activeInitial); err != nil {
		t.Fatal(err)
	}
	if activeInitial != 1 {
		t.Fatalf("active initial subscriptions = %d, want 1", activeInitial)
	}
}

func TestMigration7003MergesHistoricalDuplicateInitialSubscriptions(t *testing.T) {
	ctx := context.Background()
	db := openContextGraphTestDB(t, ctx)
	defer db.Close()
	loaded, err := postgres.LoadMigrations(os.DirFS("../../"), "migrations")
	if err != nil {
		t.Fatal(err)
	}
	migrator := postgres.NewMigrator(db)
	if err := migrator.Rollback(ctx, loaded, "7003"); err != nil {
		t.Fatal(err)
	}
	rows := []struct {
		id        string
		subgraphs string
		events    string
		createdAt string
	}{
		{id: "hist-keep", subgraphs: `["sg-a","sg-b"]`, events: `["evt-a"]`, createdAt: "2026-08-11T10:00:00Z"},
		{id: "hist-dup-a", subgraphs: `["sg-b","sg-c"]`, events: `["evt-b"]`, createdAt: "2026-08-11T10:01:00Z"},
		{id: "hist-dup-b", subgraphs: `["sg-a"]`, events: `["evt-a","evt-b"]`, createdAt: "2026-08-11T10:02:00Z"},
	}
	for _, row := range rows {
		if _, err := db.ExecContext(ctx, `INSERT INTO context_subscriptions(id, project_id, consumer_invocation_id, subgraph_ids, event_kinds, permission_snapshot, source, active, created_at, task_id, role) VALUES ($1, 'project-hist', 'inv-hist', $2::jsonb, $3::jsonb, 'project-hist:task-hist:executor', 'initial_slice', true, $4::timestamptz, 'task-hist', 'executor')`, row.id, row.subgraphs, row.events, row.createdAt); err != nil {
			t.Fatal(err)
		}
	}
	if err := migrator.Apply(ctx, loaded); err != nil {
		t.Fatal(err)
	}
	records, err := historicalInitialSubscriptionRows(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 3 {
		t.Fatalf("records = %#v", records)
	}
	if records[0].id != "hist-keep" || !records[0].active || records[0].expired {
		t.Fatalf("keeper state = %#v", records[0])
	}
	if !reflect.DeepEqual(records[0].subgraphs, []string{"sg-a", "sg-b", "sg-c"}) {
		t.Fatalf("merged subgraphs = %#v", records[0].subgraphs)
	}
	if !reflect.DeepEqual(records[0].events, []string{"evt-a", "evt-b"}) {
		t.Fatalf("merged events = %#v", records[0].events)
	}
	for _, record := range records[1:] {
		if record.active || !record.expired {
			t.Fatalf("duplicate was not expired: %#v", record)
		}
	}
	var indexName sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT to_regclass('context_subscriptions_active_initial_once_idx')::text`).Scan(&indexName); err != nil {
		t.Fatal(err)
	}
	if !indexName.Valid || indexName.String != "context_subscriptions_active_initial_once_idx" {
		t.Fatalf("index = %#v", indexName)
	}
	if err := migrator.Rollback(ctx, loaded, "7003"); err != nil {
		t.Fatal(err)
	}
	recordsAfterDown, err := historicalInitialSubscriptionRows(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(recordsAfterDown, records) {
		t.Fatalf("down should only drop index, before=%#v after=%#v", records, recordsAfterDown)
	}
	if err := db.QueryRowContext(ctx, `SELECT to_regclass('context_subscriptions_active_initial_once_idx')::text`).Scan(&indexName); err != nil {
		t.Fatal(err)
	}
	if indexName.Valid {
		t.Fatalf("index still exists after down: %#v", indexName)
	}
}

func TestPostgresStoreSearchHonorsAnchorRefs(t *testing.T) {
	ctx := context.Background()
	db := openContextGraphTestDB(t, ctx)
	defer db.Close()

	now := time.Date(2026, 8, 11, 11, 0, 0, 0, time.UTC)
	store := NewPostgresStore(db, func() time.Time { return now })
	curator := contextPrincipal(auth.ToolContextCreateSubgraph, auth.ToolContextCreateNode)
	sgA, err := store.CreateSubgraph(ctx, curator, CreateGeneralSubgraphRequest{Name: "Anchor A", Summary: "runtime"})
	if err != nil {
		t.Fatal(err)
	}
	sgB, err := store.CreateSubgraph(ctx, curator, CreateGeneralSubgraphRequest{Name: "Anchor B", Summary: "runtime"})
	if err != nil {
		t.Fatal(err)
	}
	nodeA, err := store.CreateNode(ctx, curator, CreateGeneralNodeRequest{
		Statement:   "needle anchored alpha",
		Kind:        string(NodeKindFact),
		SourceRefs:  []string{"source:alpha"},
		SubgraphIDs: []string{sgA.ID},
	})
	if err != nil {
		t.Fatal(err)
	}
	nodeB, err := store.CreateNode(ctx, curator, CreateGeneralNodeRequest{
		Statement:   "needle anchored beta",
		Kind:        string(NodeKindFact),
		SourceRefs:  []string{"source:beta"},
		SubgraphIDs: []string{sgB.ID},
	})
	if err != nil {
		t.Fatal(err)
	}
	projectBCurator := curator
	projectBCurator.ProjectID = "project-b"
	projectBSubgraph, err := store.CreateSubgraph(ctx, projectBCurator, CreateGeneralSubgraphRequest{Name: "Project B Anchor", Summary: "isolation"})
	if err != nil {
		t.Fatal(err)
	}

	searcher := contextPrincipal(auth.ToolContextSearch)
	searcher.ConsumerInvocationID = "inv-anchor-consumer"
	searcher.ConsumerTaskID = "task-anchor"
	searcher.ConsumerRole = auth.RoleExecutor

	byNode, err := store.Search(ctx, searcher, SearchRequest{Keywords: []string{"needle"}, AnchorRefs: []string{"node:" + string(nodeA)}})
	if err != nil {
		t.Fatal(err)
	}
	if len(byNode.Slice.Nodes) != 1 || byNode.Slice.Nodes[0].ID != string(nodeA) {
		t.Fatalf("node anchor search = %#v, want %s only", byNode.Slice.Nodes, nodeA)
	}
	bySubgraph, err := store.Search(ctx, searcher, SearchRequest{Keywords: []string{"needle"}, AnchorRefs: []string{"subgraph:" + sgB.ID}})
	if err != nil {
		t.Fatal(err)
	}
	if len(bySubgraph.Slice.Nodes) != 1 || bySubgraph.Slice.Nodes[0].ID != string(nodeB) {
		t.Fatalf("subgraph anchor search = %#v, want %s only", bySubgraph.Slice.Nodes, nodeB)
	}
	disjoint, err := store.Search(ctx, searcher, SearchRequest{
		Keywords:   []string{"needle"},
		Scope:      []string{"subgraph:" + sgA.ID},
		AnchorRefs: []string{"subgraph:" + sgB.ID},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(disjoint.Slice.Nodes) != 0 || len(disjoint.SubscriptionIDs) != 0 {
		t.Fatalf("disjoint scope/anchor search = nodes %#v subscriptions %#v, want empty", disjoint.Slice.Nodes, disjoint.SubscriptionIDs)
	}
	if _, err := store.Search(ctx, searcher, SearchRequest{AnchorRefs: []string{"subgraph:" + projectBSubgraph.ID}}); !kernel.IsCode(err, kernel.CodeNotFound) {
		t.Fatalf("cross-project anchor err = %v, want not_found", err)
	}
}

func TestPostgresStoreTaskContextFirstCreateIsIdempotentUnderConcurrency(t *testing.T) {
	ctx := context.Background()
	db := openContextGraphTestDB(t, ctx)
	defer db.Close()

	resolver := &mutableTaskEndpointResolver{
		tasks: map[string]bool{
			"project-a/task-race": true,
		},
		done: map[string]bool{},
		endpoints: map[string]bool{
			"project-a/task-race/plan": true,
		},
	}
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	store := NewPostgresStore(db, func() time.Time { return now })
	store.SetTaskEndpointResolver(resolver)
	tm := principal(auth.RoleTaskManager, "tm-race", "task-race", auth.ToolSet(
		auth.ToolContextRegisterTaskSubgraph,
		auth.ToolContextProjectTaskContext,
	))

	var bindingResults [2]TaskContextSubgraphBinding
	runContextGraphRace(t, func(i int) error {
		binding, err := store.RegisterTaskSubgraph(ctx, tm, "task-race")
		if err != nil {
			return err
		}
		bindingResults[i] = binding
		return nil
	})
	if bindingResults[0].SubgraphID == "" || bindingResults[0] != bindingResults[1] {
		t.Fatalf("concurrent RegisterTaskSubgraph = %#v", bindingResults)
	}

	projection := TaskContextProjection{
		ProjectionID:   "projection-race",
		SourceRevision: "1",
		Statement:      "race directive",
		Kind:           string(NodeKindDirective),
		SourceRefs:     []string{"contract:race"},
		SubgraphIDs:    []string{bindingResults[0].SubgraphID},
		Recipients: []TaskContextRecipient{{
			TaskID:       "task-race",
			EndpointRefs: []PhaseEndpointRef{{TaskID: "task-race", EndpointID: "plan"}},
		}},
	}
	var nodeResults [2]ContextNodeRef
	runContextGraphRace(t, func(i int) error {
		nodeID, err := store.ProjectTaskContext(ctx, tm, ProjectTaskContextRequest{Projection: projection})
		if err != nil {
			return err
		}
		nodeResults[i] = nodeID
		return nil
	})
	if nodeResults[0] == "" || nodeResults[0] != nodeResults[1] {
		t.Fatalf("concurrent ProjectTaskContext = %#v", nodeResults)
	}
}

func runContextGraphRace(t *testing.T, fn func(int) error) {
	t.Helper()
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs <- fn(i)
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

type historicalInitialSubscriptionRow struct {
	id        string
	subgraphs []string
	events    []string
	active    bool
	expired   bool
}

func historicalInitialSubscriptionRows(ctx context.Context, db *sql.DB) ([]historicalInitialSubscriptionRow, error) {
	rows, err := db.QueryContext(ctx, `SELECT id, subgraph_ids::text, event_kinds::text, active, expired_at IS NOT NULL FROM context_subscriptions WHERE project_id = 'project-hist' AND consumer_invocation_id = 'inv-hist' ORDER BY created_at, id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []historicalInitialSubscriptionRow
	for rows.Next() {
		var row historicalInitialSubscriptionRow
		var rawSubgraphs, rawEvents string
		if err := rows.Scan(&row.id, &rawSubgraphs, &rawEvents, &row.active, &row.expired); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(rawSubgraphs), &row.subgraphs); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(rawEvents), &row.events); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func openContextGraphTestDB(t *testing.T, ctx context.Context) *sql.DB {
	t.Helper()
	dsn := os.Getenv("CONTEXTGRAPH_POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://threadmill_test@127.0.0.1:5432/threadmill_test?sslmode=disable"
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(2)
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping postgres %s: %v", dsn, err)
	}
	schema := fmt.Sprintf("contextgraph_test_%d", time.Now().UnixNano())
	if _, err := db.ExecContext(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DROP SCHEMA `+schema+` CASCADE`)
	})
	db.Close()
	db, err = sql.Open("pgx", withSearchPath(dsn, schema))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(2)
	migrations, err := postgres.LoadMigrations(os.DirFS("../../"), "migrations")
	if err != nil {
		t.Fatal(err)
	}
	if err := postgres.NewMigrator(db).Apply(ctx, migrations); err != nil {
		t.Fatal(err)
	}
	return db
}

func withSearchPath(dsn, schema string) string {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return dsn
	}
	query := parsed.Query()
	query.Set("options", "-c search_path="+schema)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func retryContextGraphMutation(fn func() error) error {
	var last error
	for i := 0; i < 5; i++ {
		if err := fn(); err != nil {
			last = err
			code := kernel.ErrorCodeOf(err)
			if code == kernel.CodeRevisionConflict || code == kernel.CodeCommandConflict || strings.Contains(err.Error(), "serialization") || strings.Contains(err.Error(), "bad connection") {
				time.Sleep(time.Duration(i+1) * 20 * time.Millisecond)
				continue
			}
			return err
		}
		return nil
	}
	return last
}

type mutableTaskEndpointResolver struct {
	tasks     map[string]bool
	done      map[string]bool
	endpoints map[string]bool
}

func (r *mutableTaskEndpointResolver) TaskExists(_ context.Context, projectID kernel.ProjectID, taskID kernel.TaskID) (bool, error) {
	return r.tasks[string(projectID)+"/"+string(taskID)], nil
}

func (r *mutableTaskEndpointResolver) TaskDone(_ context.Context, projectID kernel.ProjectID, taskID kernel.TaskID) (bool, error) {
	return r.done[string(projectID)+"/"+string(taskID)], nil
}

func (r *mutableTaskEndpointResolver) EndpointExists(_ context.Context, projectID kernel.ProjectID, endpoint PhaseEndpointRef) (bool, error) {
	return r.endpoints[string(projectID)+"/"+string(endpoint.TaskID)+"/"+string(endpoint.EndpointID)], nil
}
