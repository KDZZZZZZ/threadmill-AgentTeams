package app

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/adapters/agentteams"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/contextagent"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/contextgraph"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	runtimepkg "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime/promptcatalog"
)

func TestProductionContextRetrieveRunsBoundInvocationAndCapturesStructuredSearchAgainstPostgres(t *testing.T) {
	databaseURL := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("THREADMILL_TEST_DATABASE_URL is required for the real PostgreSQL integration test")
	}
	ctx := context.Background()
	db := openProductionPhasePostgres(t, ctx, databaseURL)
	defer db.Close()

	projectID := kernel.ProjectID("project-context-retrieve")
	graph := coordination.NewPostgresStore(db)
	seedProductionPhaseTask(t, ctx, db, projectID, graph)
	now := time.Now().UTC()
	contexts := contextgraph.NewPostgresStore(db, time.Now)
	contexts.SetTaskEndpointResolver(productionTaskEndpointResolver{projectID: projectID, graph: graph})
	assembler := productionPhaseTestAssembler(t)
	runtime, err := newProductionContextRuntime(db, projectID, "room-context", assembler, contexts, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	runtime.waitTimeout = 2 * time.Second
	runtime.pollEvery = 10 * time.Millisecond

	curator := auth.Principal{
		ActorPrincipalID: "context-curator", Kind: auth.PrincipalAgent, ProjectID: projectID,
		Role: auth.RoleContext, Operation: "curate", InvocationID: "context-curate-seed",
		Tools: auth.ToolSet(auth.ToolContextCreateNode, auth.ToolContextCreateSubgraph), AuthenticatedAt: now,
	}
	subgraph, err := contexts.CreateSubgraph(ctx, curator, contextgraph.CreateGeneralSubgraphRequest{Name: "AgentTeams 并发运行事实", Summary: "真实并发测试和约束", NodeIDs: []string{}})
	if err != nil {
		t.Fatal(err)
	}
	nodeID, err := contexts.CreateNode(ctx, curator, contextgraph.CreateGeneralNodeRequest{
		Statement: "高并发协调图必须保留八个真实 AgentTeams worker 同时执行的证据。",
		Kind:      string(contextgraph.NodeKindFact), SourceRefs: []string{"evidence://scale-run/worker-peak"}, SubgraphIDs: []string{subgraph.ID},
	})
	if err != nil {
		t.Fatal(err)
	}

	consumer := runtimepkg.Invocation{
		ID: "tm-context-consumer", ActorPrincipalID: "task-manager-context", ProjectID: projectID,
		TaskID: "task-real", Role: auth.RoleTaskManager, Status: runtimepkg.InvocationRunning,
		CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	consumerAssembly, err := assembler.Assemble(consumer, promptcatalog.RenderData{})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.invocations.Create(ctx, consumerAssembly.Invocation); err != nil {
		t.Fatal(err)
	}
	caller := principalForContextTest(consumerAssembly.Invocation, now)

	searcher := productionContextSearcher{searcher: contexts, runtime: runtime}
	adapter := &productionContextTestAdapter{}
	adapter.onDispatch = func(ref string) error {
		invocation, ok, err := runtime.invocations.Get(ctx, kernel.InvocationID(ref))
		if err != nil || !ok {
			return err
		}
		principal := principalForContextTest(invocation, now)
		_, err = searcher.Search(ctx, principal, contextgraph.SearchRequest{Keywords: []string{"高并发"}})
		return err
	}
	if err := runtime.setDispatcher(adapter); err != nil {
		t.Fatal(err)
	}

	result, err := runtime.RetrieveForConsumer(ctx, caller, contextagent.ContextRetrieveRequest{Query: "检索高并发协调图的真实 worker 证据"})
	if err != nil {
		t.Fatalf("RetrieveForConsumer() error = %v", err)
	}
	if len(result.Slice.Nodes) != 1 || result.Slice.Nodes[0].ID != string(nodeID) {
		t.Fatalf("retrieve nodes = %#v, want seeded node %s", result.Slice.Nodes, nodeID)
	}
	if len(result.SubscriptionIDs) == 0 || !strings.Contains(result.Explanation, "keywords=高并发") {
		t.Fatalf("retrieve result did not preserve structured search/subscription: %#v", result)
	}
	var completed, captured int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM production_context_invocations WHERE project_id=$1 AND operation='retrieve' AND state='completed'`, projectID).Scan(&completed); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM production_context_retrieve_results WHERE project_id=$1`, projectID).Scan(&captured); err != nil {
		t.Fatal(err)
	}
	if completed != 1 || captured != 1 || adapter.terminateCalls == 0 {
		t.Fatalf("retrieve lifecycle completed=%d captured=%d terminate=%d", completed, captured, adapter.terminateCalls)
	}
	prepared, err := runtime.LoadPreparedInvocation(ctx, adapter.firstInvocation())
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Role != auth.RoleContext || prepared.Operation != "retrieve" || !strings.Contains(prepared.Spec, "检索高并发协调图") {
		t.Fatalf("prepared Context Agent invocation = %#v", prepared)
	}
}

func TestProductionContextRetrieveBudgetIsTransactionalAndIdempotentAgainstPostgres(t *testing.T) {
	databaseURL := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("THREADMILL_TEST_DATABASE_URL is required for the real PostgreSQL integration test")
	}
	ctx := context.Background()
	db := openProductionPhasePostgres(t, ctx, databaseURL)
	defer db.Close()

	projectID := kernel.ProjectID("project-context-retrieve-budget")
	graph := coordination.NewPostgresStore(db)
	seedProductionPhaseTask(t, ctx, db, projectID, graph)
	now := time.Now().UTC()
	contexts := contextgraph.NewPostgresStore(db, time.Now)
	contexts.SetTaskEndpointResolver(productionTaskEndpointResolver{projectID: projectID, graph: graph})
	runtime, err := newProductionContextRuntime(db, projectID, "room-context-budget", productionPhaseTestAssembler(t), contexts, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	consumer := runtimepkg.Invocation{
		ID: "manager-context-budget-consumer", ActorPrincipalID: "manager-context-budget", ProjectID: projectID,
		TaskID: "task-real", Role: auth.RoleTaskManager, Status: runtimepkg.InvocationRunning,
		CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	consumerAssembly, err := runtime.assembler.Assemble(consumer, promptcatalog.RenderData{})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.invocations.Create(ctx, consumerAssembly.Invocation); err != nil {
		t.Fatal(err)
	}
	caller := principalForContextTest(consumerAssembly.Invocation, now)

	first, err := runtime.ensureInvocation(ctx, "retrieve", stableProductionSuffix(caller.InvocationID, "query-one"), caller.TaskID, caller, map[string]string{"operation": "retrieve", "query": "query-one"})
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := runtime.ensureInvocation(ctx, "retrieve", stableProductionSuffix(caller.InvocationID, "query-one"), caller.TaskID, caller, map[string]string{"operation": "retrieve", "query": "query-one"})
	if err != nil || replayed != first {
		t.Fatalf("idempotent retrieve = %q, %v; want %q", replayed, err, first)
	}
	for _, query := range []string{"query-two", "query-three"} {
		if _, err := runtime.ensureInvocation(ctx, "retrieve", stableProductionSuffix(caller.InvocationID, query), caller.TaskID, caller, map[string]string{"operation": "retrieve", "query": query}); err != nil {
			t.Fatalf("ensure %s: %v", query, err)
		}
	}
	if _, err := runtime.ensureInvocation(ctx, "retrieve", stableProductionSuffix(caller.InvocationID, "query-four"), caller.TaskID, caller, map[string]string{"operation": "retrieve", "query": "query-four"}); !kernel.IsCode(err, kernel.CodeInvalidRequest) {
		t.Fatalf("fourth unique retrieve error = %v, want invalid_request", err)
	}
	var count int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM production_context_invocations WHERE project_id=$1 AND operation='retrieve' AND consumer_invocation_id=$2`, projectID, caller.InvocationID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != productionContextRetrieveLimit {
		t.Fatalf("persisted retrieve count = %d, want %d", count, productionContextRetrieveLimit)
	}
}

func TestProductionContextReviewPromotesEveryAtomicCandidateAndRedactsTaskManagerResultAgainstPostgres(t *testing.T) {
	databaseURL := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("THREADMILL_TEST_DATABASE_URL is required for the real PostgreSQL integration test")
	}
	ctx := context.Background()
	db := openProductionPhasePostgres(t, ctx, databaseURL)
	defer db.Close()

	projectID := kernel.ProjectID("project-context-review")
	graph := coordination.NewPostgresStore(db)
	seedProductionPhaseTask(t, ctx, db, projectID, graph)
	contexts := contextgraph.NewPostgresStore(db, time.Now)
	contexts.SetTaskEndpointResolver(productionTaskEndpointResolver{projectID: projectID, graph: graph})
	now := time.Now().UTC()
	taskManager := auth.Principal{
		ActorPrincipalID: "task-manager-review", Kind: auth.PrincipalAgent, ProjectID: projectID,
		Role: auth.RoleTaskManager, InvocationID: "tm-review", TaskID: "task-real",
		Tools: auth.ToolSet(auth.ToolContextRegisterTaskSubgraph, auth.ToolContextFinalizeTaskMemory), AuthenticatedAt: now,
	}
	if _, err := contexts.RegisterTaskSubgraph(ctx, taskManager, "task-real"); err != nil {
		t.Fatal(err)
	}
	phase := auth.Principal{
		ActorPrincipalID: "planner-review", Kind: auth.PrincipalAgent, ProjectID: projectID,
		Role: auth.RolePlanner, TaskID: "task-real", InvocationID: "planner-review-inv",
		Tools: auth.ToolSet(auth.ToolAgentSubmitMemoryCandidate), AuthenticatedAt: now,
	}
	statements := []string{
		"八个并行 worker 是当前本机容器资源上稳定达到的并发峰值。",
		"Context Agent 的空关键词检索会退化为全图读取，因此生产入口必须拒绝。",
		"Task Memory 审查必须逐条保留独立论断，不能压缩为一个总结节点。",
	}
	for index, statement := range statements {
		if _, err := contexts.SubmitCandidate(ctx, phase, contextgraph.SubmitCandidateRequest{Candidate: contextgraph.MemoryCandidate{
			Statement: statement, Kind: string(contextgraph.NodeKindFact), SourceRefs: []string{"evidence://context-review/" + string(rune('a'+index))},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	markProductionTaskDone(t, ctx, graph, projectID, "task-real")

	runtime, err := newProductionContextRuntime(db, projectID, "room-context", productionPhaseTestAssembler(t), contexts, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	adapter := &productionContextTestAdapter{}
	if err := runtime.setDispatcher(adapter); err != nil {
		t.Fatal(err)
	}
	redacted, err := runtime.FinalizeTaskMemory(ctx, taskManager, "task-real")
	if err != nil {
		t.Fatal(err)
	}
	if redacted.TaskID != "task-real" || len(redacted.Candidates) != 0 {
		t.Fatalf("Task Manager received frozen candidate bodies: %#v", redacted)
	}
	prepared, err := runtime.LoadPreparedInvocation(ctx, adapter.firstInvocation())
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range statements {
		if !strings.Contains(prepared.Spec, statement) {
			t.Fatalf("review prompt omitted atomic candidate %q", statement)
		}
	}

	invocation, ok, err := runtime.invocations.Get(ctx, prepared.InvocationID)
	if err != nil || !ok {
		t.Fatalf("load review invocation ok=%v err=%v", ok, err)
	}
	reviewPrincipal := principalForContextTest(invocation, now)
	candidates, err := contexts.ListTaskCandidates(ctx, auth.Principal{
		ActorPrincipalID: "review-reader", Kind: auth.PrincipalAgent, ProjectID: projectID, Role: auth.RolePlanner,
		TaskID: "task-real", InvocationID: "review-reader", Tools: auth.ToolSet(auth.ToolAgentListTaskMemoryCandidates),
	})
	if err != nil {
		t.Fatal(err)
	}
	decisions := make([]contextgraph.CandidateReviewDecision, 0, len(candidates.Candidates))
	for _, candidate := range candidates.Candidates {
		decisions = append(decisions, contextgraph.CandidateReviewDecision{
			CandidateID: candidate.CandidateID, Action: "create", Statement: candidate.Candidate.Statement,
			Kind: candidate.Candidate.Kind, Reason: "independent sourced assertion remains reusable",
		})
	}
	if _, err := contexts.SubmitReview(ctx, reviewPrincipal, contextgraph.CandidateReviewSubmission{Decisions: decisions}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	var generalNodes, reviewed, completed int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM context_nodes WHERE project_id=$1 AND id NOT IN (SELECT node_id FROM context_task_projections WHERE project_id=$1)`, projectID).Scan(&generalNodes); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM context_task_memory_reviews WHERE project_id=$1 AND task_id='task-real' AND state='reviewed'`, projectID).Scan(&reviewed); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM production_context_invocations WHERE project_id=$1 AND operation='review' AND state='completed'`, projectID).Scan(&completed); err != nil {
		t.Fatal(err)
	}
	if generalNodes != len(statements) || reviewed != 1 || completed != 1 {
		t.Fatalf("review lifecycle nodes=%d reviewed=%d completed=%d", generalNodes, reviewed, completed)
	}
}

func TestProductionContextRetrieveWaitsForCapacityInsteadOfLeavingGhostInvocationAgainstPostgres(t *testing.T) {
	databaseURL := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("THREADMILL_TEST_DATABASE_URL is required for the real PostgreSQL integration test")
	}
	ctx := context.Background()
	db := openProductionPhasePostgres(t, ctx, databaseURL)
	defer db.Close()

	projectID := kernel.ProjectID("project-context-abandoned-retrieve")
	graph := coordination.NewPostgresStore(db)
	seedProductionPhaseTask(t, ctx, db, projectID, graph)
	now := time.Now().UTC()
	contexts := contextgraph.NewPostgresStore(db, time.Now)
	contexts.SetTaskEndpointResolver(productionTaskEndpointResolver{projectID: projectID, graph: graph})
	runtime, err := newProductionContextRuntime(db, projectID, "room-context", productionPhaseTestAssembler(t), contexts, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	consumer := runtimepkg.Invocation{
		ID: "tm-context-abandoned-consumer", ActorPrincipalID: "task-manager-context", ProjectID: projectID,
		TaskID: "task-real", Role: auth.RoleTaskManager, Status: runtimepkg.InvocationRunning,
		CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	consumerAssembly, err := runtime.assembler.Assemble(consumer, promptcatalog.RenderData{})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.invocations.Create(ctx, consumerAssembly.Invocation); err != nil {
		t.Fatal(err)
	}
	runtime.waitTimeout = 2 * time.Second
	runtime.pollEvery = 10 * time.Millisecond
	searcher := productionContextSearcher{searcher: contexts, runtime: runtime}
	var attempts int
	dispatchErr := kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "context host is fenced", Recoverable: true}
	adapter := &productionContextTestAdapter{onDispatch: func(ref string) error {
		attempts++
		if attempts == 1 {
			return dispatchErr
		}
		invocation, ok, err := runtime.invocations.Get(ctx, kernel.InvocationID(ref))
		if err != nil || !ok {
			return err
		}
		_, err = searcher.Search(ctx, principalForContextTest(invocation, now), contextgraph.SearchRequest{Keywords: []string{"fine-grained", "context"}})
		return err
	}}
	if err := runtime.setDispatcher(adapter); err != nil {
		t.Fatal(err)
	}
	caller := principalForContextTest(consumerAssembly.Invocation, now)
	if _, err := runtime.RetrieveForConsumer(ctx, caller, contextagent.ContextRetrieveRequest{Query: "fine-grained context memory"}); err != nil {
		t.Fatalf("RetrieveForConsumer() error = %v", err)
	}
	var state, invocationStatus string
	if err := db.QueryRowContext(ctx, `
SELECT c.state, i.status
FROM production_context_invocations c
JOIN runtime_invocations i ON i.invocation_id=c.invocation_id
WHERE c.project_id=$1 AND c.operation='retrieve'`, projectID).Scan(&state, &invocationStatus); err != nil {
		t.Fatal(err)
	}
	if state != "completed" || invocationStatus != string(runtimepkg.InvocationCompleted) {
		t.Fatalf("retried retrieve state=%q invocation=%q", state, invocationStatus)
	}
	adapter.mu.Lock()
	dispatches := len(adapter.dispatched)
	adapter.mu.Unlock()
	if dispatches != 2 {
		t.Fatalf("capacity wait dispatched %d times, want initial attempt plus one retry", dispatches)
	}
}

func TestProductionContextReconcileCancelsDispatchedRetrieveAfterConsumerEndsAgainstPostgres(t *testing.T) {
	databaseURL := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("THREADMILL_TEST_DATABASE_URL is required for the real PostgreSQL integration test")
	}
	ctx := context.Background()
	db := openProductionPhasePostgres(t, ctx, databaseURL)
	defer db.Close()

	projectID := kernel.ProjectID("project-context-dispatched-orphan")
	graph := coordination.NewPostgresStore(db)
	seedProductionPhaseTask(t, ctx, db, projectID, graph)
	now := time.Now().UTC()
	contexts := contextgraph.NewPostgresStore(db, time.Now)
	contexts.SetTaskEndpointResolver(productionTaskEndpointResolver{projectID: projectID, graph: graph})
	runtime, err := newProductionContextRuntime(db, projectID, "room-context", productionPhaseTestAssembler(t), contexts, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	consumer := runtimepkg.Invocation{
		ID: "tm-context-ended-consumer", ActorPrincipalID: "task-manager-context", ProjectID: projectID,
		TaskID: "task-real", Role: auth.RoleTaskManager, Status: runtimepkg.InvocationRunning,
		CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	consumerAssembly, err := runtime.assembler.Assemble(consumer, promptcatalog.RenderData{})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.invocations.Create(ctx, consumerAssembly.Invocation); err != nil {
		t.Fatal(err)
	}
	caller := principalForContextTest(consumerAssembly.Invocation, now)
	invocationID, err := runtime.ensureInvocation(ctx, "retrieve", "orphan-after-dispatch", caller.TaskID, caller, struct {
		Operation string `json:"operation"`
		Query     string `json:"query"`
	}{Operation: "retrieve", Query: "fine-grained context memory"})
	if err != nil {
		t.Fatal(err)
	}
	adapter := &productionContextTestAdapter{}
	if err := runtime.setDispatcher(adapter); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.dispatch(ctx, invocationID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
INSERT INTO agentteams_execution_refs(
  invocation_ref, invocation_id, agentteams_task_id, host_ref, dispatch_fingerprint,
  state, attempt, host_slot_claimed_at, created_at, updated_at
) VALUES ($1,$1,$2,'manager-context','context-dispatched-orphan','dispatched',1,$3,$3,$3)`,
		invocationID, "context-task-"+string(invocationID), now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE runtime_invocations SET status='failed' WHERE invocation_id=$1`, caller.InvocationID); err != nil {
		t.Fatal(err)
	}

	if err := runtime.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	var contextState, invocationStatus string
	if err := db.QueryRowContext(ctx, `
SELECT c.state, i.status
FROM production_context_invocations c
JOIN runtime_invocations i ON i.invocation_id=c.invocation_id
WHERE c.project_id=$1 AND c.invocation_id=$2`, projectID, invocationID).Scan(&contextState, &invocationStatus); err != nil {
		t.Fatal(err)
	}
	if contextState != "failed" || invocationStatus != string(runtimepkg.InvocationFailed) {
		t.Fatalf("orphan retrieve state=%q invocation=%q", contextState, invocationStatus)
	}
	adapter.mu.Lock()
	terminateCalls := adapter.terminateCalls
	adapter.mu.Unlock()
	if terminateCalls != 1 {
		t.Fatalf("orphan retrieve terminate calls=%d, want 1", terminateCalls)
	}
}

func TestProductionContextReconcileCancelsExpiredDispatchedReviewAndDispatchesWaitingRetrieveAgainstPostgres(t *testing.T) {
	databaseURL := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("THREADMILL_TEST_DATABASE_URL is required for the real PostgreSQL integration test")
	}
	ctx := context.Background()
	db := openProductionPhasePostgres(t, ctx, databaseURL)
	defer db.Close()

	projectID := kernel.ProjectID("project-context-expired-review")
	graph := coordination.NewPostgresStore(db)
	seedProductionPhaseTask(t, ctx, db, projectID, graph)
	now := time.Now().UTC()
	contexts := contextgraph.NewPostgresStore(db, time.Now)
	contexts.SetTaskEndpointResolver(productionTaskEndpointResolver{projectID: projectID, graph: graph})
	runtime, err := newProductionContextRuntime(db, projectID, "room-context", productionPhaseTestAssembler(t), contexts, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	adapter := &productionContextTestAdapter{}
	if err := runtime.setDispatcher(adapter); err != nil {
		t.Fatal(err)
	}

	expiredReview := runtimepkg.Invocation{
		ID: "context-expired-review", ActorPrincipalID: "context-agent:" + kernel.ActorPrincipalID(projectID), ProjectID: projectID,
		TaskID: "task-real", Role: auth.RoleContext, Operation: "review", Status: runtimepkg.InvocationRunning,
		CreatedAt: now.Add(-20 * time.Minute), ExpiresAt: now.Add(-10 * time.Minute),
	}
	expiredAssembly, err := runtime.assembler.Assemble(expiredReview, promptcatalog.RenderData{})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.invocations.Create(ctx, expiredAssembly.Invocation); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
INSERT INTO production_context_invocations(
  project_id, invocation_id, operation, request_key, request_hash, task_id,
  room_id, spec, runtime_config_ref, envelope_ref, required_capabilities, state,
  agentteams_task_id, host_ref, created_at, updated_at
) VALUES ($1,$2,'review','expired-review','hash-expired-review','task-real',
  'room-context','spec','runtime-config','runtime-envelope','["context_agent"]'::jsonb,'dispatched',
  'context-task-expired-review','manager-context',$3,$3)`, projectID, expiredReview.ID, now.Add(-20*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
INSERT INTO agentteams_execution_refs(
  invocation_ref, invocation_id, agentteams_task_id, host_ref, dispatch_fingerprint,
  state, attempt, host_slot_claimed_at, created_at, updated_at
) VALUES ($1,$1,'context-task-expired-review','manager-context','expired-review','dispatched',1,$2,$2,$2)`, expiredReview.ID, now.Add(-20*time.Minute)); err != nil {
		t.Fatal(err)
	}

	consumer := runtimepkg.Invocation{
		ID: "tm-context-after-expired-review", ActorPrincipalID: "task-manager-context", ProjectID: projectID,
		Role: auth.RoleTaskManager, Status: runtimepkg.InvocationRunning, CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	consumerAssembly, err := runtime.assembler.Assemble(consumer, promptcatalog.RenderData{})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.invocations.Create(ctx, consumerAssembly.Invocation); err != nil {
		t.Fatal(err)
	}
	caller := principalForContextTest(consumerAssembly.Invocation, now)
	retrieveID, err := runtime.ensureInvocation(ctx, "retrieve", "after-expired-review", "", caller, struct {
		Operation string `json:"operation"`
		Query     string `json:"query"`
	}{Operation: "retrieve", Query: "responsive panel layout"})
	if err != nil {
		t.Fatal(err)
	}

	if err := runtime.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	var reviewState, reviewStatus, retrieveState string
	if err := db.QueryRowContext(ctx, `
SELECT c.state, i.status
FROM production_context_invocations c JOIN runtime_invocations i USING(invocation_id)
WHERE c.invocation_id=$1`, expiredReview.ID).Scan(&reviewState, &reviewStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT state FROM production_context_invocations WHERE invocation_id=$1`, retrieveID).Scan(&retrieveState); err != nil {
		t.Fatal(err)
	}
	if reviewState != "failed" || reviewStatus != string(runtimepkg.InvocationFailed) || retrieveState != "dispatched" {
		t.Fatalf("states review=%q/%q retrieve=%q", reviewState, reviewStatus, retrieveState)
	}
	adapter.mu.Lock()
	terminateCalls := adapter.terminateCalls
	dispatched := append([]string(nil), adapter.dispatched...)
	adapter.mu.Unlock()
	if terminateCalls != 1 || len(dispatched) != 1 || dispatched[0] != string(retrieveID) {
		t.Fatalf("adapter terminate=%d dispatched=%v", terminateCalls, dispatched)
	}
}

func markProductionTaskDone(t *testing.T, ctx context.Context, graph *coordination.PostgresStore, projectID kernel.ProjectID, taskID kernel.TaskID) {
	t.Helper()
	for _, endpointID := range []coordination.EndpointID{coordination.EndpointPlan, coordination.EndpointExecute, coordination.EndpointVerify} {
		snapshot, err := graph.Latest(ctx, projectID)
		if err != nil {
			t.Fatal(err)
		}
		var endpoint coordination.PhaseEndpoint
		for _, candidate := range snapshot.Endpoints {
			if candidate.Ref.TaskID == taskID && candidate.Ref.EndpointID == endpointID {
				endpoint = candidate
				break
			}
		}
		snapshot, err = graph.Transition(ctx, projectID, snapshot.Revision, coordination.GraphTransition{
			TargetKind: coordination.TargetPhaseEndpoint, Endpoint: endpoint.Ref, Generation: endpoint.Generation, Action: string(coordination.EndpointSubmitted),
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := graph.Transition(ctx, projectID, snapshot.Revision, coordination.GraphTransition{
			TargetKind: coordination.TargetPhaseEndpoint, Endpoint: endpoint.Ref, Generation: endpoint.Generation, Action: string(coordination.EndpointSatisfied),
			Result: coordination.PhaseResult{ID: "result-" + string(endpointID), Endpoint: endpoint.Ref, BindingRef: endpoint.BindingRef, OutputRef: "artifact://" + string(endpointID)},
		}); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := graph.Latest(ctx, projectID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.Transition(ctx, projectID, snapshot.Revision, coordination.GraphTransition{TargetKind: coordination.TargetTask, TaskID: taskID, Action: string(coordination.TaskDone)}); err != nil {
		t.Fatal(err)
	}
}

func principalForContextTest(invocation runtimepkg.Invocation, now time.Time) auth.Principal {
	return auth.Principal{
		ActorPrincipalID: invocation.ActorPrincipalID, Kind: auth.PrincipalAgent, ProjectID: invocation.ProjectID,
		Role: invocation.Role, Operation: invocation.Operation, TaskID: invocation.TaskID, InvocationID: invocation.ID,
		ConsumerInvocationID: invocation.ConsumerInvocationID, ConsumerTaskID: invocation.ConsumerTaskID, ConsumerRole: invocation.ConsumerRole,
		Tools: auth.ToolSet(invocation.EffectiveTools...), AuthenticatedAt: now,
	}
}

type productionContextTestAdapter struct {
	mu             sync.Mutex
	dispatched     []string
	terminateCalls int
	onDispatch     func(string) error
}

func (a *productionContextTestAdapter) Dispatch(_ context.Context, ref string) (agentteams.AgentTeamsExecutionRef, error) {
	a.mu.Lock()
	a.dispatched = append(a.dispatched, ref)
	onDispatch := a.onDispatch
	a.mu.Unlock()
	if onDispatch != nil {
		if err := onDispatch(ref); err != nil {
			return agentteams.AgentTeamsExecutionRef{}, err
		}
	}
	return agentteams.AgentTeamsExecutionRef{InvocationID: kernel.InvocationID(ref), AgentTeamsTaskID: "context-task-" + ref, HostRef: "manager-context"}, nil
}

func (a *productionContextTestAdapter) Terminate(context.Context, agentteams.AgentTeamsExecutionRef, string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.terminateCalls++
	return nil
}

func (a *productionContextTestAdapter) Collect(context.Context, agentteams.AgentTeamsExecutionRef) (agentteams.UntrustedExecutionResult, error) {
	return agentteams.UntrustedExecutionResult{}, nil
}

func (a *productionContextTestAdapter) Observe(context.Context, string) ([]agentteams.ExecutionObservation, error) {
	return nil, nil
}

func (a *productionContextTestAdapter) firstInvocation() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.dispatched) == 0 {
		return ""
	}
	return a.dispatched[0]
}

var _ agentteams.AgentTeamsHostAdapter = (*productionContextTestAdapter)(nil)
