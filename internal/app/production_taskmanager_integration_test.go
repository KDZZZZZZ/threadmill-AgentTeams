package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/contextgraph"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/evidence"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/mergequeue"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/objectstore"
	runtimepkg "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime"
	phasepkg "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime/phase"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/taskmanager"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/transport/httpapi"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/transport/mcpapi"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/workspace"
)

func TestProductionTaskManagerDoneRecoveryFinalizesMemoryAgainstPostgres(t *testing.T) {
	databaseURL := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("THREADMILL_TEST_DATABASE_URL is required for the real PostgreSQL integration test")
	}
	ctx := context.Background()
	db := openProductionPhasePostgres(t, ctx, databaseURL)
	defer db.Close()

	projectID := kernel.ProjectID("project-real")
	taskID := kernel.TaskID("task-done-recovery")
	graph := coordination.NewPostgresStore(db)
	seedProductionCompletionTask(t, ctx, db, projectID, graph, taskID, taskmanager.DeliveryPolicyNonCodeArtifact)

	contexts := contextgraph.NewPostgresStore(db, time.Now)
	contexts.SetTaskEndpointResolver(productionTaskEndpointResolver{projectID: projectID, graph: graph})
	taskManagerPrincipal := auth.Principal{
		ActorPrincipalID: "task-manager-memory", Kind: auth.PrincipalAgent, ProjectID: projectID,
		Role: auth.RoleTaskManager, TaskID: taskID, InvocationID: "task-manager-memory-inv",
		Tools: auth.ToolSet(auth.ToolContextRegisterTaskSubgraph), AuthenticatedAt: time.Now(),
	}
	if _, err := contexts.RegisterTaskSubgraph(ctx, taskManagerPrincipal, taskID); err != nil {
		t.Fatalf("register task subgraph: %v", err)
	}
	if _, err := contexts.SubmitCandidate(ctx, auth.Principal{
		ActorPrincipalID: "planner-memory", Kind: auth.PrincipalAgent, ProjectID: projectID,
		Role: auth.RolePlanner, TaskID: taskID, InvocationID: "planner-memory-inv",
		Tools: auth.ToolSet(auth.ToolAgentSubmitMemoryCandidate), AuthenticatedAt: time.Now(),
	}, contextgraph.SubmitCandidateRequest{Candidate: contextgraph.MemoryCandidate{
		Statement: "task done recovery must freeze the task memory batch exactly once",
		Kind:      string(contextgraph.NodeKindFact), SourceRefs: []string{"evidence://done-recovery"},
	}}); err != nil {
		t.Fatalf("seed memory candidate: %v", err)
	}

	assembler := productionPhaseTestAssembler(t)
	contextRuntime, err := newProductionContextRuntime(db, projectID, "room-real", assembler, contexts, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	contextAdapter := &productionContextTestAdapter{}
	if err := contextRuntime.setDispatcher(contextAdapter); err != nil {
		t.Fatal(err)
	}
	ingress, err := newProductionIngress(db, projectID, "room-real", assembler, graph, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := ingress.setDispatcher(&recordingProductionDispatcher{}); err != nil {
		t.Fatal(err)
	}
	managerRuntime, err := newProductionTaskManagerRuntime(db, projectID, graph, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	managerRuntime.followups = ingress
	if err := managerRuntime.setProductionMemoryFinalizer(contextRuntime); err != nil {
		t.Fatal(err)
	}

	verifyRef := coordination.PhaseEndpointRef{TaskID: taskID, EndpointID: coordination.EndpointVerify}
	snapshot, err := graph.Latest(ctx, projectID)
	if err != nil {
		t.Fatal(err)
	}
	verify := productionEndpoint(snapshot, verifyRef)
	snapshot, err = graph.Transition(ctx, projectID, snapshot.Revision, coordination.GraphTransition{
		TargetKind: coordination.TargetPhaseEndpoint, Endpoint: verifyRef,
		Generation: verify.Generation, Action: string(coordination.EndpointSubmitted),
	})
	if err != nil {
		t.Fatalf("submit verify endpoint: %v", err)
	}
	verify = productionEndpoint(snapshot, verifyRef)
	verifyOutput := productionPhaseOutputBoundary{OutputRef: "output-verify-done", Receipt: phasepkg.OutputReceipt{
		InvocationID: "phase-invocation-verify-done", Endpoint: verifyRef, Generation: verify.Generation,
		BindingRef: verify.BindingRef, LeaseRef: "lease-verify-done", InputRevision: "inputs-verify-done",
		Output: phasepkg.PhaseOutput{
			ReportRef: "report-verify-done", EvidenceRefs: []string{"evidence-verify-done"},
			DeliveryRefs: []string{"artifact-verify-done"},
		},
	}}
	evaluationPayload, err := json.Marshal(productionPhaseEvaluationBoundary{
		SourceInputRef: "manager-input:verify-source", Output: verifyOutput, Endpoint: verifyRef,
		Generation: verify.Generation, BindingRef: verify.BindingRef,
	})
	if err != nil {
		t.Fatal(err)
	}
	evaluationInput, err := ingress.DispatchTaskManagerFollowup(ctx, productionInput{
		Kind: "phase_orchestration", RequestID: "verify-evaluation-done", ConversationID: "runtime:done",
		Body: "evaluate verify output before done", Payload: evaluationPayload, SeenRevision: snapshot.Revision,
		SelectedEndpoint: &verifyRef, TargetKind: "phase_evaluation", TargetRef: verifyOutput.OutputRef,
	})
	if err != nil {
		t.Fatalf("persist verify evaluation input: %v", err)
	}
	evaluationPrincipal := productionTestTaskManagerPrincipal(evaluationInput.InvocationID)
	evaluationScope := auth.BoundScope{ProjectID: projectID, InvocationID: evaluationInput.InvocationID}
	if _, err := managerRuntime.SubmitTaskManagerDecision(ctx, evaluationPrincipal, evaluationScope, taskmanager.TaskManagerDecision{
		Action: "satisfied", TargetRef: string(taskID) + "/verify", Reason: "verified delivery artifact is present",
	}); err != nil {
		t.Fatalf("submit verify satisfied decision: %v", err)
	}
	if _, err := managerRuntime.Transition(ctx, evaluationPrincipal, evaluationScope); err != nil {
		t.Fatalf("apply verify satisfied transition: %v", err)
	}

	var completionInvocation kernel.InvocationID
	if err := db.QueryRowContext(ctx, `SELECT invocation_id FROM production_manager_inputs WHERE project_id=$1 AND target_kind='task_completion' AND target_ref=$2`, projectID, taskID).Scan(&completionInvocation); err != nil {
		t.Fatal(err)
	}
	completionPrincipal := productionTestTaskManagerPrincipal(completionInvocation)
	completionScope := auth.BoundScope{ProjectID: projectID, InvocationID: completionInvocation}
	doneDecision := taskmanager.TaskManagerDecision{Action: "done", TargetRef: string(taskID), Reason: "trusted non-code delivery completed"}
	decisionRef, err := managerRuntime.SubmitTaskManagerDecision(ctx, completionPrincipal, completionScope, doneDecision)
	if err != nil {
		t.Fatalf("submit done decision: %v", err)
	}
	if replayed, err := managerRuntime.SubmitTaskManagerDecision(ctx, completionPrincipal, completionScope, doneDecision); err != nil || replayed != decisionRef {
		t.Fatalf("idempotent done decision replay ref=%q err=%v, want %q nil", replayed, err, decisionRef)
	}

	binding, err := managerRuntime.binding(ctx, completionPrincipal, completionScope)
	if err != nil {
		t.Fatal(err)
	}
	crashRevision, err := managerRuntime.graph(completionPrincipal).Transition(ctx, binding.SeenRevision, binding.DecisionRef)
	if err != nil {
		t.Fatalf("simulate graph-applied crash window: %v", err)
	}
	var mutationApplied bool
	if err := db.QueryRowContext(ctx, `SELECT mutation_applied FROM production_taskmanager_bindings WHERE invocation_id=$1`, completionInvocation).Scan(&mutationApplied); err != nil {
		t.Fatal(err)
	}
	if mutationApplied {
		t.Fatal("crash simulation unexpectedly marked Task Manager binding as applied")
	}

	restartedRuntime, err := newProductionTaskManagerRuntime(db, projectID, graph, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := restartedRuntime.setProductionMemoryFinalizer(contextRuntime); err != nil {
		t.Fatal(err)
	}
	handled, err := restartedRuntime.RecoverPersistedTaskManagerDecision(ctx, completionInvocation)
	if err != nil || !handled {
		t.Fatalf("recover done decision handled=%v err=%v", handled, err)
	}
	replayedRecovery, err := restartedRuntime.RecoverPersistedTaskManagerDecision(ctx, completionInvocation)
	if err != nil || !replayedRecovery {
		t.Fatalf("idempotent recover done handled=%v err=%v", replayedRecovery, err)
	}

	completed, err := graph.Snapshot(ctx, projectID, crashRevision)
	if err != nil {
		t.Fatal(err)
	}
	if len(completed.Tasks) != 1 || completed.Tasks[0].Outcome != coordination.TaskDone {
		t.Fatalf("completed task snapshot = %#v", completed.Tasks)
	}
	var appliedRevision kernel.Revision
	var inputStatus, invocationStatus string
	if err := db.QueryRowContext(ctx, `
SELECT b.applied_graph_revision, i.status, r.status
FROM production_taskmanager_bindings b
JOIN production_manager_inputs i ON i.project_id=b.project_id AND i.input_ref=b.input_ref
JOIN runtime_invocations r ON r.invocation_id=b.invocation_id
WHERE b.invocation_id=$1`, completionInvocation).Scan(&appliedRevision, &inputStatus, &invocationStatus); err != nil {
		t.Fatal(err)
	}
	if appliedRevision != crashRevision || inputStatus != "completed" || invocationStatus != "completed" {
		t.Fatalf("recovered binding revision=%d input=%q invocation=%q, want %d completed completed", appliedRevision, inputStatus, invocationStatus, crashRevision)
	}
	var memoryState string
	var reviewCount int
	if err := db.QueryRowContext(ctx, `SELECT state FROM context_task_memory_reviews WHERE project_id=$1 AND task_id=$2`, projectID, taskID).Scan(&memoryState); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM production_context_invocations WHERE project_id=$1 AND task_id=$2 AND operation='review'`, projectID, taskID).Scan(&reviewCount); err != nil {
		t.Fatal(err)
	}
	if memoryState != string(contextgraph.TaskMemoryFrozenUnreviewed) || reviewCount != 1 {
		t.Fatalf("memory state=%q review invocations=%d, want frozen_unreviewed and one review", memoryState, reviewCount)
	}
}

func TestProductionTaskManagerRebasesQueuedInputToAuthoritativeSnapshotAgainstPostgres(t *testing.T) {
	databaseURL := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("THREADMILL_TEST_DATABASE_URL is required for the real PostgreSQL integration test")
	}
	ctx := context.Background()
	db := openProductionPhasePostgres(t, ctx, databaseURL)
	defer db.Close()

	projectID := kernel.ProjectID("project-real")
	taskID := kernel.TaskID("task-stale-manager-input")
	graph := coordination.NewPostgresStore(db)
	seedProductionCompletionTask(t, ctx, db, projectID, graph, taskID, taskmanager.DeliveryPolicyNonCodeArtifact)

	assembler := productionPhaseTestAssembler(t)
	ingress, err := newProductionIngress(db, projectID, "room-real", assembler, graph, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := ingress.setDispatcher(&recordingProductionDispatcher{}); err != nil {
		t.Fatal(err)
	}
	managerRuntime, err := newProductionTaskManagerRuntime(db, projectID, graph, time.Now)
	if err != nil {
		t.Fatal(err)
	}

	planRef := coordination.PhaseEndpointRef{TaskID: taskID, EndpointID: coordination.EndpointPlan}
	operator := auth.Principal{
		ActorPrincipalID: "operator-stale-manager-input", Kind: auth.PrincipalOperator,
		ProjectID: projectID, Role: auth.RoleOperator,
	}
	accepted, err := ingress.SubmitManagerMessage(ctx, operator, httpapi.ManagerMessageRequest{
		RequestID: "request-stale-manager-input", ProjectID: projectID,
		ConversationID: "conversation-stale-manager-input",
		Body:           "release the held planning endpoint", Intent: httpapi.ManagerIntentResume, SelectedEndpoint: &planRef,
	})
	if err != nil {
		t.Fatalf("persist queued manager input: %v", err)
	}

	queuedRevision, err := graph.Latest(ctx, projectID)
	if err != nil {
		t.Fatal(err)
	}
	plan := productionEndpoint(queuedRevision, planRef)
	advanced, err := graph.Transition(ctx, projectID, queuedRevision.Revision, coordination.GraphTransition{
		TargetKind: coordination.TargetPhaseEndpoint, Endpoint: planRef,
		Generation: plan.Generation, Action: "held",
	})
	if err != nil {
		t.Fatalf("advance graph after input was queued: %v", err)
	}

	principal := productionTestTaskManagerPrincipal(accepted.InvocationRef)
	scope := auth.BoundScope{ProjectID: projectID, InvocationID: accepted.InvocationRef}
	decision := taskmanager.TaskManagerDecision{
		Action: "released", TargetRef: string(taskID) + "/plan",
		Reason: "the latest authoritative snapshot shows the requested endpoint is held",
	}
	decisionRef, err := managerRuntime.SubmitTaskManagerDecision(ctx, principal, scope, decision)
	if err != nil {
		t.Fatalf("submit decision for stale queued input: %v", err)
	}
	var expectedRevision kernel.Revision
	if err := db.QueryRowContext(ctx, `SELECT expected_revision FROM taskmanager_decisions
WHERE project_id=$1 AND decision_ref=$2`, projectID, decisionRef).Scan(&expectedRevision); err != nil {
		t.Fatal(err)
	}
	if expectedRevision != advanced.Revision {
		t.Fatalf("decision expected revision = %d, want authoritative latest %d", expectedRevision, advanced.Revision)
	}
	appliedRevision, err := managerRuntime.Transition(ctx, principal, scope)
	if err != nil {
		t.Fatalf("apply rebased transition: %v", err)
	}
	final, err := graph.Snapshot(ctx, projectID, appliedRevision)
	if err != nil {
		t.Fatal(err)
	}
	if got := productionEndpoint(final, planRef).RunPolicy; got != coordination.RunEnabled {
		t.Fatalf("rebased endpoint run policy = %q, want %q", got, coordination.RunEnabled)
	}
}

func TestProductionTaskManagerRejectsInvalidResumeBeforeDecisionPersistenceAgainstPostgres(t *testing.T) {
	databaseURL := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("THREADMILL_TEST_DATABASE_URL is required for the real PostgreSQL integration test")
	}
	ctx := context.Background()
	db := openProductionPhasePostgres(t, ctx, databaseURL)
	defer db.Close()

	projectID := kernel.ProjectID("project-real")
	taskID := kernel.TaskID("task-resume-preflight")
	graph := coordination.NewPostgresStore(db)
	seedProductionCompletionTask(t, ctx, db, projectID, graph, taskID, taskmanager.DeliveryPolicyNonCodeArtifact)
	ingress, err := newProductionIngress(db, projectID, "room-real", productionPhaseTestAssembler(t), graph, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := ingress.setDispatcher(&recordingProductionDispatcher{}); err != nil {
		t.Fatal(err)
	}
	managerRuntime, err := newProductionTaskManagerRuntime(db, projectID, graph, time.Now)
	if err != nil {
		t.Fatal(err)
	}

	executeRef := coordination.PhaseEndpointRef{TaskID: taskID, EndpointID: coordination.EndpointExecute}
	accepted, err := ingress.SubmitManagerMessage(ctx, auth.Principal{
		ActorPrincipalID: "operator-resume-preflight", Kind: auth.PrincipalOperator,
		ProjectID: projectID, Role: auth.RoleOperator,
	}, httpapi.ManagerMessageRequest{
		RequestID: "request-resume-preflight", ProjectID: projectID,
		ConversationID: "conversation-resume-preflight", Body: "resume the selected phase",
		Intent: httpapi.ManagerIntentResume, SelectedEndpoint: &executeRef,
	})
	if err != nil {
		t.Fatal(err)
	}
	principal := productionTestTaskManagerPrincipal(accepted.InvocationRef)
	scope := auth.BoundScope{ProjectID: projectID, InvocationID: accepted.InvocationRef}
	if _, err := managerRuntime.SubmitTaskManagerDecision(ctx, principal, scope, taskmanager.TaskManagerDecision{
		Action: "released", TargetRef: string(taskID) + "/execute", Reason: "resume requested",
	}); !kernel.IsCode(err, kernel.CodeTransitionRejected) {
		t.Fatalf("invalid resume error=%v, want transition_rejected", err)
	}

	var decisions int
	var persistedDecision sql.NullString
	if err := db.QueryRowContext(ctx, `
SELECT (SELECT count(*) FROM taskmanager_decisions WHERE project_id=$1 AND input_ref=i.input_ref),
       b.decision_ref
FROM production_manager_inputs i
JOIN production_taskmanager_bindings b
  ON b.project_id=i.project_id AND b.input_ref=i.input_ref
WHERE i.project_id=$1 AND i.invocation_id=$2`, projectID, accepted.InvocationRef).Scan(&decisions, &persistedDecision); err != nil {
		t.Fatal(err)
	}
	if decisions != 0 || persistedDecision.Valid {
		t.Fatalf("rejected resume persisted decisions=%d decision_ref=%#v", decisions, persistedDecision)
	}

	corrected := taskmanager.TaskManagerDecision{Action: "no_change", Reason: "selected phase is already enabled; leave graph unchanged"}
	decisionRef, err := managerRuntime.SubmitTaskManagerDecision(ctx, principal, scope, corrected)
	if err != nil {
		t.Fatalf("submit corrected decision after preflight rejection: %v", err)
	}
	if decisionRef == "" {
		t.Fatal("corrected decision returned an empty DecisionRef")
	}
}

func TestProductionTaskManagerSnapshotProjectsAuthoritativeMergeDeliveryAgainstPostgres(t *testing.T) {
	databaseURL := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("THREADMILL_TEST_DATABASE_URL is required for the real PostgreSQL integration test")
	}
	ctx := context.Background()
	db := openProductionPhasePostgres(t, ctx, databaseURL)
	defer db.Close()

	projectID := kernel.ProjectID("project-real")
	taskID := kernel.TaskID("task-delivery-snapshot")
	graph := coordination.NewPostgresStore(db)
	seedProductionCompletionTask(t, ctx, db, projectID, graph, taskID, taskmanager.DeliveryPolicyCodeMerge)

	repository := seedProductionPhaseBareRepo(t)
	workspaces := workspace.NewPostgresService(db)
	source := &productionPhaseBindingSource{
		db: db, projectID: projectID, graph: graph,
		contracts: taskmanager.NewPostgresStore(db, projectID, graph), workspaces: workspaces,
		repositoryPath: repository, worktreeParent: t.TempDir(), now: time.Now,
	}
	workspaceRef, err := source.EnsureTaskWorkspace(ctx, productionTaskWorkspaceRequest{TaskID: taskID, Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	binding, err := workspaces.Get(ctx, workspaceRef)
	if err != nil {
		t.Fatal(err)
	}
	registry := evidence.NewPostgresArtifactRegistry(db, objectstore.NewMemoryStore(), "artifacts")
	verifyArtifact, err := registry.Register(ctx, evidence.RegisterArtifact{
		Type: evidence.ArtifactGeneratedReport, ProjectID: projectID, TaskID: taskID,
		Path: "verify/delivery-snapshot.json", ContentType: "application/json", Body: []byte(`{"verdict":"pass"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	diffArtifact, err := registry.Register(ctx, evidence.RegisterArtifact{
		Type: evidence.ArtifactDiffPatch, ProjectID: projectID, TaskID: taskID,
		Path: "merge/delivery-snapshot.patch", ContentType: "text/x-diff", Body: []byte("diff --git a/a b/a\n"),
	})
	if err != nil {
		t.Fatal(err)
	}
	failureArtifact, err := registry.Register(ctx, evidence.RegisterArtifact{
		Type: evidence.ArtifactToolOutput, ProjectID: projectID, TaskID: taskID,
		Path: "merge/delivery-snapshot-failure.log", ContentType: "text/plain", Body: []byte("verification failed"),
	})
	if err != nil {
		t.Fatal(err)
	}
	failedCandidate := "candidate-delivery-snapshot-failed"
	if _, err := db.ExecContext(ctx, `
INSERT INTO merge_candidates(id,project_id,task_id,workspace_ref,verify_result_ref,diff_artifact_ref,target_repository,target_branch,base_revision,main_revision,candidate_revision,status,failure_reason,failure_evidence_ref,created_at,updated_at)
VALUES($1,$2,$3,$4,$5,$6,$7,'main',$8,$8,$9,'failed','verify_failed',$10,now(),now())`,
		failedCandidate, projectID, taskID, binding.ID, verifyArtifact.ID, diffArtifact.ID,
		repository, binding.BaseRevision, binding.CurrentRevision, failureArtifact.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
INSERT INTO production_merge_deliveries(project_id,candidate_id,task_id,verify_result_ref,completion_payload,payload_hash,status,last_error,created_at,updated_at)
VALUES($1,$2,$3,$4,'{}'::jsonb,'failed-hash','failed','verify_failed',now(),now())`,
		projectID, failedCandidate, taskID, verifyArtifact.ID); err != nil {
		t.Fatal(err)
	}

	ingress, err := newProductionIngress(db, projectID, "room-real", productionPhaseTestAssembler(t), graph, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := ingress.setDispatcher(&recordingProductionDispatcher{}); err != nil {
		t.Fatal(err)
	}
	accepted, err := ingress.SubmitManagerMessage(ctx, auth.Principal{
		ActorPrincipalID: "operator-delivery-snapshot", Kind: auth.PrincipalOperator,
		ProjectID: projectID, Role: auth.RoleOperator,
	}, httpapi.ManagerMessageRequest{
		RequestID: "request-delivery-snapshot", ProjectID: projectID,
		ConversationID: "conversation-delivery-snapshot", Body: "replan from delivery facts",
		Intent: httpapi.ManagerIntentOrchestrate,
	})
	if err != nil {
		t.Fatal(err)
	}
	managerRuntime, err := newProductionTaskManagerRuntime(db, projectID, graph, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	principal := productionTestTaskManagerPrincipal(accepted.InvocationRef)
	scope := auth.BoundScope{ProjectID: projectID, InvocationID: accepted.InvocationRef}
	snapshot, err := managerRuntime.Snapshot(ctx, principal, scope, kernel.LatestRevision)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Deliveries) != 1 {
		t.Fatalf("delivery states=%#v, want one active Task", snapshot.Deliveries)
	}
	failed := snapshot.Deliveries[0]
	if failed.LatestCandidateID != failedCandidate || failed.LatestCandidateStatus != "failed" || failed.LatestDeliveryStatus != "failed" || failed.LatestFailureReason != "verify_failed" || failed.LatestFailureEvidenceRef != string(failureArtifact.ID) || failed.ReadyForVerify {
		t.Fatalf("failed delivery projection=%#v", failed)
	}

	mergedCandidate := "candidate-delivery-snapshot-merged"
	if _, err := db.ExecContext(ctx, `
INSERT INTO merge_candidates(id,project_id,task_id,workspace_ref,verify_result_ref,diff_artifact_ref,target_repository,target_branch,base_revision,main_revision,candidate_revision,status,merged_revision,created_at,updated_at)
VALUES($1,$2,$3,$4,$5,$6,$7,'main',$8,$8,$9,'merged',$9,now()+interval '1 second',now()+interval '1 second')`,
		mergedCandidate, projectID, taskID, binding.ID, verifyArtifact.ID, diffArtifact.ID,
		repository, binding.BaseRevision, binding.CurrentRevision); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
INSERT INTO production_merge_deliveries(project_id,candidate_id,task_id,verify_result_ref,completion_payload,payload_hash,status,created_at,updated_at)
VALUES($1,$2,$3,$4,'{}'::jsonb,'merged-hash','delivered',now()+interval '1 second',now()+interval '1 second')`,
		projectID, mergedCandidate, taskID, verifyArtifact.ID); err != nil {
		t.Fatal(err)
	}
	snapshot, err = managerRuntime.Snapshot(ctx, principal, scope, kernel.LatestRevision)
	if err != nil {
		t.Fatal(err)
	}
	merged := snapshot.Deliveries[0]
	if merged.LatestCandidateID != mergedCandidate || merged.LatestCandidateStatus != "merged" || merged.LatestDeliveryStatus != "delivered" || !merged.ReadyForVerify {
		t.Fatalf("merged delivery projection=%#v", merged)
	}
}

func TestProductionTaskManagerDoneRejectsUntrustedOrPrematureCompletionAgainstPostgres(t *testing.T) {
	databaseURL := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("THREADMILL_TEST_DATABASE_URL is required for the real PostgreSQL integration test")
	}
	ctx := context.Background()
	db := openProductionPhasePostgres(t, ctx, databaseURL)
	defer db.Close()

	projectID := kernel.ProjectID("project-real")
	taskID := kernel.TaskID("task-done-reject")
	graph := coordination.NewPostgresStore(db)
	seedProductionCompletionTask(t, ctx, db, projectID, graph, taskID, taskmanager.DeliveryPolicyNonCodeArtifact)
	ingress, err := newProductionIngress(db, projectID, "room-real", productionPhaseTestAssembler(t), graph, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := ingress.setDispatcher(&recordingProductionDispatcher{}); err != nil {
		t.Fatal(err)
	}
	managerRuntime, err := newProductionTaskManagerRuntime(db, projectID, graph, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	verifyRef := coordination.PhaseEndpointRef{TaskID: taskID, EndpointID: coordination.EndpointVerify}
	snapshot, err := graph.Latest(ctx, projectID)
	if err != nil {
		t.Fatal(err)
	}
	verify := productionEndpoint(snapshot, verifyRef)
	payload, err := json.Marshal(productionTaskCompletionBoundary{
		SourceInputRef: "manager-input:premature-source", TaskID: taskID, VerifyEndpoint: verifyRef,
		VerifyOutput: productionPhaseOutputBoundary{OutputRef: "output-premature", Receipt: phasepkg.OutputReceipt{
			Endpoint: verifyRef, Generation: verify.Generation, BindingRef: verify.BindingRef,
			Output: phasepkg.PhaseOutput{DeliveryRefs: []string{"artifact-premature"}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	input, err := ingress.DispatchTaskManagerFollowup(ctx, productionInput{
		Kind: "phase_orchestration", RequestID: "premature-completion", ConversationID: "runtime:done",
		Body: "premature done attempt", Payload: payload, SeenRevision: snapshot.Revision,
		SelectedEndpoint: &verifyRef, TargetKind: "task_completion", TargetRef: string(taskID),
	})
	if err != nil {
		t.Fatalf("persist premature completion input: %v", err)
	}
	principal := productionTestTaskManagerPrincipal(input.InvocationID)
	scope := auth.BoundScope{ProjectID: projectID, InvocationID: input.InvocationID}
	if _, err := managerRuntime.SubmitTaskManagerDecision(ctx, principal, scope, taskmanager.TaskManagerDecision{
		Action: "done", TargetRef: "forged-task", Reason: "agent tries to choose a different task",
	}); !kernel.IsCode(err, kernel.CodeInvalidRequest) {
		t.Fatalf("forged done target error=%v, want invalid_request", err)
	}
	if _, err := managerRuntime.SubmitTaskManagerDecision(ctx, principal, scope, taskmanager.TaskManagerDecision{
		Action: "done", TargetRef: string(taskID), Reason: "verify is not satisfied yet",
	}); !kernel.IsCode(err, kernel.CodeTransitionRejected) {
		t.Fatalf("premature done error=%v, want transition_rejected", err)
	}
	var decisions int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM taskmanager_decisions WHERE project_id=$1 AND input_ref=$2`, projectID, input.InputRef).Scan(&decisions); err != nil {
		t.Fatal(err)
	}
	if decisions != 0 {
		t.Fatalf("rejected done persisted %d decisions, want 0", decisions)
	}
}

func TestProductionTaskManagerCodeMergeCompletionRequiresMergedCandidateAgainstPostgres(t *testing.T) {
	databaseURL := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("THREADMILL_TEST_DATABASE_URL is required for the real PostgreSQL integration test")
	}
	ctx := context.Background()
	db := openProductionPhasePostgres(t, ctx, databaseURL)
	defer db.Close()

	projectID := kernel.ProjectID("project-real")
	taskID := kernel.TaskID("task-code-merge-done")
	graph := coordination.NewPostgresStore(db)
	seedProductionCompletionTask(t, ctx, db, projectID, graph, taskID, taskmanager.DeliveryPolicyCodeMerge)
	managerRuntime, err := newProductionTaskManagerRuntime(db, projectID, graph, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	managerRuntime.mergeEvidence = &productionMergeQueue{db: db, projectID: projectID}
	managerRuntime.memory = productionTaskManagerTestFinalizer{}

	verifyRef := coordination.PhaseEndpointRef{TaskID: taskID, EndpointID: coordination.EndpointVerify}
	snapshot, err := graph.Latest(ctx, projectID)
	if err != nil {
		t.Fatal(err)
	}
	for _, endpointID := range []coordination.EndpointID{coordination.EndpointPlan, coordination.EndpointExecute, coordination.EndpointVerify} {
		ref := coordination.PhaseEndpointRef{TaskID: taskID, EndpointID: endpointID}
		endpoint := productionEndpoint(snapshot, ref)
		snapshot, err = graph.Transition(ctx, projectID, snapshot.Revision, coordination.GraphTransition{
			TargetKind: coordination.TargetPhaseEndpoint, Endpoint: ref,
			Generation: endpoint.Generation, Action: string(coordination.EndpointSubmitted),
		})
		if err != nil {
			t.Fatalf("submit %s: %v", endpointID, err)
		}
		endpoint = productionEndpoint(snapshot, ref)
		snapshot, err = graph.Transition(ctx, projectID, snapshot.Revision, coordination.GraphTransition{
			TargetKind: coordination.TargetPhaseEndpoint, Endpoint: ref,
			Generation: endpoint.Generation, Action: string(coordination.EndpointSatisfied),
			Result: coordination.PhaseResult{
				ID: "result-" + string(endpointID), Endpoint: ref, BindingRef: endpoint.BindingRef,
				OutputRef: "output-" + string(endpointID), Verdict: coordination.VerdictSatisfied,
			},
		})
		if err != nil {
			t.Fatalf("satisfy %s: %v", endpointID, err)
		}
	}
	registry := evidence.NewPostgresArtifactRegistry(db, objectstore.NewMemoryStore(), "artifacts")
	verifyArtifact, err := registry.Register(ctx, evidence.RegisterArtifact{Type: evidence.ArtifactGeneratedReport, ProjectID: projectID, TaskID: taskID, Path: "verify.json", ContentType: "application/json", Body: []byte(`{"ok":true}`)})
	if err != nil {
		t.Fatal(err)
	}
	diffArtifact, err := registry.Register(ctx, evidence.RegisterArtifact{Type: evidence.ArtifactDiffPatch, ProjectID: projectID, TaskID: taskID, Path: "diff.patch", ContentType: "text/plain", Body: []byte("diff --git\n")})
	if err != nil {
		t.Fatal(err)
	}
	verify := productionEndpoint(snapshot, verifyRef)
	verifyOutputRef := string(verifyArtifact.ID)
	payload, err := json.Marshal(productionTaskCompletionBoundary{
		SourceInputRef: "manager-input:code-merge-source", TaskID: taskID, VerifyEndpoint: verifyRef,
		VerifyOutput: productionPhaseOutputBoundary{OutputRef: verifyOutputRef, Receipt: phasepkg.OutputReceipt{
			Endpoint: verifyRef, Generation: verify.Generation, BindingRef: verify.BindingRef,
			WorkspaceRef: "ws-code-merge-done", WorkspaceHead: "merged-revision",
			Output: phasepkg.PhaseOutput{ReportRef: "report-verify-code-merge", EvidenceRefs: []string{"evidence-verify-code-merge"}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	ingress, err := newProductionIngress(db, projectID, "room-real", productionPhaseTestAssembler(t), graph, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := ingress.setDispatcher(&recordingProductionDispatcher{}); err != nil {
		t.Fatal(err)
	}
	input, err := ingress.DispatchTaskManagerFollowup(ctx, productionInput{
		Kind: "phase_orchestration", RequestID: "code-merge-completion", ConversationID: "runtime:done",
		Body: "code merge done attempt", Payload: payload, SeenRevision: snapshot.Revision,
		SelectedEndpoint: &verifyRef, TargetKind: "task_completion", TargetRef: string(taskID),
	})
	if err != nil {
		t.Fatalf("persist completion input: %v", err)
	}
	principal := productionTestTaskManagerPrincipal(input.InvocationID)
	scope := auth.BoundScope{ProjectID: projectID, InvocationID: input.InvocationID}
	doneDecision := taskmanager.TaskManagerDecision{Action: "done", TargetRef: string(taskID), Reason: "merge queue completed"}
	if _, err := managerRuntime.SubmitTaskManagerDecision(ctx, principal, scope, doneDecision); !kernel.IsCode(err, kernel.CodeTransitionRejected) {
		t.Fatalf("done before merge error=%v, want transition_rejected", err)
	}

	store := workspace.NewPostgresStore(db)
	if err := store.Insert(ctx, workspace.Binding{
		ID: "ws-code-merge-done", Revision: 1, TaskID: taskID, Generation: 1, Kind: workspace.KindGitWorktree,
		Root: t.TempDir(), BaseRevision: "base-revision", CurrentRevision: "candidate-revision",
		AllowedDirs: []string{"."}, DeclaredWrites: workspace.WriteSet{Files: []string{"README.md"}},
		PhaseLeases: map[workspace.Phase]kernel.InvocationID{}, Status: workspace.StatusPrepared,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
INSERT INTO merge_candidates(id, project_id, task_id, workspace_ref, verify_result_ref, diff_artifact_ref,
  target_repository, target_branch, base_revision, main_revision, candidate_revision, status, merged_revision)
VALUES ('candidate-code-merge-done', $1, $2, 'ws-code-merge-done', $3, $4,
  $5, 'main', 'base-revision', 'main-revision', 'candidate-revision', 'merged', 'merged-revision')`,
		projectID, taskID, verifyArtifact.ID, diffArtifact.ID, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	if _, err := managerRuntime.SubmitTaskManagerDecision(ctx, principal, scope, doneDecision); err != nil {
		t.Fatalf("done after merge evidence: %v", err)
	}
	revision, err := managerRuntime.Transition(ctx, principal, scope)
	if err != nil {
		t.Fatalf("done transition after merge evidence: %v", err)
	}
	after, err := graph.Snapshot(ctx, projectID, revision)
	if err != nil {
		t.Fatal(err)
	}
	if after.Tasks[0].Outcome != coordination.TaskDone {
		t.Fatalf("task outcome = %s, want done", after.Tasks[0].Outcome)
	}
}

func TestProductionManagerRecoversPersistedTargetedVerifyProposalAgainstPostgres(t *testing.T) {
	databaseURL := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("THREADMILL_TEST_DATABASE_URL is required for the real PostgreSQL integration test")
	}
	ctx := context.Background()
	db := openProductionPhasePostgres(t, ctx, databaseURL)
	defer db.Close()

	projectID := kernel.ProjectID("project-targeted-reopen-round")
	taskID := kernel.TaskID("task-targeted-reopen-round")
	graph := coordination.NewPostgresStore(db)
	seedProductionCompletionTask(t, ctx, db, projectID, graph, taskID, taskmanager.DeliveryPolicyCodeMerge)
	snapshot, err := graph.Latest(ctx, projectID)
	if err != nil {
		t.Fatal(err)
	}
	// code_merge intentionally withholds the normal Verify phase until the
	// candidate is merged and delivered. A targeted verifier replan must still
	// be able to reopen this round while Verify remains pending.
	for _, endpointID := range []coordination.EndpointID{coordination.EndpointPlan, coordination.EndpointExecute} {
		ref := coordination.PhaseEndpointRef{TaskID: taskID, EndpointID: endpointID}
		endpoint := productionEndpoint(snapshot, ref)
		snapshot, err = graph.Transition(ctx, projectID, snapshot.Revision, coordination.GraphTransition{
			TargetKind: coordination.TargetPhaseEndpoint, Endpoint: ref, Generation: endpoint.Generation, Action: string(coordination.EndpointSubmitted),
		})
		if err != nil {
			t.Fatalf("submit %s: %v", endpointID, err)
		}
		endpoint = productionEndpoint(snapshot, ref)
		snapshot, err = graph.Transition(ctx, projectID, snapshot.Revision, coordination.GraphTransition{
			TargetKind: coordination.TargetPhaseEndpoint, Endpoint: ref, Generation: endpoint.Generation, Action: string(coordination.EndpointSatisfied),
			Result: coordination.PhaseResult{ID: "result-reopen-" + string(endpointID), Endpoint: ref, BindingRef: endpoint.BindingRef, OutputRef: "output-reopen-" + string(endpointID), Verdict: coordination.VerdictSatisfied},
		})
		if err != nil {
			t.Fatalf("satisfy %s: %v", endpointID, err)
		}
	}

	repo := seedProductionPhaseBareRepo(t)
	worktrees := t.TempDir()
	workspaces := workspace.NewPostgresService(db)
	phaseSource := &productionPhaseBindingSource{db: db, projectID: projectID, graph: graph, contracts: taskmanager.NewPostgresStore(db, projectID, graph), workspaces: workspaces, repositoryPath: repo, worktreeParent: worktrees, now: time.Now}
	firstRef, err := phaseSource.EnsureTaskWorkspace(ctx, productionTaskWorkspaceRequest{TaskID: taskID, Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	first, err := workspaces.Get(ctx, firstRef)
	if err != nil {
		t.Fatal(err)
	}
	first, err = workspaces.BindPhase(ctx, first.ID, workspace.PhasePlan, "inv-plan-authority", first.Revision)
	if err != nil {
		t.Fatal(err)
	}
	first, err = workspaces.CompletePhase(ctx, first.ID, workspace.PhasePlan, "inv-plan-authority", first.Revision)
	if err != nil {
		t.Fatal(err)
	}
	first, err = workspaces.AuthorizeExecuteWrites(ctx, first.ID, workspace.WriteSet{Files: []string{"workspace/reopen.go"}}, first.Revision)
	if err != nil {
		t.Fatal(err)
	}
	first, err = workspaces.BindPhase(ctx, first.ID, workspace.PhaseExecute, "inv-execute-authority", first.Revision)
	if err != nil {
		t.Fatal(err)
	}
	first, err = workspaces.CompletePhase(ctx, first.ID, workspace.PhaseExecute, "inv-execute-authority", first.Revision)
	if err != nil {
		t.Fatal(err)
	}
	latestMain := advanceProductionPhaseBareRepo(t, repo)

	registry := evidence.NewPostgresArtifactRegistry(db, objectstore.NewMemoryStore(), "artifacts")
	verifyArtifact, err := registry.Register(ctx, evidence.RegisterArtifact{Type: evidence.ArtifactGeneratedReport, ProjectID: projectID, TaskID: taskID, Path: "verify/reopen.json", ContentType: "application/json", Body: []byte(`{"verdict":"pass"}`)})
	if err != nil {
		t.Fatal(err)
	}
	diffArtifact, err := registry.Register(ctx, evidence.RegisterArtifact{Type: evidence.ArtifactDiffPatch, ProjectID: projectID, TaskID: taskID, Path: "merge/reopen.patch", ContentType: "text/x-diff", Body: []byte("diff --git a/workspace/reopen.go b/workspace/reopen.go\n")})
	if err != nil {
		t.Fatal(err)
	}
	candidateID := mergequeue.CandidateID("candidate-targeted-reopen")
	failureArtifact, err := registry.Register(ctx, evidence.RegisterArtifact{Type: evidence.ArtifactToolOutput, ProjectID: projectID, TaskID: taskID, Path: "merge/reopen-failure.log", ContentType: "text/plain", Body: []byte("verifier requested replan")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
INSERT INTO merge_candidates(id,project_id,task_id,workspace_ref,verify_result_ref,diff_artifact_ref,target_repository,target_branch,base_revision,main_revision,candidate_revision,status,failure_reason,failure_evidence_ref,created_at,updated_at)
VALUES($1,$2,$3,$4,$5,$6,$7,'main',$8,$8,$9,'failed','verify_failed',$10,now(),now())`,
		candidateID, projectID, taskID, first.ID, verifyArtifact.ID, diffArtifact.ID, repo, first.BaseRevision, first.CurrentRevision, failureArtifact.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
INSERT INTO production_merge_deliveries(project_id,candidate_id,task_id,verify_result_ref,completion_payload,payload_hash,status,created_at,updated_at)
VALUES($1,$2,$3,$4,'{}'::jsonb,'hash','failed',now(),now())`, projectID, candidateID, taskID, verifyArtifact.ID); err != nil {
		t.Fatal(err)
	}

	invocationID := kernel.InvocationID("inv-targeted-reopen")
	req := mergequeue.TargetedVerifyRequest{Candidate: mergequeue.Candidate{ID: candidateID, ProjectID: projectID, TaskID: taskID}, LatestMainRevision: latestMain}
	if err := runtimepkg.NewPostgresInvocationStoreFromSQL(db).Create(ctx, runtimepkg.Invocation{
		ID: invocationID, ActorPrincipalID: "targeted-verifier", ProjectID: projectID, TaskID: taskID, EndpointID: coordination.EndpointVerify,
		Generation: 2, Role: auth.RoleVerifier, Status: runtimepkg.InvocationFailed, BindingRef: productionTargetedVerifyBindingRef(req), LeaseID: "lease-targeted-reopen", WorkspaceRef: "targeted-temp-worktree",
		PromptHashes: map[string]string{"prompt": "hash"}, SkillHashes: map[string]string{"skill": "hash"}, EffectiveTools: []auth.Tool{auth.ToolAgentProposeOrchestration}, CreatedAt: time.Now().Add(-time.Minute), ExpiresAt: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	proposal := phasepkg.OrchestrationProposal{
		ProposalID: "proposal-targeted-reopen", ClientRef: "client-targeted-reopen",
		FromEndpoint: coordination.PhaseEndpointRef{TaskID: taskID, EndpointID: coordination.EndpointVerify}, FromInvocationID: invocationID,
		BasedOnGraphRevision: snapshot.Revision, BasedOnWorkspaceRevision: latestMain, BasedOnInputRevision: "targeted-input-revision",
		OrchestrationAdvice: phasepkg.OrchestrationReplan, DeliverySpecAdvice: "rerun execute on latest main", ReportSpecAdvice: "verify the fresh round",
		Rationale: "resolving the conflict inside verification would violate the Task Contract", EvidenceRefs: []string{string(failureArtifact.ID)},
	}
	payload, err := json.Marshal(productionTargetedVerifyProposalBoundary{OrchestrationProposal: proposal, SourceKind: productionTargetedVerifyProposalSource, CandidateID: candidateID})
	if err != nil {
		t.Fatal(err)
	}
	ingress, err := newProductionIngress(db, projectID, "room-reopen", productionPhaseTestAssembler(t), graph, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := ingress.setDispatcher(&recordingProductionDispatcher{}); err != nil {
		t.Fatal(err)
	}
	managerRuntime, err := newProductionTaskManagerRuntime(db, projectID, graph, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	managerRuntime.workspaces = phaseSource
	managerRuntime.contexts = recordingTaskContextProjector{}
	if err := managerRuntime.setProductionEventStore(evidence.NewPostgresEventStore(db, 1<<20)); err != nil {
		t.Fatal(err)
	}
	decision := taskmanager.TaskManagerDecision{Action: "reopen_round", TargetRef: string(taskID), Reason: "targeted verifier cannot preserve the Task Contract while resolving the conflict"}

	ordinaryPayload, err := json.Marshal(proposal)
	if err != nil {
		t.Fatal(err)
	}
	ordinaryInput, err := ingress.persistAndDispatch(ctx, productionInput{
		Kind: "phase_orchestration", RequestID: "ordinary-" + proposal.ProposalID, ConversationID: "runtime:" + string(taskID), Body: proposal.Rationale,
		Payload: ordinaryPayload, SeenRevision: snapshot.Revision, SelectedEndpoint: &proposal.FromEndpoint, TargetKind: "phase_orchestration", TargetRef: proposal.ProposalID,
	})
	if err != nil {
		t.Fatal(err)
	}
	ordinaryPrincipal := productionTestTaskManagerPrincipal(ordinaryInput.InvocationID)
	ordinaryPrincipal.ProjectID = projectID
	if _, err := managerRuntime.SubmitTaskManagerDecision(ctx, ordinaryPrincipal, auth.BoundScope{ProjectID: projectID, InvocationID: ordinaryInput.InvocationID}, decision); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("ordinary verifier proposal reopen error=%v, want forbidden", err)
	}

	forgedProposal := proposal
	forgedProposal.ProposalID = "proposal-forged-candidate"
	forgedPayload, err := json.Marshal(productionTargetedVerifyProposalBoundary{OrchestrationProposal: forgedProposal, SourceKind: productionTargetedVerifyProposalSource, CandidateID: "candidate-does-not-exist"})
	if err != nil {
		t.Fatal(err)
	}
	forgedInput, err := ingress.persistAndDispatch(ctx, productionInput{
		Kind: "phase_orchestration", RequestID: forgedProposal.ProposalID, ConversationID: "runtime:" + string(taskID), Body: forgedProposal.Rationale,
		Payload: forgedPayload, SeenRevision: snapshot.Revision, SelectedEndpoint: &forgedProposal.FromEndpoint, TargetKind: "phase_orchestration", TargetRef: forgedProposal.ProposalID,
	})
	if err != nil {
		t.Fatal(err)
	}
	forgedPrincipal := productionTestTaskManagerPrincipal(forgedInput.InvocationID)
	forgedPrincipal.ProjectID = projectID
	if _, err := managerRuntime.SubmitTaskManagerDecision(ctx, forgedPrincipal, auth.BoundScope{ProjectID: projectID, InvocationID: forgedInput.InvocationID}, decision); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("forged targeted verifier candidate reopen error=%v, want forbidden", err)
	}
	for _, rejectedInput := range []persistedProductionInput{ordinaryInput, forgedInput} {
		var decisions int
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM taskmanager_decisions WHERE project_id=$1 AND input_ref=$2`, projectID, rejectedInput.InputRef).Scan(&decisions); err != nil {
			t.Fatal(err)
		}
		if decisions != 0 {
			t.Fatalf("rejected reopen input %s persisted %d decisions, want 0", rejectedInput.InputRef, decisions)
		}
	}

	recoveryBody := "根据最新失败的合入事实和 targeted verifier 意见恢复该任务"
	recoveryRequest := httpapi.ManagerMessageRequest{
		RequestID: "manager-recover-" + proposal.ProposalID, ProjectID: projectID,
		ConversationID: "operator:reopen", Body: recoveryBody, Intent: httpapi.ManagerIntentOrchestrate,
	}
	recoveryPayload, err := json.Marshal(recoveryRequest)
	if err != nil {
		t.Fatal(err)
	}
	recoveryInput, err := ingress.persistAndDispatch(ctx, productionInput{
		Kind: "manager", RequestID: recoveryRequest.RequestID, ConversationID: recoveryRequest.ConversationID,
		Body: recoveryBody, Payload: recoveryPayload, SeenRevision: snapshot.Revision,
	})
	if err != nil {
		t.Fatal(err)
	}
	recoveryPrincipal := productionTestTaskManagerPrincipal(recoveryInput.InvocationID)
	recoveryPrincipal.ProjectID = projectID
	recoveryScope := auth.BoundScope{ProjectID: projectID, InvocationID: recoveryInput.InvocationID}
	if _, err := managerRuntime.SubmitTaskManagerDecision(ctx, recoveryPrincipal, recoveryScope, decision); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("manager recovery without completed targeted proposal error=%v, want forbidden", err)
	}
	var recoveryDecisions int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM taskmanager_decisions WHERE project_id=$1 AND input_ref=$2`, projectID, recoveryInput.InputRef).Scan(&recoveryDecisions); err != nil {
		t.Fatal(err)
	}
	if recoveryDecisions != 0 {
		t.Fatalf("rejected manager recovery persisted %d decisions, want 0", recoveryDecisions)
	}

	// Persist the genuine internal proposal as completed, modelling a previous
	// Manager invocation that failed to reopen the round. The same ordinary
	// Manager input may now recover because Runtime, not its text payload, can
	// prove the historical authority.
	input, err := ingress.persistAndDispatch(ctx, productionInput{
		Kind: "phase_orchestration", RequestID: proposal.ProposalID, ConversationID: "runtime:" + string(taskID), Body: proposal.Rationale,
		Payload: payload, SeenRevision: snapshot.Revision, SelectedEndpoint: &proposal.FromEndpoint, TargetKind: "phase_orchestration", TargetRef: proposal.ProposalID,
	})
	if err != nil {
		t.Fatal(err)
	}
	principal := productionTestTaskManagerPrincipal(input.InvocationID)
	principal.ProjectID = projectID
	scope := auth.BoundScope{ProjectID: projectID, InvocationID: input.InvocationID}
	if _, err := managerRuntime.SubmitTaskManagerDecision(ctx, principal, scope, taskmanager.TaskManagerDecision{Action: "no_change", Reason: "historical manager failed to apply the verifier replan"}); err != nil {
		t.Fatalf("complete historical targeted proposal: %v", err)
	}
	recoverySnapshot, err := managerRuntime.Snapshot(ctx, recoveryPrincipal, recoveryScope, kernel.LatestRevision)
	if err != nil {
		t.Fatalf("snapshot recoverable delivery facts: %v", err)
	}
	var recoveryDelivery mcpapi.TaskManagerDeliveryState
	for _, delivery := range recoverySnapshot.Deliveries {
		if delivery.TaskID == taskID {
			recoveryDelivery = delivery
			break
		}
	}
	if recoveryDelivery.LatestReplanProposalRef != proposal.ProposalID || !recoveryDelivery.ReopenRoundAvailable || recoveryDelivery.ReadyForVerify {
		t.Fatalf("recoverable delivery projection=%#v", recoveryDelivery)
	}
	if _, err := managerRuntime.SubmitTaskManagerDecision(ctx, recoveryPrincipal, recoveryScope, decision); err != nil {
		t.Fatalf("submit manager recovery reopen_round decision: %v", err)
	}
	revision, err := managerRuntime.Transition(ctx, recoveryPrincipal, recoveryScope)
	if err != nil {
		t.Fatalf("apply reopen_round transition: %v", err)
	}
	after, err := graph.Snapshot(ctx, projectID, revision)
	if err != nil {
		t.Fatal(err)
	}
	for _, endpointID := range []coordination.EndpointID{coordination.EndpointExecute, coordination.EndpointVerify} {
		endpoint := productionEndpoint(after, coordination.PhaseEndpointRef{TaskID: taskID, EndpointID: endpointID})
		if endpoint.State != coordination.EndpointPending || endpoint.RunPolicy != coordination.RunEnabled || endpoint.Generation != 2 {
			t.Fatalf("reopened %s endpoint = %#v", endpointID, endpoint)
		}
	}
	rows, err := db.QueryContext(ctx, `SELECT phase_endpoint FROM evidence_events WHERE project_id=$1 AND graph_revision=$2 AND type='endpoint.updated' ORDER BY phase_endpoint`, projectID, revision)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var updated []coordination.EndpointID
	for rows.Next() {
		var endpointID coordination.EndpointID
		if err := rows.Scan(&endpointID); err != nil {
			t.Fatal(err)
		}
		updated = append(updated, endpointID)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(updated) != 2 || updated[0] != coordination.EndpointExecute || updated[1] != coordination.EndpointVerify {
		t.Fatalf("reopen endpoint events = %v, want [execute verify]", updated)
	}
	var graphEvents int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM evidence_events WHERE project_id=$1 AND graph_revision=$2 AND type='graph.revision'`, projectID, revision).Scan(&graphEvents); err != nil {
		t.Fatal(err)
	}
	if graphEvents != 1 {
		t.Fatalf("reopen graph revision events = %d, want 1", graphEvents)
	}
	second, ok, err := workspaces.GetByRound(ctx, taskID, 2)
	if err != nil || !ok {
		t.Fatalf("fresh workspace found=%v err=%v", ok, err)
	}
	if second.BaseRevision != latestMain || second.PhaseLeases[workspace.PhasePlan] != "inv-plan-authority" || second.PhaseLeases[workspace.PhaseExecute] != "" ||
		len(second.DeclaredWrites.Files) != 1 || second.DeclaredWrites.Files[0] != "workspace/reopen.go" {
		t.Fatalf("fresh reopened workspace = %#v", second)
	}
}

type recordingTaskContextProjector struct{}

func (recordingTaskContextProjector) EnsureTaskContext(context.Context, productionTaskContextRequest) error {
	return nil
}

type productionTaskManagerTestFinalizer struct{}

func (productionTaskManagerTestFinalizer) FinalizeTaskMemory(_ context.Context, _ auth.Principal, taskID kernel.TaskID) (contextgraph.FrozenCandidateBatch, error) {
	return contextgraph.FrozenCandidateBatch{TaskID: string(taskID)}, nil
}

func seedProductionCompletionTask(t *testing.T, ctx context.Context, db *sql.DB, projectID kernel.ProjectID, graph *coordination.PostgresStore, taskID kernel.TaskID, policy taskmanager.DeliveryPolicy) {
	t.Helper()
	contract := taskmanager.TaskContract{
		TaskID: taskID, ContractRef: "contract://" + string(taskID), DeliveryPolicy: policy,
		PhaseSpecs: map[coordination.EndpointID]string{
			coordination.EndpointPlan:    "spec://" + string(taskID) + "/plan",
			coordination.EndpointExecute: "spec://" + string(taskID) + "/execute",
			coordination.EndpointVerify:  "spec://" + string(taskID) + "/verify",
		},
	}
	if err := taskmanager.NewPostgresStore(db, projectID, graph).PersistRequirementContract(ctx, taskmanager.RequirementInput{
		InputRef: "input://" + string(taskID), TaskID: taskID, ContractRef: contract.ContractRef,
		Requirement: taskmanager.Requirement{Text: "complete " + string(taskID)},
	}, contract); err != nil {
		t.Fatalf("persist contract: %v", err)
	}
	if _, err := graph.ReplacePending(ctx, projectID, coordination.PendingSubgraph{
		RequestID: kernel.IdempotencyKey("seed-" + string(taskID)), BaseRevision: 1,
		Tasks: []coordination.Task{{ID: taskID, ContractRef: contract.ContractRef, Outcome: coordination.TaskActive}},
		Endpoints: []coordination.PhaseEndpoint{
			{Ref: coordination.PhaseEndpointRef{TaskID: taskID, EndpointID: coordination.EndpointPlan}, SpecRef: contract.PhaseSpecs[coordination.EndpointPlan], BindingRef: canonicalProductionBindingRef(coordination.PhaseEndpointRef{TaskID: taskID, EndpointID: coordination.EndpointPlan}, 1), Generation: 1, State: coordination.EndpointPending, RunPolicy: coordination.RunEnabled},
			{Ref: coordination.PhaseEndpointRef{TaskID: taskID, EndpointID: coordination.EndpointExecute}, SpecRef: contract.PhaseSpecs[coordination.EndpointExecute], BindingRef: canonicalProductionBindingRef(coordination.PhaseEndpointRef{TaskID: taskID, EndpointID: coordination.EndpointExecute}, 1), Generation: 1, State: coordination.EndpointPending, RunPolicy: coordination.RunEnabled},
			{Ref: coordination.PhaseEndpointRef{TaskID: taskID, EndpointID: coordination.EndpointVerify}, SpecRef: contract.PhaseSpecs[coordination.EndpointVerify], BindingRef: canonicalProductionBindingRef(coordination.PhaseEndpointRef{TaskID: taskID, EndpointID: coordination.EndpointVerify}, 1), Generation: 1, State: coordination.EndpointPending, RunPolicy: coordination.RunEnabled},
		},
	}); err != nil {
		t.Fatalf("seed graph: %v", err)
	}
}
