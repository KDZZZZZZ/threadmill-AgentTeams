package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/adapters/agentteams"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/contextgraph"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/evidence"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/postgres"
	runtimepkg "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime"
	phasepkg "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime/phase"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime/promptcatalog"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/taskmanager"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/transport/httpapi"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/transport/mcpapi"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/uiprojection"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/migrations"
)

func TestProductionIngressPersistsInvocationConversationAndIdempotencyAgainstPostgres(t *testing.T) {
	databaseURL := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("THREADMILL_TEST_DATABASE_URL is required for the real PostgreSQL integration test")
	}
	ctx := context.Background()
	admin, err := postgres.Open(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	defer admin.Close(context.Background())
	schema := fmt.Sprintf("tm_production_ingress_%d", time.Now().UnixNano())
	if _, err := admin.SQL().ExecContext(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.SQL().ExecContext(context.Background(), `DROP SCHEMA IF EXISTS `+schema+` CASCADE`)
	})
	scopedURL, err := productionTestDatabaseURLWithSearchPath(databaseURL, schema)
	if err != nil {
		t.Fatal(err)
	}
	db, err := postgres.Open(ctx, scopedURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(context.Background())
	loaded, err := postgres.LoadMigrations(migrations.FS, migrations.Dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := postgres.NewMigrator(db.SQL()).Apply(ctx, loaded); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	available := make(map[auth.Tool]struct{})
	for _, tool := range auth.CanonicalTools() {
		available[tool] = struct{}{}
	}
	catalog, err := promptcatalog.Load(filepath.Join("..", ".."), available)
	if err != nil {
		t.Fatal(err)
	}
	assembler, err := runtimepkg.NewAssembler(catalog)
	if err != nil {
		t.Fatal(err)
	}
	graph := coordination.NewPostgresStore(db.SQL())
	now := time.Date(2026, time.August, 11, 15, 0, 0, 0, time.UTC)
	managerRuntime, err := newProductionTaskManagerRuntime(db.SQL(), "project-real", graph, func() time.Time { return now.Add(time.Minute) })
	if err != nil {
		t.Fatal(err)
	}
	ingress, err := newProductionIngress(db.SQL(), "project-real", "room-real", assembler, graph, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	decision := taskmanager.TaskManagerDecision{Action: "no_change", Reason: "the graph already reflects the request"}
	dispatcher := &recordingProductionDispatcher{duringDispatch: func(invocationRef string) error {
		principal := productionTestTaskManagerPrincipal(kernel.InvocationID(invocationRef))
		scope := auth.BoundScope{ProjectID: "project-real", InvocationID: principal.InvocationID}
		_, err := managerRuntime.SubmitTaskManagerDecision(ctx, principal, scope, decision)
		return err
	}}
	if err := ingress.setDispatcher(dispatcher); err != nil {
		t.Fatal(err)
	}
	principal := auth.Principal{ActorPrincipalID: "operator-real", Kind: auth.PrincipalOperator, ProjectID: "project-real", Role: auth.RoleOperator}
	req := httpapi.ManagerMessageRequest{RequestID: "request-real", ProjectID: "project-real", ConversationID: "conversation-real", Body: "hold the selected endpoint", SelectedEndpoint: &coordination.PhaseEndpointRef{TaskID: "task-a", EndpointID: coordination.EndpointExecute}}
	first, err := ingress.SubmitManagerMessage(ctx, principal, req)
	if err != nil {
		t.Fatalf("SubmitManagerMessage() error = %v", err)
	}
	second, err := ingress.SubmitManagerMessage(ctx, principal, req)
	if err != nil {
		t.Fatalf("idempotent SubmitManagerMessage() error = %v", err)
	}
	if first != second || dispatcher.calls != 1 {
		t.Fatalf("idempotent response first=%#v second=%#v dispatches=%d", first, second, dispatcher.calls)
	}
	conversation, err := ingress.Conversation(ctx, principal, "conversation-real", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(conversation.Messages) != 2 || conversation.Messages[0].ManagerInputRef != first.ManagerInputRef || conversation.Messages[1].DecisionRef == "" {
		t.Fatalf("persisted conversation = %#v", conversation)
	}
	var role, status string
	if err := db.SQL().QueryRowContext(ctx, `SELECT role, status FROM runtime_invocations WHERE invocation_id=$1`, first.InvocationRef).Scan(&role, &status); err != nil {
		t.Fatal(err)
	}
	if role != string(auth.RoleTaskManager) || status != string(runtimepkg.InvocationCompleted) {
		t.Fatalf("persisted invocation role=%q status=%q", role, status)
	}
	managerPrincipal := productionTestTaskManagerPrincipal(first.InvocationRef)
	managerScope := auth.BoundScope{ProjectID: "project-real", InvocationID: first.InvocationRef}
	decisionRef, err := managerRuntime.SubmitTaskManagerDecision(ctx, managerPrincipal, managerScope, decision)
	if err != nil {
		t.Fatalf("SubmitTaskManagerDecision() error = %v", err)
	}
	if replayed, err := managerRuntime.SubmitTaskManagerDecision(ctx, managerPrincipal, managerScope, decision); err != nil || replayed != decisionRef {
		t.Fatalf("idempotent decision replay ref=%q error=%v", replayed, err)
	}
	changedDecision := decision
	changedDecision.Reason = "different reason"
	if _, err := managerRuntime.SubmitTaskManagerDecision(ctx, managerPrincipal, managerScope, changedDecision); !kernel.IsCode(err, kernel.CodeIdempotencyConflict) {
		t.Fatalf("changed decision replay error = %v, want idempotency_conflict", err)
	}
	var appliedRevision kernel.Revision
	if err := db.SQL().QueryRowContext(ctx, `SELECT applied_graph_revision FROM production_taskmanager_bindings WHERE invocation_id=$1`, first.InvocationRef).Scan(&appliedRevision); err != nil {
		t.Fatal(err)
	}
	if appliedRevision != 1 {
		t.Fatalf("terminal decision applied revision = %d, want 1", appliedRevision)
	}
	conflict := req
	conflict.Body = "different payload"
	if _, err := ingress.SubmitManagerMessage(ctx, principal, conflict); !kernel.IsCode(err, kernel.CodeIdempotencyConflict) {
		t.Fatalf("conflicting replay error = %v, want idempotency_conflict", err)
	}
	dispatcher.mu.Lock()
	dispatcher.duringDispatch = nil
	dispatcher.nextErr = kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "no healthy AgentTeams host has matching capacity", Recoverable: true}
	dispatcher.mu.Unlock()
	queued, err := ingress.SubmitRequirement(ctx, principal, httpapi.RequirementCreateRequest{
		RequestID: "requirement-queued", ProjectID: "project-real", Body: "queue this durable requirement",
	})
	if err != nil {
		t.Fatalf("durably queued requirement returned dispatch capacity error: %v", err)
	}
	if queued.Status != "accepted" || queued.ManagerInputRef == "" || queued.InvocationRef == "" {
		t.Fatalf("queued requirement response = %#v", queued)
	}
	if err := db.SQL().QueryRowContext(ctx, `SELECT status FROM production_manager_inputs WHERE input_ref=$1`, queued.ManagerInputRef).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "pending" {
		t.Fatalf("queued requirement durable status = %q, want pending", status)
	}
}

func TestProductionTaskManagerTrustedResourcesAndTwoStepBoundariesAgainstPostgres(t *testing.T) {
	databaseURL := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("THREADMILL_TEST_DATABASE_URL is required for the real PostgreSQL integration test")
	}
	ctx := context.Background()
	admin, err := postgres.Open(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	defer admin.Close(context.Background())
	schema := fmt.Sprintf("tm_production_trusted_%d", time.Now().UnixNano())
	if _, err := admin.SQL().ExecContext(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.SQL().ExecContext(context.Background(), `DROP SCHEMA IF EXISTS `+schema+` CASCADE`)
	})
	scopedURL, err := productionTestDatabaseURLWithSearchPath(databaseURL, schema)
	if err != nil {
		t.Fatal(err)
	}
	db, err := postgres.Open(ctx, scopedURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(context.Background())
	loaded, err := postgres.LoadMigrations(migrations.FS, migrations.Dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := postgres.NewMigrator(db.SQL()).Apply(ctx, loaded); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	available := make(map[auth.Tool]struct{})
	for _, tool := range auth.CanonicalTools() {
		available[tool] = struct{}{}
	}
	catalog, err := promptcatalog.Load(filepath.Join("..", ".."), available)
	if err != nil {
		t.Fatal(err)
	}
	assembler, err := runtimepkg.NewAssembler(catalog)
	if err != nil {
		t.Fatal(err)
	}
	graph := coordination.NewPostgresStore(db.SQL())
	eventStore := evidence.NewPostgresEventStore(db.SQL(), 1<<20)
	now := time.Date(2026, time.August, 11, 16, 0, 0, 0, time.UTC)
	ingress, err := newProductionIngress(db.SQL(), "project-real", "room-real", assembler, graph, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	dispatcher := &recordingProductionDispatcher{}
	if err := ingress.setDispatcher(dispatcher); err != nil {
		t.Fatal(err)
	}
	contexts := contextgraph.NewPostgresStore(db.SQL(), func() time.Time { return now.Add(30 * time.Second) })
	contexts.SetTaskEndpointResolver(productionTaskEndpointResolver{projectID: "project-real", graph: graph})
	contextRuntime, err := newProductionContextRuntime(db.SQL(), "project-real", "room-real", assembler, contexts, func() time.Time { return now.Add(30 * time.Second) })
	if err != nil {
		t.Fatal(err)
	}
	if err := contextRuntime.setDispatcher(&productionContextTestAdapter{}); err != nil {
		t.Fatal(err)
	}
	resources := newRecordingTaskResources()
	resources.contextDelegate = &productionPhaseBindingSource{
		db: db.SQL(), projectID: "project-real", graph: graph,
		contracts: taskmanager.NewPostgresStore(db.SQL(), "project-real", graph), contexts: contexts,
		now: func() time.Time { return now.Add(30 * time.Second) },
	}
	managerRuntime, err := newProductionTaskManagerRuntime(db.SQL(), "project-real", graph, func() time.Time { return now.Add(time.Minute) })
	if err != nil {
		t.Fatal(err)
	}
	if err := managerRuntime.setProductionEventStore(eventStore); err != nil {
		t.Fatal(err)
	}
	if err := managerRuntime.setProductionDependencies(resources, resources, ingress); err != nil {
		t.Fatal(err)
	}
	if err := managerRuntime.setProductionMemoryFinalizer(contextRuntime); err != nil {
		t.Fatal(err)
	}
	operator := auth.Principal{ActorPrincipalID: "operator-real", Kind: auth.PrincipalOperator, ProjectID: "project-real", Role: auth.RoleOperator}
	requirement, err := ingress.SubmitRequirement(ctx, operator, httpapi.RequirementCreateRequest{
		RequestID: "requirement-trusted", ProjectID: "project-real", ConversationID: "conversation-trusted",
		Body: "implement and verify the production workflow", Motivation: "ship a recoverable workflow",
		Constraints: []string{"use real resources"}, Acceptance: []string{"two-step decisions are persisted"},
	})
	if err != nil {
		t.Fatal(err)
	}
	managerPrincipal := productionTestTaskManagerPrincipal(requirement.InvocationRef)
	managerScope := auth.BoundScope{ProjectID: "project-real", InvocationID: requirement.InvocationRef}
	decision := taskmanager.TaskManagerDecision{Action: "replace_pending", Reason: "create the fixed phase contract"}
	if _, err := managerRuntime.SubmitTaskManagerDecision(ctx, managerPrincipal, managerScope, decision); err != nil {
		t.Fatal(err)
	}
	intent := mcpapi.PendingSubgraphIntent{
		TaskPolicies: []mcpapi.PendingTaskPolicyIntent{{
			TaskID: "task-trusted", DeliveryPolicy: taskmanager.DeliveryPolicyNonCodeArtifact,
		}},
		Endpoints: []mcpapi.PendingEndpointIntent{
			productionPendingEndpoint("task-trusted", coordination.EndpointPlan),
			productionPendingEndpoint("task-trusted", coordination.EndpointExecute),
			productionPendingEndpoint("task-trusted", coordination.EndpointVerify),
		},
	}
	resources.failContextOnce = true
	if _, err := managerRuntime.ReplacePending(ctx, managerPrincipal, managerScope, intent); !errors.Is(err, errProductionContextProjection) {
		t.Fatalf("ReplacePending() first error = %v, want context projection crash", err)
	}
	afterGraph, err := graph.Latest(ctx, "project-real")
	if err != nil {
		t.Fatal(err)
	}
	if afterGraph.Revision != 2 {
		t.Fatalf("graph revision after projection crash = %d, want 2", afterGraph.Revision)
	}
	events := uiprojection.NewEventLogQuery(eventStore, allowProjectPermission{projectID: "project-real"})
	operatorPrincipal := auth.Principal{ActorPrincipalID: "operator-real", Kind: auth.PrincipalOperator, ProjectID: "project-real", Role: auth.RoleOperator}
	assertProductionUIEvents(t, ctx, events, operatorPrincipal, "project-real", map[string]int{
		"manager.interaction": 1,
	})
	assertProductionUIEventCounts(t, ctx, events, operatorPrincipal, "project-real", map[string]int{
		"graph.revision":   0,
		"endpoint.updated": 0,
	})
	if len(afterGraph.Tasks) != 1 || afterGraph.Tasks[0].Outcome != coordination.TaskActive {
		t.Fatalf("Runtime did not normalize task authority: %#v", afterGraph.Tasks)
	}
	for _, endpoint := range afterGraph.Endpoints {
		if endpoint.Generation != 1 || endpoint.State != coordination.EndpointPending || endpoint.BindingRef != canonicalProductionBindingRef(endpoint.Ref, 1) {
			t.Fatalf("Runtime did not normalize endpoint authority: %#v", endpoint)
		}
	}
	var contractRef, requirementText, phaseSpecs string
	if err := db.SQL().QueryRowContext(ctx, `SELECT c.contract_ref, r.requirement->>'text', c.phase_specs::text
FROM taskmanager_contracts c JOIN taskmanager_requirement_inputs r
ON r.project_id=c.project_id AND r.input_ref=c.input_ref
WHERE c.project_id=$1 AND c.task_id=$2`, "project-real", "task-trusted").Scan(&contractRef, &requirementText, &phaseSpecs); err != nil {
		t.Fatal(err)
	}
	wantContract := canonicalProductionContractRef("project-real", requirement.ManagerInputRef, "task-trusted")
	wantPlanSpec := canonicalProductionSpecRef("project-real", requirement.ManagerInputRef, coordination.PhaseEndpointRef{TaskID: "task-trusted", EndpointID: coordination.EndpointPlan})
	wantExecuteSpec := canonicalProductionSpecRef("project-real", requirement.ManagerInputRef, coordination.PhaseEndpointRef{TaskID: "task-trusted", EndpointID: coordination.EndpointExecute})
	wantVerifySpec := canonicalProductionSpecRef("project-real", requirement.ManagerInputRef, coordination.PhaseEndpointRef{TaskID: "task-trusted", EndpointID: coordination.EndpointVerify})
	if contractRef != wantContract || requirementText != "implement and verify the production workflow" || !stringsContainAll(phaseSpecs, wantPlanSpec, wantExecuteSpec, wantVerifySpec) {
		t.Fatalf("persisted contract=%q requirement=%q specs=%q", contractRef, requirementText, phaseSpecs)
	}
	if resources.workspaceCallCount("task-trusted", 1) == 0 {
		t.Fatal("workspace was not provisioned before graph mutation")
	}

	// A fresh Runtime instance must compensate the already-applied graph
	// revision without writing a second revision.
	restarted, err := newProductionTaskManagerRuntime(db.SQL(), "project-real", graph, func() time.Time { return now.Add(2 * time.Minute) })
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.setProductionEventStore(eventStore); err != nil {
		t.Fatal(err)
	}
	if err := restarted.setProductionDependencies(resources, resources, ingress); err != nil {
		t.Fatal(err)
	}
	if err := restarted.setProductionMemoryFinalizer(contextRuntime); err != nil {
		t.Fatal(err)
	}
	revision, err := restarted.ReplacePending(ctx, managerPrincipal, managerScope, intent)
	if err != nil {
		t.Fatalf("ReplacePending() recovery error = %v", err)
	}
	if revision != 2 || resources.contextCallCount("task-trusted") != 1 {
		t.Fatalf("recovered revision=%d successful context calls=%d", revision, resources.contextCallCount("task-trusted"))
	}
	assertProductionUIEvents(t, ctx, events, operatorPrincipal, "project-real", map[string]int{
		"graph.revision":   1,
		"endpoint.updated": 3,
	})
	changedPolicy := intent
	changedPolicy.Endpoints = append([]mcpapi.PendingEndpointIntent(nil), intent.Endpoints...)
	changedPolicy.Endpoints[0].RunPolicy = coordination.RunHeld
	if _, err := restarted.ReplacePending(ctx, managerPrincipal, managerScope, changedPolicy); !kernel.IsCode(err, kernel.CodeInvalidRequest) {
		t.Fatalf("changed frozen run policy error = %v, want invalid_request", err)
	}

	// Phase output is first acknowledged as submitted, then evaluated by a
	// separate persisted input and Task Manager invocation.
	executeRef := coordination.PhaseEndpointRef{TaskID: "task-trusted", EndpointID: coordination.EndpointExecute}
	execute := productionEndpoint(afterGraph, executeRef)
	outputBoundary := productionPhaseOutputBoundary{OutputRef: "output-execute-1", Receipt: phasepkg.OutputReceipt{
		InvocationID: "phase-invocation-1", Endpoint: executeRef, Generation: execute.Generation,
		BindingRef: execute.BindingRef, LeaseRef: "lease-execute-1", InputRevision: "inputs-1",
		Output: phasepkg.PhaseOutput{ReportRef: "report-execute-1", EvidenceRefs: []string{"evidence-execute-1"}},
	}}
	outputPayload, _ := json.Marshal(outputBoundary)
	outputInput, err := ingress.persistAndDispatch(ctx, productionInput{
		Kind: "phase_output", RequestID: "phase-output-execute-1", ConversationID: "runtime:task-trusted",
		Body: "phase output execute output-execute-1", Payload: outputPayload, SeenRevision: 2,
		SelectedEndpoint: &executeRef, TargetKind: "phase_output", TargetRef: "output-execute-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	outputPrincipal := productionTestTaskManagerPrincipal(outputInput.InvocationID)
	outputScope := auth.BoundScope{ProjectID: "project-real", InvocationID: outputInput.InvocationID}
	if _, err := restarted.SubmitTaskManagerDecision(ctx, outputPrincipal, outputScope, taskmanager.TaskManagerDecision{Action: "submitted", TargetRef: "task-trusted/execute", Reason: "record output before evaluation"}); err != nil {
		t.Fatal(err)
	}
	dispatcher.failNextDispatch()
	if _, err := restarted.Transition(ctx, outputPrincipal, outputScope); !errors.Is(err, errProductionDispatch) {
		t.Fatalf("submitted transition dispatch crash = %v, want injected dispatch error", err)
	}
	afterSubmitted, err := graph.Latest(ctx, "project-real")
	if err != nil {
		t.Fatal(err)
	}
	if afterSubmitted.Revision != 3 || productionEndpoint(afterSubmitted, executeRef).State != coordination.EndpointSubmitted {
		t.Fatalf("submitted graph = %#v", afterSubmitted)
	}
	assertProductionUIEvents(t, ctx, events, operatorPrincipal, "project-real", map[string]int{
		"manager.interaction": 2,
	})
	assertProductionUIEventCounts(t, ctx, events, operatorPrincipal, "project-real", map[string]int{
		"graph.revision":     1,
		"endpoint.updated":   3,
		"invocation.updated": 0,
	})

	restartedAgain, err := newProductionTaskManagerRuntime(db.SQL(), "project-real", graph, func() time.Time { return now.Add(3 * time.Minute) })
	if err != nil {
		t.Fatal(err)
	}
	if err := restartedAgain.setProductionEventStore(eventStore); err != nil {
		t.Fatal(err)
	}
	if err := restartedAgain.setProductionDependencies(resources, resources, ingress); err != nil {
		t.Fatal(err)
	}
	if err := restartedAgain.setProductionMemoryFinalizer(contextRuntime); err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			got, callErr := restartedAgain.Transition(ctx, outputPrincipal, outputScope)
			if callErr == nil && got != 3 {
				callErr = fmt.Errorf("recovered revision = %d, want 3", got)
			}
			errCh <- callErr
		}()
	}
	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("concurrent follow-up recovery: %v", err)
		}
	}
	var evaluationInvocation kernel.InvocationID
	var evaluationInputRef string
	if err := db.SQL().QueryRowContext(ctx, `SELECT invocation_id, input_ref FROM production_manager_inputs
WHERE project_id=$1 AND target_kind='phase_evaluation' AND target_ref=$2`, "project-real", "output-execute-1").Scan(&evaluationInvocation, &evaluationInputRef); err != nil {
		t.Fatal(err)
	}
	if evaluationInputRef == outputInput.InputRef || evaluationInvocation == outputInput.InvocationID {
		t.Fatal("phase evaluation reused the submitted decision input or invocation")
	}
	assertProductionUIEvents(t, ctx, events, operatorPrincipal, "project-real", map[string]int{
		"graph.revision":     2,
		"endpoint.updated":   4,
		"invocation.updated": 1,
	})
	evaluationPrincipal := productionTestTaskManagerPrincipal(evaluationInvocation)
	evaluationScope := auth.BoundScope{ProjectID: "project-real", InvocationID: evaluationInvocation}
	if _, err := restartedAgain.SubmitTaskManagerDecision(ctx, evaluationPrincipal, evaluationScope, taskmanager.TaskManagerDecision{Action: "satisfied", TargetRef: "task-trusted/execute", Reason: "output meets the frozen contract"}); err != nil {
		t.Fatal(err)
	}
	if revision, err = restartedAgain.Transition(ctx, evaluationPrincipal, evaluationScope); err != nil || revision != 4 {
		t.Fatalf("satisfied transition revision=%d error=%v", revision, err)
	}
	afterSatisfied, _ := graph.Latest(ctx, "project-real")
	if got := productionEndpoint(afterSatisfied, executeRef); got.State != coordination.EndpointSatisfied {
		t.Fatalf("satisfied endpoint = %#v", got)
	}
	if len(afterSatisfied.Results) != 1 || afterSatisfied.Results[0].OutputRef != "output-execute-1" || afterSatisfied.Results[0].BindingRef != execute.BindingRef {
		t.Fatalf("trusted phase results = %#v", afterSatisfied.Results)
	}

	// Held -> stopped installs a new Runtime binding, then release is decided
	// from the separate stop_release input only.
	planRef := coordination.PhaseEndpointRef{TaskID: "task-trusted", EndpointID: coordination.EndpointPlan}
	holdResponse, err := ingress.SubmitManagerMessage(ctx, operator, httpapi.ManagerMessageRequest{
		RequestID: "hold-plan", ProjectID: "project-real", ConversationID: "conversation-trusted", Body: "hold plan before stop",
		Intent: httpapi.ManagerIntentHold, SelectedEndpoint: &planRef, ObservedGraphRevision: revisionPointer(4),
	})
	if err != nil {
		t.Fatal(err)
	}
	holdPrincipal := productionTestTaskManagerPrincipal(holdResponse.InvocationRef)
	holdScope := auth.BoundScope{ProjectID: "project-real", InvocationID: holdResponse.InvocationRef}
	if _, err := restartedAgain.SubmitTaskManagerDecision(ctx, holdPrincipal, holdScope, taskmanager.TaskManagerDecision{Action: "held", TargetRef: "task-trusted/plan", Reason: "stop safely"}); err != nil {
		t.Fatal(err)
	}
	if revision, err = restartedAgain.Transition(ctx, holdPrincipal, holdScope); err != nil || revision != 5 {
		t.Fatalf("held transition revision=%d error=%v", revision, err)
	}
	held := productionEndpoint(mustProductionSnapshot(t, ctx, graph, 5), planRef)
	stoppedBoundary := productionPhaseStoppedBoundary{
		CommandID: "stop-plan-1", Endpoint: planRef, Generation: held.Generation, BindingRef: held.BindingRef,
		LeaseRef: "lease-plan-1", CheckpointRef: "checkpoint-plan-1",
	}
	stoppedPayload, _ := json.Marshal(stoppedBoundary)
	stoppedInput, err := ingress.persistAndDispatch(ctx, productionInput{
		Kind: "phase_stopped", RequestID: "phase-stopped-plan-1", ConversationID: "runtime:task-trusted",
		Body: "phase stopped task-trusted/plan", Payload: stoppedPayload, SeenRevision: 5,
		SelectedEndpoint: &planRef, TargetKind: "phase_stopped", TargetRef: "stop-plan-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	stoppedPrincipal := productionTestTaskManagerPrincipal(stoppedInput.InvocationID)
	stoppedScope := auth.BoundScope{ProjectID: "project-real", InvocationID: stoppedInput.InvocationID}
	if _, err := restartedAgain.SubmitTaskManagerDecision(ctx, stoppedPrincipal, stoppedScope, taskmanager.TaskManagerDecision{Action: "stopped", TargetRef: "task-trusted/plan", Reason: "checkpoint persisted"}); err != nil {
		t.Fatal(err)
	}
	if revision, err = restartedAgain.Transition(ctx, stoppedPrincipal, stoppedScope); err != nil || revision != 6 {
		t.Fatalf("stopped transition revision=%d error=%v", revision, err)
	}
	afterStopped := mustProductionSnapshot(t, ctx, graph, 6)
	stoppedEndpoint := productionEndpoint(afterStopped, planRef)
	if stoppedEndpoint.Generation != 2 || stoppedEndpoint.BindingRef != canonicalProductionBindingRef(planRef, 2) || stoppedEndpoint.RunPolicy != coordination.RunHeld {
		t.Fatalf("stopped endpoint = %#v", stoppedEndpoint)
	}
	var releaseInvocation kernel.InvocationID
	if err := db.SQL().QueryRowContext(ctx, `SELECT invocation_id FROM production_manager_inputs
WHERE project_id=$1 AND target_kind='stop_release' AND target_ref=$2`, "project-real", "stop-plan-1").Scan(&releaseInvocation); err != nil {
		t.Fatal(err)
	}
	releasePrincipal := productionTestTaskManagerPrincipal(releaseInvocation)
	releaseScope := auth.BoundScope{ProjectID: "project-real", InvocationID: releaseInvocation}
	if _, err := restartedAgain.SubmitTaskManagerDecision(ctx, releasePrincipal, releaseScope, taskmanager.TaskManagerDecision{Action: "released", TargetRef: "task-trusted/plan", Reason: "runtime stop is complete"}); err != nil {
		t.Fatal(err)
	}
	if revision, err = restartedAgain.Transition(ctx, releasePrincipal, releaseScope); err != nil || revision != 7 {
		t.Fatalf("released transition revision=%d error=%v", revision, err)
	}
	if got := productionEndpoint(mustProductionSnapshot(t, ctx, graph, 7), planRef); got.RunPolicy != coordination.RunEnabled || got.Generation != 2 {
		t.Fatalf("released endpoint = %#v", got)
	}

	// A verified non-code delivery creates a Runtime-authenticated task_completion
	// input. Only that separate Task Manager invocation may mark the Task done;
	// completion then freezes task memory and dispatches a real Context review.
	verifyRef := coordination.PhaseEndpointRef{TaskID: "task-trusted", EndpointID: coordination.EndpointVerify}
	verify := productionEndpoint(mustProductionSnapshot(t, ctx, graph, 7), verifyRef)
	verifyOutput := productionPhaseOutputBoundary{OutputRef: "output-verify-1", Receipt: phasepkg.OutputReceipt{
		InvocationID: "phase-invocation-verify-1", Endpoint: verifyRef, Generation: verify.Generation,
		BindingRef: verify.BindingRef, LeaseRef: "lease-verify-1", InputRevision: "inputs-verify-1",
		Output: phasepkg.PhaseOutput{
			ReportRef: "report-verify-1", EvidenceRefs: []string{"evidence-verify-1"},
			DeliveryRefs: []string{"artifact-verify-1"},
		},
	}}
	verifyPayload, _ := json.Marshal(verifyOutput)
	verifyInput, err := ingress.persistAndDispatch(ctx, productionInput{
		Kind: "phase_output", RequestID: "phase-output-verify-1", ConversationID: "runtime:task-trusted",
		Body: "phase output verify output-verify-1", Payload: verifyPayload, SeenRevision: 7,
		SelectedEndpoint: &verifyRef, TargetKind: "phase_output", TargetRef: "output-verify-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	verifyPrincipal := productionTestTaskManagerPrincipal(verifyInput.InvocationID)
	verifyScope := auth.BoundScope{ProjectID: "project-real", InvocationID: verifyInput.InvocationID}
	if _, err := restartedAgain.SubmitTaskManagerDecision(ctx, verifyPrincipal, verifyScope, taskmanager.TaskManagerDecision{Action: "submitted", TargetRef: "task-trusted/verify", Reason: "persist verified output"}); err != nil {
		t.Fatal(err)
	}
	if revision, err = restartedAgain.Transition(ctx, verifyPrincipal, verifyScope); err != nil || revision != 8 {
		t.Fatalf("verify submitted revision=%d error=%v", revision, err)
	}
	var verifyEvaluationInvocation kernel.InvocationID
	if err := db.SQL().QueryRowContext(ctx, `SELECT invocation_id FROM production_manager_inputs
WHERE project_id=$1 AND target_kind='phase_evaluation' AND target_ref=$2`, "project-real", "output-verify-1").Scan(&verifyEvaluationInvocation); err != nil {
		t.Fatal(err)
	}
	verifyEvaluationPrincipal := productionTestTaskManagerPrincipal(verifyEvaluationInvocation)
	verifyEvaluationScope := auth.BoundScope{ProjectID: "project-real", InvocationID: verifyEvaluationInvocation}
	if _, err := restartedAgain.SubmitTaskManagerDecision(ctx, verifyEvaluationPrincipal, verifyEvaluationScope, taskmanager.TaskManagerDecision{Action: "satisfied", TargetRef: "task-trusted/verify", Reason: "delivery artifact satisfies the contract"}); err != nil {
		t.Fatal(err)
	}
	if revision, err = restartedAgain.Transition(ctx, verifyEvaluationPrincipal, verifyEvaluationScope); err != nil || revision != 9 {
		t.Fatalf("verify satisfied revision=%d error=%v", revision, err)
	}
	var completionInvocation kernel.InvocationID
	if err := db.SQL().QueryRowContext(ctx, `SELECT invocation_id FROM production_manager_inputs
WHERE project_id=$1 AND target_kind='task_completion' AND target_ref=$2`, "project-real", "task-trusted").Scan(&completionInvocation); err != nil {
		t.Fatal(err)
	}
	completionPrincipal := productionTestTaskManagerPrincipal(completionInvocation)
	completionScope := auth.BoundScope{ProjectID: "project-real", InvocationID: completionInvocation}
	if _, err := restartedAgain.SubmitTaskManagerDecision(ctx, completionPrincipal, completionScope, taskmanager.TaskManagerDecision{Action: "done", TargetRef: "task-trusted", Reason: "trusted non-code delivery completed"}); err != nil {
		t.Fatal(err)
	}
	if revision, err = restartedAgain.Transition(ctx, completionPrincipal, completionScope); err != nil || revision != 10 {
		t.Fatalf("task done revision=%d error=%v", revision, err)
	}
	completed := mustProductionSnapshot(t, ctx, graph, 10)
	if len(completed.Tasks) != 1 || completed.Tasks[0].Outcome != coordination.TaskDone {
		t.Fatalf("completed task snapshot = %#v", completed.Tasks)
	}
	var memoryState, reviewOperation string
	if err := db.SQL().QueryRowContext(ctx, `SELECT state FROM context_task_memory_reviews WHERE project_id=$1 AND task_id=$2`, "project-real", "task-trusted").Scan(&memoryState); err != nil {
		t.Fatal(err)
	}
	if err := db.SQL().QueryRowContext(ctx, `SELECT operation FROM production_context_invocations WHERE project_id=$1 AND task_id=$2`, "project-real", "task-trusted").Scan(&reviewOperation); err != nil {
		t.Fatal(err)
	}
	if memoryState != string(contextgraph.TaskMemoryFrozenUnreviewed) || reviewOperation != "review" {
		t.Fatalf("memory state=%q context operation=%q, want frozen-unreviewed/review", memoryState, reviewOperation)
	}
	var semanticDecisions int
	if err := db.SQL().QueryRowContext(ctx, `SELECT COUNT(DISTINCT input_ref) FROM taskmanager_decisions
WHERE project_id=$1 AND decision->>'action' IN ('submitted','satisfied')`, "project-real").Scan(&semanticDecisions); err != nil {
		t.Fatal(err)
	}
	if semanticDecisions != 4 {
		t.Fatalf("submitted/satisfied decision input count = %d, want 4 across execute and verify", semanticDecisions)
	}
}

func TestProductionReplacePendingUsesBoundInvocationForRealContextProjectionAgainstPostgres(t *testing.T) {
	databaseURL := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("THREADMILL_TEST_DATABASE_URL is required for the real PostgreSQL integration test")
	}
	ctx := context.Background()
	admin, err := postgres.Open(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	defer admin.Close(context.Background())
	schema := fmt.Sprintf("tm_production_context_%d", time.Now().UnixNano())
	if _, err := admin.SQL().ExecContext(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.SQL().ExecContext(context.Background(), `DROP SCHEMA IF EXISTS `+schema+` CASCADE`)
	})
	scopedURL, err := productionTestDatabaseURLWithSearchPath(databaseURL, schema)
	if err != nil {
		t.Fatal(err)
	}
	db, err := postgres.Open(ctx, scopedURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(context.Background())
	loaded, err := postgres.LoadMigrations(migrations.FS, migrations.Dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := postgres.NewMigrator(db.SQL()).Apply(ctx, loaded); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	available := make(map[auth.Tool]struct{})
	for _, tool := range auth.CanonicalTools() {
		available[tool] = struct{}{}
	}
	catalog, err := promptcatalog.Load(filepath.Join("..", ".."), available)
	if err != nil {
		t.Fatal(err)
	}
	assembler, err := runtimepkg.NewAssembler(catalog)
	if err != nil {
		t.Fatal(err)
	}
	graph := coordination.NewPostgresStore(db.SQL())
	eventStore := evidence.NewPostgresEventStore(db.SQL(), 1<<20)
	now := time.Date(2026, time.August, 11, 18, 0, 0, 0, time.UTC)
	ingress, err := newProductionIngress(db.SQL(), "project-real", "room-real", assembler, graph, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if err := ingress.setDispatcher(&recordingProductionDispatcher{}); err != nil {
		t.Fatal(err)
	}
	contexts := contextgraph.NewPostgresStore(db.SQL(), func() time.Time { return now.Add(2 * time.Minute) })
	contexts.SetTaskEndpointResolver(productionTaskEndpointResolver{projectID: "project-real", graph: graph})
	realContextProjector := &productionPhaseBindingSource{
		db: db.SQL(), projectID: "project-real", graph: graph, contracts: taskmanager.NewPostgresStore(db.SQL(), "project-real", graph),
		contexts: contexts, now: func() time.Time { return now.Add(2 * time.Minute) },
	}
	managerRuntime, err := newProductionTaskManagerRuntime(db.SQL(), "project-real", graph, func() time.Time { return now.Add(time.Minute) })
	if err != nil {
		t.Fatal(err)
	}
	if err := managerRuntime.setProductionEventStore(eventStore); err != nil {
		t.Fatal(err)
	}
	if err := managerRuntime.setProductionDependencies(newRecordingTaskResources(), realContextProjector, ingress); err != nil {
		t.Fatal(err)
	}
	operator := auth.Principal{ActorPrincipalID: "operator-real", Kind: auth.PrincipalOperator, ProjectID: "project-real", Role: auth.RoleOperator}
	requirement, err := ingress.SubmitRequirement(ctx, operator, httpapi.RequirementCreateRequest{
		RequestID: "requirement-context", ProjectID: "project-real", ConversationID: "conversation-context",
		Body: "create a task with real context projection", Motivation: "exercise trusted invocation propagation",
		Constraints: []string{"project task context"}, Acceptance: []string{"projection is persisted"},
	})
	if err != nil {
		t.Fatal(err)
	}
	managerPrincipal := productionTestTaskManagerPrincipal(requirement.InvocationRef)
	managerScope := auth.BoundScope{ProjectID: "project-real", InvocationID: requirement.InvocationRef}
	if _, err := managerRuntime.SubmitTaskManagerDecision(ctx, managerPrincipal, managerScope, taskmanager.TaskManagerDecision{Action: "replace_pending", Reason: "create context-backed task"}); err != nil {
		t.Fatal(err)
	}
	intent := mcpapi.PendingSubgraphIntent{
		TaskPolicies: []mcpapi.PendingTaskPolicyIntent{
			{TaskID: "task-context", DeliveryPolicy: taskmanager.DeliveryPolicyNonCodeArtifact},
		},
		Endpoints: []mcpapi.PendingEndpointIntent{
			productionPendingEndpoint("task-context", coordination.EndpointPlan),
			productionPendingEndpoint("task-context", coordination.EndpointExecute),
			productionPendingEndpoint("task-context", coordination.EndpointVerify),
		},
	}
	revision, err := managerRuntime.ReplacePending(ctx, managerPrincipal, managerScope, intent)
	if err != nil {
		t.Fatalf("ReplacePending() with real context projection error = %v", err)
	}
	if revision != 2 {
		t.Fatalf("ReplacePending() revision = %d, want 2", revision)
	}
	var mutationApplied bool
	var appliedRevision kernel.Revision
	if err := db.SQL().QueryRowContext(context.Background(), `SELECT mutation_applied, applied_graph_revision FROM production_taskmanager_bindings WHERE invocation_id=$1`, requirement.InvocationRef).Scan(&mutationApplied, &appliedRevision); err != nil {
		t.Fatal(err)
	}
	if !mutationApplied || appliedRevision != 2 {
		t.Fatalf("binding mutation_applied=%v applied_revision=%d, want true/2", mutationApplied, appliedRevision)
	}
	var status string
	if err := db.SQL().QueryRowContext(context.Background(), `SELECT status FROM runtime_invocations WHERE invocation_id=$1`, requirement.InvocationRef).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(runtimepkg.InvocationCompleted) {
		t.Fatalf("runtime status = %q, want completed", status)
	}
	var projectionRows, taskSubgraphs, oversizedOrSerialized int
	if err := db.SQL().QueryRowContext(context.Background(), `
SELECT count(*), count(*) FILTER (WHERE length(n.statement) > 256 OR left(ltrim(n.statement), 1) IN ('{', '['))
FROM context_task_projections p
JOIN context_nodes n ON n.project_id=p.project_id AND n.id=p.node_id
WHERE p.project_id=$1`, "project-real").Scan(&projectionRows, &oversizedOrSerialized); err != nil {
		t.Fatal(err)
	}
	if err := db.SQL().QueryRowContext(context.Background(), `SELECT count(*) FROM context_task_subgraph_bindings WHERE project_id=$1 AND task_id=$2`, "project-real", "task-context").Scan(&taskSubgraphs); err != nil {
		t.Fatal(err)
	}
	if projectionRows != 5 || taskSubgraphs != 1 || oversizedOrSerialized != 0 {
		t.Fatalf("context projection rows=%d task subgraphs=%d oversized_or_serialized=%d, want 5 semantic units/1/0", projectionRows, taskSubgraphs, oversizedOrSerialized)
	}
	events := uiprojection.NewEventLogQuery(eventStore, allowProjectPermission{projectID: "project-real"})
	assertProductionUIEvents(t, context.Background(), events, operator, "project-real", map[string]int{
		"manager.interaction": 1,
		"endpoint.updated":    3,
		"graph.revision":      1,
	})
}

func TestProductionTaskManagerExecutionCleanupReleasesCompletedSlotsAgainstPostgres(t *testing.T) {
	databaseURL := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("THREADMILL_TEST_DATABASE_URL is required for the real PostgreSQL integration test")
	}
	ctx := context.Background()
	admin, err := postgres.Open(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	defer admin.Close(context.Background())
	schema := fmt.Sprintf("tm_production_cleanup_%d", time.Now().UnixNano())
	if _, err := admin.SQL().ExecContext(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.SQL().ExecContext(context.Background(), `DROP SCHEMA IF EXISTS `+schema+` CASCADE`)
	})
	scopedURL, err := productionTestDatabaseURLWithSearchPath(databaseURL, schema)
	if err != nil {
		t.Fatal(err)
	}
	db, err := postgres.Open(ctx, scopedURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(context.Background())
	loaded, err := postgres.LoadMigrations(migrations.FS, migrations.Dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := postgres.NewMigrator(db.SQL()).Apply(ctx, loaded); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	now := time.Date(2026, time.August, 11, 17, 0, 0, 0, time.UTC)
	invocations := runtimepkg.NewPostgresInvocationStoreFromSQL(db.SQL())
	completed := productionCleanupInvocation("tm-cleanup-completed", runtimepkg.InvocationCompleted, now)
	completedAbandoned := productionCleanupInvocation("tm-cleanup-completed-abandoned", runtimepkg.InvocationCompleted, now)
	completedReleased := productionCleanupInvocation("tm-cleanup-completed-released", runtimepkg.InvocationCompleted, now)
	terminatedUnreleased := productionCleanupInvocation("tm-cleanup-terminated-unreleased", runtimepkg.InvocationCompleted, now)
	expired := productionCleanupInvocation("tm-cleanup-expired", runtimepkg.InvocationRunning, now.Add(-time.Hour))
	expiredUnclaimed := productionCleanupInvocation("tm-cleanup-expired-unclaimed", runtimepkg.InvocationPrepared, now.Add(-time.Hour))
	revokedUnclaimed := productionCleanupInvocation("tm-cleanup-revoked-unclaimed", runtimepkg.InvocationPrepared, now)
	expiredNoExecution := productionCleanupInvocation("tm-cleanup-expired-no-execution", runtimepkg.InvocationPrepared, now.Add(-time.Hour))
	expiredOtherProject := productionCleanupInvocation("tm-cleanup-expired-other-project", runtimepkg.InvocationRunning, now.Add(-time.Hour))
	expiredOtherProject.ProjectID = "project-other"
	expiredOtherProject.ActorPrincipalID = "task-manager:project-other"
	running := productionCleanupInvocation("tm-cleanup-running", runtimepkg.InvocationRunning, now)
	providerTerminal := productionCleanupInvocation("tm-cleanup-provider-terminal", runtimepkg.InvocationRunning, now)
	providerQuiescent := productionCleanupInvocation("tm-cleanup-provider-quiescent", runtimepkg.InvocationRunning, now)
	providerAlreadyTerminated := productionCleanupInvocation("tm-cleanup-provider-already-terminated", runtimepkg.InvocationPrepared, now)
	if err := invocations.Create(ctx, completed); err != nil {
		t.Fatal(err)
	}
	if err := invocations.Create(ctx, completedAbandoned); err != nil {
		t.Fatal(err)
	}
	if err := invocations.Create(ctx, completedReleased); err != nil {
		t.Fatal(err)
	}
	if err := invocations.Create(ctx, terminatedUnreleased); err != nil {
		t.Fatal(err)
	}
	if err := invocations.Create(ctx, expired); err != nil {
		t.Fatal(err)
	}
	if err := invocations.Create(ctx, expiredUnclaimed); err != nil {
		t.Fatal(err)
	}
	if err := invocations.Create(ctx, revokedUnclaimed); err != nil {
		t.Fatal(err)
	}
	if err := invocations.Create(ctx, expiredNoExecution); err != nil {
		t.Fatal(err)
	}
	if err := invocations.Create(ctx, expiredOtherProject); err != nil {
		t.Fatal(err)
	}
	if err := invocations.Create(ctx, running); err != nil {
		t.Fatal(err)
	}
	if err := invocations.Create(ctx, providerTerminal); err != nil {
		t.Fatal(err)
	}
	if err := invocations.Create(ctx, providerQuiescent); err != nil {
		t.Fatal(err)
	}
	if err := invocations.Create(ctx, providerAlreadyTerminated); err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct {
		invocation kernel.InvocationID
		task       string
		host       string
		state      string
		mode       any
		claimed    any
		released   any
		revoked    any
	}{
		{completed.ID, "agentteams-cleanup-completed", "default", "dispatched", nil, now, nil, nil},
		{completedAbandoned.ID, "agentteams-cleanup-completed-abandoned", "completed-abandoned-host", "dispatched", nil, now, nil, nil},
		{completedReleased.ID, "agentteams-cleanup-completed-released", "default", "dispatched", nil, now, now.Add(time.Minute), now.Add(time.Minute)},
		{terminatedUnreleased.ID, "agentteams-cleanup-terminated-unreleased", "default", "terminated", string(agentteams.TerminateReleaseWait), now, nil, now.Add(time.Minute)},
		{expired.ID, "agentteams-cleanup-expired", "expired-host", "dispatched", nil, now, nil, nil},
		{expiredUnclaimed.ID, "agentteams-cleanup-expired-unclaimed", "default", "reserved", nil, nil, nil, nil},
		{revokedUnclaimed.ID, "agentteams-cleanup-revoked-unclaimed", "default", "reserved", nil, nil, nil, nil},
		{expiredOtherProject.ID, "agentteams-cleanup-expired-other-project", "expired-other-host", "dispatched", nil, now, nil, nil},
		{running.ID, "agentteams-cleanup-running", "other", "dispatched", nil, now, nil, nil},
		{providerTerminal.ID, "agentteams-cleanup-provider-terminal", "provider-terminal-host", "dispatched", nil, now, nil, nil},
		{providerQuiescent.ID, "agentteams-cleanup-provider-quiescent", "provider-quiescent-host", "dispatched", nil, now, nil, nil},
		{providerAlreadyTerminated.ID, "agentteams-cleanup-provider-already-terminated", "provider-ended-host", "terminated", string(agentteams.TerminateCancel), now, now.Add(time.Minute), now.Add(time.Minute)},
	} {
		if _, err := db.SQL().ExecContext(ctx, `
INSERT INTO agentteams_execution_refs (
  invocation_ref, attempt, invocation_id, agentteams_task_id, host_ref, dispatch_fingerprint,
  state, termination_mode, host_slot_claimed_at, host_slot_released_at, mcp_revoked_at,
  mcp_client_key, mcp_token_hash, mcp_token_identifier, created_at, updated_at
) VALUES ($1, 1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $14)`,
			"manager-input:"+string(row.invocation), row.invocation, row.task, row.host, "fingerprint-"+row.task, row.state, row.mode, row.claimed, row.released, row.revoked, "mcp-"+row.task, []byte("hash-"+row.task), "token-"+row.task, now); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.SQL().ExecContext(ctx, `
INSERT INTO production_manager_inputs (
  project_id, input_ref, request_id, input_kind, conversation_id, payload, payload_hash,
  observed_graph_revision, invocation_id, status, created_at, updated_at
) VALUES ($1, $2, $3, 'manager', $4, '{}'::jsonb, $5, 1, $6, 'dispatched', $7, $7)`,
		"project-real", "manager-input:expired", "request-expired", "conversation-expired", "payload-expired", expired.ID, expired.CreatedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx, `
INSERT INTO production_manager_inputs (
  project_id, input_ref, request_id, input_kind, conversation_id, payload, payload_hash,
  observed_graph_revision, invocation_id, status, created_at, updated_at
) VALUES ($1, $2, $3, 'manager', $4, '{}'::jsonb, $5, 1, $6, 'pending', $7, $7)`,
		"project-real", "manager-input:expired-unclaimed", "request-expired-unclaimed", "conversation-expired-unclaimed", "payload-expired-unclaimed", expiredUnclaimed.ID, expiredUnclaimed.CreatedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx, `
INSERT INTO production_manager_inputs (
  project_id, input_ref, request_id, input_kind, conversation_id, payload, payload_hash,
  observed_graph_revision, invocation_id, status, created_at, updated_at
) VALUES ($1, $2, $3, 'manager', $4, '{}'::jsonb, $5, 1, $6, 'pending', $7, $7)`,
		"project-real", "manager-input:revoked-unclaimed", "request-revoked-unclaimed", "conversation-revoked-unclaimed", "payload-revoked-unclaimed", revokedUnclaimed.ID, revokedUnclaimed.CreatedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx, `
INSERT INTO agent_invocation_tokens (
  token_hash, actor_principal_id, project_id, invocation_id, role, tools,
  expires_at, revoked_at, created_at
) VALUES ($1, $2, $3, $4, 'task_manager', '[]'::jsonb, $5, $6, $6)`,
		[]byte("revoked-unclaimed-token"), revokedUnclaimed.ActorPrincipalID, revokedUnclaimed.ProjectID,
		revokedUnclaimed.ID, now.Add(time.Hour), now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx, `
INSERT INTO production_manager_inputs (
  project_id, input_ref, request_id, input_kind, conversation_id, payload, payload_hash,
  observed_graph_revision, invocation_id, status, created_at, updated_at
) VALUES ($1, $2, $3, 'manager', $4, '{}'::jsonb, $5, 1, $6, 'pending', $7, $7)`,
		"project-real", "manager-input:expired-no-execution", "request-expired-no-execution", "conversation-expired-no-execution", "payload-expired-no-execution", expiredNoExecution.ID, expiredNoExecution.CreatedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx, `
INSERT INTO production_manager_inputs (
  project_id, input_ref, request_id, input_kind, conversation_id, payload, payload_hash,
  observed_graph_revision, invocation_id, status, created_at, updated_at
) VALUES ($1, $2, $3, 'manager', $4, '{}'::jsonb, $5, 1, $6, 'dispatched', $7, $7)`,
		"project-other", "manager-input:expired-other", "request-expired-other", "conversation-expired-other", "payload-expired-other", expiredOtherProject.ID, expiredOtherProject.CreatedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx, `
INSERT INTO production_manager_inputs (
  project_id, input_ref, request_id, input_kind, conversation_id, payload, payload_hash,
  observed_graph_revision, invocation_id, status, created_at, updated_at
) VALUES ($1, $2, $3, 'manager', $4, '{}'::jsonb, $5, 1, $6, 'dispatched', $7, $7)`,
		"project-real", "manager-input:provider-terminal", "request-provider-terminal", "conversation-provider-terminal", "payload-provider-terminal", providerTerminal.ID, providerTerminal.CreatedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx, `
INSERT INTO production_manager_inputs (
  project_id, input_ref, request_id, input_kind, conversation_id, payload, payload_hash,
  observed_graph_revision, invocation_id, status, created_at, updated_at
) VALUES ($1, $2, $3, 'manager', $4, '{}'::jsonb, $5, 1, $6, 'dispatched', $7, $7)`,
		"project-real", "manager-input:provider-quiescent", "request-provider-quiescent", "conversation-provider-quiescent", "payload-provider-quiescent", providerQuiescent.ID, providerQuiescent.CreatedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx, `
INSERT INTO production_manager_inputs (
  project_id, input_ref, request_id, input_kind, conversation_id, payload, payload_hash,
  observed_graph_revision, invocation_id, status, created_at, updated_at
) VALUES ($1, $2, $3, 'manager', $4, '{}'::jsonb, $5, 1, $6, 'dispatched', $7, $7)`,
		"project-real", "manager-input:provider-already-terminated", "request-provider-already-terminated", "conversation-provider-already-terminated", "payload-provider-already-terminated", providerAlreadyTerminated.ID, providerAlreadyTerminated.CreatedAt); err != nil {
		t.Fatal(err)
	}
	for _, invocationID := range []kernel.InvocationID{expired.ID, expiredUnclaimed.ID, revokedUnclaimed.ID, expiredNoExecution.ID, expiredOtherProject.ID, providerTerminal.ID, providerQuiescent.ID, providerAlreadyTerminated.ID} {
		if _, err := db.SQL().ExecContext(ctx, `
INSERT INTO production_taskmanager_bindings(
  project_id, invocation_id, input_ref, room_id, spec, runtime_config_ref, envelope_ref, required_capabilities
)
SELECT project_id, invocation_id, input_ref, 'room-real', 'retryable task manager spec',
       'runtime-config:' || invocation_id, 'runtime-envelope:' || invocation_id, '[]'::jsonb
FROM production_manager_inputs
WHERE invocation_id=$1`, invocationID); err != nil {
			t.Fatal(err)
		}
	}
	for _, invocationID := range []kernel.InvocationID{completed.ID, expired.ID} {
		if _, err := db.SQL().ExecContext(ctx, `
INSERT INTO context_subscriptions (
  id, project_id, consumer_invocation_id, subgraph_ids, event_kinds,
  permission_snapshot, source, active, created_at
) VALUES ($1, 'project-real', $2, '[]'::jsonb, '[]'::jsonb, 'permission', 'explicit', TRUE, $3)`,
			"subscription:"+string(invocationID), invocationID, now); err != nil {
			t.Fatal(err)
		}
	}
	available := make(map[auth.Tool]struct{})
	for _, tool := range auth.CanonicalTools() {
		available[tool] = struct{}{}
	}
	catalog, err := promptcatalog.Load(filepath.Join("..", ".."), available)
	if err != nil {
		t.Fatal(err)
	}
	assembler, err := runtimepkg.NewAssembler(catalog)
	if err != nil {
		t.Fatal(err)
	}
	terminator := &recordingCleanupTerminator{
		db:       db.SQL(),
		terminal: map[kernel.InvocationID]bool{completedReleased.ID: true, providerTerminal.ID: true},
		terminalErr: map[kernel.InvocationID]error{
			completedAbandoned.ID: errors.New("provider task readback rejected"),
		},
		activities: map[kernel.InvocationID]agentteams.HostActivity{
			completedAbandoned.ID: {Status: "idle", LastFinishAt: now.Add(time.Minute)},
			providerQuiescent.ID:  {Status: "running", RunningTaskCount: 0, LastRunAt: now.Add(10 * time.Second), LastFinishAt: now.Add(time.Minute)},
		},
	}
	cleaner, err := newProductionTaskManagerExecutionCleanup(
		db.SQL(),
		terminator,
		contextgraph.NewPostgresStore(db.SQL(), func() time.Time { return now.Add(2 * time.Minute) }),
		func() time.Time { return now.Add(2 * time.Minute) },
	)
	if err != nil {
		t.Fatal(err)
	}
	graph := coordination.NewPostgresStore(db.SQL())
	ingress, err := newProductionIngress(db.SQL(), "project-real", "room-real", assembler, graph, func() time.Time { return now.Add(2 * time.Minute) })
	if err != nil {
		t.Fatal(err)
	}
	dispatcher := &recordingProductionDispatcher{}
	if err := ingress.setDispatcher(dispatcher); err != nil {
		t.Fatal(err)
	}
	if err := ingress.setTaskManagerExecutionCleaner(cleaner); err != nil {
		t.Fatal(err)
	}
	operator := auth.Principal{ActorPrincipalID: "operator-real", Kind: auth.PrincipalOperator, ProjectID: "project-real", Role: auth.RoleOperator}
	if _, err := ingress.SubmitManagerMessage(ctx, operator, httpapi.ManagerMessageRequest{RequestID: "cleanup-next-dispatch", ProjectID: "project-real", ConversationID: "conversation-cleanup", Body: "dispatch after cleanup"}); err != nil {
		t.Fatal(err)
	}
	if dispatcher.calls != 1 {
		t.Fatalf("dispatcher calls = %d, want 1 after cleanup", dispatcher.calls)
	}
	gotModes := make(map[kernel.InvocationID]string, len(terminator.executions))
	for index, execution := range terminator.executions {
		gotModes[execution.InvocationID] = terminator.modes[index]
	}
	if len(gotModes) != 10 ||
		gotModes[completedAbandoned.ID] != string(agentteams.TerminateCancel) ||
		gotModes[completedReleased.ID] != string(agentteams.TerminateCancel) ||
		gotModes[terminatedUnreleased.ID] != string(agentteams.TerminateReleaseWait) ||
		gotModes[expired.ID] != string(agentteams.TerminateCancel) ||
		gotModes[expiredUnclaimed.ID] != string(agentteams.TerminateCancel) ||
		gotModes[revokedUnclaimed.ID] != string(agentteams.TerminateCancel) ||
		gotModes[expiredOtherProject.ID] != string(agentteams.TerminateCancel) ||
		gotModes[providerTerminal.ID] != string(agentteams.TerminateCancel) ||
		gotModes[providerQuiescent.ID] != string(agentteams.TerminateCancel) ||
		gotModes[providerAlreadyTerminated.ID] != string(agentteams.TerminateCancel) {
		t.Fatalf("cleanup modes=%#v, want provider-terminal/expired cancel and terminated-unreleased release_wait", gotModes)
	}
	var released, revoked bool
	if err := db.SQL().QueryRowContext(ctx, `SELECT host_slot_released_at IS NOT NULL, mcp_revoked_at IS NOT NULL FROM agentteams_execution_refs WHERE invocation_id=$1`, completed.ID).Scan(&released, &revoked); err != nil {
		t.Fatal(err)
	}
	if released || revoked {
		t.Fatalf("completed non-terminal provider slot released=%v revoked=%v, want retained with transport credential intact", released, revoked)
	}
	if !terminator.fenced[completed.ID] {
		t.Fatal("completed non-terminal provider execution was not fenced")
	}
	if len(terminator.finalized) == 0 || terminator.finalized[0].InvocationID != completed.ID {
		t.Fatalf("provider finalizations = %#v, want completed invocation first", terminator.finalized)
	}
	terminator.terminal[completed.ID] = true
	if err := cleaner.CleanupTaskManagerInvocations(ctx); err != nil {
		t.Fatal(err)
	}
	if err := db.SQL().QueryRowContext(ctx, `SELECT host_slot_released_at IS NOT NULL, mcp_revoked_at IS NOT NULL FROM agentteams_execution_refs WHERE invocation_id=$1`, completed.ID).Scan(&released, &revoked); err != nil {
		t.Fatal(err)
	}
	if !released || !revoked {
		t.Fatalf("terminalized completed slot released=%v revoked=%v, want both true", released, revoked)
	}
	if err := db.SQL().QueryRowContext(ctx, `SELECT host_slot_released_at IS NOT NULL, mcp_revoked_at IS NOT NULL FROM agentteams_execution_refs WHERE invocation_id=$1`, completedReleased.ID).Scan(&released, &revoked); err != nil {
		t.Fatal(err)
	}
	if !released || !revoked {
		t.Fatalf("completed released legacy slot released=%v revoked=%v, want both true", released, revoked)
	}
	if err := db.SQL().QueryRowContext(ctx, `SELECT host_slot_released_at IS NOT NULL, mcp_revoked_at IS NOT NULL FROM agentteams_execution_refs WHERE invocation_id=$1`, terminatedUnreleased.ID).Scan(&released, &revoked); err != nil {
		t.Fatal(err)
	}
	if !released || !revoked {
		t.Fatalf("terminated-unreleased slot released=%v revoked=%v, want both true", released, revoked)
	}
	if err := db.SQL().QueryRowContext(ctx, `SELECT host_slot_released_at IS NOT NULL, mcp_revoked_at IS NOT NULL FROM agentteams_execution_refs WHERE invocation_id=$1`, running.ID).Scan(&released, &revoked); err != nil {
		t.Fatal(err)
	}
	if released || revoked {
		t.Fatalf("running slot released=%v revoked=%v, want untouched", released, revoked)
	}
	var invocationStatus, inputStatus string
	if err := db.SQL().QueryRowContext(ctx, `SELECT status FROM runtime_invocations WHERE invocation_id=$1`, expired.ID).Scan(&invocationStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.SQL().QueryRowContext(ctx, `SELECT status FROM production_manager_inputs WHERE invocation_id=$1`, expired.ID).Scan(&inputStatus); err != nil {
		t.Fatal(err)
	}
	if invocationStatus != string(runtimepkg.InvocationFailed) || inputStatus != "failed" {
		t.Fatalf("expired statuses invocation=%q input=%q, want failed/failed", invocationStatus, inputStatus)
	}
	if err := db.SQL().QueryRowContext(ctx, `SELECT status FROM runtime_invocations WHERE invocation_id=$1`, expiredUnclaimed.ID).Scan(&invocationStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.SQL().QueryRowContext(ctx, `SELECT status FROM production_manager_inputs WHERE invocation_id=$1`, expiredUnclaimed.ID).Scan(&inputStatus); err != nil {
		t.Fatal(err)
	}
	if invocationStatus != string(runtimepkg.InvocationFailed) || inputStatus != "failed" {
		t.Fatalf("expired unclaimed statuses invocation=%q input=%q, want failed/failed", invocationStatus, inputStatus)
	}
	if err := db.SQL().QueryRowContext(ctx, `SELECT status FROM runtime_invocations WHERE invocation_id=$1`, revokedUnclaimed.ID).Scan(&invocationStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.SQL().QueryRowContext(ctx, `SELECT status FROM production_manager_inputs WHERE invocation_id=$1`, revokedUnclaimed.ID).Scan(&inputStatus); err != nil {
		t.Fatal(err)
	}
	if invocationStatus != string(runtimepkg.InvocationFailed) || inputStatus != "failed" {
		t.Fatalf("revoked unclaimed statuses invocation=%q input=%q, want failed/failed", invocationStatus, inputStatus)
	}
	if err := db.SQL().QueryRowContext(ctx, `SELECT status FROM runtime_invocations WHERE invocation_id=$1`, providerTerminal.ID).Scan(&invocationStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.SQL().QueryRowContext(ctx, `SELECT status FROM production_manager_inputs WHERE invocation_id=$1`, providerTerminal.ID).Scan(&inputStatus); err != nil {
		t.Fatal(err)
	}
	if invocationStatus != string(runtimepkg.InvocationFailed) || inputStatus != "failed" {
		t.Fatalf("provider-terminal statuses invocation=%q input=%q, want failed/failed", invocationStatus, inputStatus)
	}
	if err := db.SQL().QueryRowContext(ctx, `SELECT status FROM runtime_invocations WHERE invocation_id=$1`, providerQuiescent.ID).Scan(&invocationStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.SQL().QueryRowContext(ctx, `SELECT status FROM production_manager_inputs WHERE invocation_id=$1`, providerQuiescent.ID).Scan(&inputStatus); err != nil {
		t.Fatal(err)
	}
	if invocationStatus != string(runtimepkg.InvocationFailed) || inputStatus != "failed" {
		t.Fatalf("provider-quiescent statuses invocation=%q input=%q, want failed/failed", invocationStatus, inputStatus)
	}
	if err := db.SQL().QueryRowContext(ctx, `SELECT status FROM runtime_invocations WHERE invocation_id=$1`, providerAlreadyTerminated.ID).Scan(&invocationStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.SQL().QueryRowContext(ctx, `SELECT status FROM production_manager_inputs WHERE invocation_id=$1`, providerAlreadyTerminated.ID).Scan(&inputStatus); err != nil {
		t.Fatal(err)
	}
	if invocationStatus != string(runtimepkg.InvocationFailed) || inputStatus != "failed" {
		t.Fatalf("provider-already-terminated statuses invocation=%q input=%q, want failed/failed", invocationStatus, inputStatus)
	}
	previousQuiescentInvocation := providerQuiescent.ID
	previousTerminatedInvocation := providerAlreadyTerminated.ID
	if err := ingress.RetryFailedTaskManagerInputs(ctx); err != nil {
		t.Fatal(err)
	}
	if dispatcher.calls != 8 {
		t.Fatalf("dispatcher calls after automatic retries = %d, want 8", dispatcher.calls)
	}
	var retriedRevokedInvocation kernel.InvocationID
	var retriedRevokedAttempt int
	if err := db.SQL().QueryRowContext(ctx, `SELECT invocation_id, status, dispatch_attempt FROM production_manager_inputs WHERE project_id='project-real' AND input_ref='manager-input:revoked-unclaimed'`).Scan(&retriedRevokedInvocation, &inputStatus, &retriedRevokedAttempt); err != nil {
		t.Fatal(err)
	}
	if retriedRevokedInvocation == revokedUnclaimed.ID || inputStatus != "dispatched" || retriedRevokedAttempt != 2 {
		t.Fatalf("revoked preparation retry invocation=%q status=%q attempt=%d", retriedRevokedInvocation, inputStatus, retriedRevokedAttempt)
	}
	var retriedQuiescentInvocation kernel.InvocationID
	var retriedAttempt int
	if err := db.SQL().QueryRowContext(ctx, `SELECT invocation_id, status, dispatch_attempt FROM production_manager_inputs WHERE project_id='project-real' AND input_ref='manager-input:provider-quiescent'`).Scan(&retriedQuiescentInvocation, &inputStatus, &retriedAttempt); err != nil {
		t.Fatal(err)
	}
	if retriedQuiescentInvocation == previousQuiescentInvocation || inputStatus != "dispatched" || retriedAttempt != 2 {
		t.Fatalf("quiescent retry invocation=%q status=%q attempt=%d", retriedQuiescentInvocation, inputStatus, retriedAttempt)
	}
	if err := db.SQL().QueryRowContext(ctx, `SELECT status FROM runtime_invocations WHERE invocation_id=$1`, retriedQuiescentInvocation).Scan(&invocationStatus); err != nil {
		t.Fatal(err)
	}
	if invocationStatus != string(runtimepkg.InvocationRunning) {
		t.Fatalf("retried invocation status=%q, want running", invocationStatus)
	}
	var retriedTerminatedInvocation kernel.InvocationID
	if err := db.SQL().QueryRowContext(ctx, `SELECT invocation_id, status, dispatch_attempt FROM production_manager_inputs WHERE project_id='project-real' AND input_ref='manager-input:provider-already-terminated'`).Scan(&retriedTerminatedInvocation, &inputStatus, &retriedAttempt); err != nil {
		t.Fatal(err)
	}
	if retriedTerminatedInvocation == previousTerminatedInvocation || inputStatus != "dispatched" || retriedAttempt != 2 {
		t.Fatalf("already-terminated retry invocation=%q status=%q attempt=%d", retriedTerminatedInvocation, inputStatus, retriedAttempt)
	}
	var retriedExpiredNoExecution kernel.InvocationID
	if err := db.SQL().QueryRowContext(ctx, `SELECT invocation_id, status, dispatch_attempt FROM production_manager_inputs WHERE project_id='project-real' AND input_ref='manager-input:expired-no-execution'`).Scan(&retriedExpiredNoExecution, &inputStatus, &retriedAttempt); err != nil {
		t.Fatal(err)
	}
	if retriedExpiredNoExecution == expiredNoExecution.ID || inputStatus != "dispatched" || retriedAttempt != 2 {
		t.Fatalf("expired no-execution retry invocation=%q status=%q attempt=%d", retriedExpiredNoExecution, inputStatus, retriedAttempt)
	}
	if err := db.SQL().QueryRowContext(ctx, `SELECT status FROM runtime_invocations WHERE invocation_id=$1`, expiredNoExecution.ID).Scan(&invocationStatus); err != nil {
		t.Fatal(err)
	}
	if invocationStatus != string(runtimepkg.InvocationFailed) {
		t.Fatalf("expired no-execution old invocation status=%q, want failed", invocationStatus)
	}
	if _, err := db.SQL().ExecContext(ctx, `UPDATE runtime_invocations SET status='failed' WHERE invocation_id=$1`, retriedQuiescentInvocation); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx, `UPDATE production_manager_inputs SET status='failed' WHERE project_id='project-real' AND input_ref='manager-input:provider-quiescent'`); err != nil {
		t.Fatal(err)
	}
	recoverer := &recordingPersistedDecisionRecoverer{handled: true}
	if err := ingress.setPersistedDecisionRecoverer(recoverer); err != nil {
		t.Fatal(err)
	}
	if err := ingress.RetryFailedTaskManagerInputs(ctx); err != nil {
		t.Fatal(err)
	}
	if recoverer.calls != 1 || recoverer.invocation != retriedQuiescentInvocation {
		t.Fatalf("persisted decision recovery calls=%d invocation=%q, want one call for %q", recoverer.calls, recoverer.invocation, retriedQuiescentInvocation)
	}
	if dispatcher.calls != 8 {
		t.Fatalf("dispatcher calls after persisted decision recovery = %d, want no model redispatch", dispatcher.calls)
	}
	recoverer.err = kernel.RevisionConflict(1, 2)
	if err := ingress.RetryFailedTaskManagerInputs(ctx); err != nil {
		t.Fatal(err)
	}
	if recoverer.calls != 2 {
		t.Fatalf("persisted decision recovery calls after revision conflict = %d, want 2", recoverer.calls)
	}
	if dispatcher.calls != 9 {
		t.Fatalf("dispatcher calls after revision rebase = %d, want one new bounded invocation", dispatcher.calls)
	}
	if err := db.SQL().QueryRowContext(ctx, `SELECT status FROM production_manager_inputs WHERE project_id='project-real' AND input_ref='manager-input:provider-quiescent'`).Scan(&inputStatus); err != nil {
		t.Fatal(err)
	}
	if inputStatus != "completed" {
		t.Fatalf("stale input status after revision rebase = %q, want completed", inputStatus)
	}
	var rebased int
	if err := db.SQL().QueryRowContext(ctx, `SELECT count(*) FROM production_manager_inputs
WHERE project_id='project-real' AND request_id LIKE '%' AND input_ref <> 'manager-input:provider-quiescent'
  AND conversation_id='conversation-provider-quiescent'`).Scan(&rebased); err != nil {
		t.Fatal(err)
	}
	if rebased != 1 {
		t.Fatalf("rebased inputs = %d, want 1", rebased)
	}
	// An immutable decision that is structurally incompatible with the trusted
	// target state is just as unreplayable as a stale-revision decision. Keep the
	// old decision/input as audit evidence and dispatch one fresh, rebased model
	// invocation so it can choose an applicable action from the current prompt.
	var revisionConflictRebasedRef string
	if err := db.SQL().QueryRowContext(ctx, `SELECT input_ref FROM production_manager_inputs
WHERE project_id='project-real' AND input_ref <> 'manager-input:provider-quiescent'
  AND conversation_id='conversation-provider-quiescent'`).Scan(&revisionConflictRebasedRef); err != nil {
		t.Fatal(err)
	}
	var rejectedInvocation kernel.InvocationID
	if err := db.SQL().QueryRowContext(ctx, `SELECT invocation_id FROM production_manager_inputs
WHERE project_id='project-real' AND input_ref=$1`, revisionConflictRebasedRef).Scan(&rejectedInvocation); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx, `UPDATE runtime_invocations SET status='failed' WHERE invocation_id=$1`, rejectedInvocation); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx, `UPDATE production_manager_inputs SET status='failed'
WHERE project_id='project-real' AND input_ref=$1`, revisionConflictRebasedRef); err != nil {
		t.Fatal(err)
	}
	recoverer.err = kernel.TransitionRejected("persisted decision is incompatible with the trusted target state")
	if err := ingress.RetryFailedTaskManagerInputs(ctx); err != nil {
		t.Fatal(err)
	}
	if dispatcher.calls != 10 {
		t.Fatalf("dispatcher calls after transition-rejected rebase = %d, want one fresh bounded invocation", dispatcher.calls)
	}
	if err := db.SQL().QueryRowContext(ctx, `SELECT status FROM production_manager_inputs
WHERE project_id='project-real' AND input_ref=$1`, revisionConflictRebasedRef).Scan(&inputStatus); err != nil {
		t.Fatal(err)
	}
	if inputStatus != "completed" {
		t.Fatalf("transition-rejected input status after rebase = %q, want completed", inputStatus)
	}
	if err := db.SQL().QueryRowContext(ctx, `SELECT count(*) FROM production_manager_inputs
WHERE project_id='project-real' AND conversation_id='conversation-provider-quiescent'
  AND input_ref NOT IN ('manager-input:provider-quiescent',$1)`, revisionConflictRebasedRef).Scan(&rebased); err != nil {
		t.Fatal(err)
	}
	if rebased != 1 {
		t.Fatalf("transition-rejected rebased inputs = %d, want exactly one new input", rebased)
	}
	for _, invocationID := range []kernel.InvocationID{completed.ID, expired.ID} {
		var active bool
		if err := db.SQL().QueryRowContext(ctx, `SELECT active FROM context_subscriptions WHERE consumer_invocation_id=$1`, invocationID).Scan(&active); err != nil {
			t.Fatal(err)
		}
		if active {
			t.Fatalf("subscription for %s remained active after execution cleanup", invocationID)
		}
	}
}

type recordingPersistedDecisionRecoverer struct {
	invocation kernel.InvocationID
	calls      int
	handled    bool
	err        error
}

func (r *recordingPersistedDecisionRecoverer) RecoverPersistedTaskManagerDecision(_ context.Context, invocationID kernel.InvocationID) (bool, error) {
	r.calls++
	r.invocation = invocationID
	return r.handled, r.err
}

var (
	errProductionContextProjection = errors.New("injected context projection failure")
	errProductionDispatch          = errors.New("injected AgentTeams dispatch failure")
)

type recordingTaskResources struct {
	mu              sync.Mutex
	workspaceCalls  map[string]int
	contextCalls    map[kernel.TaskID]int
	contexts        map[kernel.TaskID]productionTaskContextRequest
	contextDelegate productionTaskContextProjector
	failContextOnce bool
}

func newRecordingTaskResources() *recordingTaskResources {
	return &recordingTaskResources{
		workspaceCalls: make(map[string]int), contextCalls: make(map[kernel.TaskID]int),
		contexts: make(map[kernel.TaskID]productionTaskContextRequest),
	}
}

func (r *recordingTaskResources) EnsureTaskWorkspace(_ context.Context, req productionTaskWorkspaceRequest) (kernel.BindingRef, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := fmt.Sprintf("%s/%d", req.TaskID, req.Generation)
	r.workspaceCalls[key]++
	return kernel.BindingRef("workspace://" + key), nil
}

func (r *recordingTaskResources) EnsureTaskContext(ctx context.Context, req productionTaskContextRequest) error {
	r.mu.Lock()
	if r.failContextOnce {
		r.failContextOnce = false
		r.mu.Unlock()
		return errProductionContextProjection
	}
	r.contextCalls[req.TaskID]++
	r.contexts[req.TaskID] = req
	delegate := r.contextDelegate
	r.mu.Unlock()
	if delegate != nil {
		return delegate.EnsureTaskContext(ctx, req)
	}
	return nil
}

func (r *recordingTaskResources) workspaceCallCount(taskID kernel.TaskID, generation int) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.workspaceCalls[fmt.Sprintf("%s/%d", taskID, generation)]
}

func (r *recordingTaskResources) contextCallCount(taskID kernel.TaskID) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.contextCalls[taskID]
}

func productionPendingEndpoint(taskID kernel.TaskID, endpointID coordination.EndpointID) mcpapi.PendingEndpointIntent {
	return mcpapi.PendingEndpointIntent{
		Ref:       coordination.PhaseEndpointRef{TaskID: taskID, EndpointID: endpointID},
		RunPolicy: coordination.RunEnabled,
	}
}

func productionEndpoint(snapshot coordination.GraphSnapshot, ref coordination.PhaseEndpointRef) coordination.PhaseEndpoint {
	for _, endpoint := range snapshot.Endpoints {
		if endpoint.Ref == ref {
			return endpoint
		}
	}
	return coordination.PhaseEndpoint{}
}

func mustProductionSnapshot(t *testing.T, ctx context.Context, graph *coordination.PostgresStore, revision kernel.Revision) coordination.GraphSnapshot {
	t.Helper()
	snapshot, err := graph.Snapshot(ctx, "project-real", revision)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func revisionPointer(revision kernel.Revision) *kernel.Revision { return &revision }

func stringsContainAll(value string, needles ...string) bool {
	for _, needle := range needles {
		if !strings.Contains(value, needle) {
			return false
		}
	}
	return true
}

func assertProductionUIEvents(t *testing.T, ctx context.Context, events *uiprojection.EventLogQuery, principal auth.Principal, projectID kernel.ProjectID, want map[string]int) {
	t.Helper()
	page, err := events.ListEvents(ctx, principal, projectID, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	counts := make(map[string]int)
	for _, event := range page.Events {
		counts[event.Type]++
	}
	for eventType, minimum := range want {
		if counts[eventType] < minimum {
			t.Fatalf("event type %s count = %d, want at least %d; events=%#v", eventType, counts[eventType], minimum, page.Events)
		}
	}
}

func assertProductionUIEventCounts(t *testing.T, ctx context.Context, events *uiprojection.EventLogQuery, principal auth.Principal, projectID kernel.ProjectID, want map[string]int) {
	t.Helper()
	page, err := events.ListEvents(ctx, principal, projectID, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	counts := make(map[string]int)
	for _, event := range page.Events {
		counts[event.Type]++
	}
	for eventType, exact := range want {
		if counts[eventType] != exact {
			t.Fatalf("event type %s count = %d, want exactly %d; events=%#v", eventType, counts[eventType], exact, page.Events)
		}
	}
}

func productionUIEventsContain(events []uiprojection.UIEvent, eventTypes ...string) bool {
	seen := make(map[string]struct{}, len(events))
	for _, event := range events {
		seen[event.Type] = struct{}{}
	}
	for _, eventType := range eventTypes {
		if _, ok := seen[eventType]; !ok {
			return false
		}
	}
	return true
}

func TestProductionTaskManagerCleanupCancelsStuckInvocationAfterContextChildFailsAgainstPostgres(t *testing.T) {
	databaseURL := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("THREADMILL_TEST_DATABASE_URL is required for the real PostgreSQL integration test")
	}
	ctx := context.Background()
	db := openProductionPhasePostgres(t, ctx, databaseURL)
	defer db.Close()
	now := time.Date(2026, time.August, 11, 17, 0, 0, 0, time.UTC)
	invocations := runtimepkg.NewPostgresInvocationStoreFromSQL(db)
	manager := productionCleanupInvocation("tm-cleanup-broken-context", runtimepkg.InvocationRunning, now.Add(-10*time.Minute))
	if err := invocations.Create(ctx, manager); err != nil {
		t.Fatal(err)
	}
	child := runtimepkg.Invocation{
		ID: "context-cleanup-broken", ActorPrincipalID: "context-agent:project-real", ProjectID: manager.ProjectID,
		Role: auth.RoleContext, Operation: "retrieve", Status: runtimepkg.InvocationFailed,
		ConsumerInvocationID: manager.ID, ConsumerRole: auth.RoleTaskManager,
		PromptHashes: map[string]string{"context": "prompt-hash"}, SkillHashes: map[string]string{"context": "skill-hash"},
		EffectiveTools: []auth.Tool{auth.ToolContextSearch}, CreatedAt: now.Add(-5 * time.Minute), ExpiresAt: now.Add(55 * time.Minute),
	}
	if err := invocations.Create(ctx, child); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
INSERT INTO production_context_invocations(
  project_id, invocation_id, operation, request_key, request_hash, consumer_invocation_id,
  room_id, spec, runtime_config_ref, envelope_ref, required_capabilities, state, last_error, created_at, updated_at
) VALUES ($1,$2,'retrieve','request-broken','hash-broken',$3,'room-real','spec','runtime-config','runtime-envelope','[]'::jsonb,'failed','transport interrupted',$4,$5)`,
		manager.ProjectID, child.ID, manager.ID, child.CreatedAt, now.Add(-3*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
INSERT INTO agentteams_execution_refs(
  invocation_ref, attempt, invocation_id, agentteams_task_id, host_ref, dispatch_fingerprint,
  state, host_slot_claimed_at, mcp_client_key, mcp_token_hash, mcp_token_identifier, created_at, updated_at
) VALUES ('manager-input:broken-context',1,$1,'agentteams-broken-context','default','fingerprint-broken','dispatched',$2,'mcp-broken',$3,'token-broken',$2,$2)`,
		manager.ID, manager.CreatedAt, []byte("hash-broken")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
INSERT INTO production_manager_inputs(
  project_id, input_ref, request_id, input_kind, conversation_id, payload, payload_hash,
  observed_graph_revision, invocation_id, status, created_at, updated_at
) VALUES ($1,'manager-input:broken-context','request-broken-context','manager','conversation-broken','{}'::jsonb,'payload-broken',1,$2,'dispatched',$3,$3)`,
		manager.ProjectID, manager.ID, manager.CreatedAt); err != nil {
		t.Fatal(err)
	}
	terminator := &recordingCleanupTerminator{
		db: db, terminal: map[kernel.InvocationID]bool{},
		activities: map[kernel.InvocationID]agentteams.HostActivity{
			manager.ID: {Status: "running", RunningTaskCount: 1, LastRunAt: manager.CreatedAt},
		},
	}
	cleaner, err := newProductionTaskManagerExecutionCleanup(db, terminator, contextgraph.NewPostgresStore(db, time.Now), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if err := cleaner.CleanupTaskManagerInvocations(ctx); err != nil {
		t.Fatal(err)
	}
	if len(terminator.executions) != 1 || terminator.executions[0].InvocationID != manager.ID {
		t.Fatalf("terminated executions = %#v, want broken manager", terminator.executions)
	}
	var invocationStatus, inputStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM runtime_invocations WHERE invocation_id=$1`, manager.ID).Scan(&invocationStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT status FROM production_manager_inputs WHERE invocation_id=$1`, manager.ID).Scan(&inputStatus); err != nil {
		t.Fatal(err)
	}
	if invocationStatus != string(runtimepkg.InvocationFailed) || inputStatus != "failed" {
		t.Fatalf("broken context statuses invocation=%q input=%q, want failed/failed", invocationStatus, inputStatus)
	}
}

func productionCleanupInvocation(id kernel.InvocationID, status runtimepkg.InvocationStatus, now time.Time) runtimepkg.Invocation {
	return runtimepkg.Invocation{
		ID:               id,
		ActorPrincipalID: "task-manager:project-real",
		ProjectID:        "project-real",
		Role:             auth.RoleTaskManager,
		Status:           status,
		PromptHashes:     map[string]string{"task-manager": "prompt-hash"},
		SkillHashes:      map[string]string{"task-manager": "skill-hash"},
		EffectiveTools: []auth.Tool{
			auth.ToolCoordinationSnapshot,
			auth.ToolTaskManagerSubmitDecision,
		},
		CreatedAt: now,
		ExpiresAt: now.Add(time.Hour),
	}
}

type recordingProductionDispatcher struct {
	mu             sync.Mutex
	calls          int
	failNext       bool
	nextErr        error
	invocations    []string
	duringDispatch func(string) error
}

type recordingCleanupTerminator struct {
	db          *sql.DB
	executions  []agentteams.AgentTeamsExecutionRef
	finalized   []agentteams.AgentTeamsExecutionRef
	modes       []string
	fenced      map[kernel.InvocationID]bool
	terminal    map[kernel.InvocationID]bool
	terminalErr map[kernel.InvocationID]error
	activities  map[kernel.InvocationID]agentteams.HostActivity
}

func (r *recordingCleanupTerminator) FinalizeExecution(_ context.Context, execution agentteams.AgentTeamsExecutionRef, _ string) error {
	r.finalized = append(r.finalized, execution)
	return nil
}

func (r *recordingCleanupTerminator) FenceExecution(ctx context.Context, execution agentteams.AgentTeamsExecutionRef) error {
	if r.fenced == nil {
		r.fenced = make(map[kernel.InvocationID]bool)
	}
	r.fenced[execution.InvocationID] = true
	return nil
}

func (r *recordingCleanupTerminator) ExecutionTerminal(_ context.Context, execution agentteams.AgentTeamsExecutionRef) (bool, error) {
	if err := r.terminalErr[execution.InvocationID]; err != nil {
		return false, err
	}
	return r.terminal[execution.InvocationID], nil
}

func (r *recordingCleanupTerminator) ExecutionActivity(_ context.Context, execution agentteams.AgentTeamsExecutionRef) (agentteams.HostActivity, error) {
	return r.activities[execution.InvocationID], nil
}

func (r *recordingCleanupTerminator) Terminate(ctx context.Context, execution agentteams.AgentTeamsExecutionRef, mode string) error {
	r.executions = append(r.executions, execution)
	r.modes = append(r.modes, mode)
	_, err := r.db.ExecContext(ctx, `
UPDATE agentteams_execution_refs
SET mcp_revoked_at = COALESCE(mcp_revoked_at, now()),
    host_slot_released_at = COALESCE(host_slot_released_at, now()),
    state = 'terminated',
    termination_mode = $2,
    updated_at = now()
WHERE agentteams_task_id = $1`, execution.AgentTeamsTaskID, mode)
	return err
}

func (r *recordingProductionDispatcher) Dispatch(_ context.Context, invocationRef string) (agentteams.AgentTeamsExecutionRef, error) {
	r.mu.Lock()
	r.calls++
	r.invocations = append(r.invocations, invocationRef)
	fail := r.failNext
	r.failNext = false
	nextErr := r.nextErr
	r.nextErr = nil
	duringDispatch := r.duringDispatch
	r.mu.Unlock()
	if fail {
		return agentteams.AgentTeamsExecutionRef{}, errProductionDispatch
	}
	if nextErr != nil {
		return agentteams.AgentTeamsExecutionRef{}, nextErr
	}
	if duringDispatch != nil {
		if err := duringDispatch(invocationRef); err != nil {
			return agentteams.AgentTeamsExecutionRef{}, err
		}
	}
	return agentteams.AgentTeamsExecutionRef{InvocationID: kernel.InvocationID(invocationRef), AgentTeamsTaskID: "task-real", HostRef: "manager-real"}, nil
}

func (r *recordingProductionDispatcher) failNextDispatch() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failNext = true
}

func productionTestTaskManagerPrincipal(invocationID kernel.InvocationID) auth.Principal {
	return auth.Principal{
		ActorPrincipalID: "task-manager-real", Kind: auth.PrincipalAgent, ProjectID: "project-real", Role: auth.RoleTaskManager, InvocationID: invocationID,
		Tools: auth.ToolSet(auth.ToolCoordinationSnapshot, auth.ToolTaskManagerSubmitDecision, auth.ToolCoordinationReplacePending, auth.ToolCoordinationTransition),
	}
}

func productionTestDatabaseURLWithSearchPath(databaseURL, schema string) (string, error) {
	parsed, err := url.Parse(databaseURL)
	if err != nil {
		return "", err
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}
