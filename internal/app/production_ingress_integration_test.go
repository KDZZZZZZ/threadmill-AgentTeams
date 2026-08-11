package app

import (
	"context"
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
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/postgres"
	runtimepkg "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime"
	phasepkg "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime/phase"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime/promptcatalog"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/taskmanager"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/transport/httpapi"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/transport/mcpapi"
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
	now := time.Date(2026, time.August, 11, 16, 0, 0, 0, time.UTC)
	ingress, err := newProductionIngress(db.SQL(), "project-real", "room-real", assembler, graph, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	dispatcher := &recordingProductionDispatcher{}
	if err := ingress.setDispatcher(dispatcher); err != nil {
		t.Fatal(err)
	}
	resources := newRecordingTaskResources()
	managerRuntime, err := newProductionTaskManagerRuntime(db.SQL(), "project-real", graph, func() time.Time { return now.Add(time.Minute) })
	if err != nil {
		t.Fatal(err)
	}
	if err := managerRuntime.setProductionDependencies(resources, resources, ingress); err != nil {
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
		Tasks: []coordination.Task{{ID: "task-trusted", ContractRef: "contract-trusted", Outcome: coordination.TaskFailed}},
		Endpoints: []coordination.PhaseEndpoint{
			productionForgedEndpoint("task-trusted", coordination.EndpointPlan, "spec-plan"),
			productionForgedEndpoint("task-trusted", coordination.EndpointExecute, "spec-execute"),
			productionForgedEndpoint("task-trusted", coordination.EndpointVerify, "spec-verify"),
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
	if contractRef != "contract-trusted" || requirementText != "implement and verify the production workflow" || !stringsContainAll(phaseSpecs, "spec-plan", "spec-execute", "spec-verify") {
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
	if err := restarted.setProductionDependencies(resources, resources, ingress); err != nil {
		t.Fatal(err)
	}
	revision, err := restarted.ReplacePending(ctx, managerPrincipal, managerScope, intent)
	if err != nil {
		t.Fatalf("ReplacePending() recovery error = %v", err)
	}
	if revision != 2 || resources.contextCallCount("task-trusted") != 1 {
		t.Fatalf("recovered revision=%d successful context calls=%d", revision, resources.contextCallCount("task-trusted"))
	}
	changedSpec := intent
	changedSpec.Endpoints = append([]coordination.PhaseEndpoint(nil), intent.Endpoints...)
	changedSpec.Endpoints[0].SpecRef = "agent-rewrites-frozen-spec"
	if _, err := restarted.ReplacePending(ctx, managerPrincipal, managerScope, changedSpec); !kernel.IsCode(err, kernel.CodeStaleBinding) {
		t.Fatalf("changed frozen spec error = %v, want stale_binding", err)
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

	restartedAgain, err := newProductionTaskManagerRuntime(db.SQL(), "project-real", graph, func() time.Time { return now.Add(3 * time.Minute) })
	if err != nil {
		t.Fatal(err)
	}
	if err := restartedAgain.setProductionDependencies(resources, resources, ingress); err != nil {
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
		SelectedEndpoint: &planRef, ObservedGraphRevision: revisionPointer(4),
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
	var semanticDecisions int
	if err := db.SQL().QueryRowContext(ctx, `SELECT COUNT(DISTINCT input_ref) FROM taskmanager_decisions
WHERE project_id=$1 AND decision->>'action' IN ('submitted','satisfied')`, "project-real").Scan(&semanticDecisions); err != nil {
		t.Fatal(err)
	}
	if semanticDecisions != 2 {
		t.Fatalf("submitted/satisfied decision input count = %d, want 2", semanticDecisions)
	}
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

func (r *recordingTaskResources) EnsureTaskContext(_ context.Context, req productionTaskContextRequest) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failContextOnce {
		r.failContextOnce = false
		return errProductionContextProjection
	}
	r.contextCalls[req.TaskID]++
	r.contexts[req.TaskID] = req
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

func productionForgedEndpoint(taskID kernel.TaskID, endpointID coordination.EndpointID, specRef string) coordination.PhaseEndpoint {
	return coordination.PhaseEndpoint{
		Ref: coordination.PhaseEndpointRef{TaskID: taskID, EndpointID: endpointID}, SpecRef: specRef,
		BindingRef: "agent://forged", Generation: 88, State: coordination.EndpointSatisfied, RunPolicy: coordination.RunEnabled,
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

type recordingProductionDispatcher struct {
	mu             sync.Mutex
	calls          int
	failNext       bool
	invocations    []string
	duringDispatch func(string) error
}

func (r *recordingProductionDispatcher) Dispatch(_ context.Context, invocationRef string) (agentteams.AgentTeamsExecutionRef, error) {
	r.mu.Lock()
	r.calls++
	r.invocations = append(r.invocations, invocationRef)
	fail := r.failNext
	r.failNext = false
	duringDispatch := r.duringDispatch
	r.mu.Unlock()
	if fail {
		return agentteams.AgentTeamsExecutionRef{}, errProductionDispatch
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
