package app

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/adapters/agentteams"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/contextgraph"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/evidence"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/config"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/objectstore"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/postgres"
	runtimepkg "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime"
	phasepkg "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime/phase"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime/promptcatalog"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/taskmanager"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/workspace"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/migrations"
)

func TestProductionPhaseBindingContextWorkspaceAndArtifactsAgainstPostgres(t *testing.T) {
	databaseURL := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("THREADMILL_TEST_DATABASE_URL is required for the real PostgreSQL integration test")
	}
	ctx := context.Background()
	db := openProductionPhasePostgres(t, ctx, databaseURL)
	defer db.Close()

	projectID := kernel.ProjectID("project-phase-real")
	graph := coordination.NewPostgresStore(db)
	seedProductionPhaseTask(t, ctx, db, projectID, graph)
	repo := seedProductionPhaseBareRepo(t)
	worktrees := t.TempDir()
	now := time.Date(2026, time.August, 11, 18, 0, 0, 0, time.UTC)
	contexts := contextgraph.NewPostgresStore(db, func() time.Time { return now })
	contexts.SetTaskEndpointResolver(productionTaskEndpointResolver{projectID: projectID, graph: graph})
	source := &productionPhaseBindingSource{
		db: db, projectID: projectID, graph: graph, contracts: taskmanager.NewPostgresStore(db, projectID, graph),
		workspaces: workspace.NewPostgresService(db), contexts: contexts,
		repositoryPath: repo, worktreeParent: worktrees, now: func() time.Time { return now },
	}
	resolver := phasepkg.NewContextBindingResolver(source, contexts)
	command := coordination.PhaseCommand{
		ID: "cmd:run:task-real:plan:1", Endpoint: coordination.PhaseEndpointRef{TaskID: "task-real", EndpointID: coordination.EndpointPlan},
		Generation: 1, BindingRef: "binding://task-real/plan/1", LeaseRef: "lease-plan-real", Action: coordination.CommandStart,
	}
	invocationID := productionPhaseInvocationID(command.ID)
	binding, err := resolver.ResolveForInvocation(ctx, phasepkg.BindingResolveRequest{Command: command, InvocationID: invocationID})
	if err != nil {
		t.Fatalf("ResolveForInvocation() error = %v", err)
	}
	if binding.WorkspaceRef == string(command.BindingRef) || binding.ContextSliceRef == "" || binding.TaskMemoryBufferRef == "" {
		t.Fatalf("binding did not separate graph/workspace/context refs: %#v", binding)
	}
	var storedWorkspace string
	if err := db.QueryRowContext(ctx, `SELECT workspace_ref FROM production_phase_bindings WHERE project_id=$1 AND task_id='task-real' AND endpoint_id='plan'`, projectID).Scan(&storedWorkspace); err != nil {
		t.Fatal(err)
	}
	if storedWorkspace != binding.WorkspaceRef {
		t.Fatalf("stored workspace_ref=%q binding=%q", storedWorkspace, binding.WorkspaceRef)
	}
	var activeSubscriptions int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM context_subscriptions WHERE project_id=$1 AND consumer_invocation_id=$2 AND active`, projectID, invocationID).Scan(&activeSubscriptions); err != nil {
		t.Fatal(err)
	}
	if activeSubscriptions != 1 {
		t.Fatalf("active initial subscriptions = %d, want 1", activeSubscriptions)
	}
	leased, err := workspace.NewPostgresService(db).Get(ctx, kernel.BindingRef(binding.WorkspaceRef))
	if err != nil {
		t.Fatal(err)
	}
	if leased.ActiveInvocation != invocationID || leased.ActivePhase != workspace.PhasePlan {
		t.Fatalf("workspace lease = %#v", leased)
	}
	stopCommand := command
	stopCommand.ID = "cmd:stop:task-real:plan:1"
	stopCommand.Action = coordination.CommandStop
	stopBinding, _, err := source.ResolvePhaseBinding(ctx, phasepkg.BindingResolveRequest{Command: stopCommand, InvocationID: productionPhaseInvocationID(stopCommand.ID)})
	if err != nil {
		t.Fatalf("stop ResolvePhaseBinding() error = %v", err)
	}
	if stopBinding.WorkspaceRef != binding.WorkspaceRef || stopBinding.WorkspaceRevision != binding.WorkspaceRevision {
		t.Fatalf("stop binding = %#v; start binding = %#v", stopBinding, binding)
	}
	stillLeased, err := workspace.NewPostgresService(db).Get(ctx, kernel.BindingRef(binding.WorkspaceRef))
	if err != nil {
		t.Fatal(err)
	}
	if stillLeased.ActiveInvocation != invocationID {
		t.Fatalf("stop resolve changed workspace lease: %#v", stillLeased)
	}
	lifecycle := productionPhaseLifecycle{workspaces: workspace.NewPostgresService(db), contexts: phasepkg.ContextBindingLifecycle{Contexts: contexts}}
	if err := lifecycle.Complete(ctx, runtimepkg.Invocation{
		ID: invocationID, ActorPrincipalID: binding.ActorPrincipalID, ProjectID: projectID,
		TaskID: "task-real", EndpointID: coordination.EndpointPlan, Generation: 1, Role: auth.RolePlanner,
		WorkspaceRef: binding.WorkspaceRef,
	}); err != nil {
		t.Fatalf("lifecycle Complete() error = %v", err)
	}
	completed, err := workspace.NewPostgresService(db).Get(ctx, kernel.BindingRef(binding.WorkspaceRef))
	if err != nil {
		t.Fatal(err)
	}
	if completed.ActiveInvocation != "" || completed.PhaseLeases[workspace.PhasePlan] != invocationID {
		t.Fatalf("completed workspace = %#v", completed)
	}
	registry := evidence.NewPostgresArtifactRegistry(db, objectstore.NewMemoryStore(), "artifacts")
	artifact, err := registry.Register(ctx, evidence.RegisterArtifact{Type: evidence.ArtifactGeneratedReport, ProjectID: projectID, TaskID: "task-real", AgentInvocationID: invocationID, Body: []byte("report")})
	if err != nil {
		t.Fatal(err)
	}
	router := productionPhaseArtifactRouter{registry: registry}
	active := phasepkg.ActiveInvocation{Invocation: runtimepkg.Invocation{ID: invocationID, ProjectID: projectID, TaskID: "task-real"}}
	if routed, err := router.Route(ctx, active, string(artifact.ID)); err != nil || routed != string(artifact.ID) {
		t.Fatalf("Route(valid) = %q, %v", routed, err)
	}
	if _, err := router.Route(ctx, active, "workspace/report.md"); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("Route(path) error = %v, want forbidden", err)
	}
	taskResolver := productionTaskEndpointResolver{projectID: projectID, graph: graph}
	if ok, err := taskResolver.TaskDone(ctx, "other-project", "task-real"); err != nil || ok {
		t.Fatalf("cross-project TaskDone = %v, %v; want false nil", ok, err)
	}
	if ok, err := taskResolver.EndpointExists(ctx, "other-project", contextgraph.PhaseEndpointRef{TaskID: "task-real", EndpointID: coordination.EndpointPlan}); err != nil || ok {
		t.Fatalf("cross-project EndpointExists = %v, %v; want false nil", ok, err)
	}
}

func TestProductionPhaseBindingAbortReleasesWorkspaceLeaseAgainstPostgres(t *testing.T) {
	databaseURL := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("THREADMILL_TEST_DATABASE_URL is required for the real PostgreSQL integration test")
	}
	ctx := context.Background()
	db := openProductionPhasePostgres(t, ctx, databaseURL)
	defer db.Close()

	projectID := kernel.ProjectID("project-phase-abort")
	graph := coordination.NewPostgresStore(db)
	seedProductionPhaseTask(t, ctx, db, projectID, graph)
	repo := seedProductionPhaseBareRepo(t)
	now := time.Date(2026, time.August, 11, 18, 30, 0, 0, time.UTC)
	contexts := contextgraph.NewPostgresStore(db, func() time.Time { return now })
	contexts.SetTaskEndpointResolver(productionTaskEndpointResolver{projectID: projectID, graph: graph})
	source := &productionPhaseBindingSource{
		db: db, projectID: projectID, graph: graph, contracts: taskmanager.NewPostgresStore(db, projectID, graph),
		workspaces: workspace.NewPostgresService(db), contexts: contexts,
		repositoryPath: repo, worktreeParent: t.TempDir(), now: func() time.Time { return now },
	}
	resolver := phasepkg.NewContextBindingResolver(source, failingProductionPhaseContextRuntime{})
	command := coordination.PhaseCommand{
		ID: "cmd:run:task-real:plan:1", Endpoint: coordination.PhaseEndpointRef{TaskID: "task-real", EndpointID: coordination.EndpointPlan},
		Generation: 1, BindingRef: "binding://task-real/plan/1", LeaseRef: "lease-plan-abort", Action: coordination.CommandStart,
	}
	invocationID := productionPhaseInvocationID(command.ID)
	if _, err := resolver.ResolveForInvocation(ctx, phasepkg.BindingResolveRequest{Command: command, InvocationID: invocationID}); !kernel.IsCode(err, kernel.CodeInvalidRequest) {
		t.Fatalf("ResolveForInvocation() error = %v, want invalid_argument", err)
	}
	var workspaceRef string
	if err := db.QueryRowContext(ctx, `SELECT workspace_ref FROM production_phase_bindings WHERE project_id=$1 AND task_id='task-real' AND endpoint_id='plan'`, projectID).Scan(&workspaceRef); err != nil {
		t.Fatal(err)
	}
	binding, err := workspace.NewPostgresService(db).Get(ctx, kernel.BindingRef(workspaceRef))
	if err != nil {
		t.Fatal(err)
	}
	if binding.ActiveInvocation != "" || binding.ActivePhase != "" {
		t.Fatalf("abort leaked active workspace lease: %#v", binding)
	}
	var activeSubscriptions int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM context_subscriptions WHERE project_id=$1 AND consumer_invocation_id=$2 AND active`, projectID, invocationID).Scan(&activeSubscriptions); err != nil {
		t.Fatal(err)
	}
	if activeSubscriptions != 0 {
		t.Fatalf("abort leaked active subscriptions = %d, want 0", activeSubscriptions)
	}
}

func TestProductionPhaseResolveFailureReleasesWorkspaceLeaseAgainstPostgres(t *testing.T) {
	databaseURL := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("THREADMILL_TEST_DATABASE_URL is required for the real PostgreSQL integration test")
	}
	ctx := context.Background()
	db := openProductionPhasePostgres(t, ctx, databaseURL)
	defer db.Close()

	projectID := kernel.ProjectID("project-phase-resolve-rollback")
	graph := coordination.NewPostgresStore(db)
	seedProductionPhaseTask(t, ctx, db, projectID, graph)
	repo := seedProductionPhaseBareRepo(t)
	contexts := contextgraph.NewPostgresStore(db, time.Now)
	contexts.SetTaskEndpointResolver(productionTaskEndpointResolver{projectID: projectID, graph: graph})
	source := &productionPhaseBindingSource{
		db: db, projectID: projectID, graph: graph, contracts: taskmanager.NewPostgresStore(db, projectID, graph),
		workspaces: workspace.NewPostgresService(db), contexts: contexts,
		repositoryPath: repo, worktreeParent: t.TempDir(), now: time.Now,
	}
	command := coordination.PhaseCommand{
		ID: "cmd:run:task-real:plan:1", Endpoint: coordination.PhaseEndpointRef{TaskID: "task-real", EndpointID: coordination.EndpointPlan},
		Generation: 1, BindingRef: "binding://task-real/plan/1", LeaseRef: "lease-plan-rollback", Action: coordination.CommandStart,
	}
	if _, err := db.ExecContext(ctx, `
INSERT INTO production_phase_bindings (
  project_id, task_id, endpoint_id, generation, graph_binding_ref, workspace_ref,
  actor_principal_id, contract_ref, spec_ref
) VALUES ($1,'task-real','plan',1,'binding://task-real/plan/1','workspace-conflict','actor-conflict','contract://task-real','spec://task-real/plan')`, projectID); err != nil {
		t.Fatal(err)
	}
	invocationID := productionPhaseInvocationID(command.ID)
	if _, _, err := source.ResolvePhaseBinding(ctx, phasepkg.BindingResolveRequest{Command: command, InvocationID: invocationID}); !kernel.IsCode(err, kernel.CodeIdempotencyConflict) {
		t.Fatalf("ResolvePhaseBinding() error = %v, want idempotency_conflict", err)
	}
	binding, err := source.EnsureTaskWorkspace(ctx, productionTaskWorkspaceRequest{TaskID: "task-real", Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	workspaceBinding, err := workspace.NewPostgresService(db).Get(ctx, binding)
	if err != nil {
		t.Fatal(err)
	}
	if workspaceBinding.ActiveInvocation != "" || workspaceBinding.ActivePhase != "" {
		t.Fatalf("resolve failure leaked active workspace lease: %#v", workspaceBinding)
	}
}

func TestProductionPhaseInputsResolveArtifactKindsAgainstPostgres(t *testing.T) {
	databaseURL := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("THREADMILL_TEST_DATABASE_URL is required for the real PostgreSQL integration test")
	}
	ctx := context.Background()
	db := openProductionPhasePostgres(t, ctx, databaseURL)
	defer db.Close()

	projectID := kernel.ProjectID("project-phase-input-kinds")
	graph := coordination.NewPostgresStore(db)
	seedProductionPhaseCrossTaskGraph(t, ctx, db, projectID, graph)
	contexts := contextgraph.NewPostgresStore(db, time.Now)
	contexts.SetTaskEndpointResolver(productionTaskEndpointResolver{projectID: projectID, graph: graph})
	source := &productionPhaseBindingSource{
		db: db, projectID: projectID, graph: graph, contracts: taskmanager.NewPostgresStore(db, projectID, graph),
		workspaces: workspace.NewPostgresService(db), contexts: contexts,
		repositoryPath: seedProductionPhaseBareRepo(t), worktreeParent: t.TempDir(), now: time.Now,
	}
	registry := evidence.NewPostgresArtifactRegistry(db, objectstore.NewMemoryStore(), "artifacts")
	artifact, err := registry.Register(ctx, evidence.RegisterArtifact{
		Type: evidence.ArtifactGeneratedReport, ProjectID: projectID, TaskID: "task-source", AgentInvocationID: "inv-source-plan", Body: []byte("source report"),
	})
	if err != nil {
		t.Fatal(err)
	}
	created := time.Now().UTC()
	invocation := runtimepkg.Invocation{
		ID: "inv-source-plan", ActorPrincipalID: "actor-source-plan", ProjectID: projectID,
		TaskID: "task-source", EndpointID: coordination.EndpointPlan, Generation: 1, Role: auth.RolePlanner,
		Status: runtimepkg.InvocationCompleted, BindingRef: "binding://task-source/plan/1", LeaseID: "lease-source-plan",
		PromptHashes: map[string]string{"prompt": "hash"}, SkillHashes: map[string]string{"skill": "hash"},
		EffectiveTools: []auth.Tool{auth.ToolAgentSubmitPhaseOutput}, CreatedAt: created, ExpiresAt: created.Add(time.Hour),
	}
	if err := runtimepkg.NewPostgresInvocationStoreFromSQL(db).Create(ctx, invocation); err != nil {
		t.Fatal(err)
	}
	output := phasepkg.PhaseOutput{Phase: string(coordination.EndpointPlan), ReportRef: string(artifact.ID)}
	outputJSON, _ := json.Marshal(output)
	artifactRefsJSON, _ := json.Marshal([]string{string(artifact.ID)})
	outputRef := "output://task-source/plan/1"
	if _, err := db.ExecContext(ctx, `
INSERT INTO production_phase_outputs (
  output_ref, project_id, task_id, endpoint_id, generation, binding_ref, lease_ref,
  invocation_id, input_revision, output, artifact_refs
) VALUES ($1,$2,'task-source','plan',1,'binding://task-source/plan/1','lease-source-plan',$3,'inputs-1',$4::jsonb,$5::jsonb)`,
		outputRef, projectID, invocation.ID, outputJSON, artifactRefsJSON); err != nil {
		t.Fatal(err)
	}
	snapshot, err := graph.Latest(ctx, projectID)
	if err != nil {
		t.Fatal(err)
	}
	if err := graph.RegisterTransition(ctx, projectID, "submit-source-plan", coordination.GraphTransition{
		TargetKind: coordination.TargetPhaseEndpoint, Endpoint: coordination.PhaseEndpointRef{TaskID: "task-source", EndpointID: coordination.EndpointPlan},
		Action: string(coordination.EndpointSubmitted), Generation: 1,
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, err = graph.Transition(ctx, projectID, snapshot.Revision, coordination.GraphTransition{
		TargetKind: coordination.TargetPhaseEndpoint, Endpoint: coordination.PhaseEndpointRef{TaskID: "task-source", EndpointID: coordination.EndpointPlan},
		Action: string(coordination.EndpointSubmitted), Generation: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := graph.RegisterTransition(ctx, projectID, "satisfy-source-plan", coordination.GraphTransition{
		TargetKind: coordination.TargetPhaseEndpoint, Endpoint: coordination.PhaseEndpointRef{TaskID: "task-source", EndpointID: coordination.EndpointPlan},
		Action: string(coordination.EndpointSatisfied), Generation: 1,
		Result: coordination.PhaseResult{
			ID: "result-source-plan", Endpoint: coordination.PhaseEndpointRef{TaskID: "task-source", EndpointID: coordination.EndpointPlan},
			BindingRef: "binding://task-source/plan/1", OutputRef: outputRef, Verdict: coordination.VerdictSatisfied,
		},
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, err = graph.Transition(ctx, projectID, snapshot.Revision, coordination.GraphTransition{
		TargetKind: coordination.TargetPhaseEndpoint, Endpoint: coordination.PhaseEndpointRef{TaskID: "task-source", EndpointID: coordination.EndpointPlan},
		Action: string(coordination.EndpointSatisfied), Generation: 1,
		Result: coordination.PhaseResult{
			ID: "result-source-plan", Endpoint: coordination.PhaseEndpointRef{TaskID: "task-source", EndpointID: coordination.EndpointPlan},
			BindingRef: "binding://task-source/plan/1", OutputRef: outputRef, Verdict: coordination.VerdictSatisfied,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	inputs, err := source.inputs(ctx, snapshot, coordination.PhaseCommand{
		Endpoint:   coordination.PhaseEndpointRef{TaskID: "task-real", EndpointID: coordination.EndpointExecute},
		Generation: 1, BindingRef: "binding://task-real/execute/1", Action: coordination.CommandStart,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(inputs.Required) != 1 || len(inputs.Delivered) != 1 {
		t.Fatalf("inputs = %#v, want one required and delivered input", inputs)
	}
	required := strings.Join(inputs.Required[0].RequiredArtifacts, ",")
	if required != string(artifact.ID) {
		t.Fatalf("required artifacts = %q, want %q", required, artifact.ID)
	}
	if got := strings.Join(inputs.Delivered[0].ArtifactRefs, ","); got != string(artifact.ID) {
		t.Fatalf("delivered artifacts = %q, want %q", got, artifact.ID)
	}
}

func TestProductionPhaseTerminalOutboxReplaysOutputAndStopAgainstPostgres(t *testing.T) {
	databaseURL := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("THREADMILL_TEST_DATABASE_URL is required for the real PostgreSQL integration test")
	}
	ctx := context.Background()
	db := openProductionPhasePostgres(t, ctx, databaseURL)
	defer db.Close()

	projectID := kernel.ProjectID("project-phase-terminal")
	graph := coordination.NewPostgresStore(db)
	seedProductionPhaseTask(t, ctx, db, projectID, graph)
	assembler := productionPhaseTestAssembler(t)
	ingress, err := newProductionIngress(db, projectID, "room-phase-terminal", assembler, graph, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	dispatcher := &recordingProductionDispatcher{}
	dispatcher.failNextDispatch()
	if err := ingress.setDispatcher(dispatcher); err != nil {
		t.Fatal(err)
	}
	runtime := &productionPhaseRuntime{
		db: db, projectID: projectID, graph: graph, ingress: ingress,
		invocations: runtimepkg.NewPostgresInvocationStoreFromSQL(db),
		artifacts:   evidence.NewPostgresArtifactRegistry(db, objectstore.NewMemoryStore(), "artifacts"),
		now:         time.Now,
	}
	created := time.Now().UTC()
	if err := runtime.invocations.Create(ctx, runtimepkg.Invocation{
		ID: "inv-terminal-output", ActorPrincipalID: "actor-terminal-output", ProjectID: projectID,
		TaskID: "task-real", EndpointID: coordination.EndpointPlan, Generation: 1, Role: auth.RolePlanner,
		Status: runtimepkg.InvocationCompleted, BindingRef: "binding://task-real/plan/1", LeaseID: "lease-terminal-output",
		PromptHashes: map[string]string{"prompt": "hash"}, SkillHashes: map[string]string{"skill": "hash"},
		EffectiveTools: []auth.Tool{auth.ToolAgentSubmitPhaseOutput}, CreatedAt: created, ExpiresAt: created.Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	receipt := phasepkg.OutputReceipt{
		Output:       phasepkg.PhaseOutput{Phase: string(coordination.EndpointPlan), ReportRef: "art_report_terminal"},
		InvocationID: "inv-terminal-output", CommandID: "cmd-terminal-output", CommandAction: coordination.CommandStart,
		Endpoint:   coordination.PhaseEndpointRef{TaskID: "task-real", EndpointID: coordination.EndpointPlan},
		Generation: 1, BindingRef: "binding://task-real/plan/1", LeaseRef: "lease-terminal-output",
		InputRevision: "inputs-terminal", WorkspaceRef: "workspace-terminal", WorkspaceHead: "head-terminal",
		SubmittedAtUTC: time.Now().UTC(),
	}
	requestID, err := runtime.persistOutputIntent(ctx, receipt.InvocationID, receipt.Output)
	if err != nil {
		t.Fatalf("persistOutputIntent: %v", err)
	}
	if err := runtime.deliverOutputReceipt(ctx, receipt, requestID, receipt.Output); !errors.Is(err, errProductionDispatch) {
		t.Fatalf("deliverOutputReceipt error = %v, want injected dispatch failure", err)
	}
	assertPhaseTerminalObligation(t, ctx, db, projectID, "phase_output", "pending")
	if err := runtime.ReplayTerminalDeliveries(ctx); err != nil {
		t.Fatalf("ReplayTerminalDeliveries output: %v", err)
	}
	assertPhaseTerminalObligation(t, ctx, db, projectID, "phase_output", "delivered")

	dispatcher.failNextDispatch()
	writer := productionPhaseObservationWriter{projectID: projectID, graph: graph, ingress: ingress, now: time.Now}
	stop := coordination.PhaseCommand{
		ID: "cmd-terminal-stop", Endpoint: coordination.PhaseEndpointRef{TaskID: "task-real", EndpointID: coordination.EndpointPlan},
		Generation: 1, BindingRef: "binding://task-real/plan/1", LeaseRef: "lease-terminal-stop", Action: coordination.CommandStop,
	}
	if _, err := db.ExecContext(ctx, `
INSERT INTO coordination_phase_leases(project_id, lease_ref, task_id, endpoint_id, generation, binding_ref, state)
VALUES ($1,$2,$3,$4,$5,$6,'active')`, projectID, stop.LeaseRef, stop.Endpoint.TaskID, stop.Endpoint.EndpointID, stop.Generation, stop.BindingRef); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
INSERT INTO coordination_phase_commands(project_id, command_id, task_id, endpoint_id, generation, binding_ref, lease_ref, action, cause_ref)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'test-stop')`, projectID, stop.ID, stop.Endpoint.TaskID, stop.Endpoint.EndpointID, stop.Generation, stop.BindingRef, stop.LeaseRef, stop.Action); err != nil {
		t.Fatal(err)
	}
	if err := writer.RecordPhaseInvocationStopped(ctx, projectID, stop, "checkpoint-terminal", false); !errors.Is(err, errProductionDispatch) {
		t.Fatalf("RecordPhaseInvocationStopped error = %v, want injected dispatch failure", err)
	}
	assertPhaseTerminalObligation(t, ctx, db, projectID, "phase_stopped", "pending")
	if err := runtime.ReplayTerminalDeliveries(ctx); err != nil {
		t.Fatalf("ReplayTerminalDeliveries stop: %v", err)
	}
	assertPhaseTerminalObligation(t, ctx, db, projectID, "phase_stopped", "delivered")
}

func TestProductionPhaseOutputIntentReplaysRecoveredReceiptAgainstPostgres(t *testing.T) {
	databaseURL := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("THREADMILL_TEST_DATABASE_URL is required for the real PostgreSQL integration test")
	}
	ctx := context.Background()
	db := openProductionPhasePostgres(t, ctx, databaseURL)
	defer db.Close()

	projectID := kernel.ProjectID("project-phase-output-intent")
	graph := coordination.NewPostgresStore(db)
	seedProductionPhaseTask(t, ctx, db, projectID, graph)
	assembler := productionPhaseTestAssembler(t)
	ingress, err := newProductionIngress(db, projectID, "room-phase-output-intent", assembler, graph, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	dispatcher := &recordingProductionDispatcher{}
	if err := ingress.setDispatcher(dispatcher); err != nil {
		t.Fatal(err)
	}
	invocations := runtimepkg.NewPostgresInvocationStoreFromSQL(db)
	recovery := phasepkg.NewPostgresRecoveryStoreFromSQL(db, invocations)
	artifacts := evidence.NewPostgresArtifactRegistry(db, objectstore.NewMemoryStore(), "artifacts")
	runtime := &productionPhaseRuntime{
		db: db, projectID: projectID, graph: graph, ingress: ingress,
		invocations: invocations, artifacts: artifacts, recovery: recovery, now: time.Now,
	}
	command := coordination.PhaseCommand{
		ID: "cmd-output-intent", Endpoint: coordination.PhaseEndpointRef{TaskID: "task-real", EndpointID: coordination.EndpointPlan},
		Generation: 1, BindingRef: "binding://task-real/plan/1", LeaseRef: "lease-output-intent", Action: coordination.CommandStart,
	}
	invocationID := productionPhaseInvocationID(command.ID)
	created := time.Now().UTC()
	invocation := runtimepkg.Invocation{
		ID: invocationID, ActorPrincipalID: "actor-output-intent", ProjectID: projectID,
		TaskID: command.Endpoint.TaskID, EndpointID: command.Endpoint.EndpointID, Generation: uint64(command.Generation), Role: auth.RolePlanner,
		Status: runtimepkg.InvocationRunning, BindingRef: command.BindingRef, LeaseID: command.LeaseRef,
		PromptHashes: map[string]string{"prompt": "hash"}, SkillHashes: map[string]string{"skill": "hash"},
		EffectiveTools: []auth.Tool{auth.ToolAgentSubmitPhaseOutput}, CreatedAt: created, ExpiresAt: created.Add(time.Hour),
	}
	if err := invocations.Create(ctx, invocation); err != nil {
		t.Fatal(err)
	}
	output := phasepkg.PhaseOutput{Phase: string(coordination.EndpointPlan), ReportRef: "art_report_output_intent"}
	requestID, err := runtime.persistOutputIntent(ctx, invocationID, output)
	if err != nil {
		t.Fatalf("persistOutputIntent: %v", err)
	}
	assertPhaseTerminalObligation(t, ctx, db, projectID, "phase_output", "intent")
	inputs := phasepkg.PhaseInputSet{InputRevision: "inputs-output-intent"}
	active := phasepkg.ActiveInvocation{
		Invocation: invocation, Command: command,
		Binding: phasepkg.BindingSnapshot{
			ProjectID: projectID, ActorPrincipalID: invocation.ActorPrincipalID, TaskID: command.Endpoint.TaskID,
			EndpointID: command.Endpoint.EndpointID, Generation: command.Generation, BindingRef: command.BindingRef,
			LeaseRef: command.LeaseRef, WorkspaceRef: "workspace-output-intent", WorkspaceRevision: "head-output-intent", Inputs: inputs,
		},
		Inputs: inputs,
	}
	if err := recovery.RecordActiveInvocation(ctx, active); err != nil {
		t.Fatalf("record active: %v", err)
	}
	receipt := phasepkg.OutputReceipt{
		Output: phasepkg.PhaseOutput{Phase: string(coordination.EndpointPlan), ReportRef: "art_report_output_intent_canonical"}, InvocationID: invocationID, CommandID: command.ID, CommandAction: command.Action,
		Endpoint: command.Endpoint, Generation: command.Generation, BindingRef: command.BindingRef, LeaseRef: command.LeaseRef,
		InputRevision: inputs.InputRevision, WorkspaceRef: active.Binding.WorkspaceRef, WorkspaceHead: active.Binding.WorkspaceRevision,
		SubmittedAtUTC: time.Now().UTC(),
	}
	outputRaw, _ := json.Marshal(output)
	receipt.OutputFingerprint = hashProductionBytes(outputRaw)
	if err := recovery.RecordOutputReceipt(ctx, active, receipt); err != nil {
		t.Fatalf("record output receipt: %v", err)
	}
	if err := runtime.abandonOutputIntent(ctx, invocationID, requestID, output, kernel.InvalidArgument("deterministic rejection after receipt must not abandon")); err != nil {
		t.Fatalf("abandonOutputIntent after receipt: %v", err)
	}
	assertPhaseTerminalObligation(t, ctx, db, projectID, "phase_output", "intent")

	rebuilt := &productionPhaseRuntime{
		db: db, projectID: projectID, graph: graph, ingress: ingress,
		invocations: invocations, artifacts: artifacts, recovery: phasepkg.NewPostgresRecoveryStoreFromSQL(db, invocations), now: time.Now,
	}
	if err := rebuilt.ReplayTerminalDeliveries(ctx); err != nil {
		t.Fatalf("ReplayTerminalDeliveries: %v", err)
	}
	assertPhaseTerminalObligation(t, ctx, db, projectID, "phase_output", "delivered")
	var outputRows, dispatchedRows int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM production_phase_outputs WHERE project_id=$1 AND invocation_id=$2`, projectID, invocationID).Scan(&outputRows); err != nil {
		t.Fatal(err)
	}
	if outputRows != 1 {
		t.Fatalf("production_phase_outputs rows=%d, want 1", outputRows)
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM production_manager_inputs WHERE project_id=$1 AND input_kind='phase_output' AND request_id=$2 AND status='dispatched'`, projectID, requestID).Scan(&dispatchedRows); err != nil {
		t.Fatal(err)
	}
	if dispatchedRows != 1 {
		t.Fatalf("dispatched manager inputs=%d, want 1", dispatchedRows)
	}

	advanced, err := graph.Latest(ctx, projectID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.Transition(ctx, projectID, advanced.Revision, coordination.GraphTransition{
		TargetKind: coordination.TargetPhaseEndpoint, Endpoint: coordination.PhaseEndpointRef{TaskID: "task-real", EndpointID: coordination.EndpointPlan},
		Action: string(coordination.EndpointSubmitted), Generation: 1,
	}); err != nil {
		t.Fatalf("advance graph revision before retry: %v", err)
	}

	retryRequestID, err := rebuilt.persistOutputIntent(ctx, invocationID, output)
	if err != nil {
		t.Fatalf("persistOutputIntent retry same output: %v", err)
	}
	if retryRequestID != requestID {
		t.Fatalf("retry request id=%s, want %s", retryRequestID, requestID)
	}
	if err := rebuilt.deliverOutputReceipt(ctx, receipt, retryRequestID, output); err != nil {
		t.Fatalf("deliverOutputReceipt retry same output: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM production_manager_inputs WHERE project_id=$1 AND input_kind='phase_output' AND request_id=$2`, projectID, requestID).Scan(&dispatchedRows); err != nil {
		t.Fatal(err)
	}
	if dispatchedRows != 1 {
		t.Fatalf("manager inputs after same-output retry=%d, want 1", dispatchedRows)
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM production_phase_outputs WHERE project_id=$1 AND invocation_id=$2`, projectID, invocationID).Scan(&outputRows); err != nil {
		t.Fatal(err)
	}
	if outputRows != 1 {
		t.Fatalf("production_phase_outputs after graph-advanced retry=%d, want 1", outputRows)
	}
	var frozenOutputRef string
	if err := db.QueryRowContext(ctx, `SELECT output_ref FROM production_phase_outputs WHERE project_id=$1 AND invocation_id=$2`, projectID, invocationID).Scan(&frozenOutputRef); err != nil {
		t.Fatal(err)
	}
	toctouPayload, _ := json.Marshal(struct {
		OutputRef string                 `json:"output_ref"`
		Receipt   phasepkg.OutputReceipt `json:"receipt"`
	}{frozenOutputRef, receipt})
	toctouInput := productionInput{
		Kind: "phase_output", RequestID: requestID, ConversationID: "runtime:task-real",
		Body: "phase output plan " + frozenOutputRef, Payload: toctouPayload, SeenRevision: 999,
		SelectedEndpoint: &receipt.Endpoint, TargetKind: "phase_output", TargetRef: frozenOutputRef,
	}
	if err := (productionPhaseTerminalOutbox{db: db, projectID: projectID, ingress: ingress, now: time.Now}).promoteAndDeliver(ctx, productionPhaseTerminalDelivery{
		Input: toctouInput, InvocationID: receipt.InvocationID, CommandID: receipt.CommandID,
		CommandAction: receipt.CommandAction, Endpoint: receipt.Endpoint, Generation: receipt.Generation, IntentOutput: output,
	}); err != nil {
		t.Fatalf("promoteAndDeliver after delivered with new seen revision: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM production_manager_inputs WHERE project_id=$1 AND input_kind='phase_output' AND request_id=$2`, projectID, requestID).Scan(&dispatchedRows); err != nil {
		t.Fatal(err)
	}
	if dispatchedRows != 1 {
		t.Fatalf("manager inputs after TOCTOU promote retry=%d, want 1", dispatchedRows)
	}

	conflictingOutput := phasepkg.PhaseOutput{Phase: string(coordination.EndpointPlan), ReportRef: "art_report_output_intent_conflict"}
	conflictingRequestID, err := rebuilt.persistOutputIntent(ctx, invocationID, conflictingOutput)
	if err != nil {
		t.Fatalf("persistOutputIntent conflicting output: %v", err)
	}
	if conflictingRequestID == requestID {
		t.Fatalf("conflicting output reused request id %s", conflictingRequestID)
	}
	if err := rebuilt.abandonOutputIntent(ctx, invocationID, conflictingRequestID, conflictingOutput, kernel.IdempotencyConflict()); err != nil {
		t.Fatalf("abandon conflicting output: %v", err)
	}
	assertPhaseTerminalObligationRequest(t, ctx, db, projectID, "phase_output", conflictingRequestID, "abandoned")
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM production_manager_inputs WHERE project_id=$1 AND input_kind='phase_output'`, projectID).Scan(&dispatchedRows); err != nil {
		t.Fatal(err)
	}
	if dispatchedRows != 1 {
		t.Fatalf("manager inputs after conflicting output=%d, want 1", dispatchedRows)
	}
}

func TestProductionPhaseOutputIntentAbandonsDeterministicRejectAgainstPostgres(t *testing.T) {
	databaseURL := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("THREADMILL_TEST_DATABASE_URL is required for the real PostgreSQL integration test")
	}
	ctx := context.Background()
	db := openProductionPhasePostgres(t, ctx, databaseURL)
	defer db.Close()

	projectID := kernel.ProjectID("project-phase-output-abandon")
	graph := coordination.NewPostgresStore(db)
	seedProductionPhaseTask(t, ctx, db, projectID, graph)
	ingress, err := newProductionIngress(db, projectID, "room-phase-output-abandon", productionPhaseTestAssembler(t), graph, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := ingress.setDispatcher(&recordingProductionDispatcher{}); err != nil {
		t.Fatal(err)
	}
	invocations := runtimepkg.NewPostgresInvocationStoreFromSQL(db)
	runtime := &productionPhaseRuntime{
		db: db, projectID: projectID, graph: graph, ingress: ingress,
		invocations: invocations, artifacts: evidence.NewPostgresArtifactRegistry(db, objectstore.NewMemoryStore(), "artifacts"), now: time.Now,
	}
	if _, err := runtime.persistOutputIntent(ctx, "inv-missing", phasepkg.PhaseOutput{Phase: string(coordination.EndpointPlan)}); !kernel.IsCode(err, kernel.CodeStaleCommand) {
		t.Fatalf("persistOutputIntent missing invocation = %v, want stale_command", err)
	}
	var obligationRows int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM production_phase_terminal_obligations WHERE project_id=$1`, projectID).Scan(&obligationRows); err != nil {
		t.Fatal(err)
	}
	if obligationRows != 0 {
		t.Fatalf("obligations after missing invocation=%d, want 0", obligationRows)
	}
	otherProjectID := kernel.ProjectID("project-phase-output-abandon-other")
	otherInvocationID := kernel.InvocationID("inv-cross-project-output")
	created := time.Now().UTC()
	if err := invocations.Create(ctx, runtimepkg.Invocation{
		ID: otherInvocationID, ActorPrincipalID: "actor-cross-project", ProjectID: otherProjectID,
		TaskID: "task-other", EndpointID: coordination.EndpointPlan, Generation: 1, Role: auth.RolePlanner,
		Status: runtimepkg.InvocationRunning, BindingRef: "binding://task-other/plan/1", LeaseID: "lease-cross-project-output",
		PromptHashes: map[string]string{"prompt": "hash"}, SkillHashes: map[string]string{"skill": "hash"},
		EffectiveTools: []auth.Tool{auth.ToolAgentSubmitPhaseOutput}, CreatedAt: created, ExpiresAt: created.Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.persistOutputIntent(ctx, otherInvocationID, phasepkg.PhaseOutput{Phase: string(coordination.EndpointPlan)}); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("persistOutputIntent cross-project invocation = %v, want forbidden", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM production_phase_terminal_obligations WHERE project_id=$1`, projectID).Scan(&obligationRows); err != nil {
		t.Fatal(err)
	}
	if obligationRows != 0 {
		t.Fatalf("obligations after cross-project invocation=%d, want 0", obligationRows)
	}

	command := coordination.PhaseCommand{
		ID: "cmd-output-abandon", Endpoint: coordination.PhaseEndpointRef{TaskID: "task-real", EndpointID: coordination.EndpointPlan},
		Generation: 1, BindingRef: "binding://task-real/plan/1", LeaseRef: "lease-output-abandon", Action: coordination.CommandStart,
	}
	invocationID := productionPhaseInvocationID(command.ID)
	if err := invocations.Create(ctx, runtimepkg.Invocation{
		ID: invocationID, ActorPrincipalID: "actor-output-abandon", ProjectID: projectID,
		TaskID: command.Endpoint.TaskID, EndpointID: command.Endpoint.EndpointID, Generation: uint64(command.Generation), Role: auth.RolePlanner,
		Status: runtimepkg.InvocationRunning, BindingRef: command.BindingRef, LeaseID: command.LeaseRef,
		PromptHashes: map[string]string{"prompt": "hash"}, SkillHashes: map[string]string{"skill": "hash"},
		EffectiveTools: []auth.Tool{auth.ToolAgentSubmitPhaseOutput}, CreatedAt: created, ExpiresAt: created.Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	requestID, err := runtime.persistOutputIntent(ctx, invocationID, phasepkg.PhaseOutput{Phase: string(coordination.EndpointPlan)})
	if err != nil {
		t.Fatalf("persistOutputIntent: %v", err)
	}
	assertPhaseTerminalObligation(t, ctx, db, projectID, "phase_output", "intent")
	if err := runtime.abandonOutputIntent(ctx, invocationID, requestID, phasepkg.PhaseOutput{Phase: string(coordination.EndpointPlan)}, kernel.InvalidArgument("invalid phase output")); err != nil {
		t.Fatalf("abandonOutputIntent: %v", err)
	}
	assertPhaseTerminalObligation(t, ctx, db, projectID, "phase_output", "abandoned")
	if err := runtime.ReplayTerminalDeliveries(ctx); err != nil {
		t.Fatalf("ReplayTerminalDeliveries abandoned intent: %v", err)
	}
	assertPhaseTerminalObligation(t, ctx, db, projectID, "phase_output", "abandoned")
	var managerRows int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM production_manager_inputs WHERE project_id=$1 AND input_kind='phase_output'`, projectID).Scan(&managerRows); err != nil {
		t.Fatal(err)
	}
	if managerRows != 0 {
		t.Fatalf("manager inputs after abandoned intent=%d, want 0", managerRows)
	}
}

func TestProductionPhaseOutputIntentReplayAbandonsCrossProjectIntentAgainstPostgres(t *testing.T) {
	databaseURL := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("THREADMILL_TEST_DATABASE_URL is required for the real PostgreSQL integration test")
	}
	ctx := context.Background()
	db := openProductionPhasePostgres(t, ctx, databaseURL)
	defer db.Close()

	projectID := kernel.ProjectID("project-phase-output-cross-replay")
	otherProjectID := kernel.ProjectID("project-phase-output-cross-replay-other")
	graph := coordination.NewPostgresStore(db)
	seedProductionPhaseTask(t, ctx, db, projectID, graph)
	ingress, err := newProductionIngress(db, projectID, "room-phase-output-cross-replay", productionPhaseTestAssembler(t), graph, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := ingress.setDispatcher(&recordingProductionDispatcher{}); err != nil {
		t.Fatal(err)
	}
	invocations := runtimepkg.NewPostgresInvocationStoreFromSQL(db)
	invocationID := kernel.InvocationID("inv-cross-project-replay")
	created := time.Now().UTC()
	if err := invocations.Create(ctx, runtimepkg.Invocation{
		ID: invocationID, ActorPrincipalID: "actor-cross-replay", ProjectID: otherProjectID,
		TaskID: "task-other", EndpointID: coordination.EndpointPlan, Generation: 1, Role: auth.RolePlanner,
		Status: runtimepkg.InvocationRunning, BindingRef: "binding://task-other/plan/1", LeaseID: "lease-cross-project-replay",
		PromptHashes: map[string]string{"prompt": "hash"}, SkillHashes: map[string]string{"skill": "hash"},
		EffectiveTools: []auth.Tool{auth.ToolAgentSubmitPhaseOutput}, CreatedAt: created, ExpiresAt: created.Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	output := phasepkg.PhaseOutput{Phase: string(coordination.EndpointPlan)}
	payload, err := json.Marshal(productionPhaseOutputIntent{InvocationID: invocationID, Output: output})
	if err != nil {
		t.Fatal(err)
	}
	requestID := stableProductionSuffix(invocationID, "phase_output", hashProductionBytes(payload))
	input := productionInput{
		Kind: "phase_output", RequestID: requestID, ConversationID: "runtime:task-other",
		Body: "phase output intent " + string(invocationID), Payload: payload, SeenRevision: 1,
		SelectedEndpoint: &coordination.PhaseEndpointRef{TaskID: "task-other", EndpointID: coordination.EndpointPlan},
		TargetKind:       "phase_output", TargetRef: "",
	}
	outbox := productionPhaseTerminalOutbox{db: db, projectID: projectID, ingress: ingress, now: time.Now}
	if err := outbox.enqueueIntent(ctx, productionPhaseTerminalDelivery{
		Input: input, InvocationID: invocationID,
		Endpoint:   coordination.PhaseEndpointRef{TaskID: "task-other", EndpointID: coordination.EndpointPlan},
		Generation: 1, CommandAction: coordination.CommandStart,
	}); err != nil {
		t.Fatalf("seed cross-project output intent: %v", err)
	}
	runtime := &productionPhaseRuntime{
		db: db, projectID: projectID, graph: graph, ingress: ingress,
		invocations: invocations, artifacts: evidence.NewPostgresArtifactRegistry(db, objectstore.NewMemoryStore(), "artifacts"), now: time.Now,
	}
	if err := runtime.ReplayTerminalDeliveries(ctx); err != nil {
		t.Fatalf("ReplayTerminalDeliveries cross-project intent: %v", err)
	}
	assertPhaseTerminalObligationRequest(t, ctx, db, projectID, "phase_output", requestID, "abandoned")
	var managerRows int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM production_manager_inputs WHERE project_id=$1 AND input_kind='phase_output'`, projectID).Scan(&managerRows); err != nil {
		t.Fatal(err)
	}
	if managerRows != 0 {
		t.Fatalf("manager inputs after cross-project replay=%d, want 0", managerRows)
	}
}

func TestProductionPhaseTerminalOutboxRejectsIdentityConflictAgainstPostgres(t *testing.T) {
	databaseURL := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("THREADMILL_TEST_DATABASE_URL is required for the real PostgreSQL integration test")
	}
	ctx := context.Background()
	db := openProductionPhasePostgres(t, ctx, databaseURL)
	defer db.Close()

	projectID := kernel.ProjectID("project-phase-outbox-conflict")
	graph := coordination.NewPostgresStore(db)
	seedProductionPhaseTask(t, ctx, db, projectID, graph)
	ingress, err := newProductionIngress(db, projectID, "room-phase-outbox-conflict", productionPhaseTestAssembler(t), graph, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := ingress.setDispatcher(&recordingProductionDispatcher{}); err != nil {
		t.Fatal(err)
	}
	endpoint := coordination.PhaseEndpointRef{TaskID: "task-real", EndpointID: coordination.EndpointPlan}
	payload := []byte(`{"output_ref":"art_conflict","receipt":{"ok":true}}`)
	input := productionInput{
		Kind: "phase_output", RequestID: "request-conflict", ConversationID: "runtime:task-real",
		Body: "phase output conflict", Payload: payload, SeenRevision: 1, SelectedEndpoint: &endpoint,
		TargetKind: "phase_output", TargetRef: "art_conflict",
	}
	outbox := productionPhaseTerminalOutbox{db: db, projectID: projectID, ingress: ingress, now: time.Now}
	delivery := productionPhaseTerminalDelivery{
		Input: input, InvocationID: "inv-conflict", CommandID: "cmd-conflict",
		CommandAction: coordination.CommandStart, Endpoint: endpoint, Generation: 1,
	}
	if _, err := outbox.enqueue(ctx, delivery); err != nil {
		t.Fatalf("enqueue initial: %v", err)
	}
	conflict := delivery
	conflict.Generation = 2
	if _, err := outbox.enqueue(ctx, conflict); !kernel.IsCode(err, kernel.CodeIdempotencyConflict) {
		t.Fatalf("enqueue conflict = %v, want idempotency_conflict", err)
	}
}

func TestProductionPhaseInputsKeepMissingArtifactKindsPendingAgainstPostgres(t *testing.T) {
	databaseURL := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("THREADMILL_TEST_DATABASE_URL is required for the real PostgreSQL integration test")
	}
	ctx := context.Background()
	db := openProductionPhasePostgres(t, ctx, databaseURL)
	defer db.Close()

	projectID := kernel.ProjectID("project-phase-missing-kind")
	graph := coordination.NewPostgresStore(db)
	seedProductionPhaseCrossTaskGraphWithEdge(t, ctx, db, projectID, graph, coordination.RequiredByCompletion)
	source := productionPhaseTestSource(t, ctx, db, projectID, graph)
	snapshot, outputRef := seedSatisfiedSourceOutput(t, ctx, db, projectID, graph, nil)
	inputs, err := source.inputs(ctx, snapshot, coordination.PhaseCommand{
		Endpoint:   coordination.PhaseEndpointRef{TaskID: "task-real", EndpointID: coordination.EndpointExecute},
		Generation: 1, BindingRef: "binding://task-real/execute/1", Action: coordination.CommandStart,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(inputs.Delivered) != 0 || len(inputs.Pending) != 1 {
		t.Fatalf("missing %s artifact kind inputs = %#v, want pending only", outputRef, inputs)
	}

	projectStart := kernel.ProjectID("project-phase-missing-start-kind")
	graphStart := coordination.NewPostgresStore(db)
	seedProductionPhaseCrossTaskGraphWithEdge(t, ctx, db, projectStart, graphStart, coordination.RequiredByStart)
	sourceStart := productionPhaseTestSource(t, ctx, db, projectStart, graphStart)
	_, _ = seedSatisfiedSourceOutput(t, ctx, db, projectStart, graphStart, nil)
	command := coordination.PhaseCommand{
		ID: "cmd:run:task-real:execute:1", Endpoint: coordination.PhaseEndpointRef{TaskID: "task-real", EndpointID: coordination.EndpointExecute},
		Generation: 1, BindingRef: "binding://task-real/execute/1", LeaseRef: "lease-execute-missing-kind", Action: coordination.CommandStart,
	}
	if _, _, err := sourceStart.ResolvePhaseBinding(ctx, phasepkg.BindingResolveRequest{Command: command, InvocationID: productionPhaseInvocationID(command.ID)}); !kernel.IsCode(err, kernel.CodeTransitionRejected) {
		t.Fatalf("ResolvePhaseBinding missing start artifact = %v, want transition_rejected", err)
	}
}

func TestProductionPhaseAwaitInputsTerminalReasonsAgainstPostgres(t *testing.T) {
	databaseURL := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("THREADMILL_TEST_DATABASE_URL is required for the real PostgreSQL integration test")
	}
	ctx := context.Background()
	db := openProductionPhasePostgres(t, ctx, databaseURL)
	defer db.Close()

	projectID := kernel.ProjectID("project-phase-await-terminal")
	graph := coordination.NewPostgresStore(db)
	seedProductionPhaseCrossTaskGraphWithEdge(t, ctx, db, projectID, graph, coordination.RequiredByCompletion)
	source := productionPhaseTestSource(t, ctx, db, projectID, graph)
	active := phasepkg.ActiveInvocation{
		Invocation: runtimepkg.Invocation{ID: "inv-await", ProjectID: projectID, TaskID: "task-real", EndpointID: coordination.EndpointExecute, ExpiresAt: time.Now().Add(time.Hour)},
		Command: coordination.PhaseCommand{
			ID: "cmd-await", Endpoint: coordination.PhaseEndpointRef{TaskID: "task-real", EndpointID: coordination.EndpointExecute},
			Generation: 1, BindingRef: "binding://task-real/execute/1", Action: coordination.CommandStart,
		},
		Inputs: phasepkg.PhaseInputSet{InputRevision: "inputs-before"},
	}
	inputRuntime := productionPhaseInputs{source: source}

	expired := active
	expired.Invocation.ExpiresAt = time.Now().Add(-time.Minute)
	if got, done, err := inputRuntime.read(ctx, expired, phasepkg.AwaitInputsRequest{InputIDs: []string{"missing"}}); err != nil || !done || got.TerminalReason != "lease_expired" {
		t.Fatalf("lease expired result=%#v done=%v err=%v", got, done, err)
	}

	projectStale := kernel.ProjectID("project-phase-await-stale")
	graphStale := coordination.NewPostgresStore(db)
	seedProductionPhaseCrossTaskGraphWithEdge(t, ctx, db, projectStale, graphStale, coordination.RequiredByCompletion)
	sourceStale := productionPhaseTestSource(t, ctx, db, projectStale, graphStale)
	staleActive := active
	staleActive.Invocation.ProjectID = projectStale
	staleSnapshot, err := graphStale.Latest(ctx, projectStale)
	if err != nil {
		t.Fatal(err)
	}
	staleInputs, err := sourceStale.inputs(ctx, staleSnapshot, staleActive.Command)
	if err != nil {
		t.Fatal(err)
	}
	staleActive.Inputs = staleInputs
	_, _ = seedSatisfiedSourceOutput(t, ctx, db, projectStale, graphStale, nil)
	inputRuntime = productionPhaseInputs{source: sourceStale}
	if got, done, err := inputRuntime.read(ctx, staleActive, phasepkg.AwaitInputsRequest{}); err != nil || !done || got.TerminalReason != "input_stale" {
		t.Fatalf("input stale result=%#v done=%v err=%v", got, done, err)
	}

	snapshot, err := graph.Latest(ctx, projectID)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err = graph.Transition(ctx, projectID, snapshot.Revision, coordination.GraphTransition{
		TargetKind: coordination.TargetPhaseEndpoint, Endpoint: coordination.PhaseEndpointRef{TaskID: "task-source", EndpointID: coordination.EndpointPlan},
		Action: string(coordination.EndpointSubmitted), Generation: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = graph.Transition(ctx, projectID, snapshot.Revision, coordination.GraphTransition{
		TargetKind: coordination.TargetPhaseEndpoint, Endpoint: coordination.PhaseEndpointRef{TaskID: "task-source", EndpointID: coordination.EndpointPlan},
		Action: string(coordination.EndpointRejected), Generation: 1,
		Result: coordination.PhaseResult{
			ID: "result-rejected", Endpoint: coordination.PhaseEndpointRef{TaskID: "task-source", EndpointID: coordination.EndpointPlan},
			BindingRef: "binding://task-source/plan/1", OutputRef: "output-rejected", Verdict: coordination.VerdictRejected,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, done, err := inputRuntime.read(ctx, active, phasepkg.AwaitInputsRequest{InputIDs: []string{"phase_satisfied:task-source:plan->task-real:execute"}}); err != nil || !done || got.TerminalReason != "source_failed" {
		t.Fatalf("source failed result=%#v done=%v err=%v", got, done, err)
	}

	projectCancel := kernel.ProjectID("project-phase-await-cancel")
	graphCancel := coordination.NewPostgresStore(db)
	seedProductionPhaseCrossTaskGraphWithEdge(t, ctx, db, projectCancel, graphCancel, coordination.RequiredByCompletion)
	sourceCancel := productionPhaseTestSource(t, ctx, db, projectCancel, graphCancel)
	cancelActive := active
	cancelActive.Invocation.ProjectID = projectCancel
	inputRuntime = productionPhaseInputs{source: sourceCancel}
	cancelSnapshot, err := graphCancel.Latest(ctx, projectCancel)
	if err != nil {
		t.Fatal(err)
	}
	_, err = graphCancel.Transition(ctx, projectCancel, cancelSnapshot.Revision, coordination.GraphTransition{TargetKind: coordination.TargetTask, TaskID: "task-source", Action: string(coordination.TaskCanceled)})
	if err != nil {
		t.Fatal(err)
	}
	if got, done, err := inputRuntime.read(ctx, cancelActive, phasepkg.AwaitInputsRequest{InputIDs: []string{"phase_satisfied:task-source:plan->task-real:execute"}}); err != nil || !done || got.TerminalReason != "source_cancelled" {
		t.Fatalf("source cancelled result=%#v done=%v err=%v", got, done, err)
	}
}

func TestBuildProductionPhaseSeamsUsesRealComponents(t *testing.T) {
	databaseURL := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("THREADMILL_TEST_DATABASE_URL is required for the real PostgreSQL integration test")
	}
	ctx := context.Background()
	db := openProductionPhasePostgres(t, ctx, databaseURL)
	defer db.Close()
	projectID := kernel.ProjectID("project-phase-seams")
	graph := coordination.NewPostgresStore(db)
	available := map[auth.Tool]struct{}{}
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
	ingress, err := newProductionIngress(db, projectID, "room-phase-real", assembler, graph, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := ingress.setDispatcher(&productionPhaseFakeAdapter{}); err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{
		ProjectID: string(projectID), RepositoryPath: seedProductionPhaseBareRepo(t), WorktreeParent: t.TempDir(),
		ObjectStoreBucket: "artifacts", AgentTeamsRoomID: "room-phase-real",
	}
	seams, err := buildProductionPhaseSeams(productionPhaseBundleOptions{
		Config: cfg, DB: db, ProjectID: projectID, Graph: graph, Assembler: assembler,
		Adapter: &productionPhaseFakeAdapter{}, Ingress: ingress, ObjectStore: objectstore.NewMemoryStore(),
	})
	if err != nil {
		t.Fatalf("buildProductionPhaseSeams() error = %v", err)
	}
	if seams.Controller == nil || seams.Runtime == nil || seams.Orchestration == nil || seams.Readiness == nil || seams.TaskWorkspaces == nil || seams.TaskContexts == nil {
		t.Fatalf("seams not fully configured: %#v", seams)
	}
}

func seedProductionPhaseTask(t *testing.T, ctx context.Context, db *sql.DB, projectID kernel.ProjectID, graph *coordination.PostgresStore) {
	t.Helper()
	contract := taskmanager.TaskContract{
		TaskID: "task-real", ContractRef: "contract://task-real", DeliveryPolicy: taskmanager.DeliveryPolicyCodeMerge,
		PhaseSpecs: map[coordination.EndpointID]string{
			coordination.EndpointPlan: "spec://task-real/plan", coordination.EndpointExecute: "spec://task-real/execute", coordination.EndpointVerify: "spec://task-real/verify",
		},
	}
	if err := taskmanager.NewPostgresStore(db, projectID, graph).PersistRequirementContract(ctx, taskmanager.RequirementInput{
		InputRef: "input://task-real", TaskID: "task-real", ContractRef: contract.ContractRef,
		Requirement: taskmanager.Requirement{Text: "build real phase"},
	}, contract); err != nil {
		t.Fatalf("persist contract: %v", err)
	}
	_, err := graph.ReplacePending(ctx, projectID, coordination.PendingSubgraph{
		RequestID: "seed-phase-task", BaseRevision: 1,
		Tasks: []coordination.Task{{ID: "task-real", ContractRef: contract.ContractRef, Outcome: coordination.TaskActive}},
		Endpoints: []coordination.PhaseEndpoint{
			{Ref: coordination.PhaseEndpointRef{TaskID: "task-real", EndpointID: coordination.EndpointPlan}, SpecRef: contract.PhaseSpecs[coordination.EndpointPlan], BindingRef: "binding://task-real/plan/1", Generation: 1, State: coordination.EndpointPending, RunPolicy: coordination.RunEnabled},
			{Ref: coordination.PhaseEndpointRef{TaskID: "task-real", EndpointID: coordination.EndpointExecute}, SpecRef: contract.PhaseSpecs[coordination.EndpointExecute], BindingRef: "binding://task-real/execute/1", Generation: 1, State: coordination.EndpointPending, RunPolicy: coordination.RunEnabled},
			{Ref: coordination.PhaseEndpointRef{TaskID: "task-real", EndpointID: coordination.EndpointVerify}, SpecRef: contract.PhaseSpecs[coordination.EndpointVerify], BindingRef: "binding://task-real/verify/1", Generation: 1, State: coordination.EndpointPending, RunPolicy: coordination.RunEnabled},
		},
	})
	if err != nil {
		t.Fatalf("seed graph: %v", err)
	}
}

func seedProductionPhaseCrossTaskGraph(t *testing.T, ctx context.Context, db *sql.DB, projectID kernel.ProjectID, graph *coordination.PostgresStore) {
	t.Helper()
	seedProductionPhaseCrossTaskGraphWithEdge(t, ctx, db, projectID, graph, coordination.RequiredByCompletion)
}

func seedProductionPhaseCrossTaskGraphWithEdge(t *testing.T, ctx context.Context, db *sql.DB, projectID kernel.ProjectID, graph *coordination.PostgresStore, requiredBy coordination.RequiredBy) {
	t.Helper()
	store := taskmanager.NewPostgresStore(db, projectID, graph)
	contracts := []taskmanager.TaskContract{
		{
			TaskID: "task-source", ContractRef: "contract://task-source", DeliveryPolicy: taskmanager.DeliveryPolicyCodeMerge,
			PhaseSpecs: map[coordination.EndpointID]string{
				coordination.EndpointPlan: "spec://task-source/plan", coordination.EndpointExecute: "spec://task-source/execute", coordination.EndpointVerify: "spec://task-source/verify",
			},
		},
		{
			TaskID: "task-real", ContractRef: "contract://task-real", DeliveryPolicy: taskmanager.DeliveryPolicyCodeMerge,
			PhaseSpecs: map[coordination.EndpointID]string{
				coordination.EndpointPlan: "spec://task-real/plan", coordination.EndpointExecute: "spec://task-real/execute", coordination.EndpointVerify: "spec://task-real/verify",
			},
		},
	}
	for _, contract := range contracts {
		if err := store.PersistRequirementContract(ctx, taskmanager.RequirementInput{
			InputRef: "input://" + string(contract.TaskID), TaskID: contract.TaskID, ContractRef: contract.ContractRef,
			Requirement: taskmanager.Requirement{Text: "build " + string(contract.TaskID)},
		}, contract); err != nil {
			t.Fatalf("persist contract %s: %v", contract.TaskID, err)
		}
	}
	_, err := graph.ReplacePending(ctx, projectID, coordination.PendingSubgraph{
		RequestID: "seed-cross-task-phase", BaseRevision: 1,
		Tasks: []coordination.Task{
			{ID: "task-source", ContractRef: "contract://task-source", Outcome: coordination.TaskActive},
			{ID: "task-real", ContractRef: "contract://task-real", Outcome: coordination.TaskActive},
		},
		Endpoints: []coordination.PhaseEndpoint{
			{Ref: coordination.PhaseEndpointRef{TaskID: "task-source", EndpointID: coordination.EndpointPlan}, SpecRef: "spec://task-source/plan", BindingRef: "binding://task-source/plan/1", Generation: 1, State: coordination.EndpointPending, RunPolicy: coordination.RunEnabled},
			{Ref: coordination.PhaseEndpointRef{TaskID: "task-source", EndpointID: coordination.EndpointExecute}, SpecRef: "spec://task-source/execute", BindingRef: "binding://task-source/execute/1", Generation: 1, State: coordination.EndpointPending, RunPolicy: coordination.RunEnabled},
			{Ref: coordination.PhaseEndpointRef{TaskID: "task-source", EndpointID: coordination.EndpointVerify}, SpecRef: "spec://task-source/verify", BindingRef: "binding://task-source/verify/1", Generation: 1, State: coordination.EndpointPending, RunPolicy: coordination.RunEnabled},
			{Ref: coordination.PhaseEndpointRef{TaskID: "task-real", EndpointID: coordination.EndpointPlan}, SpecRef: "spec://task-real/plan", BindingRef: "binding://task-real/plan/1", Generation: 1, State: coordination.EndpointPending, RunPolicy: coordination.RunEnabled},
			{Ref: coordination.PhaseEndpointRef{TaskID: "task-real", EndpointID: coordination.EndpointExecute}, SpecRef: "spec://task-real/execute", BindingRef: "binding://task-real/execute/1", Generation: 1, State: coordination.EndpointPending, RunPolicy: coordination.RunEnabled},
			{Ref: coordination.PhaseEndpointRef{TaskID: "task-real", EndpointID: coordination.EndpointVerify}, SpecRef: "spec://task-real/verify", BindingRef: "binding://task-real/verify/1", Generation: 1, State: coordination.EndpointPending, RunPolicy: coordination.RunEnabled},
		},
		Edges: []coordination.Edge{{
			From:   coordination.PhaseEndpointRef{TaskID: "task-source", EndpointID: coordination.EndpointPlan},
			To:     coordination.PhaseEndpointRef{TaskID: "task-real", EndpointID: coordination.EndpointExecute},
			Signal: coordination.SignalPhaseSatisfied, RequiredBy: requiredBy,
			ArtifactKinds: []string{"phase_output", string(evidence.ArtifactGeneratedReport)}, OnFalse: coordination.OnFalseBlock,
		}},
	})
	if err != nil {
		t.Fatalf("seed cross-task graph: %v", err)
	}
}

func productionPhaseTestAssembler(t *testing.T) *runtimepkg.Assembler {
	t.Helper()
	available := map[auth.Tool]struct{}{}
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
	return assembler
}

func productionPhaseTestSource(t *testing.T, ctx context.Context, db *sql.DB, projectID kernel.ProjectID, graph *coordination.PostgresStore) *productionPhaseBindingSource {
	t.Helper()
	contexts := contextgraph.NewPostgresStore(db, time.Now)
	contexts.SetTaskEndpointResolver(productionTaskEndpointResolver{projectID: projectID, graph: graph})
	return &productionPhaseBindingSource{
		db: db, projectID: projectID, graph: graph, contracts: taskmanager.NewPostgresStore(db, projectID, graph),
		workspaces: workspace.NewPostgresService(db), contexts: contexts,
		repositoryPath: seedProductionPhaseBareRepo(t), worktreeParent: t.TempDir(), now: time.Now,
	}
}

func seedSatisfiedSourceOutput(t *testing.T, ctx context.Context, db *sql.DB, projectID kernel.ProjectID, graph *coordination.PostgresStore, artifactRefs []string) (coordination.GraphSnapshot, string) {
	t.Helper()
	if artifactRefs == nil {
		artifactRefs = []string{}
	}
	created := time.Now().UTC()
	invocation := runtimepkg.Invocation{
		ID:               kernel.InvocationID("inv-source-plan-" + stableProductionSuffix(projectID, strings.Join(artifactRefs, ","))),
		ActorPrincipalID: "actor-source-plan", ProjectID: projectID,
		TaskID: "task-source", EndpointID: coordination.EndpointPlan, Generation: 1, Role: auth.RolePlanner,
		Status: runtimepkg.InvocationCompleted, BindingRef: "binding://task-source/plan/1", LeaseID: kernel.LeaseID("lease-source-plan-" + stableProductionSuffix(projectID)),
		PromptHashes: map[string]string{"prompt": "hash"}, SkillHashes: map[string]string{"skill": "hash"},
		EffectiveTools: []auth.Tool{auth.ToolAgentSubmitPhaseOutput}, CreatedAt: created, ExpiresAt: created.Add(time.Hour),
	}
	if err := runtimepkg.NewPostgresInvocationStoreFromSQL(db).Create(ctx, invocation); err != nil {
		t.Fatal(err)
	}
	output := phasepkg.PhaseOutput{Phase: string(coordination.EndpointPlan)}
	if len(artifactRefs) > 0 {
		output.ReportRef = artifactRefs[0]
	}
	outputJSON, _ := json.Marshal(output)
	artifactRefsJSON, _ := json.Marshal(artifactRefs)
	outputRef := "output://task-source/plan/" + stableProductionSuffix(projectID, strings.Join(artifactRefs, ","))
	if _, err := db.ExecContext(ctx, `
INSERT INTO production_phase_outputs (
  output_ref, project_id, task_id, endpoint_id, generation, binding_ref, lease_ref,
  invocation_id, input_revision, output, artifact_refs
) VALUES ($1,$2,'task-source','plan',1,'binding://task-source/plan/1','lease-source-plan',$3,'inputs-1',$4::jsonb,$5::jsonb)`,
		outputRef, projectID, invocation.ID, outputJSON, artifactRefsJSON); err != nil {
		t.Fatal(err)
	}
	snapshot, err := graph.Latest(ctx, projectID)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err = graph.Transition(ctx, projectID, snapshot.Revision, coordination.GraphTransition{
		TargetKind: coordination.TargetPhaseEndpoint, Endpoint: coordination.PhaseEndpointRef{TaskID: "task-source", EndpointID: coordination.EndpointPlan},
		Action: string(coordination.EndpointSubmitted), Generation: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err = graph.Transition(ctx, projectID, snapshot.Revision, coordination.GraphTransition{
		TargetKind: coordination.TargetPhaseEndpoint, Endpoint: coordination.PhaseEndpointRef{TaskID: "task-source", EndpointID: coordination.EndpointPlan},
		Action: string(coordination.EndpointSatisfied), Generation: 1,
		Result: coordination.PhaseResult{
			ID: "result-source-plan", Endpoint: coordination.PhaseEndpointRef{TaskID: "task-source", EndpointID: coordination.EndpointPlan},
			BindingRef: "binding://task-source/plan/1", OutputRef: outputRef, Verdict: coordination.VerdictSatisfied,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return snapshot, outputRef
}

func assertPhaseTerminalObligation(t *testing.T, ctx context.Context, db *sql.DB, projectID kernel.ProjectID, kind, wantStatus string) {
	t.Helper()
	var status string
	var attempts int
	if err := db.QueryRowContext(ctx, `SELECT status, attempts FROM production_phase_terminal_obligations WHERE project_id=$1 AND input_kind=$2`, projectID, kind).Scan(&status, &attempts); err != nil {
		t.Fatal(err)
	}
	if status != wantStatus {
		t.Fatalf("%s obligation status=%s attempts=%d, want %s", kind, status, attempts, wantStatus)
	}
}

func assertPhaseTerminalObligationRequest(t *testing.T, ctx context.Context, db *sql.DB, projectID kernel.ProjectID, kind, requestID, wantStatus string) {
	t.Helper()
	var status string
	var attempts int
	if err := db.QueryRowContext(ctx, `SELECT status, attempts FROM production_phase_terminal_obligations WHERE project_id=$1 AND input_kind=$2 AND request_id=$3`, projectID, kind, requestID).Scan(&status, &attempts); err != nil {
		t.Fatal(err)
	}
	if status != wantStatus {
		t.Fatalf("%s/%s obligation status=%s attempts=%d, want %s", kind, requestID, status, attempts, wantStatus)
	}
}

func openProductionPhasePostgres(t *testing.T, ctx context.Context, databaseURL string) *sql.DB {
	t.Helper()
	admin, err := postgres.Open(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	t.Cleanup(func() { _ = admin.Close(context.Background()) })
	schema := fmt.Sprintf("tm_production_phase_%d", time.Now().UnixNano())
	if _, err := admin.SQL().ExecContext(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.SQL().ExecContext(context.Background(), `DROP SCHEMA IF EXISTS `+schema+` CASCADE`)
	})
	scopedURL, err := productionPhaseDatabaseURLWithSearchPath(databaseURL, schema)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("pgx", scopedURL)
	if err != nil {
		t.Fatalf("open scoped postgres: %v", err)
	}
	db.SetMaxOpenConns(12)
	db.SetMaxIdleConns(12)
	loaded, err := postgres.LoadMigrations(migrations.FS, migrations.Dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := postgres.NewMigrator(db).Apply(ctx, loaded); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	return db
}

func productionPhaseDatabaseURLWithSearchPath(databaseURL, schema string) (string, error) {
	parsed, err := url.Parse(databaseURL)
	if err != nil {
		return "", err
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func seedProductionPhaseBareRepo(t *testing.T) string {
	t.Helper()
	work := filepath.Join(t.TempDir(), "seed")
	if err := os.MkdirAll(work, 0o755); err != nil {
		t.Fatal(err)
	}
	gitPhaseTest(t, work, "init")
	if err := os.WriteFile(filepath.Join(work, "README.md"), []byte("seed\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitPhaseTest(t, work, "add", "README.md")
	gitPhaseTest(t, work, "-c", "user.email=test@example.com", "-c", "user.name=Test", "commit", "-m", "seed")
	bare := filepath.Join(t.TempDir(), "repo.git")
	gitPhaseTest(t, work, "clone", "--bare", work, bare)
	return bare
}

func gitPhaseTest(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v in %s: %v\n%s", args, dir, err, out)
	}
}

func productionPhaseInvocationID(commandID string) kernel.InvocationID {
	sum := sha256.Sum256([]byte(commandID))
	return kernel.InvocationID(fmt.Sprintf("inv_%x", sum[:8]))
}

type productionPhaseFakeAdapter struct{}

func (a *productionPhaseFakeAdapter) Dispatch(_ context.Context, ref string) (agentteams.AgentTeamsExecutionRef, error) {
	invocationID := strings.TrimPrefix(ref, "threadmill://phase-invocation/")
	if cut := strings.IndexByte(invocationID, '/'); cut >= 0 {
		invocationID = invocationID[:cut]
	}
	if invocationID == ref {
		invocationID = ref
	}
	return agentteams.AgentTeamsExecutionRef{InvocationID: kernel.InvocationID(invocationID), AgentTeamsTaskID: "phase-task", HostRef: "phase-host"}, nil
}

func (a *productionPhaseFakeAdapter) Terminate(context.Context, agentteams.AgentTeamsExecutionRef, string) error {
	return nil
}

func (a *productionPhaseFakeAdapter) Collect(context.Context, agentteams.AgentTeamsExecutionRef) (agentteams.UntrustedExecutionResult, error) {
	return agentteams.UntrustedExecutionResult{}, nil
}

func (a *productionPhaseFakeAdapter) Observe(context.Context, string) ([]agentteams.ExecutionObservation, error) {
	return nil, nil
}

type failingProductionPhaseContextRuntime struct{}

func (failingProductionPhaseContextRuntime) EnsureInitialSlice(context.Context, auth.Principal, []string) (contextgraph.ContextSlice, error) {
	return contextgraph.ContextSlice{}, kernel.InvalidArgument("forced context failure")
}

func (failingProductionPhaseContextRuntime) InspectSubscriptions(context.Context, auth.Principal, kernel.InvocationID) ([]contextgraph.SubscriptionInspection, error) {
	return nil, nil
}

func (failingProductionPhaseContextRuntime) MaterializeRuntimeContext(context.Context, auth.Principal) (contextgraph.ContextSlice, error) {
	return contextgraph.ContextSlice{}, nil
}

func (failingProductionPhaseContextRuntime) ListTaskCandidates(context.Context, auth.Principal) (contextgraph.TaskMemoryBufferView, error) {
	return contextgraph.TaskMemoryBufferView{}, nil
}

func (failingProductionPhaseContextRuntime) EndInvocation(context.Context, auth.Principal, kernel.InvocationID) error {
	return nil
}
