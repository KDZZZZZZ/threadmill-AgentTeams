package app

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
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
			Signal: coordination.SignalPhaseSatisfied, RequiredBy: coordination.RequiredByCompletion,
			ArtifactKinds: []string{"phase_output", string(evidence.ArtifactGeneratedReport)}, OnFalse: coordination.OnFalseBlock,
		}},
	})
	if err != nil {
		t.Fatalf("seed cross-task graph: %v", err)
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
