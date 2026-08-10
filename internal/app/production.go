package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/contextagent"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/contextgraph"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/evidence"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/config"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/objectstore"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/postgres"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime/phase"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/scheduler"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/taskmanager"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/transport/httpapi"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/transport/mcpapi"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/transport/webui"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/uiprojection"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/workspace"
)

const productionOperatorID kernel.ActorPrincipalID = "operator://local"

type productionHost struct {
	db     *postgres.DB
	api    http.Handler
	mcp    http.Handler
	web    http.Handler
	secret string
}

func newProductionHost(ctx context.Context, cfg config.Config) (*productionHost, error) {
	if err := runMigrations(ctx, cfg); err != nil {
		return nil, err
	}
	db, err := postgres.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, err
	}
	host, err := buildProductionHost(ctx, cfg, db)
	if err != nil {
		_ = db.Close(context.Background())
		return nil, err
	}
	return host, nil
}

func buildProductionHost(ctx context.Context, cfg config.Config, db *postgres.DB) (*productionHost, error) {
	projectID := kernel.ProjectID(cfg.ProjectID)
	if err := kernel.RequireID("project_id", projectID); err != nil {
		return nil, err
	}
	sqlDB := db.SQL()
	authenticator := auth.NewAuthenticator(auth.NewPostgresStore(sqlDB), time.Now)
	sessionSecret, _, err := authenticator.IssueOperatorSession(ctx, productionOperatorID, []kernel.ProjectID{projectID}, 24*time.Hour)
	if err != nil {
		return nil, fmt.Errorf("issue local operator session: %w", err)
	}
	if _, err := scheduler.NewPostgresCapacityLedger(sqlDB, projectID).Ensure(ctx, 1, 1); err != nil {
		return nil, err
	}
	if _, err := scheduler.NewPostgresBudgetLedger(sqlDB, projectID).Ensure(ctx, scheduler.BudgetPolicy{VerifyLevel: scheduler.VerifyStandard, ExplorationLevel: scheduler.ExplorationTargeted}, scheduler.BudgetStatus{}); err != nil {
		return nil, err
	}

	objectStore, err := objectstore.NewMinIOStore(objectstore.MinIOConfig{
		Endpoint:  cfg.ObjectStoreEndpoint,
		AccessKey: cfg.ObjectStoreAccessKey,
		SecretKey: cfg.ObjectStoreSecretKey,
		Bucket:    cfg.ObjectStoreBucket,
		Secure:    cfg.ObjectStoreSecure,
	})
	if err != nil {
		return nil, err
	}
	artifactRegistry := evidence.NewPostgresArtifactRegistry(sqlDB, objectStore, cfg.ObjectStoreBucket)
	eventStore := evidence.NewPostgresEventStore(sqlDB, 1<<20)
	permissions := allowProjectPermission{projectID: projectID}
	events := uiprojection.NewEventLogQuery(eventStore, permissions)
	coordStore := coordination.NewPostgresStore(sqlDB)
	contextStore := contextgraph.NewPostgresStore(sqlDB, time.Now)
	invocations := runtime.NewPostgresInvocationStoreFromSQL(sqlDB)
	capacity := productionCapacity{projectID: projectID, ledger: scheduler.NewPostgresCapacityLedger(sqlDB, projectID), db: sqlDB}
	ui := uiprojection.NewService(capacity, coordStore, productionInvocations{db: sqlDB}, productionContextInspector{db: sqlDB, contexts: contextStore}, events, permissions)
	query := productionQuery{projectID: projectID, graph: coordStore, ui: ui, invocations: productionInvocations{db: sqlDB}, contracts: taskmanager.NewPostgresStore(sqlDB, projectID, coordStore)}
	manager := disabledManagerPort{projectID: projectID}
	api := httpapi.New(httpapi.Options{
		Authenticator: authenticator,
		CSRFGuard:     noopStateGuard{},
		Requirements:  disabledRequirementPort{},
		Capacity:      capacity,
		Human:         disabledHumanPort{},
		Manager:       manager,
		Query:         query,
		Readiness:     productionReadiness{db: db},
		Events:        events,
	}).Handler()
	registry, err := mcpapi.NewRegistry(mcpapi.AllRuntimeToolSpecs(mcpapi.RuntimeToolDependencies{
		ContextReader:      contextStore,
		ContextRetrieve:    productionContextRetrieve{agent: contextagent.Agent{Searcher: contextStore}},
		ContextCurator:     contextStore,
		ContextSearcher:    contextStore,
		ContextReviewer:    contextStore,
		TaskMemoryReader:   contextStore,
		CandidateSubmitter: contextStore,
		TaskContextWriter:  contextStore,
		MemoryFinalizer:    contextStore,
		Phase:              disabledPhaseRuntime{},
		Requirement:        disabledRequirementRuntime{},
		Orchestration:      disabledOrchestrationRuntime{},
		TaskManager:        disabledTaskManagerRuntime{},
		Workspace:          disabledWorkspacePort{},
		Evidence:           artifactRegistry,
	})...)
	if err != nil {
		return nil, err
	}
	mcp, err := mcpapi.NewHTTPHandler(authenticator, registry, mcpapi.HTTPOptions{ServerVersion: "production"})
	if err != nil {
		return nil, err
	}
	web, err := webui.New(webui.Options{DistDir: cfg.WebDistDir})
	if err != nil {
		return nil, err
	}
	_ = invocations
	return &productionHost{db: db, api: api, mcp: mcp, web: web.Handler(), secret: sessionSecret}, nil
}

func (h *productionHost) Handler() http.Handler {
	return productionCookieMiddleware(h.secret, routeProductionHTTP(h.api, h.mcp, h.web))
}

func (h *productionHost) Close(ctx context.Context) error {
	if h == nil || h.db == nil {
		return nil
	}
	return h.db.Close(ctx)
}

func routeProductionHTTP(api, mcp, web http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/mcp" {
			mcp.ServeHTTP(w, r)
			return
		}
		if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" || r.URL.Path == "/v1" || strings.HasPrefix(r.URL.Path, "/v1/") {
			api.ServeHTTP(w, r)
			return
		}
		web.ServeHTTP(w, r)
	})
}

func productionCookieMiddleware(secret string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: auth.SessionCookieName, Value: secret, Path: "/", HttpOnly: true, SameSite: http.SameSiteLaxMode})
		if _, err := r.Cookie(auth.SessionCookieName); err != nil {
			r.AddCookie(&http.Cookie{Name: auth.SessionCookieName, Value: secret})
		}
		next.ServeHTTP(w, r)
	})
}

type allowProjectPermission struct{ projectID kernel.ProjectID }

func (p allowProjectPermission) CanReadProject(_ context.Context, principal auth.Principal, projectID kernel.ProjectID) (bool, error) {
	return principal.ProjectID == p.projectID && projectID == p.projectID, nil
}

func (p allowProjectPermission) TaskGrant(_ context.Context, principal auth.Principal, projectID kernel.ProjectID, taskID kernel.TaskID) (uiprojection.TaskReadGrant, error) {
	if principal.ProjectID != p.projectID || projectID != p.projectID || taskID == "" {
		return uiprojection.TaskReadGrant{}, nil
	}
	return uiprojection.TaskReadGrant{Visible: true, ContextBodies: true, CandidateBodies: true}, nil
}

type productionCapacity struct {
	projectID kernel.ProjectID
	ledger    *scheduler.PostgresCapacityLedger
	db        *sql.DB
}

func (p productionCapacity) ReadCapacity(ctx context.Context, projectID kernel.ProjectID) (uiprojection.CapacityRecord, error) {
	if projectID != p.projectID {
		return uiprojection.CapacityRecord{}, kernel.Forbidden("capacity project mismatch")
	}
	capacity, err := p.ledger.Snapshot(ctx)
	if err != nil {
		return uiprojection.CapacityRecord{}, err
	}
	var updatedAt time.Time
	if err := p.db.QueryRowContext(ctx, `SELECT updated_at FROM scheduler_capacity_ledger WHERE project_id = $1`, projectID).Scan(&updatedAt); err != nil {
		return uiprojection.CapacityRecord{}, err
	}
	return uiprojection.CapacityRecord{Capacity: capacity, UpdatedAt: updatedAt}, nil
}

func (p productionCapacity) GetCapacity(ctx context.Context, principal auth.Principal, projectID kernel.ProjectID) (httpapi.CapacityState, error) {
	record, err := p.ReadCapacity(ctx, projectID)
	if err != nil {
		return httpapi.CapacityState{}, err
	}
	return httpapi.CapacityState{ProjectID: projectID, Revision: record.Capacity.Revision, DesiredConcurrency: record.Capacity.Desired, HealthyCapacity: record.Capacity.Healthy, ActiveInvocations: record.Capacity.Active, UpdatedAt: record.UpdatedAt}, nil
}

func (p productionCapacity) AdjustCapacity(ctx context.Context, principal auth.Principal, req httpapi.CapacityAdjustmentRequest) (httpapi.CapacityAdjustmentResponse, error) {
	if req.ProjectID != p.projectID || principal.ProjectID != p.projectID {
		return httpapi.CapacityAdjustmentResponse{}, kernel.Forbidden("capacity project mismatch")
	}
	capacity, err := p.ledger.SetDesired(ctx, req.ExpectedRevision, req.DesiredConcurrency)
	if err != nil {
		return httpapi.CapacityAdjustmentResponse{}, err
	}
	record, err := p.ReadCapacity(ctx, req.ProjectID)
	if err != nil {
		return httpapi.CapacityAdjustmentResponse{}, err
	}
	state := httpapi.CapacityState{ProjectID: req.ProjectID, Revision: capacity.Revision, DesiredConcurrency: capacity.Desired, HealthyCapacity: capacity.Healthy, ActiveInvocations: capacity.Active, UpdatedAt: record.UpdatedAt}
	return httpapi.CapacityAdjustmentResponse{CommandRef: "capacity://" + req.RequestID, Capacity: state}, nil
}

type productionInvocations struct{ db *sql.DB }

func (p productionInvocations) ListInvocations(ctx context.Context, filter uiprojection.InvocationFilter) ([]runtime.Invocation, error) {
	rows, err := p.db.QueryContext(ctx, `
SELECT invocation_id, actor_principal_id, project_id, COALESCE(task_id, ''), COALESCE(endpoint_id, ''),
       COALESCE(generation, 0), role, COALESCE(operation, ''), status, COALESCE(binding_ref, ''),
       COALESCE(lease_id, ''), COALESCE(workspace_ref, ''), COALESCE(context_slice_ref, ''),
       COALESCE(task_memory_buffer_ref, ''), COALESCE(consumer_invocation_id, ''),
       COALESCE(consumer_task_id, ''), COALESCE(consumer_role, ''), prompt_hashes, skill_hashes,
       effective_tools, created_at, expires_at
FROM runtime_invocations
WHERE ($1 = '' OR project_id = $1)
  AND ($2 = '' OR task_id = $2)
  AND ($3 = '' OR endpoint_id = $3)
  AND ($4 = 0 OR generation = $4)
ORDER BY created_at, invocation_id`, filter.ProjectID, filter.TaskID, filter.EndpointID, int64(filter.Generation))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []runtime.Invocation
	for rows.Next() {
		invocation, err := scanProductionInvocation(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, invocation)
	}
	return out, rows.Err()
}

type productionContextInspector struct {
	db       *sql.DB
	contexts *contextgraph.PostgresStore
}

func (p productionContextInspector) InspectInvocation(ctx context.Context, principal auth.Principal, invocation runtime.Invocation) (uiprojection.ContextInspection, error) {
	agent := auth.Principal{
		ActorPrincipalID: invocation.ActorPrincipalID,
		Kind:             auth.PrincipalAgent,
		ProjectID:        invocation.ProjectID,
		Role:             invocation.Role,
		Operation:        invocation.Operation,
		TaskID:           invocation.TaskID,
		InvocationID:     invocation.ID,
		Tools: auth.ToolSet(
			auth.ToolContextListSubgraphs,
			auth.ToolContextExplore,
			auth.ToolContextSubscribe,
			auth.ToolContextUnsubscribe,
			auth.ToolAgentListTaskMemoryCandidates,
		),
		AuthenticatedAt: time.Now(),
	}
	subs, err := p.contexts.InspectSubscriptions(ctx, principal, invocation.ID)
	if err != nil {
		return uiprojection.ContextInspection{}, err
	}
	slice, err := p.contexts.MaterializeRuntimeContext(ctx, agent)
	if err != nil {
		return uiprojection.ContextInspection{}, err
	}
	candidates, err := p.candidates(ctx, invocation)
	if err != nil {
		return uiprojection.ContextInspection{}, err
	}
	return uiprojection.ContextInspection{Subscriptions: subs, Slice: slice, Frontier: slice.SubscriptionIDs, Candidates: candidates}, nil
}

func (p productionContextInspector) candidates(ctx context.Context, invocation runtime.Invocation) ([]uiprojection.CandidateInspectionRecord, error) {
	rows, err := p.db.QueryContext(ctx, `
SELECT candidate_id, candidate::text
FROM context_task_memory_candidates
WHERE project_id = $1 AND task_id = $2 AND created_by_invocation_id = $3
ORDER BY candidate_id`, invocation.ProjectID, invocation.TaskID, invocation.ID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []uiprojection.CandidateInspectionRecord
	for rows.Next() {
		var id string
		var raw string
		if err := rows.Scan(&id, &raw); err != nil {
			return nil, err
		}
		var candidate contextgraph.MemoryCandidate
		if err := json.Unmarshal([]byte(raw), &candidate); err != nil {
			return nil, err
		}
		records = append(records, uiprojection.CandidateInspectionRecord{ProjectID: invocation.ProjectID, TaskID: invocation.TaskID, CreatedByInvocationID: invocation.ID, View: contextgraph.TaskMemoryCandidateView{CandidateID: id, Candidate: candidate}})
	}
	return records, rows.Err()
}

type productionQuery struct {
	projectID   kernel.ProjectID
	graph       *coordination.PostgresStore
	ui          *uiprojection.Service
	invocations productionInvocations
	contracts   *taskmanager.PostgresStore
}

func (q productionQuery) ProjectSnapshot(ctx context.Context, principal auth.Principal, projectID kernel.ProjectID, revision kernel.Revision) (httpapi.CoordinationSnapshot, error) {
	return q.ui.Snapshot(ctx, principal, projectID, revision)
}

func (q productionQuery) InspectEndpoint(ctx context.Context, principal auth.Principal, ref coordination.PhaseEndpointRef, generation int) (httpapi.EndpointInspector, error) {
	return q.ui.InspectEndpoint(ctx, principal, q.projectID, ref, generation)
}

func (q productionQuery) Task(ctx context.Context, principal auth.Principal, taskID kernel.TaskID) (httpapi.TaskProjection, error) {
	snapshot, err := q.ui.Snapshot(ctx, principal, q.projectID, kernel.LatestRevision)
	if err != nil {
		return httpapi.TaskProjection{}, err
	}
	graph, err := q.graph.Latest(ctx, q.projectID)
	if err != nil {
		return httpapi.TaskProjection{}, err
	}
	var task coordination.Task
	for _, candidate := range graph.Tasks {
		if candidate.ID == taskID {
			task = candidate
			break
		}
	}
	if task.ID == "" {
		return httpapi.TaskProjection{}, kernel.Error{Code: kernel.CodeNotFound, Message: "task not found"}
	}
	invocations, err := q.invocations.ListInvocations(ctx, uiprojection.InvocationFilter{ProjectID: q.projectID, TaskID: taskID})
	if err != nil {
		return httpapi.TaskProjection{}, err
	}
	endpoints := make([]httpapi.EndpointProjection, 0, 3)
	for _, endpoint := range graph.Endpoints {
		if endpoint.Ref.TaskID != taskID {
			continue
		}
		endpoints = append(endpoints, httpapi.EndpointProjection{TaskID: endpoint.Ref.TaskID, EndpointID: endpoint.Ref.EndpointID, Generation: endpoint.Generation, State: string(endpoint.State), RunPolicy: string(endpoint.RunPolicy), BindingRef: string(endpoint.BindingRef), LatestInvocationRef: latestProductionInvocation(invocations, endpoint.Ref, endpoint.Generation)})
	}
	sort.Slice(endpoints, func(i, j int) bool { return endpoints[i].EndpointID < endpoints[j].EndpointID })
	status := "pending"
	for _, summary := range snapshot.Tasks {
		if summary.TaskID == taskID {
			status = summary.Status
		}
	}
	policy := string(taskmanager.DeliveryPolicyCodeMerge)
	if contract, err := q.contracts.TaskContract(ctx, taskID); err == nil && contract.DeliveryPolicy != "" {
		policy = string(contract.DeliveryPolicy)
	}
	return httpapi.TaskProjection{TaskID: taskID, ProjectID: q.projectID, Status: status, GraphRevision: graph.Revision, ContractRef: task.ContractRef, DeliveryPolicy: policy, Endpoints: endpoints}, nil
}

type productionReadiness struct{ db *postgres.DB }

func (p productionReadiness) Readiness(ctx context.Context) httpapi.ReadinessStatus {
	status := httpapi.ReadinessStatus{Status: "ready", Dependencies: []httpapi.DependencyReadiness{{Name: "postgres", Status: "ready"}}}
	if err := p.db.Ping(ctx); err != nil {
		status.Status = "not_ready"
		status.Dependencies[0].Status = "not_ready"
		status.Dependencies[0].Message = err.Error()
	}
	return status
}

type noopStateGuard struct{}

func (noopStateGuard) Check(*http.Request, auth.SessionRecord) error { return nil }

type disabledRequirementPort struct{}

func (disabledRequirementPort) SubmitRequirement(context.Context, auth.Principal, httpapi.RequirementCreateRequest) (httpapi.RequirementCreateResponse, error) {
	return httpapi.RequirementCreateResponse{}, kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "requirement runtime is not configured", Recoverable: true}
}

type disabledHumanPort struct{}

func (disabledHumanPort) SubmitHumanDecision(context.Context, auth.Principal, httpapi.HumanDecisionRequest) (httpapi.HumanDecisionResponse, error) {
	return httpapi.HumanDecisionResponse{}, kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "human decision runtime is not configured", Recoverable: true}
}

type disabledManagerPort struct{ projectID kernel.ProjectID }

func (p disabledManagerPort) SubmitManagerMessage(context.Context, auth.Principal, httpapi.ManagerMessageRequest) (httpapi.ManagerMessageResponse, error) {
	return httpapi.ManagerMessageResponse{}, kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "task manager agent runtime is not configured", Recoverable: true}
}

func (p disabledManagerPort) Conversation(context.Context, auth.Principal, string, string) (httpapi.ManagerConversation, error) {
	return httpapi.ManagerConversation{ProjectID: p.projectID, Messages: []httpapi.ManagerConversationEntry{}}, nil
}

func latestProductionInvocation(invocations []runtime.Invocation, ref coordination.PhaseEndpointRef, generation int) kernel.InvocationID {
	var latest runtime.Invocation
	for _, invocation := range invocations {
		if invocation.TaskID == ref.TaskID && invocation.EndpointID == ref.EndpointID && invocation.Generation == uint64(generation) {
			latest = invocation
		}
	}
	return latest.ID
}

func scanProductionInvocation(row interface{ Scan(...any) error }) (runtime.Invocation, error) {
	var invocation runtime.Invocation
	var generation int64
	var promptHashes, skillHashes, effectiveTools []byte
	if err := row.Scan(&invocation.ID, &invocation.ActorPrincipalID, &invocation.ProjectID, &invocation.TaskID, &invocation.EndpointID, &generation, &invocation.Role, &invocation.Operation, &invocation.Status, &invocation.BindingRef, &invocation.LeaseID, &invocation.WorkspaceRef, &invocation.ContextSliceRef, &invocation.TaskMemoryBufferRef, &invocation.ConsumerInvocationID, &invocation.ConsumerTaskID, &invocation.ConsumerRole, &promptHashes, &skillHashes, &effectiveTools, &invocation.CreatedAt, &invocation.ExpiresAt); err != nil {
		return runtime.Invocation{}, err
	}
	invocation.Generation = uint64(generation)
	if len(promptHashes) > 0 {
		_ = json.Unmarshal(promptHashes, &invocation.PromptHashes)
	}
	if len(skillHashes) > 0 {
		_ = json.Unmarshal(skillHashes, &invocation.SkillHashes)
	}
	if len(effectiveTools) > 0 {
		_ = json.Unmarshal(effectiveTools, &invocation.EffectiveTools)
	}
	return invocation, nil
}

type disabledPhaseRuntime struct{}
type disabledRequirementRuntime struct{}
type disabledOrchestrationRuntime struct{}
type disabledTaskManagerRuntime struct{}
type disabledWorkspacePort struct{}

var errRuntimeNotConfigured = kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "runtime port is not configured", Recoverable: true}

func (disabledPhaseRuntime) AwaitInputs(context.Context, kernel.InvocationID, phase.AwaitInputsRequest) (phase.InputWaitResult, error) {
	return phase.InputWaitResult{}, errRuntimeNotConfigured
}

func (disabledPhaseRuntime) SubmitPhaseOutput(context.Context, kernel.InvocationID, phase.PhaseOutput) (phase.OutputReceipt, error) {
	return phase.OutputReceipt{}, errRuntimeNotConfigured
}

func (disabledRequirementRuntime) SubmitRequirement(context.Context, auth.Principal, taskmanager.Requirement) (any, error) {
	return nil, errRuntimeNotConfigured
}

func (disabledOrchestrationRuntime) SubmitOrchestrationIntent(context.Context, auth.Principal, auth.BoundScope, phase.OrchestrationIntent) (phase.OrchestrationProposal, error) {
	return phase.OrchestrationProposal{}, errRuntimeNotConfigured
}

func (disabledTaskManagerRuntime) Snapshot(context.Context, auth.Principal, auth.BoundScope, kernel.Revision) (coordination.GraphSnapshot, error) {
	return coordination.GraphSnapshot{}, errRuntimeNotConfigured
}

func (disabledTaskManagerRuntime) SubmitTaskManagerDecision(context.Context, auth.Principal, auth.BoundScope, taskmanager.TaskManagerDecision) (string, error) {
	return "", errRuntimeNotConfigured
}

func (disabledTaskManagerRuntime) ReplacePending(context.Context, auth.Principal, auth.BoundScope, mcpapi.PendingSubgraphIntent) (kernel.Revision, error) {
	return 0, errRuntimeNotConfigured
}

func (disabledTaskManagerRuntime) Transition(context.Context, auth.Principal, auth.BoundScope) (kernel.Revision, error) {
	return 0, errRuntimeNotConfigured
}

func (disabledWorkspacePort) List(context.Context, kernel.InvocationID, workspace.PathRequest) (workspace.ListResult, error) {
	return workspace.ListResult{}, errRuntimeNotConfigured
}

func (disabledWorkspacePort) Read(context.Context, kernel.InvocationID, workspace.PathRequest) (workspace.ReadResult, error) {
	return workspace.ReadResult{}, errRuntimeNotConfigured
}

func (disabledWorkspacePort) WritePlan(context.Context, kernel.InvocationID, workspace.WriteRequest) (workspace.WriteResult, error) {
	return workspace.WriteResult{}, errRuntimeNotConfigured
}

func (disabledWorkspacePort) Write(context.Context, kernel.InvocationID, workspace.WriteRequest) (workspace.WriteResult, error) {
	return workspace.WriteResult{}, errRuntimeNotConfigured
}

func (disabledWorkspacePort) Run(context.Context, kernel.InvocationID, workspace.RunRequest) (workspace.RunResult, error) {
	return workspace.RunResult{}, errRuntimeNotConfigured
}

func (disabledWorkspacePort) Diff(context.Context, kernel.InvocationID, workspace.PathRequest) (workspace.DiffResult, error) {
	return workspace.DiffResult{}, errRuntimeNotConfigured
}

type productionContextRetrieve struct{ agent contextagent.Agent }

func (d productionContextRetrieve) RetrieveForConsumer(ctx context.Context, caller auth.Principal, req contextagent.ContextRetrieveRequest) (contextagent.ContextRetrieveResult, error) {
	contextPrincipal := auth.Principal{
		ActorPrincipalID:     "context-agent://production-retrieve",
		Kind:                 auth.PrincipalAgent,
		ProjectID:            caller.ProjectID,
		Role:                 auth.RoleContext,
		Operation:            "retrieve",
		TaskID:               caller.TaskID,
		InvocationID:         kernel.InvocationID("context-retrieve://" + string(caller.InvocationID)),
		ConsumerInvocationID: caller.InvocationID,
		ConsumerTaskID:       caller.TaskID,
		ConsumerRole:         caller.Role,
		Tools:                auth.ToolSet(auth.ToolContextSearch),
		AuthenticatedAt:      time.Now(),
	}
	return d.agent.Retrieve(ctx, contextPrincipal, req)
}
