package fakehost

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/contextgraph"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/evidence"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/scheduler"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/taskmanager"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/transport/httpapi"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/transport/webui"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/uiprojection"
)

const (
	DemoProjectID kernel.ProjectID        = "demo-project"
	sessionSecret                         = "fakehost-operator-session"
	operatorID    kernel.ActorPrincipalID = "operator-demo"
)

type Options struct {
	ProjectID  kernel.ProjectID
	WebDistDir string
	Now        func() time.Time
}

type App struct {
	projectID kernel.ProjectID
	handler   http.Handler
	state     *state
	close     func() error
}

func New(ctx context.Context, options Options) (*App, error) {
	projectID := options.ProjectID
	if projectID == "" {
		projectID = DemoProjectID
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}

	graphStore := coordination.NewMemoryStore()
	decisionLog := coordination.NewMemoryDecisionLog()
	taskManagerPrincipal := auth.Principal{
		ActorPrincipalID: "fake-task-manager",
		Kind:             auth.PrincipalAgent,
		ProjectID:        projectID,
		Role:             auth.RoleTaskManager,
		InvocationID:     "invocation://fake-task-manager/bootstrap",
		Tools: auth.ToolSet(
			auth.ToolCoordinationSnapshot,
			auth.ToolTaskManagerSubmitDecision,
			auth.ToolCoordinationReplacePending,
			auth.ToolCoordinationTransition,
		),
		AuthenticatedAt: now(),
	}
	graph := coordination.NewTaskManagerGraph(taskManagerPrincipal, graphStore, decisionLog, kernel.NewMemoryIdempotencyStore())
	decisions := newDecisionStore(projectID, decisionLog)
	eventLog := evidence.NewEventLog(64 * 1024)
	permissions := permissionReader{projectID: projectID}
	events := uiprojection.NewEventLogQuery(eventLog, permissions)
	st := &state{
		projectID:     projectID,
		now:           now,
		graphStore:    graphStore,
		graph:         graph,
		decisions:     decisions,
		capacity:      scheduler.NewCapacityLedger(4, 2),
		events:        events,
		conversations: make(map[string][]httpapi.ManagerConversationEntry),
		contexts:      make(map[kernel.InvocationID]uiprojection.ContextInspection),
	}
	st.manager = taskmanager.NewManager(taskmanager.Options{
		ProjectID: projectID,
		Graph:     graph,
		Decisions: decisions,
		Replies:   st,
	})
	if err := st.bootstrap(ctx); err != nil {
		return nil, err
	}

	ui := uiprojection.NewService(st, graphStore, st, st, events, permissions)
	api := httpapi.New(httpapi.Options{
		Authenticator: fixedAuthenticator{projectID: projectID, now: now},
		CSRFGuard:     noopCSRFGuard{},
		Requirements:  st,
		Capacity:      st,
		Human:         st,
		Manager:       st,
		Query:         queryPort{projectID: projectID, graph: graphStore, ui: ui, invocations: st},
		Readiness:     readinessPort{},
		Events:        events,
	}).Handler()
	web, cleanup, err := webUIServer(options.WebDistDir)
	if err != nil {
		return nil, err
	}
	handler := fixedCookieMiddleware(projectID, sameOrigin(api, web.Handler()))
	return &App{projectID: projectID, handler: handler, state: st, close: cleanup}, nil
}

func (a *App) Handler() http.Handler { return a.handler }

func (a *App) ProjectID() kernel.ProjectID { return a.projectID }

func (a *App) Close() error {
	if a.close == nil {
		return nil
	}
	return a.close()
}

type state struct {
	mu            sync.Mutex
	projectID     kernel.ProjectID
	now           func() time.Time
	graphStore    *coordination.MemoryStore
	graph         coordination.TaskManagerGraph
	decisions     *decisionStore
	manager       *taskmanager.Manager
	capacity      *scheduler.CapacityLedger
	events        *uiprojection.EventLogQuery
	invocations   []runtime.Invocation
	conversations map[string][]httpapi.ManagerConversationEntry
	contexts      map[kernel.InvocationID]uiprojection.ContextInspection
}

func (s *state) bootstrap(ctx context.Context) error {
	snapshot, err := s.graph.Snapshot(ctx, kernel.LatestRevision)
	if err != nil {
		return err
	}
	ref := kernel.IdempotencyKey("decision://fakehost/bootstrap/demo-project")
	if err := s.decisions.log.RegisterReplacePending(s.projectID, ref); err != nil {
		return err
	}
	_, err = s.graph.ReplacePending(ctx, coordination.PendingSubgraph{
		RequestID:    ref,
		BaseRevision: snapshot.Revision,
		Tasks: []coordination.Task{
			{ID: "task-alpha", ContractRef: "contract://demo/task-alpha", Outcome: coordination.TaskActive},
			{ID: "task-beta", ContractRef: "contract://demo/task-beta", Outcome: coordination.TaskActive},
		},
		Endpoints: append(taskEndpoints("task-alpha"), taskEndpoints("task-beta")...),
	})
	if err != nil {
		return err
	}
	if _, err := s.createInvocation(ctx, "task-alpha", coordination.EndpointExecute, 1, runtime.InvocationRunning); err != nil {
		return err
	}
	if _, err := s.createInvocation(ctx, "task-alpha", coordination.EndpointPlan, 1, runtime.InvocationCompleted); err != nil {
		return err
	}
	return s.appendGraphRevision(ctx, "bootstrap", string(ref), 0)
}

func taskEndpoints(taskID kernel.TaskID) []coordination.PhaseEndpoint {
	return []coordination.PhaseEndpoint{
		phaseEndpoint(taskID, coordination.EndpointPlan),
		phaseEndpoint(taskID, coordination.EndpointExecute),
		phaseEndpoint(taskID, coordination.EndpointVerify),
	}
}

func phaseEndpoint(taskID kernel.TaskID, endpointID coordination.EndpointID) coordination.PhaseEndpoint {
	return coordination.PhaseEndpoint{
		Ref:        coordination.PhaseEndpointRef{TaskID: taskID, EndpointID: endpointID},
		SpecRef:    fmt.Sprintf("spec://demo/%s/%s", taskID, endpointID),
		BindingRef: kernel.BindingRef(fmt.Sprintf("binding://demo/%s/%s/1", taskID, endpointID)),
		Generation: 1,
		State:      coordination.EndpointPending,
		RunPolicy:  coordination.RunEnabled,
	}
}

func sameOrigin(api http.Handler, web http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if directGraphMutationPath(r) {
			http.NotFound(w, r)
			return
		}
		if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" || r.URL.Path == "/v1" || strings.HasPrefix(r.URL.Path, "/v1/") {
			api.ServeHTTP(w, r)
			return
		}
		web.ServeHTTP(w, r)
	})
}

func directGraphMutationPath(r *http.Request) bool {
	if r.Method == http.MethodGet {
		return false
	}
	if r.URL.Path == "/v1/coordination/snapshot" {
		return true
	}
	if strings.HasPrefix(r.URL.Path, "/v1/coordination/endpoints/") && !strings.HasSuffix(r.URL.Path, "/inspector") {
		return true
	}
	return false
}

func webUIServer(dist string) (*webui.Server, func() error, error) {
	cleanup := func() error { return nil }
	if strings.TrimSpace(dist) == "" {
		dir, err := os.MkdirTemp("", "threadmill-fakehost-web-*")
		if err != nil {
			return nil, cleanup, err
		}
		cleanup = func() error { return os.RemoveAll(dir) }
		dist = dir
		if err := os.WriteFile(filepath.Join(dist, "index.html"), []byte("<!doctype html><html><body><div id=\"root\">Threadmill fake host</div></body></html>"), 0600); err != nil {
			_ = cleanup()
			return nil, func() error { return nil }, err
		}
	}
	web, err := webui.New(webui.Options{DistDir: dist})
	if err != nil {
		_ = cleanup()
		return nil, func() error { return nil }, err
	}
	return web, cleanup, nil
}

func fixedCookieMiddleware(projectID kernel.ProjectID, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: auth.SessionCookieName, Value: sessionSecret, Path: "/", HttpOnly: true, SameSite: http.SameSiteLaxMode})
		if _, err := r.Cookie(auth.SessionCookieName); err != nil {
			r.AddCookie(&http.Cookie{Name: auth.SessionCookieName, Value: sessionSecret})
		}
		next.ServeHTTP(w, r)
	})
}

type fixedAuthenticator struct {
	projectID kernel.ProjectID
	now       func() time.Time
}

func (a fixedAuthenticator) AuthenticateOperatorSession(_ context.Context, secret string, projectID kernel.ProjectID) (auth.Principal, auth.SessionRecord, error) {
	if secret != sessionSecret {
		return auth.Principal{}, auth.SessionRecord{}, kernel.Error{Code: kernel.CodeUnauthorized, Message: "fake operator cookie is required"}
	}
	if projectID != a.projectID {
		return auth.Principal{}, auth.SessionRecord{}, kernel.Forbidden("operator is not allowed for project")
	}
	return auth.Principal{ActorPrincipalID: operatorID, Kind: auth.PrincipalOperator, ProjectID: projectID, Role: auth.RoleOperator, AuthenticatedAt: a.now()}, auth.SessionRecord{}, nil
}

type noopCSRFGuard struct{}

func (noopCSRFGuard) Check(*http.Request, auth.SessionRecord) error { return nil }

type readinessPort struct{}

func (readinessPort) Readiness(context.Context) httpapi.ReadinessStatus {
	return httpapi.ReadinessStatus{Status: "ready", Dependencies: []httpapi.DependencyReadiness{
		{Name: "coordination.memory_store", Status: "ready"},
		{Name: "evidence.event_log", Status: "ready"},
		{Name: "fake_operator_auth", Status: "ready"},
	}}
}

type permissionReader struct{ projectID kernel.ProjectID }

func (p permissionReader) CanReadProject(_ context.Context, principal auth.Principal, projectID kernel.ProjectID) (bool, error) {
	return principal.Kind == auth.PrincipalOperator && principal.Role == auth.RoleOperator && principal.ProjectID == projectID && projectID == p.projectID, nil
}

func (p permissionReader) TaskGrant(context.Context, auth.Principal, kernel.ProjectID, kernel.TaskID) (uiprojection.TaskReadGrant, error) {
	return uiprojection.TaskReadGrant{Visible: true, ContextBodies: true, CandidateBodies: true}, nil
}

func (s *state) ReadCapacity(_ context.Context, projectID kernel.ProjectID) (uiprojection.CapacityRecord, error) {
	if projectID != s.projectID {
		return uiprojection.CapacityRecord{}, kernel.Forbidden("project not found")
	}
	return uiprojection.CapacityRecord{Capacity: s.capacity.Snapshot(), UpdatedAt: s.now().UTC()}, nil
}

func (s *state) GetCapacity(ctx context.Context, _ auth.Principal, projectID kernel.ProjectID) (httpapi.CapacityState, error) {
	record, err := s.ReadCapacity(ctx, projectID)
	if err != nil {
		return httpapi.CapacityState{}, err
	}
	return capacityState(projectID, record, s.waitingInvocations()), nil
}

func (s *state) AdjustCapacity(ctx context.Context, _ auth.Principal, req httpapi.CapacityAdjustmentRequest) (httpapi.CapacityAdjustmentResponse, error) {
	capacity, err := s.capacity.SetDesired(ctx, req.ExpectedRevision, req.DesiredConcurrency)
	if err != nil {
		return httpapi.CapacityAdjustmentResponse{}, err
	}
	state := capacityState(req.ProjectID, uiprojection.CapacityRecord{Capacity: capacity, UpdatedAt: s.now().UTC()}, s.waitingInvocations())
	_, err = s.events.Append(ctx, evidence.AppendEvent{
		StableKey: kernel.IdempotencyKey("capacity:" + req.RequestID),
		Type:      "capacity.updated",
		ProjectID: req.ProjectID,
		Payload:   state,
	})
	if err != nil {
		return httpapi.CapacityAdjustmentResponse{}, err
	}
	return httpapi.CapacityAdjustmentResponse{CommandRef: "capacity-command://" + req.RequestID, Capacity: state}, nil
}

func capacityState(projectID kernel.ProjectID, record uiprojection.CapacityRecord, waiting int) httpapi.CapacityState {
	return httpapi.CapacityState{
		ProjectID:          projectID,
		Revision:           record.Capacity.Revision,
		DesiredConcurrency: record.Capacity.Desired,
		HealthyCapacity:    record.Capacity.Healthy,
		ActiveInvocations:  record.Capacity.Active,
		WaitingInvocations: waiting,
		UpdatedAt:          record.UpdatedAt,
	}
}

func (s *state) waitingInvocations() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	waiting := 0
	for _, invocation := range s.invocations {
		if invocation.Status == runtime.InvocationWaiting {
			waiting++
		}
	}
	return waiting
}

func (s *state) SubmitManagerMessage(ctx context.Context, _ auth.Principal, req httpapi.ManagerMessageRequest) (httpapi.ManagerMessageResponse, error) {
	inputRef := "manager-input://" + req.RequestID
	invocationID := kernel.InvocationID("invocation://manager/" + req.RequestID)
	if err := s.appendConversation(ctx, req.ConversationID, httpapi.ManagerConversationEntry{
		EntryID:         "entry://" + req.RequestID + "/message",
		Kind:            "user_message",
		CreatedAt:       s.now().UTC(),
		ManagerInputRef: inputRef,
		Body:            req.Body,
	}); err != nil {
		return httpapi.ManagerMessageResponse{}, err
	}
	action := managerAction(req.Body, req.SelectedEndpoint)
	decisionTarget := ""
	if action == "held" || action == "released" {
		decisionTarget = targetRef(req.SelectedEndpoint)
	}
	seen := kernel.Revision(0)
	if req.ObservedGraphRevision != nil {
		seen = *req.ObservedGraphRevision
	}
	result, err := s.manager.HandleManagerDecision(ctx, taskmanager.ManagerDecisionInput{
		InputRef:     inputRef,
		SeenRevision: seen,
		Endpoint:     selectedEndpoint(req.SelectedEndpoint),
	}, taskmanager.TaskManagerDecision{
		Action:    action,
		TargetRef: decisionTarget,
		Reason:    "fake Task Manager parsed operator message",
	})
	if err != nil {
		return httpapi.ManagerMessageResponse{}, err
	}
	revision := result.GraphRevision
	decisionRef := result.DecisionRef
	phaseInvocationID := kernel.InvocationID("")
	if action == "held" {
		endpoint := selectedEndpoint(req.SelectedEndpoint)
		previousGeneration := generationFor(ctx, s.graph, endpoint)
		stopRef := "manager-stop://" + req.RequestID
		stopped, stopErr := s.manager.HandlePhaseStopped(ctx, taskmanager.PhaseStoppedInput{
			InputRef:      inputRef,
			CommandID:     "fake-stop-command://" + req.RequestID,
			LeaseRef:      kernel.LeaseID("fake-lease://" + req.RequestID),
			Endpoint:      endpoint,
			Generation:    previousGeneration,
			EvidenceRefs:  []string{stopRef},
			CheckpointRef: "checkpoint://" + req.RequestID,
			NewBindingRef: kernel.BindingRef(fmt.Sprintf("binding://demo/%s/%s/%d", endpoint.TaskID, endpoint.EndpointID, previousGeneration+1)),
			NonResumable:  false,
		})
		if stopErr != nil {
			return httpapi.ManagerMessageResponse{}, stopErr
		}
		s.stopInvocation(endpoint, previousGeneration)
		revision = stopped.GraphRevision
		decisionRef = stopped.DecisionRef
	}
	if action == "released" {
		endpoint := selectedEndpoint(req.SelectedEndpoint)
		var err error
		phaseInvocationID, err = s.createInvocation(ctx, endpoint.TaskID, endpoint.EndpointID, generationFor(ctx, s.graph, endpoint), runtime.InvocationRunning)
		if err != nil {
			return httpapi.ManagerMessageResponse{}, err
		}
	}
	if action != "no_change" {
		if err := s.appendGraphRevision(ctx, inputRef, decisionRef, revision); err != nil {
			return httpapi.ManagerMessageResponse{}, err
		}
	}
	if phaseInvocationID != "" {
		if err := s.appendInvocationUpdated(ctx, req.RequestID, selectedEndpoint(req.SelectedEndpoint), phaseInvocationID); err != nil {
			return httpapi.ManagerMessageResponse{}, err
		}
	}
	_ = s.appendConversation(ctx, req.ConversationID, httpapi.ManagerConversationEntry{
		EntryID:         "entry://" + req.RequestID + "/decision",
		Kind:            "decision",
		CreatedAt:       s.now().UTC(),
		DecisionRef:     decisionRef,
		GraphRevision:   revision,
		Disposition:     string(result.Status),
		ManagerInputRef: inputRef,
	})
	return httpapi.ManagerMessageResponse{ManagerInputRef: inputRef, InvocationRef: invocationID, ConversationID: req.ConversationID, Status: string(result.Status)}, nil
}

func managerAction(body string, endpoint *coordination.PhaseEndpointRef) string {
	if endpoint == nil {
		return "no_change"
	}
	lower := strings.ToLower(body)
	if strings.Contains(lower, "hold") || strings.Contains(lower, "stop") || strings.Contains(body, "\u6682\u505c") || strings.Contains(body, "\u505c\u6b62") {
		return "held"
	}
	if strings.Contains(lower, "resume") || strings.Contains(lower, "release") || strings.Contains(body, "\u6062\u590d") {
		return "released"
	}
	return "no_change"
}

func selectedEndpoint(endpoint *coordination.PhaseEndpointRef) coordination.PhaseEndpointRef {
	if endpoint == nil {
		return coordination.PhaseEndpointRef{}
	}
	return *endpoint
}

func targetRef(endpoint *coordination.PhaseEndpointRef) string {
	if endpoint == nil {
		return ""
	}
	return string(endpoint.TaskID) + "/" + string(endpoint.EndpointID)
}

func generationFor(ctx context.Context, graph coordination.TaskManagerGraph, ref coordination.PhaseEndpointRef) int {
	snapshot, err := graph.Snapshot(ctx, kernel.LatestRevision)
	if err != nil {
		return 1
	}
	for _, endpoint := range snapshot.Endpoints {
		if endpoint.Ref == ref {
			return endpoint.Generation
		}
	}
	return 1
}

func (s *state) AppendManagerReply(ctx context.Context, reply taskmanager.ManagerReplyEvent) error {
	_, err := s.events.Append(ctx, evidence.AppendEvent{
		StableKey:     kernel.IdempotencyKey("manager-reply:" + reply.InputRef + ":" + reply.DecisionRef),
		Type:          "manager.interaction",
		ProjectID:     s.projectID,
		GraphRevision: int64(reply.GraphRevision),
		Payload: uiprojection.ManagerInteractionEventData{
			Kind:            "decision",
			CreatedAt:       s.now().UTC(),
			ManagerInputRef: reply.InputRef,
			DecisionRef:     reply.DecisionRef,
			GraphRevision:   reply.GraphRevision,
			Disposition:     string(reply.Status),
		},
	})
	return err
}

func (s *state) appendConversation(ctx context.Context, conversationID string, entry httpapi.ManagerConversationEntry) error {
	s.mu.Lock()
	s.conversations[conversationID] = append(s.conversations[conversationID], entry)
	s.mu.Unlock()
	if entry.Kind != "user_message" {
		return nil
	}
	_, err := s.events.Append(ctx, evidence.AppendEvent{
		StableKey: kernel.IdempotencyKey("manager-message:" + entry.ManagerInputRef),
		Type:      "manager.interaction",
		ProjectID: s.projectID,
		Payload: uiprojection.ManagerInteractionEventData{
			ConversationID:  conversationID,
			EntryID:         entry.EntryID,
			Kind:            entry.Kind,
			CreatedAt:       entry.CreatedAt,
			ManagerInputRef: entry.ManagerInputRef,
			Body:            entry.Body,
		},
	})
	return err
}

func (s *state) appendGraphRevision(ctx context.Context, inputRef, decisionRef string, revision kernel.Revision) error {
	if revision == 0 {
		snapshot, err := s.graph.Snapshot(ctx, kernel.LatestRevision)
		if err != nil {
			return err
		}
		revision = snapshot.Revision
	}
	_, err := s.events.Append(ctx, evidence.AppendEvent{
		StableKey:     kernel.IdempotencyKey(fmt.Sprintf("graph:%s:%s:%d", inputRef, decisionRef, revision)),
		Type:          "graph.revision",
		ProjectID:     s.projectID,
		GraphRevision: int64(revision),
		Payload: uiprojection.GraphRevisedEventData{
			Revision:        revision,
			ManagerInputRef: inputRef,
			DecisionRef:     decisionRef,
		},
	})
	return err
}

func (s *state) appendInvocationUpdated(ctx context.Context, requestID string, endpoint coordination.PhaseEndpointRef, invocationID kernel.InvocationID) error {
	if endpoint.TaskID == "" {
		return nil
	}
	_, err := s.events.Append(ctx, evidence.AppendEvent{
		StableKey:         kernel.IdempotencyKey("invocation-updated:" + requestID),
		Type:              "invocation.updated",
		ProjectID:         s.projectID,
		TaskID:            endpoint.TaskID,
		PhaseEndpoint:     endpoint.EndpointID,
		AgentInvocationID: invocationID,
		Payload: uiprojection.InvocationUpdatedEventData{
			Endpoint:     endpoint,
			Generation:   generationFor(ctx, s.graph, endpoint),
			InvocationID: invocationID,
			Status:       "running",
		},
	})
	return err
}

func (s *state) Conversation(_ context.Context, _ auth.Principal, conversationID, after string) (httpapi.ManagerConversation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries := make([]httpapi.ManagerConversationEntry, len(s.conversations[conversationID]))
	copy(entries, s.conversations[conversationID])
	if after != "" {
		for i, entry := range entries {
			if entry.EntryID == after {
				entries = entries[i+1:]
				break
			}
		}
	}
	cursor := ""
	if len(entries) > 0 {
		cursor = entries[len(entries)-1].EntryID
	}
	return httpapi.ManagerConversation{ConversationID: conversationID, ProjectID: s.projectID, Messages: entries, Cursor: cursor}, nil
}

func (s *state) SubmitRequirement(context.Context, auth.Principal, httpapi.RequirementCreateRequest) (httpapi.RequirementCreateResponse, error) {
	return httpapi.RequirementCreateResponse{}, kernel.InvalidArgument("fakehost demo is preloaded; requirement creation is not implemented")
}

func (s *state) SubmitHumanDecision(context.Context, auth.Principal, httpapi.HumanDecisionRequest) (httpapi.HumanDecisionResponse, error) {
	return httpapi.HumanDecisionResponse{}, kernel.InvalidArgument("fakehost routes graph changes through Manager messages")
}

func (s *state) ListInvocations(_ context.Context, filter uiprojection.InvocationFilter) ([]runtime.Invocation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]runtime.Invocation, 0, len(s.invocations))
	for _, invocation := range s.invocations {
		if filter.ProjectID != "" && invocation.ProjectID != filter.ProjectID {
			continue
		}
		if filter.TaskID != "" && invocation.TaskID != filter.TaskID {
			continue
		}
		if filter.EndpointID != "" && invocation.EndpointID != filter.EndpointID {
			continue
		}
		if filter.Generation != 0 && invocation.Generation != filter.Generation {
			continue
		}
		out = append(out, invocation)
	}
	return out, nil
}

func (s *state) createInvocation(ctx context.Context, taskID kernel.TaskID, endpointID kernel.EndpointID, generation int, status runtime.InvocationStatus) (kernel.InvocationID, error) {
	role := auth.RolePlanner
	if endpointID == coordination.EndpointExecute {
		role = auth.RoleExecutor
	}
	if endpointID == coordination.EndpointVerify {
		role = auth.RoleVerifier
	}
	invocation := runtime.Invocation{
		ID:                  kernel.InvocationID(fmt.Sprintf("invocation://%s/%s/%d/%d", taskID, endpointID, generation, len(s.invocations)+1)),
		ActorPrincipalID:    kernel.ActorPrincipalID(fmt.Sprintf("fake-%s", role)),
		ProjectID:           s.projectID,
		TaskID:              taskID,
		EndpointID:          endpointID,
		Generation:          uint64(generation),
		Role:                role,
		Status:              status,
		BindingRef:          kernel.BindingRef(fmt.Sprintf("binding://demo/%s/%s/%d", taskID, endpointID, generation)),
		LeaseID:             kernel.LeaseID(fmt.Sprintf("lease://demo/%s/%s/%d", taskID, endpointID, generation)),
		WorkspaceRef:        fmt.Sprintf("workspace://demo/%s", taskID),
		ContextSliceRef:     fmt.Sprintf("context-slice://%s/%s/%d", taskID, endpointID, generation),
		TaskMemoryBufferRef: fmt.Sprintf("task-memory-buffer://%s", taskID),
		PromptHashes:        map[string]string{"system": "fake"},
		SkillHashes:         map[string]string{"role": "fake"},
		EffectiveTools:      []auth.Tool{auth.ToolContextListSubgraphs},
		CreatedAt:           s.now().UTC(),
		ExpiresAt:           s.now().UTC().Add(time.Hour),
	}
	s.mu.Lock()
	s.invocations = append(s.invocations, invocation)
	s.contexts[invocation.ID] = inspectionFor(s.projectID, invocation)
	s.mu.Unlock()
	if status == runtime.InvocationRunning || status == runtime.InvocationWaiting {
		_, _ = s.capacity.Observe(ctx, s.capacity.Snapshot().Healthy, 1)
	}
	return invocation.ID, nil
}

func (s *state) stopInvocation(ref coordination.PhaseEndpointRef, generation int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.invocations {
		invocation := &s.invocations[i]
		if invocation.TaskID == ref.TaskID && invocation.EndpointID == ref.EndpointID && invocation.Generation == uint64(generation) && invocation.Status == runtime.InvocationRunning {
			invocation.Status = runtime.InvocationStopped
		}
	}
}

func inspectionFor(projectID kernel.ProjectID, invocation runtime.Invocation) uiprojection.ContextInspection {
	subgraph := "general-demo"
	taskSubgraph := "task-" + string(invocation.TaskID)
	return uiprojection.ContextInspection{
		Subscriptions: []contextgraph.SubscriptionInspection{
			{ID: "sub-" + string(invocation.ID) + "-initial", ConsumerInvocationID: string(invocation.ID), Source: "initial_slice", SubgraphIDs: []string{taskSubgraph}, Active: true},
			{ID: "sub-" + string(invocation.ID) + "-search", ConsumerInvocationID: string(invocation.ID), Source: "search", SubgraphIDs: []string{subgraph}, Active: true},
		},
		Slice: contextgraph.ContextSlice{GraphRevision: int64(invocation.Generation), Nodes: []contextgraph.ContextNode{
			{ID: "ctx-" + string(invocation.ID), Kind: "fact", Statement: "demo context for " + string(invocation.TaskID), Status: "accepted", SourceRefs: []string{"artifact://demo/context"}, SubgraphIDs: []string{subgraph, taskSubgraph}},
		}},
		Frontier: []string{"ctx-frontier-" + string(invocation.TaskID)},
		Candidates: []uiprojection.CandidateInspectionRecord{
			{ProjectID: projectID, TaskID: invocation.TaskID, CreatedByInvocationID: invocation.ID, View: contextgraph.TaskMemoryCandidateView{CandidateID: "candidate-" + string(invocation.ID), Candidate: contextgraph.MemoryCandidate{Statement: "candidate from " + string(invocation.ID), Kind: "fact", SourceRefs: []string{"artifact://demo/candidate"}, SubgraphIDs: []string{taskSubgraph}}}},
			{ProjectID: projectID, TaskID: "task-beta", CreatedByInvocationID: invocation.ID, View: contextgraph.TaskMemoryCandidateView{CandidateID: "candidate-cross-task", Candidate: contextgraph.MemoryCandidate{Statement: "must not leak", Kind: "fact", SourceRefs: []string{"artifact://demo/hidden"}, SubgraphIDs: []string{"task-beta"}}}},
		},
	}
}

func (s *state) InspectInvocation(_ context.Context, _ auth.Principal, invocation runtime.Invocation) (uiprojection.ContextInspection, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.contexts[invocation.ID], nil
}

type queryPort struct {
	projectID   kernel.ProjectID
	graph       *coordination.MemoryStore
	ui          *uiprojection.Service
	invocations uiprojection.InvocationReader
}

func (q queryPort) ProjectSnapshot(ctx context.Context, principal auth.Principal, projectID kernel.ProjectID, revision kernel.Revision) (httpapi.CoordinationSnapshot, error) {
	return q.ui.Snapshot(ctx, principal, projectID, revision)
}

func (q queryPort) InspectEndpoint(ctx context.Context, principal auth.Principal, ref coordination.PhaseEndpointRef, generation int) (httpapi.EndpointInspector, error) {
	return q.ui.InspectEndpoint(ctx, principal, q.projectID, ref, generation)
}

func (q queryPort) Task(ctx context.Context, principal auth.Principal, taskID kernel.TaskID) (httpapi.TaskProjection, error) {
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
	invocations, _ := q.invocations.ListInvocations(ctx, uiprojection.InvocationFilter{ProjectID: q.projectID, TaskID: taskID})
	endpoints := make([]httpapi.EndpointProjection, 0, 3)
	for _, endpoint := range graph.Endpoints {
		if endpoint.Ref.TaskID != taskID {
			continue
		}
		endpoints = append(endpoints, httpapi.EndpointProjection{
			TaskID:              endpoint.Ref.TaskID,
			EndpointID:          endpoint.Ref.EndpointID,
			Generation:          endpoint.Generation,
			State:               string(endpoint.State),
			RunPolicy:           string(endpoint.RunPolicy),
			BindingRef:          string(endpoint.BindingRef),
			LatestInvocationRef: latestInvocation(invocations, endpoint.Ref, endpoint.Generation),
		})
	}
	sort.Slice(endpoints, func(i, j int) bool { return endpoints[i].EndpointID < endpoints[j].EndpointID })
	status := "pending"
	for _, summary := range snapshot.Tasks {
		if summary.TaskID == taskID {
			status = summary.Status
		}
	}
	return httpapi.TaskProjection{TaskID: taskID, ProjectID: q.projectID, Status: status, GraphRevision: graph.Revision, ContractRef: task.ContractRef, DeliveryPolicy: string(taskmanager.DeliveryPolicyCodeMerge), Endpoints: endpoints}, nil
}

func latestInvocation(invocations []runtime.Invocation, ref coordination.PhaseEndpointRef, generation int) kernel.InvocationID {
	var latest runtime.Invocation
	for _, invocation := range invocations {
		if invocation.TaskID == ref.TaskID && invocation.EndpointID == ref.EndpointID && invocation.Generation == uint64(generation) {
			latest = invocation
		}
	}
	return latest.ID
}

type decisionStore struct {
	mu        sync.Mutex
	projectID kernel.ProjectID
	log       *coordination.MemoryDecisionLog
	next      int
	records   []taskmanager.DecisionSubmission
}

func newDecisionStore(projectID kernel.ProjectID, log *coordination.MemoryDecisionLog) *decisionStore {
	return &decisionStore{projectID: projectID, log: log}
}

func (d *decisionStore) SubmitDecision(_ context.Context, submission taskmanager.DecisionSubmission) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.next++
	ref := fmt.Sprintf("decision://fakehost/%d", d.next)
	switch submission.Kind {
	case taskmanager.DecisionKindReplacePending:
		if err := d.log.RegisterReplacePending(submission.ProjectID, kernel.IdempotencyKey(ref)); err != nil {
			return "", err
		}
	case taskmanager.DecisionKindTransition:
		if err := d.log.RegisterTransition(submission.ProjectID, ref, submission.Transition); err != nil {
			return "", err
		}
	}
	d.records = append(d.records, submission)
	return ref, nil
}
