package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/uiprojection"
)

func TestManagerMessageRejectsUnknownGraphMutationFields(t *testing.T) {
	h := newHTTPHarness()
	body := `{"request_id":"req-1","project_id":"project-a","conversation_id":"conv-1","body":"hold execute","graph_patch":{"bad":true}}`
	rec := h.post("/v1/manager/messages", body)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body=%s, want 400", rec.Code, rec.Body.String())
	}
	if h.manager.messages != 0 {
		t.Fatalf("manager port called for unknown graph mutation field")
	}
}

func TestManagerMessageUsesObservedGraphRevisionOnly(t *testing.T) {
	h := newHTTPHarness()
	rec := h.post("/v1/manager/messages", `{"request_id":"req-1","project_id":"project-a","conversation_id":"conv-1","body":"hold execute","observed_graph_revision":7}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if h.manager.req.ObservedGraphRevision == nil || *h.manager.req.ObservedGraphRevision != 7 {
		t.Fatalf("observed_graph_revision = %#v, want 7", h.manager.req.ObservedGraphRevision)
	}

	rec = h.post("/v1/manager/messages", `{"request_id":"req-2","project_id":"project-a","conversation_id":"conv-1","body":"hold execute","seen_graph_revision":7}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("legacy field status = %d body=%s, want 400", rec.Code, rec.Body.String())
	}
}

func TestManagerMessageRequiresExplicitScopedLifecycleIntent(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "unknown intent", body: `{"request_id":"req-unknown","project_id":"project-a","conversation_id":"conv-1","body":"change it","intent":"release"}`},
		{name: "hold without endpoint", body: `{"request_id":"req-hold","project_id":"project-a","conversation_id":"conv-1","body":"pause it","intent":"hold"}`},
		{name: "resume without endpoint", body: `{"request_id":"req-resume","project_id":"project-a","conversation_id":"conv-1","body":"continue it","intent":"resume"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			h := newHTTPHarness()
			rec := h.post("/v1/manager/messages", test.body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d body=%s, want 400", rec.Code, rec.Body.String())
			}
			if h.manager.messages != 0 {
				t.Fatal("invalid lifecycle intent reached Manager port")
			}
		})
	}

	h := newHTTPHarness()
	rec := h.post("/v1/manager/messages", `{"request_id":"req-orchestrate","project_id":"project-a","conversation_id":"conv-1","body":"replan remaining work"}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("omitted orchestration intent status = %d body=%s", rec.Code, rec.Body.String())
	}
	if h.manager.req.Intent != "" {
		t.Fatalf("transport rewrote omitted intent = %q; Runtime should default it authoritatively", h.manager.req.Intent)
	}

	ref := `{"task_id":"task-a","endpoint_id":"execute"}`
	rec = h.post("/v1/manager/messages", `{"request_id":"req-resume-ok","project_id":"project-a","conversation_id":"conv-1","body":"continue it","intent":"resume","selected_endpoint":`+ref+`}`)
	if rec.Code != http.StatusAccepted || h.manager.req.Intent != ManagerIntentResume {
		t.Fatalf("explicit resume status=%d intent=%q body=%s", rec.Code, h.manager.req.Intent, rec.Body.String())
	}
}

func TestRequirementWriteAuthenticatesCSRFGateAndForwardsToPort(t *testing.T) {
	h := newHTTPHarness()
	rec := h.post("/v1/requirements", `{"request_id":"req-1","project_id":"project-a","body":"ship it","motivation":"done"}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if h.auth.projectID != "project-a" || !h.guard.checked {
		t.Fatalf("auth project=%q csrf checked=%v", h.auth.projectID, h.guard.checked)
	}
	if h.requirements.req.RequestID != "req-1" || h.requirements.principal.Role != auth.RoleOperator {
		t.Fatalf("requirement forwarding principal=%#v req=%#v", h.requirements.principal, h.requirements.req)
	}
}

func TestCapacityAdjustmentRevisionConflictMapsTo409(t *testing.T) {
	h := newHTTPHarness()
	h.capacity.adjustErr = kernel.RevisionConflict(3, 4)
	rec := h.post("/v1/capacity-adjustments", `{"request_id":"cap-1","project_id":"project-a","expected_revision":3,"desired_concurrency":4}`)
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d body=%s, want 409", rec.Code, rec.Body.String())
	}
	var got kernel.Error
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Code != kernel.CodeRevisionConflict {
		t.Fatalf("error = %#v", got)
	}
}

func TestQueryHandlersUseReadPortsOnly(t *testing.T) {
	h := newHTTPHarness()
	rec := h.get("/v1/coordination/snapshot?project_id=project-a&revision=6")
	if rec.Code != http.StatusOK {
		t.Fatalf("snapshot status = %d body=%s", rec.Code, rec.Body.String())
	}
	if h.query.snapshotProject != "project-a" || h.query.snapshotRevision != 6 {
		t.Fatalf("snapshot project=%q revision=%d", h.query.snapshotProject, h.query.snapshotRevision)
	}
	rec = h.get("/v1/context/snapshot?project_id=project-a")
	if rec.Code != http.StatusOK {
		t.Fatalf("context snapshot status = %d body=%s", rec.Code, rec.Body.String())
	}
	if h.query.contextProject != "project-a" {
		t.Fatalf("context snapshot project=%q", h.query.contextProject)
	}
	rec = h.get("/v1/coordination/endpoints/task-a/execute/inspector?project_id=project-a&generation=2")
	if rec.Code != http.StatusOK {
		t.Fatalf("inspector status = %d body=%s", rec.Code, rec.Body.String())
	}
	if h.query.endpoint.TaskID != "task-a" || h.query.endpoint.EndpointID != "execute" || h.query.generation != 2 {
		t.Fatalf("inspector endpoint=%#v generation=%d", h.query.endpoint, h.query.generation)
	}
	rec = h.get("/v1/tasks/task-a?project_id=project-a")
	if rec.Code != http.StatusOK || h.query.taskID != "task-a" {
		t.Fatalf("task status=%d task_id=%q body=%s", rec.Code, h.query.taskID, rec.Body.String())
	}
	rec = h.get("/v1/manager/conversations/conv-a?project_id=project-a&after=cursor-4")
	if rec.Code != http.StatusOK || h.manager.conversationID != "conv-a" || h.manager.after != "cursor-4" {
		t.Fatalf("conversation status=%d id=%q after=%q body=%s", rec.Code, h.manager.conversationID, h.manager.after, rec.Body.String())
	}
	if h.guard.checked {
		t.Fatalf("CSRF guard should not run for GET")
	}
}

func TestWriteRequestValidationMatchesOpenAPI(t *testing.T) {
	tests := []struct {
		name string
		path string
		body string
	}{
		{name: "requirement request id too long", path: "/v1/requirements", body: `{"request_id":"` + strings.Repeat("x", 129) + `","project_id":"project-a","body":"ship"}`},
		{name: "requirement source enum", path: "/v1/requirements", body: `{"request_id":"req-1","project_id":"project-a","body":"ship","source":{"kind":"shell"}}`},
		{name: "capacity negative revision", path: "/v1/capacity-adjustments", body: `{"request_id":"req-1","project_id":"project-a","expected_revision":-1,"desired_concurrency":2}`},
		{name: "capacity above maximum", path: "/v1/capacity-adjustments", body: `{"request_id":"req-1","project_id":"project-a","expected_revision":1,"desired_concurrency":10001}`},
		{name: "human decision enum", path: "/v1/human-decisions", body: `{"request_id":"req-1","project_id":"project-a","target":{"kind":"task","ref":"task-a"},"decision":"force","reason":"because"}`},
		{name: "manager endpoint enum", path: "/v1/manager/messages", body: `{"request_id":"req-1","project_id":"project-a","conversation_id":"conv-a","body":"change it","selected_endpoint":{"task_id":"task-a","endpoint_id":"deploy"}}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			h := newHTTPHarness()
			rec := h.post(test.path, test.body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d body=%s, want 400", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestEndpointInspectorRejectsInvalidGenerationAndEndpoint(t *testing.T) {
	h := newHTTPHarness()
	for _, path := range []string{
		"/v1/coordination/endpoints/task-a/execute/inspector?project_id=project-a&generation=0",
		"/v1/coordination/endpoints/task-a/deploy/inspector?project_id=project-a",
	} {
		rec := h.get(path)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("path=%s status=%d body=%s, want 400", path, rec.Code, rec.Body.String())
		}
	}
}

func TestMethodAndRuntimeErrorStatusMapping(t *testing.T) {
	h := newHTTPHarness()
	req := httptest.NewRequest(http.MethodPut, "/v1/capacity?project_id=project-a", nil)
	rec := httptest.NewRecorder()
	h.handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") != http.MethodGet {
		t.Fatalf("method status=%d allow=%q", rec.Code, rec.Header().Get("Allow"))
	}

	h.capacity.adjustErr = kernel.Error{Code: kernel.CodeStaleCommand, Message: "stale", Recoverable: true}
	rec = h.post("/v1/capacity-adjustments", `{"request_id":"cap-1","project_id":"project-a","expected_revision":3,"desired_concurrency":4}`)
	if rec.Code != http.StatusConflict {
		t.Fatalf("stale command status=%d body=%s", rec.Code, rec.Body.String())
	}
	h.capacity.adjustErr = kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "offline", Recoverable: true}
	rec = h.post("/v1/capacity-adjustments", `{"request_id":"cap-2","project_id":"project-a","expected_revision":3,"desired_concurrency":4}`)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("executor unavailable status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestHealthAndReadinessDoNotRequireOperatorSession(t *testing.T) {
	h := newHTTPHarness()
	for _, test := range []struct {
		path string
		want int
	}{
		{path: "/healthz", want: http.StatusOK},
		{path: "/readyz", want: http.StatusOK},
	} {
		req := httptest.NewRequest(http.MethodGet, test.path, nil)
		rec := httptest.NewRecorder()
		h.handler.ServeHTTP(rec, req)
		if rec.Code != test.want {
			t.Fatalf("%s status=%d body=%s, want %d", test.path, rec.Code, rec.Body.String(), test.want)
		}
	}
	if h.auth.projectID != "" {
		t.Fatalf("health endpoint authenticated as project %q", h.auth.projectID)
	}

	h.readiness.status = ReadinessStatus{
		Status: "not_ready",
		Dependencies: []DependencyReadiness{
			{Name: "agentteams", Status: "unavailable", Message: "host offline"},
		},
	}
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	h.handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("degraded readyz status=%d body=%s, want 503", rec.Code, rec.Body.String())
	}
}

type httpHarness struct {
	auth         *fakeHTTPAuth
	guard        *fakeCSRFGuard
	requirements *fakeRequirementPort
	capacity     *fakeCapacityPort
	human        *fakeHumanPort
	manager      *fakeManagerPort
	query        *fakeQueryPort
	readiness    *fakeReadinessPort
	events       *fakeEventQuery
	handler      http.Handler
}

func newHTTPHarness() *httpHarness {
	h := &httpHarness{
		auth:         &fakeHTTPAuth{},
		guard:        &fakeCSRFGuard{},
		requirements: &fakeRequirementPort{},
		capacity:     &fakeCapacityPort{},
		human:        &fakeHumanPort{},
		manager:      &fakeManagerPort{},
		query:        &fakeQueryPort{},
		readiness:    &fakeReadinessPort{status: ReadinessStatus{Status: "ready", Dependencies: []DependencyReadiness{}}},
		events:       &fakeEventQuery{},
	}
	h.handler = New(Options{
		Authenticator:     h.auth,
		CSRFGuard:         h.guard,
		Requirements:      h.requirements,
		Capacity:          h.capacity,
		Human:             h.human,
		Manager:           h.manager,
		Query:             h.query,
		Readiness:         h.readiness,
		Events:            h.events,
		EventStreamBuffer: 2,
	}).Handler()
	return h
}

type fakeReadinessPort struct {
	status ReadinessStatus
}

func (f *fakeReadinessPort) Readiness(context.Context) ReadinessStatus {
	return f.status
}

func (h *httpHarness) post(path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.AddCookie(&http.Cookie{Name: auth.SessionCookieName, Value: "session"})
	req.Header.Set("Origin", "https://threadmill.test")
	req.Header.Set(auth.CSRFHeaderName, "csrf")
	rec := httptest.NewRecorder()
	h.handler.ServeHTTP(rec, req)
	return rec
}

func (h *httpHarness) get(path string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.AddCookie(&http.Cookie{Name: auth.SessionCookieName, Value: "session"})
	rec := httptest.NewRecorder()
	h.handler.ServeHTTP(rec, req)
	return rec
}

type fakeHTTPAuth struct {
	projectID kernel.ProjectID
}

func (f *fakeHTTPAuth) AuthenticateOperatorSession(_ context.Context, _ string, projectID kernel.ProjectID) (auth.Principal, auth.SessionRecord, error) {
	f.projectID = projectID
	return auth.Principal{Kind: auth.PrincipalOperator, Role: auth.RoleOperator, ProjectID: projectID, ActorPrincipalID: "operator-a"}, auth.SessionRecord{}, nil
}

type fakeCSRFGuard struct {
	checked bool
}

func (f *fakeCSRFGuard) Check(*http.Request, auth.SessionRecord) error {
	f.checked = true
	return nil
}

type fakeRequirementPort struct {
	principal auth.Principal
	req       RequirementCreateRequest
}

func (f *fakeRequirementPort) SubmitRequirement(_ context.Context, principal auth.Principal, req RequirementCreateRequest) (RequirementCreateResponse, error) {
	f.principal, f.req = principal, req
	return RequirementCreateResponse{RequirementID: "requirement-1", ManagerInputRef: "manager-input-1", InvocationRef: "inv-1", Status: "accepted"}, nil
}

type fakeCapacityPort struct {
	adjustErr error
}

func (f *fakeCapacityPort) GetCapacity(context.Context, auth.Principal, kernel.ProjectID) (CapacityState, error) {
	return CapacityState{ProjectID: "project-a", Revision: 1, UpdatedAt: time.Now().UTC()}, nil
}

func (f *fakeCapacityPort) AdjustCapacity(context.Context, auth.Principal, CapacityAdjustmentRequest) (CapacityAdjustmentResponse, error) {
	if f.adjustErr != nil {
		return CapacityAdjustmentResponse{}, f.adjustErr
	}
	return CapacityAdjustmentResponse{CommandRef: "capacity-command-1", Capacity: CapacityState{ProjectID: "project-a", Revision: 2, UpdatedAt: time.Now().UTC()}}, nil
}

type fakeHumanPort struct{}

func (fakeHumanPort) SubmitHumanDecision(context.Context, auth.Principal, HumanDecisionRequest) (HumanDecisionResponse, error) {
	return HumanDecisionResponse{HumanDecisionRef: "human-1", ManagerInputRef: "manager-input-1", InvocationRef: "inv-1", Status: "accepted"}, nil
}

type fakeManagerPort struct {
	messages       int
	req            ManagerMessageRequest
	conversationID string
	after          string
}

func (f *fakeManagerPort) SubmitManagerMessage(_ context.Context, _ auth.Principal, req ManagerMessageRequest) (ManagerMessageResponse, error) {
	f.messages++
	f.req = req
	return ManagerMessageResponse{ManagerInputRef: "manager-input-1", InvocationRef: "inv-1", Status: "accepted"}, nil
}

func (f *fakeManagerPort) Conversation(_ context.Context, _ auth.Principal, conversationID, after string) (ManagerConversation, error) {
	f.conversationID, f.after = conversationID, after
	return ManagerConversation{ConversationID: "conv-1", ProjectID: "project-a", Cursor: "cursor-1"}, nil
}

type fakeQueryPort struct {
	snapshotProject  kernel.ProjectID
	snapshotRevision kernel.Revision
	contextProject   kernel.ProjectID
	endpoint         coordination.PhaseEndpointRef
	generation       int
	taskID           kernel.TaskID
}

func (f *fakeQueryPort) Task(_ context.Context, _ auth.Principal, taskID kernel.TaskID) (TaskProjection, error) {
	f.taskID = taskID
	return TaskProjection{TaskID: "task-a", ProjectID: "project-a", Status: "pending", DeliveryPolicy: "code_merge", Endpoints: []EndpointProjection{}}, nil
}

func (f *fakeQueryPort) ProjectSnapshot(_ context.Context, _ auth.Principal, projectID kernel.ProjectID, revision kernel.Revision) (CoordinationSnapshot, error) {
	f.snapshotProject, f.snapshotRevision = projectID, revision
	return CoordinationSnapshot{ProjectID: projectID, Revision: revision, Cursor: "cursor-1", Capacity: CapacityState{ProjectID: projectID, Revision: 1, UpdatedAt: time.Now().UTC()}}, nil
}

func (f *fakeQueryPort) ContextSnapshot(_ context.Context, _ auth.Principal, projectID kernel.ProjectID) (ContextGraphSnapshot, error) {
	f.contextProject = projectID
	return ContextGraphSnapshot{ProjectID: projectID, Revision: 3, Nodes: []ContextSnapshotNode{}, Edges: []ContextSnapshotEdge{}, Subgraphs: []ContextSnapshotSubgraph{}}, nil
}

func (f *fakeQueryPort) InspectEndpoint(_ context.Context, _ auth.Principal, endpoint coordination.PhaseEndpointRef, generation int) (EndpointInspector, error) {
	f.endpoint, f.generation = endpoint, generation
	return EndpointInspector{Endpoint: endpoint, Generation: generation, GraphRevision: 7, Subscriptions: []SubscriptionProjection{}}, nil
}

type fakeEventQuery struct {
	after       string
	limit       int
	projectID   kernel.ProjectID
	pageErr     error
	streamAfter string
	streamErr   error
	stream      chan uiprojection.UIEvent
}

func (f *fakeEventQuery) ListEvents(_ context.Context, _ auth.Principal, projectID kernel.ProjectID, after string, limit int) (uiprojection.EventPage, error) {
	f.projectID, f.after, f.limit = projectID, after, limit
	if f.pageErr != nil {
		return uiprojection.EventPage{}, f.pageErr
	}
	return uiprojection.EventPage{NextCursor: "cursor-next", Events: []uiprojection.UIEvent{}}, nil
}

func (f *fakeEventQuery) SubscribeEvents(_ context.Context, _ auth.Principal, projectID kernel.ProjectID, after string) (<-chan uiprojection.UIEvent, error) {
	f.projectID, f.streamAfter = projectID, after
	if f.streamErr != nil {
		return nil, f.streamErr
	}
	if f.stream == nil {
		f.stream = make(chan uiprojection.UIEvent)
		close(f.stream)
	}
	return f.stream, nil
}
