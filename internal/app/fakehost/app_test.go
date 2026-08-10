package fakehost

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/transport/httpapi"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/uiprojection"
)

func TestCapacityAdjustmentDoesNotChangeGraphRevision(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	before := h.snapshot(t)

	rec := h.post(t, "/v1/capacity-adjustments", map[string]any{
		"request_id":          "cap-1",
		"project_id":          DemoProjectID,
		"expected_revision":   before.Capacity.Revision,
		"desired_concurrency": 3,
	})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("capacity status=%d body=%s", rec.Code, rec.Body.String())
	}
	after := h.snapshot(t)
	if after.Revision != before.Revision {
		t.Fatalf("graph revision changed on capacity update: before=%d after=%d", before.Revision, after.Revision)
	}
	if after.Capacity.Revision != before.Capacity.Revision+1 || after.Capacity.DesiredConcurrency != 3 {
		t.Fatalf("capacity after update = %+v; before=%+v", after.Capacity, before.Capacity)
	}
}

func TestManagerHoldResumeTracksDecisionAndCreatesNewGenerationInvocation(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	start := h.snapshot(t)
	ref := coordination.PhaseEndpointRef{TaskID: "task-alpha", EndpointID: coordination.EndpointExecute}

	hold := h.managerMessage(t, "mgr-hold-1", "hold execute", ref, start.Revision)
	held := h.snapshot(t)
	heldNode := node(t, held, ref)
	if heldNode.Generation != 2 || heldNode.RunPolicy != coordination.RunHeld {
		t.Fatalf("held node = %+v, want generation 2 held", heldNode)
	}
	if hold.ManagerInputRef == "" || hold.InvocationRef == "" {
		t.Fatalf("hold response missing trace refs: %+v", hold)
	}
	heldInspector := h.inspect(t, ref, 1)
	if heldInspector.Invocation == nil || heldInspector.Invocation.Status != "stopped" {
		t.Fatalf("held generation inspector = %+v, want stopped invocation", heldInspector)
	}

	resume := h.managerMessage(t, "mgr-resume-1", "resume execute", ref, held.Revision)
	resumed := h.snapshot(t)
	resumedNode := node(t, resumed, ref)
	if resumedNode.Generation != 2 || resumedNode.RunPolicy != coordination.RunEnabled {
		t.Fatalf("resumed node = %+v, want generation 2 enabled", resumedNode)
	}
	if resumedNode.LatestInvocationRef == "" || resumedNode.LatestInvocationRef == heldNode.LatestInvocationRef {
		t.Fatalf("resume did not expose a new latest invocation: held=%q resumed=%q", heldNode.LatestInvocationRef, resumedNode.LatestInvocationRef)
	}
	if resume.ManagerInputRef == "" || resume.InvocationRef == "" {
		t.Fatalf("resume response missing trace refs: %+v", resume)
	}
	resumedInspector := h.inspect(t, ref, 2)
	if resumedInspector.Invocation == nil || resumedInspector.Invocation.Status != "running" || resumedInspector.Invocation.InvocationID != resumedNode.LatestInvocationRef {
		t.Fatalf("resumed generation inspector = %+v, latest node invocation=%q", resumedInspector, resumedNode.LatestInvocationRef)
	}

	events := h.events(t)
	if !hasEvent(events, "manager.interaction") || !hasEvent(events, "graph.revision") || !hasEvent(events, "invocation.updated") {
		t.Fatalf("events missing manager/graph/invocation trace: %+v", events.Events)
	}
}

func TestManagerProjectMessagePersistsNoChangeWithoutGraphMutation(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	before := h.snapshot(t)
	rec := h.post(t, "/v1/manager/messages", map[string]any{
		"request_id":              "mgr-project-1",
		"project_id":              DemoProjectID,
		"conversation_id":         "conv-project",
		"body":                    "summarize current execution risks",
		"observed_graph_revision": before.Revision,
	})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("project manager message status=%d body=%s", rec.Code, rec.Body.String())
	}
	after := h.snapshot(t)
	if after.Revision != before.Revision {
		t.Fatalf("project-level no-change message mutated graph: before=%d after=%d", before.Revision, after.Revision)
	}
	conversationReq := httptest.NewRequest(http.MethodGet, "/v1/manager/conversations/conv-project?project_id="+string(DemoProjectID), nil)
	conversationRec := httptest.NewRecorder()
	h.app.Handler().ServeHTTP(conversationRec, conversationReq)
	if conversationRec.Code != http.StatusOK {
		t.Fatalf("conversation status=%d body=%s", conversationRec.Code, conversationRec.Body.String())
	}
	var conversation httpapi.ManagerConversation
	decode(t, conversationRec, &conversation)
	if len(conversation.Messages) != 2 || conversation.Messages[1].DecisionRef == "" {
		t.Fatalf("conversation missing persisted no-change decision: %+v", conversation.Messages)
	}
	allowedKinds := map[string]bool{
		"user_message":    true,
		"manager_reply":   true,
		"decision":        true,
		"mutation_result": true,
	}
	for _, entry := range conversation.Messages {
		if !allowedKinds[entry.Kind] {
			t.Fatalf("conversation entry kind %q is outside the OpenAPI enum", entry.Kind)
		}
	}
	if !hasManagerInteractionKind(h.events(t), "user_message") {
		t.Fatal("manager user message was not projected as a manager.interaction event")
	}
}

func TestInspectorKeepsInvocationSubscriptionsSliceAndCandidatesSeparate(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ref := coordination.PhaseEndpointRef{TaskID: "task-alpha", EndpointID: coordination.EndpointExecute}
	got := h.inspect(t, ref, 0)

	if got.Invocation == nil || got.Invocation.InvocationID == "" {
		t.Fatalf("inspector invocation = %+v", got.Invocation)
	}
	if len(got.Subscriptions) != 2 {
		t.Fatalf("subscriptions = %+v, want active and search subscriptions", got.Subscriptions)
	}
	if got.ContextSlice == nil || len(got.ContextSlice.Nodes) != 1 {
		t.Fatalf("context slice = %+v", got.ContextSlice)
	}
	if got.TaskMemoryBuffer == nil || len(got.TaskMemoryBuffer.Candidates) != 1 {
		t.Fatalf("task memory buffer = %+v", got.TaskMemoryBuffer)
	}
	if got.ContextSlice.Nodes[0].NodeID == got.TaskMemoryBuffer.Candidates[0].CandidateID {
		t.Fatalf("context node and candidate collapsed into one object: %+v", got)
	}

	neverRun := h.inspect(t, coordination.PhaseEndpointRef{TaskID: "task-beta", EndpointID: coordination.EndpointVerify}, 0)
	if neverRun.Invocation != nil || neverRun.ContextSlice != nil || neverRun.TaskMemoryBuffer != nil {
		t.Fatalf("never-run endpoint fabricated inspector data: %+v", neverRun)
	}
}

func TestDirectGraphMutationPathsReturn404(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	for _, test := range []struct {
		method string
		path   string
	}{
		{method: http.MethodPost, path: "/v1/coordination/graph"},
		{method: http.MethodPost, path: "/v1/coordination/endpoints/task-alpha/execute/hold"},
		{method: http.MethodPost, path: "/v1/coordination/endpoints/task-alpha/execute/resume"},
		{method: http.MethodPatch, path: "/v1/coordination/snapshot"},
	} {
		req := httptest.NewRequest(test.method, test.path, bytes.NewReader([]byte(`{}`)))
		rec := httptest.NewRecorder()
		h.app.Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("%s %s status=%d body=%s, want 404", test.method, test.path, rec.Code, rec.Body.String())
		}
	}
}

func TestEmptyManagerConversationUsesStableArrayShape(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/manager/conversations/empty?project_id="+string(DemoProjectID), nil)
	rec := httptest.NewRecorder()
	h.app.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("conversation status=%d body=%s", rec.Code, rec.Body.String())
	}
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if string(payload["messages"]) != "[]" {
		t.Fatalf("messages JSON = %s, want []", payload["messages"])
	}
}

type harness struct {
	t   *testing.T
	app *App
}

func newHarness(t *testing.T) harness {
	t.Helper()
	now := time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)
	app, err := New(context.Background(), Options{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := app.Close(); err != nil {
			t.Fatalf("close fakehost app: %v", err)
		}
	})
	return harness{t: t, app: app}
}

func (h harness) snapshot(t *testing.T) httpapi.CoordinationSnapshot {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/v1/coordination/snapshot?project_id="+string(DemoProjectID), nil)
	rec := httptest.NewRecorder()
	h.app.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("snapshot status=%d body=%s", rec.Code, rec.Body.String())
	}
	var got httpapi.CoordinationSnapshot
	decode(t, rec, &got)
	return got
}

func (h harness) post(t *testing.T, path string, body map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.app.Handler().ServeHTTP(rec, req)
	return rec
}

func (h harness) managerMessage(t *testing.T, requestID string, body string, endpoint coordination.PhaseEndpointRef, revision kernel.Revision) httpapi.ManagerMessageResponse {
	t.Helper()
	rec := h.post(t, "/v1/manager/messages", map[string]any{
		"request_id":              requestID,
		"project_id":              DemoProjectID,
		"conversation_id":         "conv-demo",
		"body":                    body,
		"selected_endpoint":       endpoint,
		"observed_graph_revision": revision,
	})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("manager message status=%d body=%s", rec.Code, rec.Body.String())
	}
	var got httpapi.ManagerMessageResponse
	decode(t, rec, &got)
	return got
}

func (h harness) inspect(t *testing.T, ref coordination.PhaseEndpointRef, generation int) httpapi.EndpointInspector {
	t.Helper()
	path := "/v1/coordination/endpoints/" + string(ref.TaskID) + "/" + string(ref.EndpointID) + "/inspector?project_id=" + string(DemoProjectID)
	if generation > 0 {
		path += "&generation=" + strconv.Itoa(generation)
	}
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	h.app.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("inspect status=%d body=%s", rec.Code, rec.Body.String())
	}
	var got httpapi.EndpointInspector
	decode(t, rec, &got)
	return got
}

func (h harness) events(t *testing.T) uiprojection.EventPage {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/v1/events?project_id="+string(DemoProjectID)+"&limit=1000", nil)
	rec := httptest.NewRecorder()
	h.app.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("events status=%d body=%s", rec.Code, rec.Body.String())
	}
	var got uiprojection.EventPage
	decode(t, rec, &got)
	return got
}

func decode(t *testing.T, rec *httptest.ResponseRecorder, target any) {
	t.Helper()
	if err := json.Unmarshal(rec.Body.Bytes(), target); err != nil {
		t.Fatalf("decode %T from %s: %v", target, rec.Body.String(), err)
	}
}

func node(t *testing.T, snapshot httpapi.CoordinationSnapshot, ref coordination.PhaseEndpointRef) httpapi.GraphNode {
	t.Helper()
	for _, node := range snapshot.Nodes {
		if node.TaskID == ref.TaskID && node.EndpointID == ref.EndpointID {
			return node
		}
	}
	t.Fatalf("node %v not found in %+v", ref, snapshot.Nodes)
	return httpapi.GraphNode{}
}

func hasEvent(page uiprojection.EventPage, eventType string) bool {
	for _, event := range page.Events {
		if event.Type == eventType {
			return true
		}
	}
	return false
}

func hasManagerInteractionKind(page uiprojection.EventPage, kind string) bool {
	for _, event := range page.Events {
		if event.Type == "manager.interaction" && event.Payload["kind"] == kind {
			return true
		}
	}
	return false
}
