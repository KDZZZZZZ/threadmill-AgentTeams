package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/uiprojection"
)

func TestEventPaginationUsesReadOnlyEventQueryAndLimitBounds(t *testing.T) {
	h := newHTTPHarness()
	rec := h.get("/v1/events?project_id=project-a&after=cursor-7&limit=25")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if h.events.projectID != "project-a" || h.events.after != "cursor-7" || h.events.limit != 25 {
		t.Fatalf("event query project=%q after=%q limit=%d", h.events.projectID, h.events.after, h.events.limit)
	}
	if h.guard.checked {
		t.Fatal("CSRF guard should not run for event pagination")
	}

	rec = h.get("/v1/events?project_id=project-a&limit=1001")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("limit status = %d body=%s, want 400", rec.Code, rec.Body.String())
	}
}

func TestEventPaginationMapsExpiredCursorTo410(t *testing.T) {
	h := newHTTPHarness()
	h.events.pageErr = uiprojection.CursorExpired("cursor-old")
	rec := h.get("/v1/events?project_id=project-a&after=cursor-old")
	if rec.Code != http.StatusGone {
		t.Fatalf("status = %d body=%s, want 410", rec.Code, rec.Body.String())
	}
	var got kernel.Error
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Code != kernel.ErrorCode("cursor_expired") || !got.Recoverable {
		t.Fatalf("cursor error = %#v", got)
	}
}

func TestEventStreamUsesLastEventIDAndWritesExactSSEFields(t *testing.T) {
	h := newHTTPHarness()
	ch := make(chan uiprojection.UIEvent, 1)
	ch <- uiprojection.UIEvent{
		EventID:    "evt-8",
		Cursor:     "cursor-8",
		Type:       "task.updated",
		ProjectID:  "project-a",
		TaskID:     "task-a",
		OccurredAt: time.Date(2026, 8, 11, 12, 30, 0, 0, time.UTC),
		Payload:    map[string]any{"task_id": "task-a", "status": "running"},
	}
	close(ch)
	h.events.stream = ch

	req := httptest.NewRequest(http.MethodGet, "/v1/events/stream?project_id=project-a", nil)
	req.AddCookie(&http.Cookie{Name: auth.SessionCookieName, Value: "session"})
	req.Header.Set("Last-Event-ID", "cursor-7")
	rec := httptest.NewRecorder()
	h.handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if h.events.streamAfter != "cursor-7" {
		t.Fatalf("stream after = %q", h.events.streamAfter)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "id: cursor-8\n") || !strings.Contains(body, "event: task.updated\n") || !strings.Contains(body, "data: {") {
		t.Fatalf("SSE body missing exact fields:\n%s", body)
	}
	if strings.Contains(body, "raw_tool_output") || strings.Contains(body, "transcript") {
		t.Fatalf("SSE body leaked raw data: %s", body)
	}
}

func TestEventStreamLastEventIDOverridesBootstrapCursor(t *testing.T) {
	h := newHTTPHarness()
	req := httptest.NewRequest(http.MethodGet, "/v1/events/stream?project_id=project-a&after=query-cursor", nil)
	req.AddCookie(&http.Cookie{Name: auth.SessionCookieName, Value: "session"})
	req.Header.Set("Last-Event-ID", "header-cursor")
	rec := httptest.NewRecorder()
	h.handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s, want 200", rec.Code, rec.Body.String())
	}
	if h.events.streamAfter != "header-cursor" {
		t.Fatalf("stream after = %q, want reconnect header cursor", h.events.streamAfter)
	}
}

func TestEventStreamCancelsWhenBoundedBufferFills(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	source := make(chan uiprojection.UIEvent, 3)
	source <- uiprojection.UIEvent{EventID: "evt-1", Cursor: "1", Type: "task.updated"}
	source <- uiprojection.UIEvent{EventID: "evt-2", Cursor: "2", Type: "task.updated"}
	source <- uiprojection.UIEvent{EventID: "evt-3", Cursor: "3", Type: "task.updated"}
	buffer := make(chan uiprojection.UIEvent, 1)

	bufferStreamEvents(ctx, cancel, source, buffer)
	if ctx.Err() == nil {
		t.Fatal("context was not canceled when the bounded stream buffer filled")
	}
	if len(buffer) != 1 {
		t.Fatalf("buffer len = %d, want bounded single event", len(buffer))
	}
}
