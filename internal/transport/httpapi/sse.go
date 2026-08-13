package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/uiprojection"
)

const defaultEventStreamBuffer = 32

func (s *Server) handleEventStream(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	if s.events == nil {
		writeError(w, kernel.Error{Code: kernel.CodeInternalError, Message: "event query reader is not configured", Recoverable: true})
		return
	}
	after, err := eventStreamAfter(r)
	if err != nil {
		writeError(w, err)
		return
	}
	projectID := kernel.ProjectID(r.URL.Query().Get("project_id"))
	principal, ok := s.authenticateRead(w, r, projectID)
	if !ok {
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, kernel.Error{Code: kernel.CodeInternalError, Message: "streaming is not supported", Recoverable: true})
		return
	}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	source, err := s.events.SubscribeEvents(ctx, principal, projectID, after)
	if err != nil {
		writeEventError(w, err)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	buffer := make(chan uiprojection.UIEvent, s.eventBufferSize())
	go bufferStreamEvents(ctx, cancel, source, buffer)
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-buffer:
			if !ok {
				return
			}
			if err := writeSSEEvent(w, event); err != nil {
				cancel()
				return
			}
			flusher.Flush()
		}
	}
}

func eventStreamAfter(r *http.Request) (string, error) {
	queryAfter := strings.TrimSpace(r.URL.Query().Get("after"))
	lastEventID := strings.TrimSpace(r.Header.Get("Last-Event-ID"))
	// EventSource retains the original bootstrap URL while automatically
	// sending the newest delivered cursor in Last-Event-ID on reconnect. The
	// header must therefore supersede the stale query cursor.
	if lastEventID != "" {
		return lastEventID, nil
	}
	return queryAfter, nil
}

func (s *Server) eventBufferSize() int {
	if s.eventStreamBuffer <= 0 {
		return defaultEventStreamBuffer
	}
	return s.eventStreamBuffer
}

func bufferStreamEvents(ctx context.Context, cancel context.CancelFunc, source <-chan uiprojection.UIEvent, buffer chan<- uiprojection.UIEvent) {
	defer close(buffer)
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-source:
			if !ok {
				return
			}
			select {
			case buffer <- event:
			case <-ctx.Done():
				return
			default:
				cancel()
				return
			}
		}
	}
}

func writeSSEEvent(w http.ResponseWriter, event uiprojection.UIEvent) error {
	if event.EventID == "" || event.Cursor == "" || event.Type == "" || event.ProjectID == "" || event.OccurredAt.IsZero() || event.Payload == nil {
		return kernel.InvalidArgument("SSE event does not satisfy the canonical UiEvent contract")
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "id: %s\nevent: %s\ndata: %s\n\n", event.Cursor, event.Type, payload); err != nil {
		return err
	}
	return nil
}
