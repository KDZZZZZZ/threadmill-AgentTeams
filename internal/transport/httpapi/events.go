package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/uiprojection"
)

const (
	defaultEventPageLimit = 100
	maxEventPageLimit     = 1000
)

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	if s.events == nil {
		writeError(w, kernel.Error{Code: kernel.CodeInternalError, Message: "event query reader is not configured", Recoverable: true})
		return
	}
	projectID := kernel.ProjectID(r.URL.Query().Get("project_id"))
	principal, ok := s.authenticateRead(w, r, projectID)
	if !ok {
		return
	}
	limit, err := parseEventLimit(r.URL.Query().Get("limit"))
	if err != nil {
		writeError(w, err)
		return
	}
	after := r.URL.Query().Get("after")
	page, err := s.events.ListEvents(r.Context(), principal, projectID, after, limit)
	writeEventResult(w, page, err)
}

func parseEventLimit(raw string) (int, error) {
	if raw == "" {
		return defaultEventPageLimit, nil
	}
	limit, err := strconv.Atoi(raw)
	if err != nil || limit < 1 || limit > maxEventPageLimit {
		return 0, kernel.InvalidArgument("limit must be an integer from 1 to 1000")
	}
	return limit, nil
}

func writeEventResult(w http.ResponseWriter, value any, err error) {
	if err != nil {
		writeEventError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(value)
}

func writeEventError(w http.ResponseWriter, err error) {
	if uiprojection.IsCursorExpired(err) {
		writeCodedError(w, err, http.StatusGone)
		return
	}
	writeError(w, err)
}

func writeCodedError(w http.ResponseWriter, err error, status int) {
	var coded kernel.Error
	if !errors.As(err, &coded) {
		coded = kernel.Error{Code: kernel.ErrorCodeOf(err), Message: err.Error()}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(coded)
}
