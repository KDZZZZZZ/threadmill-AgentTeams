package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/httputil"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/service"
)

// MCPCredentialBindingHandler exposes a deliberately redacted control-plane
// API. The private header value is accepted only on create and is never
// returned by this handler.
type MCPCredentialBindingHandler struct {
	store service.MCPCredentialBindingStore
}

func NewMCPCredentialBindingHandler(store service.MCPCredentialBindingStore) *MCPCredentialBindingHandler {
	return &MCPCredentialBindingHandler{store: store}
}

type createMCPCredentialBindingRequest struct {
	WorkerName  string `json:"workerName"`
	HeaderName  string `json:"headerName"`
	SecretValue string `json:"secretValue"`
}

func (h *MCPCredentialBindingHandler) Create(w http.ResponseWriter, r *http.Request) {
	if h.store == nil {
		httputil.WriteError(w, http.StatusServiceUnavailable, "mcp credential binding store is not available")
		return
	}
	var request createMCPCredentialBindingRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	view, err := h.store.Create(r.Context(), service.MCPCredentialBinding{
		WorkerName: strings.TrimSpace(request.WorkerName),
		HeaderName: strings.TrimSpace(request.HeaderName),
		Value:      request.SecretValue,
	})
	if err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "invalid mcp credential binding")
		return
	}
	httputil.WriteJSON(w, http.StatusCreated, view)
}

func (h *MCPCredentialBindingHandler) Get(w http.ResponseWriter, r *http.Request) {
	if h.store == nil {
		httputil.WriteError(w, http.StatusServiceUnavailable, "mcp credential binding store is not available")
		return
	}
	view, err := h.store.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		writeMCPCredentialBindingError(w, err)
		return
	}
	httputil.WriteJSON(w, http.StatusOK, view)
}

func (h *MCPCredentialBindingHandler) Revoke(w http.ResponseWriter, r *http.Request) {
	if h.store == nil {
		httputil.WriteError(w, http.StatusServiceUnavailable, "mcp credential binding store is not available")
		return
	}
	if err := h.store.Revoke(r.Context(), r.PathValue("id")); err != nil {
		writeMCPCredentialBindingError(w, err)
		return
	}
	view, err := h.store.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		writeMCPCredentialBindingError(w, err)
		return
	}
	httputil.WriteJSON(w, http.StatusOK, view)
}

func writeMCPCredentialBindingError(w http.ResponseWriter, err error) {
	if errors.Is(err, service.ErrMCPBindingNotFound) {
		httputil.WriteError(w, http.StatusNotFound, "mcp credential binding not found")
		return
	}
	httputil.WriteError(w, http.StatusBadRequest, "invalid mcp credential binding")
}
