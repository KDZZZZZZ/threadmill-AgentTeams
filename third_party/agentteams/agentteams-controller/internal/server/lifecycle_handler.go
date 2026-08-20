package server

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"

	v1beta1 "github.com/agentscope-ai/AgentTeams/agentteams-controller/api/v1beta1"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/backend"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/httputil"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// LifecycleHandler handles imperative worker lifecycle operations.
type LifecycleHandler struct {
	k8s       client.Client
	registry  *backend.Registry
	namespace string

	readyMu sync.RWMutex
	ready   map[string]bool
	runtime map[string]WorkerRuntimeConfigStatus
}

func NewLifecycleHandler(k8s client.Client, registry *backend.Registry, namespace string) *LifecycleHandler {
	return &LifecycleHandler{
		k8s:       k8s,
		registry:  registry,
		namespace: namespace,
		ready:     make(map[string]bool),
		runtime:   make(map[string]WorkerRuntimeConfigStatus),
	}
}

// Wake handles POST /api/v1/workers/{name}/wake
func (h *LifecycleHandler) Wake(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if name == "" {
		httputil.WriteError(w, http.StatusBadRequest, "worker name is required")
		return
	}

	var worker v1beta1.Worker
	if err := h.k8s.Get(r.Context(), client.ObjectKey{Name: name, Namespace: h.namespace}, &worker); err != nil {
		writeK8sError(w, "get worker", err)
		return
	}

	// Set desired state in spec (declarative, triggers reconciler)
	running := "Running"
	worker.Spec.State = &running
	if err := h.k8s.Update(r.Context(), &worker); err != nil {
		writeK8sError(w, "update worker spec.state", err)
		return
	}

	// Directly operate on backend for immediate response
	b := h.registry.DetectWorkerBackend(r.Context())
	if b != nil {
		_ = b.Start(r.Context(), name)
	}

	h.setReady(name, false)

	// Refresh and update status
	_ = h.k8s.Get(r.Context(), client.ObjectKey{Name: name, Namespace: h.namespace}, &worker)
	worker.Status.Phase = "Running"
	worker.Status.Message = ""
	_ = h.k8s.Status().Update(r.Context(), &worker)

	httputil.WriteJSON(w, http.StatusOK, WorkerLifecycleResponse{Name: name, Phase: "Running"})
}

// Sleep handles POST /api/v1/workers/{name}/sleep
func (h *LifecycleHandler) Sleep(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if name == "" {
		httputil.WriteError(w, http.StatusBadRequest, "worker name is required")
		return
	}

	var worker v1beta1.Worker
	if err := h.k8s.Get(r.Context(), client.ObjectKey{Name: name, Namespace: h.namespace}, &worker); err != nil {
		writeK8sError(w, "get worker", err)
		return
	}

	// Set desired state in spec (declarative, triggers reconciler)
	sleeping := "Sleeping"
	worker.Spec.State = &sleeping
	if err := h.k8s.Update(r.Context(), &worker); err != nil {
		writeK8sError(w, "update worker spec.state", err)
		return
	}

	// Directly operate on backend for immediate response
	b := h.registry.DetectWorkerBackend(r.Context())
	if b != nil {
		_ = b.Stop(r.Context(), name)
	}

	h.setReady(name, false)

	// Refresh and update status
	_ = h.k8s.Get(r.Context(), client.ObjectKey{Name: name, Namespace: h.namespace}, &worker)
	worker.Status.Phase = "Sleeping"
	worker.Status.Message = ""
	_ = h.k8s.Status().Update(r.Context(), &worker)

	httputil.WriteJSON(w, http.StatusOK, WorkerLifecycleResponse{Name: name, Phase: "Sleeping"})
}

// EnsureReady handles POST /api/v1/workers/{name}/ensure-ready
func (h *LifecycleHandler) EnsureReady(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if name == "" {
		httputil.WriteError(w, http.StatusBadRequest, "worker name is required")
		return
	}

	var worker v1beta1.Worker
	if err := h.k8s.Get(r.Context(), client.ObjectKey{Name: name, Namespace: h.namespace}, &worker); err != nil {
		writeK8sError(w, "get worker", err)
		return
	}

	if worker.Status.Phase == "Stopped" || worker.Status.Phase == "Sleeping" {
		// Set desired state in spec (declarative)
		running := "Running"
		worker.Spec.State = &running
		if err := h.k8s.Update(r.Context(), &worker); err != nil {
			writeK8sError(w, "update worker spec.state", err)
			return
		}

		// Directly operate on backend for immediate response
		b := h.registry.DetectWorkerBackend(r.Context())
		if b != nil {
			if err := b.Start(r.Context(), name); err != nil {
				// Start may fail if container/pod was removed (Stopped state on K8s).
				// The reconciler will handle recreation.
				log.Printf("[WARN] ensure-ready start worker %s: %v (reconciler will retry)", name, err)
			}
		}

		h.setReady(name, false)

		// Refresh and update status
		_ = h.k8s.Get(r.Context(), client.ObjectKey{Name: name, Namespace: h.namespace}, &worker)
		worker.Status.Phase = "Running"
		worker.Status.Message = ""
		_ = h.k8s.Status().Update(r.Context(), &worker)
	}

	phase := worker.Status.Phase
	if phase == "Running" && h.isReady(name) {
		phase = "Ready"
	}

	httputil.WriteJSON(w, http.StatusOK, WorkerLifecycleResponse{Name: name, Phase: phase})
}

// Ready handles POST /api/v1/workers/{name}/ready — worker self-reports readiness.
func (h *LifecycleHandler) Ready(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if name == "" {
		httputil.WriteError(w, http.StatusBadRequest, "worker name is required")
		return
	}

	var report workerReadyReport
	if r.Body != nil {
		decoder := json.NewDecoder(r.Body)
		if err := decoder.Decode(&report); err != nil && !errors.Is(err, io.EOF) {
			httputil.WriteError(w, http.StatusBadRequest, "invalid ready report")
			return
		}
	}

	// Authorization (self-only for workers) is enforced by RequireAuthz middleware.
	h.setReady(name, true)
	if report.RuntimeConfig != nil {
		h.setRuntimeConfig(name, *report.RuntimeConfig)
	}
	log.Printf("[READY] Worker %s reported ready", name)
	w.WriteHeader(http.StatusNoContent)
}

// GetWorkerRuntimeStatus handles GET /api/v1/workers/{name}/status — aggregates CR + backend state.
func (h *LifecycleHandler) GetWorkerRuntimeStatus(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if name == "" {
		httputil.WriteError(w, http.StatusBadRequest, "worker name is required")
		return
	}

	var worker v1beta1.Worker
	if err := h.k8s.Get(r.Context(), client.ObjectKey{Name: name, Namespace: h.namespace}, &worker); err != nil {
		writeK8sError(w, "get worker", err)
		return
	}

	resp := workerToResponse(&worker)
	resp.RuntimeConfig = h.runtimeConfigStatus(name, &worker)

	b := h.registry.DetectWorkerBackend(r.Context())
	if b != nil {
		result, err := b.Status(r.Context(), name)
		if err == nil && result != nil {
			resp.Message = "backend=" + result.Backend + " status=" + string(result.Status)
			if result.Message != "" {
				resp.Message += " message=" + result.Message
			}
			resp.ContainerState = string(result.Status)
			if result.Status == backend.StatusRunning && h.isReady(name) && h.runtimeConfigReady(&worker) {
				resp.Phase = "Ready"
			}
		}
	}

	httputil.WriteJSON(w, http.StatusOK, resp)
}

type workerReadyReport struct {
	LastActiveAt  string                     `json:"lastActiveAt,omitempty"`
	RuntimeConfig *WorkerRuntimeConfigStatus `json:"runtimeConfig,omitempty"`
}

// --- readiness helpers ---

func (h *LifecycleHandler) setReady(name string, ready bool) {
	h.readyMu.Lock()
	defer h.readyMu.Unlock()
	if ready {
		h.ready[name] = true
	} else {
		delete(h.ready, name)
		// A stopped or restarted runtime has not yet re-applied its desired
		// configuration, even when the desired Worker generation is unchanged.
		delete(h.runtime, name)
	}
}

func (h *LifecycleHandler) isReady(name string) bool {
	h.readyMu.RLock()
	defer h.readyMu.RUnlock()
	return h.ready[name]
}

func (h *LifecycleHandler) setRuntimeConfig(name string, status WorkerRuntimeConfigStatus) {
	h.readyMu.Lock()
	defer h.readyMu.Unlock()
	h.runtime[name] = cloneRuntimeConfigStatus(status)
}

func (h *LifecycleHandler) runtimeConfigStatus(name string, worker *v1beta1.Worker) *WorkerRuntimeConfigStatus {
	h.readyMu.RLock()
	reported, ok := h.runtime[name]
	h.readyMu.RUnlock()
	if !ok && len(worker.Spec.McpServers) == 0 {
		return nil
	}

	status := cloneRuntimeConfigStatus(reported)
	status.DesiredGeneration = strconv.FormatInt(worker.Generation, 10)
	return &status
}

func (h *LifecycleHandler) runtimeConfigReady(worker *v1beta1.Worker) bool {
	if len(worker.Spec.McpServers) == 0 {
		return true
	}
	status := h.runtimeConfigStatus(worker.Name, worker)
	if status == nil || status.AppliedGeneration != status.DesiredGeneration {
		return false
	}
	applied := make(map[string]bool, len(status.MCPServers))
	for _, server := range status.MCPServers {
		applied[server.Name] = server.Applied && !server.Removed
	}
	for _, server := range worker.Spec.McpServers {
		if !applied[server.Name] {
			return false
		}
	}
	return true
}

func cloneRuntimeConfigStatus(status WorkerRuntimeConfigStatus) WorkerRuntimeConfigStatus {
	result := WorkerRuntimeConfigStatus{
		DesiredGeneration: status.DesiredGeneration,
		AppliedGeneration: status.AppliedGeneration,
	}
	for _, server := range status.MCPServers {
		result.MCPServers = append(result.MCPServers, WorkerMCPServerApplyStatus{
			Name:        strings.TrimSpace(server.Name),
			Applied:     server.Applied,
			HeaderNames: append([]string(nil), server.HeaderNames...),
			Removed:     server.Removed,
			Error:       strings.TrimSpace(server.Error),
		})
	}
	return result
}

func writeBackendError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, backend.ErrNotFound):
		httputil.WriteError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, backend.ErrConflict):
		httputil.WriteError(w, http.StatusConflict, err.Error())
	default:
		httputil.WriteError(w, http.StatusInternalServerError, err.Error())
	}
}
