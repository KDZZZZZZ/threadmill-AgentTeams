package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	v1beta1 "github.com/agentscope-ai/AgentTeams/agentteams-controller/api/v1beta1"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/backend"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestLifecycleSleepSetsSleepingPhase(t *testing.T) {
	scheme := newLifecycleTestScheme(t)
	worker := &v1beta1.Worker{
		ObjectMeta: metav1.ObjectMeta{Name: "alpha-dev", Namespace: "default"},
		Status:     v1beta1.WorkerStatus{Phase: "Running"},
	}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.Worker{}).
		WithObjects(worker).
		Build()
	backendStub := &stubWorkerBackend{status: backend.StatusStopped}
	handler := NewLifecycleHandler(k8sClient, backend.NewRegistry([]backend.WorkerBackend{backendStub}), "default")

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workers/alpha-dev/sleep", nil)
	req.SetPathValue("name", "alpha-dev")
	rec := httptest.NewRecorder()

	handler.Sleep(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
	}
	if backendStub.stopCalls != 1 {
		t.Fatalf("expected one stop call, got %d", backendStub.stopCalls)
	}

	var updated v1beta1.Worker
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Name: "alpha-dev", Namespace: "default"}, &updated); err != nil {
		t.Fatalf("get worker: %v", err)
	}
	if updated.Status.Phase != "Sleeping" {
		t.Fatalf("expected phase Sleeping, got %q", updated.Status.Phase)
	}
	if updated.Spec.DesiredState() != "Sleeping" {
		t.Fatalf("expected spec.state Sleeping, got %q", updated.Spec.DesiredState())
	}

	var resp WorkerLifecycleResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Phase != "Sleeping" {
		t.Fatalf("expected response phase Sleeping, got %q", resp.Phase)
	}
}

func TestLifecycleWakeSetsRunningPhase(t *testing.T) {
	scheme := newLifecycleTestScheme(t)
	sleeping := "Sleeping"
	worker := &v1beta1.Worker{
		ObjectMeta: metav1.ObjectMeta{Name: "alpha-dev", Namespace: "default"},
		Spec:       v1beta1.WorkerSpec{State: &sleeping},
		Status:     v1beta1.WorkerStatus{Phase: "Sleeping"},
	}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.Worker{}).
		WithObjects(worker).
		Build()
	backendStub := &stubWorkerBackend{status: backend.StatusRunning}
	handler := NewLifecycleHandler(k8sClient, backend.NewRegistry([]backend.WorkerBackend{backendStub}), "default")

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workers/alpha-dev/wake", nil)
	req.SetPathValue("name", "alpha-dev")
	rec := httptest.NewRecorder()

	handler.Wake(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	var updated v1beta1.Worker
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Name: "alpha-dev", Namespace: "default"}, &updated); err != nil {
		t.Fatalf("get worker: %v", err)
	}
	if updated.Status.Phase != "Running" {
		t.Fatalf("expected phase Running, got %q", updated.Status.Phase)
	}
	if updated.Spec.DesiredState() != "Running" {
		t.Fatalf("expected spec.state Running, got %q", updated.Spec.DesiredState())
	}
}

func TestLifecycleEnsureReadyStartsSleepingWorker(t *testing.T) {
	scheme := newLifecycleTestScheme(t)
	worker := &v1beta1.Worker{
		ObjectMeta: metav1.ObjectMeta{Name: "alpha-dev", Namespace: "default"},
		Status:     v1beta1.WorkerStatus{Phase: "Sleeping"},
	}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.Worker{}).
		WithObjects(worker).
		Build()
	backendStub := &stubWorkerBackend{status: backend.StatusRunning}
	handler := NewLifecycleHandler(k8sClient, backend.NewRegistry([]backend.WorkerBackend{backendStub}), "default")

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workers/alpha-dev/ensure-ready", nil)
	req.SetPathValue("name", "alpha-dev")
	rec := httptest.NewRecorder()

	handler.EnsureReady(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	var updated v1beta1.Worker
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Name: "alpha-dev", Namespace: "default"}, &updated); err != nil {
		t.Fatalf("get worker: %v", err)
	}
	if updated.Status.Phase != "Running" {
		t.Fatalf("expected phase Running, got %q", updated.Status.Phase)
	}
	if updated.Spec.DesiredState() != "Running" {
		t.Fatalf("expected spec.state Running, got %q", updated.Spec.DesiredState())
	}
}

func TestLifecycleRuntimeStatusRequiresCurrentAppliedGeneration(t *testing.T) {
	scheme := newLifecycleTestScheme(t)
	worker := &v1beta1.Worker{
		ObjectMeta: metav1.ObjectMeta{Name: "alpha-dev", Namespace: "default", Generation: 7},
		Spec: v1beta1.WorkerSpec{McpServers: []v1beta1.MCPServer{{
			Name: "threadmill", URL: "http://threadmill.test/mcp", Transport: "streamable_http",
		}}},
		Status: v1beta1.WorkerStatus{Phase: "Running"},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(worker).Build()
	handler := NewLifecycleHandler(k8sClient, backend.NewRegistry([]backend.WorkerBackend{
		&stubWorkerBackend{status: backend.StatusRunning},
	}), "default")

	ready := httptest.NewRequest(http.MethodPost, "/api/v1/workers/alpha-dev/ready", strings.NewReader(`{
		"runtimeConfig":{"appliedGeneration":"6","mcpServers":[{
			"name":"threadmill","applied":true,"headerNames":["X-Threadmill-Execution-Token"]
		}]}}`))
	ready.SetPathValue("name", "alpha-dev")
	readyRec := httptest.NewRecorder()
	handler.Ready(readyRec, ready)
	if readyRec.Code != http.StatusNoContent {
		t.Fatalf("expected stale ready report accepted, got %d: %s", readyRec.Code, readyRec.Body.String())
	}

	statusReq := httptest.NewRequest(http.MethodGet, "/api/v1/workers/alpha-dev/status", nil)
	statusReq.SetPathValue("name", "alpha-dev")
	statusRec := httptest.NewRecorder()
	handler.GetWorkerRuntimeStatus(statusRec, statusReq)
	if statusRec.Code != http.StatusOK {
		t.Fatalf("status request: %d: %s", statusRec.Code, statusRec.Body.String())
	}
	var stale WorkerResponse
	if err := json.Unmarshal(statusRec.Body.Bytes(), &stale); err != nil {
		t.Fatalf("decode stale status: %v", err)
	}
	if stale.Phase == "Ready" {
		t.Fatal("stale applied generation must not mark worker Ready")
	}
	if stale.RuntimeConfig == nil || stale.RuntimeConfig.DesiredGeneration != "7" || stale.RuntimeConfig.AppliedGeneration != "6" {
		t.Fatalf("unexpected stale runtime status: %#v", stale.RuntimeConfig)
	}

	ready = httptest.NewRequest(http.MethodPost, "/api/v1/workers/alpha-dev/ready", strings.NewReader(`{
		"runtimeConfig":{"appliedGeneration":"7","mcpServers":[{
			"name":"threadmill","applied":true,"headerNames":["X-Threadmill-Execution-Token"]
		}]}}`))
	ready.SetPathValue("name", "alpha-dev")
	readyRec = httptest.NewRecorder()
	handler.Ready(readyRec, ready)
	if readyRec.Code != http.StatusNoContent {
		t.Fatalf("expected current ready report accepted, got %d: %s", readyRec.Code, readyRec.Body.String())
	}

	statusRec = httptest.NewRecorder()
	handler.GetWorkerRuntimeStatus(statusRec, statusReq)
	var current WorkerResponse
	if err := json.Unmarshal(statusRec.Body.Bytes(), &current); err != nil {
		t.Fatalf("decode current status: %v", err)
	}
	if current.Phase != "Ready" {
		t.Fatalf("expected ready after current MCP apply, got %q", current.Phase)
	}
	if strings.Contains(statusRec.Body.String(), "test-threadmill-token-a") {
		t.Fatal("runtime status must not expose MCP header values")
	}
}

func TestLifecycleRuntimeStatusReportsFailedAndRemovedMCP(t *testing.T) {
	scheme := newLifecycleTestScheme(t)
	worker := &v1beta1.Worker{
		ObjectMeta: metav1.ObjectMeta{Name: "alpha-dev", Namespace: "default", Generation: 9},
		Spec:       v1beta1.WorkerSpec{McpServers: []v1beta1.MCPServer{{Name: "threadmill", URL: "http://threadmill.test/mcp"}}},
		Status:     v1beta1.WorkerStatus{Phase: "Running"},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(worker).Build()
	handler := NewLifecycleHandler(k8sClient, backend.NewRegistry([]backend.WorkerBackend{
		&stubWorkerBackend{status: backend.StatusRunning},
	}), "default")

	report := `{"runtimeConfig":{"appliedGeneration":"9","mcpServers":[{"name":"threadmill","applied":false,"error":"QwenPawApiError"},{"name":"old","applied":false,"removed":true}]}}`
	ready := httptest.NewRequest(http.MethodPost, "/api/v1/workers/alpha-dev/ready", strings.NewReader(report))
	ready.SetPathValue("name", "alpha-dev")
	handler.Ready(httptest.NewRecorder(), ready)

	request := httptest.NewRequest(http.MethodGet, "/api/v1/workers/alpha-dev/status", nil)
	request.SetPathValue("name", "alpha-dev")
	response := httptest.NewRecorder()
	handler.GetWorkerRuntimeStatus(response, request)
	var status WorkerResponse
	if err := json.Unmarshal(response.Body.Bytes(), &status); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if status.Phase == "Ready" {
		t.Fatal("failed MCP apply must not mark worker Ready")
	}
	if status.RuntimeConfig == nil || len(status.RuntimeConfig.MCPServers) != 2 {
		t.Fatalf("unexpected runtime config: %#v", status.RuntimeConfig)
	}
	if got := status.RuntimeConfig.MCPServers[0]; got.Error != "QwenPawApiError" || got.Applied {
		t.Fatalf("failed MCP status lost: %#v", got)
	}
	if got := status.RuntimeConfig.MCPServers[1]; !got.Removed || got.Applied {
		t.Fatalf("removed MCP status lost: %#v", got)
	}
}

func newLifecycleTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("add agentteams scheme: %v", err)
	}
	return scheme
}

type stubWorkerBackend struct {
	status     backend.WorkerStatus
	startCalls int
	stopCalls  int
}

func (s *stubWorkerBackend) Name() string                   { return "stub" }
func (s *stubWorkerBackend) DeploymentMode() string         { return backend.DeployLocal }
func (s *stubWorkerBackend) Available(context.Context) bool { return true }
func (s *stubWorkerBackend) NeedsCredentialInjection() bool { return false }
func (s *stubWorkerBackend) Create(context.Context, backend.CreateRequest) (*backend.WorkerResult, error) {
	return nil, nil
}
func (s *stubWorkerBackend) Delete(context.Context, string) error { return nil }
func (s *stubWorkerBackend) Start(_ context.Context, _ string) error {
	s.startCalls++
	return nil
}
func (s *stubWorkerBackend) Stop(_ context.Context, _ string) error {
	s.stopCalls++
	return nil
}
func (s *stubWorkerBackend) Status(context.Context, string) (*backend.WorkerResult, error) {
	return &backend.WorkerResult{Backend: "stub", Status: s.status}, nil
}
