package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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
	if updated.Status.LastHeartbeat == "" || updated.Status.LastActiveAt == "" {
		t.Fatalf("expected ensure-ready to refresh heartbeat/active time, got heartbeat=%q active=%q", updated.Status.LastHeartbeat, updated.Status.LastActiveAt)
	}
	if updated.Status.LastHeartbeat != updated.Status.LastActiveAt {
		t.Fatalf("ensure-ready heartbeat/active mismatch: %q/%q", updated.Status.LastHeartbeat, updated.Status.LastActiveAt)
	}
}

func TestLifecycleEnsureReadyTouchesRunningWorkerWithoutRestart(t *testing.T) {
	scheme := newLifecycleTestScheme(t)
	running := "Running"
	worker := &v1beta1.Worker{
		ObjectMeta: metav1.ObjectMeta{Name: "alpha-dev", Namespace: "default"},
		Spec:       v1beta1.WorkerSpec{State: &running, IdleTimeout: "15m"},
		Status: v1beta1.WorkerStatus{
			Phase:         "Running",
			Message:       "ready",
			LastHeartbeat: "2000-01-01T00:00:00Z",
			LastActiveAt:  "2000-01-01T00:00:00Z",
		},
	}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.Worker{}).
		WithObjects(worker).
		Build()
	backendStub := &stubWorkerBackend{status: backend.StatusRunning}
	handler := NewLifecycleHandler(k8sClient, backend.NewRegistry([]backend.WorkerBackend{backendStub}), "default")
	handler.setReady("alpha-dev", true)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workers/alpha-dev/ensure-ready", nil)
	req.SetPathValue("name", "alpha-dev")
	rec := httptest.NewRecorder()

	handler.EnsureReady(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
	}
	if backendStub.startCalls != 0 || backendStub.stopCalls != 0 || backendStub.deleteCalls != 0 {
		t.Fatalf("running ensure-ready should not operate backend, start=%d stop=%d delete=%d", backendStub.startCalls, backendStub.stopCalls, backendStub.deleteCalls)
	}
	var resp WorkerLifecycleResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Phase != "Ready" {
		t.Fatalf("response phase=%q, want Ready for running worker marked ready", resp.Phase)
	}
	var updated v1beta1.Worker
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Name: "alpha-dev", Namespace: "default"}, &updated); err != nil {
		t.Fatalf("get worker: %v", err)
	}
	if updated.Spec.DesiredState() != "Running" || updated.Status.Phase != "Running" || updated.Status.Message != "ready" {
		t.Fatalf("running ensure-ready changed worker state/status: desired=%q phase=%q message=%q", updated.Spec.DesiredState(), updated.Status.Phase, updated.Status.Message)
	}
	if updated.Status.LastHeartbeat == "2000-01-01T00:00:00Z" || updated.Status.LastActiveAt == "2000-01-01T00:00:00Z" {
		t.Fatalf("running ensure-ready did not touch heartbeat/active time: heartbeat=%q active=%q", updated.Status.LastHeartbeat, updated.Status.LastActiveAt)
	}
	if updated.Status.LastHeartbeat != updated.Status.LastActiveAt {
		t.Fatalf("running ensure-ready heartbeat/active mismatch: %q/%q", updated.Status.LastHeartbeat, updated.Status.LastActiveAt)
	}
}

func TestLifecycleEnsureReadyDoesNotMutateFailedWorker(t *testing.T) {
	scheme := newLifecycleTestScheme(t)
	running := "Running"
	worker := &v1beta1.Worker{
		ObjectMeta: metav1.ObjectMeta{Name: "alpha-dev", Namespace: "default"},
		Spec:       v1beta1.WorkerSpec{State: &running},
		Status: v1beta1.WorkerStatus{
			Phase:         "Failed",
			Message:       "crash loop",
			LastHeartbeat: "2000-01-01T00:00:00Z",
			LastActiveAt:  "2000-01-01T00:00:00Z",
		},
	}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.Worker{}).
		WithObjects(worker).
		Build()
	backendStub := &stubWorkerBackend{status: backend.StatusStopped}
	handler := NewLifecycleHandler(k8sClient, backend.NewRegistry([]backend.WorkerBackend{backendStub}), "default")

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workers/alpha-dev/ensure-ready", nil)
	req.SetPathValue("name", "alpha-dev")
	rec := httptest.NewRecorder()

	handler.EnsureReady(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
	}
	if backendStub.startCalls != 0 || backendStub.stopCalls != 0 || backendStub.deleteCalls != 0 {
		t.Fatalf("failed ensure-ready should not operate backend, start=%d stop=%d delete=%d", backendStub.startCalls, backendStub.stopCalls, backendStub.deleteCalls)
	}
	var updated v1beta1.Worker
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Name: "alpha-dev", Namespace: "default"}, &updated); err != nil {
		t.Fatalf("get worker: %v", err)
	}
	if updated.Status.Phase != "Failed" || updated.Status.Message != "crash loop" || updated.Status.LastHeartbeat != "2000-01-01T00:00:00Z" || updated.Status.LastActiveAt != "2000-01-01T00:00:00Z" {
		t.Fatalf("failed ensure-ready mutated status: %#v", updated.Status)
	}
}

func TestLifecycleEnsureReadyDeletesStaleImageInsteadOfStarting(t *testing.T) {
	scheme := newLifecycleTestScheme(t)
	worker := &v1beta1.Worker{
		ObjectMeta: metav1.ObjectMeta{Name: "alpha-dev", Namespace: "default"},
		Spec:       v1beta1.WorkerSpec{Image: "agentteams/qwenpaw-worker:current"},
		Status:     v1beta1.WorkerStatus{Phase: "Sleeping"},
	}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.Worker{}).
		WithObjects(worker).
		Build()
	backendStub := &stubWorkerBackend{
		status: backend.StatusStopped,
		image:  "agentteams/qwenpaw-worker:v1.2.1",
	}
	handler := NewLifecycleHandler(k8sClient, backend.NewRegistry([]backend.WorkerBackend{backendStub}), "default")

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workers/alpha-dev/ensure-ready", nil)
	req.SetPathValue("name", "alpha-dev")
	rec := httptest.NewRecorder()

	handler.EnsureReady(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
	}
	if backendStub.deleteCalls != 1 {
		t.Fatalf("expected stale worker delete, got %d", backendStub.deleteCalls)
	}
	if backendStub.startCalls != 0 {
		t.Fatalf("stale worker was started, start calls=%d", backendStub.startCalls)
	}
}

func TestLifecycleEnsureReadyDeletesSameTagStaleDigest(t *testing.T) {
	scheme := newLifecycleTestScheme(t)
	worker := &v1beta1.Worker{
		ObjectMeta: metav1.ObjectMeta{Name: "alpha-dev", Namespace: "default"},
		Spec:       v1beta1.WorkerSpec{Image: "agentteams/qwenpaw-worker:threadmill-current"},
		Status:     v1beta1.WorkerStatus{Phase: "Sleeping"},
	}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.Worker{}).
		WithObjects(worker).
		Build()
	backendStub := &stubWorkerBackend{
		status:  backend.StatusStopped,
		image:   "agentteams/qwenpaw-worker:threadmill-current",
		imageID: "sha256:old",
		imageIDs: map[string]string{
			"agentteams/qwenpaw-worker:threadmill-current": "sha256:new",
		},
	}
	handler := NewLifecycleHandler(k8sClient, backend.NewRegistry([]backend.WorkerBackend{backendStub}), "default")

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workers/alpha-dev/ensure-ready", nil)
	req.SetPathValue("name", "alpha-dev")
	rec := httptest.NewRecorder()

	handler.EnsureReady(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
	}
	if backendStub.deleteCalls != 1 {
		t.Fatalf("expected stale digest delete, got %d", backendStub.deleteCalls)
	}
	if backendStub.startCalls != 0 {
		t.Fatalf("stale digest worker was started, start calls=%d", backendStub.startCalls)
	}
}

func TestLifecycleEnsureReadySameTagSameDigestStarts(t *testing.T) {
	scheme := newLifecycleTestScheme(t)
	worker := &v1beta1.Worker{
		ObjectMeta: metav1.ObjectMeta{Name: "alpha-dev", Namespace: "default"},
		Spec:       v1beta1.WorkerSpec{Image: "agentteams/qwenpaw-worker:threadmill-current"},
		Status:     v1beta1.WorkerStatus{Phase: "Sleeping"},
	}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.Worker{}).
		WithObjects(worker).
		Build()
	backendStub := &stubWorkerBackend{
		status:  backend.StatusStopped,
		image:   "agentteams/qwenpaw-worker:threadmill-current",
		imageID: "sha256:same",
		imageIDs: map[string]string{
			"agentteams/qwenpaw-worker:threadmill-current": "sha256:same",
		},
	}
	handler := NewLifecycleHandler(k8sClient, backend.NewRegistry([]backend.WorkerBackend{backendStub}), "default")

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workers/alpha-dev/ensure-ready", nil)
	req.SetPathValue("name", "alpha-dev")
	rec := httptest.NewRecorder()

	handler.EnsureReady(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
	}
	if backendStub.deleteCalls != 0 {
		t.Fatalf("same digest should not delete, got %d", backendStub.deleteCalls)
	}
	if backendStub.startCalls != 1 {
		t.Fatalf("same digest worker should start, start calls=%d", backendStub.startCalls)
	}
}

func TestLifecycleEnsureReadyUnknownDesiredDigestStarts(t *testing.T) {
	scheme := newLifecycleTestScheme(t)
	worker := &v1beta1.Worker{
		ObjectMeta: metav1.ObjectMeta{Name: "alpha-dev", Namespace: "default"},
		Spec:       v1beta1.WorkerSpec{Image: "agentteams/qwenpaw-worker:threadmill-current"},
		Status:     v1beta1.WorkerStatus{Phase: "Sleeping"},
	}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.Worker{}).
		WithObjects(worker).
		Build()
	backendStub := &stubWorkerBackend{
		status:     backend.StatusStopped,
		image:      "agentteams/qwenpaw-worker:threadmill-current",
		imageID:    "sha256:old",
		resolveErr: errors.New("image inspect failed"),
	}
	handler := NewLifecycleHandler(k8sClient, backend.NewRegistry([]backend.WorkerBackend{backendStub}), "default")

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workers/alpha-dev/ensure-ready", nil)
	req.SetPathValue("name", "alpha-dev")
	rec := httptest.NewRecorder()

	handler.EnsureReady(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
	}
	if backendStub.deleteCalls != 0 {
		t.Fatalf("unknown desired digest should not delete, got %d", backendStub.deleteCalls)
	}
	if backendStub.startCalls != 1 {
		t.Fatalf("unknown desired digest should start, start calls=%d", backendStub.startCalls)
	}
}

func TestLifecycleRuntimeStatusIncludesLastHeartbeat(t *testing.T) {
	scheme := newLifecycleTestScheme(t)
	worker := &v1beta1.Worker{
		ObjectMeta: metav1.ObjectMeta{Name: "alpha-dev", Namespace: "default"},
		Status:     v1beta1.WorkerStatus{Phase: "Running", LastHeartbeat: "2026-08-12T14:32:00Z"},
	}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.Worker{}).
		WithObjects(worker).
		Build()
	backendStub := &stubWorkerBackend{status: backend.StatusRunning}
	handler := NewLifecycleHandler(k8sClient, backend.NewRegistry([]backend.WorkerBackend{backendStub}), "default")

	req := httptest.NewRequest(http.MethodGet, "/api/v1/workers/alpha-dev/status", nil)
	req.SetPathValue("name", "alpha-dev")
	rec := httptest.NewRecorder()

	handler.GetWorkerRuntimeStatus(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
	}
	var resp WorkerResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.LastHeartbeat != "2026-08-12T14:32:00Z" {
		t.Fatalf("lastHeartbeat=%q, want CR status heartbeat", resp.LastHeartbeat)
	}
}

func TestLifecycleReadyPersistsHeartbeatAndLastActiveAt(t *testing.T) {
	scheme := newLifecycleTestScheme(t)
	worker := &v1beta1.Worker{
		ObjectMeta: metav1.ObjectMeta{Name: "alpha-dev", Namespace: "default"},
	}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.Worker{}).
		WithObjects(worker).
		Build()
	handler := NewLifecycleHandler(k8sClient, backend.NewRegistry(nil), "default")

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workers/alpha-dev/ready", bytes.NewReader([]byte(`{"lastActiveAt":"2026-08-12T14:33:00Z"}`)))
	req.SetPathValue("name", "alpha-dev")
	rec := httptest.NewRecorder()

	handler.Ready(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected status %d, got %d: %s", http.StatusNoContent, rec.Code, rec.Body.String())
	}
	if !handler.isReady("alpha-dev") {
		t.Fatal("worker was not marked ready after successful status persistence")
	}
	var updated v1beta1.Worker
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Name: "alpha-dev", Namespace: "default"}, &updated); err != nil {
		t.Fatalf("get worker: %v", err)
	}
	if updated.Status.LastHeartbeat == "" {
		t.Fatal("lastHeartbeat was not persisted")
	}
	if _, err := time.Parse(time.RFC3339, updated.Status.LastHeartbeat); err != nil {
		t.Fatalf("lastHeartbeat is not RFC3339: %q", updated.Status.LastHeartbeat)
	}
	if updated.Status.LastActiveAt != "2026-08-12T14:33:00Z" {
		t.Fatalf("lastActiveAt=%q, want payload timestamp", updated.Status.LastActiveAt)
	}
}

func TestLifecycleReadyWithoutPayloadDoesNotRefreshActiveTime(t *testing.T) {
	scheme := newLifecycleTestScheme(t)
	worker := &v1beta1.Worker{
		ObjectMeta: metav1.ObjectMeta{Name: "alpha-dev", Namespace: "default"},
		Status:     v1beta1.WorkerStatus{LastActiveAt: "2000-01-01T00:00:00Z"},
	}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.Worker{}).
		WithObjects(worker).
		Build()
	handler := NewLifecycleHandler(k8sClient, backend.NewRegistry(nil), "default")

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workers/alpha-dev/ready", nil)
	req.SetPathValue("name", "alpha-dev")
	rec := httptest.NewRecorder()

	handler.Ready(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected status %d, got %d: %s", http.StatusNoContent, rec.Code, rec.Body.String())
	}
	var updated v1beta1.Worker
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Name: "alpha-dev", Namespace: "default"}, &updated); err != nil {
		t.Fatalf("get worker: %v", err)
	}
	if updated.Status.LastActiveAt != "2000-01-01T00:00:00Z" {
		t.Fatalf("lastActiveAt=%q, want unchanged without authoritative payload", updated.Status.LastActiveAt)
	}
	if updated.Status.LastHeartbeat == "" || updated.Status.LastHeartbeat == updated.Status.LastActiveAt {
		t.Fatalf("ready without payload should refresh only heartbeat, got heartbeat=%q active=%q", updated.Status.LastHeartbeat, updated.Status.LastActiveAt)
	}
}

func TestLifecycleReadyRefreshesHeartbeatOnRepeat(t *testing.T) {
	scheme := newLifecycleTestScheme(t)
	worker := &v1beta1.Worker{
		ObjectMeta: metav1.ObjectMeta{Name: "alpha-dev", Namespace: "default"},
		Status:     v1beta1.WorkerStatus{LastHeartbeat: "2000-01-01T00:00:00Z"},
	}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.Worker{}).
		WithObjects(worker).
		Build()
	handler := NewLifecycleHandler(k8sClient, backend.NewRegistry(nil), "default")

	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/workers/alpha-dev/ready", nil)
		req.SetPathValue("name", "alpha-dev")
		rec := httptest.NewRecorder()
		handler.Ready(rec, req)
		if rec.Code != http.StatusNoContent {
			t.Fatalf("ready call %d status=%d body=%s", i+1, rec.Code, rec.Body.String())
		}
		if i == 0 {
			time.Sleep(1100 * time.Millisecond)
		}
	}

	var updated v1beta1.Worker
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Name: "alpha-dev", Namespace: "default"}, &updated); err != nil {
		t.Fatalf("get worker: %v", err)
	}
	if updated.Status.LastHeartbeat == "2000-01-01T00:00:00Z" {
		t.Fatal("lastHeartbeat was not refreshed")
	}
}

func TestLifecycleReadyPayloadOnlyAdvancesLastActiveAt(t *testing.T) {
	scheme := newLifecycleTestScheme(t)
	worker := &v1beta1.Worker{
		ObjectMeta: metav1.ObjectMeta{Name: "alpha-dev", Namespace: "default"},
		Status:     v1beta1.WorkerStatus{LastActiveAt: "2026-08-12T14:40:00Z"},
	}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.Worker{}).
		WithObjects(worker).
		Build()
	handler := NewLifecycleHandler(k8sClient, backend.NewRegistry(nil), "default")

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workers/alpha-dev/ready", bytes.NewReader([]byte(`{"lastActiveAt":"2026-08-12T14:33:00Z"}`)))
	req.SetPathValue("name", "alpha-dev")
	rec := httptest.NewRecorder()

	handler.Ready(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected status %d, got %d: %s", http.StatusNoContent, rec.Code, rec.Body.String())
	}
	var updated v1beta1.Worker
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Name: "alpha-dev", Namespace: "default"}, &updated); err != nil {
		t.Fatalf("get worker: %v", err)
	}
	if updated.Status.LastActiveAt != "2026-08-12T14:40:00Z" {
		t.Fatalf("lastActiveAt=%q, want unchanged by older payload", updated.Status.LastActiveAt)
	}
}

func TestLifecycleReadyRejectsInvalidPayload(t *testing.T) {
	scheme := newLifecycleTestScheme(t)
	worker := &v1beta1.Worker{
		ObjectMeta: metav1.ObjectMeta{Name: "alpha-dev", Namespace: "default"},
	}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.Worker{}).
		WithObjects(worker).
		Build()
	handler := NewLifecycleHandler(k8sClient, backend.NewRegistry(nil), "default")

	for _, body := range []string{
		`{"lastActiveAt":"not-a-time"}`,
		`{"lastActiveAt":"2026-08-12T14:33:00Z","phase":"Ready"}`,
	} {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/workers/alpha-dev/ready", bytes.NewReader([]byte(body)))
		req.SetPathValue("name", "alpha-dev")
		rec := httptest.NewRecorder()
		handler.Ready(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("body %s: expected status %d, got %d: %s", body, http.StatusBadRequest, rec.Code, rec.Body.String())
		}
	}
	if handler.isReady("alpha-dev") {
		t.Fatal("invalid payload marked worker ready")
	}
}

func TestLifecycleReadyStatusUpdateFailureDoesNotMarkReady(t *testing.T) {
	scheme := newLifecycleTestScheme(t)
	worker := &v1beta1.Worker{
		ObjectMeta: metav1.ObjectMeta{Name: "alpha-dev", Namespace: "default"},
	}
	base := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.Worker{}).
		WithObjects(worker).
		Build()
	handler := NewLifecycleHandler(failingStatusClient{Client: base}, backend.NewRegistry(nil), "default")

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workers/alpha-dev/ready", nil)
	req.SetPathValue("name", "alpha-dev")
	rec := httptest.NewRecorder()
	handler.Ready(rec, req)

	if rec.Code < 500 {
		t.Fatalf("expected non-2xx status update failure, got %d: %s", rec.Code, rec.Body.String())
	}
	if handler.isReady("alpha-dev") {
		t.Fatal("failed status update marked worker ready")
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
	status      backend.WorkerStatus
	image       string
	imageID     string
	imageIDs    map[string]string
	resolveErr  error
	deleteCalls int
	startCalls  int
	stopCalls   int
}

func (s *stubWorkerBackend) Name() string                   { return "stub" }
func (s *stubWorkerBackend) DeploymentMode() string         { return backend.DeployLocal }
func (s *stubWorkerBackend) Available(context.Context) bool { return true }
func (s *stubWorkerBackend) NeedsCredentialInjection() bool { return false }
func (s *stubWorkerBackend) Create(context.Context, backend.CreateRequest) (*backend.WorkerResult, error) {
	return nil, nil
}
func (s *stubWorkerBackend) Delete(context.Context, string) error {
	s.deleteCalls++
	return nil
}
func (s *stubWorkerBackend) Start(_ context.Context, _ string) error {
	s.startCalls++
	return nil
}
func (s *stubWorkerBackend) Stop(_ context.Context, _ string) error {
	s.stopCalls++
	return nil
}
func (s *stubWorkerBackend) Status(context.Context, string) (*backend.WorkerResult, error) {
	return &backend.WorkerResult{Backend: "stub", Status: s.status, Image: s.image, ImageID: s.imageID}, nil
}
func (s *stubWorkerBackend) ResolveImageID(_ context.Context, image string) (string, error) {
	if s.resolveErr != nil {
		return "", s.resolveErr
	}
	return s.imageIDs[image], nil
}

type failingStatusClient struct {
	client.Client
}

func (c failingStatusClient) Status() client.SubResourceWriter {
	return failingStatusWriter{}
}

type failingStatusWriter struct{}

func (failingStatusWriter) Create(context.Context, client.Object, client.Object, ...client.SubResourceCreateOption) error {
	return errors.New("forced status create failure")
}

func (failingStatusWriter) Update(context.Context, client.Object, ...client.SubResourceUpdateOption) error {
	return errors.New("forced status update failure")
}

func (failingStatusWriter) Patch(context.Context, client.Object, client.Patch, ...client.SubResourcePatchOption) error {
	return errors.New("forced status patch failure")
}
