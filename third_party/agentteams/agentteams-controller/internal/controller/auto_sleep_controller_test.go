package controller

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	v1beta1 "github.com/agentscope-ai/AgentTeams/agentteams-controller/api/v1beta1"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/backend"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/server"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestAutoSleepControllerSleepsIdleStandaloneWorker(t *testing.T) {
	scheme := newControllerTestScheme(t)
	worker := &v1beta1.Worker{
		ObjectMeta: metav1.ObjectMeta{Name: "alpha-dev", Namespace: "default"},
		Spec: v1beta1.WorkerSpec{
			IdleTimeout: "15m",
		},
		Status: v1beta1.WorkerStatus{
			LastActiveAt: "2026-05-12T10:00:00Z",
		},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(worker).Build()
	controller := &AutoSleepController{
		Client:    k8sClient,
		Namespace: "default",
		Now:       func() time.Time { return mustParseTime(t, "2026-05-12T10:16:00Z") },
	}

	controller.reconcile(context.Background())

	var updated v1beta1.Worker
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Name: "alpha-dev", Namespace: "default"}, &updated); err != nil {
		t.Fatalf("get worker: %v", err)
	}
	if updated.Spec.DesiredState() != "Sleeping" {
		t.Fatalf("state=%q, want Sleeping", updated.Spec.DesiredState())
	}
}

func TestAutoSleepControllerLeavesThreadmillManagedWorkerRunning(t *testing.T) {
	scheme := newControllerTestScheme(t)
	worker := &v1beta1.Worker{
		ObjectMeta: metav1.ObjectMeta{Name: "threadmill-phase-a", Namespace: "default"},
		Spec: v1beta1.WorkerSpec{
			IdleTimeout: "15m",
		},
		Status: v1beta1.WorkerStatus{
			LastActiveAt: "2026-05-12T10:00:00Z",
		},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(worker).Build()
	controller := &AutoSleepController{
		Client:    k8sClient,
		Namespace: "default",
		Now:       func() time.Time { return mustParseTime(t, "2026-05-12T10:16:00Z") },
	}

	controller.reconcile(context.Background())

	var updated v1beta1.Worker
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Name: worker.Name, Namespace: worker.Namespace}, &updated); err != nil {
		t.Fatalf("get worker: %v", err)
	}
	if updated.Spec.DesiredState() != "Running" {
		t.Fatalf("state=%q, want Threadmill runtime to retain lifecycle ownership", updated.Spec.DesiredState())
	}
}

func TestAutoSleepControllerSleepsIdleTeamWorker(t *testing.T) {
	scheme := newControllerTestScheme(t)
	worker := &v1beta1.Worker{
		ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: "default"},
		Spec: v1beta1.WorkerSpec{
			IdleTimeout: "15m",
		},
		Status: v1beta1.WorkerStatus{
			LastActiveAt: "2026-05-12T10:00:00Z",
		},
	}
	team := &v1beta1.Team{
		ObjectMeta: metav1.ObjectMeta{Name: "alpha", Namespace: "default"},
		Spec: v1beta1.TeamSpec{
			WorkerMembers: []v1beta1.TeamWorkerRef{{Name: "dev", Role: RoleTeamWorker.String()}},
		},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(worker, team).Build()
	controller := &AutoSleepController{
		Client:    k8sClient,
		Namespace: "default",
		Now:       func() time.Time { return mustParseTime(t, "2026-05-12T10:16:00Z") },
	}

	controller.reconcile(context.Background())

	var updated v1beta1.Worker
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Name: "dev", Namespace: "default"}, &updated); err != nil {
		t.Fatalf("get worker: %v", err)
	}
	if updated.Spec.DesiredState() != "Sleeping" {
		t.Fatalf("state=%q, want Sleeping", updated.Spec.DesiredState())
	}
}

func TestAutoSleepControllerDoesNotTreatHeartbeatAsActivity(t *testing.T) {
	scheme := newControllerTestScheme(t)
	worker := &v1beta1.Worker{
		ObjectMeta: metav1.ObjectMeta{Name: "alpha-dev", Namespace: "default"},
		Spec: v1beta1.WorkerSpec{
			IdleTimeout: "15m",
		},
		Status: v1beta1.WorkerStatus{
			LastActiveAt:  "2026-05-12T10:00:00Z",
			LastHeartbeat: "2026-05-12T10:15:30Z",
		},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(worker).Build()
	controller := &AutoSleepController{
		Client:    k8sClient,
		Namespace: "default",
		Now:       func() time.Time { return mustParseTime(t, "2026-05-12T10:16:00Z") },
	}

	controller.reconcile(context.Background())

	var updated v1beta1.Worker
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Name: "alpha-dev", Namespace: "default"}, &updated); err != nil {
		t.Fatalf("get worker: %v", err)
	}
	if updated.Spec.DesiredState() != "Sleeping" {
		t.Fatalf("state=%q, want Sleeping because heartbeat is not business activity", updated.Spec.DesiredState())
	}
}

func TestAutoSleepControllerStaysRunningWhenLastActiveIsRecent(t *testing.T) {
	scheme := newControllerTestScheme(t)
	worker := &v1beta1.Worker{
		ObjectMeta: metav1.ObjectMeta{Name: "alpha-dev", Namespace: "default"},
		Spec: v1beta1.WorkerSpec{
			IdleTimeout: "15m",
		},
		Status: v1beta1.WorkerStatus{
			LastActiveAt:  "2026-05-12T10:15:30Z",
			LastHeartbeat: "2026-05-12T10:00:30Z",
		},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(worker).Build()
	controller := &AutoSleepController{
		Client:    k8sClient,
		Namespace: "default",
		Now:       func() time.Time { return mustParseTime(t, "2026-05-12T10:16:00Z") },
	}

	controller.reconcile(context.Background())

	var updated v1beta1.Worker
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Name: "alpha-dev", Namespace: "default"}, &updated); err != nil {
		t.Fatalf("get worker: %v", err)
	}
	if updated.Spec.DesiredState() != "Running" {
		t.Fatalf("state=%q, want Running when lastActiveAt is recent", updated.Spec.DesiredState())
	}
}

func TestAutoSleepControllerDoesNotImmediatelySleepAfterEnsureReadyGrace(t *testing.T) {
	scheme := newControllerTestScheme(t)
	worker := &v1beta1.Worker{
		ObjectMeta: metav1.ObjectMeta{Name: "alpha-dev", Namespace: "default"},
		Spec: v1beta1.WorkerSpec{
			IdleTimeout: "15m",
		},
		Status: v1beta1.WorkerStatus{
			LastActiveAt:  "2026-05-12T10:15:30Z",
			LastHeartbeat: "2026-05-12T10:15:30Z",
		},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(worker).Build()
	controller := &AutoSleepController{
		Client:    k8sClient,
		Namespace: "default",
		Now:       func() time.Time { return mustParseTime(t, "2026-05-12T10:16:40Z") },
	}

	controller.reconcile(context.Background())

	var updated v1beta1.Worker
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Name: "alpha-dev", Namespace: "default"}, &updated); err != nil {
		t.Fatalf("get worker: %v", err)
	}
	if updated.Spec.DesiredState() != "Running" {
		t.Fatalf("state=%q, want Running within ensure-ready grace window", updated.Spec.DesiredState())
	}
}

func TestAutoSleepControllerDoesNotSleepRunningWorkerTouchedByEnsureReady(t *testing.T) {
	scheme := newControllerTestScheme(t)
	running := "Running"
	worker := &v1beta1.Worker{
		ObjectMeta: metav1.ObjectMeta{Name: "alpha-dev", Namespace: "default"},
		Spec: v1beta1.WorkerSpec{
			State:       &running,
			IdleTimeout: "15m",
		},
		Status: v1beta1.WorkerStatus{
			Phase:         "Running",
			LastActiveAt:  "2000-01-01T00:00:00Z",
			LastHeartbeat: "2000-01-01T00:00:00Z",
		},
	}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.Worker{}).
		WithObjects(worker).
		Build()
	lifecycle := server.NewLifecycleHandler(k8sClient, backend.NewRegistry(nil), "default")
	req := httptest.NewRequest(http.MethodPost, "/api/v1/workers/alpha-dev/ensure-ready", nil)
	req.SetPathValue("name", "alpha-dev")
	rec := httptest.NewRecorder()

	lifecycle.EnsureReady(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("ensure-ready status=%d body=%s", rec.Code, rec.Body.String())
	}
	controller := &AutoSleepController{
		Client:    k8sClient,
		Namespace: "default",
		Now:       func() time.Time { return time.Now().UTC().Add(time.Minute) },
	}
	controller.reconcile(context.Background())

	var updated v1beta1.Worker
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Name: "alpha-dev", Namespace: "default"}, &updated); err != nil {
		t.Fatalf("get worker: %v", err)
	}
	if updated.Spec.DesiredState() != "Running" {
		t.Fatalf("state=%q, want Running after ensure-ready touched active time", updated.Spec.DesiredState())
	}
	if updated.Status.LastActiveAt == "2000-01-01T00:00:00Z" {
		t.Fatal("ensure-ready did not refresh lastActiveAt before auto-sleep reconciliation")
	}
}

func newControllerTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	return scheme
}

func mustParseTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		t.Fatalf("parse time: %v", err)
	}
	return parsed
}
