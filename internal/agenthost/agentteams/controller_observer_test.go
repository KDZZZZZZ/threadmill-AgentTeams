package agentteams

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime"
)

type observingTaskflow struct {
	mu       sync.Mutex
	snapshot TeamHarnessTaskSnapshot
	err      error
	calls    int
}

func (f *observingTaskflow) DelegateTask(context.Context, TeamHarnessDelegateTaskRequest) error {
	return errors.New("write must not be called by observation")
}
func (f *observingTaskflow) CheckTask(context.Context, string) (TeamHarnessTaskSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.snapshot, f.err
}

func observationRequest() runtime.PhysicalExecutionObservationRequest {
	return runtime.PhysicalExecutionObservationRequest{
		TaskID: "task", InvocationID: "invocation-b", Generation: 3, ExecutionEpoch: 2,
		WorkerID: "tm-invocation-b-g3-e2", WorkerName: "tm-invocation-b-g3-e2",
		TeamHarnessTaskID: "tm-phase-invocation-b-g3-e2", DesiredGeneration: 7, AppliedGeneration: 7,
		MCPClientID: "threadmill", AgentSessionRef: "matrix:!room:test",
	}
}

func observationStatus(phase, container, desired, applied string, mcpApplied bool) map[string]any {
	return map[string]any{
		"name": "tm-invocation-b-g3-e2", "state": "Running", "phase": phase, "containerState": container,
		"roomID": "!room:test", "runtimeConfig": map[string]any{
			"desiredGeneration": desired, "appliedGeneration": applied,
			"mcpServers": []map[string]any{{"name": "threadmill", "applied": mcpApplied, "headerNames": []string{"X-Threadmill-Execution-Token"}}},
		},
	}
}

func TestControllerPhysicalExecutionObserverMapsReadOnlyStatus(t *testing.T) {
	cases := []struct {
		name       string
		status     int
		body       map[string]any
		task       TeamHarnessTaskSnapshot
		wantWorker runtime.ObservedWorkerState
		wantTask   runtime.ObservedTaskState
		wantRun    runtime.ObservedRuntimeState
		wantMCP    runtime.ObservedMCPState
		wantID     runtime.ObservedCarrierIdentity
	}{
		{"ready", http.StatusOK, observationStatus("Ready", "running", "7", "7", true), TeamHarnessTaskSnapshot{TaskID: "tm-phase-invocation-b-g3-e2", Status: TeamHarnessTaskInProgress}, runtime.ObservedWorkerReady, runtime.ObservedTaskInProgress, runtime.ObservedRuntimeApplied, runtime.ObservedMCPApplied, runtime.ObservedCarrierIdentityVerified},
		{"provisioning", http.StatusOK, observationStatus("Starting", "creating", "7", "", false), TeamHarnessTaskSnapshot{TaskID: "tm-phase-invocation-b-g3-e2", Status: TeamHarnessTaskAssigned}, runtime.ObservedWorkerProvisioning, runtime.ObservedTaskAssigned, runtime.ObservedRuntimeGenerationPending, runtime.ObservedMCPNotApplied, runtime.ObservedCarrierIdentityVerified},
		{"terminating", http.StatusOK, observationStatus("Stopping", "stopping", "7", "7", true), TeamHarnessTaskSnapshot{TaskID: "tm-phase-invocation-b-g3-e2", Status: TeamHarnessTaskCancelled}, runtime.ObservedWorkerTerminating, runtime.ObservedTaskCancelled, runtime.ObservedRuntimeApplied, runtime.ObservedMCPApplied, runtime.ObservedCarrierIdentityVerified},
		{"failed", http.StatusOK, observationStatus("Failed", "failed", "7", "7", false), TeamHarnessTaskSnapshot{TaskID: "tm-phase-invocation-b-g3-e2", Status: TeamHarnessTaskFailed}, runtime.ObservedWorkerFailed, runtime.ObservedTaskFailed, runtime.ObservedRuntimeApplied, runtime.ObservedMCPNotApplied, runtime.ObservedCarrierIdentityVerified},
		{"not found", http.StatusNotFound, nil, TeamHarnessTaskSnapshot{TaskID: "tm-phase-invocation-b-g3-e2", Status: TeamHarnessTaskSubmitted}, runtime.ObservedWorkerNotFound, runtime.ObservedTaskCompleted, runtime.ObservedRuntimeUnknown, runtime.ObservedMCPUnknown, runtime.ObservedCarrierIdentityUnknown},
		{"generation mismatch", http.StatusOK, observationStatus("Ready", "running", "8", "8", true), TeamHarnessTaskSnapshot{TaskID: "tm-phase-invocation-b-g3-e2", Status: TeamHarnessTaskInProgress}, runtime.ObservedWorkerReady, runtime.ObservedTaskInProgress, runtime.ObservedRuntimeGenerationMismatch, runtime.ObservedMCPApplied, runtime.ObservedCarrierIdentityMismatch},
		{"runtime pending", http.StatusOK, observationStatus("Ready", "running", "7", "6", true), TeamHarnessTaskSnapshot{TaskID: "tm-phase-invocation-b-g3-e2", Status: TeamHarnessTaskInProgress}, runtime.ObservedWorkerReady, runtime.ObservedTaskInProgress, runtime.ObservedRuntimeGenerationPending, runtime.ObservedMCPApplied, runtime.ObservedCarrierIdentityVerified},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			var methods []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				methods = append(methods, request.Method)
				if request.Method != http.MethodGet || request.URL.Path != "/api/v1/workers/tm-invocation-b-g3-e2/status" {
					t.Fatalf("unexpected write/request: %s %s", request.Method, request.URL.Path)
				}
				if test.status != http.StatusOK {
					http.Error(w, "not found", test.status)
					return
				}
				_ = jsonEncode(w, test.body)
			}))
			defer server.Close()
			taskflow := &observingTaskflow{snapshot: test.task}
			observer := &ControllerPhysicalExecutionObserver{Controller: &ControllerReprovisioner{BaseURL: server.URL}, Taskflow: taskflow, Now: func() time.Time { return time.Date(2026, 8, 22, 0, 0, 0, 0, time.UTC) }}
			got, err := observer.Observe(context.Background(), observationRequest())
			if err != nil {
				t.Fatal(err)
			}
			if got.Worker != test.wantWorker || got.Task != test.wantTask || got.Runtime != test.wantRun || got.MCP != test.wantMCP || got.Identity != test.wantID {
				t.Fatalf("observation=%#v", got)
			}
			if len(methods) != 1 || methods[0] != http.MethodGet || taskflow.calls != 1 {
				t.Fatalf("observation was not read-only methods=%v taskCalls=%d", methods, taskflow.calls)
			}
		})
	}
}

func TestControllerPhysicalExecutionObserverRejectsIdentityAndPreservesUnknownFailures(t *testing.T) {
	t.Run("task identity mismatch", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
			_ = jsonEncode(w, observationStatus("Ready", "running", "7", "7", true))
		}))
		defer server.Close()
		observer := &ControllerPhysicalExecutionObserver{Controller: &ControllerReprovisioner{BaseURL: server.URL}, Taskflow: &observingTaskflow{snapshot: TeamHarnessTaskSnapshot{TaskID: "other-task", Status: TeamHarnessTaskInProgress}}}
		got, err := observer.Observe(context.Background(), observationRequest())
		if err != nil || got.Identity != runtime.ObservedCarrierIdentityMismatch {
			t.Fatalf("observation=%#v err=%v", got, err)
		}
	})
	t.Run("controller unavailable is not absent", func(t *testing.T) {
		client := &http.Client{Transport: roundTripError{err: context.DeadlineExceeded}}
		observer := &ControllerPhysicalExecutionObserver{Controller: &ControllerReprovisioner{BaseURL: "http://controller.invalid", HTTPClient: client}, Taskflow: &observingTaskflow{snapshot: TeamHarnessTaskSnapshot{TaskID: "tm-phase-invocation-b-g3-e2", Status: TeamHarnessTaskInProgress}}}
		got, err := observer.Observe(context.Background(), observationRequest())
		if err != nil || got.Worker != runtime.ObservedWorkerUnknown || len(got.SourceErrors) != 1 || got.SourceErrors[0].Kind != "timeout" {
			t.Fatalf("observation=%#v err=%v", got, err)
		}
	})
	t.Run("controller server error is not absent", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
			http.Error(w, "controller unavailable", http.StatusServiceUnavailable)
		}))
		defer server.Close()
		observer := &ControllerPhysicalExecutionObserver{Controller: &ControllerReprovisioner{BaseURL: server.URL}, Taskflow: &observingTaskflow{snapshot: TeamHarnessTaskSnapshot{TaskID: "tm-phase-invocation-b-g3-e2", Status: TeamHarnessTaskInProgress}}}
		got, err := observer.Observe(context.Background(), observationRequest())
		if err != nil || got.Worker != runtime.ObservedWorkerUnknown || len(got.SourceErrors) != 1 || got.SourceErrors[0].Kind != "server_error" {
			t.Fatalf("observation=%#v err=%v", got, err)
		}
	})
	t.Run("task missing is not phase output", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
			_ = jsonEncode(w, observationStatus("Ready", "running", "7", "7", true))
		}))
		defer server.Close()
		observer := &ControllerPhysicalExecutionObserver{Controller: &ControllerReprovisioner{BaseURL: server.URL}, Taskflow: &observingTaskflow{err: errors.New("task not found")}}
		got, err := observer.Observe(context.Background(), observationRequest())
		if err != nil || got.Task != runtime.ObservedTaskNotFound || got.Worker != runtime.ObservedWorkerReady || got.Identity != runtime.ObservedCarrierIdentityUnknown {
			t.Fatalf("observation=%#v err=%v", got, err)
		}
	})
}

type roundTripError struct{ err error }

func (r roundTripError) RoundTrip(*http.Request) (*http.Response, error) { return nil, r.err }

func jsonEncode(w http.ResponseWriter, value any) error {
	w.Header().Set("Content-Type", "application/json")
	return json.NewEncoder(w).Encode(value)
}
