package agentteams

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/artifacts"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/phaseagent"
)

type rehydrationEnvelopeResolver struct{ envelope HostEnvelope }

func (r rehydrationEnvelopeResolver) ResolveHostEnvelope(context.Context, phaseagent.ExecutionContext) (HostEnvelope, error) {
	return r.envelope, nil
}

func TestControllerReprovisionerUsesRedactedCredentialAndReadyStatus(t *testing.T) {
	const secret = "test-threadmill-token-b"
	var credentialPayload map[string]any
	var workerPayload map[string]any
	ensureReadyCalls := 0
	requests := make([]string, 0)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.Method+" "+r.URL.Path)
		if got := r.Header.Get("Authorization"); got != "Bearer controller-auth" {
			t.Fatalf("authorization = %q", got)
		}
		switch r.Method + " " + r.URL.Path {
		case "POST /api/v1/mcp-credentials":
			if err := json.NewDecoder(r.Body).Decode(&credentialPayload); err != nil {
				t.Fatal(err)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "credential-b", "workerName": "tm-invocation-a-g3-e2", "headerName": "X-Threadmill-Execution-Token", "state": "active"})
		case "POST /api/v1/workers":
			if err := json.NewDecoder(r.Body).Decode(&workerPayload); err != nil {
				t.Fatal(err)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"name": "tm-invocation-a-g3-e2"})
		case "POST /api/v1/workers/tm-invocation-a-g3-e2/ensure-ready":
			ensureReadyCalls++
			if ensureReadyCalls == 1 {
				http.Error(w, "worker desired state is not visible yet", http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		case "GET /api/v1/workers/tm-invocation-a-g3-e2/status":
			_ = json.NewEncoder(w).Encode(readyWorkerStatus())
		case "POST /api/v1/mcp-credentials/credential-b/revoke", "DELETE /api/v1/workers/tm-invocation-a-g3-e2":
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected controller request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	client := &ControllerReprovisioner{BaseURL: server.URL, BearerToken: "controller-auth", Model: "test-model", Runtime: "qwenpaw", PollInterval: time.Millisecond}
	credential, err := client.CreateMCPCredential(context.Background(), runtime.MCPCredentialRequest{WorkerName: "tm-invocation-a-g3-e2", HeaderName: "X-Threadmill-Execution-Token", Token: secret})
	if err != nil {
		t.Fatal(err)
	}
	if credential.Ref != "credential-b" || credential.WorkerName != "tm-invocation-a-g3-e2" {
		t.Fatalf("credential = %#v", credential)
	}
	worker, err := client.ProvisionWorker(context.Background(), runtime.WorkerProvisionRequest{WorkerName: credential.WorkerName, CredentialRef: credential.Ref, MCPName: "threadmill", MCPURL: "http://phase-mcp.test/mcp", Transport: "streamable_http"})
	if err != nil {
		t.Fatal(err)
	}
	if worker.RuntimeGeneration != 7 || worker.MCPClientID != "threadmill" {
		t.Fatalf("worker = %#v", worker)
	}
	if ensureReadyCalls != 2 {
		t.Fatalf("ensure-ready calls = %d, want transient 404 retry", ensureReadyCalls)
	}
	readback, err := client.WaitForRuntimeReady(context.Background(), worker)
	if err != nil {
		t.Fatal(err)
	}
	if !readback.BackendRunning || !readback.APIVersionReady || !readback.MCPApplied || readback.AppliedGeneration != 7 {
		t.Fatalf("readback = %#v", readback)
	}
	if err := client.RevokeMCPCredential(context.Background(), credential); err != nil {
		t.Fatal(err)
	}
	if err := client.DeleteWorker(context.Background(), worker); err != nil {
		t.Fatal(err)
	}

	if credentialPayload["secretValue"] != secret {
		t.Fatalf("credential payload did not contain supplied private value")
	}
	encoded, _ := json.Marshal(workerPayload)
	if strings.Contains(string(encoded), secret) || strings.Contains(string(encoded), "controller-auth") {
		t.Fatalf("worker desired state leaked a secret: %s", encoded)
	}
	mcpServers, ok := workerPayload["mcpServers"].([]any)
	if !ok || len(mcpServers) != 1 {
		t.Fatalf("mcpServers = %#v", workerPayload["mcpServers"])
	}
	mcp := mcpServers[0].(map[string]any)
	if mcp["credentialBindingRef"] != "credential-b" || mcp["url"] != "http://phase-mcp.test/mcp" || mcp["transport"] != "streamable_http" {
		t.Fatalf("mcp desired state = %#v", mcp)
	}
	if !containsRequest(requests, "POST /api/v1/mcp-credentials/credential-b/revoke") || !containsRequest(requests, "DELETE /api/v1/workers/tm-invocation-a-g3-e2") {
		t.Fatalf("cleanup requests = %#v", requests)
	}
}

func TestControllerReprovisionerRejectsUnreadyOrRedactedMCPStatus(t *testing.T) {
	worker := runtime.ProvisionedWorker{ID: "worker", Name: "worker", RuntimeGeneration: 2, MCPClientID: "threadmill"}
	status := controllerWorkerStatus{Name: "worker", Phase: "Ready", ContainerState: "running"}
	status.RuntimeConfig.DesiredGeneration = "2"
	status.RuntimeConfig.AppliedGeneration = "1"
	status.RuntimeConfig.MCPServers = append(status.RuntimeConfig.MCPServers, struct {
		Name        string   `json:"name"`
		Applied     bool     `json:"applied"`
		HeaderNames []string `json:"headerNames"`
		Removed     bool     `json:"removed"`
		Error       string   `json:"error"`
	}{Name: "threadmill", Applied: true, HeaderNames: []string{"X-Threadmill-Execution-Token"}})
	readback, err := controllerReadback(status, worker)
	if err != nil {
		t.Fatal(err)
	}
	if readback.AppliedGeneration == worker.RuntimeGeneration {
		t.Fatalf("stale generation reported current: %#v", readback)
	}
	encoded, _ := json.Marshal(readback)
	if strings.Contains(string(encoded), "test-threadmill-token") {
		t.Fatalf("readback leaked token: %s", encoded)
	}
}

func TestControllerReprovisionerDeleteWorkerTreatsNotFoundAsIdempotentCleanup(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete || r.URL.Path != "/api/v1/workers/worker-gone" {
			t.Fatalf("request=%s %s", r.Method, r.URL.Path)
		}
		http.Error(w, "delete worker: not found", http.StatusNotFound)
	}))
	defer server.Close()
	client := &ControllerReprovisioner{BaseURL: server.URL}
	if err := client.DeleteWorker(context.Background(), runtime.ProvisionedWorker{Name: "worker-gone"}); err != nil {
		t.Fatalf("repeated worker cleanup must accept not-found: %v", err)
	}
}

func TestRehydratedTeamHarnessTaskActivatorUsesObservedWorkerAcceptance(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/workers/worker-b/status" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"name": "worker-b", "roomID": "!worker-b:test", "matrixUserID": "@worker-b:test"})
	}))
	defer server.Close()
	client := &fakeTaskflowClient{snapshots: []TeamHarnessTaskSnapshot{{TaskID: "tm-phase-invocation-b-g3-e2", AssignedTo: "@worker-b:test", Status: TeamHarnessTaskInProgress, Acknowledged: true}}}
	activator := &RehydratedTeamHarnessTaskActivator{Controller: &ControllerReprovisioner{BaseURL: server.URL}, Taskflow: client, PollInterval: time.Millisecond}
	pkg := runtime.RehydratedExecutionPackage{TaskID: "task-b", InvocationID: "invocation-b", Generation: 3, ExecutionEpoch: 2, TaskContract: "task contract", PhaseInstruction: "execute"}
	task, err := activator.CreateTeamHarnessTask(context.Background(), runtime.TeamHarnessTaskRequest{Plan: runtime.RehydrationPlan{TaskID: "task-b", InvocationID: "invocation-b", Generation: 3, NextExecutionEpoch: 2, Endpoint: phaseagent.PhaseEndpointRef{EndpointID: string(phaseagent.PhaseExecute)}}, Worker: runtime.ProvisionedWorker{ID: "worker-b-id", Name: "worker-b"}, Package: pkg})
	if err != nil {
		t.Fatal(err)
	}
	if task.ID != "tm-phase-invocation-b-g3-e2" || task.AssignedTo != "worker-b" || task.Status != string(TeamHarnessTaskInProgress) || !task.Acknowledged {
		t.Fatalf("activation=%#v", task)
	}
	wantDigest, err := runtime.RehydratedExecutionPackageDigest(pkg)
	if err != nil {
		t.Fatal(err)
	}
	if task.AgentPackageDigest != wantDigest || task.AgentSessionRef != "matrix:!worker-b:test" {
		t.Fatalf("agent-start receipt=%#v", task)
	}
	if len(client.delegates) != 1 || client.delegates[0].RoomID != "!worker-b:test" || client.delegates[0].Assignee != "@worker-b:test" {
		t.Fatalf("delegate request=%#v", client.delegates)
	}
	if !strings.Contains(client.delegates[0].Spec, `"task_contract": "task contract"`) || !strings.Contains(client.delegates[0].Spec, "agent.submitPhaseOutput") {
		t.Fatalf("task spec did not carry the controlled package: %s", client.delegates[0].Spec)
	}
	if !strings.Contains(client.delegates[0].Spec, wantDigest) || !strings.Contains(client.delegates[0].Spec, "runtime.confirmPackageConsumption") || !strings.Contains(client.delegates[0].Spec, "fresh physical session") {
		t.Fatalf("task spec omitted fresh-session package receipt: %s", client.delegates[0].Spec)
	}
}

func TestRehydratedTaskActivationRejectsUnacknowledgedSession(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"name": "worker-b", "roomID": "!worker-b:test", "matrixUserID": "@worker-b:test"})
	}))
	defer server.Close()
	client := &fakeTaskflowClient{snapshots: []TeamHarnessTaskSnapshot{{TaskID: "tm-phase-invocation-b-g3-e2", AssignedTo: "@worker-b:test", Status: TeamHarnessTaskInProgress, Acknowledged: false}}}
	activator := &RehydratedTeamHarnessTaskActivator{Controller: &ControllerReprovisioner{BaseURL: server.URL}, Taskflow: client, PollInterval: time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err := activator.CreateTeamHarnessTask(ctx, runtime.TeamHarnessTaskRequest{Plan: runtime.RehydrationPlan{TaskID: "task-b", InvocationID: "invocation-b", Generation: 3, NextExecutionEpoch: 2, Endpoint: phaseagent.PhaseEndpointRef{EndpointID: "execute"}}, Worker: runtime.ProvisionedWorker{ID: "worker-b-id", Name: "worker-b"}, Package: runtime.RehydratedExecutionPackage{TaskID: "task-b"}})
	if err == nil {
		t.Fatal("unacknowledged QwenPaw session was accepted")
	}
	if len(client.cancellations) != 1 || !strings.Contains(client.cancellations[0], "tm-phase-invocation-b-g3-e2") {
		t.Fatalf("unaccepted task was not cancelled: %#v", client.cancellations)
	}
}

func TestRehydratedHostPackageMaterializerProjectsOnlyAgentVisibleEnvelope(t *testing.T) {
	const secret = "private-token-value"
	inputs := phaseagent.PhaseInputSet{InputRevision: "r5", Delivered: []phaseagent.InputDelivery{{InputID: "review", PhaseOutputRef: "phase-output-review"}}}
	plan := runtime.RehydrationPlan{
		TaskID: "task-a", InvocationID: "invocation-a", Generation: 3, NextExecutionEpoch: 2,
		Endpoint: phaseagent.PhaseEndpointRef{TaskID: "task-a", EndpointID: "execute"}, NewBindingRef: "B2", NewInputRevision: "r5",
		Inputs: inputs, NewlyDelivered: append([]phaseagent.InputDelivery(nil), inputs.Delivered...),
		Context:      runtime.RehydratedContext{SliceRef: "context-slice", BaselineRef: "context-baseline"},
		TaskMemory:   runtime.RehydratedTaskMemory{View: phaseagent.TaskMemoryBufferView{Candidates: []phaseagent.TaskMemoryCandidateView{{CandidateID: "memory-1"}}}},
		Workspace:    runtime.WorkspaceBinding{Ref: "workspace-a", Revision: "workspace-r7", AllowedDirs: []string{"src"}},
		ArtifactRefs: []artifacts.ArtifactRef{"artifact-a"}, EventRefs: []string{"event-a"}, EvidenceRefs: []string{"evidence-a"},
	}
	materializer := RehydratedHostPackageMaterializer{Envelopes: rehydrationEnvelopeResolver{envelope: HostEnvelope{
		BindingRef: "B2", TaskContract: "task contract", PhaseInstruction: "continue with new input",
		Workspace: WorkspaceMount{Root: `C:\private\workspace`, AllowedDirs: []string{"src"}},
		Context:   MaterializedContext{Content: "authorized context"}, TaskMemory: plan.TaskMemory.View,
		MCPBinding: TrustedMCPBinding{Token: secret},
	}}}
	pkg, err := materializer.MaterializeRehydratedExecution(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.ValidateRehydratedExecutionPackage(plan, pkg); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(pkg)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{secret, `C:\private\workspace`, "authorization", "credential", "hidden reasoning"} {
		if strings.Contains(strings.ToLower(string(encoded)), strings.ToLower(forbidden)) {
			t.Fatalf("package leaked %q: %s", forbidden, encoded)
		}
	}
}

func readyWorkerStatus() map[string]any {
	return map[string]any{"name": "tm-invocation-a-g3-e2", "phase": "Ready", "containerState": "running", "runtimeConfig": map[string]any{"desiredGeneration": "7", "appliedGeneration": "7", "mcpServers": []map[string]any{{"name": "threadmill", "applied": true, "headerNames": []string{"X-Threadmill-Execution-Token"}}}}}
}

func containsRequest(requests []string, expected string) bool {
	for _, request := range requests {
		if request == expected {
			return true
		}
	}
	return false
}
