package agentteams

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

func TestProductionClientImplementsClientFlowWithoutMatrixParsing(t *testing.T) {
	heartbeat := time.Now().UTC().Format(time.RFC3339Nano)
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/version":
			_ = json.NewEncoder(w).Encode(map[string]string{"version": "test"})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/workers":
			_ = json.NewEncoder(w).Encode(map[string]any{"workers": []map[string]any{{
				"name": "worker-a", "phase": "Running", "runtime": "qwenpaw", "lastHeartbeat": heartbeat,
			}}})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/managers":
			_ = json.NewEncoder(w).Encode(map[string]any{"managers": []map[string]any{}})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/workers/worker-a/ensure-ready":
			_ = json.NewEncoder(w).Encode(map[string]string{"name": "worker-a", "phase": "Running"})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(controller.Close)
	controllerClient, err := NewAgentTeamsControllerClient(controller.URL, "controller-token", controller.Client())
	if err != nil {
		t.Fatal(err)
	}
	qwenState := newQwenPawContractState()
	qwenServer := httptest.NewServer(qwenState)
	t.Cleanup(qwenServer.Close)
	qwenAPI, err := NewQwenPawAPI(qwenServer.URL, qwenServer.Client())
	if err != nil {
		t.Fatal(err)
	}
	store := NewMemoryExecutionStore()
	ref := "execution://prod/1"
	record, created, err := store.Reserve(context.Background(), ref, "fp", AgentTeamsExecutionRef{InvocationID: "inv-prod", HostRef: "worker-a"})
	if err != nil || !created {
		t.Fatalf("Reserve() = created %v err %v", created, err)
	}
	slotStore := newFakeHostSlotStore(record.Execution.AgentTeamsTaskID)
	resolver := &staticMCPResolver{material: InvocationMCPMaterial{URL: "https://threadmill.example.test/mcp", BearerToken: "short-token", TokenIdentifier: "tok-1", ExpectedTools: []string{"phase.submit"}}}
	client, err := NewProductionClient(ProductionClientOptions{
		Controller:  controllerClient,
		Slots:       slotStore,
		MCPResolver: resolver,
		QwenPaw:     staticQwenPawProvider{api: qwenAPI},
		Taskflow:    recordingTaskflow{},
		Containers:  StaticContainerResolver{"worker-a": "qwenpaw-worker-a"},
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := client.PrepareHost(context.Background(), HostPreparation{
		HostRef: "worker-a", InvocationID: "inv-prod", AgentTeamsTaskID: record.Execution.AgentTeamsTaskID, Role: auth.RoleExecutor, RuntimeConfigRef: "runtime", EnvelopeRef: "envelope",
	}); err != nil {
		t.Fatalf("PrepareHost() error = %v", err)
	}
	key, err := InvocationMCPKey(record.Execution.AgentTeamsTaskID)
	if err != nil {
		t.Fatal(err)
	}
	qwenState.mu.Lock()
	installed := qwenState.clients[key]
	qwenState.mu.Unlock()
	if installed.Authorization != "Bearer short-token" {
		t.Fatalf("installed auth = %q", installed.Authorization)
	}
	hosts, err := client.ListHosts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(hosts) != 1 || hosts[0].ActiveExecutions != 1 {
		t.Fatalf("hosts after PrepareHost = %#v", hosts)
	}
	if err := client.RevokeInvocation(context.Background(), "worker-a", "inv-prod"); err != nil {
		t.Fatalf("RevokeInvocation() error = %v", err)
	}
	if len(resolver.revoked) != 1 || resolver.revoked[0] != "inv-prod" {
		t.Fatalf("revoked server-side tokens = %#v", resolver.revoked)
	}
	if raw, err := client.ReadObservations(context.Background(), "cursor"); err != nil || len(raw) != 0 {
		t.Fatalf("ReadObservations() = %#v, %v; want no Matrix-derived events", raw, err)
	}
}

func TestProductionClientPrunesOnlyStaleCanonicalInvocationMCPBeforeInstall(t *testing.T) {
	heartbeat := time.Now().UTC().Format(time.RFC3339Nano)
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/workers":
			_ = json.NewEncoder(w).Encode(map[string]any{"workers": []map[string]any{{
				"name": "worker-a", "phase": "Running", "runtime": "qwenpaw", "lastHeartbeat": heartbeat,
			}}})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/managers":
			_ = json.NewEncoder(w).Encode(map[string]any{"managers": []map[string]any{}})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/workers/worker-a/ensure-ready":
			_ = json.NewEncoder(w).Encode(map[string]string{"name": "worker-a", "phase": "Running"})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(controller.Close)
	controllerClient, err := NewAgentTeamsControllerClient(controller.URL, "controller-token", controller.Client())
	if err != nil {
		t.Fatal(err)
	}
	qwenState := newQwenPawContractState()
	const staleKey = "threadmill-0123456789abcdef01234567"
	const operatorKey = "operator-owned-client"
	qwenState.clients[staleKey] = qwenPawContractClient{Enabled: true}
	qwenState.clients[operatorKey] = qwenPawContractClient{Enabled: true}
	qwenServer := httptest.NewServer(qwenState)
	t.Cleanup(qwenServer.Close)
	qwenAPI, err := NewQwenPawAPI(qwenServer.URL, qwenServer.Client())
	if err != nil {
		t.Fatal(err)
	}
	slots := newFakeHostSlotStore("threadmill-0123456789abcdef0123456789abcdef")
	slots.staleMCPKeys = []string{staleKey, operatorKey, slots.taskID}
	client, err := NewProductionClient(ProductionClientOptions{
		Controller: controllerClient,
		Slots:      slots,
		MCPResolver: &staticMCPResolver{material: InvocationMCPMaterial{
			URL: "https://threadmill.example.test/mcp", BearerToken: "short-token", TokenIdentifier: "tok-prune", ExpectedTools: []string{"phase.submit"},
		}},
		QwenPaw:    staticQwenPawProvider{api: qwenAPI},
		Taskflow:   recordingTaskflow{},
		Containers: StaticContainerResolver{"worker-a": "qwenpaw-worker-a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.PrepareHost(context.Background(), HostPreparation{
		HostRef: "worker-a", InvocationID: "inv-prune", AgentTeamsTaskID: slots.taskID, Role: auth.RoleExecutor,
		RuntimeConfigRef: "runtime", EnvelopeRef: "envelope",
	}); err != nil {
		t.Fatalf("PrepareHost() error = %v", err)
	}

	qwenState.mu.Lock()
	defer qwenState.mu.Unlock()
	if _, ok := qwenState.clients[staleKey]; ok {
		t.Fatal("stale canonical invocation MCP was not removed")
	}
	if _, ok := qwenState.clients[operatorKey]; !ok {
		t.Fatal("operator-owned MCP was removed")
	}
	if _, ok := qwenState.clients[slots.taskID]; !ok {
		t.Fatal("current invocation MCP was not installed")
	}
	if len(qwenState.deleteCalls) != 1 || qwenState.deleteCalls[0] != staleKey {
		t.Fatalf("deleted MCP keys = %#v, want only %q", qwenState.deleteCalls, staleKey)
	}
}

func TestProductionClientRevokesServerTokenWhenSlotClaimFails(t *testing.T) {
	controller := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(controller.Close)
	controllerClient, err := NewAgentTeamsControllerClient(controller.URL, "controller-token", controller.Client())
	if err != nil {
		t.Fatal(err)
	}
	claimErr := errors.New("slot unavailable")
	slots := newFakeHostSlotStore("task-claim-failure")
	slots.claimErr = claimErr
	resolver := &staticMCPResolver{material: InvocationMCPMaterial{
		URL: "https://threadmill.example.test/mcp", BearerToken: "short-token", TokenIdentifier: "tok-claim-failure",
	}}
	client, err := NewProductionClient(ProductionClientOptions{
		Controller: controllerClient, Slots: slots, MCPResolver: resolver,
		QwenPaw: staticQwenPawProvider{}, Taskflow: recordingTaskflow{},
		Containers: StaticContainerResolver{"worker-a": "qwenpaw-worker-a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = client.PrepareHost(context.Background(), HostPreparation{
		HostRef: "worker-a", InvocationID: "inv-claim-failure", AgentTeamsTaskID: "threadmill-claim-failure", Role: auth.RoleExecutor,
		RuntimeConfigRef: "runtime", EnvelopeRef: "envelope",
	})
	if !errors.Is(err, claimErr) {
		t.Fatalf("PrepareHost() error = %v, want claim failure", err)
	}
	if slots.active || len(resolver.revoked) != 1 || resolver.revoked[0] != "inv-claim-failure" {
		t.Fatalf("claim cleanup active=%v revoked=%#v", slots.active, resolver.revoked)
	}
}

func TestProductionClientTreatsMissingQwenHostAsRevokedAfterServerTokenRevoke(t *testing.T) {
	controller := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(controller.Close)
	controllerClient, err := NewAgentTeamsControllerClient(controller.URL, "controller-token", controller.Client())
	if err != nil {
		t.Fatal(err)
	}
	slots := newFakeHostSlotStore("threadmill-missing-qwen-revoke")
	slots.active = true
	slots.claim = HostSlotClaim{
		TaskID: "threadmill-missing-qwen-revoke", HostRef: "worker-a", InvocationID: "inv-missing-qwen-revoke",
		MCPClientKey: "threadmill-missing-qwen-revoke", ClaimedAt: time.Unix(1, 0).UTC(),
	}
	resolver := &staticMCPResolver{}
	client, err := NewProductionClient(ProductionClientOptions{
		Controller: controllerClient, Slots: slots, MCPResolver: resolver,
		QwenPaw:  failingQwenPawProvider{err: kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "AgentTeams host not found", Recoverable: true}},
		Taskflow: recordingTaskflow{}, Containers: StaticContainerResolver{"worker-a": "qwenpaw-worker-a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.RevokeInvocation(context.Background(), "worker-a", "inv-missing-qwen-revoke"); err != nil {
		t.Fatalf("RevokeInvocation() after server token revoke and missing Qwen host = %v", err)
	}
	if len(resolver.revoked) != 1 || resolver.revoked[0] != "inv-missing-qwen-revoke" || !slots.revoked || !slots.active {
		t.Fatalf("cleanup state revokedTokens=%#v slotRevoked=%v active=%v, want server token revoked, slot revoked, not released", resolver.revoked, slots.revoked, slots.active)
	}
}

func TestProductionClientDoesNotSwallowServerTokenRevokeFailure(t *testing.T) {
	controller := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(controller.Close)
	controllerClient, err := NewAgentTeamsControllerClient(controller.URL, "controller-token", controller.Client())
	if err != nil {
		t.Fatal(err)
	}
	slots := newFakeHostSlotStore("threadmill-server-revoke-failure")
	slots.active = true
	slots.claim = HostSlotClaim{
		TaskID: "threadmill-server-revoke-failure", HostRef: "worker-a", InvocationID: "inv-server-revoke-failure",
		MCPClientKey: "threadmill-server-revoke-failure", ClaimedAt: time.Unix(1, 0).UTC(),
	}
	revokeErr := errors.New("server token store unavailable")
	client, err := NewProductionClient(ProductionClientOptions{
		Controller: controllerClient, Slots: slots, MCPResolver: &staticMCPResolver{revokeErr: revokeErr},
		QwenPaw:  failingQwenPawProvider{err: kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "AgentTeams host not found", Recoverable: true}},
		Taskflow: recordingTaskflow{}, Containers: StaticContainerResolver{"worker-a": "qwenpaw-worker-a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.RevokeInvocation(context.Background(), "worker-a", "inv-server-revoke-failure"); !errors.Is(err, revokeErr) {
		t.Fatalf("RevokeInvocation() error = %v, want server token revoke failure", err)
	}
	if slots.revoked {
		t.Fatal("slot was marked revoked despite failed logical token revoke")
	}
}

func TestDockerQwenPawProviderUsesHostManagementPort(t *testing.T) {
	provider := DockerQwenPawProvider{
		Containers: StaticContainerResolver{
			"default":  "agentteams-manager",
			"worker-a": "agentteams-worker-a",
		},
	}
	manager, err := provider.ForHost(context.Background(), "default")
	if err != nil {
		t.Fatal(err)
	}
	if manager.baseURL != "http://127.0.0.1:18799" {
		t.Fatalf("manager base URL = %q, want manager management port", manager.baseURL)
	}
	worker, err := provider.ForHost(context.Background(), "worker-a")
	if err != nil {
		t.Fatal(err)
	}
	if worker.baseURL != "http://127.0.0.1:8088" {
		t.Fatalf("worker base URL = %q, want default worker management port", worker.baseURL)
	}
}

func TestProductionClientProjectsDedicatedManagerWorker(t *testing.T) {
	var dispatcherReadyCalls atomic.Int32
	var managerReadyCalls atomic.Int32
	heartbeat := time.Now().UTC().Format(time.RFC3339Nano)
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/workers":
			_ = json.NewEncoder(w).Encode(map[string]any{"workers": []map[string]any{
				{"name": "threadmill-manager", "phase": "Running", "runtime": "qwenpaw", "skills": []string{"teamharness"}, "lastHeartbeat": heartbeat},
				{"name": "threadmill-context", "phase": "Running", "runtime": "qwenpaw", "skills": []string{"teamharness"}, "lastHeartbeat": heartbeat},
				{"name": "threadmill-dispatcher", "phase": "Running", "runtime": "qwenpaw", "skills": []string{"teamharness"}, "lastHeartbeat": heartbeat},
			}})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/managers":
			_ = json.NewEncoder(w).Encode(map[string]any{"managers": []map[string]any{{
				"name": "default", "phase": "Running", "runtime": "copaw",
			}}})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/workers/threadmill-dispatcher/ensure-ready":
			dispatcherReadyCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]string{"name": "threadmill-dispatcher", "phase": "Running"})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/workers/threadmill-manager/ensure-ready":
			managerReadyCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]string{"name": "threadmill-manager", "phase": "Running"})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(controller.Close)
	controllerClient, err := NewAgentTeamsControllerClient(controller.URL, "controller-token", controller.Client())
	if err != nil {
		t.Fatal(err)
	}
	qwenState := newQwenPawContractState()
	qwenServer := httptest.NewServer(qwenState)
	t.Cleanup(qwenServer.Close)
	qwenAPI, err := NewQwenPawAPI(qwenServer.URL, qwenServer.Client())
	if err != nil {
		t.Fatal(err)
	}
	taskflow := &capturingTaskflow{}
	slots := newFakeHostSlotStore("task-manager-alias")
	client, err := NewProductionClient(ProductionClientOptions{
		Controller: controllerClient, Slots: slots,
		MCPResolver: &staticMCPResolver{}, QwenPaw: staticQwenPawProvider{api: qwenAPI}, Taskflow: taskflow,
		Containers: StaticContainerResolver{
			"default":               "agentteams-worker-threadmill-manager",
			"context":               "agentteams-worker-threadmill-context",
			"threadmill-dispatcher": "agentteams-worker-threadmill-dispatcher",
		},
		ManagerWorkers:  map[string]string{"default": "threadmill-manager", "context": "threadmill-context"},
		TaskflowHostRef: "threadmill-dispatcher",
	})
	if err != nil {
		t.Fatal(err)
	}
	hosts, err := client.ListHosts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(hosts) != 2 {
		t.Fatalf("projected manager hosts = %#v", hosts)
	}
	projected := map[string]HostStatus{hosts[0].Ref: hosts[0], hosts[1].Ref: hosts[1]}
	if projected["default"].Kind != HostManager || !containsAll(projected["default"].Capabilities, []string{"teamharness", "manager", CapabilityTaskManager}) || containsAll(projected["default"].Capabilities, []string{CapabilityContextAgent}) {
		t.Fatalf("projected task manager host = %#v", projected["default"])
	}
	if projected["context"].Kind != HostManager || !containsAll(projected["context"].Capabilities, []string{"teamharness", "manager", CapabilityContextAgent}) || containsAll(projected["context"].Capabilities, []string{CapabilityTaskManager}) {
		t.Fatalf("projected context host = %#v", projected["context"])
	}
	task, err := client.DelegateTask(context.Background(), DelegateTaskRequest{
		ProjectID: "project-a", TaskID: "task-manager-alias", HostRef: "default", RoomID: "!room:example.test", Spec: "bounded manager work",
	})
	if err != nil {
		t.Fatal(err)
	}
	if taskflow.container != "agentteams-worker-threadmill-dispatcher" || taskflow.call.AssignedTo != "threadmill-manager" || task.HostRef != "default" {
		t.Fatalf("taskflow projection container=%q call=%#v task=%#v", taskflow.container, taskflow.call, task)
	}
	if got := dispatcherReadyCalls.Load(); got != 1 {
		t.Fatalf("dispatcher ensure-ready calls = %d, want 1", got)
	}
	if !strings.HasPrefix(taskflow.call.Spec, "Read the complete authoritative task specification before acting: shared/tasks/task-manager-alias/spec.md") ||
		!strings.Contains(taskflow.call.Spec, "Immediately call TeamHarness taskflow ack_task for task task-manager-alias") ||
		!strings.Contains(taskflow.call.Spec, "Runtime owns TeamHarness SUCCESS finalization with an empty deliverables list") ||
		!strings.HasSuffix(taskflow.call.Spec, "bounded manager work") {
		t.Fatalf("taskflow specification = %q", taskflow.call.Spec)
	}
	if err := slots.Claim(context.Background(), "default", "inv-manager-alias", "client", []byte("hash"), "token"); err != nil {
		t.Fatal(err)
	}
	if err := client.CompleteTask(context.Background(), "task-manager-alias", "Threadmill accepted the decision"); err != nil {
		t.Fatal(err)
	}
	if taskflow.container != "agentteams-worker-threadmill-manager" || taskflow.call.Action != "submit_task" || taskflow.call.Role != "worker" || taskflow.call.Status != "SUCCESS" || len(taskflow.call.Deliverables) != 0 {
		t.Fatalf("CompleteTask projection container=%q call=%#v, want assigned worker SUCCESS with empty deliverables", taskflow.container, taskflow.call)
	}
	if got := managerReadyCalls.Load(); got != 1 {
		t.Fatalf("manager ensure-ready calls = %d, want 1", got)
	}
	if _, err := client.CheckTask(context.Background(), "task-manager-alias"); err != nil {
		t.Fatal(err)
	}
	if taskflow.container != "agentteams-worker-threadmill-dispatcher" || taskflow.call.Action != "check_task" {
		t.Fatalf("CheckTask projection container=%q call=%#v, want dedicated taskflow worker", taskflow.container, taskflow.call)
	}
	if got := dispatcherReadyCalls.Load(); got != 2 {
		t.Fatalf("dispatcher ensure-ready calls after CheckTask = %d, want 2", got)
	}
	if err := client.CancelTask(context.Background(), "task-manager-alias", "test cleanup"); err != nil {
		t.Fatal(err)
	}
	if taskflow.container != "agentteams-worker-threadmill-manager" || taskflow.call.Action != "cancel_task" {
		t.Fatalf("CancelTask projection container=%q call=%#v, want assigned manager worker", taskflow.container, taskflow.call)
	}
	previousCalls := taskflow.calls
	slots.claim.ReleasedAt = time.Now().UTC()
	check, err := client.CheckTask(context.Background(), "task-manager-alias")
	if err != nil || check.Task.Status != "released" {
		t.Fatalf("CheckTask after host release check=%#v error=%v, want released terminal snapshot", check, err)
	}
	if err := client.CancelTask(context.Background(), "task-manager-alias", "replayed cleanup"); err != nil {
		t.Fatalf("CancelTask after host release: %v", err)
	}
	if taskflow.calls != previousCalls {
		t.Fatalf("released host lifecycle called TeamHarness %d additional times", taskflow.calls-previousCalls)
	}
}

func TestProductionClientProjectsStoppedDedicatedManagerAsWakeable(t *testing.T) {
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/workers":
			_ = json.NewEncoder(w).Encode(map[string]any{"workers": []map[string]any{{
				"name": "threadmill-manager", "phase": "Stopped", "state": "Stopped", "runtime": "qwenpaw",
			}}})
		case "/api/v1/managers":
			_ = json.NewEncoder(w).Encode(map[string]any{"managers": []map[string]any{}})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(controller.Close)
	controllerClient, err := NewAgentTeamsControllerClient(controller.URL, "controller-token", controller.Client())
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewProductionClient(ProductionClientOptions{
		Controller: controllerClient, Slots: newFakeHostSlotStore("task-wakeable-manager"),
		MCPResolver: &staticMCPResolver{}, QwenPaw: staticQwenPawProvider{}, Taskflow: recordingTaskflow{},
		Containers:     StaticContainerResolver{"default": "agentteams-worker-threadmill-manager"},
		ManagerWorkers: map[string]string{"default": "threadmill-manager"},
	})
	if err != nil {
		t.Fatal(err)
	}
	hosts, err := client.ListHosts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(hosts) != 1 || hosts[0].Ref != "default" || hosts[0].Kind != HostManager || hosts[0].Phase != "Sleeping" {
		t.Fatalf("projected stopped manager = %#v, want wakeable logical manager", hosts)
	}
}

func TestProductionClientClearsLogicalManagerFenceFromProviderWorkerObservation(t *testing.T) {
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/workers":
			_ = json.NewEncoder(w).Encode(map[string]any{"workers": []map[string]any{{
				"name": "threadmill-context", "phase": "Running", "runtime": "qwenpaw",
			}}})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/managers":
			_ = json.NewEncoder(w).Encode(map[string]any{"managers": []map[string]any{}})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(controller.Close)
	controllerClient, err := NewAgentTeamsControllerClient(controller.URL, "controller-token", controller.Client())
	if err != nil {
		t.Fatal(err)
	}
	slots := newFakeHostSlotStore("context-task")
	client, err := NewProductionClient(ProductionClientOptions{
		Controller: controllerClient, Slots: slots, MCPResolver: &staticMCPResolver{},
		QwenPaw: staticQwenPawProvider{}, Taskflow: recordingTaskflow{},
		Containers: StaticContainerResolver{
			"context":               "agentteams-worker-threadmill-context",
			"threadmill-dispatcher": "agentteams-worker-threadmill-dispatcher",
		},
		ManagerWorkers: map[string]string{"context": "threadmill-context"}, TaskflowHostRef: "threadmill-dispatcher",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.clearReusableFenceForClaim(context.Background(), "context"); err != nil {
		t.Fatal(err)
	}
	if len(slots.clearedHosts) != 1 || slots.clearedHosts[0] != "context" {
		t.Fatalf("cleared logical hosts = %#v, want context", slots.clearedHosts)
	}
}

func TestProductionClientWakesWorkerBeforeQwenPawPreparation(t *testing.T) {
	var ready atomic.Bool
	heartbeat := time.Now().UTC().Format(time.RFC3339Nano)
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/workers":
			phase := "Starting"
			lastHeartbeat := ""
			if ready.Load() {
				phase = "Running"
				lastHeartbeat = heartbeat
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"workers": []map[string]any{{
				"name": "worker-a", "phase": phase, "runtime": "qwenpaw", "lastHeartbeat": lastHeartbeat,
			}}})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/managers":
			_ = json.NewEncoder(w).Encode(map[string]any{"managers": []map[string]any{}})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/workers/worker-a/ensure-ready":
			ready.Store(true)
			_ = json.NewEncoder(w).Encode(map[string]string{"name": "worker-a", "phase": "Running"})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(controller.Close)
	controllerClient, err := NewAgentTeamsControllerClient(controller.URL, "controller-token", controller.Client())
	if err != nil {
		t.Fatal(err)
	}
	qwenState := newQwenPawContractState()
	var postEnsureVersionFailures int32
	qwenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !ready.Load() {
			http.Error(w, "worker is not ready", http.StatusServiceUnavailable)
			return
		}
		if r.URL.Path == "/api/version" && atomic.AddInt32(&postEnsureVersionFailures, 1) <= 2 {
			http.Error(w, "worker api is still starting", http.StatusServiceUnavailable)
			return
		}
		qwenState.ServeHTTP(w, r)
	}))
	t.Cleanup(qwenServer.Close)
	qwenAPI, err := NewQwenPawAPI(qwenServer.URL, qwenServer.Client())
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewProductionClient(ProductionClientOptions{
		Controller:  controllerClient,
		Slots:       newFakeHostSlotStore("threadmill-wake-before-qwenpaw"),
		MCPResolver: &staticMCPResolver{material: InvocationMCPMaterial{URL: "https://threadmill.example.test/mcp", BearerToken: "short-token", TokenIdentifier: "tok-wake", ExpectedTools: []string{"phase.submit"}}},
		QwenPaw:     staticQwenPawProvider{api: qwenAPI},
		Taskflow:    recordingTaskflow{},
		Containers:  StaticContainerResolver{"worker-a": "qwenpaw-worker-a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.PrepareHost(context.Background(), HostPreparation{
		HostRef: "worker-a", InvocationID: "inv-wake-before-qwenpaw", AgentTeamsTaskID: "threadmill-wake-before-qwenpaw", Role: auth.RoleExecutor,
		RuntimeConfigRef: "runtime", EnvelopeRef: "envelope",
	}); err != nil {
		t.Fatalf("PrepareHost() error = %v", err)
	}
	if !ready.Load() {
		t.Fatal("worker was not ensured ready before QwenPaw preparation")
	}
	if atomic.LoadInt32(&postEnsureVersionFailures) < 3 {
		t.Fatal("PrepareHost did not retry QwenPaw readiness after controller ensure-ready")
	}
}

func TestProductionClientAcceptsFreshHeartbeatForAlreadyRunningWorker(t *testing.T) {
	heartbeat := time.Now().UTC().Add(-30 * time.Second).Format(time.RFC3339Nano)
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/workers":
			_ = json.NewEncoder(w).Encode(map[string]any{"workers": []map[string]any{{
				"name": "worker-a", "phase": "Running", "runtime": "qwenpaw", "lastHeartbeat": heartbeat,
			}}})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/managers":
			_ = json.NewEncoder(w).Encode(map[string]any{"managers": []map[string]any{}})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/workers/worker-a/ensure-ready":
			_ = json.NewEncoder(w).Encode(map[string]string{"name": "worker-a", "phase": "Running"})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(controller.Close)
	controllerClient, err := NewAgentTeamsControllerClient(controller.URL, "controller-token", controller.Client())
	if err != nil {
		t.Fatal(err)
	}
	qwenState := newQwenPawContractState()
	qwenServer := httptest.NewServer(qwenState)
	t.Cleanup(qwenServer.Close)
	qwenAPI, err := NewQwenPawAPI(qwenServer.URL, qwenServer.Client())
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewProductionClient(ProductionClientOptions{
		Controller:  controllerClient,
		Slots:       newFakeHostSlotStore("threadmill-running-fresh-heartbeat"),
		MCPResolver: &staticMCPResolver{material: InvocationMCPMaterial{URL: "https://threadmill.example.test/mcp", BearerToken: "short-token", TokenIdentifier: "tok-running", ExpectedTools: []string{"phase.submit"}}},
		QwenPaw:     staticQwenPawProvider{api: qwenAPI},
		Taskflow:    recordingTaskflow{},
		Containers:  StaticContainerResolver{"worker-a": "qwenpaw-worker-a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.PrepareHost(ctx, HostPreparation{
		HostRef: "worker-a", InvocationID: "inv-running-fresh-heartbeat", AgentTeamsTaskID: "threadmill-running-fresh-heartbeat", Role: auth.RoleExecutor,
		RuntimeConfigRef: "runtime", EnvelopeRef: "envelope",
	}); err != nil {
		t.Fatalf("PrepareHost() error = %v", err)
	}
}

func TestProductionClientFailsClosedUntilWorkerHeartbeatObserved(t *testing.T) {
	var installCalls atomic.Int32
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/workers":
			_ = json.NewEncoder(w).Encode(map[string]any{"workers": []map[string]any{{
				"name": "worker-a", "phase": "Running", "runtime": "qwenpaw",
			}}})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/managers":
			_ = json.NewEncoder(w).Encode(map[string]any{"managers": []map[string]any{}})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/workers/worker-a/ensure-ready":
			_ = json.NewEncoder(w).Encode(map[string]string{"name": "worker-a", "phase": "Running"})
		case r.Method == http.MethodPut && r.URL.Path == "/api/v1/workers/worker-a":
			_, _ = w.Write([]byte(`{}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(controller.Close)
	controllerClient, err := NewAgentTeamsControllerClient(controller.URL, "controller-token", controller.Client())
	if err != nil {
		t.Fatal(err)
	}
	qwenState := newQwenPawContractState()
	qwenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/api/mcp" {
			installCalls.Add(1)
		}
		qwenState.ServeHTTP(w, r)
	}))
	t.Cleanup(qwenServer.Close)
	qwenAPI, err := NewQwenPawAPI(qwenServer.URL, qwenServer.Client())
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewProductionClient(ProductionClientOptions{
		Controller:  controllerClient,
		Slots:       newFakeHostSlotStore("threadmill-no-heartbeat"),
		MCPResolver: &staticMCPResolver{material: InvocationMCPMaterial{URL: "https://threadmill.example.test/mcp", BearerToken: "short-token", TokenIdentifier: "tok-no-heartbeat", ExpectedTools: []string{"phase.submit"}}},
		QwenPaw:     staticQwenPawProvider{api: qwenAPI},
		Taskflow:    recordingTaskflow{},
		Containers:  StaticContainerResolver{"worker-a": "qwenpaw-worker-a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err = client.PrepareHost(ctx, HostPreparation{
		HostRef: "worker-a", InvocationID: "inv-no-heartbeat", AgentTeamsTaskID: "threadmill-no-heartbeat", Role: auth.RoleExecutor,
		RuntimeConfigRef: "runtime", EnvelopeRef: "envelope",
	})
	if !kernel.IsCode(err, kernel.CodeExecutorUnavailable) {
		t.Fatalf("PrepareHost() error = %v, want executor_unavailable", err)
	}
	if got := installCalls.Load(); got != 0 {
		t.Fatalf("invocation MCP install calls = %d, want 0 before worker heartbeat", got)
	}
}

func TestProductionClientRejectsHeartbeatPredatingReadinessRequest(t *testing.T) {
	oldHeartbeat := time.Now().UTC().Add(-30 * time.Second).Format(time.RFC3339Nano)
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/workers":
			_ = json.NewEncoder(w).Encode(map[string]any{"workers": []map[string]any{{
				"name": "worker-a", "phase": "Running", "runtime": "qwenpaw", "lastHeartbeat": oldHeartbeat,
			}}})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/managers":
			_ = json.NewEncoder(w).Encode(map[string]any{"managers": []map[string]any{}})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(controller.Close)
	controllerClient, err := NewAgentTeamsControllerClient(controller.URL, "controller-token", controller.Client())
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewProductionClient(ProductionClientOptions{
		Controller:  controllerClient,
		Slots:       newFakeHostSlotStore("threadmill-heartbeat-predates-ready"),
		MCPResolver: &staticMCPResolver{},
		QwenPaw:     staticQwenPawProvider{},
		Taskflow:    recordingTaskflow{},
		Containers:  StaticContainerResolver{"worker-a": "qwenpaw-worker-a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = client.providerWorkerObservedReady(context.Background(), "worker-a", time.Now().UTC(), time.Time{})
	if !kernel.IsCode(err, kernel.CodeExecutorUnavailable) {
		t.Fatalf("providerWorkerObservedReady() error = %v, want executor_unavailable", err)
	}
}

func TestProductionClientRejectsUnchangedHeartbeatAfterReadinessRequest(t *testing.T) {
	heartbeat := time.Now().UTC().Truncate(time.Second)
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/workers":
			_ = json.NewEncoder(w).Encode(map[string]any{"workers": []map[string]any{{
				"name": "worker-a", "phase": "Running", "runtime": "qwenpaw", "lastHeartbeat": heartbeat.Format(time.RFC3339),
			}}})
		case "/api/v1/managers":
			_ = json.NewEncoder(w).Encode(map[string]any{"managers": []map[string]any{}})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(controller.Close)
	controllerClient, err := NewAgentTeamsControllerClient(controller.URL, "controller-token", controller.Client())
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewProductionClient(ProductionClientOptions{
		Controller: controllerClient, Slots: newFakeHostSlotStore("threadmill-unchanged-heartbeat"), MCPResolver: &staticMCPResolver{},
		QwenPaw: staticQwenPawProvider{}, Taskflow: recordingTaskflow{}, Containers: StaticContainerResolver{"worker-a": "qwenpaw-worker-a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = client.providerWorkerObservedReady(context.Background(), "worker-a", heartbeat, heartbeat)
	if !kernel.IsCode(err, kernel.CodeExecutorUnavailable) {
		t.Fatalf("providerWorkerObservedReady() error = %v, want executor_unavailable", err)
	}
}

func TestProductionClientReleaseHostSlotDoesNotSleepReusablePhysicalWorker(t *testing.T) {
	var sleepCalls int32
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/workers/worker-a/sleep":
			atomic.AddInt32(&sleepCalls, 1)
			_, _ = w.Write([]byte(`{}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(controller.Close)
	controllerClient, err := NewAgentTeamsControllerClient(controller.URL, "controller-token", controller.Client())
	if err != nil {
		t.Fatal(err)
	}
	slots := newFakeHostSlotStore("threadmill-release-sleep")
	slots.active = true
	slots.claim = HostSlotClaim{TaskID: "threadmill-release-sleep", HostRef: "worker-a", InvocationID: "inv-release-sleep"}
	client, err := NewProductionClient(ProductionClientOptions{
		Controller:  controllerClient,
		Slots:       slots,
		MCPResolver: &staticMCPResolver{},
		QwenPaw:     staticQwenPawProvider{},
		Taskflow:    recordingTaskflow{},
		Containers:  StaticContainerResolver{"worker-a": "qwenpaw-worker-a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.ReleaseHostSlot(context.Background(), "threadmill-release-sleep", "worker-a"); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&sleepCalls); got != 0 {
		t.Fatalf("sleep calls = %d, want 0", got)
	}
}

func TestProductionClientReleaseHostSlotDoesNotFailAfterDurableReleaseWhenSleepFails(t *testing.T) {
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/api/v1/workers/worker-a/sleep" {
			http.Error(w, "controller restarting", http.StatusServiceUnavailable)
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(controller.Close)
	controllerClient, err := NewAgentTeamsControllerClient(controller.URL, "controller-token", controller.Client())
	if err != nil {
		t.Fatal(err)
	}
	slots := newFakeHostSlotStore("threadmill-release-sleep-failure")
	slots.active = true
	slots.claim = HostSlotClaim{TaskID: "threadmill-release-sleep-failure", HostRef: "worker-a", InvocationID: "inv-release-sleep-failure"}
	client, err := NewProductionClient(ProductionClientOptions{
		Controller: controllerClient, Slots: slots, MCPResolver: &staticMCPResolver{},
		QwenPaw: staticQwenPawProvider{}, Taskflow: recordingTaskflow{},
		Containers: StaticContainerResolver{"worker-a": "qwenpaw-worker-a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.ReleaseHostSlot(context.Background(), slots.taskID, "worker-a"); err != nil {
		t.Fatalf("ReleaseHostSlot() after durable release = %v, want nil", err)
	}
	if slots.active {
		t.Fatal("durable host slot remains active after sleep failure")
	}
}

func TestProductionClientClearsReusableFenceBeforeWakingSleepingWorker(t *testing.T) {
	controller := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(controller.Close)
	controllerClient, err := NewAgentTeamsControllerClient(controller.URL, "controller-token", controller.Client())
	if err != nil {
		t.Fatal(err)
	}
	slots := newFakeHostSlotStore("threadmill-clear-sleeping-fence")
	client, err := NewProductionClient(ProductionClientOptions{
		Controller: controllerClient, Slots: slots, MCPResolver: &staticMCPResolver{},
		QwenPaw: staticQwenPawProvider{}, Taskflow: recordingTaskflow{},
		Containers: StaticContainerResolver{"worker-a": "qwenpaw-worker-a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.clearReusableFenceForClaim(context.Background(), "worker-a"); err != nil {
		t.Fatal(err)
	}
	if len(slots.clearedHosts) != 1 || slots.clearedHosts[0] != "worker-a" {
		t.Fatalf("cleared hosts = %#v, want worker-a", slots.clearedHosts)
	}
}

func TestProductionClientCheckTaskWakesSleepingClaimedCarrier(t *testing.T) {
	var ensureCalls int32
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/workers":
			_ = json.NewEncoder(w).Encode(map[string]any{"workers": []map[string]any{{"name": "worker-a", "phase": "Sleeping", "runtime": "qwenpaw"}}})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/managers":
			_ = json.NewEncoder(w).Encode(map[string]any{"managers": []map[string]any{}})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/workers/worker-a/ensure-ready":
			atomic.AddInt32(&ensureCalls, 1)
			_ = json.NewEncoder(w).Encode(map[string]string{"name": "worker-a", "phase": "Running"})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(controller.Close)
	controllerClient, err := NewAgentTeamsControllerClient(controller.URL, "controller-token", controller.Client())
	if err != nil {
		t.Fatal(err)
	}
	slots := newFakeHostSlotStore("threadmill-0123456789abcdef01234567")
	slots.active = true
	slots.claim = HostSlotClaim{TaskID: slots.taskID, HostRef: "worker-a", InvocationID: "inv-sleeping", ClaimedAt: time.Unix(1, 0).UTC()}
	taskflow := &capturingTaskflow{}
	resolver := &staticMCPResolver{}
	client, err := NewProductionClient(ProductionClientOptions{
		Controller: controllerClient, Slots: slots, MCPResolver: resolver,
		QwenPaw: staticQwenPawProvider{}, Taskflow: taskflow,
		Containers: StaticContainerResolver{"worker-a": "qwenpaw-worker-a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.CheckTask(context.Background(), slots.taskID); err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&ensureCalls) != 1 || taskflow.calls != 1 || taskflow.call.Action != "check_task" {
		t.Fatalf("ensure=%d taskflow=%d/%s, want 1/1/check_task", ensureCalls, taskflow.calls, taskflow.call.Action)
	}
	if len(resolver.revoked) != 0 {
		t.Fatalf("CheckTask reactivated or revoked invocation MCP: %#v", resolver.revoked)
	}
}

func TestProductionClientHostActivityRefreshesRunningClaimedCarrier(t *testing.T) {
	var ensureCalls int32
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/workers":
			_ = json.NewEncoder(w).Encode(map[string]any{"workers": []map[string]any{{"name": "worker-a", "phase": "Running", "runtime": "qwenpaw"}}})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/managers":
			_ = json.NewEncoder(w).Encode(map[string]any{"managers": []map[string]any{}})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/workers/worker-a/ensure-ready":
			atomic.AddInt32(&ensureCalls, 1)
			_ = json.NewEncoder(w).Encode(map[string]string{"name": "worker-a", "phase": "Running"})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(controller.Close)
	controllerClient, err := NewAgentTeamsControllerClient(controller.URL, "controller-token", controller.Client())
	if err != nil {
		t.Fatal(err)
	}
	qwenState := newQwenPawContractState()
	qwenServer := httptest.NewServer(qwenState)
	t.Cleanup(qwenServer.Close)
	qwenAPI, err := NewQwenPawAPI(qwenServer.URL, qwenServer.Client())
	if err != nil {
		t.Fatal(err)
	}
	slots := newFakeHostSlotStore("threadmill-running-heartbeat")
	slots.active = true
	slots.claim = HostSlotClaim{TaskID: slots.taskID, HostRef: "worker-a", InvocationID: "inv-running", ClaimedAt: time.Unix(1, 0).UTC()}
	client, err := NewProductionClient(ProductionClientOptions{
		Controller: controllerClient, Slots: slots, MCPResolver: &staticMCPResolver{},
		QwenPaw: staticQwenPawProvider{api: qwenAPI}, Taskflow: recordingTaskflow{},
		Containers: StaticContainerResolver{"worker-a": "qwenpaw-worker-a"},
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := client.HostActivity(context.Background(), "worker-a"); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&ensureCalls); got != 1 {
		t.Fatalf("ensure-ready calls = %d, want 1 for a running carrier with an active slot", got)
	}
}

func TestProductionClientCheckTaskTreatsDurablyReleasedSlotAsTerminal(t *testing.T) {
	controller := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(controller.Close)
	controllerClient, err := NewAgentTeamsControllerClient(controller.URL, "controller-token", controller.Client())
	if err != nil {
		t.Fatal(err)
	}
	slots := newFakeHostSlotStore("threadmill-released-task")
	slots.claim = HostSlotClaim{
		TaskID: "threadmill-released-task", HostRef: "worker-a", InvocationID: "inv-released-task",
		ClaimedAt: time.Unix(1, 0).UTC(), ReleasedAt: time.Unix(2, 0).UTC(), RevokedAt: time.Unix(2, 0).UTC(),
	}
	client, err := NewProductionClient(ProductionClientOptions{
		Controller: controllerClient, Slots: slots, MCPResolver: &staticMCPResolver{},
		QwenPaw: staticQwenPawProvider{}, Taskflow: recordingTaskflow{},
		Containers: StaticContainerResolver{"worker-a": "qwenpaw-worker-a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	check, err := client.CheckTask(context.Background(), slots.taskID)
	if err != nil {
		t.Fatal(err)
	}
	if check.Task.Status != "released" || !isTerminalTaskStatus(check.Task.Status) {
		t.Fatalf("released task check = %#v, want terminal released", check)
	}
}

func TestProductionClientCheckTaskUsesDedicatedTaskflowCarrier(t *testing.T) {
	var dispatcherEnsureCalls int32
	var phaseEnsureCalls int32
	heartbeat := time.Now().UTC().Format(time.RFC3339Nano)
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/workers":
			_ = json.NewEncoder(w).Encode(map[string]any{"workers": []map[string]any{
				{"name": "phase-a", "phase": "Sleeping", "runtime": "qwenpaw", "skills": []string{"teamharness"}, "lastHeartbeat": heartbeat},
				{"name": "threadmill-dispatcher", "phase": "Running", "runtime": "qwenpaw", "skills": []string{"teamharness"}, "lastHeartbeat": heartbeat},
			}})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/managers":
			_ = json.NewEncoder(w).Encode(map[string]any{"managers": []map[string]any{}})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/workers/threadmill-dispatcher/ensure-ready":
			atomic.AddInt32(&dispatcherEnsureCalls, 1)
			_ = json.NewEncoder(w).Encode(map[string]string{"name": "threadmill-dispatcher", "phase": "Running"})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/workers/phase-a/ensure-ready":
			atomic.AddInt32(&phaseEnsureCalls, 1)
			http.Error(w, "phase worker must not be used for taskflow checks", http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(controller.Close)
	controllerClient, err := NewAgentTeamsControllerClient(controller.URL, "controller-token", controller.Client())
	if err != nil {
		t.Fatal(err)
	}
	qwenState := newQwenPawContractState()
	qwenServer := httptest.NewServer(qwenState)
	t.Cleanup(qwenServer.Close)
	qwenAPI, err := NewQwenPawAPI(qwenServer.URL, qwenServer.Client())
	if err != nil {
		t.Fatal(err)
	}
	slots := newFakeHostSlotStore("threadmill-phase-a-check")
	slots.active = true
	slots.claim = HostSlotClaim{TaskID: slots.taskID, HostRef: "phase-a", InvocationID: "inv-phase-a", ClaimedAt: time.Unix(1, 0).UTC()}
	taskflow := &capturingTaskflow{}
	client, err := NewProductionClient(ProductionClientOptions{
		Controller: controllerClient, Slots: slots,
		MCPResolver: &staticMCPResolver{}, QwenPaw: staticQwenPawProvider{api: qwenAPI}, Taskflow: taskflow,
		Containers: StaticContainerResolver{
			"phase-a":               "agentteams-worker-phase-a",
			"threadmill-dispatcher": "agentteams-worker-threadmill-dispatcher",
		},
		TaskflowHostRef: "threadmill-dispatcher",
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := client.CheckTask(context.Background(), slots.taskID); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&dispatcherEnsureCalls); got != 1 {
		t.Fatalf("dispatcher ensure-ready calls = %d, want 1", got)
	}
	if got := atomic.LoadInt32(&phaseEnsureCalls); got != 0 {
		t.Fatalf("phase-a ensure-ready calls = %d, want 0", got)
	}
	if taskflow.calls != 1 || taskflow.call.Action != "check_task" || taskflow.container != "agentteams-worker-threadmill-dispatcher" {
		t.Fatalf("taskflow calls=%d container=%q call=%#v, want check_task on dispatcher", taskflow.calls, taskflow.container, taskflow.call)
	}
	for _, container := range taskflow.containers {
		if container == "agentteams-worker-phase-a" {
			t.Fatalf("check_task touched claimed phase worker containers=%#v", taskflow.containers)
		}
	}
}

func TestProductionClientReleaseHostSlotDoesNotSleepLogicalManagerAlias(t *testing.T) {
	var sleepCalls int32
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/sleep") {
			atomic.AddInt32(&sleepCalls, 1)
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(controller.Close)
	controllerClient, err := NewAgentTeamsControllerClient(controller.URL, "controller-token", controller.Client())
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewProductionClient(ProductionClientOptions{
		Controller:  controllerClient,
		Slots:       newFakeHostSlotStore("threadmill-release-manager"),
		MCPResolver: &staticMCPResolver{},
		QwenPaw:     staticQwenPawProvider{},
		Taskflow:    recordingTaskflow{},
		Containers:  StaticContainerResolver{"default": "agentteams-worker-threadmill-manager"},
		ManagerWorkers: map[string]string{
			"default": "threadmill-manager",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.ReleaseHostSlot(context.Background(), "threadmill-release-manager", "default"); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&sleepCalls); got != 0 {
		t.Fatalf("sleep calls = %d, want 0", got)
	}
}

func TestProductionClientRevokesServerTokenWhenResolvedMCPMaterialIsInvalid(t *testing.T) {
	controller := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(controller.Close)
	controllerClient, err := NewAgentTeamsControllerClient(controller.URL, "controller-token", controller.Client())
	if err != nil {
		t.Fatal(err)
	}
	resolver := &staticMCPResolver{material: InvocationMCPMaterial{
		URL: "https://user:pass@threadmill.example.test/mcp", BearerToken: "short-token", TokenIdentifier: "tok-invalid",
	}}
	client, err := NewProductionClient(ProductionClientOptions{
		Controller: controllerClient, Slots: newFakeHostSlotStore("task-invalid"), MCPResolver: resolver,
		QwenPaw: staticQwenPawProvider{}, Taskflow: recordingTaskflow{},
		Containers: StaticContainerResolver{"worker-a": "qwenpaw-worker-a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = client.PrepareHost(context.Background(), HostPreparation{
		HostRef: "worker-a", InvocationID: "inv-invalid", AgentTeamsTaskID: "threadmill-invalid", Role: auth.RoleExecutor,
		RuntimeConfigRef: "runtime", EnvelopeRef: "envelope",
	})
	if !kernel.IsCode(err, kernel.CodeInvalidRequest) {
		t.Fatalf("PrepareHost() error = %v, want invalid_argument", err)
	}
	if len(resolver.revoked) != 1 || resolver.revoked[0] != "inv-invalid" {
		t.Fatalf("validation cleanup revoked=%#v, want inv-invalid", resolver.revoked)
	}
}

func TestProductionClientForceStopRevokesAllServerTokensBeforeFencing(t *testing.T) {
	heartbeat := time.Now().UTC().Format(time.RFC3339Nano)
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/workers":
			_ = json.NewEncoder(w).Encode(map[string]any{"workers": []map[string]any{{
				"name": "worker-a", "phase": "Running", "runtime": "qwenpaw", "lastHeartbeat": heartbeat,
			}}})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/managers":
			_ = json.NewEncoder(w).Encode(map[string]any{"managers": []map[string]any{}})
		case r.Method == http.MethodPut && r.URL.Path == "/api/v1/workers/worker-a":
			_, _ = w.Write([]byte(`{}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(controller.Close)
	controllerClient, err := NewAgentTeamsControllerClient(controller.URL, "controller-token", controller.Client())
	if err != nil {
		t.Fatal(err)
	}
	slots := newFakeHostSlotStore("task-force-stop")
	slots.active = true
	slots.claim = HostSlotClaim{TaskID: "task-force-stop", HostRef: "worker-a", InvocationID: "inv-force-stop"}
	resolver := &staticMCPResolver{}
	client, err := NewProductionClient(ProductionClientOptions{
		Controller: controllerClient, Slots: slots, MCPResolver: resolver,
		QwenPaw: staticQwenPawProvider{}, Taskflow: recordingTaskflow{},
		Containers: StaticContainerResolver{"worker-a": "qwenpaw-worker-a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.ForceStopHost(context.Background(), "worker-a"); err != nil {
		t.Fatal(err)
	}
	if len(resolver.revoked) != 1 || resolver.revoked[0] != "inv-force-stop" || !slots.revoked {
		t.Fatalf("force-stop cleanup revoked=%#v fenced=%v", resolver.revoked, slots.revoked)
	}
}

type staticMCPResolver struct {
	material  InvocationMCPMaterial
	revoked   []kernel.InvocationID
	revokeErr error
}

func (r *staticMCPResolver) ResolveInvocationMCP(context.Context, HostPreparation) (InvocationMCPMaterial, error) {
	return r.material, nil
}

func (r *staticMCPResolver) RevokeInvocationMCP(_ context.Context, invocationID kernel.InvocationID) error {
	if r.revokeErr != nil {
		return r.revokeErr
	}
	r.revoked = append(r.revoked, invocationID)
	return nil
}

type staticQwenPawProvider struct{ api *QwenPawAPI }

func (p staticQwenPawProvider) ForHost(context.Context, string) (*QwenPawAPI, error) {
	return p.api, nil
}

type failingQwenPawProvider struct{ err error }

func (p failingQwenPawProvider) ForHost(context.Context, string) (*QwenPawAPI, error) {
	return nil, p.err
}

type recordingTaskflow struct{}

func (recordingTaskflow) Call(_ context.Context, _ string, call TaskflowCall) (TaskflowCallResult, error) {
	return TaskflowCallResult{
		OK: true, Action: call.Action, Effective: true,
		Task: TaskSnapshot{TaskID: call.TaskID, ProjectID: kernel.ProjectID(call.ProjectID), HostRef: call.AssignedTo, Status: "submitted"},
	}, nil
}

type capturingTaskflow struct {
	container  string
	containers []string
	call       TaskflowCall
	calls      int
}

func (c *capturingTaskflow) Call(_ context.Context, container string, call TaskflowCall) (TaskflowCallResult, error) {
	c.container = container
	c.containers = append(c.containers, container)
	c.call = call
	c.calls++
	return TaskflowCallResult{
		OK: true, Action: call.Action, Effective: true,
		Task: TaskSnapshot{TaskID: call.TaskID, ProjectID: kernel.ProjectID(call.ProjectID), HostRef: call.AssignedTo, Status: "submitted"},
	}, nil
}

type fakeHostSlotStore struct {
	taskID       string
	claim        HostSlotClaim
	active       bool
	revoked      bool
	claimErr     error
	clearedHosts []string
	staleMCPKeys []string
}

func newFakeHostSlotStore(taskID string) *fakeHostSlotStore {
	return &fakeHostSlotStore{taskID: taskID}
}

func (s *fakeHostSlotStore) ActiveCounts(context.Context) (map[string]int, error) {
	if !s.active {
		return map[string]int{}, nil
	}
	return map[string]int{s.claim.HostRef: 1}, nil
}

func (s *fakeHostSlotStore) StaleMCPClientKeysByHost(context.Context, string) ([]string, error) {
	return append([]string(nil), s.staleMCPKeys...), nil
}

func (s *fakeHostSlotStore) Claim(_ context.Context, hostRef string, invocationID kernel.InvocationID, mcpClientKey string, tokenHash []byte, tokenIdentifier string) error {
	if s.claimErr != nil {
		return s.claimErr
	}
	s.active = true
	s.claim = HostSlotClaim{
		InvocationID:    invocationID,
		TaskID:          s.taskID,
		HostRef:         hostRef,
		MCPClientKey:    mcpClientKey,
		TokenHash:       append([]byte(nil), tokenHash...),
		TokenIdentifier: tokenIdentifier,
		ClaimedAt:       time.Unix(1, 0).UTC(),
	}
	return nil
}

func (s *fakeHostSlotStore) Release(context.Context, string, string) error {
	s.active = false
	return nil
}

func (s *fakeHostSlotStore) MarkRevoked(context.Context, string, string) error {
	s.revoked = true
	return nil
}

func (s *fakeHostSlotStore) MarkHostFenced(context.Context, string) error {
	s.revoked = true
	return nil
}

func (s *fakeHostSlotStore) BeginHostFence(context.Context, string) ([]HostSlotClaim, error) {
	if !s.active {
		return nil, nil
	}
	return []HostSlotClaim{s.claim}, nil
}

func (s *fakeHostSlotStore) CompleteHostFence(context.Context, string, []HostSlotClaim) error {
	s.revoked = true
	return nil
}

func (s *fakeHostSlotStore) ClearHostFenceIfReusable(_ context.Context, hostRef string) (bool, error) {
	s.clearedHosts = append(s.clearedHosts, hostRef)
	return true, nil
}

func (s *fakeHostSlotStore) ByInvocation(_ context.Context, _ string, invocationID kernel.InvocationID) (HostSlotClaim, bool, error) {
	return s.claim, s.claim.InvocationID == invocationID, nil
}

func (s *fakeHostSlotStore) ByTaskID(_ context.Context, taskID string) (HostSlotClaim, bool, error) {
	return s.claim, s.claim.TaskID == taskID, nil
}

func (s *fakeHostSlotStore) ActiveByHost(_ context.Context, hostRef string) ([]HostSlotClaim, error) {
	if !s.active || s.claim.HostRef != hostRef {
		return nil, nil
	}
	return []HostSlotClaim{s.claim}, nil
}
