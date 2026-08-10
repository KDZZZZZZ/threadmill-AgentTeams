package agentteams

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

func TestProductionClientImplementsClientFlowWithoutMatrixParsing(t *testing.T) {
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/version":
			_ = json.NewEncoder(w).Encode(map[string]string{"version": "test"})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/workers":
			_ = json.NewEncoder(w).Encode(map[string]any{"workers": []map[string]any{{
				"name": "worker-a", "phase": "Running", "runtime": "qwenpaw", "lastHeartbeat": "2026-08-11T10:00:00Z",
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
	client, err := NewProductionClient(ProductionClientOptions{
		Controller:  controllerClient,
		Slots:       slotStore,
		MCPResolver: staticMCPResolver{material: InvocationMCPMaterial{URL: "https://threadmill.example.test/mcp", BearerToken: "short-token", TokenIdentifier: "tok-1", ExpectedTools: []string{"phase.submit"}}},
		QwenPaw:     staticQwenPawProvider{api: qwenAPI},
		Taskflow:    recordingTaskflow{},
		Containers:  StaticContainerResolver{"worker-a": "qwenpaw-worker-a"},
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := client.PrepareHost(context.Background(), HostPreparation{
		HostRef: "worker-a", InvocationID: "inv-prod", Role: auth.RoleExecutor, RuntimeConfigRef: "runtime", EnvelopeRef: "envelope",
	}); err != nil {
		t.Fatalf("PrepareHost() error = %v", err)
	}
	key, err := InvocationMCPKey("inv-prod")
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
	if raw, err := client.ReadObservations(context.Background(), "cursor"); err != nil || len(raw) != 0 {
		t.Fatalf("ReadObservations() = %#v, %v; want no Matrix-derived events", raw, err)
	}
}

type staticMCPResolver struct{ material InvocationMCPMaterial }

func (r staticMCPResolver) ResolveInvocationMCP(context.Context, HostPreparation) (InvocationMCPMaterial, error) {
	return r.material, nil
}

type staticQwenPawProvider struct{ api *QwenPawAPI }

func (p staticQwenPawProvider) ForHost(context.Context, string) (*QwenPawAPI, error) {
	return p.api, nil
}

type recordingTaskflow struct{}

func (recordingTaskflow) Call(_ context.Context, _ string, call TaskflowCall) (TaskflowCallResult, error) {
	return TaskflowCallResult{
		OK: true, Action: call.Action, Effective: true,
		Task: TaskSnapshot{TaskID: call.TaskID, ProjectID: kernel.ProjectID(call.ProjectID), HostRef: call.AssignedTo, Status: "submitted"},
	}, nil
}

type fakeHostSlotStore struct {
	taskID  string
	claim   HostSlotClaim
	active  bool
	revoked bool
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

func (s *fakeHostSlotStore) Claim(_ context.Context, hostRef string, invocationID kernel.InvocationID, mcpClientKey string, tokenHash []byte, tokenIdentifier string) error {
	s.active = true
	s.claim = HostSlotClaim{
		InvocationID:    invocationID,
		TaskID:          s.taskID,
		HostRef:         hostRef,
		MCPClientKey:    mcpClientKey,
		TokenHash:       append([]byte(nil), tokenHash...),
		TokenIdentifier: tokenIdentifier,
	}
	return nil
}

func (s *fakeHostSlotStore) Release(context.Context, string, string) error {
	s.active = false
	return nil
}

func (s *fakeHostSlotStore) MarkRevoked(context.Context, string, kernel.InvocationID) error {
	s.revoked = true
	return nil
}

func (s *fakeHostSlotStore) ByInvocation(_ context.Context, _ string, invocationID kernel.InvocationID) (HostSlotClaim, bool, error) {
	return s.claim, s.claim.InvocationID == invocationID, nil
}

func (s *fakeHostSlotStore) ByTaskID(_ context.Context, taskID string) (HostSlotClaim, bool, error) {
	return s.claim, s.claim.TaskID == taskID, nil
}
