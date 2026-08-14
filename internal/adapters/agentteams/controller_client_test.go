package agentteams

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

func TestAgentTeamsControllerClientListsHostsAndUsesBearer(t *testing.T) {
	const token = "controller-token-that-must-not-leak"
	var seen []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			t.Fatalf("Authorization = %q", r.Header.Get("Authorization"))
		}
		seen = append(seen, r.Method+" "+r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/workers":
			_ = json.NewEncoder(w).Encode(map[string]any{"workers": []map[string]any{{
				"name":          "worker-a",
				"phase":         "Ready",
				"runtime":       "qwenpaw",
				"model":         "deepseek",
				"skills":        []string{"go", "repo"},
				"lastHeartbeat": time.Now().UTC().Format(time.RFC3339Nano),
			}}})
		case "/api/v1/managers":
			_ = json.NewEncoder(w).Encode(map[string]any{"managers": []map[string]any{{
				"name": "manager-a", "phase": "Running", "runtime": "qwenpaw", "model": "deepseek",
			}}})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	client, err := NewAgentTeamsControllerClient(server.URL, token, server.Client())
	if err != nil {
		t.Fatal(err)
	}

	hosts, err := client.ListHosts(context.Background())
	if err != nil {
		t.Fatalf("ListHosts() error = %v", err)
	}
	if len(hosts) != 2 || hosts[0].Ref != "manager-a" || hosts[0].Kind != HostManager ||
		hosts[1].Ref != "worker-a" || hosts[1].Kind != HostWorker || hosts[1].Phase != "Running" {
		t.Fatalf("hosts = %#v", hosts)
	}
	if strings.Join(seen, ",") != "GET /api/v1/workers,GET /api/v1/managers" {
		t.Fatalf("controller calls = %v", seen)
	}
}

func TestParseControllerTimeLeavesMissingHeartbeatUnknown(t *testing.T) {
	if got := parseControllerTime(""); !got.IsZero() {
		t.Fatalf("missing heartbeat parsed as %v, want zero", got)
	}
}

func TestAgentTeamsControllerClientLifecycleAndFailureRedaction(t *testing.T) {
	const token = "secret-controller-token"
	var calls []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls = append(calls, r.Method+" "+r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api/v1/workers/worker-a/ensure-ready" {
			_, _ = w.Write([]byte(`{"name":"worker-a","phase":"Running"}`))
			return
		}
		if r.URL.Path == "/api/v1/workers/worker-a" {
			var payload map[string]string
			_ = json.NewDecoder(r.Body).Decode(&payload)
			if payload["state"] != "Stopped" {
				t.Fatalf("stop payload = %#v", payload)
			}
			_, _ = w.Write([]byte(`{}`))
			return
		}
		if r.URL.Path == "/api/v1/managers/manager-a" {
			var payload map[string]string
			_ = json.NewDecoder(r.Body).Decode(&payload)
			if payload["state"] != "Stopped" {
				t.Fatalf("manager stop payload = %#v", payload)
			}
			_, _ = w.Write([]byte(`{}`))
			return
		}
		http.Error(w, "provider echoed "+token, http.StatusInternalServerError)
	}))
	t.Cleanup(server.Close)
	client, err := NewAgentTeamsControllerClient(server.URL, token, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if err := client.EnsureWorkerReady(context.Background(), "worker-a"); err != nil {
		t.Fatalf("EnsureWorkerReady() error = %v", err)
	}
	if err := client.StopWorker(context.Background(), "worker-a"); err != nil {
		t.Fatalf("StopWorker() error = %v", err)
	}
	err = client.WakeWorker(context.Background(), "worker-a")
	if !kernel.IsCode(err, kernel.CodeExecutorUnavailable) {
		t.Fatalf("WakeWorker() error = %v, want executor_unavailable", err)
	}
	if strings.Contains(err.Error(), token) {
		t.Fatalf("controller error leaked bearer: %v", err)
	}
	if err := client.StopManager(context.Background(), "manager-a"); err != nil {
		t.Fatalf("StopManager() error = %v", err)
	}
	if strings.Join(calls, ",") != "POST /api/v1/workers/worker-a/ensure-ready,PUT /api/v1/workers/worker-a,POST /api/v1/workers/worker-a/wake,PUT /api/v1/managers/manager-a" {
		t.Fatalf("calls = %v", calls)
	}
}

func TestAgentTeamsControllerClientRejectsUnsafeInputs(t *testing.T) {
	if _, err := NewAgentTeamsControllerClient("file:///tmp/controller", "token", nil); err == nil {
		t.Fatal("file controller URL accepted")
	}
	if _, err := NewAgentTeamsControllerClient("http://user:pass@127.0.0.1:8080", "token", nil); err == nil {
		t.Fatal("controller URL with userinfo accepted")
	}
	if _, err := NewAgentTeamsControllerClient("http://127.0.0.1:8080?token=secret", "token", nil); err == nil {
		t.Fatal("controller URL with query accepted")
	}
	if _, err := NewAgentTeamsControllerClient("http://127.0.0.1:8080", "bad token", nil); err == nil {
		t.Fatal("controller bearer with whitespace accepted")
	}
}
