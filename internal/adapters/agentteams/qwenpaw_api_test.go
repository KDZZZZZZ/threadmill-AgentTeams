package agentteams

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

func TestQwenPawAPIInstallsAndRevokesInvocationMCP(t *testing.T) {
	const token = "invocation-secret-that-must-not-be-logged"
	state := newQwenPawContractState()
	server := httptest.NewServer(state)
	t.Cleanup(server.Close)

	api, err := NewQwenPawAPI(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	api.pollInterval = time.Millisecond
	desired := InvocationMCP{
		Key:           "threadmill-inv-a",
		URL:           "https://threadmill.example.test/mcp",
		BearerToken:   token,
		ExpectedTools: []string{"phase.submit", "context.explore"},
	}
	if err := api.InstallInvocationMCP(context.Background(), desired); err != nil {
		t.Fatalf("InstallInvocationMCP failed: %v", err)
	}

	state.mu.Lock()
	installed := state.clients[desired.Key]
	policy := state.policies[desired.Key]
	state.mu.Unlock()
	if installed.URL != desired.URL || installed.Transport != "streamable_http" || !installed.Enabled {
		t.Fatalf("installed MCP = %#v", installed)
	}
	if installed.Authorization != "Bearer "+token {
		t.Fatal("QwenPaw MCP did not receive the invocation bearer")
	}
	wantTools := []string{"context.explore", "phase.submit"}
	if !reflect.DeepEqual(installed.Tools, wantTools) {
		t.Fatalf("installed tools = %#v, want %#v", installed.Tools, wantTools)
	}
	if policy.DefaultEffect != "deny" || !reflect.DeepEqual(policy.AllowedTools(), wantTools) {
		t.Fatalf("policy = %#v, want deny-by-default with %#v", policy, wantTools)
	}

	if err := api.RevokeInvocationMCP(context.Background(), desired.Key); err != nil {
		t.Fatalf("RevokeInvocationMCP failed: %v", err)
	}
	state.mu.Lock()
	_, stillPresent := state.clients[desired.Key]
	state.mu.Unlock()
	if stillPresent {
		t.Fatal("revoked MCP client is still present")
	}
}

func TestQwenPawAPIDeletesCanonicalInvocationMCPWithoutListing(t *testing.T) {
	const existing = "threadmill-0123456789abcdef01234567"
	var listCalls atomic.Int32
	var deleteCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/mcp":
			listCalls.Add(1)
			http.Error(w, "collection is blocked by stale credentials", http.StatusGatewayTimeout)
		case r.Method == http.MethodDelete && r.URL.Path == "/api/mcp/"+existing:
			deleteCalls.Add(1)
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodDelete && r.URL.Path == "/api/mcp/threadmill-89abcdef0123456789abcdef":
			deleteCalls.Add(1)
			http.NotFound(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	api, err := NewQwenPawAPI(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}

	if err := api.DeleteInvocationMCPIfPresent(context.Background(), existing); err != nil {
		t.Fatalf("delete existing invocation MCP: %v", err)
	}
	if err := api.DeleteInvocationMCPIfPresent(context.Background(), "threadmill-89abcdef0123456789abcdef"); err != nil {
		t.Fatalf("delete missing invocation MCP: %v", err)
	}
	if err := api.DeleteInvocationMCPIfPresent(context.Background(), "operator-owned-client"); !kernel.IsCode(err, kernel.CodeInvalidRequest) {
		t.Fatalf("delete non-Threadmill MCP error = %v, want invalid_request", err)
	}
	if got := listCalls.Load(); got != 0 {
		t.Fatalf("GET /api/mcp calls = %d, want 0", got)
	}
	if got := deleteCalls.Load(); got != 2 {
		t.Fatalf("DELETE invocation MCP calls = %d, want 2", got)
	}
}

func TestQwenPawAPIPrunesOnlyHistoricalInvocationMCP(t *testing.T) {
	state := newQwenPawContractState()
	current := "threadmill-0123456789abcdef0123456789abcdef"
	stale24 := "threadmill-89abcdef0123456789abcdef"
	stale32 := "threadmill-fedcba9876543210fedcba9876543210"
	staleAttempt := "threadmill-eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee-attempt-335"
	state.clients[current] = qwenPawContractClient{Enabled: true}
	state.clients[stale24] = qwenPawContractClient{Enabled: true}
	state.clients[stale32] = qwenPawContractClient{Enabled: true}
	state.clients[staleAttempt] = qwenPawContractClient{Enabled: true}
	state.clients["teamharness"] = qwenPawContractClient{Enabled: true}
	state.clients["operator-threadmill-client"] = qwenPawContractClient{Enabled: true}
	server := httptest.NewServer(state)
	t.Cleanup(server.Close)
	api, err := NewQwenPawAPI(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if err := api.PruneInvocationMCPExcept(context.Background(), current); err != nil {
		t.Fatal(err)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if _, ok := state.clients[current]; !ok {
		t.Fatal("current invocation MCP was pruned")
	}
	if _, ok := state.clients["teamharness"]; !ok {
		t.Fatal("package MCP was pruned")
	}
	if _, ok := state.clients["operator-threadmill-client"]; !ok {
		t.Fatal("operator MCP was pruned")
	}
	if _, ok := state.clients[stale24]; ok {
		t.Fatal("24-character stale invocation MCP remains")
	}
	if _, ok := state.clients[stale32]; ok {
		t.Fatal("32-character stale invocation MCP remains")
	}
	if _, ok := state.clients[staleAttempt]; ok {
		t.Fatal("retry-attempt stale invocation MCP remains")
	}
}

func TestInvocationMCPRecoveryKeyShape(t *testing.T) {
	for _, key := range []string{
		"threadmill-0123456789abcdef01234567",
		"threadmill-0123456789abcdef0123456789abcdef",
		"threadmill-0123456789abcdef0123456789abcdef-attempt-335",
	} {
		if !isInvocationMCPKey(key) {
			t.Fatalf("canonical invocation MCP key %q was rejected", key)
		}
	}
	for _, key := range []string{
		"threadmill-invocation-name",
		"threadmill-0123456789ABCDEF01234567",
		"threadmill-0123456789abcdef0123456g",
		"threadmill-0123456789abcdef0123456789abcdef-attempt-",
		"threadmill-0123456789abcdef0123456789abcdef-attempt-x",
		"other-0123456789abcdef01234567",
	} {
		if isInvocationMCPKey(key) {
			t.Fatalf("non-canonical invocation MCP key %q was accepted", key)
		}
	}
}

func TestQwenPawAPIReturnsAuthoritativeAgentActivity(t *testing.T) {
	lastRun := time.Date(2026, time.August, 11, 10, 0, 0, 0, time.UTC)
	lastFinish := lastRun.Add(2 * time.Minute)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/agents/default/agent-status" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":             "running",
			"running_task_count": 0,
			"last_run_at":        lastRun,
			"last_finish_at":     lastFinish,
		})
	}))
	t.Cleanup(server.Close)
	api, err := NewQwenPawAPI(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}

	activity, err := api.AgentActivity(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if activity.Status != "running" || activity.RunningTaskCount != 0 || !activity.LastRunAt.Equal(lastRun) || !activity.LastFinishAt.Equal(lastFinish) {
		t.Fatalf("activity = %#v", activity)
	}
}

func TestQwenPawAPIWaitsForAnUninterruptedIdleWindowAfterHotReload(t *testing.T) {
	state := newQwenPawContractState()
	const key = "threadmill-0123456789abcdef01234567"
	state.clients[key] = qwenPawContractClient{Enabled: true, Tools: []string{"phase.submit"}}
	for name := range state.builtinTools {
		if _, required := qwenPawNativeProjectTools[name]; required {
			state.builtinTools[name] = true
		}
	}
	var statusCalls atomic.Int32
	var reloadFailureAt atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/api/agents/default/agent-status" {
			call := statusCalls.Add(1)
			if call == 3 {
				reloadFailureAt.Store(time.Now().UnixNano())
				http.Error(w, "workspace is reloading", http.StatusServiceUnavailable)
				return
			}
		}
		state.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	api, err := NewQwenPawAPI(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	api.pollInterval = 2 * time.Millisecond
	api.stableWindow = 10 * time.Millisecond

	if err := api.WaitInvocationReady(context.Background(), key, []string{"phase.submit"}); err != nil {
		t.Fatalf("WaitInvocationReady() error = %v", err)
	}
	failedAt := reloadFailureAt.Load()
	if failedAt == 0 {
		t.Fatal("agent-status never observed the simulated hot reload failure")
	}
	if elapsed := time.Since(time.Unix(0, failedAt)); elapsed < api.stableWindow {
		t.Fatalf("returned %s after reload failure, want a fresh stable window of at least %s", elapsed, api.stableWindow)
	}
}

func TestQwenPawAPIWaitStartupReadyWaitsBeyondVersion(t *testing.T) {
	state := newQwenPawContractState()
	var toolCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/api/agents/default/tools" {
			if toolCalls.Add(1) <= 2 {
				http.Error(w, "wrapper desired state is still applying", http.StatusServiceUnavailable)
				return
			}
		}
		state.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	api, err := NewQwenPawAPI(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	api.pollInterval = 2 * time.Millisecond

	if err := api.WaitStartupReady(context.Background()); err != nil {
		t.Fatalf("WaitStartupReady() error = %v", err)
	}
	if got := toolCalls.Load(); got < 3 {
		t.Fatalf("tool readiness probes = %d, want retries after version was already reachable", got)
	}
}

func TestQwenPawAPIEnablesNativeProjectToolsWithoutChangingOtherTools(t *testing.T) {
	state := newQwenPawContractState()
	server := httptest.NewServer(state)
	t.Cleanup(server.Close)
	api, err := NewQwenPawAPI(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}

	if err := api.EnsureNativeProjectTools(context.Background()); err != nil {
		t.Fatalf("EnsureNativeProjectTools failed: %v", err)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	for _, name := range []string{"read_file", "write_file", "edit_file", "grep_search", "glob_search", "execute_shell_command", "web_search"} {
		if !state.builtinTools[name] {
			t.Fatalf("native project tool %s was not enabled", name)
		}
	}
	if state.builtinTools["spawn_subagent"] {
		t.Fatal("unrelated sub-agent tool was changed")
	}
}

func TestQwenPawAPIFailsClosedWhenNativeProjectToolReadbackStaysDisabled(t *testing.T) {
	state := newQwenPawContractState()
	state.ignoreBuiltinToggle = true
	server := httptest.NewServer(state)
	t.Cleanup(server.Close)
	api, err := NewQwenPawAPI(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}

	err = api.EnsureNativeProjectTools(context.Background())
	if !kernel.IsCode(err, kernel.CodeExecutorUnavailable) {
		t.Fatalf("native project tool error = %v, want executor_unavailable", err)
	}
}

func TestQwenPawAPIFailsClosedWithoutLeakingBearer(t *testing.T) {
	const token = "do-not-expose-this-token"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/version" {
			_ = json.NewEncoder(w).Encode(map[string]string{"version": qwenPawAPIVersion})
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("provider echoed " + token))
	}))
	t.Cleanup(server.Close)

	api, err := NewQwenPawAPI(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	err = api.InstallInvocationMCP(context.Background(), InvocationMCP{
		Key:         "threadmill-secret-test",
		URL:         "https://threadmill.example.test/mcp",
		BearerToken: token,
	})
	if err == nil {
		t.Fatal("InstallInvocationMCP unexpectedly succeeded")
	}
	if strings.Contains(err.Error(), token) {
		t.Fatalf("error leaked bearer token: %v", err)
	}
}

func TestQwenPawAPIRollsBackClientWhenPolicyInstallationFails(t *testing.T) {
	state := newQwenPawContractState()
	state.failPolicy = true
	server := httptest.NewServer(state)
	t.Cleanup(server.Close)
	api, err := NewQwenPawAPI(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	err = api.InstallInvocationMCP(context.Background(), InvocationMCP{
		Key:         "threadmill-rollback",
		URL:         "https://threadmill.example.test/mcp",
		BearerToken: "short-lived-secret",
	})
	if err == nil {
		t.Fatal("InstallInvocationMCP unexpectedly succeeded")
	}
	state.mu.Lock()
	_, stillPresent := state.clients["threadmill-rollback"]
	state.mu.Unlock()
	if stillPresent {
		t.Fatal("failed installation left an invocation MCP client active")
	}
}

func TestQwenPawAPIRollsBackClientWhenPolicyReadbackContainsOverride(t *testing.T) {
	state := newQwenPawContractState()
	state.injectPolicyOverride = true
	server := httptest.NewServer(state)
	t.Cleanup(server.Close)
	api, err := NewQwenPawAPI(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	err = api.InstallInvocationMCP(context.Background(), InvocationMCP{
		Key:           "threadmill-policy-override",
		URL:           "https://threadmill.example.test/mcp",
		BearerToken:   "short-lived-secret",
		ExpectedTools: []string{"phase.submit"},
	})
	if err == nil {
		t.Fatal("InstallInvocationMCP accepted an unexpected policy override")
	}
	state.mu.Lock()
	_, stillPresent := state.clients["threadmill-policy-override"]
	state.mu.Unlock()
	if stillPresent {
		t.Fatal("policy readback mismatch left an invocation MCP client active")
	}
}

func TestQwenPawAPIRejectsUnsafeURLsAndUnexpectedVersion(t *testing.T) {
	if _, err := NewQwenPawAPI("file:///tmp/qwenpaw", nil); err == nil {
		t.Fatal("file QwenPaw URL accepted")
	}
	if _, err := NewQwenPawAPI("https://qwenpaw.example.test", nil); err == nil {
		t.Fatal("non-loopback QwenPaw management URL accepted")
	}
	if _, err := NewQwenPawAPI("http://127.0.0.1:8088/api", nil); err == nil {
		t.Fatal("QwenPaw management URL with path accepted")
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"version": "1.0.0"})
	}))
	t.Cleanup(server.Close)
	api, err := NewQwenPawAPI(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	err = api.Ready(context.Background())
	if !kernel.IsCode(err, kernel.CodeExecutorUnavailable) {
		t.Fatalf("Ready error = %v, want executor_unavailable", err)
	}

	err = api.InstallInvocationMCP(context.Background(), InvocationMCP{
		Key:         "threadmill-a",
		URL:         "http://user:password@example.test/mcp",
		BearerToken: "secret",
	})
	if err == nil {
		t.Fatal("MCP URL with userinfo accepted")
	}
	err = api.InstallInvocationMCP(context.Background(), InvocationMCP{
		Key:         "../another-client",
		URL:         "https://threadmill.example.test/mcp",
		BearerToken: "secret",
	})
	if err == nil {
		t.Fatal("unsafe MCP key accepted")
	}
	err = api.InstallInvocationMCP(context.Background(), InvocationMCP{
		Key:           "threadmill-empty-tool",
		URL:           "https://threadmill.example.test/mcp",
		BearerToken:   "secret",
		ExpectedTools: []string{"phase.submit", " "},
	})
	if err == nil {
		t.Fatal("blank expected MCP tool accepted")
	}
}

func TestInvocationMCPKeyMatchesOpaqueProviderTask(t *testing.T) {
	first, err := InvocationMCPKey("threadmill-0123456789abcdef0123456789abcdef")
	if err != nil {
		t.Fatal(err)
	}
	second, err := InvocationMCPKey("threadmill-0123456789abcdef0123456789abcdef")
	if err != nil {
		t.Fatal(err)
	}
	if first != second || first != "threadmill-0123456789abcdef0123456789abcdef" {
		t.Fatalf("keys = %q/%q", first, second)
	}
	if _, err := InvocationMCPKey("invocation-visible-name"); !kernel.IsCode(err, kernel.CodeInvalidRequest) {
		t.Fatalf("non-provider task MCP key error = %v", err)
	}
}

type qwenPawContractState struct {
	mu                   sync.Mutex
	clients              map[string]qwenPawContractClient
	policies             map[string]qwenPawContractPolicy
	failPolicy           bool
	injectPolicyOverride bool
	builtinTools         map[string]bool
	ignoreBuiltinToggle  bool
	deleteCalls          []string
}

type qwenPawContractClient struct {
	Enabled       bool
	Transport     string
	URL           string
	Authorization string
	Tools         []string
}

type qwenPawContractPolicy struct {
	DefaultEffect   string                      `json:"default_effect"`
	ClientOverrides []map[string]any            `json:"client_overrides"`
	ToolDefaults    []qwenPawContractToolPolicy `json:"tool_defaults"`
	ToolOverrides   []map[string]any            `json:"tool_overrides"`
}

type qwenPawContractToolPolicy struct {
	ToolName string `json:"tool_name"`
	Effect   string `json:"effect"`
}

func (p qwenPawContractPolicy) AllowedTools() []string {
	var tools []string
	for _, policy := range p.ToolDefaults {
		if policy.Effect == "allow" {
			tools = append(tools, policy.ToolName)
		}
	}
	return normalizeToolNames(tools)
}

func newQwenPawContractState() *qwenPawContractState {
	return &qwenPawContractState{
		clients:  make(map[string]qwenPawContractClient),
		policies: make(map[string]qwenPawContractPolicy),
		builtinTools: map[string]bool{
			"read_file": false, "write_file": false, "edit_file": false, "append_file": false,
			"grep_search": false, "glob_search": false, "ast_search": false,
			"execute_shell_command": false, "web_search": false, "spawn_subagent": false,
		},
	}
}

func (s *qwenPawContractState) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")

	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/api/version":
		_ = json.NewEncoder(w).Encode(map[string]string{"version": qwenPawAPIVersion})
	case r.Method == http.MethodGet && r.URL.Path == "/api/agents/default/agent-status":
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "idle", "running_task_count": 0})
	case r.Method == http.MethodGet && r.URL.Path == "/api/agents/default/tools":
		items := make([]map[string]any, 0, len(s.builtinTools))
		for name, enabled := range s.builtinTools {
			items = append(items, map[string]any{"name": name, "enabled": enabled})
		}
		_ = json.NewEncoder(w).Encode(items)
	case r.Method == http.MethodPatch && strings.HasPrefix(r.URL.Path, "/api/agents/default/tools/") && strings.HasSuffix(r.URL.Path, "/toggle"):
		name := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/agents/default/tools/"), "/toggle")
		enabled, ok := s.builtinTools[name]
		if !ok {
			http.NotFound(w, r)
			return
		}
		if !s.ignoreBuiltinToggle {
			s.builtinTools[name] = !enabled
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"name": name, "enabled": s.builtinTools[name]})
	case r.Method == http.MethodGet && r.URL.Path == "/api/mcp":
		items := make([]map[string]string, 0, len(s.clients))
		for key := range s.clients {
			items = append(items, map[string]string{"key": key})
		}
		_ = json.NewEncoder(w).Encode(items)
	case r.Method == http.MethodPost && r.URL.Path == "/api/mcp":
		var payload struct {
			Key    string `json:"client_key"`
			Client struct {
				Enabled   bool              `json:"enabled"`
				Transport string            `json:"transport"`
				URL       string            `json:"url"`
				Headers   map[string]string `json:"headers"`
				Tools     []string          `json:"tools"`
			} `json:"client"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		s.clients[payload.Key] = qwenPawContractClient{
			Enabled:       payload.Client.Enabled,
			Transport:     payload.Client.Transport,
			URL:           payload.Client.URL,
			Authorization: payload.Client.Headers["Authorization"],
			Tools:         append([]string(nil), payload.Client.Tools...),
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{}`))
	case strings.HasPrefix(r.URL.Path, "/api/mcp/policy/"):
		key := strings.TrimPrefix(r.URL.Path, "/api/mcp/policy/")
		if r.Method == http.MethodPut {
			if s.failPolicy {
				http.Error(w, "policy unavailable", http.StatusServiceUnavailable)
				return
			}
			var policy qwenPawContractPolicy
			_ = json.NewDecoder(r.Body).Decode(&policy)
			s.policies[key] = policy
			_, _ = w.Write([]byte(`{}`))
			return
		}
		policy := s.policies[key]
		if s.injectPolicyOverride {
			policy.ToolOverrides = []map[string]any{{"tool_name": "unsafe.unlisted", "effect": "allow"}}
		}
		_ = json.NewEncoder(w).Encode(policy)
	case strings.HasPrefix(r.URL.Path, "/api/mcp/tools/") && r.Method == http.MethodGet:
		key := strings.TrimPrefix(r.URL.Path, "/api/mcp/tools/")
		client := s.clients[key]
		items := make([]map[string]any, 0, len(client.Tools)+1)
		for _, tool := range client.Tools {
			items = append(items, map[string]any{"name": tool, "enabled": true})
		}
		items = append(items, map[string]any{"name": "unsafe.unlisted", "enabled": false})
		_ = json.NewEncoder(w).Encode(items)
	case strings.HasPrefix(r.URL.Path, "/api/mcp/"):
		key := strings.TrimPrefix(r.URL.Path, "/api/mcp/")
		switch r.Method {
		case http.MethodGet:
			client, ok := s.clients[key]
			if !ok {
				http.NotFound(w, r)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"enabled": client.Enabled, "transport": client.Transport, "url": client.URL,
				"tools": client.Tools,
			})
		case http.MethodPut:
			var payload struct {
				Enabled   bool              `json:"enabled"`
				Transport string            `json:"transport"`
				URL       string            `json:"url"`
				Headers   map[string]string `json:"headers"`
				Tools     []string          `json:"tools"`
			}
			_ = json.NewDecoder(r.Body).Decode(&payload)
			s.clients[key] = qwenPawContractClient{
				Enabled: payload.Enabled, Transport: payload.Transport, URL: payload.URL,
				Authorization: payload.Headers["Authorization"], Tools: append([]string(nil), payload.Tools...),
			}
			_, _ = w.Write([]byte(`{}`))
		case http.MethodDelete:
			s.deleteCalls = append(s.deleteCalls, key)
			delete(s.clients, key)
			w.WriteHeader(http.StatusNoContent)
		}
	default:
		http.NotFound(w, r)
	}
}
