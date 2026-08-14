package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/config"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/objectstore"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/transport/httpapi"
)

const productionTestOrigin = "https://threadmill.example"

func TestProductionHandlerDoesNotGrantAnonymousOperatorSession(t *testing.T) {
	handler, _ := productionSecurityTestHandler(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/capacity?project_id=project-1", nil)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, req)

	if response.Code != http.StatusUnauthorized {
		t.Fatalf("anonymous response status = %d, want %d; body=%s", response.Code, http.StatusUnauthorized, response.Body.String())
	}
	if got := response.Header().Values("Set-Cookie"); len(got) != 0 {
		t.Fatalf("anonymous response Set-Cookie = %v, want none", got)
	}
}

func TestProductionWritesRejectInvalidOriginAndCSRF(t *testing.T) {
	handler, authenticator := productionSecurityTestHandler(t)
	sessionSecret, csrfSecret, err := authenticator.IssueOperatorSession(
		context.Background(),
		"operator://test",
		[]kernel.ProjectID{"project-1"},
		time.Hour,
	)
	if err != nil {
		t.Fatalf("IssueOperatorSession() error = %v", err)
	}

	tests := []struct {
		name     string
		origin   string
		csrf     string
		wantCode kernel.ErrorCode
	}{
		{name: "origin", origin: "https://attacker.example", csrf: csrfSecret, wantCode: kernel.CodeOriginInvalid},
		{name: "csrf", origin: productionTestOrigin, csrf: "wrong-csrf", wantCode: kernel.CodeCSRFInvalid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body := bytes.NewBufferString(`{"request_id":"capacity-1","project_id":"project-1","expected_revision":1,"desired_concurrency":2}`)
			req := httptest.NewRequest(http.MethodPost, "/v1/capacity-adjustments", body)
			req.AddCookie(&http.Cookie{Name: auth.SessionCookieName, Value: sessionSecret})
			req.Header.Set("Origin", test.origin)
			req.Header.Set(auth.CSRFHeaderName, test.csrf)
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, req)

			if response.Code != http.StatusForbidden {
				t.Fatalf("response status = %d, want %d; body=%s", response.Code, http.StatusForbidden, response.Body.String())
			}
			var coded kernel.Error
			if err := json.Unmarshal(response.Body.Bytes(), &coded); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if coded.Code != test.wantCode {
				t.Fatalf("error code = %q, want %q", coded.Code, test.wantCode)
			}
		})
	}
}

func TestInjectedProductionHostFailsClosedWhenRuntimeDependenciesAreMissing(t *testing.T) {
	err := validateProductionRuntimeDependencies(productionRuntimeDependencies{})
	if err == nil {
		t.Fatal("validateProductionRuntimeDependencies() error = nil, want missing runtime dependency error")
	}
	for _, dependency := range []string{"manager", "phase_runtime", "phase_controller", "workspace", "agentteams_readiness"} {
		if !strings.Contains(err.Error(), dependency) {
			t.Fatalf("newProductionHost() error = %q, want missing dependency %q", err, dependency)
		}
	}
}

func TestProductionReadinessReportsUnconfiguredExternalDependencies(t *testing.T) {
	status := (productionReadiness{db: productionTestPinger{}}).Readiness(context.Background())
	if status.Status != "not_ready" {
		t.Fatalf("readiness status = %q, want not_ready", status.Status)
	}
	got := make(map[string]httpapi.DependencyReadiness, len(status.Dependencies))
	for _, dependency := range status.Dependencies {
		got[dependency.Name] = dependency
	}
	if got["postgres"].Status != "ready" {
		t.Fatalf("postgres readiness = %#v, want ready", got["postgres"])
	}
	for _, name := range []string{"object_store", "agentteams", "runtime"} {
		dependency := got[name]
		if dependency.Status != "not_ready" || dependency.Message != "not configured" {
			t.Fatalf("%s readiness = %#v, want not_ready/not configured", name, dependency)
		}
	}
}

func TestProductionReadinessPropagatesDependencyFailure(t *testing.T) {
	status := (productionReadiness{
		db:          productionTestPinger{},
		objectStore: productionTestProbe{},
		agentTeams:  productionTestProbe{err: errors.New("controller unavailable")},
		runtime:     productionTestProbe{},
	}).Readiness(context.Background())
	if status.Status != "not_ready" {
		t.Fatalf("readiness status = %q, want not_ready", status.Status)
	}
	for _, dependency := range status.Dependencies {
		if dependency.Name == "agentteams" && dependency.Message != "controller unavailable" {
			t.Fatalf("agentteams readiness = %#v, want failure message", dependency)
		}
	}
}

func TestProductionEvidenceObjectStoreUsesOneNamespaceAcrossRuntimeAndMCP(t *testing.T) {
	base := objectstore.NewMemoryStore()
	cfg := config.Config{ObjectStoreBucket: "artifacts", AgentTeamsSharedBucket: "artifacts"}
	phaseStore, err := productionEvidenceObjectStore(cfg, base)
	if err != nil {
		t.Fatal(err)
	}
	mcpStore, err := productionEvidenceObjectStore(cfg, base)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mcpStore.Put(context.Background(), objectstore.PutObject{
		Bucket: "artifacts", Key: "plan/hash", Body: strings.NewReader("registered by MCP"),
	}); err != nil {
		t.Fatal(err)
	}
	opened, err := phaseStore.Get(context.Background(), objectstore.ObjectRef{Bucket: "artifacts", Key: "plan/hash"})
	if err != nil {
		t.Fatalf("phase runtime could not open MCP artifact: %v", err)
	}
	_ = opened.Body.Close()
	physical, err := base.Get(context.Background(), objectstore.ObjectRef{Bucket: "artifacts", Key: "shared/threadmill/evidence/plan/hash"})
	if err != nil {
		t.Fatalf("physical evidence object is not in the restricted shared namespace: %v", err)
	}
	_ = physical.Body.Close()
}

func TestProductionInvocationToolCallGuardRejectsTerminalInvocation(t *testing.T) {
	store := runtime.NewMemoryInvocationStore()
	created := time.Date(2026, 8, 12, 6, 0, 0, 0, time.UTC)
	invocation := runtime.Invocation{
		ID: "inv-guard", ActorPrincipalID: "agent-guard", ProjectID: "project-guard",
		TaskID: "task-guard", EndpointID: "execute", Generation: 1, Role: auth.RoleExecutor,
		Status: runtime.InvocationRunning, BindingRef: "binding-guard", LeaseID: "lease-guard",
		EffectiveTools: []auth.Tool{auth.ToolWorkspaceRead}, PromptHashes: map[string]string{"p": "h"},
		SkillHashes: map[string]string{"s": "h"}, CreatedAt: created, ExpiresAt: created.Add(time.Hour),
	}
	if err := store.Create(context.Background(), invocation); err != nil {
		t.Fatal(err)
	}
	principal := auth.Principal{ActorPrincipalID: invocation.ActorPrincipalID, Kind: auth.PrincipalAgent, ProjectID: invocation.ProjectID, TaskID: invocation.TaskID, Role: invocation.Role, InvocationID: invocation.ID}
	guard := productionInvocationToolCallGuard{invocations: store}
	if err := guard.AuthorizeToolCall(context.Background(), principal); err != nil {
		t.Fatalf("active guard: %v", err)
	}
	if err := store.Transition(context.Background(), invocation.ID, runtime.InvocationRunning, runtime.InvocationCompleted); err != nil {
		t.Fatal(err)
	}
	if err := guard.AuthorizeToolCall(context.Background(), principal); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("terminal guard error=%v, want forbidden", err)
	}
}

func TestProductionTaskBlockersProjectsOnlyAuthoritativeTaskTargets(t *testing.T) {
	graph := coordination.GraphSnapshot{Blockers: []coordination.Blocker{
		{ID: "blocker-z", Target: coordination.PhaseEndpointRef{TaskID: "task-a", EndpointID: coordination.EndpointVerify}, State: coordination.BlockerResolved},
		{ID: "blocker-other", Target: coordination.PhaseEndpointRef{TaskID: "task-b", EndpointID: coordination.EndpointPlan}, State: coordination.BlockerActive},
		{ID: "blocker-a", Target: coordination.PhaseEndpointRef{TaskID: "task-a", EndpointID: coordination.EndpointExecute}, State: coordination.BlockerActive},
	}}

	got := productionTaskBlockers(graph, "task-a")
	if len(got) != 2 || got[0].BlockerID != "blocker-a" || got[1].BlockerID != "blocker-z" {
		t.Fatalf("task blocker projection = %#v", got)
	}
	if got[0].Target.TaskID != "task-a" || got[0].State != string(coordination.BlockerActive) || got[1].State != string(coordination.BlockerResolved) {
		t.Fatalf("task blocker authority lost: %#v", got)
	}
}

func productionSecurityTestHandler(t *testing.T) (http.Handler, *auth.Authenticator) {
	t.Helper()
	now := time.Date(2026, time.August, 11, 10, 0, 0, 0, time.UTC)
	authenticator := auth.NewAuthenticator(auth.NewMemoryStore(), func() time.Time { return now })
	api := newProductionAPIHandler(config.Config{AllowedOrigins: []string{productionTestOrigin}}, httpapi.Options{
		Authenticator: authenticator,
	})
	host := &productionHost{api: api, mcp: http.NotFoundHandler(), web: http.NotFoundHandler()}
	return host.Handler(), authenticator
}

type productionTestPinger struct{}

func (productionTestPinger) Ping(context.Context) error { return nil }

type productionTestProbe struct{ err error }

func (p productionTestProbe) Check(context.Context) error { return p.err }
