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

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/config"
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
