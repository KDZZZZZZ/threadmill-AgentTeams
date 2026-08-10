package security_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

func TestBrowserAPIAndSSEShareProjectPrincipalAndCSRFGate(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC)
	authenticator := auth.NewAuthenticator(auth.NewMemoryStore(), func() time.Time { return now })
	session, csrf, err := authenticator.IssueOperatorSession(
		context.Background(),
		"operator-1",
		[]kernel.ProjectID{"project-a"},
		time.Hour,
	)
	if err != nil {
		t.Fatalf("issue operator session: %v", err)
	}

	apiPrincipal, sessionRecord, err := authenticator.AuthenticateOperatorSession(context.Background(), session, "project-a")
	if err != nil {
		t.Fatalf("authenticate ordinary API request: %v", err)
	}
	ssePrincipal, _, err := authenticator.AuthenticateOperatorSession(context.Background(), session, "project-a")
	if err != nil {
		t.Fatalf("authenticate SSE request: %v", err)
	}
	if apiPrincipal.ActorPrincipalID != ssePrincipal.ActorPrincipalID || apiPrincipal.ProjectID != ssePrincipal.ProjectID {
		t.Fatalf("API principal %#v differs from SSE principal %#v", apiPrincipal, ssePrincipal)
	}
	if _, _, err := authenticator.AuthenticateOperatorSession(context.Background(), session, "project-b"); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("cross-project SSE/API authentication = %v, want forbidden", err)
	}

	guard := auth.NewStateChangeGuard([]string{"https://threadmill.test"})
	request := httptest.NewRequest(http.MethodPost, "https://threadmill.test/v1/manager/messages", nil)
	request.Header.Set("Origin", "https://threadmill.test")
	request.Header.Set(auth.CSRFHeaderName, csrf)
	if err := guard.Check(request, sessionRecord); err != nil {
		t.Fatalf("valid same-origin state change rejected: %v", err)
	}
	request.Header.Set("Origin", "https://attacker.invalid")
	if err := guard.Check(request, sessionRecord); !kernel.IsCode(err, kernel.CodeOriginInvalid) {
		t.Fatalf("cross-origin state change = %v, want origin_invalid", err)
	}
}

func TestInvocationTokenCannotEscalateScopeAndRevocationIsImmediate(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC)
	store := auth.NewMemoryStore()
	authenticator := auth.NewAuthenticator(store, func() time.Time { return now })
	token, err := authenticator.IssueAgentToken(context.Background(), "agent-1", auth.Capability{
		ProjectID:    "project-a",
		TaskID:       "task-a",
		InvocationID: "invocation-a",
		Role:         auth.RoleExecutor,
		Tools:        auth.ToolSet(auth.ToolContextExplore, auth.ToolAgentSubmitPhaseOutput),
		ExpiresAt:    now.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("issue invocation token: %v", err)
	}
	if _, ok, err := store.TokenByHash(context.Background(), []byte(token)); err != nil || ok {
		t.Fatalf("raw opaque token must not be a persisted lookup key: ok=%v err=%v", ok, err)
	}

	principal, err := authenticator.AuthenticateAgentToken(context.Background(), token)
	if err != nil {
		t.Fatalf("authenticate invocation token: %v", err)
	}
	for name, requested := range map[string]auth.Scope{
		"project":    {ProjectID: "project-b"},
		"task":       {ProjectID: "project-a", TaskID: "task-b"},
		"invocation": {ProjectID: "project-a", InvocationID: "invocation-b"},
	} {
		if _, err := auth.RequireTool(principal, auth.ToolContextExplore, requested); !kernel.IsCode(err, kernel.CodeForbidden) {
			t.Errorf("%s override = %v, want forbidden", name, err)
		}
	}
	if _, err := auth.RequireTool(principal, auth.ToolCoordinationReplacePending, auth.Scope{ProjectID: "project-a"}); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("phase principal used TaskManagerGraph: %v", err)
	}

	if err := authenticator.RevokeAgentToken(context.Background(), token); err != nil {
		t.Fatalf("revoke invocation token: %v", err)
	}
	if _, err := authenticator.AuthenticateAgentToken(context.Background(), token); !kernel.IsCode(err, kernel.CodeUnauthorized) {
		t.Fatalf("revoked token authentication = %v, want unauthorized", err)
	}
}
