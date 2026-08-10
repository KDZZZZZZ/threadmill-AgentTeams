package auth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

func TestOperatorSessionProjectACLAndCSRF(t *testing.T) {
	now := time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)
	authenticator := NewAuthenticator(NewMemoryStore(), func() time.Time { return now })
	session, csrf, err := authenticator.IssueOperatorSession(context.Background(), "operator-1", []kernel.ProjectID{"project-a"}, time.Hour)
	if err != nil {
		t.Fatalf("IssueOperatorSession() error = %v", err)
	}

	principal, record, err := authenticator.AuthenticateOperatorSession(context.Background(), session, "project-a")
	if err != nil {
		t.Fatalf("AuthenticateOperatorSession() error = %v", err)
	}
	if principal.ProjectID != "project-a" || principal.Kind != PrincipalOperator {
		t.Fatalf("principal = %#v", principal)
	}
	if string(record.CSRFHash) == csrf {
		t.Fatalf("CSRF secret was stored in plaintext")
	}

	if _, _, err := authenticator.AuthenticateOperatorSession(context.Background(), session, "project-b"); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("cross-project session err = %v, want forbidden", err)
	}

	guard := NewStateChangeGuard([]string{"https://threadmill.test"})
	req := httptest.NewRequest(http.MethodPost, "https://threadmill.test/v1/capacity-adjustments", nil)
	req.Header.Set("Origin", "https://threadmill.test")
	req.Header.Set(CSRFHeaderName, csrf)
	if err := guard.Check(req, record); err != nil {
		t.Fatalf("CSRF guard error = %v", err)
	}

	badOrigin := httptest.NewRequest(http.MethodPost, "https://threadmill.test/v1/capacity-adjustments", nil)
	badOrigin.Header.Set("Origin", "https://evil.test")
	badOrigin.Header.Set(CSRFHeaderName, csrf)
	if err := guard.Check(badOrigin, record); !kernel.IsCode(err, kernel.CodeOriginInvalid) {
		t.Fatalf("bad origin err = %v, want origin_invalid", err)
	}

	missingCSRF := httptest.NewRequest(http.MethodPost, "https://threadmill.test/v1/capacity-adjustments", nil)
	missingCSRF.Header.Set("Origin", "https://threadmill.test")
	if err := guard.Check(missingCSRF, record); !kernel.IsCode(err, kernel.CodeCSRFInvalid) {
		t.Fatalf("missing CSRF err = %v, want csrf_invalid", err)
	}
}

func TestAgentTokenHashExpiryAndRevocation(t *testing.T) {
	now := time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)
	store := NewMemoryStore()
	authenticator := NewAuthenticator(store, func() time.Time { return now })
	capability := Capability{
		ProjectID:    "project-a",
		TaskID:       "task-1",
		InvocationID: "inv-1",
		Role:         RoleExecutor,
		Tools:        ToolSet(ToolContextListSubgraphs, ToolAgentSubmitPhaseOutput),
		ExpiresAt:    now.Add(time.Minute),
	}
	token, err := authenticator.IssueAgentToken(context.Background(), "agent-1", capability)
	if err != nil {
		t.Fatalf("IssueAgentToken() error = %v", err)
	}
	if _, ok, err := store.TokenByHash(context.Background(), []byte(token)); err != nil || ok {
		t.Fatalf("store accepted raw token lookup ok=%v err=%v; token must be stored only by hash", ok, err)
	}
	if _, ok, err := store.TokenByHash(context.Background(), HashOpaqueSecret(token)); err != nil || !ok {
		t.Fatalf("store hash lookup ok=%v err=%v", ok, err)
	}

	principal, err := authenticator.AuthenticateAgentToken(context.Background(), token)
	if err != nil {
		t.Fatalf("AuthenticateAgentToken() error = %v", err)
	}
	if principal.TaskID != "task-1" || principal.InvocationID != "inv-1" || principal.Role != RoleExecutor {
		t.Fatalf("principal = %#v", principal)
	}

	if err := authenticator.RevokeAgentToken(context.Background(), token); err != nil {
		t.Fatalf("RevokeAgentToken() error = %v", err)
	}
	if _, err := authenticator.AuthenticateAgentToken(context.Background(), token); !kernel.IsCode(err, kernel.CodeUnauthorized) {
		t.Fatalf("revoked token err = %v, want unauthorized", err)
	}

	expired := NewAuthenticator(NewMemoryStore(), func() time.Time { return now })
	expiringCapability := capability
	expiringCapability.InvocationID = "inv-expiring"
	expiringCapability.ExpiresAt = now.Add(time.Nanosecond)
	expiringToken, err := expired.IssueAgentToken(context.Background(), "agent-1", expiringCapability)
	if err != nil {
		t.Fatalf("IssueAgentToken(expiring) error = %v", err)
	}
	later := NewAuthenticator(expired.store, func() time.Time { return now.Add(time.Second) })
	if _, err := later.AuthenticateAgentToken(context.Background(), expiringToken); !kernel.IsCode(err, kernel.CodeUnauthorized) {
		t.Fatalf("expired token err = %v, want unauthorized", err)
	}
}

func TestCapabilityCannotEscalateOrOverrideAuthBoundScope(t *testing.T) {
	now := time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)
	authenticator := NewAuthenticator(NewMemoryStore(), func() time.Time { return now })
	token, err := authenticator.IssueAgentToken(context.Background(), "agent-1", Capability{
		ProjectID:    "project-a",
		TaskID:       "task-1",
		InvocationID: "inv-1",
		Role:         RoleExecutor,
		Tools:        ToolSet(ToolContextListSubgraphs, ToolAgentSubmitPhaseOutput),
		ExpiresAt:    now.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("IssueAgentToken() error = %v", err)
	}
	principal, err := authenticator.AuthenticateAgentToken(context.Background(), token)
	if err != nil {
		t.Fatalf("AuthenticateAgentToken() error = %v", err)
	}

	if _, err := RequireTool(principal, ToolCoordinationReplacePending, Scope{ProjectID: "project-a"}); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("phase coordination write err = %v, want forbidden", err)
	}
	if _, err := RequireTool(principal, ToolContextListSubgraphs, Scope{ProjectID: "project-a", TaskID: "other-task"}); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("task override err = %v, want forbidden", err)
	}
	if _, err := RequireTool(principal, ToolContextListSubgraphs, Scope{ProjectID: "project-a", InvocationID: "other-inv"}); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("invocation override err = %v, want forbidden", err)
	}

	bound, err := RequireTool(principal, ToolContextListSubgraphs, Scope{ProjectID: "project-a"})
	if err != nil {
		t.Fatalf("RequireTool(context read) error = %v", err)
	}
	if bound.TaskID != "task-1" || bound.InvocationID != "inv-1" {
		t.Fatalf("bound scope = %#v, want auth-bound task/invocation", bound)
	}
}

func TestRejectsPhaseCapabilityWithCoordinationWriteTools(t *testing.T) {
	now := time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)
	authenticator := NewAuthenticator(NewMemoryStore(), func() time.Time { return now })
	for _, tool := range []Tool{ToolCoordinationReplacePending, ToolCoordinationTransition} {
		_, err := authenticator.IssueAgentToken(context.Background(), "agent-1", Capability{
			ProjectID:    "project-a",
			TaskID:       "task-1",
			InvocationID: "inv-1",
			Role:         RolePlanner,
			Tools:        ToolSet(tool),
			ExpiresAt:    now.Add(time.Minute),
		})
		if !kernel.IsCode(err, kernel.CodeForbidden) {
			t.Fatalf("phase capability with %s err = %v, want forbidden", tool, err)
		}
	}
}

func TestOperatorCannotUseMCPTools(t *testing.T) {
	principal := Principal{
		ActorPrincipalID: "operator-1",
		Kind:             PrincipalOperator,
		ProjectID:        "project-a",
		Role:             RoleOperator,
	}
	if _, err := RequireTool(principal, ToolContextListSubgraphs, Scope{ProjectID: "project-a"}); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("operator MCP tool err = %v, want forbidden", err)
	}
}

func TestIssueRequestsFailClosed(t *testing.T) {
	now := time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)
	authenticator := NewAuthenticator(NewMemoryStore(), func() time.Time { return now })
	if _, _, err := authenticator.IssueOperatorSession(context.Background(), "", []kernel.ProjectID{"project-a"}, time.Hour); !kernel.IsCode(err, kernel.CodeInvalidRequest) {
		t.Fatalf("empty actor session err = %v, want invalid_request", err)
	}
	if _, _, err := authenticator.IssueOperatorSession(context.Background(), "operator-1", nil, time.Hour); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("empty project session err = %v, want forbidden", err)
	}
	if _, _, err := authenticator.IssueOperatorSession(context.Background(), "operator-1", []kernel.ProjectID{"project-a"}, 0); !kernel.IsCode(err, kernel.CodeInvalidRequest) {
		t.Fatalf("non-positive ttl session err = %v, want invalid_request", err)
	}
	if _, err := authenticator.IssueAgentToken(context.Background(), "", Capability{
		ProjectID:    "project-a",
		TaskID:       "task-1",
		InvocationID: "inv-1",
		Role:         RoleExecutor,
		Tools:        ToolSet(ToolContextListSubgraphs),
		ExpiresAt:    now.Add(time.Minute),
	}); !kernel.IsCode(err, kernel.CodeInvalidRequest) {
		t.Fatalf("empty actor token err = %v, want invalid_request", err)
	}
	if _, err := authenticator.IssueAgentToken(context.Background(), "agent-1", Capability{
		ProjectID:    "project-a",
		InvocationID: "inv-1",
		Role:         RoleExecutor,
		Tools:        ToolSet(ToolContextListSubgraphs),
		ExpiresAt:    now.Add(time.Minute),
	}); !kernel.IsCode(err, kernel.CodeStaleBinding) {
		t.Fatalf("phase without task err = %v, want stale_binding", err)
	}
	if _, err := authenticator.IssueAgentToken(context.Background(), "agent-1", Capability{
		ProjectID:    "project-a",
		TaskID:       "task-1",
		InvocationID: "inv-1",
		Role:         RoleExecutor,
		ExpiresAt:    now.Add(time.Minute),
	}); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("empty tool err = %v, want forbidden", err)
	}
	if _, err := authenticator.IssueAgentToken(context.Background(), "agent-1", Capability{
		ProjectID:    "project-a",
		TaskID:       "task-1",
		InvocationID: "inv-1",
		Role:         RoleExecutor,
		Tools:        ToolSet(ToolContextSearch),
		ExpiresAt:    now.Add(time.Minute),
	}); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("role tool mismatch err = %v, want forbidden", err)
	}
}

func TestSearchToolOnlyContextAgent(t *testing.T) {
	now := time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)
	authenticator := NewAuthenticator(NewMemoryStore(), func() time.Time { return now })
	if _, err := authenticator.IssueAgentToken(context.Background(), "phase-1", Capability{
		ProjectID:    "project-a",
		TaskID:       "task-1",
		InvocationID: "inv-phase",
		Role:         RoleExecutor,
		Tools:        ToolSet(ToolContextSearch),
		ExpiresAt:    now.Add(time.Minute),
	}); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("phase search err = %v, want forbidden", err)
	}
	if _, err := authenticator.IssueAgentToken(context.Background(), "tm-1", Capability{
		ProjectID:    "project-a",
		InvocationID: "inv-tm",
		Role:         RoleTaskManager,
		Tools:        ToolSet(ToolContextSearch),
		ExpiresAt:    now.Add(time.Minute),
	}); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("task manager search err = %v, want forbidden", err)
	}
	if _, err := authenticator.IssueAgentToken(context.Background(), "ctx-1", Capability{
		ProjectID:    "project-a",
		InvocationID: "inv-ctx",
		Role:         RoleContext,
		Operation:    "retrieve",
		Tools:        ToolSet(ToolContextSearch),
		ExpiresAt:    now.Add(time.Minute),
	}); err != nil {
		t.Fatalf("context search token err = %v", err)
	}
}

func TestContextCapabilityIsBoundToOneOperation(t *testing.T) {
	now := time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)
	authenticator := NewAuthenticator(NewMemoryStore(), func() time.Time { return now })
	tests := []struct {
		name      string
		operation string
		allowed   Tool
		forbidden Tool
	}{
		{name: "retrieve", operation: "retrieve", allowed: ToolContextSearch, forbidden: ToolContextCreateNode},
		{name: "curate", operation: "curate", allowed: ToolContextCreateNode, forbidden: ToolContextSearch},
		{name: "review", operation: "review", allowed: ToolContextSubmitReview, forbidden: ToolContextUpdateNode},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			token, err := authenticator.IssueAgentToken(context.Background(), "ctx-1", Capability{
				ProjectID:    "project-a",
				InvocationID: kernel.InvocationID("inv-ctx-" + test.name),
				Role:         RoleContext,
				Operation:    test.operation,
				Tools:        ToolSet(test.allowed),
				ExpiresAt:    now.Add(time.Minute),
			})
			if err != nil {
				t.Fatalf("issue %s token: %v", test.name, err)
			}
			principal, err := authenticator.AuthenticateAgentToken(context.Background(), token)
			if err != nil {
				t.Fatal(err)
			}
			if principal.Operation != test.operation {
				t.Fatalf("principal operation = %q, want %q", principal.Operation, test.operation)
			}
			if _, err := RequireTool(principal, test.allowed, Scope{}); err != nil {
				t.Fatalf("allowed %s tool failed: %v", test.name, err)
			}

			forged := principal
			forged.Tools = ToolSet(test.forbidden)
			if _, err := RequireTool(forged, test.forbidden, Scope{}); !kernel.IsCode(err, kernel.CodeForbidden) {
				t.Fatalf("%s operation used %s: %v, want forbidden", test.name, test.forbidden, err)
			}
		})
	}

	if _, err := authenticator.IssueAgentToken(context.Background(), "ctx-1", Capability{
		ProjectID:    "project-a",
		InvocationID: "inv-ctx-cross-operation",
		Role:         RoleContext,
		Operation:    "retrieve",
		Tools:        ToolSet(ToolContextSearch, ToolContextCreateNode),
		ExpiresAt:    now.Add(time.Minute),
	}); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("cross-operation token = %v, want forbidden", err)
	}
}

func TestRequireToolRechecksRolePolicy(t *testing.T) {
	principal := Principal{
		ActorPrincipalID: "agent-1",
		Kind:             PrincipalAgent,
		ProjectID:        "project-a",
		TaskID:           "task-a",
		InvocationID:     "invocation-a",
		Role:             RoleExecutor,
		Tools:            ToolSet(ToolContextSearch),
	}
	if _, err := RequireTool(principal, ToolContextSearch, Scope{ProjectID: "project-a"}); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("forged role/tool principal = %v, want forbidden", err)
	}
}

func TestIssueDoesNotReturnSecretsWhenStoreFails(t *testing.T) {
	now := time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)
	authenticator := NewAuthenticator(failingStore{err: errors.New("boom")}, func() time.Time { return now })
	session, csrf, err := authenticator.IssueOperatorSession(context.Background(), "operator-1", []kernel.ProjectID{"project-a"}, time.Hour)
	if err == nil {
		t.Fatalf("IssueOperatorSession() err = nil, want failure")
	}
	if session != "" || csrf != "" {
		t.Fatalf("IssueOperatorSession() returned secrets on store failure: session=%q csrf=%q", session, csrf)
	}
	token, err := authenticator.IssueAgentToken(context.Background(), "agent-1", Capability{
		ProjectID:    "project-a",
		TaskID:       "task-1",
		InvocationID: "inv-1",
		Role:         RoleExecutor,
		Tools:        ToolSet(ToolContextListSubgraphs),
		ExpiresAt:    now.Add(time.Minute),
	})
	if err == nil {
		t.Fatalf("IssueAgentToken() err = nil, want failure")
	}
	if token != "" {
		t.Fatalf("IssueAgentToken() returned secret on store failure: %q", token)
	}
}

func TestSessionCookieIsAlwaysSecure(t *testing.T) {
	cookie := SessionCookie("session-secret", time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC))
	if !cookie.Secure {
		t.Fatalf("SessionCookie().Secure = false, want true")
	}
	if !cookie.HttpOnly || cookie.SameSite != http.SameSiteStrictMode {
		t.Fatalf("SessionCookie() security flags = HttpOnly:%v SameSite:%v", cookie.HttpOnly, cookie.SameSite)
	}
}

func TestOriginFromURLStrictOriginOnly(t *testing.T) {
	origin, err := OriginFromURL("https://threadmill.test:8443")
	if err != nil {
		t.Fatalf("OriginFromURL(valid) error = %v", err)
	}
	if origin != "https://threadmill.test:8443" {
		t.Fatalf("origin = %q", origin)
	}
	for _, raw := range []string{
		"threadmill.test",
		"ftp://threadmill.test",
		"https://threadmill.test/path",
		"https://threadmill.test?x=1",
		"https://threadmill.test#frag",
		"https://user@threadmill.test",
	} {
		if _, err := OriginFromURL(raw); !kernel.IsCode(err, kernel.CodeInvalidRequest) {
			t.Fatalf("OriginFromURL(%q) err = %v, want invalid_request", raw, err)
		}
	}
}

type failingStore struct {
	err error
}

func (s failingStore) PutSession(context.Context, SessionRecord) error {
	return s.err
}

func (s failingStore) SessionByHash(context.Context, []byte) (SessionRecord, bool, error) {
	return SessionRecord{}, false, s.err
}

func (s failingStore) RevokeSession(context.Context, []byte, time.Time) error {
	return s.err
}

func (s failingStore) PutToken(context.Context, TokenRecord) error {
	return s.err
}

func (s failingStore) TokenByHash(context.Context, []byte) (TokenRecord, bool, error) {
	return TokenRecord{}, false, s.err
}

func (s failingStore) RevokeToken(context.Context, []byte, time.Time) error {
	return s.err
}
