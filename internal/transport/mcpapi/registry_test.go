package mcpapi

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

func TestRegistryUsesSameEntryForVisibilityAndDispatch(t *testing.T) {
	called := false
	registry, err := NewRegistry(ToolSpec{
		ID: auth.ToolContextExplore,
		Handler: HandlerFunc(func(_ context.Context, principal auth.Principal, scope auth.BoundScope, _ json.RawMessage) (any, error) {
			called = true
			if scope.TaskID != "task-a" || scope.InvocationID != "inv-a" {
				t.Fatalf("handler received unbound scope: %#v", scope)
			}
			if principal.InvocationID != scope.InvocationID || principal.Role != auth.RoleExecutor {
				t.Fatalf("handler did not receive authenticated principal: %#v", principal)
			}
			return "ok", nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := registry.AvailableTools()[auth.ToolContextExplore]; !ok {
		t.Fatal("registered tool is not visible")
	}
	principal := auth.Principal{
		ActorPrincipalID: "agent-a",
		Kind:             auth.PrincipalAgent,
		ProjectID:        "project-a",
		TaskID:           "task-a",
		InvocationID:     "inv-a",
		Role:             auth.RoleExecutor,
		Tools:            auth.ToolSet(auth.ToolContextExplore),
		AuthenticatedAt:  time.Now(),
	}
	result, err := registry.Invoke(context.Background(), principal, auth.ToolContextExplore, auth.Scope{ProjectID: "project-a"}, nil)
	if err != nil || result != "ok" || !called {
		t.Fatalf("invoke result=%v called=%v err=%v", result, called, err)
	}
}

func TestRegistryRejectsUnknownDuplicateMissingAndUnauthorizedTools(t *testing.T) {
	noop := HandlerFunc(func(context.Context, auth.Principal, auth.BoundScope, json.RawMessage) (any, error) { return nil, nil })
	if _, err := NewRegistry(ToolSpec{ID: "unknown.tool", Handler: noop}); err == nil {
		t.Fatal("unknown tool registration succeeded")
	}
	if _, err := NewRegistry(ToolSpec{ID: auth.ToolContextExplore}); err == nil {
		t.Fatal("nil handler registration succeeded")
	}
	if _, err := NewRegistry(
		ToolSpec{ID: auth.ToolContextExplore, Handler: noop},
		ToolSpec{ID: auth.ToolContextExplore, Handler: noop},
	); err == nil {
		t.Fatal("duplicate tool registration succeeded")
	}

	registry, err := NewRegistry(ToolSpec{ID: auth.ToolContextSearch, Handler: noop})
	if err != nil {
		t.Fatal(err)
	}
	principal := auth.Principal{
		ActorPrincipalID: "agent-a",
		Kind:             auth.PrincipalAgent,
		ProjectID:        "project-a",
		TaskID:           "task-a",
		InvocationID:     "inv-a",
		Role:             auth.RoleExecutor,
		Tools:            auth.ToolSet(auth.ToolContextSearch),
	}
	if _, err := registry.Invoke(context.Background(), principal, auth.ToolContextSearch, auth.Scope{}, nil); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("executor search invocation = %v, want forbidden", err)
	}
}
