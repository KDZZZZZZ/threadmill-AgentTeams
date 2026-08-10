package mcpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

type Handler interface {
	Call(context.Context, auth.BoundScope, json.RawMessage) (any, error)
}

type HandlerFunc func(context.Context, auth.BoundScope, json.RawMessage) (any, error)

func (f HandlerFunc) Call(ctx context.Context, scope auth.BoundScope, request json.RawMessage) (any, error) {
	return f(ctx, scope, request)
}

type ToolSpec struct {
	ID      auth.Tool
	Handler Handler
}

// Registry is the sole MCP tool dispatch table. Visibility and invocation use
// the same entries so an unregistered tool cannot be injected or called.
type Registry struct {
	mu    sync.RWMutex
	tools map[auth.Tool]Handler
}

func NewRegistry(specs ...ToolSpec) (*Registry, error) {
	registry := &Registry{tools: make(map[auth.Tool]Handler, len(specs))}
	for _, spec := range specs {
		if !auth.IsCanonicalTool(spec.ID) {
			return nil, fmt.Errorf("unknown MCP tool %s", spec.ID)
		}
		if spec.Handler == nil {
			return nil, fmt.Errorf("MCP tool %s requires a handler", spec.ID)
		}
		if _, exists := registry.tools[spec.ID]; exists {
			return nil, fmt.Errorf("duplicate MCP tool %s", spec.ID)
		}
		registry.tools[spec.ID] = spec.Handler
	}
	return registry, nil
}

func (r *Registry) AvailableTools() map[auth.Tool]struct{} {
	r.mu.RLock()
	defer r.mu.RUnlock()
	tools := make(map[auth.Tool]struct{}, len(r.tools))
	for tool := range r.tools {
		tools[tool] = struct{}{}
	}
	return tools
}

func (r *Registry) ToolIDs() []auth.Tool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	tools := make([]auth.Tool, 0, len(r.tools))
	for tool := range r.tools {
		tools = append(tools, tool)
	}
	sort.Slice(tools, func(i, j int) bool { return tools[i] < tools[j] })
	return tools
}

func (r *Registry) Invoke(
	ctx context.Context,
	principal auth.Principal,
	tool auth.Tool,
	requested auth.Scope,
	payload json.RawMessage,
) (any, error) {
	scope, err := auth.RequireTool(principal, tool, requested)
	if err != nil {
		return nil, err
	}
	r.mu.RLock()
	handler := r.tools[tool]
	r.mu.RUnlock()
	if handler == nil {
		return nil, kernel.Error{Code: kernel.CodeForbidden, Message: "tool is not registered in this runtime"}
	}
	return handler.Call(ctx, scope, payload)
}
