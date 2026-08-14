package mcpapi

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/workspace"
)

func WorkspaceToolSpecs(port workspace.AgentToolPort) []ToolSpec {
	return []ToolSpec{
		{ID: auth.ToolWorkspaceList, Handler: HandlerFunc(func(ctx context.Context, _ auth.Principal, scope auth.BoundScope, payload json.RawMessage) (any, error) {
			var req workspace.PathRequest
			if err := decodePayload(payload, &req); err != nil {
				return nil, err
			}
			return port.List(ctx, scope.InvocationID, req)
		})},
		{ID: auth.ToolWorkspaceRead, Handler: HandlerFunc(func(ctx context.Context, _ auth.Principal, scope auth.BoundScope, payload json.RawMessage) (any, error) {
			var req workspace.PathRequest
			if err := decodePayload(payload, &req); err != nil {
				return nil, err
			}
			if err := requireWorkspacePath(req.Path); err != nil {
				return nil, err
			}
			return port.Read(ctx, scope.InvocationID, req)
		})},
		{ID: auth.ToolWorkspaceWritePlan, Handler: HandlerFunc(func(ctx context.Context, _ auth.Principal, scope auth.BoundScope, payload json.RawMessage) (any, error) {
			var req workspace.WriteRequest
			if err := decodePayload(payload, &req); err != nil {
				return nil, err
			}
			if err := requireWorkspacePath(req.Path); err != nil {
				return nil, err
			}
			return port.WritePlan(ctx, scope.InvocationID, req)
		})},
		{ID: auth.ToolWorkspaceWrite, Handler: HandlerFunc(func(ctx context.Context, _ auth.Principal, scope auth.BoundScope, payload json.RawMessage) (any, error) {
			var req workspace.WriteRequest
			if err := decodePayload(payload, &req); err != nil {
				return nil, err
			}
			if err := requireWorkspacePath(req.Path); err != nil {
				return nil, err
			}
			return port.Write(ctx, scope.InvocationID, req)
		})},
		{ID: auth.ToolWorkspaceRun, Handler: HandlerFunc(func(ctx context.Context, _ auth.Principal, scope auth.BoundScope, payload json.RawMessage) (any, error) {
			var req workspace.RunRequest
			if err := decodePayload(payload, &req); err != nil {
				return nil, err
			}
			if len(req.Command) == 0 || strings.TrimSpace(req.Command[0]) == "" {
				return nil, kernel.InvalidArgument("command is required")
			}
			return port.Run(ctx, scope.InvocationID, req)
		})},
		{ID: auth.ToolWorkspaceDiff, Handler: HandlerFunc(func(ctx context.Context, _ auth.Principal, scope auth.BoundScope, payload json.RawMessage) (any, error) {
			var req workspace.PathRequest
			if err := decodePayload(payload, &req); err != nil {
				return nil, err
			}
			return port.Diff(ctx, scope.InvocationID, req)
		})},
	}
}

func requireWorkspacePath(path string) error {
	if strings.TrimSpace(path) == "" {
		return kernel.InvalidArgument("path is required")
	}
	return nil
}
