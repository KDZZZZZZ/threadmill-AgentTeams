package mcpapi

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/taskmanager"
)

type coordinationSnapshotRequest struct {
	Revision kernel.Revision `json:"revision,omitempty"`
}

// PendingSubgraphIntent is the Agent-visible portion of PendingSubgraph.
// Runtime injects BaseRevision and RequestID from the current persisted
// TaskManagerDecision; neither value can be supplied in MCP JSON.
type PendingSubgraphIntent struct {
	Tasks     []coordination.Task          `json:"tasks,omitempty"`
	Endpoints []coordination.PhaseEndpoint `json:"endpoints"`
	Edges     []coordination.Edge          `json:"edges"`
	Blockers  []coordination.Blocker       `json:"blockers"`
}

// TaskManagerAgentRuntime is an internal transport port, not a second graph
// authority. Its implementation resolves the invocation-bound
// coordination.TaskManagerGraph, persists decisions, injects DecisionRef and
// expected revision, and delegates exactly one mutation to that graph seam.
type TaskManagerAgentRuntime interface {
	Snapshot(context.Context, auth.Principal, auth.BoundScope, kernel.Revision) (coordination.GraphSnapshot, error)
	SubmitTaskManagerDecision(context.Context, auth.Principal, auth.BoundScope, taskmanager.TaskManagerDecision) (string, error)
	ReplacePending(context.Context, auth.Principal, auth.BoundScope, PendingSubgraphIntent) (kernel.Revision, error)
	Transition(context.Context, auth.Principal, auth.BoundScope) (kernel.Revision, error)
}

func TaskManagerCoordinationToolSpecs(runtime TaskManagerAgentRuntime) []ToolSpec {
	return []ToolSpec{
		{ID: auth.ToolCoordinationSnapshot, Handler: HandlerFunc(func(ctx context.Context, principal auth.Principal, scope auth.BoundScope, payload json.RawMessage) (any, error) {
			var req coordinationSnapshotRequest
			if err := decodePayload(payload, &req); err != nil {
				return nil, err
			}
			return runtime.Snapshot(ctx, principal, scope, req.Revision)
		})},
		{ID: auth.ToolTaskManagerSubmitDecision, Handler: HandlerFunc(func(ctx context.Context, principal auth.Principal, scope auth.BoundScope, payload json.RawMessage) (any, error) {
			var req taskmanager.TaskManagerDecision
			if err := decodePayload(payload, &req); err != nil {
				return nil, err
			}
			if err := validateTaskManagerDecision(req); err != nil {
				return nil, err
			}
			return runtime.SubmitTaskManagerDecision(ctx, principal, scope, req)
		})},
		{ID: auth.ToolCoordinationReplacePending, Handler: HandlerFunc(func(ctx context.Context, principal auth.Principal, scope auth.BoundScope, payload json.RawMessage) (any, error) {
			var req PendingSubgraphIntent
			if err := decodePayload(payload, &req); err != nil {
				return nil, err
			}
			if len(req.Endpoints) == 0 {
				return nil, kernel.InvalidArgument("endpoints are required")
			}
			return runtime.ReplacePending(ctx, principal, scope, req)
		})},
		{ID: auth.ToolCoordinationTransition, Handler: HandlerFunc(func(ctx context.Context, principal auth.Principal, scope auth.BoundScope, payload json.RawMessage) (any, error) {
			// Transition carries no Agent-authored mutation body. The persisted
			// decision and trusted boundary input fully determine the transition.
			var req struct{}
			if err := decodePayload(payload, &req); err != nil {
				return nil, err
			}
			return runtime.Transition(ctx, principal, scope)
		})},
	}
}

func validateTaskManagerDecision(decision taskmanager.TaskManagerDecision) error {
	action := strings.TrimSpace(decision.Action)
	if action == "" {
		return kernel.InvalidArgument("action is required")
	}
	if strings.TrimSpace(decision.Reason) == "" {
		return kernel.InvalidArgument("reason is required")
	}
	terminal := action == "replace_pending" || action == "reject" || action == "defer" || action == "no_change"
	if terminal {
		if strings.TrimSpace(decision.TargetRef) != "" {
			return kernel.InvalidArgument("target_ref must be omitted for replace_pending, reject, defer, and no_change")
		}
		return nil
	}
	switch action {
	case "submitted", "satisfied", "rejected", "reopened", "held", "released", "stopped",
		"resolved", "denied", "obsolete", "done", "canceled", "failed":
	default:
		return kernel.InvalidArgument("action is not a Coordination Graph transition")
	}
	if strings.TrimSpace(decision.TargetRef) == "" {
		return kernel.InvalidArgument("target_ref is required for a transition")
	}
	return nil
}
