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

// PendingEndpointIntent contains only scheduling intent. Runtime derives the
// Task set from endpoint refs and injects contract/spec/binding/generation and
// state authority from the invocation-bound decision.
type PendingEndpointIntent struct {
	Ref       coordination.PhaseEndpointRef `json:"ref"`
	RunPolicy coordination.RunPolicy        `json:"run_policy"`
}

// PendingBlockerIntent excludes blocker state because new graph-control gates
// always enter as active; later state changes require a persisted transition.
type PendingBlockerIntent struct {
	ID         string                        `json:"id"`
	Target     coordination.PhaseEndpointRef `json:"target"`
	RequiredBy coordination.RequiredBy       `json:"required_by"`
	OnFalse    coordination.OnFalse          `json:"on_false"`
}

// PendingTaskPolicyIntent carries the one TaskContract choice that cannot be
// derived from endpoint refs. Runtime still injects the authoritative Task,
// contract ref, PhaseSpecs, binding, generation, and state.
type PendingTaskPolicyIntent struct {
	TaskID         kernel.TaskID              `json:"task_id"`
	DeliveryPolicy taskmanager.DeliveryPolicy `json:"delivery_policy"`
}

// PendingSubgraphIntent is the minimal Agent-visible graph intent. Runtime
// injects RequestID, BaseRevision, Tasks, endpoint authority fields, and
// blocker state from the current persisted TaskManagerDecision.
type PendingSubgraphIntent struct {
	TaskPolicies []PendingTaskPolicyIntent `json:"task_policies,omitempty"`
	Endpoints    []PendingEndpointIntent   `json:"endpoints"`
	Edges        []coordination.Edge       `json:"edges,omitempty"`
	Blockers     []PendingBlockerIntent    `json:"blockers,omitempty"`
}

// TaskManagerDeliveryState is the minimal Runtime-owned delivery view needed
// to distinguish graph readiness from code-merge readiness. Agents cannot
// author these fields; Snapshot derives them from persisted contracts, merge
// candidates, and production deliveries.
type TaskManagerDeliveryState struct {
	TaskID                   kernel.TaskID              `json:"task_id"`
	DeliveryPolicy           taskmanager.DeliveryPolicy `json:"delivery_policy"`
	LatestCandidateID        string                     `json:"latest_candidate_id,omitempty"`
	LatestCandidateStatus    string                     `json:"latest_candidate_status,omitempty"`
	LatestDeliveryStatus     string                     `json:"latest_delivery_status,omitempty"`
	LatestFailureReason      string                     `json:"latest_failure_reason,omitempty"`
	LatestFailureEvidenceRef string                     `json:"latest_failure_evidence_ref,omitempty"`
	LatestReplanProposalRef  string                     `json:"latest_replan_proposal_ref,omitempty"`
	ReopenRoundAvailable     bool                       `json:"reopen_round_available"`
	ReadyForVerify           bool                       `json:"ready_for_verify"`
}

// TaskManagerSnapshot keeps the Coordination Graph as the authoritative graph
// object while attaching a read-only delivery projection for Manager
// decisions. Embedding preserves the existing top-level graph JSON contract.
type TaskManagerSnapshot struct {
	coordination.GraphSnapshot
	Deliveries []TaskManagerDeliveryState `json:"deliveries"`
}

// TaskManagerAgentRuntime is an internal transport port, not a second graph
// authority. Its implementation resolves the invocation-bound
// coordination.TaskManagerGraph, persists decisions, injects DecisionRef and
// expected revision, and delegates exactly one mutation to that graph seam.
type TaskManagerAgentRuntime interface {
	Snapshot(context.Context, auth.Principal, auth.BoundScope, kernel.Revision) (TaskManagerSnapshot, error)
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
	case "submitted", "satisfied", "rejected", "reopened", "reopen_round", "held", "released", "stopped",
		"resolved", "denied", "obsolete", "done", "canceled", "failed":
	default:
		return kernel.InvalidArgument("action is not a Coordination Graph transition")
	}
	if strings.TrimSpace(decision.TargetRef) == "" {
		return kernel.InvalidArgument("target_ref is required for a transition")
	}
	return nil
}
