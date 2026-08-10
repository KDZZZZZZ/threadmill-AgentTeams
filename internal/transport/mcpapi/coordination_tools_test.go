package mcpapi

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/taskmanager"
)

type fakeTaskManagerAgentRuntime struct {
	principal        auth.Principal
	scope            auth.BoundScope
	snapshotRevision kernel.Revision
	decision         taskmanager.TaskManagerDecision
	replacement      PendingSubgraphIntent
	transitionCalls  int
	replacementCalls int
	decisionCalls    int
}

func (f *fakeTaskManagerAgentRuntime) Snapshot(_ context.Context, principal auth.Principal, scope auth.BoundScope, revision kernel.Revision) (coordination.GraphSnapshot, error) {
	f.principal, f.scope, f.snapshotRevision = principal, scope, revision
	return coordination.GraphSnapshot{Revision: 7}, nil
}

func (f *fakeTaskManagerAgentRuntime) SubmitTaskManagerDecision(_ context.Context, principal auth.Principal, scope auth.BoundScope, decision taskmanager.TaskManagerDecision) (string, error) {
	f.principal, f.scope, f.decision = principal, scope, decision
	f.decisionCalls++
	return "decision-1", nil
}

func (f *fakeTaskManagerAgentRuntime) ReplacePending(_ context.Context, principal auth.Principal, scope auth.BoundScope, replacement PendingSubgraphIntent) (kernel.Revision, error) {
	f.principal, f.scope, f.replacement = principal, scope, replacement
	f.replacementCalls++
	return 8, nil
}

func (f *fakeTaskManagerAgentRuntime) Transition(_ context.Context, principal auth.Principal, scope auth.BoundScope) (kernel.Revision, error) {
	f.principal, f.scope = principal, scope
	f.transitionCalls++
	return 9, nil
}

func TestTaskManagerCoordinationToolsForwardOnlyAgentIntent(t *testing.T) {
	runtime := &fakeTaskManagerAgentRuntime{}
	registry, err := NewRegistry(TaskManagerCoordinationToolSpecs(runtime)...)
	if err != nil {
		t.Fatal(err)
	}
	principal := principalWithTools(auth.RoleTaskManager,
		auth.ToolCoordinationSnapshot,
		auth.ToolTaskManagerSubmitDecision,
		auth.ToolCoordinationReplacePending,
		auth.ToolCoordinationTransition,
	)
	scope := auth.Scope{ProjectID: "project-a", TaskID: "task-a"}
	if _, err := registry.Invoke(context.Background(), principal, auth.ToolCoordinationSnapshot, scope, mustJSON(t, coordinationSnapshotRequest{Revision: 6})); err != nil {
		t.Fatal(err)
	}
	if runtime.snapshotRevision != 6 || runtime.scope.InvocationID != principal.InvocationID {
		t.Fatalf("snapshot revision=%d scope=%#v", runtime.snapshotRevision, runtime.scope)
	}
	decision := taskmanager.TaskManagerDecision{Action: "replace_pending", Reason: "replace future work"}
	if _, err := registry.Invoke(context.Background(), principal, auth.ToolTaskManagerSubmitDecision, scope, mustJSON(t, decision)); err != nil {
		t.Fatal(err)
	}
	if runtime.decision.Action != "replace_pending" || runtime.decisionCalls != 1 {
		t.Fatalf("decision=%#v calls=%d", runtime.decision, runtime.decisionCalls)
	}
	intent := PendingSubgraphIntent{Endpoints: []coordination.PhaseEndpoint{{
		Ref: coordination.PhaseEndpointRef{TaskID: "task-a", EndpointID: coordination.EndpointVerify},
	}}}
	if _, err := registry.Invoke(context.Background(), principal, auth.ToolCoordinationReplacePending, scope, mustJSON(t, intent)); err != nil {
		t.Fatal(err)
	}
	if runtime.replacementCalls != 1 || len(runtime.replacement.Endpoints) != 1 {
		t.Fatalf("replacement=%#v calls=%d", runtime.replacement, runtime.replacementCalls)
	}
	if _, err := registry.Invoke(context.Background(), principal, auth.ToolCoordinationTransition, scope, json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	if runtime.transitionCalls != 1 || runtime.principal.InvocationID != principal.InvocationID {
		t.Fatalf("transition calls=%d principal=%#v", runtime.transitionCalls, runtime.principal)
	}
}

func TestTaskManagerCoordinationToolsRejectSpoofedRuntimeFields(t *testing.T) {
	runtime := &fakeTaskManagerAgentRuntime{}
	registry, err := NewRegistry(TaskManagerCoordinationToolSpecs(runtime)...)
	if err != nil {
		t.Fatal(err)
	}
	principal := principalWithTools(auth.RoleTaskManager, auth.ToolCoordinationReplacePending, auth.ToolCoordinationTransition, auth.ToolTaskManagerSubmitDecision)
	tests := []struct {
		tool    auth.Tool
		payload json.RawMessage
	}{
		{auth.ToolCoordinationReplacePending, json.RawMessage(`{"request_id":"forged","base_revision":7,"endpoints":[{}],"edges":[],"blockers":[]}`)},
		{auth.ToolCoordinationTransition, json.RawMessage(`{"decision_ref":"forged","expected_revision":7}`)},
		{auth.ToolTaskManagerSubmitDecision, json.RawMessage(`{"action":"replace_pending","reason":"x","expected_revision":7}`)},
	}
	for _, test := range tests {
		if _, err := registry.Invoke(context.Background(), principal, test.tool, auth.Scope{ProjectID: "project-a", TaskID: "task-a"}, test.payload); !kernel.IsCode(err, kernel.CodeInvalidRequest) {
			t.Errorf("%s spoof payload err=%v, want invalid_request", test.tool, err)
		}
	}
	if runtime.replacementCalls != 0 || runtime.transitionCalls != 0 || runtime.decisionCalls != 0 {
		t.Fatalf("spoofed payload reached runtime: %#v", runtime)
	}
}

func TestTaskManagerDecisionShapeMatchesDocumentedContract(t *testing.T) {
	valid := []taskmanager.TaskManagerDecision{
		{Action: "reject", Reason: "evidence stale"},
		{Action: "held", TargetRef: "task-a/execute", Reason: "manager requested hold"},
	}
	for _, decision := range valid {
		if err := validateTaskManagerDecision(decision); err != nil {
			t.Errorf("valid decision %#v: %v", decision, err)
		}
	}
	invalid := []taskmanager.TaskManagerDecision{
		{Action: "replace_pending", TargetRef: "task-a", Reason: "target must be omitted"},
		{Action: "held", Reason: "missing target"},
		{Action: "arbitrary_patch", TargetRef: "task-a", Reason: "not a transition"},
	}
	for _, decision := range invalid {
		if err := validateTaskManagerDecision(decision); !kernel.IsCode(err, kernel.CodeInvalidRequest) {
			t.Errorf("invalid decision %#v err=%v", decision, err)
		}
	}
}
