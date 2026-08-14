package mcpapi

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
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

func (f *fakeTaskManagerAgentRuntime) Snapshot(_ context.Context, principal auth.Principal, scope auth.BoundScope, revision kernel.Revision) (TaskManagerSnapshot, error) {
	f.principal, f.scope, f.snapshotRevision = principal, scope, revision
	return TaskManagerSnapshot{GraphSnapshot: coordination.GraphSnapshot{Revision: 7}}, nil
}

func (f *fakeTaskManagerAgentRuntime) SubmitTaskManagerDecision(_ context.Context, principal auth.Principal, scope auth.BoundScope, decision taskmanager.TaskManagerDecision) (string, error) {
	f.principal, f.scope, f.decision = principal, scope, decision
	f.decisionCalls++
	return "decision-1", nil
}

func TestTaskManagerSnapshotKeepsGraphContractAndAddsRuntimeDeliveryFacts(t *testing.T) {
	snapshot := TaskManagerSnapshot{
		GraphSnapshot: coordination.GraphSnapshot{Revision: 7},
		Deliveries: []TaskManagerDeliveryState{{
			TaskID: "task-a", DeliveryPolicy: taskmanager.DeliveryPolicyCodeMerge,
			LatestCandidateID: "candidate-a", LatestCandidateStatus: "failed",
			LatestDeliveryStatus: "failed", LatestFailureReason: "verify_failed",
			LatestFailureEvidenceRef: "artifact-failure", LatestReplanProposalRef: "proposal-a",
			ReopenRoundAvailable: true, ReadyForVerify: false,
		}},
	}
	payload, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["revision"] != float64(7) {
		t.Fatalf("flattened revision = %#v, want 7", decoded["revision"])
	}
	deliveries, ok := decoded["deliveries"].([]any)
	if !ok || len(deliveries) != 1 {
		t.Fatalf("deliveries = %#v, want one Runtime projection", decoded["deliveries"])
	}
	delivery := deliveries[0].(map[string]any)
	if delivery["latest_candidate_status"] != "failed" || delivery["latest_replan_proposal_ref"] != "proposal-a" || delivery["reopen_round_available"] != true || delivery["ready_for_verify"] != false {
		t.Fatalf("delivery = %#v", delivery)
	}
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
	intent := PendingSubgraphIntent{Endpoints: []PendingEndpointIntent{{
		Ref:       coordination.PhaseEndpointRef{TaskID: "task-a", EndpointID: coordination.EndpointVerify},
		RunPolicy: coordination.RunEnabled,
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

func TestTaskManagerCoordinationToolsAcceptTrustedReopenRoundIntent(t *testing.T) {
	runtime := &fakeTaskManagerAgentRuntime{}
	registry, err := NewRegistry(TaskManagerCoordinationToolSpecs(runtime)...)
	if err != nil {
		t.Fatal(err)
	}
	principal := principalWithTools(auth.RoleTaskManager, auth.ToolTaskManagerSubmitDecision)
	decision := taskmanager.TaskManagerDecision{
		Action:    "reopen_round",
		TargetRef: "task-a",
		Reason:    "targeted verifier reported that conflict resolution cannot preserve the Task Contract",
	}
	if _, err := registry.Invoke(context.Background(), principal, auth.ToolTaskManagerSubmitDecision, auth.Scope{ProjectID: "project-a"}, mustJSON(t, decision)); err != nil {
		t.Fatalf("submit reopen_round through MCP tool: %v", err)
	}
	if runtime.decisionCalls != 1 || !reflect.DeepEqual(runtime.decision, decision) {
		t.Fatalf("runtime decision=%#v calls=%d", runtime.decision, runtime.decisionCalls)
	}
}

func TestReplacePendingSchemaExposesOnlySemanticIntentAndNestedRequirements(t *testing.T) {
	schema := definitionForTool(auth.ToolCoordinationReplacePending).InputSchema
	properties := schema["properties"].(map[string]any)
	if _, advertised := properties["tasks"]; advertised {
		t.Fatal("replacePending schema must derive tasks instead of advertising Agent-authored contracts")
	}
	taskPolicy := properties["task_policies"].(map[string]any)["items"].(map[string]any)
	if got := taskPolicy["required"]; !reflect.DeepEqual(got, []string{"task_id", "delivery_policy"}) {
		t.Fatalf("task policy required=%#v", got)
	}
	wantPolicies := []string{"non_code_artifact", "code_merge", "human_acceptance", "external_delivery"}
	if got := taskPolicy["properties"].(map[string]any)["delivery_policy"].(map[string]any)["enum"]; !reflect.DeepEqual(got, wantPolicies) {
		t.Fatalf("delivery policy enum=%#v", got)
	}
	endpoint := properties["endpoints"].(map[string]any)["items"].(map[string]any)
	endpointProperties := endpoint["properties"].(map[string]any)
	for _, forbidden := range []string{"spec_ref", "binding_ref", "generation", "state"} {
		if _, advertised := endpointProperties[forbidden]; advertised {
			t.Fatalf("replacePending endpoint schema advertises Runtime field %q", forbidden)
		}
	}
	if got := endpoint["required"]; !reflect.DeepEqual(got, []string{"ref", "run_policy"}) {
		t.Fatalf("endpoint required=%#v", got)
	}
	blocker := properties["blockers"].(map[string]any)["items"].(map[string]any)
	if _, advertised := blocker["properties"].(map[string]any)["state"]; advertised {
		t.Fatal("replacePending blocker schema advertises Runtime-owned state")
	}
}

func TestTaskManagerDecisionSchemaExplainsAuthoritativeLifecycleTarget(t *testing.T) {
	definition := definitionForTool(auth.ToolTaskManagerSubmitDecision)
	properties := definition.InputSchema["properties"].(map[string]any)
	wantActions := []string{
		"replace_pending", "reject", "defer", "no_change",
		"submitted", "satisfied", "rejected", "reopened", "reopen_round", "held", "released", "stopped",
		"resolved", "denied", "obsolete", "done", "canceled", "failed",
	}
	if got := properties["action"].(map[string]any)["enum"]; !reflect.DeepEqual(got, wantActions) {
		t.Fatalf("action enum=%#v", got)
	}
	targetDescription, _ := properties["target_ref"].(map[string]any)["description"].(string)
	for _, fragment := range []string{
		`selected_endpoint.task_id + "/" + selected_endpoint.endpoint_id`,
		"task-a/execute",
		"task_completion=done",
		"trusted targeted-verify reopen_round",
		"Never use an artifact ref",
	} {
		if !strings.Contains(targetDescription, fragment) {
			t.Fatalf("target_ref description %q does not contain %q", targetDescription, fragment)
		}
	}
	if !strings.Contains(definition.Description, "Runtime-selected task or task/endpoint target exactly") {
		t.Fatalf("tool description=%q", definition.Description)
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
		{Action: "reopen_round", TargetRef: "task-a", Reason: "trusted targeted verifier requested a fresh execution round"},
	}
	for _, decision := range valid {
		if err := validateTaskManagerDecision(decision); err != nil {
			t.Errorf("valid decision %#v: %v", decision, err)
		}
	}
	invalid := []taskmanager.TaskManagerDecision{
		{Action: "replace_pending", TargetRef: "task-a", Reason: "target must be omitted"},
		{Action: "held", Reason: "missing target"},
		{Action: "reopen_round", Reason: "missing trusted task target"},
		{Action: "arbitrary_patch", TargetRef: "task-a", Reason: "not a transition"},
	}
	for _, decision := range invalid {
		if err := validateTaskManagerDecision(decision); !kernel.IsCode(err, kernel.CodeInvalidRequest) {
			t.Errorf("invalid decision %#v err=%v", decision, err)
		}
	}
}
