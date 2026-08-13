package app

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/evidence"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	phasepkg "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime/phase"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/taskmanager"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/transport/httpapi"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/transport/mcpapi"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/uiprojection"
)

func TestTrustedDecisionMutationUsesRuntimeSelectedEndpoint(t *testing.T) {
	binding := productionTaskManagerBinding{InputKind: "manager", SelectedTaskID: "task-a", SelectedEndpoint: coordination.EndpointExecute}
	snapshot := coordination.GraphSnapshot{Revision: 7, Endpoints: []coordination.PhaseEndpoint{{Ref: coordination.PhaseEndpointRef{TaskID: "task-a", EndpointID: coordination.EndpointExecute}, Generation: 3}}}
	kind, transition, err := trustedDecisionMutation(binding, snapshot, taskmanager.TaskManagerDecision{Action: "held", TargetRef: "task-a/execute", Reason: "pause"})
	if err != nil {
		t.Fatal(err)
	}
	if kind != taskmanager.DecisionKindTransition || transition.Endpoint.TaskID != "task-a" || transition.Generation != 3 || transition.Action != "held" {
		t.Fatalf("trusted transition = kind %q transition %#v", kind, transition)
	}
}

func TestTrustedDecisionMutationRejectsAgentSelectedAuthority(t *testing.T) {
	binding := productionTaskManagerBinding{InputKind: "manager", SelectedTaskID: "task-a", SelectedEndpoint: coordination.EndpointExecute}
	snapshot := coordination.GraphSnapshot{Revision: 7, Endpoints: []coordination.PhaseEndpoint{{Ref: coordination.PhaseEndpointRef{TaskID: "task-a", EndpointID: coordination.EndpointExecute}, Generation: 3}}}
	_, _, err := trustedDecisionMutation(binding, snapshot, taskmanager.TaskManagerDecision{Action: "held", TargetRef: "task-b/execute", Reason: "pause"})
	if !kernel.IsCode(err, kernel.CodeInvalidRequest) {
		t.Fatalf("trustedDecisionMutation() error = %v, want invalid_request", err)
	}
}

func TestNormalizePendingFreezesContractAndRuntimeAuthority(t *testing.T) {
	payload, err := json.Marshal(httpapi.RequirementCreateRequest{RequestID: "req-1", ProjectID: "project-a", Body: "build the feature", Motivation: "ship safely", Constraints: []string{"keep tests green"}})
	if err != nil {
		t.Fatal(err)
	}
	binding := productionTaskManagerBinding{InputRef: "manager-input:1", InputKind: "requirement", InputBody: "build the feature", InputPayload: payload, SeenRevision: 1, DecisionRef: "tmdec:1"}
	intent := mcpapi.PendingSubgraphIntent{
		Tasks: []coordination.Task{{ID: "task-a", ContractRef: "contract-a", Outcome: coordination.TaskDone}},
		Endpoints: []coordination.PhaseEndpoint{
			forgedPendingEndpoint("task-a", coordination.EndpointPlan, "spec-plan"),
			forgedPendingEndpoint("task-a", coordination.EndpointExecute, "spec-execute"),
			forgedPendingEndpoint("task-a", coordination.EndpointVerify, "spec-verify"),
		},
	}
	runtime := &productionTaskManagerRuntime{projectID: "project-a"}
	plan, err := runtime.normalizePending(binding, coordination.GraphSnapshot{Revision: 1}, intent)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Subgraph.Tasks) != 1 || plan.Subgraph.Tasks[0].Outcome != coordination.TaskActive {
		t.Fatalf("normalized tasks = %#v", plan.Subgraph.Tasks)
	}
	if len(plan.Subgraph.Endpoints) != 3 {
		t.Fatalf("normalized endpoints = %#v", plan.Subgraph.Endpoints)
	}
	for _, endpoint := range plan.Subgraph.Endpoints {
		wantBinding := canonicalProductionBindingRef(endpoint.Ref, 1)
		if endpoint.Generation != 1 || endpoint.State != coordination.EndpointPending || endpoint.BindingRef != wantBinding {
			t.Fatalf("endpoint retained Agent authority: %#v, want binding %q", endpoint, wantBinding)
		}
	}
	if len(plan.Resources) != 1 || !plan.Resources[0].IsNew || plan.Resources[0].Input.InputRef == binding.InputRef {
		t.Fatalf("runtime resources = %#v", plan.Resources)
	}
	resource := plan.Resources[0]
	if resource.Contract.ContractRef != "contract-a" || resource.Contract.PhaseSpecs[coordination.EndpointPlan] != "spec-plan" || resource.Input.Requirement.Text != "build the feature" {
		t.Fatalf("frozen requirement contract = %#v %#v", resource.Input, resource.Contract)
	}
}

func TestNormalizePendingRequiresExactlyFixedEndpointSet(t *testing.T) {
	payload, _ := json.Marshal(httpapi.ManagerMessageRequest{RequestID: "req-1", ProjectID: "project-a", ConversationID: "conversation-a", Body: "replan"})
	binding := productionTaskManagerBinding{InputRef: "manager-input:1", InputKind: "manager", InputBody: "replan", InputPayload: payload, SeenRevision: 1, DecisionRef: "tmdec:1"}
	_, err := (&productionTaskManagerRuntime{}).normalizePending(binding, coordination.GraphSnapshot{Revision: 1}, mcpapi.PendingSubgraphIntent{
		Tasks: []coordination.Task{{ID: "task-a", ContractRef: "contract-a"}},
		Endpoints: []coordination.PhaseEndpoint{
			forgedPendingEndpoint("task-a", coordination.EndpointPlan, "spec-plan"),
			forgedPendingEndpoint("task-a", coordination.EndpointExecute, "spec-execute"),
		},
	})
	if !kernel.IsCode(err, kernel.CodeInvalidRequest) {
		t.Fatalf("normalizePending() error = %v, want invalid_request", err)
	}
}

func TestTrustedPhaseOutputUsesPersistedBoundaryAndSnapshotAuthority(t *testing.T) {
	ref := coordination.PhaseEndpointRef{TaskID: "task-a", EndpointID: coordination.EndpointExecute}
	endpoint := coordination.PhaseEndpoint{Ref: ref, SpecRef: "spec-execute", BindingRef: canonicalProductionBindingRef(ref, 2), Generation: 2, State: coordination.EndpointPending, RunPolicy: coordination.RunEnabled}
	boundary := productionPhaseOutputBoundary{OutputRef: "output-1", Receipt: phasepkg.OutputReceipt{
		Endpoint: ref, Generation: 2, BindingRef: endpoint.BindingRef,
		Output: phasepkg.PhaseOutput{ReportRef: "report-1", EvidenceRefs: []string{"trusted-evidence"}},
	}}
	payload, _ := json.Marshal(boundary)
	binding := productionTaskManagerBinding{InputKind: "phase_output", InputPayload: payload, SelectedTaskID: ref.TaskID, SelectedEndpoint: ref.EndpointID, TargetKind: "phase_output", TargetRef: "output-1"}
	_, transition, err := trustedDecisionMutation(binding, coordination.GraphSnapshot{Revision: 4, Endpoints: []coordination.PhaseEndpoint{endpoint}}, taskmanager.TaskManagerDecision{
		Action: "submitted", TargetRef: "task-a/execute", Reason: "accept output for evaluation", EvidenceRefs: []string{"agent-forged"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if transition.Generation != 2 || transition.Result.BindingRef != endpoint.BindingRef || transition.Result.OutputRef != "output-1" || transition.Result.Verdict != coordination.VerdictSubmitted {
		t.Fatalf("trusted submitted transition = %#v", transition)
	}
	for _, ref := range transition.EvidenceRefs {
		if ref == "agent-forged" {
			t.Fatalf("Agent-authored evidence leaked into transition: %#v", transition)
		}
	}
}

func TestTrustedPhaseEvaluationAndStopReleaseRequireSeparateBoundaries(t *testing.T) {
	ref := coordination.PhaseEndpointRef{TaskID: "task-a", EndpointID: coordination.EndpointVerify}
	endpoint := coordination.PhaseEndpoint{Ref: ref, SpecRef: "spec-verify", BindingRef: canonicalProductionBindingRef(ref, 1), Generation: 1, State: coordination.EndpointSubmitted, RunPolicy: coordination.RunEnabled}
	output := productionPhaseOutputBoundary{OutputRef: "output-verify", Receipt: phasepkg.OutputReceipt{Endpoint: ref, Generation: 1, BindingRef: endpoint.BindingRef}}
	evaluation := productionPhaseEvaluationBoundary{SourceInputRef: "source-1", Output: output, Endpoint: ref, Generation: 1, BindingRef: endpoint.BindingRef}
	payload, _ := json.Marshal(evaluation)
	binding := productionTaskManagerBinding{InputKind: "phase_orchestration", InputPayload: payload, SelectedTaskID: ref.TaskID, SelectedEndpoint: ref.EndpointID, TargetKind: "phase_evaluation", TargetRef: "output-verify"}
	_, transition, err := trustedDecisionMutation(binding, coordination.GraphSnapshot{Endpoints: []coordination.PhaseEndpoint{endpoint}}, taskmanager.TaskManagerDecision{Action: "satisfied", TargetRef: "task-a/verify", Reason: "verified"})
	if err != nil {
		t.Fatal(err)
	}
	if transition.Result.Verdict != coordination.VerdictSatisfied || transition.Result.OutputRef != "output-verify" {
		t.Fatalf("satisfied transition = %#v", transition)
	}
	binding.InputKind, binding.TargetKind = "phase_output", "phase_output"
	if _, _, err := trustedDecisionMutation(binding, coordination.GraphSnapshot{Endpoints: []coordination.PhaseEndpoint{endpoint}}, taskmanager.TaskManagerDecision{Action: "satisfied", TargetRef: "task-a/verify", Reason: "skip evaluation"}); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("direct satisfied error = %v, want forbidden", err)
	}
}

func TestTrustedStoppedGeneratesNextBindingAndReleasedRequiresFollowup(t *testing.T) {
	ref := coordination.PhaseEndpointRef{TaskID: "task-a", EndpointID: coordination.EndpointExecute}
	endpoint := coordination.PhaseEndpoint{Ref: ref, SpecRef: "spec", BindingRef: canonicalProductionBindingRef(ref, 1), Generation: 1, State: coordination.EndpointSubmitted, RunPolicy: coordination.RunHeld}
	stopped := productionPhaseStoppedBoundary{CommandID: "stop-1", Endpoint: ref, Generation: 1, BindingRef: endpoint.BindingRef, LeaseRef: "lease-1", CheckpointRef: "checkpoint-1"}
	payload, _ := json.Marshal(stopped)
	binding := productionTaskManagerBinding{InputKind: "phase_stopped", InputPayload: payload, SelectedTaskID: ref.TaskID, SelectedEndpoint: ref.EndpointID, TargetKind: "phase_stopped", TargetRef: "stop-1"}
	_, transition, err := trustedDecisionMutation(binding, coordination.GraphSnapshot{Endpoints: []coordination.PhaseEndpoint{endpoint}}, taskmanager.TaskManagerDecision{Action: "stopped", TargetRef: "task-a/execute", Reason: "stop acknowledged"})
	if err != nil {
		t.Fatal(err)
	}
	if transition.NewBindingRef != canonicalProductionBindingRef(ref, 2) || transition.Generation != 1 || transition.CheckpointRef != "checkpoint-1" {
		t.Fatalf("stopped transition = %#v", transition)
	}
	if _, _, err := trustedDecisionMutation(productionTaskManagerBinding{InputKind: "manager", SelectedTaskID: ref.TaskID, SelectedEndpoint: ref.EndpointID}, coordination.GraphSnapshot{Endpoints: []coordination.PhaseEndpoint{endpoint}}, taskmanager.TaskManagerDecision{Action: "released", TargetRef: "task-a/execute", Reason: "skip release boundary"}); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("unbound released error = %v, want forbidden", err)
	}
}

func forgedPendingEndpoint(taskID kernel.TaskID, endpointID coordination.EndpointID, specRef string) coordination.PhaseEndpoint {
	return coordination.PhaseEndpoint{
		Ref: coordination.PhaseEndpointRef{TaskID: taskID, EndpointID: endpointID}, SpecRef: specRef,
		BindingRef: "agent-forged-binding", Generation: 99, State: coordination.EndpointSatisfied, RunPolicy: coordination.RunEnabled,
	}
}

func TestStableProductionSuffixSeparatesIngressNamespaces(t *testing.T) {
	a := stableProductionSuffix("project-a", "manager", "request-1")
	b := stableProductionSuffix("project-a", "human", "request-1")
	if a == "" || a == b || a != stableProductionSuffix("project-a", "manager", "request-1") {
		t.Fatalf("stable production IDs are not deterministic and namespaced: %q %q", a, b)
	}
}

func TestProductionTaskManagerEventsAreIdempotentAndQueryable(t *testing.T) {
	ctx := context.Background()
	log := evidence.NewEventLog(64 * 1024)
	runtime := &productionTaskManagerRuntime{projectID: "project-a", events: log}
	binding := productionTaskManagerBinding{
		InputRef: "manager-input-1", ConversationID: "conversation-1", SeenRevision: 1,
		DecisionRef: "decision-1",
	}
	endpoint := coordination.PhaseEndpoint{
		Ref:        coordination.PhaseEndpointRef{TaskID: "task-a", EndpointID: coordination.EndpointExecute},
		Generation: 1, State: coordination.EndpointPending,
	}
	if err := runtime.appendDecisionAcceptedEvent(ctx, binding, "decision-1"); err != nil {
		t.Fatal(err)
	}
	if err := runtime.appendGraphRevisionEvents(ctx, binding, 2, []coordination.PhaseEndpoint{endpoint}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.appendDecisionAcceptedEvent(ctx, binding, "decision-1"); err != nil {
		t.Fatal(err)
	}
	if err := runtime.appendGraphRevisionEvents(ctx, binding, 2, []coordination.PhaseEndpoint{endpoint}); err != nil {
		t.Fatal(err)
	}

	query := uiprojection.NewEventLogQuery(log, allowProjectPermission{projectID: "project-a"})
	page, err := query.ListEvents(ctx, auth.Principal{ActorPrincipalID: "operator-a", Kind: auth.PrincipalOperator, ProjectID: "project-a", Role: auth.RoleOperator, AuthenticatedAt: time.Now()}, "project-a", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 3 || !productionUIEventsContain(page.Events, "manager.interaction", "graph.revision", "endpoint.updated") {
		t.Fatalf("events = %#v, want idempotent manager/graph/endpoint events", page.Events)
	}
}
