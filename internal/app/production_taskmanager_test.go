package app

import (
	"testing"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/taskmanager"
)

func TestTrustedDecisionMutationUsesRuntimeSelectedEndpoint(t *testing.T) {
	binding := productionTaskManagerBinding{SelectedTaskID: "task-a", SelectedEndpoint: coordination.EndpointExecute}
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
	binding := productionTaskManagerBinding{SelectedTaskID: "task-a", SelectedEndpoint: coordination.EndpointExecute}
	snapshot := coordination.GraphSnapshot{Revision: 7, Endpoints: []coordination.PhaseEndpoint{{Ref: coordination.PhaseEndpointRef{TaskID: "task-a", EndpointID: coordination.EndpointExecute}, Generation: 3}}}
	_, _, err := trustedDecisionMutation(binding, snapshot, taskmanager.TaskManagerDecision{Action: "held", TargetRef: "task-b/execute", Reason: "pause"})
	if !kernel.IsCode(err, kernel.CodeInvalidRequest) {
		t.Fatalf("trustedDecisionMutation() error = %v, want invalid_request", err)
	}
}

func TestStableProductionSuffixSeparatesIngressNamespaces(t *testing.T) {
	a := stableProductionSuffix("project-a", "manager", "request-1")
	b := stableProductionSuffix("project-a", "human", "request-1")
	if a == "" || a == b || a != stableProductionSuffix("project-a", "manager", "request-1") {
		t.Fatalf("stable production IDs are not deterministic and namespaced: %q %q", a, b)
	}
}
