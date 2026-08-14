package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/evidence"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/mergequeue"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	phasepkg "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime/phase"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/taskmanager"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/transport/httpapi"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/transport/mcpapi"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/uiprojection"
)

type fixedProductionFollowupDispatcher struct {
	stored persistedProductionInput
	err    error
}

type fakeProductionTaskManagerDecisionStore struct {
	contracts map[kernel.TaskID]taskmanager.TaskContract
}

type recordingWorkspaceProvisioner struct {
	requests []productionTaskWorkspaceRequest
}

func (r *recordingWorkspaceProvisioner) EnsureTaskWorkspace(_ context.Context, req productionTaskWorkspaceRequest) (kernel.BindingRef, error) {
	r.requests = append(r.requests, req)
	return kernel.BindingRef("workspace://" + string(req.TaskID)), nil
}

func (s fakeProductionTaskManagerDecisionStore) SubmitDecision(context.Context, taskmanager.DecisionSubmission) (string, error) {
	return "", nil
}

func (s fakeProductionTaskManagerDecisionStore) PersistRequirementContract(context.Context, taskmanager.RequirementInput, taskmanager.TaskContract) error {
	return nil
}

func (s fakeProductionTaskManagerDecisionStore) TaskContract(_ context.Context, taskID kernel.TaskID) (taskmanager.TaskContract, error) {
	contract, ok := s.contracts[taskID]
	if !ok {
		return taskmanager.TaskContract{}, kernel.Error{Code: kernel.CodeNotFound, Message: "task contract not found"}
	}
	return contract, nil
}

// allowProjectPermission is a test-only fixture. Production uses persisted
// actor/project/task/body grants through productionUIPermissions.
type allowProjectPermission struct{ projectID kernel.ProjectID }

func (p allowProjectPermission) CanReadProject(_ context.Context, principal auth.Principal, projectID kernel.ProjectID) (bool, error) {
	return principal.ProjectID == p.projectID && projectID == p.projectID, nil
}

func (p allowProjectPermission) TaskGrant(_ context.Context, principal auth.Principal, projectID kernel.ProjectID, taskID kernel.TaskID) (uiprojection.TaskReadGrant, error) {
	if principal.ProjectID != p.projectID || projectID != p.projectID || taskID == "" {
		return uiprojection.TaskReadGrant{}, nil
	}
	return uiprojection.TaskReadGrant{Visible: true, ContextBodies: true, CandidateBodies: true}, nil
}

func (d fixedProductionFollowupDispatcher) DispatchTaskManagerFollowup(context.Context, productionInput) (persistedProductionInput, error) {
	return d.stored, d.err
}

func TestProductionReopenRoundAvailableRequiresCompleteRuntimeRecoveryFacts(t *testing.T) {
	taskID := kernel.TaskID("task-recoverable")
	baseSnapshot := func() coordination.GraphSnapshot {
		return coordination.GraphSnapshot{
			Tasks: []coordination.Task{{ID: taskID, Outcome: coordination.TaskActive}},
			Endpoints: []coordination.PhaseEndpoint{
				{Ref: coordination.PhaseEndpointRef{TaskID: taskID, EndpointID: coordination.EndpointPlan}, State: coordination.EndpointSatisfied, RunPolicy: coordination.RunEnabled},
				{Ref: coordination.PhaseEndpointRef{TaskID: taskID, EndpointID: coordination.EndpointExecute}, State: coordination.EndpointSatisfied, RunPolicy: coordination.RunEnabled},
				{Ref: coordination.PhaseEndpointRef{TaskID: taskID, EndpointID: coordination.EndpointVerify}, State: coordination.EndpointPending, RunPolicy: coordination.RunEnabled},
			},
		}
	}
	baseState := func() mcpapi.TaskManagerDeliveryState {
		return mcpapi.TaskManagerDeliveryState{
			TaskID: taskID, DeliveryPolicy: taskmanager.DeliveryPolicyCodeMerge,
			LatestCandidateStatus: string(mergequeue.StatusFailed), LatestDeliveryStatus: "failed",
			LatestReplanProposalRef: "proposal-recoverable",
		}
	}
	if !productionReopenRoundAvailable(baseSnapshot(), baseState()) {
		t.Fatal("complete persisted recovery facts were not projected as reopenable")
	}
	tests := []struct {
		name   string
		mutate func(*coordination.GraphSnapshot, *mcpapi.TaskManagerDeliveryState)
	}{
		{name: "proposal missing", mutate: func(_ *coordination.GraphSnapshot, state *mcpapi.TaskManagerDeliveryState) {
			state.LatestReplanProposalRef = ""
		}},
		{name: "execute not complete", mutate: func(snapshot *coordination.GraphSnapshot, _ *mcpapi.TaskManagerDeliveryState) {
			snapshot.Endpoints[1].State = coordination.EndpointPending
		}},
		{name: "verify held", mutate: func(snapshot *coordination.GraphSnapshot, _ *mcpapi.TaskManagerDeliveryState) {
			snapshot.Endpoints[2].RunPolicy = coordination.RunHeld
		}},
		{name: "task terminal", mutate: func(snapshot *coordination.GraphSnapshot, _ *mcpapi.TaskManagerDeliveryState) {
			snapshot.Tasks[0].Outcome = coordination.TaskDone
		}},
		{name: "already delivered", mutate: func(_ *coordination.GraphSnapshot, state *mcpapi.TaskManagerDeliveryState) {
			state.ReadyForVerify = true
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			snapshot, state := baseSnapshot(), baseState()
			tc.mutate(&snapshot, &state)
			if productionReopenRoundAvailable(snapshot, state) {
				t.Fatalf("invalid recovery facts projected as reopenable: snapshot=%#v state=%#v", snapshot, state)
			}
		})
	}
}

func TestDispatchPersistedTaskManagerFollowupWaitsForCapacityOnlyAfterPersistence(t *testing.T) {
	capacityErr := kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "no healthy AgentTeams host has matching capacity", Recoverable: true}
	pending := persistedProductionInput{InputRef: "manager-input:followup", InvocationID: "tm-invocation:followup", Status: "pending"}
	if err := dispatchPersistedTaskManagerFollowup(context.Background(), fixedProductionFollowupDispatcher{stored: pending, err: capacityErr}, productionInput{}); err != nil {
		t.Fatalf("durable capacity wait returned error: %v", err)
	}
	if err := dispatchPersistedTaskManagerFollowup(context.Background(), fixedProductionFollowupDispatcher{err: capacityErr}, productionInput{}); !kernel.IsCode(err, kernel.CodeExecutorUnavailable) {
		t.Fatalf("unpersisted capacity failure = %v, want executor_unavailable", err)
	}
	nonCapacityErr := errors.New("protocol failure")
	if err := dispatchPersistedTaskManagerFollowup(context.Background(), fixedProductionFollowupDispatcher{stored: pending, err: nonCapacityErr}, productionInput{}); !errors.Is(err, nonCapacityErr) {
		t.Fatalf("non-capacity failure = %v, want protocol failure", err)
	}
}

func TestProductionTransitionNeedsFollowupIncludesSatisfiedCodeExecute(t *testing.T) {
	tests := []struct {
		name    string
		binding productionTaskManagerBinding
		want    bool
	}{
		{name: "submitted", binding: productionTaskManagerBinding{DecisionAction: "submitted"}, want: true},
		{name: "stopped", binding: productionTaskManagerBinding{DecisionAction: "stopped"}, want: true},
		{name: "satisfied execute", binding: productionTaskManagerBinding{DecisionAction: "satisfied", SelectedEndpoint: coordination.EndpointExecute}, want: true},
		{name: "satisfied verify", binding: productionTaskManagerBinding{DecisionAction: "satisfied", SelectedEndpoint: coordination.EndpointVerify}, want: true},
		{name: "satisfied plan", binding: productionTaskManagerBinding{DecisionAction: "satisfied", SelectedEndpoint: coordination.EndpointPlan}, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := productionTransitionNeedsFollowup(test.binding); got != test.want {
				t.Fatalf("productionTransitionNeedsFollowup() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestTrustedDecisionMutationUsesRuntimeSelectedEndpoint(t *testing.T) {
	payload, _ := json.Marshal(httpapi.ManagerMessageRequest{RequestID: "req-hold", ProjectID: "project-a", ConversationID: "conversation-a", Body: "hold selected endpoint", Intent: httpapi.ManagerIntentHold})
	binding := productionTaskManagerBinding{InputKind: "manager", InputPayload: payload, SelectedTaskID: "task-a", SelectedEndpoint: coordination.EndpointExecute}
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
	payload, _ := json.Marshal(httpapi.ManagerMessageRequest{RequestID: "req-hold", ProjectID: "project-a", ConversationID: "conversation-a", Body: "hold selected endpoint", Intent: httpapi.ManagerIntentHold})
	binding := productionTaskManagerBinding{InputKind: "manager", InputPayload: payload, SelectedTaskID: "task-a", SelectedEndpoint: coordination.EndpointExecute}
	snapshot := coordination.GraphSnapshot{Revision: 7, Endpoints: []coordination.PhaseEndpoint{{Ref: coordination.PhaseEndpointRef{TaskID: "task-a", EndpointID: coordination.EndpointExecute}, Generation: 3}}}
	_, _, err := trustedDecisionMutation(binding, snapshot, taskmanager.TaskManagerDecision{Action: "held", TargetRef: "task-b/execute", Reason: "pause"})
	if !kernel.IsCode(err, kernel.CodeInvalidRequest) {
		t.Fatalf("trustedDecisionMutation() error = %v, want invalid_request", err)
	}
}

func TestTrustedDecisionMutationAllowsOrchestrationHeldOnlyForRuntimeSelectedEndpoint(t *testing.T) {
	ref := coordination.PhaseEndpointRef{TaskID: "task-a", EndpointID: coordination.EndpointExecute}
	binding := productionTaskManagerBinding{InputKind: "phase_orchestration", TargetKind: "phase_orchestration", SelectedTaskID: ref.TaskID, SelectedEndpoint: ref.EndpointID}
	snapshot := coordination.GraphSnapshot{Revision: 7, Endpoints: []coordination.PhaseEndpoint{{Ref: ref, Generation: 3}}}

	kind, transition, err := trustedDecisionMutation(binding, snapshot, taskmanager.TaskManagerDecision{Action: "held", TargetRef: "task-a/execute", Reason: "pause before retry"})
	if err != nil {
		t.Fatal(err)
	}
	if kind != taskmanager.DecisionKindTransition || transition.Endpoint != ref || transition.Generation != 3 || transition.Action != "held" {
		t.Fatalf("orchestration held transition = kind %q transition %#v", kind, transition)
	}

	_, _, err = trustedDecisionMutation(binding, snapshot, taskmanager.TaskManagerDecision{Action: "held", TargetRef: "task-b/execute", Reason: "forged pause"})
	if !kernel.IsCode(err, kernel.CodeInvalidRequest) {
		t.Fatalf("forged orchestration held target = %v, want invalid_request", err)
	}
}

func TestTrustedTargetedVerifyReopenRoundRejectsMissingAuthorityBeforePersistingDecision(t *testing.T) {
	binding := productionTaskManagerBinding{
		InputKind: "phase_orchestration", TargetKind: "phase_orchestration",
		SelectedTaskID: "task-a", SelectedEndpoint: coordination.EndpointVerify,
	}
	snapshot := coordination.GraphSnapshot{Endpoints: []coordination.PhaseEndpoint{
		{Ref: coordination.PhaseEndpointRef{TaskID: "task-a", EndpointID: coordination.EndpointExecute}, State: coordination.EndpointSatisfied, Generation: 1},
		{Ref: coordination.PhaseEndpointRef{TaskID: "task-a", EndpointID: coordination.EndpointVerify}, State: coordination.EndpointPending, Generation: 1},
	}}
	runtime := &productionTaskManagerRuntime{db: &sql.DB{}, projectID: "project-a"}
	_, _, err := runtime.trustedTargetedVerifyReopenRound(context.Background(), binding, snapshot, taskmanager.TaskManagerDecision{Action: "reopen_round", TargetRef: "task-a"})
	// The trusted proposal envelope is intentionally absent, so identity fails
	// before the database is touched. Integration coverage exercises both the
	// live proposal and historical Manager recovery authority paths.
	if err == nil {
		t.Fatal("targeted verify reopen_round without authority unexpectedly accepted")
	}
}

func TestTargetedVerifyReopenBoundaryAcceptsLiveRuntimeProposal(t *testing.T) {
	proposal := phasepkg.OrchestrationProposal{
		ProposalID: "proposal-live-reopen",
		FromEndpoint: coordination.PhaseEndpointRef{
			TaskID: "task-a", EndpointID: coordination.EndpointVerify,
		},
		FromInvocationID:    "inv-targeted-verify",
		OrchestrationAdvice: phasepkg.OrchestrationReplan,
	}
	payload, err := json.Marshal(productionTargetedVerifyProposalBoundary{
		OrchestrationProposal: proposal,
		SourceKind:            productionTargetedVerifyProposalSource,
		CandidateID:           "candidate-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	binding := productionTaskManagerBinding{
		InputKind: "phase_orchestration", TargetKind: "phase_orchestration", TargetRef: proposal.ProposalID,
		SelectedTaskID: proposal.FromEndpoint.TaskID, SelectedEndpoint: proposal.FromEndpoint.EndpointID, InputPayload: payload,
	}
	got, err := (&productionTaskManagerRuntime{}).targetedVerifyReopenBoundary(context.Background(), binding, "task-a")
	if err != nil {
		t.Fatal(err)
	}
	if got.CandidateID != "candidate-a" || got.ProposalID != proposal.ProposalID {
		t.Fatalf("live targeted verifier boundary = %#v", got)
	}
}

func TestNormalizePendingFreezesContractAndRuntimeAuthority(t *testing.T) {
	payload, err := json.Marshal(httpapi.RequirementCreateRequest{RequestID: "req-1", ProjectID: "project-a", Body: "build the feature", Motivation: "ship safely", Constraints: []string{"keep tests green"}})
	if err != nil {
		t.Fatal(err)
	}
	binding := productionTaskManagerBinding{InputRef: "manager-input:1", InputKind: "requirement", InputBody: "build the feature", InputPayload: payload, SeenRevision: 1, DecisionRef: "tmdec:1"}
	intent := mcpapi.PendingSubgraphIntent{
		TaskPolicies: []mcpapi.PendingTaskPolicyIntent{{
			TaskID: "task-a", DeliveryPolicy: taskmanager.DeliveryPolicyNonCodeArtifact,
		}},
		Endpoints: []mcpapi.PendingEndpointIntent{
			pendingEndpointIntent("task-a", coordination.EndpointPlan),
			pendingEndpointIntent("task-a", coordination.EndpointExecute),
			pendingEndpointIntent("task-a", coordination.EndpointVerify),
		},
	}
	runtime := &productionTaskManagerRuntime{projectID: "project-a"}
	plan, err := runtime.normalizePending(context.Background(), binding, coordination.GraphSnapshot{Revision: 1}, intent)
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
	wantContract := canonicalProductionContractRef("project-a", binding.InputRef, "task-a")
	wantPlanSpec := canonicalProductionSpecRef("project-a", binding.InputRef, coordination.PhaseEndpointRef{TaskID: "task-a", EndpointID: coordination.EndpointPlan})
	if resource.Contract.ContractRef != wantContract || resource.Contract.PhaseSpecs[coordination.EndpointPlan] != wantPlanSpec || resource.Input.Requirement.Text != "build the feature" {
		t.Fatalf("frozen requirement contract = %#v %#v", resource.Input, resource.Contract)
	}
	if resource.Contract.DeliveryPolicy != taskmanager.DeliveryPolicyNonCodeArtifact {
		t.Fatalf("delivery policy = %q, want non_code_artifact", resource.Contract.DeliveryPolicy)
	}
}

func TestNormalizePendingRejectsInvalidTaskPolicies(t *testing.T) {
	payload, _ := json.Marshal(httpapi.RequirementCreateRequest{RequestID: "req-policy", ProjectID: "project-a", Body: "produce a report"})
	binding := productionTaskManagerBinding{InputRef: "manager-input:policy", InputKind: "requirement", InputBody: "produce a report", InputPayload: payload, SeenRevision: 1, DecisionRef: "tmdec:policy"}
	endpoints := []mcpapi.PendingEndpointIntent{
		pendingEndpointIntent("task-a", coordination.EndpointPlan),
		pendingEndpointIntent("task-a", coordination.EndpointExecute),
		pendingEndpointIntent("task-a", coordination.EndpointVerify),
	}
	tests := []struct {
		name     string
		policies []mcpapi.PendingTaskPolicyIntent
	}{
		{name: "duplicate", policies: []mcpapi.PendingTaskPolicyIntent{
			{TaskID: "task-a", DeliveryPolicy: taskmanager.DeliveryPolicyNonCodeArtifact},
			{TaskID: "task-a", DeliveryPolicy: taskmanager.DeliveryPolicyCodeMerge},
		}},
		{name: "unknown task", policies: []mcpapi.PendingTaskPolicyIntent{
			{TaskID: "task-b", DeliveryPolicy: taskmanager.DeliveryPolicyNonCodeArtifact},
		}},
		{name: "unsupported", policies: []mcpapi.PendingTaskPolicyIntent{
			{TaskID: "task-a", DeliveryPolicy: taskmanager.DeliveryPolicy("invented")},
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := (&productionTaskManagerRuntime{projectID: "project-a"}).normalizePending(context.Background(), binding, coordination.GraphSnapshot{Revision: 1}, mcpapi.PendingSubgraphIntent{
				TaskPolicies: test.policies,
				Endpoints:    endpoints,
			})
			if !kernel.IsCode(err, kernel.CodeInvalidRequest) {
				t.Fatalf("normalizePending() error = %v, want invalid_request", err)
			}
		})
	}
}

func TestNormalizePendingRequiresExactlyFixedEndpointSet(t *testing.T) {
	payload, _ := json.Marshal(httpapi.ManagerMessageRequest{RequestID: "req-1", ProjectID: "project-a", ConversationID: "conversation-a", Body: "replan"})
	binding := productionTaskManagerBinding{InputRef: "manager-input:1", InputKind: "manager", InputBody: "replan", InputPayload: payload, SeenRevision: 1, DecisionRef: "tmdec:1"}
	_, err := (&productionTaskManagerRuntime{}).normalizePending(context.Background(), binding, coordination.GraphSnapshot{Revision: 1}, mcpapi.PendingSubgraphIntent{
		Endpoints: []mcpapi.PendingEndpointIntent{
			pendingEndpointIntent("task-a", coordination.EndpointPlan),
			pendingEndpointIntent("task-a", coordination.EndpointExecute),
		},
	})
	if !kernel.IsCode(err, kernel.CodeInvalidRequest) {
		t.Fatalf("normalizePending() error = %v, want invalid_request", err)
	}
}

func TestNormalizePendingRequiresExplicitDeliveryPolicyForNewTask(t *testing.T) {
	payload, _ := json.Marshal(httpapi.RequirementCreateRequest{RequestID: "req-policy-required", ProjectID: "project-a", Body: "build task"})
	binding := productionTaskManagerBinding{InputRef: "manager-input:policy-required", InputKind: "requirement", InputBody: "build task", InputPayload: payload, SeenRevision: 1, DecisionRef: "tmdec:policy-required"}

	_, err := (&productionTaskManagerRuntime{projectID: "project-a"}).normalizePending(context.Background(), binding, coordination.GraphSnapshot{Revision: 1}, mcpapi.PendingSubgraphIntent{
		Endpoints: []mcpapi.PendingEndpointIntent{
			pendingEndpointIntent("task-a", coordination.EndpointPlan),
			pendingEndpointIntent("task-a", coordination.EndpointExecute),
			pendingEndpointIntent("task-a", coordination.EndpointVerify),
		},
	})
	if !kernel.IsCode(err, kernel.CodeInvalidRequest) {
		t.Fatalf("normalizePending() error = %v, want invalid_request", err)
	}
}

func TestNormalizePendingAllowsActiveTaskPendingEndpointSubsetAndPreservesContract(t *testing.T) {
	payload, _ := json.Marshal(httpapi.ManagerMessageRequest{RequestID: "req-subset", ProjectID: "project-a", ConversationID: "conversation-a", Body: "retry execute only"})
	binding := productionTaskManagerBinding{InputRef: "manager-input:subset", InputKind: "manager", InputBody: "retry execute only", InputPayload: payload, SeenRevision: 6, DecisionRef: "tmdec:subset"}
	taskID := kernel.TaskID("task-a")
	contract := taskmanager.TaskContract{
		TaskID: taskID, ContractRef: "contract://task-a", DeliveryPolicy: taskmanager.DeliveryPolicyHumanAcceptance,
		PhaseSpecs: map[coordination.EndpointID]string{
			coordination.EndpointPlan:    "spec://task-a/plan/original",
			coordination.EndpointExecute: "spec://task-a/execute/original",
			coordination.EndpointVerify:  "spec://task-a/verify/original",
		},
	}
	executeRef := coordination.PhaseEndpointRef{TaskID: taskID, EndpointID: coordination.EndpointExecute}
	snapshot := coordination.GraphSnapshot{
		Revision: 6,
		Tasks:    []coordination.Task{{ID: taskID, ContractRef: contract.ContractRef, Outcome: coordination.TaskActive}},
		Endpoints: []coordination.PhaseEndpoint{
			{Ref: coordination.PhaseEndpointRef{TaskID: taskID, EndpointID: coordination.EndpointPlan}, SpecRef: contract.PhaseSpecs[coordination.EndpointPlan], BindingRef: canonicalProductionBindingRef(coordination.PhaseEndpointRef{TaskID: taskID, EndpointID: coordination.EndpointPlan}, 1), Generation: 1, State: coordination.EndpointSatisfied, RunPolicy: coordination.RunEnabled},
			{Ref: executeRef, SpecRef: contract.PhaseSpecs[coordination.EndpointExecute], BindingRef: canonicalProductionBindingRef(executeRef, 4), Generation: 4, State: coordination.EndpointPending, RunPolicy: coordination.RunHeld},
			{Ref: coordination.PhaseEndpointRef{TaskID: taskID, EndpointID: coordination.EndpointVerify}, SpecRef: contract.PhaseSpecs[coordination.EndpointVerify], BindingRef: canonicalProductionBindingRef(coordination.PhaseEndpointRef{TaskID: taskID, EndpointID: coordination.EndpointVerify}, 1), Generation: 1, State: coordination.EndpointPending, RunPolicy: coordination.RunEnabled},
		},
	}
	runtime := &productionTaskManagerRuntime{
		projectID: "project-a",
		decisions: fakeProductionTaskManagerDecisionStore{contracts: map[kernel.TaskID]taskmanager.TaskContract{
			taskID: contract,
		}},
	}

	plan, err := runtime.normalizePending(context.Background(), binding, snapshot, mcpapi.PendingSubgraphIntent{
		TaskPolicies: []mcpapi.PendingTaskPolicyIntent{{TaskID: taskID, DeliveryPolicy: taskmanager.DeliveryPolicyHumanAcceptance}},
		Endpoints:    []mcpapi.PendingEndpointIntent{{Ref: executeRef, RunPolicy: coordination.RunHeld}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Subgraph.Tasks) != 1 || plan.Subgraph.Tasks[0].ContractRef != contract.ContractRef {
		t.Fatalf("tasks = %#v, want existing active task unchanged", plan.Subgraph.Tasks)
	}
	if len(plan.Subgraph.Endpoints) != 1 || plan.Subgraph.Endpoints[0] != snapshot.Endpoints[1] {
		t.Fatalf("endpoints = %#v, want only current execute endpoint in replacement scope", plan.Subgraph.Endpoints)
	}
	if len(plan.Resources) != 1 || plan.Resources[0].IsNew || !sameProductionJSON(plan.Resources[0].Contract, contract) {
		t.Fatalf("resources = %#v, want stored contract preserved", plan.Resources)
	}
	if len(plan.Resources[0].Generations) != 0 {
		t.Fatalf("generations = %#v, want existing Task workspace lineage to remain Phase-owned", plan.Resources[0].Generations)
	}

	_, err = runtime.normalizePending(context.Background(), binding, snapshot, mcpapi.PendingSubgraphIntent{
		TaskPolicies: []mcpapi.PendingTaskPolicyIntent{{TaskID: taskID, DeliveryPolicy: taskmanager.DeliveryPolicyCodeMerge}},
		Endpoints:    []mcpapi.PendingEndpointIntent{{Ref: executeRef, RunPolicy: coordination.RunHeld}},
	})
	if !kernel.IsCode(err, kernel.CodeInvalidRequest) {
		t.Fatalf("delivery policy rewrite err = %v, want invalid_request", err)
	}

	_, err = runtime.normalizePending(context.Background(), binding, snapshot, mcpapi.PendingSubgraphIntent{
		Endpoints: []mcpapi.PendingEndpointIntent{{Ref: executeRef, RunPolicy: coordination.RunEnabled}},
	})
	if !kernel.IsCode(err, kernel.CodeInvalidRequest) {
		t.Fatalf("run policy rewrite err = %v, want invalid_request", err)
	}

	_, err = runtime.normalizePending(context.Background(), binding, snapshot, mcpapi.PendingSubgraphIntent{
		Endpoints: []mcpapi.PendingEndpointIntent{{Ref: coordination.PhaseEndpointRef{TaskID: taskID, EndpointID: coordination.EndpointPlan}, RunPolicy: coordination.RunEnabled}},
	})
	if !kernel.IsCode(err, kernel.CodeScopeNotPending) {
		t.Fatalf("non-pending endpoint err = %v, want scope_not_pending", err)
	}
}

func TestEnsurePendingPrerequisitesDoesNotMapExistingEndpointGenerationToWorkspaceRound(t *testing.T) {
	workspaces := &recordingWorkspaceProvisioner{}
	runtime := &productionTaskManagerRuntime{
		decisions:  fakeProductionTaskManagerDecisionStore{contracts: map[kernel.TaskID]taskmanager.TaskContract{"task-a": {TaskID: "task-a", ContractRef: "contract://task-a", DeliveryPolicy: taskmanager.DeliveryPolicyCodeMerge}}},
		workspaces: workspaces,
		contexts:   recordingTaskContextProjector{},
	}
	plan := productionPendingPlan{Resources: []productionPendingTaskResource{{
		Contract:    taskmanager.TaskContract{TaskID: "task-a", ContractRef: "contract://task-a", DeliveryPolicy: taskmanager.DeliveryPolicyCodeMerge},
		Generations: nil,
		IsNew:       false,
	}}}

	if err := runtime.ensurePendingPrerequisites(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	if len(workspaces.requests) != 0 {
		t.Fatalf("existing Task replacement provisioned endpoint generation as workspace round: %#v", workspaces.requests)
	}
}

func TestNormalizePendingDerivesDistinctAuthorityForMultipleTasks(t *testing.T) {
	payload, _ := json.Marshal(httpapi.RequirementCreateRequest{RequestID: "req-multi", ProjectID: "project-a", Body: "build three tasks"})
	binding := productionTaskManagerBinding{InputRef: "manager-input:multi", InputKind: "requirement", InputBody: "build three tasks", InputPayload: payload, SeenRevision: 1, DecisionRef: "tmdec:multi"}
	intent := mcpapi.PendingSubgraphIntent{}
	for _, taskID := range []kernel.TaskID{"task-alpha", "task-beta", "task-gamma"} {
		for _, endpointID := range []coordination.EndpointID{coordination.EndpointPlan, coordination.EndpointExecute, coordination.EndpointVerify} {
			intent.Endpoints = append(intent.Endpoints, pendingEndpointIntent(taskID, endpointID))
		}
	}
	for _, taskID := range []kernel.TaskID{"task-alpha", "task-beta", "task-gamma"} {
		intent.TaskPolicies = append(intent.TaskPolicies, mcpapi.PendingTaskPolicyIntent{TaskID: taskID, DeliveryPolicy: taskmanager.DeliveryPolicyCodeMerge})
	}
	plan, err := (&productionTaskManagerRuntime{projectID: "project-a"}).normalizePending(context.Background(), binding, coordination.GraphSnapshot{Revision: 1}, intent)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Subgraph.Tasks) != 3 || len(plan.Subgraph.Endpoints) != 9 || len(plan.Resources) != 3 {
		t.Fatalf("multi-task plan tasks=%d endpoints=%d resources=%d", len(plan.Subgraph.Tasks), len(plan.Subgraph.Endpoints), len(plan.Resources))
	}
	contracts := make(map[string]struct{}, 3)
	specs := make(map[string]struct{}, 9)
	for _, resource := range plan.Resources {
		contracts[resource.Contract.ContractRef] = struct{}{}
		for _, specRef := range resource.Contract.PhaseSpecs {
			specs[specRef] = struct{}{}
		}
	}
	if len(contracts) != 3 || len(specs) != 9 {
		t.Fatalf("Runtime authority collided contracts=%d specs=%d", len(contracts), len(specs))
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

func TestTrustedStoppedGeneratesNextBindingAndManagerCanReleaseSafely(t *testing.T) {
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
	resumePayload, _ := json.Marshal(httpapi.ManagerMessageRequest{RequestID: "req-resume", ProjectID: "project-a", ConversationID: "conversation-a", Body: "resume selected endpoint", Intent: httpapi.ManagerIntentResume})
	kind, release, err := trustedDecisionMutation(productionTaskManagerBinding{InputKind: "manager", InputPayload: resumePayload, SelectedTaskID: ref.TaskID, SelectedEndpoint: ref.EndpointID}, coordination.GraphSnapshot{Endpoints: []coordination.PhaseEndpoint{endpoint}}, taskmanager.TaskManagerDecision{Action: "released", TargetRef: "task-a/execute", Reason: "resume the held endpoint"})
	if err != nil {
		t.Fatalf("manager released decision: %v", err)
	}
	if kind != taskmanager.DecisionKindTransition || release.Action != "released" || release.Endpoint != ref || release.Generation != endpoint.Generation {
		t.Fatalf("manager release = kind %q transition %#v", kind, release)
	}
}

func TestTrustedManagerLifecycleRequiresExplicitIntentAndPreflightsRelease(t *testing.T) {
	ref := coordination.PhaseEndpointRef{TaskID: "task-a", EndpointID: coordination.EndpointExecute}
	orchestrationPayload, _ := json.Marshal(httpapi.ManagerMessageRequest{
		RequestID: "req-replan", ProjectID: "project-a", ConversationID: "conversation-a",
		Body: "replan the remaining delivery graph", Intent: httpapi.ManagerIntentOrchestrate, SelectedEndpoint: &ref,
	})
	binding := productionTaskManagerBinding{
		InputKind: "manager", InputPayload: orchestrationPayload,
		SelectedTaskID: ref.TaskID, SelectedEndpoint: ref.EndpointID,
	}
	enabled := coordination.PhaseEndpoint{Ref: ref, Generation: 1, State: coordination.EndpointPending, RunPolicy: coordination.RunEnabled}
	_, _, err := trustedDecisionMutation(binding, coordination.GraphSnapshot{Endpoints: []coordination.PhaseEndpoint{enabled}}, taskmanager.TaskManagerDecision{
		Action: "released", TargetRef: "task-a/execute", Reason: "misclassified free-text replan",
	})
	if !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("orchestration message release error = %v, want forbidden", err)
	}

	resumePayload, _ := json.Marshal(httpapi.ManagerMessageRequest{
		RequestID: "req-resume", ProjectID: "project-a", ConversationID: "conversation-a",
		Body: "resume selected endpoint", Intent: httpapi.ManagerIntentResume, SelectedEndpoint: &ref,
	})
	binding.InputPayload = resumePayload
	_, _, err = trustedDecisionMutation(binding, coordination.GraphSnapshot{Endpoints: []coordination.PhaseEndpoint{enabled}}, taskmanager.TaskManagerDecision{
		Action: "released", TargetRef: "task-a/execute", Reason: "resume selected endpoint",
	})
	if !kernel.IsCode(err, kernel.CodeTransitionRejected) {
		t.Fatalf("enabled endpoint release error = %v, want transition_rejected before persistence", err)
	}

	held := enabled
	held.RunPolicy = coordination.RunHeld
	_, transition, err := trustedDecisionMutation(binding, coordination.GraphSnapshot{Endpoints: []coordination.PhaseEndpoint{held}}, taskmanager.TaskManagerDecision{
		Action: "released", TargetRef: "task-a/execute", Reason: "resume selected endpoint",
	})
	if err != nil || transition.Action != "released" || transition.Endpoint != ref {
		t.Fatalf("held endpoint release transition=%#v err=%v", transition, err)
	}
}

func TestTrustedPhaseFailureCanOnlyReopenExactEndpointOrFailExactTask(t *testing.T) {
	ref := coordination.PhaseEndpointRef{TaskID: "task-a", EndpointID: coordination.EndpointExecute}
	endpoint := coordination.PhaseEndpoint{Ref: ref, SpecRef: "spec", BindingRef: canonicalProductionBindingRef(ref, 3), Generation: 3, State: coordination.EndpointPending, RunPolicy: coordination.RunEnabled}
	failed := productionPhaseFailedBoundary{
		CommandID: "start-execute-3", CommandAction: coordination.CommandStart, Endpoint: ref,
		Generation: endpoint.Generation, BindingRef: endpoint.BindingRef, LeaseRef: "lease-execute-3",
	}
	payload, _ := json.Marshal(failed)
	binding := productionTaskManagerBinding{
		InputKind: "phase_failed", InputPayload: payload, SelectedTaskID: ref.TaskID, SelectedEndpoint: ref.EndpointID,
		TargetKind: "phase_failed", TargetRef: failed.CommandID,
	}
	snapshot := coordination.GraphSnapshot{Tasks: []coordination.Task{{ID: ref.TaskID, Outcome: coordination.TaskActive}}, Endpoints: []coordination.PhaseEndpoint{endpoint}}
	_, reopened, err := trustedDecisionMutation(binding, snapshot, taskmanager.TaskManagerDecision{Action: "reopened", TargetRef: "task-a/execute", Reason: "retry with a fresh generation"})
	if err != nil {
		t.Fatal(err)
	}
	if reopened.Endpoint != ref || reopened.Generation != 3 || reopened.NewBindingRef != canonicalProductionBindingRef(ref, 4) || len(reopened.EvidenceRefs) != 1 {
		t.Fatalf("trusted reopened transition = %#v", reopened)
	}
	_, taskFailed, err := trustedDecisionMutation(binding, snapshot, taskmanager.TaskManagerDecision{Action: "failed", TargetRef: "task-a", Reason: "task cannot recover"})
	if err != nil {
		t.Fatal(err)
	}
	if taskFailed.TargetKind != coordination.TargetTask || taskFailed.TaskID != "task-a" || taskFailed.Action != string(coordination.TaskFailed) {
		t.Fatalf("trusted task failed transition = %#v", taskFailed)
	}
	if _, _, err := trustedDecisionMutation(binding, snapshot, taskmanager.TaskManagerDecision{Action: "reopened", TargetRef: "task-b/execute", Reason: "forged target"}); !kernel.IsCode(err, kernel.CodeInvalidRequest) {
		t.Fatalf("forged failure target = %v, want invalid_request", err)
	}
	stale := failed
	stale.BindingRef = "binding://task-a/execute/2"
	stalePayload, _ := json.Marshal(stale)
	binding.InputPayload = stalePayload
	if _, _, err := trustedDecisionMutation(binding, snapshot, taskmanager.TaskManagerDecision{Action: "reopened", TargetRef: "task-a/execute", Reason: "stale"}); !kernel.IsCode(err, kernel.CodeStaleBinding) {
		t.Fatalf("stale failure boundary = %v, want stale_binding", err)
	}
}

func pendingEndpointIntent(taskID kernel.TaskID, endpointID coordination.EndpointID) mcpapi.PendingEndpointIntent {
	return mcpapi.PendingEndpointIntent{
		Ref:       coordination.PhaseEndpointRef{TaskID: taskID, EndpointID: endpointID},
		RunPolicy: coordination.RunEnabled,
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
