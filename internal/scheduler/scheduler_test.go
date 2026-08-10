package scheduler

import (
	"context"
	"testing"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

func TestSchedulerSelectsRunnableCapacityBudgetAndProtectsVerify(t *testing.T) {
	s := New(BudgetPolicy{MaxAgentInvocations: 1, VerifyLevel: VerifyStrict})
	selected := s.Select([]Candidate{
		{Endpoint: endpoint("task-a", coordination.EndpointExecute), Runnable: true, CapabilityMatched: true},
		{Endpoint: endpoint("task-b", coordination.EndpointVerify), Runnable: true, CapabilityMatched: true, UnblocksDependents: true},
		{Endpoint: endpoint("task-c", coordination.EndpointPlan), Runnable: false, CapabilityMatched: true},
		{Endpoint: endpoint("task-d", coordination.EndpointVerify), Runnable: true, CapabilityMatched: false},
	}, Capacity{Desired: 2, Active: 0, Healthy: 4}, BudgetStatus{AgentInvocationsUsed: 1})
	if len(selected) != 1 || selected[0].Ref.EndpointID != coordination.EndpointVerify || selected[0].Ref.TaskID != "task-b" {
		t.Fatalf("selected = %#v, want only protected verify", selected)
	}
}

func TestCapacityLedgerCASAndDecreaseDoesNotCancelActive(t *testing.T) {
	ledger := NewCapacityLedger(4, 3)
	current, err := ledger.Observe(context.Background(), 4, 3)
	if err != nil {
		t.Fatal(err)
	}
	if current.Desired != 3 || current.Active != 3 || current.Revision != 2 {
		t.Fatalf("observed capacity = %#v", current)
	}
	next, err := ledger.SetDesired(context.Background(), current.Revision, 1)
	if err != nil {
		t.Fatal(err)
	}
	if next.Desired != 1 || next.Active != 3 {
		t.Fatalf("capacity after decrease = %#v, want desired=1 active=3", next)
	}
	_, err = ledger.SetDesired(context.Background(), current.Revision, 2)
	if !kernel.IsCode(err, kernel.CodeRevisionConflict) {
		t.Fatalf("stale CAS err = %v, want revision_conflict", err)
	}
}

func TestSchedulerProtectsVerifyWhenRetryBudgetReached(t *testing.T) {
	s := New(BudgetPolicy{MaxRetries: 1})
	selected := s.Select([]Candidate{
		{Endpoint: endpoint("task-execute", coordination.EndpointExecute), Runnable: true, CapabilityMatched: true},
		{Endpoint: endpoint("task-verify", coordination.EndpointVerify), Runnable: true, CapabilityMatched: true},
	}, Capacity{Desired: 2, Healthy: 2}, BudgetStatus{RetriesUsed: 1})
	if len(selected) != 1 || selected[0].Ref.EndpointID != coordination.EndpointVerify {
		t.Fatalf("selected = %#v, want only verify at retry limit", selected)
	}
}

func TestSchedulerPrioritizesMergeThenUnblockingVerifyThenExecute(t *testing.T) {
	s := New(BudgetPolicy{})
	selected := s.Select([]Candidate{
		{Endpoint: endpoint("task-exec", coordination.EndpointExecute), Runnable: true, CapabilityMatched: true},
		{Endpoint: endpoint("task-verify", coordination.EndpointVerify), Runnable: true, CapabilityMatched: true, UnblocksDependents: true},
		{Endpoint: endpoint("task-merge", coordination.EndpointVerify), Runnable: true, CapabilityMatched: true, LatestMainMergeCandidate: true},
	}, Capacity{Desired: 3, Active: 0, Healthy: 3}, BudgetStatus{})
	if len(selected) != 3 {
		t.Fatalf("selected = %#v, want 3", selected)
	}
	if selected[0].Ref.TaskID != "task-merge" || selected[1].Ref.TaskID != "task-verify" || selected[2].Ref.TaskID != "task-exec" {
		t.Fatalf("priority order = %#v", selected)
	}
}

func endpoint(taskID kernel.TaskID, endpointID coordination.EndpointID) coordination.PhaseEndpoint {
	return coordination.PhaseEndpoint{
		Ref:        coordination.PhaseEndpointRef{TaskID: taskID, EndpointID: endpointID},
		SpecRef:    "spec://" + string(taskID) + "/" + string(endpointID),
		BindingRef: kernel.BindingRef("binding://" + string(taskID) + "/" + string(endpointID)),
		Generation: 1,
		State:      coordination.EndpointPending,
		RunPolicy:  coordination.RunEnabled,
	}
}
