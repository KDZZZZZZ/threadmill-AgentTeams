package scheduler

import (
	"context"
	"testing"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

var _ coordination.RuntimeSelectionRuntime = Scheduler{}
var _ coordination.RuntimeSchedulingStateProvider = (*SchedulingStateProvider)(nil)

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

func TestCapacityLedgerNoopObservationKeepsRevision(t *testing.T) {
	ledger := NewCapacityLedger(4, 3)

	current, err := ledger.Observe(context.Background(), 4, 0)
	if err != nil {
		t.Fatal(err)
	}
	if current.Revision != 1 {
		t.Fatalf("no-op observation revision = %d, want 1", current.Revision)
	}

	current, err = ledger.Observe(context.Background(), 4, 2)
	if err != nil {
		t.Fatal(err)
	}
	if current.Revision != 2 {
		t.Fatalf("changed observation revision = %d, want 2", current.Revision)
	}

	current, err = ledger.Observe(context.Background(), 4, 2)
	if err != nil {
		t.Fatal(err)
	}
	if current.Revision != 2 {
		t.Fatalf("repeated observation revision = %d, want 2", current.Revision)
	}
}

func TestCapacityLedgerKeepsDesiredSeparateFromHealthy(t *testing.T) {
	ledger := NewCapacityLedger(2, 5)
	current := ledger.Snapshot()
	var err error
	if current.Desired != 5 || current.Healthy != 2 {
		t.Fatalf("initial capacity = %#v, want desired=5 healthy=2", current)
	}
	current, err = ledger.SetDesired(context.Background(), current.Revision, 7)
	if err != nil {
		t.Fatal(err)
	}
	if current.Desired != 7 || current.Healthy != 2 {
		t.Fatalf("capacity after desired increase = %#v, want desired=7 healthy=2", current)
	}
	current, err = ledger.Observe(context.Background(), 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if current.Desired != 7 || current.Healthy != 1 || current.Active != 1 {
		t.Fatalf("observed capacity = %#v, want desired=7 healthy=1 active=1", current)
	}
}

func TestSchedulerAvailabilityUsesDesiredHealthyMinAndAllowsDrain(t *testing.T) {
	s := New(BudgetPolicy{})
	candidates := []Candidate{
		{Endpoint: endpoint("task-a", coordination.EndpointPlan), Runnable: true, CapabilityMatched: true},
		{Endpoint: endpoint("task-b", coordination.EndpointPlan), Runnable: true, CapabilityMatched: true},
		{Endpoint: endpoint("task-c", coordination.EndpointPlan), Runnable: true, CapabilityMatched: true},
	}
	selected := s.Select(candidates, Capacity{Desired: 5, Healthy: 2, Active: 1}, BudgetStatus{})
	if len(selected) != 1 {
		t.Fatalf("selected = %#v, want one available slot from min(desired, healthy)-active", selected)
	}
	selected = s.Select(candidates, Capacity{Desired: 1, Healthy: 5, Active: 3}, BudgetStatus{})
	if len(selected) != 0 {
		t.Fatalf("selected = %#v, want drain with active above desired", selected)
	}
}

func TestSchedulingStateProviderReadsCapacityAndBudgetDynamically(t *testing.T) {
	capacity := NewCapacityLedger(1, 3)
	budget := NewBudgetLedger(BudgetStatus{})
	provider := NewSchedulingStateProvider(capacity, budget)

	state, err := provider.RuntimeSchedulingState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if state.Capacity.Desired != 3 || state.Capacity.Healthy != 1 || state.Budget.AgentInvocationsUsed != 0 {
		t.Fatalf("initial scheduling state = %#v", state)
	}
	if _, err := capacity.Observe(context.Background(), 2, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := budget.Observe(context.Background(), BudgetStatus{AgentInvocationsUsed: 2}); err != nil {
		t.Fatal(err)
	}
	state, err = provider.RuntimeSchedulingState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if state.Capacity.Desired != 3 || state.Capacity.Healthy != 2 || state.Capacity.Active != 1 || state.Budget.AgentInvocationsUsed != 2 {
		t.Fatalf("updated scheduling state = %#v", state)
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
