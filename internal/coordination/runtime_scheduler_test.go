package coordination

import (
	"context"
	"testing"
)

func TestRuntimeReconcileUsesSelectionPort(t *testing.T) {
	graph, decisions, store := newGraphHarness()
	_ = createTask(t, graph, decisions, "task-a")
	_ = createTask(t, graph, decisions, "task-b")
	controller := &recordingController{}
	selector := &recordingSelector{
		selected: []PhaseEndpointRef{ref("task-b", EndpointPlan)},
	}
	runtime := newGraphRuntime(projectID, store, controller)
	runtime.selectionRuntime = selector

	if err := runtime.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}

	if len(selector.requests) != 1 {
		t.Fatalf("selector requests = %d, want 1", len(selector.requests))
	}
	if len(selector.requests[0].Candidates) != 2 {
		t.Fatalf("selector candidates = %#v, want both runnable plan endpoints", selector.requests[0].Candidates)
	}
	if len(controller.commands) != 1 || controller.commands[0].Endpoint != ref("task-b", EndpointPlan) {
		t.Fatalf("commands = %#v, want only selector-chosen task-b plan", controller.commands)
	}
}

func TestRuntimeSchedulingStateCapacityControlsDispatchAndDrain(t *testing.T) {
	graph, decisions, store := newGraphHarness()
	_ = createTask(t, graph, decisions, "task-a")
	_ = createTask(t, graph, decisions, "task-b")
	_ = createTask(t, graph, decisions, "task-c")
	controller := &recordingController{}
	selector := &capacityBudgetSelector{}
	state := &mutableSchedulingStateProvider{
		state: RuntimeSchedulingState{Capacity: RuntimeCapacity{Desired: 0, Healthy: 3}},
	}
	runtime := newGraphRuntime(projectID, store, controller)
	runtime.selectionRuntime = selector
	runtime.schedulingStateProvider = state

	if err := runtime.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(controller.commands) != 0 {
		t.Fatalf("commands with desired=0 = %#v, want none", controller.commands)
	}
	if got := selector.lastCapacity().Active; got != 0 {
		t.Fatalf("active sent to selector = %d, want persisted active lease count 0", got)
	}

	state.state.Capacity = RuntimeCapacity{Desired: 5, Healthy: 2}
	if err := runtime.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(controller.commands) != 2 {
		t.Fatalf("commands with desired>healthy = %#v, want two healthy slots", controller.commands)
	}
	if active := countActiveRuntimeLeases(t, store); active != 2 {
		t.Fatalf("active leases = %d, want 2", active)
	}

	state.state.Capacity = RuntimeCapacity{Desired: 1, Healthy: 5}
	if err := runtime.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(controller.commands) != 2 {
		t.Fatalf("commands after lowering desired = %#v, want no cancellation or new dispatch", controller.commands)
	}
	if active := countActiveRuntimeLeases(t, store); active != 2 {
		t.Fatalf("active leases after lowering desired = %d, want running work preserved", active)
	}
	if got := selector.lastCapacity().Active; got != 2 {
		t.Fatalf("active sent to selector after lowering desired = %d, want persisted active lease count 2", got)
	}
}

func TestRuntimeSchedulingStateBudgetControlsDispatch(t *testing.T) {
	graph, decisions, store := newGraphHarness()
	_ = createTask(t, graph, decisions, "task-a")
	controller := &recordingController{}
	selector := &capacityBudgetSelector{}
	state := &mutableSchedulingStateProvider{
		state: RuntimeSchedulingState{
			Capacity: RuntimeCapacity{Desired: 1, Healthy: 1},
			Budget:   RuntimeBudgetStatus{AgentInvocationsUsed: 1},
		},
	}
	runtime := newGraphRuntime(projectID, store, controller)
	runtime.selectionRuntime = selector
	runtime.schedulingStateProvider = state

	if err := runtime.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(controller.commands) != 0 {
		t.Fatalf("commands with exhausted budget = %#v, want none", controller.commands)
	}
	if got := selector.lastBudget().AgentInvocationsUsed; got != 1 {
		t.Fatalf("budget sent to selector = %d, want 1", got)
	}

	state.state.Budget = RuntimeBudgetStatus{}
	if err := runtime.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(controller.commands) != 1 {
		t.Fatalf("commands after budget refresh = %#v, want dispatch", controller.commands)
	}
}

func TestRuntimeDefaultSelectorUsesRequestCapacity(t *testing.T) {
	graph, decisions, store := newGraphHarness()
	_ = createTask(t, graph, decisions, "task-a")
	_ = createTask(t, graph, decisions, "task-b")
	controller := &recordingController{}
	state := &mutableSchedulingStateProvider{
		state: RuntimeSchedulingState{Capacity: RuntimeCapacity{Desired: 0, Healthy: 2}},
	}
	runtime := newGraphRuntime(projectID, store, controller)
	runtime.schedulingStateProvider = state

	if err := runtime.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(controller.commands) != 0 {
		t.Fatalf("default selector commands with desired=0 = %#v, want none", controller.commands)
	}

	state.state.Capacity = RuntimeCapacity{Desired: 2, Healthy: 1}
	if err := runtime.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(controller.commands) != 1 {
		t.Fatalf("default selector commands with healthy=1 = %#v, want one", controller.commands)
	}
}

type recordingSelector struct {
	requests []RuntimeSelectionRequest
	selected []PhaseEndpointRef
}

func (s *recordingSelector) SelectRunnable(_ context.Context, request RuntimeSelectionRequest) ([]PhaseEndpoint, error) {
	s.requests = append(s.requests, request)
	byRef := make(map[PhaseEndpointRef]PhaseEndpoint, len(request.Candidates))
	for _, candidate := range request.Candidates {
		byRef[candidate.Endpoint.Ref] = candidate.Endpoint
	}
	selected := make([]PhaseEndpoint, 0, len(s.selected))
	for _, ref := range s.selected {
		if endpoint, ok := byRef[ref]; ok {
			selected = append(selected, endpoint)
		}
	}
	return selected, nil
}

type mutableSchedulingStateProvider struct {
	state RuntimeSchedulingState
}

func (p *mutableSchedulingStateProvider) RuntimeSchedulingState(context.Context) (RuntimeSchedulingState, error) {
	return p.state, nil
}

type capacityBudgetSelector struct {
	requests []RuntimeSelectionRequest
}

func (s *capacityBudgetSelector) SelectRunnable(_ context.Context, request RuntimeSelectionRequest) ([]PhaseEndpoint, error) {
	s.requests = append(s.requests, request)
	if request.Budget.AgentInvocationsUsed > 0 {
		return nil, nil
	}
	available := request.Capacity.Available()
	selected := make([]PhaseEndpoint, 0, available)
	for _, candidate := range request.Candidates {
		if len(selected) == available {
			break
		}
		if candidate.Runnable && candidate.CapabilityMatched {
			selected = append(selected, candidate.Endpoint)
		}
	}
	return selected, nil
}

func (s *capacityBudgetSelector) lastCapacity() RuntimeCapacity {
	if len(s.requests) == 0 {
		return RuntimeCapacity{}
	}
	return s.requests[len(s.requests)-1].Capacity
}

func (s *capacityBudgetSelector) lastBudget() RuntimeBudgetStatus {
	if len(s.requests) == 0 {
		return RuntimeBudgetStatus{}
	}
	return s.requests[len(s.requests)-1].Budget
}

func countActiveRuntimeLeases(t *testing.T, store *MemoryStore) int {
	t.Helper()
	active := 0
	for _, lease := range store.runtimeLeases(context.Background(), projectID) {
		if lease.State == "active" {
			active++
		}
	}
	return active
}
