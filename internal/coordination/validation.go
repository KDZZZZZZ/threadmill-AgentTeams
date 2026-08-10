package coordination

import (
	"fmt"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

var fixedEndpoints = []EndpointID{EndpointPlan, EndpointExecute, EndpointVerify}

func validatePendingSubgraph(current graphState, next PendingSubgraph) error {
	if next.BaseRevision.IsLatestRead() {
		return kernel.InvalidArgument("base_revision must be a concrete revision")
	}
	if len(next.Endpoints) == 0 {
		return kernel.InvalidArgument("pending subgraph endpoints define the replacement scope")
	}

	scope := make(map[PhaseEndpointRef]struct{}, len(next.Endpoints))
	for _, endpoint := range next.Endpoints {
		if err := validateEndpoint(endpoint); err != nil {
			return err
		}
		if _, ok := scope[endpoint.Ref]; ok {
			return kernel.InvalidGraph("duplicate endpoint in pending subgraph scope")
		}
		scope[endpoint.Ref] = struct{}{}
		if existing, ok := current.endpoints[endpoint.Ref]; ok {
			if existing.State != EndpointPending {
				return kernel.ScopeNotPending("only pending endpoints can be replaced")
			}
			if existing.Generation != endpoint.Generation {
				return kernel.StaleBinding("pending endpoint generation cannot be changed by ReplacePending")
			}
			if existing.BindingRef != endpoint.BindingRef {
				return kernel.StaleBinding("BindingRef is immutable for an endpoint generation")
			}
			if existing.SpecRef != endpoint.SpecRef {
				return kernel.StaleBinding("SpecRef is immutable for an endpoint generation")
			}
			if existing.RunPolicy != endpoint.RunPolicy {
				return kernel.InvalidArgument("ReplacePending cannot rewrite endpoint run policy")
			}
		}
	}

	for _, task := range next.Tasks {
		if err := validateTask(task); err != nil {
			return err
		}
		if existing, ok := current.tasks[task.ID]; ok && existing != task {
			return kernel.InvalidArgument("ReplacePending cannot rewrite an existing task contract or outcome")
		}
		if _, ok := current.tasks[task.ID]; !ok {
			if task.Outcome != TaskActive {
				return kernel.InvalidArgument("new tasks must be active")
			}
			if err := requireFixedEndpointSet(task.ID, next.Endpoints); err != nil {
				return err
			}
		}
	}

	edgeKeys := make(map[string]struct{}, len(next.Edges))
	for _, edge := range next.Edges {
		if err := validateEdge(edge); err != nil {
			return err
		}
		if _, ok := edgeKeys[edgeKey(edge)]; ok {
			return kernel.InvalidGraph("duplicate edge in pending subgraph")
		}
		edgeKeys[edgeKey(edge)] = struct{}{}
		if _, ok := scope[edge.To]; !ok {
			return kernel.InvalidArgument("pending subgraph edges must target an endpoint in scope")
		}
	}
	blockerIDs := make(map[string]struct{}, len(next.Blockers))
	for _, blocker := range next.Blockers {
		if err := validateBlocker(blocker); err != nil {
			return err
		}
		if _, ok := blockerIDs[blocker.ID]; ok {
			return kernel.InvalidGraph("duplicate blocker in pending subgraph")
		}
		blockerIDs[blocker.ID] = struct{}{}
		if _, ok := scope[blocker.Target]; !ok {
			return kernel.InvalidArgument("pending subgraph blockers must target an endpoint in scope")
		}
	}
	return nil
}

func applyPendingSubgraph(state *graphState, next PendingSubgraph) {
	scope := make(map[PhaseEndpointRef]struct{}, len(next.Endpoints))
	for _, endpoint := range next.Endpoints {
		scope[endpoint.Ref] = struct{}{}
	}
	for _, task := range next.Tasks {
		state.tasks[task.ID] = task
	}
	for _, endpoint := range next.Endpoints {
		state.endpoints[endpoint.Ref] = endpoint
	}

	keptEdges := state.edges[:0]
	for _, edge := range state.edges {
		if _, replaced := scope[edge.To]; !replaced {
			keptEdges = append(keptEdges, edge)
		}
	}
	state.edges = append(keptEdges, next.Edges...)

	for id, blocker := range state.blockers {
		if _, replaced := scope[blocker.Target]; replaced {
			delete(state.blockers, id)
		}
	}
	for _, blocker := range next.Blockers {
		state.blockers[blocker.ID] = blocker
	}
}

func validatePendingSubgraphRuntime(runtime memoryRuntimeState, next PendingSubgraph) error {
	scope := make(map[PhaseEndpointRef]struct{}, len(next.Endpoints))
	for _, endpoint := range next.Endpoints {
		scope[endpoint.Ref] = struct{}{}
	}
	for _, lease := range runtime.leases {
		if lease.State != "active" {
			continue
		}
		if _, ok := scope[lease.Endpoint]; ok {
			return kernel.EndpointInFlight("ReplacePending scope contains an endpoint with an active phase lease; hold and stop the phase before replacing it")
		}
	}
	return nil
}

func validateGraph(state graphState) error {
	for _, task := range state.tasks {
		if err := validateTask(task); err != nil {
			return err
		}
		if err := taskHasFixedEndpointSet(state, task.ID); err != nil {
			return err
		}
	}
	for _, endpoint := range state.endpoints {
		if err := validateEndpoint(endpoint); err != nil {
			return err
		}
		if _, ok := state.tasks[endpoint.Ref.TaskID]; !ok {
			return kernel.InvalidGraph("endpoint references unknown task")
		}
	}
	edgeKeys := make(map[string]struct{}, len(state.edges))
	for _, edge := range state.edges {
		if err := validateEdge(edge); err != nil {
			return err
		}
		if _, ok := edgeKeys[edgeKey(edge)]; ok {
			return kernel.InvalidGraph("coordination graph cannot contain duplicate edges")
		}
		edgeKeys[edgeKey(edge)] = struct{}{}
		if _, ok := state.endpoints[edge.From]; !ok {
			return kernel.InvalidGraph("edge source references unknown endpoint")
		}
		if _, ok := state.endpoints[edge.To]; !ok {
			return kernel.InvalidGraph("edge target references unknown endpoint")
		}
	}
	for _, blocker := range state.blockers {
		if err := validateBlocker(blocker); err != nil {
			return err
		}
		if _, ok := state.endpoints[blocker.Target]; !ok {
			return kernel.InvalidGraph("blocker references unknown endpoint")
		}
	}
	for _, result := range state.results {
		if err := validateResult(result); err != nil {
			return err
		}
		if _, ok := state.endpoints[result.Endpoint]; !ok {
			return kernel.InvalidGraph("phase result references unknown endpoint")
		}
	}
	return validateAcyclic(state)
}

func validateTask(task Task) error {
	if err := kernel.RequireID("task.id", task.ID); err != nil {
		return err
	}
	if task.ContractRef == "" {
		return kernel.InvalidArgument("task.contract_ref is required")
	}
	switch task.Outcome {
	case TaskActive, TaskDone, TaskCanceled, TaskFailed:
		return nil
	default:
		return kernel.InvalidArgument("task.outcome is not allowed")
	}
}

func validateEndpoint(endpoint PhaseEndpoint) error {
	if err := validateEndpointRef(endpoint.Ref); err != nil {
		return err
	}
	if endpoint.SpecRef == "" {
		return kernel.InvalidArgument("endpoint.spec_ref is required")
	}
	if err := kernel.RequireID("endpoint.binding_ref", endpoint.BindingRef); err != nil {
		return err
	}
	if endpoint.Generation <= 0 {
		return kernel.InvalidArgument("endpoint.generation must be positive")
	}
	switch endpoint.State {
	case EndpointPending, EndpointSubmitted, EndpointSatisfied, EndpointRejected:
	default:
		return kernel.InvalidArgument("endpoint.state is not allowed")
	}
	switch endpoint.RunPolicy {
	case RunEnabled, RunHeld:
	default:
		return kernel.InvalidArgument("endpoint.run_policy is not allowed")
	}
	return nil
}

func validateEndpointRef(ref PhaseEndpointRef) error {
	if err := kernel.RequireID("endpoint.task_id", ref.TaskID); err != nil {
		return err
	}
	if !isFixedEndpoint(ref.EndpointID) {
		return kernel.InvalidArgument("endpoint_id must be one of plan, execute, verify")
	}
	return nil
}

func validateEdge(edge Edge) error {
	if err := validateEndpointRef(edge.From); err != nil {
		return err
	}
	if err := validateEndpointRef(edge.To); err != nil {
		return err
	}
	switch edge.Signal {
	case SignalPhaseSatisfied, SignalTaskDone:
	default:
		return kernel.InvalidArgument("edge.signal is not allowed")
	}
	switch edge.RequiredBy {
	case RequiredByStart, RequiredByCompletion:
	default:
		return kernel.InvalidArgument("edge.required_by is not allowed")
	}
	switch edge.OnFalse {
	case OnFalseBlock, OnFalseReplan, OnFalseCancel:
	default:
		return kernel.InvalidArgument("edge.on_false is not allowed")
	}
	if edge.From.TaskID == edge.To.TaskID {
		return kernel.InvalidGraph("fixed plan/execute/verify order is not an editable edge")
	}
	return nil
}

func validateBlocker(blocker Blocker) error {
	if blocker.ID == "" {
		return kernel.InvalidArgument("blocker.id is required")
	}
	if err := validateEndpointRef(blocker.Target); err != nil {
		return err
	}
	switch blocker.RequiredBy {
	case RequiredByStart, RequiredByCompletion:
	default:
		return kernel.InvalidArgument("blocker.required_by is not allowed")
	}
	switch blocker.OnFalse {
	case OnFalseBlock, OnFalseReplan, OnFalseCancel:
	default:
		return kernel.InvalidArgument("blocker.on_false is not allowed")
	}
	switch blocker.State {
	case BlockerActive, BlockerResolved, BlockerDenied, BlockerObsolete:
	default:
		return kernel.InvalidArgument("blocker.state is not allowed")
	}
	return nil
}

func validateResult(result PhaseResult) error {
	if result.ID == "" {
		return kernel.InvalidArgument("phase_result.id is required")
	}
	if err := validateEndpointRef(result.Endpoint); err != nil {
		return err
	}
	if err := kernel.RequireID("phase_result.binding_ref", result.BindingRef); err != nil {
		return err
	}
	if result.OutputRef == "" {
		return kernel.InvalidArgument("phase_result.output_ref is required")
	}
	switch result.Verdict {
	case VerdictSubmitted, VerdictSatisfied, VerdictRejected, VerdictInvalidated:
		return nil
	default:
		return kernel.InvalidArgument("phase_result.verdict is not allowed")
	}
}

func requireFixedEndpointSet(taskID kernel.TaskID, endpoints []PhaseEndpoint) error {
	seen := map[EndpointID]struct{}{}
	for _, endpoint := range endpoints {
		if endpoint.Ref.TaskID == taskID {
			seen[endpoint.Ref.EndpointID] = struct{}{}
		}
	}
	for _, endpointID := range fixedEndpoints {
		if _, ok := seen[endpointID]; !ok {
			return kernel.InvalidArgument(fmt.Sprintf("new task %s must include %s endpoint", taskID, endpointID))
		}
	}
	if len(seen) != len(fixedEndpoints) {
		return kernel.InvalidArgument("new task endpoint set must be exactly plan, execute, verify")
	}
	return nil
}

func taskHasFixedEndpointSet(state graphState, taskID kernel.TaskID) error {
	seen := map[EndpointID]struct{}{}
	for ref := range state.endpoints {
		if ref.TaskID == taskID {
			seen[ref.EndpointID] = struct{}{}
		}
	}
	for _, endpointID := range fixedEndpoints {
		if _, ok := seen[endpointID]; !ok {
			return kernel.InvalidArgument(fmt.Sprintf("task %s is missing %s endpoint", taskID, endpointID))
		}
	}
	if len(seen) != len(fixedEndpoints) {
		return kernel.InvalidArgument("task endpoint set must be exactly plan, execute, verify")
	}
	return nil
}

func validateAcyclic(state graphState) error {
	visiting := make(map[PhaseEndpointRef]bool, len(state.endpoints))
	visited := make(map[PhaseEndpointRef]bool, len(state.endpoints))
	adjacent := make(map[PhaseEndpointRef][]PhaseEndpointRef, len(state.endpoints))
	for _, edge := range state.edges {
		adjacent[edge.From] = append(adjacent[edge.From], edge.To)
	}
	for taskID := range state.tasks {
		adjacent[PhaseEndpointRef{TaskID: taskID, EndpointID: EndpointPlan}] = append(adjacent[PhaseEndpointRef{TaskID: taskID, EndpointID: EndpointPlan}], PhaseEndpointRef{TaskID: taskID, EndpointID: EndpointExecute})
		adjacent[PhaseEndpointRef{TaskID: taskID, EndpointID: EndpointExecute}] = append(adjacent[PhaseEndpointRef{TaskID: taskID, EndpointID: EndpointExecute}], PhaseEndpointRef{TaskID: taskID, EndpointID: EndpointVerify})
	}
	var visit func(PhaseEndpointRef) error
	visit = func(ref PhaseEndpointRef) error {
		if visiting[ref] {
			return kernel.InvalidGraph("coordination graph cannot contain cycles")
		}
		if visited[ref] {
			return nil
		}
		visiting[ref] = true
		for _, next := range adjacent[ref] {
			if err := visit(next); err != nil {
				return err
			}
		}
		visiting[ref] = false
		visited[ref] = true
		return nil
	}
	for ref := range state.endpoints {
		if err := visit(ref); err != nil {
			return err
		}
	}
	return nil
}

func isFixedEndpoint(endpointID EndpointID) bool {
	for _, fixed := range fixedEndpoints {
		if endpointID == fixed {
			return true
		}
	}
	return false
}

func edgeKey(edge Edge) string {
	return fmt.Sprintf("%s/%s>%s/%s:%s:%s", edge.From.TaskID, edge.From.EndpointID, edge.To.TaskID, edge.To.EndpointID, edge.Signal, edge.RequiredBy)
}

func validateTransitionShape(transition GraphTransition) error {
	switch transition.TargetKind {
	case TargetPhaseEndpoint:
		if err := validateEndpointRef(transition.Endpoint); err != nil {
			return err
		}
		if transition.Generation <= 0 {
			return kernel.TransitionRejected("phase endpoint transition generation is required")
		}
		switch transition.Action {
		case string(EndpointSubmitted), string(EndpointSatisfied), string(EndpointRejected), "reopened", "held", "released", "stopped":
			return nil
		default:
			return kernel.InvalidArgument("phase endpoint transition action is not allowed")
		}
	case TargetBlocker:
		if transition.BlockerID == "" {
			return kernel.InvalidArgument("blocker_id is required")
		}
		switch transition.Action {
		case string(BlockerResolved), string(BlockerDenied), string(BlockerObsolete):
			return nil
		default:
			return kernel.InvalidArgument("blocker transition action is not allowed")
		}
	case TargetTask:
		if err := kernel.RequireID("task_id", transition.TaskID); err != nil {
			return err
		}
		switch transition.Action {
		case string(TaskDone), string(TaskCanceled), string(TaskFailed):
			return nil
		default:
			return kernel.InvalidArgument("task transition action is not allowed")
		}
	default:
		return kernel.InvalidArgument("transition target kind is not allowed")
	}
}
