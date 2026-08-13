package uiprojection

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime"
)

func (s *Service) Snapshot(ctx context.Context, principal auth.Principal, projectID kernel.ProjectID, revision kernel.Revision) (CoordinationSnapshot, error) {
	if err := s.requireProject(ctx, principal, projectID); err != nil {
		return CoordinationSnapshot{}, err
	}
	if s.capacity == nil || s.graphs == nil || s.invocations == nil || s.cursors == nil {
		return CoordinationSnapshot{}, kernel.Error{Code: kernel.CodeInternalError, Message: "UI projection readers are not configured", Recoverable: true}
	}

	// Capture the cursor before reading stores. A concurrent change can then be
	// replayed after this cursor; reading it last could skip a change absent from
	// an earlier store read. Duplicate delivery remains harmless to the client.
	cursor, err := s.cursors.CurrentCursor(ctx, projectID)
	if err != nil {
		return CoordinationSnapshot{}, err
	}
	if cursor == "" {
		cursor = "0"
	}
	graph, err := s.readGraph(ctx, projectID, revision)
	if err != nil {
		return CoordinationSnapshot{}, err
	}
	capacity, err := s.capacity.ReadCapacity(ctx, projectID)
	if err != nil {
		return CoordinationSnapshot{}, err
	}
	if err := validateCapacity(capacity); err != nil {
		return CoordinationSnapshot{}, err
	}
	invocations, err := s.invocations.ListInvocations(ctx, InvocationFilter{ProjectID: projectID})
	if err != nil {
		return CoordinationSnapshot{}, err
	}
	invocations = filterInvocations(invocations, InvocationFilter{ProjectID: projectID})

	visibleTasks := make(map[kernel.TaskID]coordination.Task, len(graph.Tasks))
	tasks := make([]TaskSummary, 0, len(graph.Tasks))
	for _, task := range graph.Tasks {
		grant, err := s.taskGrant(ctx, principal, projectID, task.ID)
		if err != nil {
			return CoordinationSnapshot{}, err
		}
		if !grant.Visible {
			continue
		}
		visibleTasks[task.ID] = task
		tasks = append(tasks, TaskSummary{TaskID: task.ID, Status: taskProjectionStatus(task, graph, invocations)})
	}

	nodes := make([]GraphNode, 0, len(graph.Endpoints))
	for _, endpoint := range graph.Endpoints {
		if _, ok := visibleTasks[endpoint.Ref.TaskID]; !ok {
			continue
		}
		latest, _ := selectInvocation(invocations, endpoint.Ref, uint64(endpoint.Generation), true)
		nodes = append(nodes, graphNode(endpoint, latest))
	}
	edges := make([]GraphEdge, 0, len(graph.Edges))
	for _, edge := range graph.Edges {
		if _, ok := visibleTasks[edge.From.TaskID]; !ok {
			continue
		}
		if _, ok := visibleTasks[edge.To.TaskID]; !ok {
			continue
		}
		edges = append(edges, graphEdge(edge, graph))
	}

	waiting := 0
	for _, invocation := range invocations {
		if _, visible := visibleTasks[invocation.TaskID]; visible && invocation.Status == runtime.InvocationWaiting {
			waiting++
		}
	}
	return CoordinationSnapshot{
		ProjectID: projectID,
		Revision:  graph.Revision,
		Cursor:    cursor,
		Tasks:     nonNilTasks(tasks),
		Nodes:     nonNilNodes(nodes),
		Edges:     nonNilEdges(edges),
		Capacity: CapacityState{
			ProjectID:          projectID,
			Revision:           capacity.Capacity.Revision,
			DesiredConcurrency: capacity.Capacity.Desired,
			HealthyCapacity:    capacity.Capacity.Healthy,
			ActiveInvocations:  capacity.Capacity.Active,
			WaitingInvocations: waiting,
			DegradedReason:     capacity.DegradedReason,
			UpdatedAt:          capacity.UpdatedAt,
		},
	}, nil
}

func (s *Service) readGraph(ctx context.Context, projectID kernel.ProjectID, revision kernel.Revision) (coordination.GraphSnapshot, error) {
	if revision.IsLatestRead() {
		return s.graphs.Latest(ctx, projectID)
	}
	return s.graphs.Snapshot(ctx, projectID, revision)
}

func validateCapacity(record CapacityRecord) error {
	capacity := record.Capacity
	if capacity.Desired < 0 || capacity.Healthy < 0 || capacity.Active < 0 || capacity.Active > capacity.Healthy || capacity.Revision < 0 {
		return kernel.Error{Code: kernel.CodeInternalError, Message: "scheduler returned an invalid capacity snapshot", Recoverable: true}
	}
	if record.UpdatedAt.IsZero() {
		return kernel.Error{Code: kernel.CodeInternalError, Message: "capacity snapshot is missing updated_at", Recoverable: true}
	}
	return nil
}

func taskProjectionStatus(task coordination.Task, graph coordination.GraphSnapshot, invocations []runtime.Invocation) string {
	switch task.Outcome {
	case coordination.TaskDone:
		return "done"
	case coordination.TaskCanceled:
		return "canceled"
	case coordination.TaskFailed:
		return "failed"
	}
	for _, blocker := range graph.Blockers {
		if blocker.Target.TaskID == task.ID && blocker.State == coordination.BlockerActive {
			return "blocked"
		}
	}
	for _, invocation := range invocations {
		if invocation.TaskID == task.ID && invocationIsActive(invocation) {
			return "running"
		}
	}
	return "pending"
}

func graphNode(endpoint coordination.PhaseEndpoint, invocation runtime.Invocation) GraphNode {
	node := GraphNode{
		ID:         endpointKey(endpoint.Ref),
		Kind:       "endpoint",
		Label:      string(endpoint.Ref.EndpointID),
		TaskID:     endpoint.Ref.TaskID,
		EndpointID: endpoint.Ref.EndpointID,
		Generation: endpoint.Generation,
		State:      string(endpoint.State),
		RunPolicy:  endpoint.RunPolicy,
		BindingRef: endpoint.BindingRef,
	}
	if invocation.ID != "" {
		node.LatestInvocationRef = invocation.ID
		switch invocation.Status {
		case runtime.InvocationPrepared:
			node.State = "starting"
		case runtime.InvocationRunning:
			node.State = "running"
		case runtime.InvocationWaiting:
			node.State = "waiting"
		case runtime.InvocationStopped:
			node.State = "stopped"
		}
	}
	return node
}

func graphEdge(edge coordination.Edge, graph coordination.GraphSnapshot) GraphEdge {
	return GraphEdge{
		ID:            fmt.Sprintf("%s>%s:%s:%s", endpointKey(edge.From), endpointKey(edge.To), edge.Signal, edge.RequiredBy),
		From:          edge.From,
		To:            edge.To,
		RequiredBy:    edge.RequiredBy,
		State:         edgeProjectionState(edge, graph),
		ArtifactKinds: append([]string(nil), edge.ArtifactKinds...),
	}
}

func edgeProjectionState(edge coordination.Edge, graph coordination.GraphSnapshot) string {
	if edge.Signal == coordination.SignalTaskDone {
		for _, task := range graph.Tasks {
			if task.ID != edge.From.TaskID {
				continue
			}
			switch task.Outcome {
			case coordination.TaskDone:
				return "satisfied"
			case coordination.TaskCanceled:
				return "obsolete"
			case coordination.TaskFailed:
				return "failed"
			default:
				return "pending"
			}
		}
	}
	for _, endpoint := range graph.Endpoints {
		if endpoint.Ref != edge.From {
			continue
		}
		switch endpoint.State {
		case coordination.EndpointSatisfied:
			return "satisfied"
		case coordination.EndpointRejected:
			return "failed"
		default:
			return "pending"
		}
	}
	return "obsolete"
}

func endpointKey(ref coordination.PhaseEndpointRef) string {
	return string(ref.TaskID) + "/" + string(ref.EndpointID)
}

func filterInvocations(invocations []runtime.Invocation, filter InvocationFilter) []runtime.Invocation {
	out := make([]runtime.Invocation, 0, len(invocations))
	for _, invocation := range invocations {
		if filter.ProjectID != "" && invocation.ProjectID != filter.ProjectID {
			continue
		}
		if filter.TaskID != "" && invocation.TaskID != filter.TaskID {
			continue
		}
		if filter.EndpointID != "" && invocation.EndpointID != filter.EndpointID {
			continue
		}
		if filter.Generation != 0 && invocation.Generation != filter.Generation {
			continue
		}
		if !invocation.Role.IsPhase() {
			continue
		}
		out = append(out, invocation)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Generation != out[j].Generation {
			return out[i].Generation < out[j].Generation
		}
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}
		return out[i].ID < out[j].ID
	})
	return out
}

func selectInvocation(invocations []runtime.Invocation, ref coordination.PhaseEndpointRef, generation uint64, exactGeneration bool) (runtime.Invocation, bool) {
	filtered := filterInvocations(invocations, InvocationFilter{TaskID: ref.TaskID, EndpointID: ref.EndpointID})
	var active []runtime.Invocation
	var historical []runtime.Invocation
	for _, invocation := range filtered {
		if exactGeneration && generation != 0 && invocation.Generation != generation {
			continue
		}
		if invocationIsActive(invocation) {
			active = append(active, invocation)
		} else {
			historical = append(historical, invocation)
		}
	}
	if len(active) > 0 {
		return active[len(active)-1], true
	}
	if len(historical) > 0 {
		return historical[len(historical)-1], true
	}
	return runtime.Invocation{}, false
}

func invocationIsActive(invocation runtime.Invocation) bool {
	switch invocation.Status {
	case runtime.InvocationPrepared, runtime.InvocationRunning, runtime.InvocationWaiting:
		return true
	default:
		return false
	}
}

func nonNilTasks(in []TaskSummary) []TaskSummary {
	if in == nil {
		return []TaskSummary{}
	}
	return in
}

func nonNilNodes(in []GraphNode) []GraphNode {
	if in == nil {
		return []GraphNode{}
	}
	return in
}

func nonNilEdges(in []GraphEdge) []GraphEdge {
	if in == nil {
		return []GraphEdge{}
	}
	return in
}

func revisionString(revision int64) string { return strconv.FormatInt(revision, 10) }
