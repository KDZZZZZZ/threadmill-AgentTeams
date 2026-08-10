package uiprojection

import (
	"context"
	"sort"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/contextgraph"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime"
)

func (s *Service) InspectEndpoint(ctx context.Context, principal auth.Principal, projectID kernel.ProjectID, ref coordination.PhaseEndpointRef, generation int) (EndpointInspector, error) {
	if err := s.requireProject(ctx, principal, projectID); err != nil {
		return EndpointInspector{}, err
	}
	if ref.TaskID == "" || ref.EndpointID == "" {
		return EndpointInspector{}, kernel.InvalidArgument("endpoint task_id and endpoint_id are required")
	}
	if generation < 0 {
		return EndpointInspector{}, kernel.InvalidArgument("generation cannot be negative")
	}
	grant, err := s.taskGrant(ctx, principal, projectID, ref.TaskID)
	if err != nil {
		return EndpointInspector{}, err
	}
	if !grant.Visible {
		return EndpointInspector{}, kernel.Error{Code: kernel.CodeNotFound, Message: "endpoint not found"}
	}
	if s.graphs == nil || s.invocations == nil || s.contexts == nil {
		return EndpointInspector{}, kernel.Error{Code: kernel.CodeInternalError, Message: "endpoint inspector readers are not configured", Recoverable: true}
	}
	graph, err := s.graphs.Latest(ctx, projectID)
	if err != nil {
		return EndpointInspector{}, err
	}
	endpoint, ok := findEndpoint(graph, ref)
	if !ok {
		return EndpointInspector{}, kernel.Error{Code: kernel.CodeNotFound, Message: "endpoint not found"}
	}
	invocations, err := s.invocations.ListInvocations(ctx, InvocationFilter{ProjectID: projectID, TaskID: ref.TaskID, EndpointID: ref.EndpointID})
	if err != nil {
		return EndpointInspector{}, err
	}
	invocations = filterInvocations(invocations, InvocationFilter{ProjectID: projectID, TaskID: ref.TaskID, EndpointID: ref.EndpointID})

	selectedGeneration := generation
	var invocation runtime.Invocation
	var found bool
	if generation > 0 {
		invocation, found = selectInvocation(invocations, ref, uint64(generation), true)
	} else {
		invocation, found = selectInvocation(invocations, ref, uint64(endpoint.Generation), true)
		if !found {
			invocation, found = selectInvocation(invocations, ref, 0, false)
		}
		if found {
			selectedGeneration = int(invocation.Generation)
		} else {
			selectedGeneration = endpoint.Generation
		}
	}
	if generation > 0 && !found && generation != endpoint.Generation {
		return EndpointInspector{}, kernel.Error{Code: kernel.CodeNotFound, Message: "endpoint generation not found"}
	}

	result := EndpointInspector{
		Endpoint:      ref,
		Generation:    selectedGeneration,
		GraphRevision: graph.Revision,
		Subscriptions: []SubscriptionProjection{},
	}
	if !found {
		return result, nil
	}
	projection := invocationProjection(invocation)
	result.Invocation = &projection

	inspection, err := s.contexts.InspectInvocation(ctx, principal, invocation)
	if err != nil {
		return EndpointInspector{}, err
	}
	result.Subscriptions = projectSubscriptions(inspection.Subscriptions, invocation)
	contextSlice := projectContextSlice(inspection, invocation, grant.ContextBodies)
	result.ContextSlice = &contextSlice
	memory := projectTaskMemory(inspection.Candidates, invocation, grant.CandidateBodies)
	result.TaskMemoryBuffer = &memory
	return result, nil
}

func findEndpoint(graph coordination.GraphSnapshot, ref coordination.PhaseEndpointRef) (coordination.PhaseEndpoint, bool) {
	for _, endpoint := range graph.Endpoints {
		if endpoint.Ref == ref {
			return endpoint, true
		}
	}
	return coordination.PhaseEndpoint{}, false
}

func invocationProjection(invocation runtime.Invocation) InvocationProjection {
	status := string(invocation.Status)
	if invocation.Status == runtime.InvocationPrepared {
		status = "pending"
	}
	createdAt := invocation.CreatedAt.UTC()
	projection := InvocationProjection{
		InvocationID:        invocation.ID,
		Provider:            InvocationProviderAgentTeams,
		Status:              status,
		WorkspaceRef:        invocation.WorkspaceRef,
		ContextSliceRef:     invocation.ContextSliceRef,
		TaskMemoryBufferRef: invocation.TaskMemoryBufferRef,
	}
	if !createdAt.IsZero() {
		projection.StartedAt = timePointer(createdAt)
	}
	return projection
}

func projectSubscriptions(records []contextgraph.SubscriptionInspection, invocation runtime.Invocation) []SubscriptionProjection {
	out := make([]SubscriptionProjection, 0, len(records))
	for _, record := range records {
		if record.ConsumerInvocationID != string(invocation.ID) {
			continue
		}
		out = append(out, SubscriptionProjection{
			SubscriptionID: record.ID,
			SubgraphIDs:    sortedUnique(record.SubgraphIDs),
			Active:         record.Active && invocationIsActive(invocation),
			Source:         record.Source,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SubscriptionID < out[j].SubscriptionID })
	return out
}

func projectContextSlice(inspection ContextInspection, invocation runtime.Invocation, bodiesAllowed bool) ContextSliceView {
	view := ContextSliceView{
		ContextSliceRef: invocation.ContextSliceRef,
		Revision:        revisionString(inspection.Slice.GraphRevision),
		Nodes:           []ContextNodeView{},
		Frontier:        sortedUnique(inspection.Frontier),
		Omitted:         append([]OmittedContext{}, inspection.Omitted...),
	}
	if !bodiesAllowed {
		if len(inspection.Slice.Nodes) > 0 {
			view.Omitted = appendOmission(view.Omitted, "forbidden", len(inspection.Slice.Nodes))
		}
		return view
	}
	for _, node := range inspection.Slice.Nodes {
		view.Nodes = append(view.Nodes, ContextNodeView{
			NodeID:      node.ID,
			Kind:        node.Kind,
			Statement:   node.Statement,
			Status:      node.Status,
			SourceRefs:  append([]string(nil), node.SourceRefs...),
			SubgraphIDs: sortedUnique(node.SubgraphIDs),
		})
	}
	return view
}

func projectTaskMemory(records []CandidateInspectionRecord, invocation runtime.Invocation, bodiesAllowed bool) TaskMemoryBufferView {
	view := TaskMemoryBufferView{
		TaskMemoryBufferRef: invocation.TaskMemoryBufferRef,
		Candidates:          []contextgraph.TaskMemoryCandidateView{},
	}
	matched := make([]contextgraph.TaskMemoryCandidateView, 0, len(records))
	for _, record := range records {
		if record.ProjectID != invocation.ProjectID || record.TaskID != invocation.TaskID || record.CreatedByInvocationID != invocation.ID {
			continue
		}
		matched = append(matched, cloneCandidateView(record.View))
	}
	sort.Slice(matched, func(i, j int) bool { return matched[i].CandidateID < matched[j].CandidateID })
	if !bodiesAllowed {
		if len(matched) > 0 {
			view.Omitted = appendOmission(view.Omitted, "forbidden", len(matched))
		}
		return view
	}
	view.Candidates = matched
	return view
}

func cloneCandidateView(view contextgraph.TaskMemoryCandidateView) contextgraph.TaskMemoryCandidateView {
	view.Candidate.SourceRefs = append([]string(nil), view.Candidate.SourceRefs...)
	view.Candidate.SubgraphIDs = append([]string(nil), view.Candidate.SubgraphIDs...)
	return view
}

func appendOmission(omitted []OmittedContext, reason string, count int) []OmittedContext {
	for i := range omitted {
		if omitted[i].Reason == reason {
			omitted[i].Count += count
			return omitted
		}
	}
	return append(omitted, OmittedContext{Reason: reason, Count: count})
}

func sortedUnique(values []string) []string {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value != "" {
			set[value] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for value := range set {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func timePointer(value time.Time) *time.Time { return &value }
