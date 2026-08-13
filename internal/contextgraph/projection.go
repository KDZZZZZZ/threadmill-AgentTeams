package contextgraph

import (
	"context"
	"fmt"
	"strconv"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

type TaskContextWriter interface {
	RegisterTaskSubgraph(context.Context, auth.Principal, kernel.TaskID) (TaskContextSubgraphBinding, error)
	ProjectTaskContext(context.Context, auth.Principal, ProjectTaskContextRequest) (ContextNodeRef, error)
}

func (s *MemoryStore) ProjectTaskContext(ctx context.Context, principal auth.Principal, req ProjectTaskContextRequest) (ContextNodeRef, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := requireTool(principal, auth.ToolContextProjectTaskContext, principal.ProjectID); err != nil {
		return "", err
	}
	projection := req.Projection
	if err := validateTaskProjectionShape(projection); err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	working := s.cloneLocked()
	if err := validateProjectionBindings(ctx, principal, s.taskResolver, working, projection); err != nil {
		return "", err
	}
	now := s.now().UTC()
	projectionKey := scopedProjection(principal.ProjectID, projection.ProjectionID)
	if existingNodeID, ok := working.projections[projectionKey]; ok {
		record := working.nodes[existingNodeID]
		if record.ProjectID != principal.ProjectID {
			return "", kernel.Error{Code: kernel.CodeNotFound, Message: "projection not found"}
		}
		currentRevision := firstSourceRevision(record.Node.SourceRefs)
		cmp, err := compareSourceRevision(projection.SourceRevision, currentRevision)
		if err != nil || cmp < 0 {
			return "", kernel.Error{Code: kernel.CodeRevisionConflict, Message: "projection source_revision is stale or not comparable"}
		}
		if cmp == 0 {
			return ContextNodeRef(existingNodeID), nil
		}
		oldSubgraphs := append([]string(nil), record.Node.SubgraphIDs...)
		record.Node.Statement = projection.Statement
		record.Node.Kind = projection.Kind
		record.Node.SourceRefs = projectionSourceRefs(projection)
		record.Node.SubgraphIDs = uniqueStrings(projection.SubgraphIDs)
		record.Node.Status = string(NodeStatusAccepted)
		record.Revision++
		working.nodes[existingNodeID] = record
		working.recipients[existingNodeID] = cloneRecipients(projection.Recipients)
		for _, subgraphID := range changedSubgraphMemberships(oldSubgraphs, record.Node.SubgraphIDs) {
			subgraph := working.subgraphs[subgraphID]
			subgraph.Subgraph.Revision++
			subgraph.UpdatedAt = now
			working.subgraphs[subgraphID] = subgraph
		}
		if err := appendMutationEvents(working, now, principal, "context.task_projection.update", "context.task_projection.updated", existingNodeID, record.Node); err != nil {
			return "", err
		}
		working.graphRevision++
		working.appendContextDeltas(principal, lastOutboxID(working), "context.task_projection.updated", unionStringSlices(oldSubgraphs, record.Node.SubgraphIDs))
		s.replaceLocked(working)
		return ContextNodeRef(existingNodeID), nil
	}
	working.nextSeq++
	nodeID := fmt.Sprintf("task-node-%d", working.nextSeq)
	node := normalizeNode(ContextNode{
		ID:             nodeID,
		Kind:           projection.Kind,
		Statement:      projection.Statement,
		Status:         string(NodeStatusAccepted),
		SubgraphIDs:    projection.SubgraphIDs,
		SourceRefs:     projectionSourceRefs(projection),
		CreatorAgentID: string(principal.ActorPrincipalID),
	})
	working.nodes[nodeID] = NodeRecord{Node: node, Revision: 1, ProjectID: principal.ProjectID, CreatedAt: now, Sequence: working.nextSeq}
	working.projections[projectionKey] = nodeID
	working.recipients[nodeID] = cloneRecipients(projection.Recipients)
	for _, subgraphID := range node.SubgraphIDs {
		subgraph := working.subgraphs[subgraphID]
		subgraph.Subgraph.Revision++
		subgraph.UpdatedAt = now
		working.subgraphs[subgraphID] = subgraph
	}
	if err := appendMutationEvents(working, now, principal, "context.task_projection.create", "context.task_projection.created", nodeID, node); err != nil {
		return "", err
	}
	working.graphRevision++
	working.appendContextDeltas(principal, lastOutboxID(working), "context.task_projection.created", node.SubgraphIDs)
	s.replaceLocked(working)
	return ContextNodeRef(nodeID), nil
}

func validateTaskProjectionShape(projection TaskContextProjection) error {
	if projection.ProjectionID == "" || projection.SourceRevision == "" || projection.Statement == "" {
		return kernel.InvalidArgument("projection_id, source_revision, and statement are required")
	}
	if err := validateNodeKind(projection.Kind); err != nil {
		return err
	}
	if len(projection.SourceRefs) == 0 || len(projection.SubgraphIDs) == 0 || len(projection.Recipients) == 0 {
		return kernel.InvalidArgument("source_refs, subgraph_ids, and recipients are required")
	}
	return nil
}

func validateProjectionBindings(ctx context.Context, principal auth.Principal, resolver TaskEndpointResolver, working *memoryData, projection TaskContextProjection) error {
	if resolver == nil {
		return kernel.InvalidArgument("task endpoint resolver is required")
	}
	subgraphSet := map[string]struct{}{}
	for _, subgraphID := range uniqueStrings(projection.SubgraphIDs) {
		record, ok := working.subgraphs[subgraphID]
		if !ok || record.ProjectID != principal.ProjectID || record.Subgraph.Kind != string(SubgraphKindTask) {
			return kernel.Error{Code: kernel.CodeNotFound, Message: "task subgraph not found"}
		}
		subgraphSet[subgraphID] = struct{}{}
	}
	recipientTaskSet := map[string]struct{}{}
	for _, recipient := range projection.Recipients {
		if recipient.TaskID == "" {
			return kernel.InvalidArgument("recipient task_id is required")
		}
		taskID := kernel.TaskID(recipient.TaskID)
		taskExists, err := resolver.TaskExists(ctx, principal.ProjectID, taskID)
		if err != nil {
			return err
		}
		if !taskExists {
			return kernel.Error{Code: kernel.CodeNotFound, Message: "task not found"}
		}
		binding, ok := working.taskBindings[scopedTask(principal.ProjectID, taskID)]
		if !ok {
			return kernel.Error{Code: kernel.CodeNotFound, Message: "task binding not found"}
		}
		if _, ok := subgraphSet[binding.SubgraphID]; !ok {
			return kernel.Forbidden("recipient task binding must be included in subgraph_ids")
		}
		for _, endpoint := range recipient.EndpointRefs {
			if endpoint.TaskID != taskID || endpoint.EndpointID == "" {
				return kernel.Forbidden("recipient endpoint_ref must belong to recipient task")
			}
			endpointExists, err := resolver.EndpointExists(ctx, principal.ProjectID, endpoint)
			if err != nil {
				return err
			}
			if !endpointExists {
				return kernel.Error{Code: kernel.CodeNotFound, Message: "endpoint not found"}
			}
		}
		recipientTaskSet[binding.SubgraphID] = struct{}{}
	}
	if len(recipientTaskSet) != len(subgraphSet) {
		return kernel.Forbidden("subgraph_ids must match recipient task bindings")
	}
	return nil
}

func projectionSourceRefs(projection TaskContextProjection) []string {
	refs := append([]string{"projection-revision:" + projection.SourceRevision}, projection.SourceRefs...)
	return uniqueStrings(refs)
}

func firstSourceRevision(sourceRefs []string) string {
	for _, ref := range sourceRefs {
		if len(ref) > len("projection-revision:") && ref[:len("projection-revision:")] == "projection-revision:" {
			return ref[len("projection-revision:"):]
		}
	}
	return ""
}

func compareSourceRevision(next, current string) (int, error) {
	nextInt, err := strconv.ParseInt(next, 10, 64)
	if err != nil {
		return 0, err
	}
	currentInt, err := strconv.ParseInt(current, 10, 64)
	if err != nil {
		return 0, err
	}
	switch {
	case nextInt > currentInt:
		return 1, nil
	case nextInt < currentInt:
		return -1, nil
	default:
		return 0, nil
	}
}
