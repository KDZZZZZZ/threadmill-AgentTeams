package contextgraph

import (
	"context"
	"sort"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

func (s *MemoryStore) ProjectContextSnapshot(ctx context.Context, projectID kernel.ProjectID) (ContextGraphSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return ContextGraphSnapshot{}, err
	}
	if err := kernel.RequireID("project_id", projectID); err != nil {
		return ContextGraphSnapshot{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	usage := make(map[string]contextNodeUsage)
	for key, slice := range s.initialSlices {
		if key.ProjectID != projectID {
			continue
		}
		seen := map[string]struct{}{}
		for _, node := range slice.Nodes {
			if node.ID == "" {
				continue
			}
			seen[node.ID] = struct{}{}
		}
		for nodeID := range seen {
			item := usage[nodeID]
			item.Count++
			if item.LastUsedAt == nil {
				item.LastUsedAt = new(time.Time)
			}
			// Memory initial slices do not persist their own timestamp. Use the
			// store clock as the best in-memory equivalent for tests/fakehost.
			*item.LastUsedAt = s.now().UTC()
			usage[nodeID] = item
		}
	}

	nodes := make([]ContextSnapshotNode, 0, len(s.nodes))
	for _, record := range s.sortedNodesLocked() {
		if record.ProjectID != projectID {
			continue
		}
		nodes = append(nodes, snapshotNode(record.Node, usage[record.Node.ID]))
	}

	visibleNodes := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		visibleNodes[node.NodeID] = struct{}{}
	}
	visibleSubgraphs := make(map[string]struct{})
	subgraphs := make([]ContextSnapshotSubgraph, 0, len(s.subgraphs))
	for _, record := range s.subgraphs {
		if record.ProjectID != projectID {
			continue
		}
		visibleSubgraphs[record.Subgraph.ID] = struct{}{}
		subgraphs = append(subgraphs, snapshotSubgraph(record))
	}
	sort.Slice(subgraphs, func(i, j int) bool { return subgraphs[i].SubgraphID < subgraphs[j].SubgraphID })

	edges := make([]ContextSnapshotEdge, 0, len(s.edges))
	for _, edge := range s.sortedEdgesLocked() {
		if _, ok := visibleNodes[edge.ToNodeID]; !ok {
			continue
		}
		if !snapshotFromRefVisible(edge.FromRef, visibleNodes, visibleSubgraphs) {
			continue
		}
		edges = append(edges, ContextSnapshotEdge{FromRef: edge.FromRef, ToNodeID: edge.ToNodeID, Kind: edge.Kind})
	}

	return ContextGraphSnapshot{
		ProjectID: projectID,
		Revision:  s.graphRevision,
		Nodes:     nonNilSnapshotNodes(nodes),
		Edges:     nonNilSnapshotEdges(edges),
		Subgraphs: nonNilSnapshotSubgraphs(subgraphs),
	}, nil
}

type contextNodeUsage struct {
	Count      int
	LastUsedAt *time.Time
}

func snapshotNode(node ContextNode, usage contextNodeUsage) ContextSnapshotNode {
	var lastUsed *time.Time
	if usage.LastUsedAt != nil {
		t := usage.LastUsedAt.UTC()
		lastUsed = &t
	}
	return ContextSnapshotNode{
		NodeID:      node.ID,
		Kind:        node.Kind,
		Statement:   node.Statement,
		Status:      node.Status,
		SourceRefs:  append([]string(nil), node.SourceRefs...),
		SubgraphIDs: append([]string(nil), node.SubgraphIDs...),
		UsageCount:  usage.Count,
		LastUsedAt:  lastUsed,
	}
}

func snapshotSubgraph(record SubgraphRecord) ContextSnapshotSubgraph {
	return ContextSnapshotSubgraph{
		SubgraphID: record.Subgraph.ID,
		Name:       record.Subgraph.Name,
		Summary:    record.Subgraph.Summary,
		Revision:   record.Subgraph.Revision,
		Kind:       record.Subgraph.Kind,
		TaskID:     record.TaskID,
	}
}

func snapshotFromRefVisible(fromRef string, nodes, subgraphs map[string]struct{}) bool {
	if len(fromRef) > 5 && fromRef[:5] == "node:" {
		_, ok := nodes[fromRef[5:]]
		return ok
	}
	if len(fromRef) > 9 && fromRef[:9] == "subgraph:" {
		_, ok := subgraphs[fromRef[9:]]
		return ok
	}
	return false
}

func nonNilSnapshotNodes(in []ContextSnapshotNode) []ContextSnapshotNode {
	if in == nil {
		return []ContextSnapshotNode{}
	}
	return in
}

func nonNilSnapshotEdges(in []ContextSnapshotEdge) []ContextSnapshotEdge {
	if in == nil {
		return []ContextSnapshotEdge{}
	}
	return in
}

func nonNilSnapshotSubgraphs(in []ContextSnapshotSubgraph) []ContextSnapshotSubgraph {
	if in == nil {
		return []ContextSnapshotSubgraph{}
	}
	return in
}
