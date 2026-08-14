package contextgraph

import (
	"context"
	"database/sql"
	"encoding/json"
	"sort"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

func (s *PostgresStore) ProjectContextSnapshot(ctx context.Context, projectID kernel.ProjectID) (ContextGraphSnapshot, error) {
	if err := kernel.RequireID("project_id", projectID); err != nil {
		return ContextGraphSnapshot{}, err
	}
	tx, err := s.begin(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ContextGraphSnapshot{}, err
	}
	defer tx.Rollback()

	revision, err := graphRevision(ctx, tx, projectID, false)
	if err != nil {
		return ContextGraphSnapshot{}, err
	}
	usage, err := contextSnapshotUsageSQL(ctx, tx, projectID)
	if err != nil {
		return ContextGraphSnapshot{}, err
	}
	nodes, nodeSet, err := contextSnapshotNodesSQL(ctx, tx, projectID, usage)
	if err != nil {
		return ContextGraphSnapshot{}, err
	}
	subgraphs, subgraphSet, err := contextSnapshotSubgraphsSQL(ctx, tx, projectID)
	if err != nil {
		return ContextGraphSnapshot{}, err
	}
	edges, err := contextSnapshotEdgesSQL(ctx, tx, nodeSet, subgraphSet)
	if err != nil {
		return ContextGraphSnapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return ContextGraphSnapshot{}, mapContextPostgresError(err)
	}
	return ContextGraphSnapshot{
		ProjectID: projectID,
		Revision:  revision,
		Nodes:     nonNilSnapshotNodes(nodes),
		Edges:     nonNilSnapshotEdges(edges),
		Subgraphs: nonNilSnapshotSubgraphs(subgraphs),
	}, nil
}

func contextSnapshotUsageSQL(ctx context.Context, q postgresDBTX, projectID kernel.ProjectID) (map[string]contextNodeUsage, error) {
	rows, err := q.QueryContext(ctx, `
SELECT node_id, count(*)::int, max(created_at)
FROM (
  SELECT DISTINCT consumer_invocation_id, created_at, node->>'id' AS node_id
  FROM context_invocation_initial_slices
  CROSS JOIN LATERAL jsonb_array_elements(context_slice -> 'nodes') AS node
  WHERE project_id = $1 AND COALESCE(node->>'id', '') <> ''
) used
GROUP BY node_id`, projectID)
	if err != nil {
		return nil, mapContextPostgresError(err)
	}
	defer rows.Close()
	usage := map[string]contextNodeUsage{}
	for rows.Next() {
		var nodeID string
		var count int
		var last time.Time
		if err := rows.Scan(&nodeID, &count, &last); err != nil {
			return nil, mapContextPostgresError(err)
		}
		last = last.UTC()
		usage[nodeID] = contextNodeUsage{Count: count, LastUsedAt: &last}
	}
	return usage, mapContextPostgresError(rows.Err())
}

func contextSnapshotNodesSQL(ctx context.Context, q postgresDBTX, projectID kernel.ProjectID, usage map[string]contextNodeUsage) ([]ContextSnapshotNode, map[string]struct{}, error) {
	rows, err := q.QueryContext(ctx, `
SELECT n.id, n.kind, n.statement, n.status,
       COALESCE(jsonb_agg(m.subgraph_id ORDER BY m.subgraph_id) FILTER (WHERE m.subgraph_id IS NOT NULL), '[]'::jsonb)::text,
       n.source_refs::text
FROM context_nodes n
LEFT JOIN context_node_subgraph_memberships m ON m.node_id = n.id
WHERE n.project_id = $1
GROUP BY n.id, n.kind, n.statement, n.status, n.source_refs, n.created_sequence
ORDER BY n.created_sequence, n.id`, projectID)
	if err != nil {
		return nil, nil, mapContextPostgresError(err)
	}
	defer rows.Close()
	var nodes []ContextSnapshotNode
	nodeSet := map[string]struct{}{}
	for rows.Next() {
		var node ContextNode
		var subgraphsRaw, refsRaw string
		if err := rows.Scan(&node.ID, &node.Kind, &node.Statement, &node.Status, &subgraphsRaw, &refsRaw); err != nil {
			return nil, nil, mapContextPostgresError(err)
		}
		if err := json.Unmarshal([]byte(subgraphsRaw), &node.SubgraphIDs); err != nil {
			return nil, nil, err
		}
		if err := json.Unmarshal([]byte(refsRaw), &node.SourceRefs); err != nil {
			return nil, nil, err
		}
		nodes = append(nodes, snapshotNode(node, usage[node.ID]))
		nodeSet[node.ID] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, mapContextPostgresError(err)
	}
	return nodes, nodeSet, nil
}

func contextSnapshotSubgraphsSQL(ctx context.Context, q postgresDBTX, projectID kernel.ProjectID) ([]ContextSnapshotSubgraph, map[string]struct{}, error) {
	rows, err := q.QueryContext(ctx, `SELECT id, COALESCE(task_id, ''), name, summary, revision, kind, created_at, updated_at FROM context_subgraphs WHERE project_id = $1 ORDER BY id`, projectID)
	if err != nil {
		return nil, nil, mapContextPostgresError(err)
	}
	defer rows.Close()
	var subgraphs []ContextSnapshotSubgraph
	subgraphSet := map[string]struct{}{}
	for rows.Next() {
		var record SubgraphRecord
		record.ProjectID = projectID
		if err := rows.Scan(&record.Subgraph.ID, &record.TaskID, &record.Subgraph.Name, &record.Subgraph.Summary, &record.Subgraph.Revision, &record.Subgraph.Kind, &record.CreatedAt, &record.UpdatedAt); err != nil {
			return nil, nil, mapContextPostgresError(err)
		}
		subgraphs = append(subgraphs, snapshotSubgraph(record))
		subgraphSet[record.Subgraph.ID] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, mapContextPostgresError(err)
	}
	sort.Slice(subgraphs, func(i, j int) bool { return subgraphs[i].SubgraphID < subgraphs[j].SubgraphID })
	return subgraphs, subgraphSet, nil
}

func contextSnapshotEdgesSQL(ctx context.Context, q postgresDBTX, nodeSet, subgraphSet map[string]struct{}) ([]ContextSnapshotEdge, error) {
	rows, err := q.QueryContext(ctx, `SELECT from_ref, to_node_id, kind FROM context_edges ORDER BY from_ref, to_node_id, kind`)
	if err != nil {
		return nil, mapContextPostgresError(err)
	}
	defer rows.Close()
	var edges []ContextSnapshotEdge
	for rows.Next() {
		var edge ContextSnapshotEdge
		if err := rows.Scan(&edge.FromRef, &edge.ToNodeID, &edge.Kind); err != nil {
			return nil, mapContextPostgresError(err)
		}
		if _, ok := nodeSet[edge.ToNodeID]; !ok {
			continue
		}
		if !snapshotFromRefVisible(edge.FromRef, nodeSet, subgraphSet) {
			continue
		}
		edges = append(edges, edge)
	}
	if err := rows.Err(); err != nil {
		return nil, mapContextPostgresError(err)
	}
	return edges, nil
}
