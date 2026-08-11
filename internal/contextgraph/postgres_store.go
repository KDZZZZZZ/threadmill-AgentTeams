package contextgraph

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	"github.com/jackc/pgx/v5/pgconn"
)

type postgresDBTX interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type postgresBeginner interface {
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
}

type PostgresStore struct {
	db           postgresBeginner
	now          func() time.Time
	taskResolver TaskEndpointResolver
}

func NewPostgresStore(db postgresBeginner, now func() time.Time) *PostgresStore {
	if now == nil {
		now = time.Now
	}
	return &PostgresStore{db: db, now: now}
}

func (s *PostgresStore) SetTaskEndpointResolver(resolver TaskEndpointResolver) {
	s.taskResolver = resolver
}

func (s *PostgresStore) GraphRevision(ctx context.Context, projectID kernel.ProjectID) (kernel.Revision, error) {
	tx, err := s.begin(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	revision, err := graphRevision(ctx, tx, projectID, false)
	if err != nil {
		return 0, err
	}
	return revision, mapContextPostgresError(tx.Commit())
}

func (s *PostgresStore) CreateNodes(ctx context.Context, principal auth.Principal, req CreateNodesRequest) (CreateNodesResult, error) {
	if err := ctx.Err(); err != nil {
		return CreateNodesResult{}, err
	}
	if err := requireTool(principal, auth.ToolContextSubmitReview, principal.ProjectID); err != nil {
		return CreateNodesResult{}, err
	}
	tx, err := s.begin(ctx, serializableContextTx())
	if err != nil {
		return CreateNodesResult{}, err
	}
	defer tx.Rollback()
	currentRevision, err := graphRevision(ctx, tx, principal.ProjectID, true)
	if err != nil {
		return CreateNodesResult{}, err
	}
	if err := kernel.CheckExpectedRevision(req.ExpectedGraphRevision, currentRevision); err != nil {
		return CreateNodesResult{}, err
	}
	now := s.now().UTC()
	var result CreateNodesResult
	changed := map[string]struct{}{}
	for _, input := range req.Nodes {
		node := normalizeNode(input.Node)
		if node.ID == "" {
			node.ID = newContextID("node", principal.ProjectID)
		} else {
			node.ID = scopedContextID("node", principal.ProjectID, node.ID)
		}
		if input.CreationContext.PreviousNodeID != "" {
			input.CreationContext.PreviousNodeID = scopedContextID("node", principal.ProjectID, input.CreationContext.PreviousNodeID)
		}
		if err := validateNodeKind(node.Kind); err != nil {
			return CreateNodesResult{}, err
		}
		if err := validateNodeStatus(node.Status); err != nil {
			return CreateNodesResult{}, err
		}
		if err := validateNodeFields(node); err != nil {
			return CreateNodesResult{}, err
		}
		if node.CreatorAgentID != input.CreationContext.CreatorAgentID && input.CreationContext.CreatorAgentID != "" {
			return CreateNodesResult{}, kernel.Forbidden("node creator does not match trusted creation context")
		}
		if err := validateWritableGeneralSubgraphs(ctx, tx, principal, node.SubgraphIDs); err != nil {
			return CreateNodesResult{}, err
		}
		sequence, err := insertContextNode(ctx, tx, principal.ProjectID, node, 1, now)
		if err != nil {
			return CreateNodesResult{}, err
		}
		_ = sequence
		for _, subgraphID := range node.SubgraphIDs {
			if err := addMembership(ctx, tx, node.ID, subgraphID); err != nil {
				return CreateNodesResult{}, err
			}
			if err := bumpSubgraphRevision(ctx, tx, principal.ProjectID, subgraphID, now); err != nil {
				return CreateNodesResult{}, err
			}
			changed[subgraphID] = struct{}{}
		}
		edges, err := insertCreationEdges(ctx, tx, principal, input.CreationContext, node.ID, now)
		if err != nil {
			return CreateNodesResult{}, err
		}
		audit, outbox, err := appendMutationEventsSQL(ctx, tx, principal, "context.node.create", "context.node.created", node.ID, node, now)
		if err != nil {
			return CreateNodesResult{}, err
		}
		result.Nodes = append(result.Nodes, cloneNode(node))
		result.Edges = append(result.Edges, edges...)
		result.AuditEvents = append(result.AuditEvents, audit)
		result.OutboxEvents = append(result.OutboxEvents, outbox)
	}
	result.GraphRevision, err = bumpGraphRevision(ctx, tx, principal.ProjectID, now)
	if err != nil {
		return CreateNodesResult{}, err
	}
	if len(result.OutboxEvents) > 0 {
		if err := appendContextDeltasSQL(ctx, tx, principal.ProjectID, result.OutboxEvents[len(result.OutboxEvents)-1].ID, "context.node.created", sortedStringSet(changed), result.GraphRevision); err != nil {
			return CreateNodesResult{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return CreateNodesResult{}, mapContextPostgresError(err)
	}
	return result, nil
}

func (s *PostgresStore) ListSubgraphs(ctx context.Context, principal auth.Principal, req ListSubgraphsRequest) ([]ContextSubgraph, error) {
	if err := requireTool(principal, auth.ToolContextListSubgraphs, principal.ProjectID); err != nil {
		return nil, err
	}
	tx, err := s.begin(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	query := `SELECT id, name, summary, revision, kind FROM context_subgraphs WHERE project_id = $1 AND (kind = 'general' OR task_id = NULLIF($2, ''))`
	args := []any{principal.ProjectID, principal.TaskID}
	if req.Filter != "" {
		query += ` AND kind = $3`
		args = append(args, req.Filter)
	}
	query += ` ORDER BY id`
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, mapContextPostgresError(err)
	}
	defer rows.Close()
	var out []ContextSubgraph
	for rows.Next() {
		var sg ContextSubgraph
		if err := rows.Scan(&sg.ID, &sg.Name, &sg.Summary, &sg.Revision, &sg.Kind); err != nil {
			return nil, mapContextPostgresError(err)
		}
		out = append(out, sg)
	}
	if err := rows.Err(); err != nil {
		return nil, mapContextPostgresError(err)
	}
	return out, mapContextPostgresError(tx.Commit())
}

func (s *PostgresStore) Explore(ctx context.Context, principal auth.Principal, req ExploreRequest) (ContextSliceDelta, error) {
	if err := requireTool(principal, auth.ToolContextExplore, principal.ProjectID); err != nil {
		return ContextSliceDelta{}, err
	}
	tx, err := s.begin(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ContextSliceDelta{}, err
	}
	defer tx.Rollback()
	revision, err := graphRevision(ctx, tx, principal.ProjectID, false)
	if err != nil {
		return ContextSliceDelta{}, err
	}
	out := ContextSliceDelta{GraphRevision: int64(revision)}
	kind, id, err := parseAnchorRef(req.AnchorRef)
	if err != nil {
		return ContextSliceDelta{}, err
	}
	if id == "" {
		return out, mapContextPostgresError(tx.Commit())
	}
	switch kind {
	case "subgraph":
		if ok, err := canSeeSubgraphSQL(ctx, tx, principal, id); err != nil || !ok {
			if err != nil {
				return ContextSliceDelta{}, err
			}
			return ContextSliceDelta{}, kernel.Error{Code: kernel.CodeNotFound, Message: "subgraph not found"}
		}
		nodes, err := nodesInSubgraphsSQL(ctx, tx, principal, []string{id})
		if err != nil {
			return ContextSliceDelta{}, err
		}
		out.Nodes = nodes
		for _, node := range nodes {
			out.Frontier = append(out.Frontier, "node:"+node.ID)
		}
	case "node":
		node, err := visibleNodeSQL(ctx, tx, principal, id)
		if err != nil {
			return ContextSliceDelta{}, err
		}
		out.Nodes = []ContextNode{node}
	}
	sort.Strings(out.Frontier)
	return out, mapContextPostgresError(tx.Commit())
}

func (s *PostgresStore) Search(ctx context.Context, principal auth.Principal, req SearchRequest) (ContextSearchResult, error) {
	if principal.Role != auth.RoleContext {
		return ContextSearchResult{}, kernel.Forbidden("search is exposed only to Context Agent")
	}
	if err := requireTool(principal, auth.ToolContextSearch, principal.ProjectID); err != nil {
		return ContextSearchResult{}, err
	}
	tx, err := s.begin(ctx, serializableContextTx())
	if err != nil {
		return ContextSearchResult{}, err
	}
	defer tx.Rollback()
	scope, err := visibleScopeSQL(ctx, tx, principal, req.Scope)
	if err != nil {
		return ContextSearchResult{}, err
	}
	anchored, err := visibleAnchorNodesSQL(ctx, tx, principal, req.AnchorRefs)
	if err != nil {
		return ContextSearchResult{}, err
	}
	nodes, err := searchNodesSQL(ctx, tx, principal, lowerAll(req.Keywords), scope)
	if err != nil {
		return ContextSearchResult{}, err
	}
	if len(anchored) > 0 {
		filtered := nodes[:0]
		for _, node := range nodes {
			if _, ok := anchored[node.ID]; ok {
				filtered = append(filtered, node)
			}
		}
		nodes = append([]ContextNode(nil), filtered...)
	}
	hitSubgraphs := map[string]struct{}{}
	for _, node := range nodes {
		for _, subgraphID := range node.SubgraphIDs {
			hitSubgraphs[subgraphID] = struct{}{}
		}
	}
	var sub ContextSubscription
	if len(hitSubgraphs) > 0 {
		sub, err = createSubscriptionSQL(ctx, tx, principal, SubscribeRequest{SubgraphIDs: sortedStringSet(hitSubgraphs)}, subscriptionSourceSearch, s.now().UTC())
		if err != nil {
			return ContextSearchResult{}, err
		}
	}
	revision, err := graphRevision(ctx, tx, principal.ProjectID, false)
	if err != nil {
		return ContextSearchResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return ContextSearchResult{}, mapContextPostgresError(err)
	}
	out := ContextSearchResult{Slice: ContextSliceDelta{Nodes: nodes, GraphRevision: int64(revision)}, MatchedKeywords: lowerAll(req.Keywords)}
	if sub.ID != "" {
		out.SubscriptionIDs = []string{sub.ID}
	}
	return out, nil
}

func (s *PostgresStore) Subscribe(ctx context.Context, principal auth.Principal, req SubscribeRequest) (ContextSubscription, error) {
	if err := requireTool(principal, auth.ToolContextSubscribe, principal.ProjectID); err != nil {
		return ContextSubscription{}, err
	}
	tx, err := s.begin(ctx, serializableContextTx())
	if err != nil {
		return ContextSubscription{}, err
	}
	defer tx.Rollback()
	sub, err := createSubscriptionSQL(ctx, tx, principal, req, subscriptionSourceExplicit, s.now().UTC())
	if err != nil {
		return ContextSubscription{}, err
	}
	return sub, mapContextPostgresError(tx.Commit())
}

func (s *PostgresStore) Unsubscribe(ctx context.Context, principal auth.Principal, subscriptionID string) error {
	if err := requireTool(principal, auth.ToolContextUnsubscribe, principal.ProjectID); err != nil {
		return err
	}
	if strings.TrimSpace(subscriptionID) == "" {
		return kernel.InvalidArgument("subscription_id is required")
	}
	tx, err := s.begin(ctx, serializableContextTx())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE context_subscriptions SET active = false, canceled_at = COALESCE(canceled_at, $4) WHERE id = $1 AND project_id = $2 AND consumer_invocation_id = $3`, subscriptionID, principal.ProjectID, consumerInvocationID(principal), s.now().UTC())
	if err != nil {
		return mapContextPostgresError(err)
	}
	if affected(result) == 0 {
		return subscriptionNotFound()
	}
	if _, err := appendAuditSQL(ctx, tx, principal, "context.subscription.cancel", subscriptionID, nil, s.now().UTC()); err != nil {
		return err
	}
	return mapContextPostgresError(tx.Commit())
}

func (s *PostgresStore) EndInvocation(ctx context.Context, principal auth.Principal, invocationID kernel.InvocationID) error {
	if invocationID == "" {
		return kernel.InvalidArgument("invocation_id is required")
	}
	if principal.ProjectID == "" || principal.Kind != auth.PrincipalAgent {
		return kernel.Forbidden("invocation expiry requires agent principal")
	}
	if invocationID != principal.InvocationID {
		return kernel.Forbidden("invocation expiry is limited to the authenticated invocation")
	}
	tx, err := s.begin(ctx, serializableContextTx())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `UPDATE context_subscriptions SET active = false, expired_at = COALESCE(expired_at, $3) WHERE project_id = $1 AND consumer_invocation_id = $2 AND active`, principal.ProjectID, invocationID, s.now().UTC())
	if err != nil {
		return mapContextPostgresError(err)
	}
	return mapContextPostgresError(tx.Commit())
}

func (s *PostgresStore) InspectSubscriptions(ctx context.Context, principal auth.Principal, invocationID kernel.InvocationID) ([]SubscriptionInspection, error) {
	if principal.ProjectID == "" {
		return nil, kernel.Forbidden("inspection requires project principal")
	}
	if invocationID == "" {
		invocationID = consumerInvocationID(principal)
	}
	switch principal.Kind {
	case auth.PrincipalAgent:
		if invocationID == "" || invocationID != principal.InvocationID {
			return nil, kernel.Forbidden("agent subscription inspection is limited to the authenticated invocation")
		}
	case auth.PrincipalOperator:
		if principal.Role != auth.RoleOperator || invocationID == "" {
			return nil, kernel.Forbidden("operator subscription inspection requires an invocation")
		}
	default:
		return nil, kernel.Forbidden("subscription inspection requires an authenticated principal")
	}
	tx, err := s.begin(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT id, consumer_invocation_id, source, subgraph_ids::text, event_kinds::text, active FROM context_subscriptions WHERE project_id = $1 AND consumer_invocation_id = $2 ORDER BY id`, principal.ProjectID, invocationID)
	if err != nil {
		return nil, mapContextPostgresError(err)
	}
	defer rows.Close()
	var out []SubscriptionInspection
	for rows.Next() {
		var item SubscriptionInspection
		var subgraphs, events string
		if err := rows.Scan(&item.ID, &item.ConsumerInvocationID, &item.Source, &subgraphs, &events, &item.Active); err != nil {
			return nil, mapContextPostgresError(err)
		}
		if err := json.Unmarshal([]byte(subgraphs), &item.SubgraphIDs); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(events), &item.EventKinds); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, mapContextPostgresError(err)
	}
	return out, mapContextPostgresError(tx.Commit())
}

func (s *PostgresStore) CreateInitialSlice(ctx context.Context, principal auth.Principal, subgraphIDs []string) (ContextSlice, error) {
	return s.EnsureInitialSlice(ctx, principal, subgraphIDs)
}

func (s *PostgresStore) EnsureInitialSlice(ctx context.Context, principal auth.Principal, subgraphIDs []string) (ContextSlice, error) {
	if err := requireTool(principal, auth.ToolContextSubscribe, principal.ProjectID); err != nil {
		return ContextSlice{}, err
	}
	if consumerInvocationID(principal) == "" {
		return ContextSlice{}, kernel.InvalidArgument("consumer invocation is required")
	}
	tx, err := s.begin(ctx, serializableContextTx())
	if err != nil {
		return ContextSlice{}, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2))`, principal.ProjectID, consumerInvocationID(principal)); err != nil {
		return ContextSlice{}, mapContextPostgresError(err)
	}
	active, err := activeSubscriptionIDsSQL(ctx, tx, principal.ProjectID, consumerInvocationID(principal))
	if err != nil {
		return ContextSlice{}, err
	}
	if len(active) == 0 {
		if _, err := ensureInitialSubscriptionSQL(ctx, tx, principal, SubscribeRequest{SubgraphIDs: subgraphIDs}, s.now().UTC()); err != nil {
			return ContextSlice{}, err
		}
	}
	slice, err := materializeInvocationSliceSQL(ctx, tx, principal)
	if err != nil {
		return ContextSlice{}, err
	}
	return slice, mapContextPostgresError(tx.Commit())
}

func (s *PostgresStore) EffectiveSubgraphs(ctx context.Context, principal auth.Principal) ([]string, error) {
	if consumerInvocationID(principal) == "" {
		return nil, kernel.InvalidArgument("consumer invocation is required")
	}
	tx, err := s.begin(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	out, err := effectiveSubgraphsSQL(ctx, tx, principal)
	if err != nil {
		return nil, err
	}
	return out, mapContextPostgresError(tx.Commit())
}

func (s *PostgresStore) MaterializeRuntimeContext(ctx context.Context, principal auth.Principal) (ContextSlice, error) {
	if consumerInvocationID(principal) == "" {
		return ContextSlice{}, kernel.InvalidArgument("consumer invocation is required")
	}
	tx, err := s.begin(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ContextSlice{}, err
	}
	defer tx.Rollback()
	out, err := materializeInvocationSliceSQL(ctx, tx, principal)
	if err != nil {
		return ContextSlice{}, err
	}
	return out, mapContextPostgresError(tx.Commit())
}

func (s *PostgresStore) GetSubgraph(ctx context.Context, principal auth.Principal, req GetSubgraphRequest) (ContextSubgraph, error) {
	if err := requireContextAgent(principal, auth.ToolContextGetSubgraph, principal.ProjectID); err != nil {
		return ContextSubgraph{}, err
	}
	tx, err := s.begin(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ContextSubgraph{}, err
	}
	defer tx.Rollback()
	sg, err := visibleSubgraphSQL(ctx, tx, principal, req.SubgraphID)
	if err != nil {
		return ContextSubgraph{}, err
	}
	return sg, mapContextPostgresError(tx.Commit())
}

func (s *PostgresStore) GetNode(ctx context.Context, principal auth.Principal, req GetNodeRequest) (ContextNode, error) {
	if err := requireContextAgent(principal, auth.ToolContextGetNode, principal.ProjectID); err != nil {
		return ContextNode{}, err
	}
	tx, err := s.begin(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ContextNode{}, err
	}
	defer tx.Rollback()
	node, err := visibleNodeSQL(ctx, tx, principal, req.NodeID)
	if err != nil {
		return ContextNode{}, err
	}
	return node, mapContextPostgresError(tx.Commit())
}

func (s *PostgresStore) CreateNode(ctx context.Context, principal auth.Principal, req CreateGeneralNodeRequest) (ContextNodeRef, error) {
	if err := requireContextAgent(principal, auth.ToolContextCreateNode, principal.ProjectID); err != nil {
		return "", err
	}
	tx, err := s.begin(ctx, serializableContextTx())
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	now := s.now().UTC()
	node := normalizeNode(ContextNode{
		ID:             newContextID("node", principal.ProjectID),
		Kind:           req.Kind,
		Statement:      req.Statement,
		Status:         string(NodeStatusAccepted),
		SubgraphIDs:    req.SubgraphIDs,
		SourceRefs:     req.SourceRefs,
		CreatorAgentID: string(principal.ActorPrincipalID),
	})
	if err := validateNodeKind(node.Kind); err != nil {
		return "", err
	}
	if err := validateNodeFields(node); err != nil {
		return "", err
	}
	if err := validateWritableGeneralSubgraphs(ctx, tx, principal, node.SubgraphIDs); err != nil {
		return "", err
	}
	if _, err := insertContextNode(ctx, tx, principal.ProjectID, node, 1, now); err != nil {
		return "", err
	}
	for _, subgraphID := range node.SubgraphIDs {
		if err := addMembership(ctx, tx, node.ID, subgraphID); err != nil {
			return "", err
		}
		if err := bumpSubgraphRevision(ctx, tx, principal.ProjectID, subgraphID, now); err != nil {
			return "", err
		}
	}
	_, outbox, err := appendMutationEventsSQL(ctx, tx, principal, "context.node.create", "context.node.created", node.ID, node, now)
	if err != nil {
		return "", err
	}
	revision, err := bumpGraphRevision(ctx, tx, principal.ProjectID, now)
	if err != nil {
		return "", err
	}
	if err := appendContextDeltasSQL(ctx, tx, principal.ProjectID, outbox.ID, outbox.Topic, node.SubgraphIDs, revision); err != nil {
		return "", err
	}
	if err := tx.Commit(); err != nil {
		return "", mapContextPostgresError(err)
	}
	return ContextNodeRef(node.ID), nil
}

func (s *PostgresStore) UpdateNode(ctx context.Context, principal auth.Principal, req UpdateGeneralNodeRequest) (ContextNodeRef, error) {
	if err := requireContextAgent(principal, auth.ToolContextUpdateNode, principal.ProjectID); err != nil {
		return "", err
	}
	expected, err := parseExpectedRevision(req.SourceRevision)
	if err != nil {
		return "", err
	}
	tx, err := s.begin(ctx, serializableContextTx())
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	record, err := nodeRecordSQL(ctx, tx, principal.ProjectID, req.NodeID, true)
	if err != nil {
		return "", err
	}
	if err := rejectTaskMembershipSQL(ctx, tx, record.Node.SubgraphIDs); err != nil {
		return "", err
	}
	if err := kernel.CheckExpectedRevision(expected, record.Revision); err != nil {
		return "", err
	}
	updated := normalizeNode(ContextNode{ID: req.NodeID, Kind: req.Kind, Statement: req.Statement, Status: req.Status, SourceRefs: req.SourceRefs, SubgraphIDs: req.SubgraphIDs, CreatorAgentID: record.Node.CreatorAgentID})
	if err := validateNodeKind(updated.Kind); err != nil {
		return "", err
	}
	if err := validateNodeStatus(updated.Status); err != nil {
		return "", err
	}
	if err := validateNodeFields(updated); err != nil {
		return "", err
	}
	if err := validateWritableGeneralSubgraphs(ctx, tx, principal, updated.SubgraphIDs); err != nil {
		return "", err
	}
	oldSubgraphs := record.Node.SubgraphIDs
	refs, _ := json.Marshal(updated.SourceRefs)
	now := s.now().UTC()
	if _, err := tx.ExecContext(ctx, `UPDATE context_nodes SET kind = $3, statement = $4, status = $5, source_refs = $6::jsonb, revision = revision + 1, updated_at = $7 WHERE project_id = $1 AND id = $2`, principal.ProjectID, req.NodeID, updated.Kind, updated.Statement, updated.Status, string(refs), now); err != nil {
		return "", mapContextPostgresError(err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM context_node_subgraph_memberships WHERE node_id = $1`, req.NodeID); err != nil {
		return "", mapContextPostgresError(err)
	}
	for _, subgraphID := range updated.SubgraphIDs {
		if err := addMembership(ctx, tx, req.NodeID, subgraphID); err != nil {
			return "", err
		}
	}
	changed := changedSubgraphMemberships(oldSubgraphs, updated.SubgraphIDs)
	for _, subgraphID := range changed {
		if err := bumpSubgraphRevision(ctx, tx, principal.ProjectID, subgraphID, now); err != nil {
			return "", err
		}
	}
	_, outbox, err := appendMutationEventsSQL(ctx, tx, principal, "context.node.update", "context.node.updated", req.NodeID, updated, now)
	if err != nil {
		return "", err
	}
	revision, err := bumpGraphRevision(ctx, tx, principal.ProjectID, now)
	if err != nil {
		return "", err
	}
	if err := appendContextDeltasSQL(ctx, tx, principal.ProjectID, outbox.ID, outbox.Topic, changed, revision); err != nil {
		return "", err
	}
	return ContextNodeRef(req.NodeID), mapContextPostgresError(tx.Commit())
}

func (s *PostgresStore) DeleteNode(ctx context.Context, principal auth.Principal, req DeleteGeneralNodeRequest) error {
	if err := requireContextAgent(principal, auth.ToolContextDeleteNode, principal.ProjectID); err != nil {
		return err
	}
	expected, err := parseExpectedRevision(req.SourceRevision)
	if err != nil {
		return err
	}
	tx, err := s.begin(ctx, serializableContextTx())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	record, err := nodeRecordSQL(ctx, tx, principal.ProjectID, req.NodeID, true)
	if err != nil {
		return err
	}
	if err := rejectTaskMembershipSQL(ctx, tx, record.Node.SubgraphIDs); err != nil {
		return err
	}
	if err := kernel.CheckExpectedRevision(expected, record.Revision); err != nil {
		return err
	}
	now := s.now().UTC()
	if _, err := tx.ExecContext(ctx, `DELETE FROM context_edges WHERE to_node_id = $1 OR from_ref = $2`, req.NodeID, "node:"+req.NodeID); err != nil {
		return mapContextPostgresError(err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM context_nodes WHERE project_id = $1 AND id = $2`, principal.ProjectID, req.NodeID); err != nil {
		return mapContextPostgresError(err)
	}
	for _, subgraphID := range record.Node.SubgraphIDs {
		if err := bumpSubgraphRevision(ctx, tx, principal.ProjectID, subgraphID, now); err != nil {
			return err
		}
	}
	_, outbox, err := appendMutationEventsSQL(ctx, tx, principal, "context.node.delete", "context.node.deleted", req.NodeID, req, now)
	if err != nil {
		return err
	}
	revision, err := bumpGraphRevision(ctx, tx, principal.ProjectID, now)
	if err != nil {
		return err
	}
	if err := appendContextDeltasSQL(ctx, tx, principal.ProjectID, outbox.ID, outbox.Topic, record.Node.SubgraphIDs, revision); err != nil {
		return err
	}
	return mapContextPostgresError(tx.Commit())
}

func (s *PostgresStore) CreateSubgraph(ctx context.Context, principal auth.Principal, req CreateGeneralSubgraphRequest) (ContextSubgraph, error) {
	if err := requireContextAgent(principal, auth.ToolContextCreateSubgraph, principal.ProjectID); err != nil {
		return ContextSubgraph{}, err
	}
	if req.Name == "" {
		return ContextSubgraph{}, kernel.InvalidArgument("subgraph name is required")
	}
	tx, err := s.begin(ctx, serializableContextTx())
	if err != nil {
		return ContextSubgraph{}, err
	}
	defer tx.Rollback()
	now := s.now().UTC()
	sg := ContextSubgraph{ID: newContextID("subgraph", principal.ProjectID), Name: req.Name, Summary: req.Summary, Revision: 1, Kind: string(SubgraphKindGeneral)}
	if _, err := tx.ExecContext(ctx, `INSERT INTO context_subgraphs(id, project_id, name, summary, revision, kind, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $7)`, sg.ID, principal.ProjectID, sg.Name, sg.Summary, sg.Revision, sg.Kind, now); err != nil {
		return ContextSubgraph{}, mapContextPostgresError(err)
	}
	if err := setGeneralSubgraphMembersSQL(ctx, tx, principal, sg.ID, req.NodeIDs); err != nil {
		return ContextSubgraph{}, err
	}
	_, outbox, err := appendMutationEventsSQL(ctx, tx, principal, "context.subgraph.create", "context.subgraph.created", sg.ID, sg, now)
	if err != nil {
		return ContextSubgraph{}, err
	}
	revision, err := bumpGraphRevision(ctx, tx, principal.ProjectID, now)
	if err != nil {
		return ContextSubgraph{}, err
	}
	if err := appendContextDeltasSQL(ctx, tx, principal.ProjectID, outbox.ID, outbox.Topic, []string{sg.ID}, revision); err != nil {
		return ContextSubgraph{}, err
	}
	return sg, mapContextPostgresError(tx.Commit())
}

func (s *PostgresStore) UpdateSubgraph(ctx context.Context, principal auth.Principal, req UpdateGeneralSubgraphRequest) (ContextSubgraph, error) {
	if err := requireContextAgent(principal, auth.ToolContextUpdateSubgraph, principal.ProjectID); err != nil {
		return ContextSubgraph{}, err
	}
	tx, err := s.begin(ctx, serializableContextTx())
	if err != nil {
		return ContextSubgraph{}, err
	}
	defer tx.Rollback()
	record, err := subgraphRecordSQL(ctx, tx, principal.ProjectID, req.SubgraphID, true)
	if err != nil {
		return ContextSubgraph{}, err
	}
	if record.Subgraph.Kind != string(SubgraphKindGeneral) {
		return ContextSubgraph{}, kernel.Forbidden("Context Agent curator cannot update task subgraphs")
	}
	if err := kernel.CheckExpectedRevision(kernel.Revision(req.Revision), kernel.Revision(record.Subgraph.Revision)); err != nil {
		return ContextSubgraph{}, err
	}
	now := s.now().UTC()
	next := record.Subgraph
	next.Name = req.Name
	next.Summary = req.Summary
	next.Revision++
	if _, err := tx.ExecContext(ctx, `UPDATE context_subgraphs SET name = $3, summary = $4, revision = revision + 1, updated_at = $5 WHERE project_id = $1 AND id = $2`, principal.ProjectID, req.SubgraphID, req.Name, req.Summary, now); err != nil {
		return ContextSubgraph{}, mapContextPostgresError(err)
	}
	if err := setGeneralSubgraphMembersSQL(ctx, tx, principal, req.SubgraphID, req.NodeIDs); err != nil {
		return ContextSubgraph{}, err
	}
	_, outbox, err := appendMutationEventsSQL(ctx, tx, principal, "context.subgraph.update", "context.subgraph.updated", req.SubgraphID, next, now)
	if err != nil {
		return ContextSubgraph{}, err
	}
	revision, err := bumpGraphRevision(ctx, tx, principal.ProjectID, now)
	if err != nil {
		return ContextSubgraph{}, err
	}
	if err := appendContextDeltasSQL(ctx, tx, principal.ProjectID, outbox.ID, outbox.Topic, []string{req.SubgraphID}, revision); err != nil {
		return ContextSubgraph{}, err
	}
	return next, mapContextPostgresError(tx.Commit())
}

func (s *PostgresStore) DeleteSubgraph(ctx context.Context, principal auth.Principal, req DeleteGeneralSubgraphRequest) error {
	if err := requireContextAgent(principal, auth.ToolContextDeleteSubgraph, principal.ProjectID); err != nil {
		return err
	}
	tx, err := s.begin(ctx, serializableContextTx())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	record, err := subgraphRecordSQL(ctx, tx, principal.ProjectID, req.SubgraphID, true)
	if err != nil {
		return err
	}
	if record.Subgraph.Kind != string(SubgraphKindGeneral) {
		return kernel.Forbidden("Context Agent curator cannot delete task subgraphs")
	}
	if err := kernel.CheckExpectedRevision(kernel.Revision(req.Revision), kernel.Revision(record.Subgraph.Revision)); err != nil {
		return err
	}
	now := s.now().UTC()
	if _, err := tx.ExecContext(ctx, `DELETE FROM context_edges WHERE from_ref = $1`, "subgraph:"+req.SubgraphID); err != nil {
		return mapContextPostgresError(err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM context_subgraphs WHERE project_id = $1 AND id = $2`, principal.ProjectID, req.SubgraphID); err != nil {
		return mapContextPostgresError(err)
	}
	_, outbox, err := appendMutationEventsSQL(ctx, tx, principal, "context.subgraph.delete", "context.subgraph.deleted", req.SubgraphID, req, now)
	if err != nil {
		return err
	}
	revision, err := bumpGraphRevision(ctx, tx, principal.ProjectID, now)
	if err != nil {
		return err
	}
	if err := appendContextDeltasSQL(ctx, tx, principal.ProjectID, outbox.ID, outbox.Topic, []string{req.SubgraphID}, revision); err != nil {
		return err
	}
	return mapContextPostgresError(tx.Commit())
}

func (s *PostgresStore) RecordContextDelta(ctx context.Context, principal auth.Principal, eventID, eventKind string, subgraphIDs []string) ([]ContextDelta, error) {
	if eventID == "" || eventKind == "" {
		return nil, kernel.InvalidArgument("event_id and event_kind are required")
	}
	tx, err := s.begin(ctx, serializableContextTx())
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	revision, err := graphRevision(ctx, tx, principal.ProjectID, false)
	if err != nil {
		return nil, err
	}
	out, err := appendContextDeltasSQLReturning(ctx, tx, principal.ProjectID, eventID, eventKind, subgraphIDs, revision)
	if err != nil {
		return nil, err
	}
	return out, mapContextPostgresError(tx.Commit())
}

func (s *PostgresStore) PendingDeltas(ctx context.Context, principal auth.Principal) ([]ContextDelta, error) {
	tx, err := s.begin(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `
SELECT d.id, d.project_id, d.subscription_id, d.consumer_invocation_id, d.event_id, d.event_kind, d.subgraph_ids::text, d.graph_revision
FROM context_delta_deliveries d
JOIN context_subscriptions s ON s.project_id = d.project_id AND s.id = d.subscription_id
WHERE d.project_id = $1 AND d.consumer_invocation_id = $2 AND d.acked_at IS NULL AND s.active
ORDER BY d.id`, principal.ProjectID, consumerInvocationID(principal))
	if err != nil {
		return nil, mapContextPostgresError(err)
	}
	defer rows.Close()
	var out []ContextDelta
	for rows.Next() {
		var d ContextDelta
		var subgraphs string
		if err := rows.Scan(&d.ID, &d.ProjectID, &d.SubscriptionID, &d.InvocationID, &d.EventID, &d.EventKind, &subgraphs, &d.GraphRevision); err != nil {
			return nil, mapContextPostgresError(err)
		}
		if err := json.Unmarshal([]byte(subgraphs), &d.SubgraphIDs); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		return nil, mapContextPostgresError(err)
	}
	return out, mapContextPostgresError(tx.Commit())
}

func (s *PostgresStore) AckDelta(ctx context.Context, principal auth.Principal, deltaID string) error {
	tx, err := s.begin(ctx, serializableContextTx())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `
UPDATE context_delta_deliveries d SET acked_at = $4
FROM context_subscriptions s
WHERE d.id = $1 AND d.project_id = $2 AND d.consumer_invocation_id = $3
  AND s.project_id = d.project_id AND s.id = d.subscription_id AND s.consumer_invocation_id = d.consumer_invocation_id`,
		deltaID, principal.ProjectID, consumerInvocationID(principal), s.now().UTC())
	if err != nil {
		return mapContextPostgresError(err)
	}
	if affected(result) == 0 {
		return kernel.Error{Code: kernel.CodeNotFound, Message: "delta not found"}
	}
	return mapContextPostgresError(tx.Commit())
}

func (s *PostgresStore) RegisterTaskSubgraph(ctx context.Context, principal auth.Principal, taskID kernel.TaskID) (TaskContextSubgraphBinding, error) {
	if err := requireTool(principal, auth.ToolContextRegisterTaskSubgraph, principal.ProjectID); err != nil {
		return TaskContextSubgraphBinding{}, err
	}
	if taskID == "" {
		taskID = principal.TaskID
	}
	if taskID == "" {
		return TaskContextSubgraphBinding{}, kernel.InvalidArgument("task_id is required")
	}
	if s.taskResolver == nil {
		return TaskContextSubgraphBinding{}, kernel.InvalidArgument("task endpoint resolver is required")
	}
	exists, err := s.taskResolver.TaskExists(ctx, principal.ProjectID, taskID)
	if err != nil {
		return TaskContextSubgraphBinding{}, err
	}
	if !exists {
		return TaskContextSubgraphBinding{}, kernel.Error{Code: kernel.CodeNotFound, Message: "task not found"}
	}
	tx, err := s.begin(ctx, idempotentContextTx())
	if err != nil {
		return TaskContextSubgraphBinding{}, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, fmt.Sprintf("context-task-subgraph:%s:%s", principal.ProjectID, taskID)); err != nil {
		return TaskContextSubgraphBinding{}, mapContextPostgresError(err)
	}
	var existing string
	err = tx.QueryRowContext(ctx, `SELECT subgraph_id FROM context_task_subgraph_bindings WHERE project_id = $1 AND task_id = $2 FOR UPDATE`, principal.ProjectID, taskID).Scan(&existing)
	if err == nil {
		if err := tx.Commit(); err != nil {
			return TaskContextSubgraphBinding{}, mapContextPostgresError(err)
		}
		return TaskContextSubgraphBinding{TaskID: string(taskID), SubgraphID: existing}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return TaskContextSubgraphBinding{}, mapContextPostgresError(err)
	}
	now := s.now().UTC()
	subgraphID := newContextID("task-subgraph", principal.ProjectID)
	if _, err := tx.ExecContext(ctx, `INSERT INTO context_subgraphs(id, project_id, task_id, name, summary, revision, kind, created_at, updated_at) VALUES ($1, $2, $3, $4, 'task context', 1, 'task', $5, $5)`, subgraphID, principal.ProjectID, taskID, taskID, now); err != nil {
		return TaskContextSubgraphBinding{}, mapContextPostgresError(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO context_task_subgraph_bindings(project_id, task_id, subgraph_id) VALUES ($1, $2, $3)`, principal.ProjectID, taskID, subgraphID); err != nil {
		return TaskContextSubgraphBinding{}, mapContextPostgresError(err)
	}
	if _, err := appendAuditSQL(ctx, tx, principal, "context.task_subgraph.register", subgraphID, nil, now); err != nil {
		return TaskContextSubgraphBinding{}, err
	}
	if _, err := bumpGraphRevision(ctx, tx, principal.ProjectID, now); err != nil {
		return TaskContextSubgraphBinding{}, err
	}
	return TaskContextSubgraphBinding{TaskID: string(taskID), SubgraphID: subgraphID}, mapContextPostgresError(tx.Commit())
}

func (s *PostgresStore) ProjectTaskContext(ctx context.Context, principal auth.Principal, req ProjectTaskContextRequest) (ContextNodeRef, error) {
	if err := requireTool(principal, auth.ToolContextProjectTaskContext, principal.ProjectID); err != nil {
		return "", err
	}
	if s.taskResolver == nil {
		return "", kernel.InvalidArgument("task endpoint resolver is required")
	}
	projection := req.Projection
	if err := validateTaskProjectionShape(projection); err != nil {
		return "", err
	}
	tx, err := s.begin(ctx, idempotentContextTx())
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, fmt.Sprintf("context-task-projection:%s:%s", principal.ProjectID, projection.ProjectionID)); err != nil {
		return "", mapContextPostgresError(err)
	}
	if err := validateProjectionBindingsSQL(ctx, tx, principal, s.taskResolver, projection); err != nil {
		return "", err
	}
	now := s.now().UTC()
	var existingNodeID, currentRevision string
	err = tx.QueryRowContext(ctx, `SELECT p.node_id, p.source_revision FROM context_task_projections p WHERE p.project_id = $1 AND p.projection_id = $2 FOR UPDATE`, principal.ProjectID, projection.ProjectionID).Scan(&existingNodeID, &currentRevision)
	if err == nil {
		cmp, err := compareSourceRevision(projection.SourceRevision, currentRevision)
		if err != nil || cmp < 0 {
			return "", kernel.Error{Code: kernel.CodeRevisionConflict, Message: "projection source_revision is stale or not comparable"}
		}
		if cmp == 0 {
			return ContextNodeRef(existingNodeID), mapContextPostgresError(tx.Commit())
		}
		record, err := nodeRecordSQL(ctx, tx, principal.ProjectID, existingNodeID, true)
		if err != nil {
			return "", err
		}
		node := normalizeNode(ContextNode{ID: existingNodeID, Kind: projection.Kind, Statement: projection.Statement, Status: string(NodeStatusAccepted), SourceRefs: projectionSourceRefs(projection), SubgraphIDs: projection.SubgraphIDs, CreatorAgentID: record.Node.CreatorAgentID})
		if err := updateTaskProjectionNodeSQL(ctx, tx, principal, node, projection, record.Node.SubgraphIDs, now); err != nil {
			return "", err
		}
		return ContextNodeRef(existingNodeID), mapContextPostgresError(tx.Commit())
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", mapContextPostgresError(err)
	}
	nodeID := newContextID("task-node", principal.ProjectID)
	node := normalizeNode(ContextNode{ID: nodeID, Kind: projection.Kind, Statement: projection.Statement, Status: string(NodeStatusAccepted), SourceRefs: projectionSourceRefs(projection), SubgraphIDs: projection.SubgraphIDs, CreatorAgentID: string(principal.ActorPrincipalID)})
	if _, err := insertContextNode(ctx, tx, principal.ProjectID, node, 1, now); err != nil {
		return "", err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO context_task_projections(project_id, projection_id, node_id, source_revision, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $5)`, principal.ProjectID, projection.ProjectionID, nodeID, projection.SourceRevision, now); err != nil {
		return "", mapContextPostgresError(err)
	}
	if err := storeTaskProjectionBindingsSQL(ctx, tx, principal, nodeID, projection, nil, now); err != nil {
		return "", err
	}
	return ContextNodeRef(nodeID), mapContextPostgresError(tx.Commit())
}

func (s *PostgresStore) ListTaskCandidates(ctx context.Context, principal auth.Principal) (TaskMemoryBufferView, error) {
	if err := requireTool(principal, auth.ToolAgentListTaskMemoryCandidates, principal.ProjectID); err != nil {
		return TaskMemoryBufferView{}, err
	}
	if principal.TaskID == "" {
		return TaskMemoryBufferView{}, kernel.InvalidArgument("task_id is required")
	}
	tx, err := s.begin(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return TaskMemoryBufferView{}, err
	}
	defer tx.Rollback()
	records, err := candidateRecordsSQL(ctx, tx, principal.ProjectID, principal.TaskID)
	if err != nil {
		return TaskMemoryBufferView{}, err
	}
	return candidateView(records), mapContextPostgresError(tx.Commit())
}

func (s *PostgresStore) SubmitCandidate(ctx context.Context, principal auth.Principal, req SubmitCandidateRequest) (TaskMemoryCandidateView, error) {
	if err := requireTool(principal, auth.ToolAgentSubmitMemoryCandidate, principal.ProjectID); err != nil {
		return TaskMemoryCandidateView{}, err
	}
	if principal.TaskID == "" {
		return TaskMemoryCandidateView{}, kernel.InvalidArgument("task_id is required")
	}
	candidate := cloneMemoryCandidate(req.Candidate)
	if err := validateMemoryCandidate(candidate); err != nil {
		return TaskMemoryCandidateView{}, err
	}
	tx, err := s.begin(ctx, serializableContextTx())
	if err != nil {
		return TaskMemoryCandidateView{}, err
	}
	defer tx.Rollback()
	state, err := taskMemoryStateSQL(ctx, tx, principal.ProjectID, principal.TaskID, true)
	if err != nil {
		return TaskMemoryCandidateView{}, err
	}
	if state == TaskMemoryFrozenUnreviewed || state == TaskMemoryReviewed {
		return TaskMemoryCandidateView{}, kernel.TransitionRejected("task memory is frozen")
	}
	if err := validateWritableGeneralSubgraphs(ctx, tx, principal, candidate.SubgraphIDs); err != nil {
		return TaskMemoryCandidateView{}, err
	}
	creation, err := creationContextSQL(ctx, tx, principal)
	if err != nil {
		return TaskMemoryCandidateView{}, err
	}
	id := newContextID("candidate", principal.ProjectID)
	rawCandidate, _ := json.Marshal(candidate)
	rawCreation, _ := json.Marshal(creation)
	if _, err := tx.ExecContext(ctx, `INSERT INTO context_task_memory_candidates(project_id, task_id, candidate_id, candidate, creation_context, created_by_invocation_id, created_at) VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6, $7)`, principal.ProjectID, principal.TaskID, id, string(rawCandidate), string(rawCreation), principal.InvocationID, s.now().UTC()); err != nil {
		return TaskMemoryCandidateView{}, mapContextPostgresError(err)
	}
	return TaskMemoryCandidateView{CandidateID: id, Candidate: cloneMemoryCandidate(candidate)}, mapContextPostgresError(tx.Commit())
}

func (s *PostgresStore) FinalizeTaskMemory(ctx context.Context, principal auth.Principal, taskID kernel.TaskID) (FrozenCandidateBatch, error) {
	if err := requireTool(principal, auth.ToolContextFinalizeTaskMemory, principal.ProjectID); err != nil {
		return FrozenCandidateBatch{}, err
	}
	if taskID == "" {
		taskID = principal.TaskID
	}
	if taskID == "" {
		return FrozenCandidateBatch{}, kernel.InvalidArgument("task_id is required")
	}
	if s.taskResolver == nil {
		return FrozenCandidateBatch{}, kernel.InvalidArgument("task endpoint resolver is required")
	}
	done, err := s.taskResolver.TaskDone(ctx, principal.ProjectID, taskID)
	if err != nil {
		return FrozenCandidateBatch{}, err
	}
	tx, err := s.begin(ctx, serializableContextTx())
	if err != nil {
		return FrozenCandidateBatch{}, err
	}
	defer tx.Rollback()
	if !done {
		if _, err := appendAuditSQL(ctx, tx, principal, "context.task_memory.finalize_rejected", string(taskID), nil, s.now().UTC()); err != nil {
			return FrozenCandidateBatch{}, err
		}
		if err := tx.Commit(); err != nil {
			return FrozenCandidateBatch{}, mapContextPostgresError(err)
		}
		return FrozenCandidateBatch{}, kernel.TransitionRejected("task memory can only be finalized after done")
	}
	records, err := candidateRecordsSQL(ctx, tx, principal.ProjectID, taskID)
	if err != nil {
		return FrozenCandidateBatch{}, err
	}
	state, err := taskMemoryStateSQL(ctx, tx, principal.ProjectID, taskID, true)
	if err != nil {
		return FrozenCandidateBatch{}, err
	}
	if state == "" || state == TaskMemoryOpen {
		if _, err := tx.ExecContext(ctx, `INSERT INTO context_task_memory_reviews(project_id, task_id, state, receipt, updated_at) VALUES ($1, $2, $3, '{}'::jsonb, $4) ON CONFLICT (project_id, task_id) DO UPDATE SET state = EXCLUDED.state, updated_at = EXCLUDED.updated_at`, principal.ProjectID, taskID, TaskMemoryFrozenUnreviewed, s.now().UTC()); err != nil {
			return FrozenCandidateBatch{}, mapContextPostgresError(err)
		}
	}
	return FrozenCandidateBatch{TaskID: string(taskID), Candidates: candidateView(records).Candidates}, mapContextPostgresError(tx.Commit())
}

func (s *PostgresStore) SubmitReview(ctx context.Context, principal auth.Principal, submission CandidateReviewSubmission) (TaskMemoryReviewReceipt, error) {
	if err := requireContextAgent(principal, auth.ToolContextSubmitReview, principal.ProjectID); err != nil {
		return TaskMemoryReviewReceipt{}, err
	}
	if principal.TaskID == "" {
		return TaskMemoryReviewReceipt{}, kernel.InvalidArgument("task_id is required for candidate review")
	}
	tx, err := s.begin(ctx, serializableContextTx())
	if err != nil {
		return TaskMemoryReviewReceipt{}, err
	}
	defer tx.Rollback()
	state, err := taskMemoryStateSQL(ctx, tx, principal.ProjectID, principal.TaskID, true)
	if err != nil {
		return TaskMemoryReviewReceipt{}, err
	}
	if state == TaskMemoryReviewed {
		receipt, err := reviewReceiptSQL(ctx, tx, principal.ProjectID, principal.TaskID)
		if err != nil {
			return TaskMemoryReviewReceipt{}, err
		}
		return receipt, mapContextPostgresError(tx.Commit())
	}
	if state != TaskMemoryFrozenUnreviewed {
		return TaskMemoryReviewReceipt{}, kernel.TransitionRejected("task memory batch is not frozen")
	}
	records, err := candidateRecordsSQL(ctx, tx, principal.ProjectID, principal.TaskID)
	if err != nil {
		return TaskMemoryReviewReceipt{}, err
	}
	if len(submission.Decisions) != len(records) {
		return TaskMemoryReviewReceipt{}, kernel.InvalidArgument("review decisions must cover frozen batch exactly once")
	}
	byID := map[string]CandidateBufferRecord{}
	for _, record := range records {
		byID[record.CandidateID] = record
	}
	seen := map[string]struct{}{}
	for i, decision := range submission.Decisions {
		if _, ok := seen[decision.CandidateID]; ok {
			return TaskMemoryReviewReceipt{}, kernel.InvalidArgument("duplicate candidate review decision")
		}
		seen[decision.CandidateID] = struct{}{}
		record, ok := byID[decision.CandidateID]
		if !ok {
			return TaskMemoryReviewReceipt{}, kernel.InvalidArgument("review decision references candidate outside frozen batch")
		}
		decision = normalizeReviewDecision(record, decision)
		if err := validateReviewDecisionSQL(ctx, tx, principal, decision); err != nil {
			return TaskMemoryReviewReceipt{}, err
		}
		submission.Decisions[i] = decision
	}
	now := s.now().UTC()
	var receipt TaskMemoryReviewReceipt
	receipt.TaskID = string(principal.TaskID)
	changed := map[string]struct{}{}
	for _, decision := range submission.Decisions {
		receipt.ReviewedIDs = append(receipt.ReviewedIDs, decision.CandidateID)
		if decision.Action == "reject" {
			receipt.RejectedIDs = append(receipt.RejectedIDs, decision.CandidateID)
			if _, _, err := appendMutationEventsSQL(ctx, tx, principal, "context.candidate_review.reject", "context.candidate_review.reject", decision.CandidateID, decision, now); err != nil {
				return TaskMemoryReviewReceipt{}, err
			}
			continue
		}
		record := byID[decision.CandidateID]
		node := normalizeNode(ContextNode{
			ID:             newContextID("review-node", principal.ProjectID),
			Kind:           decision.Kind,
			Statement:      decision.Statement,
			Status:         reviewStatus(decision.Action),
			SubgraphIDs:    decision.SubgraphIDs,
			SourceRefs:     uniqueStrings(append(append([]string(nil), record.Candidate.SourceRefs...), "candidate:"+decision.CandidateID)),
			CreatorAgentID: string(principal.ActorPrincipalID),
		})
		if _, err := insertContextNode(ctx, tx, principal.ProjectID, node, 1, now); err != nil {
			return TaskMemoryReviewReceipt{}, err
		}
		for _, subgraphID := range node.SubgraphIDs {
			if err := addMembership(ctx, tx, node.ID, subgraphID); err != nil {
				return TaskMemoryReviewReceipt{}, err
			}
			if err := bumpSubgraphRevision(ctx, tx, principal.ProjectID, subgraphID, now); err != nil {
				return TaskMemoryReviewReceipt{}, err
			}
			changed[subgraphID] = struct{}{}
		}
		if _, err := insertCreationEdges(ctx, tx, principal, record.CreationContext, node.ID, now); err != nil {
			return TaskMemoryReviewReceipt{}, err
		}
		if decision.TargetNodeID != "" {
			if _, err := tx.ExecContext(ctx, `UPDATE context_nodes SET status = $3, revision = revision + 1, updated_at = $4 WHERE project_id = $1 AND id = $2`, principal.ProjectID, decision.TargetNodeID, targetStatus(decision.Action), now); err != nil {
				return TaskMemoryReviewReceipt{}, mapContextPostgresError(err)
			}
		}
		if _, _, err := appendMutationEventsSQL(ctx, tx, principal, "context.candidate_review."+decision.Action, "context.candidate_review."+decision.Action, node.ID, node, now); err != nil {
			return TaskMemoryReviewReceipt{}, err
		}
		receipt.NodeIDs = append(receipt.NodeIDs, node.ID)
	}
	sort.Strings(receipt.ReviewedIDs)
	sort.Strings(receipt.RejectedIDs)
	rawReceipt, _ := json.Marshal(receipt)
	if _, err := tx.ExecContext(ctx, `UPDATE context_task_memory_reviews SET state = $3, receipt = $4::jsonb, updated_at = $5 WHERE project_id = $1 AND task_id = $2`, principal.ProjectID, principal.TaskID, TaskMemoryReviewed, string(rawReceipt), now); err != nil {
		return TaskMemoryReviewReceipt{}, mapContextPostgresError(err)
	}
	revision, err := bumpGraphRevision(ctx, tx, principal.ProjectID, now)
	if err != nil {
		return TaskMemoryReviewReceipt{}, err
	}
	if err := appendContextDeltasSQL(ctx, tx, principal.ProjectID, newContextID("review-event", principal.ProjectID), "context.candidate_review.reviewed", sortedStringSet(changed), revision); err != nil {
		return TaskMemoryReviewReceipt{}, err
	}
	return receipt, mapContextPostgresError(tx.Commit())
}

func (s *PostgresStore) begin(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	if s == nil || s.db == nil {
		return nil, kernel.Error{Code: kernel.CodeInternalError, Message: "postgres context graph store is not configured", Recoverable: true}
	}
	tx, err := s.db.BeginTx(ctx, opts)
	return tx, mapContextPostgresError(err)
}

func serializableContextTx() *sql.TxOptions {
	return &sql.TxOptions{Isolation: sql.LevelSerializable}
}

func idempotentContextTx() *sql.TxOptions {
	return &sql.TxOptions{Isolation: sql.LevelReadCommitted}
}

func newContextID(prefix string, projectID kernel.ProjectID) string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return fmt.Sprintf("%s-%s-%s", prefix, safeIDPart(string(projectID)), hex.EncodeToString(b[:]))
}

func scopedContextID(prefix string, projectID kernel.ProjectID, id string) string {
	if strings.HasPrefix(id, prefix+"-"+safeIDPart(string(projectID))+"-") {
		return id
	}
	return fmt.Sprintf("%s-%s-%s", prefix, safeIDPart(string(projectID)), safeIDPart(id))
}

func safeIDPart(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "empty"
	}
	var b strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	return b.String()
}

func graphRevision(ctx context.Context, q postgresDBTX, projectID kernel.ProjectID, lock bool) (kernel.Revision, error) {
	if projectID == "" {
		return 0, kernel.InvalidArgument("project_id is required")
	}
	query := `SELECT revision FROM context_graph_revisions WHERE project_id = $1`
	if lock {
		query += ` FOR UPDATE`
	}
	var revision kernel.Revision
	err := q.QueryRowContext(ctx, query, projectID).Scan(&revision)
	if errors.Is(err, sql.ErrNoRows) {
		if !lock {
			return 1, nil
		}
		if _, err := q.ExecContext(ctx, `INSERT INTO context_graph_revisions(project_id, revision) VALUES ($1, 1) ON CONFLICT (project_id) DO NOTHING`, projectID); err != nil {
			return 0, mapContextPostgresError(err)
		}
		return graphRevision(ctx, q, projectID, true)
	}
	if err != nil {
		return 0, mapContextPostgresError(err)
	}
	return revision, nil
}

func bumpGraphRevision(ctx context.Context, q postgresDBTX, projectID kernel.ProjectID, now time.Time) (kernel.Revision, error) {
	if _, err := graphRevision(ctx, q, projectID, true); err != nil {
		return 0, err
	}
	var revision kernel.Revision
	err := q.QueryRowContext(ctx, `UPDATE context_graph_revisions SET revision = revision + 1, updated_at = $2 WHERE project_id = $1 RETURNING revision`, projectID, now).Scan(&revision)
	return revision, mapContextPostgresError(err)
}

func insertContextNode(ctx context.Context, q postgresDBTX, projectID kernel.ProjectID, node ContextNode, revision kernel.Revision, now time.Time) (uint64, error) {
	refs, _ := marshalJSONArray(node.SourceRefs)
	var sequence uint64
	err := q.QueryRowContext(ctx, `INSERT INTO context_nodes(id, project_id, kind, statement, status, source_refs, creator_agent_id, revision, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9, $9) RETURNING created_sequence`, node.ID, projectID, node.Kind, node.Statement, node.Status, string(refs), node.CreatorAgentID, revision, now).Scan(&sequence)
	return sequence, mapContextPostgresError(err)
}

func addMembership(ctx context.Context, q postgresDBTX, nodeID, subgraphID string) error {
	_, err := q.ExecContext(ctx, `INSERT INTO context_node_subgraph_memberships(node_id, subgraph_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`, nodeID, subgraphID)
	return mapContextPostgresError(err)
}

func bumpSubgraphRevision(ctx context.Context, q postgresDBTX, projectID kernel.ProjectID, subgraphID string, now time.Time) error {
	_, err := q.ExecContext(ctx, `UPDATE context_subgraphs SET revision = revision + 1, updated_at = $3 WHERE project_id = $1 AND id = $2`, projectID, subgraphID, now)
	return mapContextPostgresError(err)
}

func validateNodeFields(node ContextNode) error {
	if node.ID == "" || node.Statement == "" || node.CreatorAgentID == "" {
		return kernel.InvalidArgument("node id, statement, and creator_agent_id are required")
	}
	if len(node.SourceRefs) == 0 {
		return kernel.InvalidArgument("source_refs are required")
	}
	return nil
}

func validateWritableGeneralSubgraphs(ctx context.Context, q postgresDBTX, principal auth.Principal, subgraphIDs []string) error {
	for _, subgraphID := range uniqueStrings(subgraphIDs) {
		var kind string
		err := q.QueryRowContext(ctx, `SELECT kind FROM context_subgraphs WHERE project_id = $1 AND id = $2`, principal.ProjectID, subgraphID).Scan(&kind)
		if errors.Is(err, sql.ErrNoRows) {
			return kernel.Error{Code: kernel.CodeNotFound, Message: "subgraph not found"}
		}
		if err != nil {
			return mapContextPostgresError(err)
		}
		if kind != string(SubgraphKindGeneral) {
			return kernel.Forbidden("Context Agent cannot write task subgraphs")
		}
	}
	return nil
}

func rejectTaskMembershipSQL(ctx context.Context, q postgresDBTX, subgraphIDs []string) error {
	for _, subgraphID := range subgraphIDs {
		var kind string
		err := q.QueryRowContext(ctx, `SELECT kind FROM context_subgraphs WHERE id = $1`, subgraphID).Scan(&kind)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return mapContextPostgresError(err)
		}
		if kind == string(SubgraphKindTask) {
			return kernel.Forbidden("Context Agent curator cannot mutate task nodes or task subgraphs")
		}
	}
	return nil
}

func insertCreationEdges(ctx context.Context, q postgresDBTX, principal auth.Principal, creation NodeCreationContext, nodeID string, now time.Time) ([]ContextEdge, error) {
	var edges []ContextEdge
	if creation.PreviousNodeID != "" {
		var creator, status string
		var canSee bool
		err := q.QueryRowContext(ctx, `
SELECT n.creator_agent_id, n.status,
  (NOT EXISTS (SELECT 1 FROM context_node_subgraph_memberships m WHERE m.node_id = n.id)
   OR EXISTS (
     SELECT 1 FROM context_node_subgraph_memberships m
     JOIN context_subgraphs s ON s.id = m.subgraph_id
     WHERE m.node_id = n.id AND s.project_id = $1 AND (s.kind = 'general' OR s.task_id = NULLIF($3, ''))
   ))
FROM context_nodes n WHERE n.project_id = $1 AND n.id = $2`, principal.ProjectID, creation.PreviousNodeID, principal.TaskID).Scan(&creator, &status, &canSee)
		if err == nil && canSee && creator == creation.CreatorAgentID && status != string(NodeStatusSuperseded) && status != string(NodeStatusOutdated) {
			edge := ContextEdge{FromRef: "node:" + creation.PreviousNodeID, ToNodeID: nodeID, Kind: string(EdgeKindLogicalAdjacent)}
			if err := insertEdgeSQL(ctx, q, edge, now); err != nil {
				return nil, err
			}
			edges = append(edges, edge)
		} else if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, mapContextPostgresError(err)
		}
	}
	for _, subgraphID := range uniqueStrings(creation.SubscribedSubgraphIDs) {
		ok, err := canSeeSubgraphSQL(ctx, q, principal, subgraphID)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		edge := ContextEdge{FromRef: "subgraph:" + subgraphID, ToNodeID: nodeID, Kind: string(EdgeKindDerivesFromSubgraph)}
		if err := insertEdgeSQL(ctx, q, edge, now); err != nil {
			return nil, err
		}
		edges = append(edges, edge)
	}
	return edges, nil
}

func insertEdgeSQL(ctx context.Context, q postgresDBTX, edge ContextEdge, now time.Time) error {
	_, err := q.ExecContext(ctx, `INSERT INTO context_edges(from_ref, to_node_id, kind, created_at) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING`, edge.FromRef, edge.ToNodeID, edge.Kind, now)
	return mapContextPostgresError(err)
}

func appendMutationEventsSQL(ctx context.Context, q postgresDBTX, principal auth.Principal, auditAction, outboxTopic, subjectID string, payload any, now time.Time) (AuditEvent, OutboxEvent, error) {
	audit, err := appendAuditSQL(ctx, q, principal, auditAction, subjectID, payload, now)
	if err != nil {
		return AuditEvent{}, OutboxEvent{}, err
	}
	outbox, err := appendOutboxSQL(ctx, q, principal.ProjectID, outboxTopic, subjectID, payload, now)
	if err != nil {
		return AuditEvent{}, OutboxEvent{}, err
	}
	return audit, outbox, nil
}

func appendAuditSQL(ctx context.Context, q postgresDBTX, principal auth.Principal, action, subjectID string, payload any, now time.Time) (AuditEvent, error) {
	raw, _ := json.Marshal(payload)
	if raw == nil || string(raw) == "null" {
		raw = []byte("{}")
	}
	event := AuditEvent{ID: newContextID("audit", principal.ProjectID), ProjectID: principal.ProjectID, ActorID: principal.ActorPrincipalID, Action: action, SubjectID: subjectID, CreatedAt: now}
	_, err := q.ExecContext(ctx, `INSERT INTO context_audit_events(id, project_id, actor_principal_id, action, subject_id, payload, created_at) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)`, event.ID, event.ProjectID, event.ActorID, event.Action, event.SubjectID, string(raw), event.CreatedAt)
	return event, mapContextPostgresError(err)
}

func appendOutboxSQL(ctx context.Context, q postgresDBTX, projectID kernel.ProjectID, topic, key string, payload any, now time.Time) (OutboxEvent, error) {
	raw, _ := json.Marshal(payload)
	if raw == nil || string(raw) == "null" {
		raw = []byte("{}")
	}
	event := OutboxEvent{ID: newContextID("outbox", projectID), Topic: topic, Key: key, Payload: raw, CreatedAt: now}
	_, err := q.ExecContext(ctx, `INSERT INTO context_outbox_events(id, project_id, topic, key, payload, created_at) VALUES ($1, $2, $3, $4, $5::jsonb, $6)`, event.ID, projectID, event.Topic, event.Key, string(event.Payload), event.CreatedAt)
	return event, mapContextPostgresError(err)
}

func appendContextDeltasSQL(ctx context.Context, q postgresDBTX, projectID kernel.ProjectID, eventID, eventKind string, subgraphIDs []string, revision kernel.Revision) error {
	_, err := appendContextDeltasSQLReturning(ctx, q, projectID, eventID, eventKind, subgraphIDs, revision)
	return err
}

func appendContextDeltasSQLReturning(ctx context.Context, q postgresDBTX, projectID kernel.ProjectID, eventID, eventKind string, subgraphIDs []string, revision kernel.Revision) ([]ContextDelta, error) {
	if len(subgraphIDs) == 0 {
		return nil, nil
	}
	rows, err := q.QueryContext(ctx, `SELECT id, consumer_invocation_id, subgraph_ids::text, COALESCE(task_id, '') FROM context_subscriptions WHERE project_id = $1 AND active AND (jsonb_array_length(event_kinds) = 0 OR event_kinds ? $2) ORDER BY id`, projectID, eventKind)
	if err != nil {
		return nil, mapContextPostgresError(err)
	}
	defer rows.Close()
	type subscriptionRow struct {
		id        string
		invID     string
		subgraphs []string
		taskID    kernel.TaskID
	}
	var subscriptions []subscriptionRow
	for rows.Next() {
		var item subscriptionRow
		var rawSubgraphs string
		if err := rows.Scan(&item.id, &item.invID, &rawSubgraphs, &item.taskID); err != nil {
			return nil, mapContextPostgresError(err)
		}
		if err := json.Unmarshal([]byte(rawSubgraphs), &item.subgraphs); err != nil {
			return nil, err
		}
		subscriptions = append(subscriptions, item)
	}
	if err := rows.Err(); err != nil {
		return nil, mapContextPostgresError(err)
	}
	if err := rows.Close(); err != nil {
		return nil, mapContextPostgresError(err)
	}
	var created []ContextDelta
	for _, subscription := range subscriptions {
		visible := intersectStrings(uniqueStrings(subgraphIDs), subscription.subgraphs)
		if len(visible) == 0 {
			continue
		}
		filtered, err := filterSubscriptionVisibleSubgraphs(ctx, q, projectID, subscription.taskID, visible)
		if err != nil {
			return nil, err
		}
		if len(filtered) == 0 {
			continue
		}
		rawVisible, _ := marshalJSONArray(filtered)
		delta := ContextDelta{ID: newContextID("delta", projectID), ProjectID: string(projectID), SubscriptionID: subscription.id, InvocationID: subscription.invID, EventID: eventID, EventKind: eventKind, SubgraphIDs: filtered, GraphRevision: int64(revision)}
		if _, err := q.ExecContext(ctx, `INSERT INTO context_delta_deliveries(id, project_id, subscription_id, consumer_invocation_id, event_id, event_kind, subgraph_ids, graph_revision) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8) ON CONFLICT (subscription_id, event_id) DO NOTHING`, delta.ID, delta.ProjectID, delta.SubscriptionID, delta.InvocationID, delta.EventID, delta.EventKind, string(rawVisible), delta.GraphRevision); err != nil {
			return nil, mapContextPostgresError(err)
		}
		created = append(created, delta)
	}
	return created, nil
}

func createSubscriptionSQL(ctx context.Context, q postgresDBTX, principal auth.Principal, req SubscribeRequest, source string, now time.Time) (ContextSubscription, error) {
	invocationID, err := subscriptionConsumerInvocationID(principal, source)
	if err != nil {
		return ContextSubscription{}, err
	}
	if invocationID == "" {
		return ContextSubscription{}, kernel.InvalidArgument("consumer invocation is required")
	}
	subgraphIDs, err := visibleSubscriptionSubgraphsSQL(ctx, q, principal, req.SubgraphIDs)
	if err != nil {
		return ContextSubscription{}, err
	}
	events := uniqueStrings(req.EventKinds)
	rawSubgraphs, _ := marshalJSONArray(subgraphIDs)
	rawEvents, _ := marshalJSONArray(events)
	sub := ContextSubscription{ID: newContextID("sub", principal.ProjectID), ConsumerInvocationID: string(invocationID), SubgraphIDs: subgraphIDs, EventKinds: events, PermissionSnapshot: fmt.Sprintf("%s:%s:%s", principal.ProjectID, subscriptionConsumerTaskID(principal, source), subscriptionConsumerRole(principal, source))}
	_, err = q.ExecContext(ctx, `INSERT INTO context_subscriptions(id, project_id, consumer_invocation_id, subgraph_ids, event_kinds, permission_snapshot, source, active, created_at, task_id, role) VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6, $7, true, $8, NULLIF($9, ''), $10)`, sub.ID, principal.ProjectID, sub.ConsumerInvocationID, string(rawSubgraphs), string(rawEvents), sub.PermissionSnapshot, source, now, subscriptionConsumerTaskID(principal, source), subscriptionConsumerRole(principal, source))
	return sub, mapContextPostgresError(err)
}

func ensureInitialSubscriptionSQL(ctx context.Context, q postgresDBTX, principal auth.Principal, req SubscribeRequest, now time.Time) (ContextSubscription, error) {
	invocationID, err := subscriptionConsumerInvocationID(principal, subscriptionSourceInitial)
	if err != nil {
		return ContextSubscription{}, err
	}
	if invocationID == "" {
		return ContextSubscription{}, kernel.InvalidArgument("consumer invocation is required")
	}
	subgraphIDs, err := visibleSubscriptionSubgraphsSQL(ctx, q, principal, req.SubgraphIDs)
	if err != nil {
		return ContextSubscription{}, err
	}
	events := uniqueStrings(req.EventKinds)
	rawSubgraphs, _ := marshalJSONArray(subgraphIDs)
	rawEvents, _ := marshalJSONArray(events)
	sub := ContextSubscription{ID: newContextID("sub", principal.ProjectID), ConsumerInvocationID: string(invocationID), SubgraphIDs: subgraphIDs, EventKinds: events, PermissionSnapshot: fmt.Sprintf("%s:%s:%s", principal.ProjectID, subscriptionConsumerTaskID(principal, subscriptionSourceInitial), subscriptionConsumerRole(principal, subscriptionSourceInitial))}
	if _, err := q.ExecContext(ctx, `INSERT INTO context_subscriptions(id, project_id, consumer_invocation_id, subgraph_ids, event_kinds, permission_snapshot, source, active, created_at, task_id, role) VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6, $7, true, $8, NULLIF($9, ''), $10) ON CONFLICT DO NOTHING`, sub.ID, principal.ProjectID, sub.ConsumerInvocationID, string(rawSubgraphs), string(rawEvents), sub.PermissionSnapshot, subscriptionSourceInitial, now, subscriptionConsumerTaskID(principal, subscriptionSourceInitial), subscriptionConsumerRole(principal, subscriptionSourceInitial)); err != nil {
		return ContextSubscription{}, mapContextPostgresError(err)
	}
	var rawExistingSubgraphs, rawExistingEvents string
	if err := q.QueryRowContext(ctx, `SELECT id, subgraph_ids::text, event_kinds::text, permission_snapshot FROM context_subscriptions WHERE project_id = $1 AND consumer_invocation_id = $2 AND source = $3 AND active ORDER BY created_at, id LIMIT 1`, principal.ProjectID, invocationID, subscriptionSourceInitial).Scan(&sub.ID, &rawExistingSubgraphs, &rawExistingEvents, &sub.PermissionSnapshot); err != nil {
		return ContextSubscription{}, mapContextPostgresError(err)
	}
	if err := json.Unmarshal([]byte(rawExistingSubgraphs), &sub.SubgraphIDs); err != nil {
		return ContextSubscription{}, err
	}
	if err := json.Unmarshal([]byte(rawExistingEvents), &sub.EventKinds); err != nil {
		return ContextSubscription{}, err
	}
	return sub, nil
}

func activeSubscriptionIDsSQL(ctx context.Context, q postgresDBTX, projectID kernel.ProjectID, invocationID kernel.InvocationID) ([]string, error) {
	rows, err := q.QueryContext(ctx, `SELECT id FROM context_subscriptions WHERE project_id = $1 AND consumer_invocation_id = $2 AND active ORDER BY id`, projectID, invocationID)
	if err != nil {
		return nil, mapContextPostgresError(err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, mapContextPostgresError(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, mapContextPostgresError(err)
	}
	return ids, nil
}

func visibleSubscriptionSubgraphsSQL(ctx context.Context, q postgresDBTX, principal auth.Principal, subgraphIDs []string) ([]string, error) {
	if len(subgraphIDs) == 0 {
		return nil, kernel.InvalidArgument("subgraph_ids are required")
	}
	var out []string
	for _, subgraphID := range uniqueStrings(subgraphIDs) {
		ok, err := canSeeSubgraphSQL(ctx, q, principal, subgraphID)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, kernel.Error{Code: kernel.CodeNotFound, Message: "subgraph not found"}
		}
		out = append(out, subgraphID)
	}
	return out, nil
}

func canSeeSubgraphSQL(ctx context.Context, q postgresDBTX, principal auth.Principal, subgraphID string) (bool, error) {
	var kind string
	var taskID sql.NullString
	err := q.QueryRowContext(ctx, `SELECT kind, task_id FROM context_subgraphs WHERE project_id = $1 AND id = $2`, principal.ProjectID, subgraphID).Scan(&kind, &taskID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, mapContextPostgresError(err)
	}
	return kind == string(SubgraphKindGeneral) || (taskID.Valid && taskID.String == string(principal.TaskID)), nil
}

func visibleSubgraphSQL(ctx context.Context, q postgresDBTX, principal auth.Principal, subgraphID string) (ContextSubgraph, error) {
	var sg ContextSubgraph
	var taskID sql.NullString
	err := q.QueryRowContext(ctx, `SELECT id, name, summary, revision, kind, task_id FROM context_subgraphs WHERE project_id = $1 AND id = $2`, principal.ProjectID, subgraphID).Scan(&sg.ID, &sg.Name, &sg.Summary, &sg.Revision, &sg.Kind, &taskID)
	if errors.Is(err, sql.ErrNoRows) {
		return ContextSubgraph{}, kernel.Error{Code: kernel.CodeNotFound, Message: "subgraph not found"}
	}
	if err != nil {
		return ContextSubgraph{}, mapContextPostgresError(err)
	}
	if sg.Kind != string(SubgraphKindGeneral) && (!taskID.Valid || taskID.String != string(principal.TaskID)) {
		return ContextSubgraph{}, kernel.Error{Code: kernel.CodeNotFound, Message: "subgraph not found"}
	}
	return sg, nil
}

func nodesInSubgraphsSQL(ctx context.Context, q postgresDBTX, principal auth.Principal, subgraphIDs []string) ([]ContextNode, error) {
	if len(subgraphIDs) == 0 {
		return nil, nil
	}
	rows, err := q.QueryContext(ctx, `
SELECT n.id, n.kind, n.statement, n.status, COALESCE(array_to_json(array_agg(m.subgraph_id ORDER BY m.subgraph_id))::text, '[]'), n.source_refs::text, n.creator_agent_id
FROM context_nodes n
JOIN context_node_subgraph_memberships m ON m.node_id = n.id
WHERE n.project_id = $1 AND m.subgraph_id = ANY($2::text[])
GROUP BY n.id, n.kind, n.statement, n.status, n.source_refs, n.creator_agent_id, n.created_sequence
ORDER BY n.created_sequence`, principal.ProjectID, textArrayLiteral(subgraphIDs))
	if err != nil {
		return nil, mapContextPostgresError(err)
	}
	defer rows.Close()
	var out []ContextNode
	for rows.Next() {
		node, err := scanContextNode(rows, principal, q)
		if err != nil {
			return nil, err
		}
		out = append(out, node)
	}
	if err := rows.Err(); err != nil {
		return nil, mapContextPostgresError(err)
	}
	return out, nil
}

type contextNodeScanner interface {
	Scan(...any) error
}

func scanContextNode(scanner contextNodeScanner, principal auth.Principal, q postgresDBTX) (ContextNode, error) {
	var node ContextNode
	var subgraphsRaw, refsRaw string
	if err := scanner.Scan(&node.ID, &node.Kind, &node.Statement, &node.Status, &subgraphsRaw, &refsRaw, &node.CreatorAgentID); err != nil {
		return ContextNode{}, mapContextPostgresError(err)
	}
	if err := json.Unmarshal([]byte(subgraphsRaw), &node.SubgraphIDs); err != nil {
		return ContextNode{}, err
	}
	if err := json.Unmarshal([]byte(refsRaw), &node.SourceRefs); err != nil {
		return ContextNode{}, err
	}
	node.SubgraphIDs = uniqueStrings(node.SubgraphIDs)
	return node, nil
}

func nodeVisibleAndCreatorSQL(ctx context.Context, q postgresDBTX, principal auth.Principal, nodeID, creator string) (bool, error) {
	record, err := nodeRecordSQL(ctx, q, principal.ProjectID, nodeID, false)
	if err != nil {
		if kernel.IsCode(err, kernel.CodeNotFound) {
			return false, nil
		}
		return false, err
	}
	if creator != "" && record.Node.CreatorAgentID != creator {
		return false, nil
	}
	node, err := visibleNodeSQL(ctx, q, principal, nodeID)
	if err != nil {
		if kernel.IsCode(err, kernel.CodeNotFound) {
			return false, nil
		}
		return false, err
	}
	return node.Status != string(NodeStatusSuperseded) && node.Status != string(NodeStatusOutdated), nil
}

func effectiveSubgraphsSQL(ctx context.Context, q postgresDBTX, principal auth.Principal) ([]string, error) {
	rows, err := q.QueryContext(ctx, `SELECT subgraph_ids::text FROM context_subscriptions WHERE project_id = $1 AND consumer_invocation_id = $2 AND active ORDER BY id`, principal.ProjectID, consumerInvocationID(principal))
	if err != nil {
		return nil, mapContextPostgresError(err)
	}
	defer rows.Close()
	var subscribed [][]string
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, mapContextPostgresError(err)
		}
		var ids []string
		if err := json.Unmarshal([]byte(raw), &ids); err != nil {
			return nil, err
		}
		subscribed = append(subscribed, ids)
	}
	if err := rows.Err(); err != nil {
		return nil, mapContextPostgresError(err)
	}
	if err := rows.Close(); err != nil {
		return nil, mapContextPostgresError(err)
	}
	set := map[string]struct{}{}
	for _, ids := range subscribed {
		for _, id := range ids {
			ok, err := canSeeSubgraphSQL(ctx, q, principal, id)
			if err != nil {
				return nil, err
			}
			if ok {
				set[id] = struct{}{}
			}
		}
	}
	return sortedStringSet(set), nil
}

func materializeInvocationSliceSQL(ctx context.Context, q postgresDBTX, principal auth.Principal) (ContextSlice, error) {
	effective, err := effectiveSubgraphsSQL(ctx, q, principal)
	if err != nil {
		return ContextSlice{}, err
	}
	nodes, err := nodesInSubgraphsSQL(ctx, q, principal, effective)
	if err != nil {
		return ContextSlice{}, err
	}
	rows, err := q.QueryContext(ctx, `SELECT id FROM context_subscriptions WHERE project_id = $1 AND consumer_invocation_id = $2 AND active ORDER BY id`, principal.ProjectID, consumerInvocationID(principal))
	if err != nil {
		return ContextSlice{}, mapContextPostgresError(err)
	}
	defer rows.Close()
	var subs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return ContextSlice{}, mapContextPostgresError(err)
		}
		subs = append(subs, id)
	}
	if err := rows.Err(); err != nil {
		return ContextSlice{}, mapContextPostgresError(err)
	}
	revision, err := graphRevision(ctx, q, principal.ProjectID, false)
	if err != nil {
		return ContextSlice{}, err
	}
	return ContextSlice{Nodes: nodes, SubscriptionIDs: subs, GraphRevision: int64(revision)}, nil
}

func visibleNodeSQL(ctx context.Context, q postgresDBTX, principal auth.Principal, nodeID string) (ContextNode, error) {
	record, err := nodeRecordSQL(ctx, q, principal.ProjectID, nodeID, false)
	if err != nil {
		return ContextNode{}, err
	}
	var visible []string
	for _, subgraphID := range record.Node.SubgraphIDs {
		ok, err := canSeeSubgraphSQL(ctx, q, principal, subgraphID)
		if err != nil {
			return ContextNode{}, err
		}
		if ok {
			visible = append(visible, subgraphID)
		}
	}
	if len(record.Node.SubgraphIDs) > 0 && len(visible) == 0 {
		return ContextNode{}, kernel.Error{Code: kernel.CodeNotFound, Message: "node not found"}
	}
	node := cloneNode(record.Node)
	node.SubgraphIDs = visible
	return node, nil
}

func nodeRecordSQL(ctx context.Context, q postgresDBTX, projectID kernel.ProjectID, nodeID string, lock bool) (NodeRecord, error) {
	query := `SELECT id, kind, statement, status, source_refs::text, creator_agent_id, revision, created_sequence, created_at FROM context_nodes WHERE project_id = $1 AND id = $2`
	if lock {
		query += ` FOR UPDATE`
	}
	var record NodeRecord
	var refs string
	record.ProjectID = projectID
	err := q.QueryRowContext(ctx, query, projectID, nodeID).Scan(&record.Node.ID, &record.Node.Kind, &record.Node.Statement, &record.Node.Status, &refs, &record.Node.CreatorAgentID, &record.Revision, &record.Sequence, &record.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return NodeRecord{}, kernel.Error{Code: kernel.CodeNotFound, Message: "node not found"}
	}
	if err != nil {
		return NodeRecord{}, mapContextPostgresError(err)
	}
	if err := json.Unmarshal([]byte(refs), &record.Node.SourceRefs); err != nil {
		return NodeRecord{}, err
	}
	members, err := nodeMembershipsSQL(ctx, q, nodeID)
	if err != nil {
		return NodeRecord{}, err
	}
	record.Node.SubgraphIDs = members
	return record, nil
}

func nodeMembershipsSQL(ctx context.Context, q postgresDBTX, nodeID string) ([]string, error) {
	rows, err := q.QueryContext(ctx, `SELECT subgraph_id FROM context_node_subgraph_memberships WHERE node_id = $1 ORDER BY subgraph_id`, nodeID)
	if err != nil {
		return nil, mapContextPostgresError(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, mapContextPostgresError(err)
		}
		out = append(out, id)
	}
	return out, mapContextPostgresError(rows.Err())
}

func subgraphRecordSQL(ctx context.Context, q postgresDBTX, projectID kernel.ProjectID, subgraphID string, lock bool) (SubgraphRecord, error) {
	query := `SELECT id, COALESCE(task_id, ''), name, summary, revision, kind, created_at, updated_at FROM context_subgraphs WHERE project_id = $1 AND id = $2`
	if lock {
		query += ` FOR UPDATE`
	}
	var r SubgraphRecord
	r.ProjectID = projectID
	err := q.QueryRowContext(ctx, query, projectID, subgraphID).Scan(&r.Subgraph.ID, &r.TaskID, &r.Subgraph.Name, &r.Subgraph.Summary, &r.Subgraph.Revision, &r.Subgraph.Kind, &r.CreatedAt, &r.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return SubgraphRecord{}, kernel.Error{Code: kernel.CodeNotFound, Message: "subgraph not found"}
	}
	return r, mapContextPostgresError(err)
}

func setGeneralSubgraphMembersSQL(ctx context.Context, q postgresDBTX, principal auth.Principal, subgraphID string, nodeIDs []string) error {
	for _, nodeID := range uniqueStrings(nodeIDs) {
		record, err := nodeRecordSQL(ctx, q, principal.ProjectID, nodeID, false)
		if err != nil {
			return err
		}
		if err := rejectTaskMembershipSQL(ctx, q, record.Node.SubgraphIDs); err != nil {
			return err
		}
	}
	if _, err := q.ExecContext(ctx, `DELETE FROM context_node_subgraph_memberships WHERE subgraph_id = $1`, subgraphID); err != nil {
		return mapContextPostgresError(err)
	}
	for _, nodeID := range uniqueStrings(nodeIDs) {
		if err := addMembership(ctx, q, nodeID, subgraphID); err != nil {
			return err
		}
	}
	return nil
}

func visibleScopeSQL(ctx context.Context, q postgresDBTX, principal auth.Principal, refs []string) ([]string, error) {
	var out []string
	for _, ref := range uniqueStrings(refs) {
		id := strings.TrimPrefix(ref, "subgraph:")
		ok, err := canSeeSubgraphSQL(ctx, q, principal, id)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, kernel.Error{Code: kernel.CodeNotFound, Message: "scope not found"}
		}
		out = append(out, id)
	}
	return out, nil
}

func visibleAnchorNodesSQL(ctx context.Context, q postgresDBTX, principal auth.Principal, anchorRefs []string) (map[string]struct{}, error) {
	if len(anchorRefs) == 0 {
		return nil, nil
	}
	nodes := map[string]struct{}{}
	for _, ref := range uniqueStrings(anchorRefs) {
		kind, id, err := parseAnchorRef(ref)
		if err != nil {
			return nil, err
		}
		switch kind {
		case "node":
			if _, err := visibleNodeSQL(ctx, q, principal, id); err != nil {
				if kernel.IsCode(err, kernel.CodeNotFound) {
					return nil, kernel.Error{Code: kernel.CodeNotFound, Message: "anchor not found"}
				}
				return nil, err
			}
			nodes[id] = struct{}{}
		case "subgraph":
			ok, err := canSeeSubgraphSQL(ctx, q, principal, id)
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, kernel.Error{Code: kernel.CodeNotFound, Message: "anchor not found"}
			}
			anchored, err := nodesInSubgraphsSQL(ctx, q, principal, []string{id})
			if err != nil {
				return nil, err
			}
			for _, node := range anchored {
				nodes[node.ID] = struct{}{}
			}
		}
	}
	return nodes, nil
}

func searchNodesSQL(ctx context.Context, q postgresDBTX, principal auth.Principal, keywords, scope []string) ([]ContextNode, error) {
	if len(scope) > 0 {
		nodes, err := nodesInSubgraphsSQL(ctx, q, principal, scope)
		if err != nil {
			return nil, err
		}
		return filterKeywordNodes(nodes, keywords), nil
	}
	rows, err := q.QueryContext(ctx, `SELECT id FROM context_nodes WHERE project_id = $1 ORDER BY created_sequence, id`, principal.ProjectID)
	if err != nil {
		return nil, mapContextPostgresError(err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, mapContextPostgresError(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, mapContextPostgresError(err)
	}
	if err := rows.Close(); err != nil {
		return nil, mapContextPostgresError(err)
	}
	var out []ContextNode
	for _, id := range ids {
		node, err := visibleNodeSQL(ctx, q, principal, id)
		if kernel.IsCode(err, kernel.CodeNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if keywordMatch(node.Statement, keywords) {
			out = append(out, node)
		}
	}
	return out, nil
}

func filterKeywordNodes(nodes []ContextNode, keywords []string) []ContextNode {
	out := nodes[:0]
	for _, node := range nodes {
		if keywordMatch(node.Statement, keywords) {
			out = append(out, node)
		}
	}
	return append([]ContextNode(nil), out...)
}

func filterSubscriptionVisibleSubgraphs(ctx context.Context, q postgresDBTX, projectID kernel.ProjectID, taskID kernel.TaskID, subgraphIDs []string) ([]string, error) {
	var out []string
	for _, subgraphID := range uniqueStrings(subgraphIDs) {
		var exists bool
		err := q.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM context_subgraphs WHERE project_id = $1 AND id = $2 AND (kind = 'general' OR task_id = NULLIF($3, '')))`, projectID, subgraphID, taskID).Scan(&exists)
		if err != nil {
			return nil, mapContextPostgresError(err)
		}
		if exists {
			out = append(out, subgraphID)
		}
	}
	return out, nil
}

func validateProjectionBindingsSQL(ctx context.Context, q postgresDBTX, principal auth.Principal, resolver TaskEndpointResolver, projection TaskContextProjection) error {
	subgraphSet := map[string]struct{}{}
	for _, subgraphID := range uniqueStrings(projection.SubgraphIDs) {
		var kind string
		err := q.QueryRowContext(ctx, `SELECT kind FROM context_subgraphs WHERE project_id = $1 AND id = $2`, principal.ProjectID, subgraphID).Scan(&kind)
		if errors.Is(err, sql.ErrNoRows) || kind != string(SubgraphKindTask) {
			return kernel.Error{Code: kernel.CodeNotFound, Message: "task subgraph not found"}
		}
		if err != nil {
			return mapContextPostgresError(err)
		}
		subgraphSet[subgraphID] = struct{}{}
	}
	recipientSet := map[string]struct{}{}
	for _, recipient := range projection.Recipients {
		if recipient.TaskID == "" {
			return kernel.InvalidArgument("recipient task_id is required")
		}
		taskID := kernel.TaskID(recipient.TaskID)
		exists, err := resolver.TaskExists(ctx, principal.ProjectID, taskID)
		if err != nil {
			return err
		}
		if !exists {
			return kernel.Error{Code: kernel.CodeNotFound, Message: "task not found"}
		}
		var binding string
		err = q.QueryRowContext(ctx, `SELECT subgraph_id FROM context_task_subgraph_bindings WHERE project_id = $1 AND task_id = $2`, principal.ProjectID, taskID).Scan(&binding)
		if errors.Is(err, sql.ErrNoRows) {
			return kernel.Error{Code: kernel.CodeNotFound, Message: "task binding not found"}
		}
		if err != nil {
			return mapContextPostgresError(err)
		}
		if _, ok := subgraphSet[binding]; !ok {
			return kernel.Forbidden("recipient task binding must be included in subgraph_ids")
		}
		for _, endpoint := range recipient.EndpointRefs {
			if endpoint.TaskID != taskID || endpoint.EndpointID == "" {
				return kernel.Forbidden("recipient endpoint_ref must belong to recipient task")
			}
			ok, err := resolver.EndpointExists(ctx, principal.ProjectID, endpoint)
			if err != nil {
				return err
			}
			if !ok {
				return kernel.Error{Code: kernel.CodeNotFound, Message: "endpoint not found"}
			}
		}
		recipientSet[binding] = struct{}{}
	}
	if len(recipientSet) != len(subgraphSet) {
		return kernel.Forbidden("subgraph_ids must match recipient task bindings")
	}
	return nil
}

func updateTaskProjectionNodeSQL(ctx context.Context, q postgresDBTX, principal auth.Principal, node ContextNode, projection TaskContextProjection, oldSubgraphs []string, now time.Time) error {
	refs, _ := marshalJSONArray(node.SourceRefs)
	if _, err := q.ExecContext(ctx, `UPDATE context_nodes SET kind = $3, statement = $4, status = $5, source_refs = $6::jsonb, revision = revision + 1, updated_at = $7 WHERE project_id = $1 AND id = $2`, principal.ProjectID, node.ID, node.Kind, node.Statement, node.Status, string(refs), now); err != nil {
		return mapContextPostgresError(err)
	}
	if _, err := q.ExecContext(ctx, `UPDATE context_task_projections SET source_revision = $3, updated_at = $4 WHERE project_id = $1 AND projection_id = $2`, principal.ProjectID, projection.ProjectionID, projection.SourceRevision, now); err != nil {
		return mapContextPostgresError(err)
	}
	return storeTaskProjectionBindingsSQL(ctx, q, principal, node.ID, projection, oldSubgraphs, now)
}

func storeTaskProjectionBindingsSQL(ctx context.Context, q postgresDBTX, principal auth.Principal, nodeID string, projection TaskContextProjection, oldSubgraphs []string, now time.Time) error {
	if _, err := q.ExecContext(ctx, `DELETE FROM context_node_subgraph_memberships WHERE node_id = $1`, nodeID); err != nil {
		return mapContextPostgresError(err)
	}
	for _, subgraphID := range projection.SubgraphIDs {
		if err := addMembership(ctx, q, nodeID, subgraphID); err != nil {
			return err
		}
	}
	if _, err := q.ExecContext(ctx, `DELETE FROM context_task_recipients WHERE node_id = $1 AND project_id = $2`, nodeID, principal.ProjectID); err != nil {
		return mapContextPostgresError(err)
	}
	for _, recipient := range projection.Recipients {
		refs, _ := marshalJSONArray(recipient.EndpointRefs)
		if _, err := q.ExecContext(ctx, `INSERT INTO context_task_recipients(node_id, project_id, task_id, endpoint_refs) VALUES ($1, $2, $3, $4::jsonb)`, nodeID, principal.ProjectID, recipient.TaskID, string(refs)); err != nil {
			return mapContextPostgresError(err)
		}
	}
	changed := projection.SubgraphIDs
	if oldSubgraphs != nil {
		changed = changedSubgraphMemberships(oldSubgraphs, projection.SubgraphIDs)
	}
	for _, subgraphID := range changed {
		if err := bumpSubgraphRevision(ctx, q, principal.ProjectID, subgraphID, now); err != nil {
			return err
		}
	}
	_, outbox, err := appendMutationEventsSQL(ctx, q, principal, "context.task_projection.create", "context.task_projection.created", nodeID, projection, now)
	if err != nil {
		return err
	}
	revision, err := bumpGraphRevision(ctx, q, principal.ProjectID, now)
	if err != nil {
		return err
	}
	return appendContextDeltasSQL(ctx, q, principal.ProjectID, outbox.ID, outbox.Topic, changed, revision)
}

func taskMemoryStateSQL(ctx context.Context, q postgresDBTX, projectID kernel.ProjectID, taskID kernel.TaskID, lock bool) (TaskMemoryReviewState, error) {
	query := `SELECT state FROM context_task_memory_reviews WHERE project_id = $1 AND task_id = $2`
	if lock {
		query += ` FOR UPDATE`
	}
	var state TaskMemoryReviewState
	err := q.QueryRowContext(ctx, query, projectID, taskID).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		return TaskMemoryOpen, nil
	}
	return state, mapContextPostgresError(err)
}

func creationContextSQL(ctx context.Context, q postgresDBTX, principal auth.Principal) (NodeCreationContext, error) {
	creation := NodeCreationContext{CreatorAgentID: string(principal.ActorPrincipalID)}
	err := q.QueryRowContext(ctx, `SELECT id FROM context_nodes WHERE project_id = $1 AND creator_agent_id = $2 ORDER BY created_sequence DESC LIMIT 1`, principal.ProjectID, principal.ActorPrincipalID).Scan(&creation.PreviousNodeID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return NodeCreationContext{}, mapContextPostgresError(err)
	}
	subs, err := effectiveSubgraphsSQL(ctx, q, principal)
	if err != nil {
		return NodeCreationContext{}, err
	}
	creation.SubscribedSubgraphIDs = subs
	return creation, nil
}

func candidateRecordsSQL(ctx context.Context, q postgresDBTX, projectID kernel.ProjectID, taskID kernel.TaskID) ([]CandidateBufferRecord, error) {
	rows, err := q.QueryContext(ctx, `SELECT candidate_id, candidate::text, creation_context::text, created_by_invocation_id, created_at FROM context_task_memory_candidates WHERE project_id = $1 AND task_id = $2 ORDER BY created_at, candidate_id`, projectID, taskID)
	if err != nil {
		return nil, mapContextPostgresError(err)
	}
	defer rows.Close()
	var out []CandidateBufferRecord
	for rows.Next() {
		var r CandidateBufferRecord
		var candidateRaw, creationRaw string
		r.TaskID = string(taskID)
		if err := rows.Scan(&r.CandidateID, &candidateRaw, &creationRaw, &r.CreatedByInvocationID, &r.CreatedAt); err != nil {
			return nil, mapContextPostgresError(err)
		}
		if err := json.Unmarshal([]byte(candidateRaw), &r.Candidate); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(creationRaw), &r.CreationContext); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, mapContextPostgresError(rows.Err())
}

func validateReviewDecisionSQL(ctx context.Context, q postgresDBTX, principal auth.Principal, decision CandidateReviewDecision) error {
	switch decision.Action {
	case "create", "revise", "supersede", "dispute", "reject":
	default:
		return kernel.InvalidArgument("review action is not allowed")
	}
	if decision.Action == "reject" {
		return nil
	}
	if err := validateNodeKind(decision.Kind); err != nil {
		return err
	}
	if err := validateWritableGeneralSubgraphs(ctx, q, principal, decision.SubgraphIDs); err != nil {
		return err
	}
	if decision.Action != "create" {
		if decision.TargetNodeID == "" {
			return kernel.InvalidArgument("target_node_id is required")
		}
		record, err := nodeRecordSQL(ctx, q, principal.ProjectID, decision.TargetNodeID, false)
		if err != nil {
			return err
		}
		if err := rejectTaskMembershipSQL(ctx, q, record.Node.SubgraphIDs); err != nil {
			return err
		}
	}
	return nil
}

func reviewReceiptSQL(ctx context.Context, q postgresDBTX, projectID kernel.ProjectID, taskID kernel.TaskID) (TaskMemoryReviewReceipt, error) {
	var raw string
	err := q.QueryRowContext(ctx, `SELECT receipt::text FROM context_task_memory_reviews WHERE project_id = $1 AND task_id = $2`, projectID, taskID).Scan(&raw)
	if err != nil {
		return TaskMemoryReviewReceipt{}, mapContextPostgresError(err)
	}
	var receipt TaskMemoryReviewReceipt
	if err := json.Unmarshal([]byte(raw), &receipt); err != nil {
		return TaskMemoryReviewReceipt{}, err
	}
	return receipt, nil
}

func marshalJSONArray[T any](values []T) ([]byte, error) {
	if values == nil {
		return []byte("[]"), nil
	}
	return json.Marshal(values)
}

func intersectStrings(left, right []string) []string {
	set := map[string]struct{}{}
	for _, value := range right {
		set[value] = struct{}{}
	}
	var out []string
	for _, value := range left {
		if _, ok := set[value]; ok {
			out = append(out, value)
		}
	}
	return uniqueStrings(out)
}

func textArrayLiteral(values []string) string {
	if len(values) == 0 {
		return "{}"
	}
	quoted := make([]string, len(values))
	for i, value := range values {
		quoted[i] = `"` + strings.ReplaceAll(strings.ReplaceAll(value, `\`, `\\`), `"`, `\"`) + `"`
	}
	return "{" + strings.Join(quoted, ",") + "}"
}

func affected(result sql.Result) int64 {
	n, err := result.RowsAffected()
	if err != nil {
		return 0
	}
	return n
}

func mapContextPostgresError(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505":
			return kernel.Error{Code: kernel.CodeCommandConflict, Message: "context graph row already exists: " + pgErr.ConstraintName, Recoverable: true}
		case "23503":
			return kernel.InvalidGraph("context graph references missing rows")
		case "40001":
			return kernel.Error{Code: kernel.CodeRevisionConflict, Message: "serialization conflict while updating context graph", Recoverable: true}
		}
	}
	return err
}

var _ WritePort = (*PostgresStore)(nil)
var _ ContextGraphReader = (*PostgresStore)(nil)
var _ ContextGraphSearcher = (*PostgresStore)(nil)
var _ ContextGraphCurator = (*PostgresStore)(nil)
var _ TaskContextWriter = (*PostgresStore)(nil)
var _ TaskMemoryBufferReader = (*PostgresStore)(nil)
var _ CandidateSubmitter = (*PostgresStore)(nil)
var _ TaskMemoryFinalizer = (*PostgresStore)(nil)
var _ ContextCandidateReviewer = (*PostgresStore)(nil)
