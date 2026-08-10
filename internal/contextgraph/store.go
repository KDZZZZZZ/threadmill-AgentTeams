package contextgraph

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

type Fault string

const (
	FaultAudit  Fault = "audit"
	FaultOutbox Fault = "outbox"
	FaultEdge   Fault = "edge"
)

type MemoryStore struct {
	mu            sync.Mutex
	now           func() time.Time
	graphRevision kernel.Revision
	nextSeq       uint64
	nodes         map[string]NodeRecord
	subgraphs     map[string]SubgraphRecord
	edges         map[string]ContextEdge
	audit         []AuditEvent
	outbox        []OutboxEvent
	failNext      Fault
}

type memoryData struct {
	graphRevision kernel.Revision
	nextSeq       uint64
	nodes         map[string]NodeRecord
	subgraphs     map[string]SubgraphRecord
	edges         map[string]ContextEdge
	audit         []AuditEvent
	outbox        []OutboxEvent
	failNext      Fault
}

func NewMemoryStore(now func() time.Time) *MemoryStore {
	if now == nil {
		now = time.Now
	}
	return &MemoryStore{
		now:           now,
		graphRevision: 1,
		nodes:         make(map[string]NodeRecord),
		subgraphs:     make(map[string]SubgraphRecord),
		edges:         make(map[string]ContextEdge),
	}
}

func (s *MemoryStore) SetNextFault(fault Fault) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failNext = fault
}

func (s *MemoryStore) GraphRevision() kernel.Revision {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.graphRevision
}

func (s *MemoryStore) AuditEvents() []AuditEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]AuditEvent(nil), s.audit...)
}

func (s *MemoryStore) OutboxEvents() []OutboxEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]OutboxEvent(nil), s.outbox...)
}

func (s *MemoryStore) CreateNodes(ctx context.Context, principal auth.Principal, req CreateNodesRequest) (CreateNodesResult, error) {
	if err := ctx.Err(); err != nil {
		return CreateNodesResult{}, err
	}
	if err := requireTool(principal, auth.ToolContextSubmitReview, principal.ProjectID); err != nil {
		return CreateNodesResult{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	working := s.cloneLocked()
	if err := kernel.CheckExpectedRevision(req.ExpectedGraphRevision, working.graphRevision); err != nil {
		return CreateNodesResult{}, err
	}
	var created []ContextNode
	var createdEdges []ContextEdge
	now := s.now().UTC()
	for _, input := range req.Nodes {
		node := normalizeNode(input.Node)
		if err := validateNewNode(principal, working.subgraphs, node, input.CreationContext); err != nil {
			return CreateNodesResult{}, err
		}
		if _, exists := working.nodes[node.ID]; exists {
			return CreateNodesResult{}, kernel.InvalidArgument("node already exists")
		}
		working.nextSeq++
		working.nodes[node.ID] = NodeRecord{
			Node:      node,
			Revision:  1,
			ProjectID: principal.ProjectID,
			CreatedAt: now,
			Sequence:  working.nextSeq,
		}
		for _, subgraphID := range node.SubgraphIDs {
			subgraph := working.subgraphs[subgraphID]
			subgraph.Subgraph.Revision++
			subgraph.UpdatedAt = now
			working.subgraphs[subgraphID] = subgraph
		}
		if input.CreationContext.PreviousNodeID != "" {
			previous, ok := working.nodes[input.CreationContext.PreviousNodeID]
			if ok && previous.ProjectID == principal.ProjectID && canSeeNode(principal, working.subgraphs, previous) && previous.Node.CreatorAgentID == input.CreationContext.CreatorAgentID && previous.Node.Status != string(NodeStatusSuperseded) && previous.Node.Status != string(NodeStatusOutdated) {
				edge := ContextEdge{
					FromRef:  "node:" + previous.Node.ID,
					ToNodeID: node.ID,
					Kind:     string(EdgeKindLogicalAdjacent),
				}
				if err := working.addEdge(edge); err != nil {
					return CreateNodesResult{}, err
				}
				createdEdges = append(createdEdges, edge)
			}
		}
		for _, subgraphID := range uniqueStrings(input.CreationContext.SubscribedSubgraphIDs) {
			subgraph, ok := working.subgraphs[subgraphID]
			if !ok || !canSeeSubgraph(principal, subgraph) {
				continue
			}
			edge := ContextEdge{
				FromRef:  "subgraph:" + subgraphID,
				ToNodeID: node.ID,
				Kind:     string(EdgeKindDerivesFromSubgraph),
			}
			if err := working.addEdge(edge); err != nil {
				return CreateNodesResult{}, err
			}
			createdEdges = append(createdEdges, edge)
		}
		created = append(created, cloneNode(node))
		if err := working.appendAudit(AuditEvent{
			ID:        fmt.Sprintf("audit-%d", len(working.audit)+1),
			ProjectID: principal.ProjectID,
			ActorID:   principal.ActorPrincipalID,
			Action:    "context.node.create",
			SubjectID: node.ID,
			CreatedAt: now,
		}); err != nil {
			return CreateNodesResult{}, err
		}
		if err := working.appendOutbox(OutboxEvent{
			ID:        fmt.Sprintf("outbox-%d", len(working.outbox)+1),
			Topic:     "context.node.created",
			Key:       node.ID,
			Payload:   mustJSON(node),
			CreatedAt: now,
		}); err != nil {
			return CreateNodesResult{}, err
		}
	}
	working.graphRevision++
	result := CreateNodesResult{
		GraphRevision: working.graphRevision,
		Nodes:         created,
		Edges:         append([]ContextEdge(nil), createdEdges...),
		AuditEvents:   append([]AuditEvent(nil), working.audit[len(s.audit):]...),
		OutboxEvents:  append([]OutboxEvent(nil), working.outbox[len(s.outbox):]...),
	}
	s.replaceLocked(working)
	return result, nil
}

func (s *MemoryStore) ListSubgraphs(ctx context.Context, principal auth.Principal, req ListSubgraphsRequest) ([]ContextSubgraph, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := requireTool(principal, auth.ToolContextListSubgraphs, principal.ProjectID); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var subgraphs []ContextSubgraph
	for _, record := range s.subgraphs {
		if req.Filter != "" && record.Subgraph.Kind != req.Filter {
			continue
		}
		if canSeeSubgraph(principal, record) {
			subgraphs = append(subgraphs, cloneSubgraph(record.Subgraph))
		}
	}
	sort.Slice(subgraphs, func(i, j int) bool { return subgraphs[i].ID < subgraphs[j].ID })
	return subgraphs, nil
}

func (s *MemoryStore) Explore(ctx context.Context, principal auth.Principal, req ExploreRequest) (ContextSliceDelta, error) {
	if err := ctx.Err(); err != nil {
		return ContextSliceDelta{}, err
	}
	if err := requireTool(principal, auth.ToolContextExplore, principal.ProjectID); err != nil {
		return ContextSliceDelta{}, err
	}
	depth := req.Depth
	if depth <= 0 {
		depth = 1
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	result := ContextSliceDelta{GraphRevision: int64(s.graphRevision)}
	addedNodes := map[string]struct{}{}
	anchorKind, anchorID, err := parseAnchorRef(req.AnchorRef)
	if err != nil {
		return ContextSliceDelta{}, err
	}
	if req.AnchorRef == "" {
		return result, nil
	}
	if anchorKind == "subgraph" {
		record, ok := s.subgraphs[anchorID]
		if !ok || !canSeeSubgraph(principal, record) {
			return ContextSliceDelta{}, kernel.Error{Code: kernel.CodeNotFound, Message: "subgraph not found"}
		}
		for _, node := range s.sortedNodesLocked() {
			if _, seen := addedNodes[node.Node.ID]; seen || !contains(node.Node.SubgraphIDs, anchorID) || !canSeeNode(principal, s.subgraphs, node) {
				continue
			}
			result.Nodes = append(result.Nodes, visibleNode(principal, s.subgraphs, node.Node))
			addedNodes[node.Node.ID] = struct{}{}
			result.Frontier = append(result.Frontier, "node:"+node.Node.ID)
		}
	}
	if anchorKind == "node" {
		record, ok := s.nodes[anchorID]
		if !ok || !canSeeNode(principal, s.subgraphs, record) {
			return ContextSliceDelta{}, kernel.Error{Code: kernel.CodeNotFound, Message: "node not found"}
		}
		if _, seen := addedNodes[record.Node.ID]; !seen {
			result.Nodes = append(result.Nodes, visibleNode(principal, s.subgraphs, record.Node))
			addedNodes[record.Node.ID] = struct{}{}
		}
		s.expandNodeLocked(principal, anchorID, depth, addedNodes, &result)
	}
	return result, nil
}

func (s *MemoryStore) Search(ctx context.Context, principal auth.Principal, req SearchRequest) (ContextSearchResult, error) {
	if err := ctx.Err(); err != nil {
		return ContextSearchResult{}, err
	}
	if principal.Role != auth.RoleContext {
		return ContextSearchResult{}, kernel.Forbidden("search is exposed only to Context Agent")
	}
	if err := requireTool(principal, auth.ToolContextSearch, principal.ProjectID); err != nil {
		return ContextSearchResult{}, err
	}
	keywords := lowerAll(req.Keywords)
	s.mu.Lock()
	defer s.mu.Unlock()
	scope, err := s.visibleScopeLocked(principal, req.Scope)
	if err != nil {
		return ContextSearchResult{}, err
	}
	anchored, err := s.visibleAnchorNodesLocked(principal, req.AnchorRefs)
	if err != nil {
		return ContextSearchResult{}, err
	}
	var nodes []ContextNode
	for _, record := range s.sortedNodesLocked() {
		if !canSeeNode(principal, s.subgraphs, record) {
			continue
		}
		if len(scope) > 0 && !intersects(record.Node.SubgraphIDs, scope) {
			continue
		}
		if len(anchored) > 0 {
			if _, ok := anchored[record.Node.ID]; !ok {
				continue
			}
		}
		if keywordMatch(record.Node.Statement, keywords) {
			nodes = append(nodes, visibleNode(principal, s.subgraphs, record.Node))
		}
	}
	return ContextSearchResult{
		Slice: ContextSliceDelta{
			Nodes:         nodes,
			GraphRevision: int64(s.graphRevision),
		},
		MatchedKeywords: keywords,
		SubscriptionIDs: nil,
	}, nil
}

func (s *MemoryStore) expandNodeLocked(principal auth.Principal, anchorID string, depth int, addedNodes map[string]struct{}, result *ContextSliceDelta) {
	frontier := map[string]struct{}{anchorID: {}}
	for i := 0; i < depth; i++ {
		next := map[string]struct{}{}
		for currentID := range frontier {
			for _, edge := range s.sortedEdgesLocked() {
				if !s.canSeeEdge(principal, edge) {
					continue
				}
				if edge.FromRef != "node:"+currentID && edge.ToNodeID != currentID {
					continue
				}
				otherID := edge.ToNodeID
				if edge.ToNodeID == currentID && strings.HasPrefix(edge.FromRef, "node:") {
					otherID = strings.TrimPrefix(edge.FromRef, "node:")
				}
				other, ok := s.nodes[otherID]
				if !ok || !canSeeNode(principal, s.subgraphs, other) {
					continue
				}
				if _, seen := addedNodes[other.Node.ID]; !seen {
					result.Nodes = append(result.Nodes, visibleNode(principal, s.subgraphs, other.Node))
					addedNodes[other.Node.ID] = struct{}{}
					next[other.Node.ID] = struct{}{}
				}
			}
		}
		frontier = next
	}
	for nodeID := range frontier {
		result.Frontier = append(result.Frontier, "node:"+nodeID)
	}
	sort.Strings(result.Frontier)
}

func (s *MemoryStore) visibleScopeLocked(principal auth.Principal, scopeRefs []string) ([]string, error) {
	var scope []string
	for _, ref := range uniqueStrings(scopeRefs) {
		subgraphID := strings.TrimPrefix(ref, "subgraph:")
		record, ok := s.subgraphs[subgraphID]
		if !ok || !canSeeSubgraph(principal, record) {
			return nil, kernel.Error{Code: kernel.CodeNotFound, Message: "scope not found"}
		}
		scope = append(scope, subgraphID)
	}
	return scope, nil
}

func (s *MemoryStore) visibleAnchorNodesLocked(principal auth.Principal, anchorRefs []string) (map[string]struct{}, error) {
	if len(anchorRefs) == 0 {
		return nil, nil
	}
	nodes := make(map[string]struct{})
	for _, ref := range uniqueStrings(anchorRefs) {
		kind, id, err := parseAnchorRef(ref)
		if err != nil {
			return nil, err
		}
		switch kind {
		case "node":
			record, ok := s.nodes[id]
			if !ok || !canSeeNode(principal, s.subgraphs, record) {
				return nil, kernel.Error{Code: kernel.CodeNotFound, Message: "anchor not found"}
			}
			nodes[id] = struct{}{}
		case "subgraph":
			record, ok := s.subgraphs[id]
			if !ok || !canSeeSubgraph(principal, record) {
				return nil, kernel.Error{Code: kernel.CodeNotFound, Message: "anchor not found"}
			}
			for _, node := range s.sortedNodesLocked() {
				if contains(node.Node.SubgraphIDs, id) && canSeeNode(principal, s.subgraphs, node) {
					nodes[node.Node.ID] = struct{}{}
				}
			}
		}
	}
	return nodes, nil
}

func (s *MemoryStore) canSeeEdge(principal auth.Principal, edge ContextEdge) bool {
	to, ok := s.nodes[edge.ToNodeID]
	if !ok || !canSeeNode(principal, s.subgraphs, to) {
		return false
	}
	if strings.HasPrefix(edge.FromRef, "node:") {
		from, ok := s.nodes[strings.TrimPrefix(edge.FromRef, "node:")]
		return ok && canSeeNode(principal, s.subgraphs, from)
	}
	if strings.HasPrefix(edge.FromRef, "subgraph:") {
		from, ok := s.subgraphs[strings.TrimPrefix(edge.FromRef, "subgraph:")]
		return ok && canSeeSubgraph(principal, from)
	}
	return false
}

func parseAnchorRef(ref string) (string, string, error) {
	if ref == "" {
		return "", "", nil
	}
	kind, id, ok := strings.Cut(ref, ":")
	if !ok || id == "" {
		return "", "", kernel.InvalidArgument("anchor_ref must be node:<id> or subgraph:<id>")
	}
	switch kind {
	case "node", "subgraph":
		return kind, id, nil
	default:
		return "", "", kernel.InvalidArgument("anchor_ref must be node:<id> or subgraph:<id>")
	}
}

func visibleNode(principal auth.Principal, subgraphs map[string]SubgraphRecord, node ContextNode) ContextNode {
	visible := cloneNode(node)
	visible.SubgraphIDs = visible.SubgraphIDs[:0]
	for _, subgraphID := range node.SubgraphIDs {
		if subgraph, ok := subgraphs[subgraphID]; ok && canSeeSubgraph(principal, subgraph) {
			visible.SubgraphIDs = append(visible.SubgraphIDs, subgraphID)
		}
	}
	if len(visible.SubgraphIDs) == 0 {
		visible.SubgraphIDs = nil
	}
	return visible
}

func (s *MemoryStore) GetSubgraph(ctx context.Context, principal auth.Principal, req GetSubgraphRequest) (ContextSubgraph, error) {
	if err := ctx.Err(); err != nil {
		return ContextSubgraph{}, err
	}
	if err := requireContextAgent(principal, auth.ToolContextGetSubgraph, principal.ProjectID); err != nil {
		return ContextSubgraph{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.subgraphs[req.SubgraphID]
	if !ok || !canSeeSubgraph(principal, record) {
		return ContextSubgraph{}, kernel.Error{Code: kernel.CodeNotFound, Message: "subgraph not found"}
	}
	return cloneSubgraph(record.Subgraph), nil
}

func (s *MemoryStore) GetNode(ctx context.Context, principal auth.Principal, req GetNodeRequest) (ContextNode, error) {
	if err := ctx.Err(); err != nil {
		return ContextNode{}, err
	}
	if err := requireContextAgent(principal, auth.ToolContextGetNode, principal.ProjectID); err != nil {
		return ContextNode{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.nodes[req.NodeID]
	if !ok || !canSeeNode(principal, s.subgraphs, record) {
		return ContextNode{}, kernel.Error{Code: kernel.CodeNotFound, Message: "node not found"}
	}
	return visibleNode(principal, s.subgraphs, record.Node), nil
}

func (s *MemoryStore) CreateNode(ctx context.Context, principal auth.Principal, req CreateGeneralNodeRequest) (ContextNodeRef, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := requireContextAgent(principal, auth.ToolContextCreateNode, principal.ProjectID); err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	working := s.cloneLocked()
	now := s.now().UTC()
	working.nextSeq++
	node := normalizeNode(ContextNode{
		ID:             fmt.Sprintf("node-%d", working.nextSeq),
		Kind:           req.Kind,
		Statement:      req.Statement,
		Status:         string(NodeStatusAccepted),
		SubgraphIDs:    req.SubgraphIDs,
		SourceRefs:     req.SourceRefs,
		CreatorAgentID: string(principal.ActorPrincipalID),
	})
	if err := validateGeneralNode(principal, working.subgraphs, node, NodeCreationContext{CreatorAgentID: node.CreatorAgentID}); err != nil {
		return "", err
	}
	working.nodes[node.ID] = NodeRecord{Node: node, Revision: 1, ProjectID: principal.ProjectID, CreatedAt: now, Sequence: working.nextSeq}
	for _, subgraphID := range node.SubgraphIDs {
		record := working.subgraphs[subgraphID]
		record.Subgraph.Revision++
		record.UpdatedAt = now
		working.subgraphs[subgraphID] = record
	}
	if err := appendMutationEvents(working, now, principal, "context.node.create", "context.node.created", node.ID, node); err != nil {
		return "", err
	}
	working.graphRevision++
	s.replaceLocked(working)
	return ContextNodeRef(node.ID), nil
}

func (s *MemoryStore) UpdateNode(ctx context.Context, principal auth.Principal, req UpdateGeneralNodeRequest) (ContextNodeRef, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := requireContextAgent(principal, auth.ToolContextUpdateNode, principal.ProjectID); err != nil {
		return "", err
	}
	expected, err := parseExpectedRevision(req.SourceRevision)
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	working := s.cloneLocked()
	record, ok := working.nodes[req.NodeID]
	if !ok || record.ProjectID != principal.ProjectID {
		return "", kernel.Error{Code: kernel.CodeNotFound, Message: "node not found"}
	}
	if err := rejectTaskMembership(working.subgraphs, record.Node.SubgraphIDs); err != nil {
		return "", err
	}
	if err := kernel.CheckExpectedRevision(expected, record.Revision); err != nil {
		return "", err
	}
	node := normalizeNode(ContextNode{
		ID:             record.Node.ID,
		Kind:           req.Kind,
		Statement:      req.Statement,
		Status:         req.Status,
		SubgraphIDs:    req.SubgraphIDs,
		SourceRefs:     req.SourceRefs,
		CreatorAgentID: record.Node.CreatorAgentID,
	})
	if err := validateGeneralNode(principal, working.subgraphs, node, NodeCreationContext{CreatorAgentID: record.Node.CreatorAgentID}); err != nil {
		return "", err
	}
	oldSubgraphIDs := append([]string(nil), record.Node.SubgraphIDs...)
	record.Node = node
	record.Revision++
	working.nodes[node.ID] = record
	now := s.now().UTC()
	for _, subgraphID := range changedSubgraphMemberships(oldSubgraphIDs, node.SubgraphIDs) {
		subgraph := working.subgraphs[subgraphID]
		subgraph.Subgraph.Revision++
		subgraph.UpdatedAt = now
		working.subgraphs[subgraphID] = subgraph
	}
	if err := appendMutationEvents(working, now, principal, "context.node.update", "context.node.updated", node.ID, node); err != nil {
		return "", err
	}
	working.graphRevision++
	s.replaceLocked(working)
	return ContextNodeRef(node.ID), nil
}

func (s *MemoryStore) DeleteNode(ctx context.Context, principal auth.Principal, req DeleteGeneralNodeRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := requireContextAgent(principal, auth.ToolContextDeleteNode, principal.ProjectID); err != nil {
		return err
	}
	expected, err := parseExpectedRevision(req.SourceRevision)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	working := s.cloneLocked()
	record, ok := working.nodes[req.NodeID]
	if !ok || record.ProjectID != principal.ProjectID {
		return kernel.Error{Code: kernel.CodeNotFound, Message: "node not found"}
	}
	if err := rejectTaskMembership(working.subgraphs, record.Node.SubgraphIDs); err != nil {
		return err
	}
	if err := kernel.CheckExpectedRevision(expected, record.Revision); err != nil {
		return err
	}
	delete(working.nodes, req.NodeID)
	for key, edge := range working.edges {
		if edge.ToNodeID == req.NodeID || edge.FromRef == "node:"+req.NodeID {
			delete(working.edges, key)
		}
	}
	now := s.now().UTC()
	for _, subgraphID := range record.Node.SubgraphIDs {
		subgraph := working.subgraphs[subgraphID]
		subgraph.Subgraph.Revision++
		subgraph.UpdatedAt = now
		working.subgraphs[subgraphID] = subgraph
	}
	if err := appendMutationEvents(working, now, principal, "context.node.delete", "context.node.deleted", req.NodeID, req); err != nil {
		return err
	}
	working.graphRevision++
	s.replaceLocked(working)
	return nil
}

func (s *MemoryStore) CreateSubgraph(ctx context.Context, principal auth.Principal, req CreateGeneralSubgraphRequest) (ContextSubgraph, error) {
	if err := ctx.Err(); err != nil {
		return ContextSubgraph{}, err
	}
	if err := requireContextAgent(principal, auth.ToolContextCreateSubgraph, principal.ProjectID); err != nil {
		return ContextSubgraph{}, err
	}
	if req.Name == "" {
		return ContextSubgraph{}, kernel.InvalidArgument("subgraph name is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	working := s.cloneLocked()
	id := ""
	for i := len(working.subgraphs) + 1; ; i++ {
		id = fmt.Sprintf("subgraph-%d", i)
		if _, exists := working.subgraphs[id]; !exists {
			break
		}
	}
	now := s.now().UTC()
	subgraph := ContextSubgraph{ID: id, Name: req.Name, Summary: req.Summary, Revision: 1, Kind: string(SubgraphKindGeneral)}
	working.subgraphs[id] = SubgraphRecord{Subgraph: subgraph, ProjectID: principal.ProjectID, CreatedAt: now, UpdatedAt: now}
	if err := setGeneralSubgraphMembers(principal, working, id, req.NodeIDs); err != nil {
		return ContextSubgraph{}, err
	}
	if err := appendMutationEvents(working, now, principal, "context.subgraph.create", "context.subgraph.created", id, subgraph); err != nil {
		return ContextSubgraph{}, err
	}
	working.graphRevision++
	s.replaceLocked(working)
	return cloneSubgraph(subgraph), nil
}

func (s *MemoryStore) UpdateSubgraph(ctx context.Context, principal auth.Principal, req UpdateGeneralSubgraphRequest) (ContextSubgraph, error) {
	if err := ctx.Err(); err != nil {
		return ContextSubgraph{}, err
	}
	if err := requireContextAgent(principal, auth.ToolContextUpdateSubgraph, principal.ProjectID); err != nil {
		return ContextSubgraph{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	working := s.cloneLocked()
	record, ok := working.subgraphs[req.SubgraphID]
	if !ok || record.ProjectID != principal.ProjectID {
		return ContextSubgraph{}, kernel.Error{Code: kernel.CodeNotFound, Message: "subgraph not found"}
	}
	if record.Subgraph.Kind != string(SubgraphKindGeneral) {
		return ContextSubgraph{}, kernel.Forbidden("Context Agent curator cannot update task subgraphs")
	}
	if err := kernel.CheckExpectedRevision(kernel.Revision(req.Revision), kernel.Revision(record.Subgraph.Revision)); err != nil {
		return ContextSubgraph{}, err
	}
	record.Subgraph.Name = req.Name
	record.Subgraph.Summary = req.Summary
	record.Subgraph.Revision++
	record.UpdatedAt = s.now().UTC()
	working.subgraphs[req.SubgraphID] = record
	if err := setGeneralSubgraphMembers(principal, working, req.SubgraphID, req.NodeIDs); err != nil {
		return ContextSubgraph{}, err
	}
	if err := appendMutationEvents(working, record.UpdatedAt, principal, "context.subgraph.update", "context.subgraph.updated", req.SubgraphID, record.Subgraph); err != nil {
		return ContextSubgraph{}, err
	}
	working.graphRevision++
	s.replaceLocked(working)
	return cloneSubgraph(record.Subgraph), nil
}

func (s *MemoryStore) DeleteSubgraph(ctx context.Context, principal auth.Principal, req DeleteGeneralSubgraphRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := requireContextAgent(principal, auth.ToolContextDeleteSubgraph, principal.ProjectID); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	working := s.cloneLocked()
	record, ok := working.subgraphs[req.SubgraphID]
	if !ok || record.ProjectID != principal.ProjectID {
		return kernel.Error{Code: kernel.CodeNotFound, Message: "subgraph not found"}
	}
	if record.Subgraph.Kind != string(SubgraphKindGeneral) {
		return kernel.Forbidden("Context Agent curator cannot delete task subgraphs")
	}
	if err := kernel.CheckExpectedRevision(kernel.Revision(req.Revision), kernel.Revision(record.Subgraph.Revision)); err != nil {
		return err
	}
	delete(working.subgraphs, req.SubgraphID)
	for id, node := range working.nodes {
		node.Node.SubgraphIDs = removeString(node.Node.SubgraphIDs, req.SubgraphID)
		working.nodes[id] = node
	}
	for key, edge := range working.edges {
		if edge.FromRef == "subgraph:"+req.SubgraphID {
			delete(working.edges, key)
		}
	}
	if err := appendMutationEvents(working, s.now().UTC(), principal, "context.subgraph.delete", "context.subgraph.deleted", req.SubgraphID, req); err != nil {
		return err
	}
	working.graphRevision++
	s.replaceLocked(working)
	return nil
}

func validateGeneralNode(principal auth.Principal, subgraphs map[string]SubgraphRecord, node ContextNode, creation NodeCreationContext) error {
	if err := validateNewNode(principal, subgraphs, node, creation); err != nil {
		return err
	}
	return rejectTaskMembership(subgraphs, node.SubgraphIDs)
}

func parseExpectedRevision(value string) (kernel.Revision, error) {
	if value == "" {
		return kernel.LatestRevision, kernel.InvalidArgument("source_revision is required")
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil || parsed == 0 {
		return kernel.LatestRevision, kernel.InvalidArgument("source_revision must be a positive integer")
	}
	return kernel.Revision(parsed), nil
}

func changedSubgraphMemberships(before, after []string) []string {
	changed := make(map[string]struct{})
	beforeSet := make(map[string]struct{}, len(before))
	afterSet := make(map[string]struct{}, len(after))
	for _, id := range before {
		beforeSet[id] = struct{}{}
	}
	for _, id := range after {
		afterSet[id] = struct{}{}
		if _, ok := beforeSet[id]; !ok {
			changed[id] = struct{}{}
		}
	}
	for _, id := range before {
		if _, ok := afterSet[id]; !ok {
			changed[id] = struct{}{}
		}
	}
	out := make([]string, 0, len(changed))
	for id := range changed {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func setGeneralSubgraphMembers(principal auth.Principal, working *memoryData, subgraphID string, nodeIDs []string) error {
	wanted := make(map[string]struct{}, len(nodeIDs))
	for _, nodeID := range uniqueStrings(nodeIDs) {
		record, ok := working.nodes[nodeID]
		if !ok || record.ProjectID != principal.ProjectID {
			return kernel.Error{Code: kernel.CodeNotFound, Message: "node not found"}
		}
		if err := rejectTaskMembership(working.subgraphs, record.Node.SubgraphIDs); err != nil {
			return err
		}
		wanted[nodeID] = struct{}{}
	}
	for id, record := range working.nodes {
		if record.ProjectID != principal.ProjectID {
			continue
		}
		_, shouldHave := wanted[id]
		has := contains(record.Node.SubgraphIDs, subgraphID)
		if shouldHave && !has {
			record.Node.SubgraphIDs = uniqueStrings(append(record.Node.SubgraphIDs, subgraphID))
			working.nodes[id] = record
		}
		if !shouldHave && has {
			record.Node.SubgraphIDs = removeString(record.Node.SubgraphIDs, subgraphID)
			working.nodes[id] = record
		}
	}
	return nil
}

func appendMutationEvents(working *memoryData, now time.Time, principal auth.Principal, auditAction, outboxTopic, subjectID string, payload any) error {
	if err := working.appendAudit(AuditEvent{
		ID:        fmt.Sprintf("audit-%d", len(working.audit)+1),
		ProjectID: principal.ProjectID,
		ActorID:   principal.ActorPrincipalID,
		Action:    auditAction,
		SubjectID: subjectID,
		CreatedAt: now,
	}); err != nil {
		return err
	}
	return working.appendOutbox(OutboxEvent{
		ID:        fmt.Sprintf("outbox-%d", len(working.outbox)+1),
		Topic:     outboxTopic,
		Key:       subjectID,
		Payload:   mustJSON(payload),
		CreatedAt: now,
	})
}

func validateNewNode(principal auth.Principal, subgraphs map[string]SubgraphRecord, node ContextNode, creation NodeCreationContext) error {
	if node.ID == "" || node.Statement == "" || node.CreatorAgentID == "" {
		return kernel.InvalidArgument("node id, statement, and creator_agent_id are required")
	}
	if node.CreatorAgentID != creation.CreatorAgentID && creation.CreatorAgentID != "" {
		return kernel.Forbidden("node creator does not match trusted creation context")
	}
	if err := validateNodeKind(node.Kind); err != nil {
		return err
	}
	if err := validateNodeStatus(node.Status); err != nil {
		return err
	}
	if len(node.SourceRefs) == 0 {
		return kernel.InvalidArgument("source_refs are required")
	}
	for _, subgraphID := range node.SubgraphIDs {
		subgraph, ok := subgraphs[subgraphID]
		if !ok {
			return kernel.Error{Code: kernel.CodeNotFound, Message: "subgraph not found"}
		}
		if !canSeeSubgraph(principal, subgraph) {
			return kernel.Forbidden("subgraph is not visible to principal")
		}
		if subgraph.Subgraph.Kind != string(SubgraphKindGeneral) {
			return kernel.Forbidden("Context Agent cannot write task subgraphs")
		}
	}
	return nil
}

func rejectTaskMembership(subgraphs map[string]SubgraphRecord, subgraphIDs []string) error {
	for _, subgraphID := range subgraphIDs {
		if subgraph, ok := subgraphs[subgraphID]; ok && subgraph.Subgraph.Kind == string(SubgraphKindTask) {
			return kernel.Forbidden("Context Agent curator cannot mutate task nodes or task subgraphs")
		}
	}
	return nil
}

func (s *memoryData) addEdge(edge ContextEdge) error {
	if s.failNext == FaultEdge {
		s.failNext = ""
		return fmt.Errorf("injected edge failure")
	}
	if edge.FromRef == "" || edge.ToNodeID == "" || edge.Kind == "" {
		return kernel.InvalidArgument("edge fields are required")
	}
	key := edgeKey(edge)
	s.edges[key] = edge
	return nil
}

func (s *memoryData) appendAudit(event AuditEvent) error {
	if s.failNext == FaultAudit {
		s.failNext = ""
		return fmt.Errorf("injected audit failure")
	}
	s.audit = append(s.audit, event)
	return nil
}

func (s *memoryData) appendOutbox(event OutboxEvent) error {
	if s.failNext == FaultOutbox {
		s.failNext = ""
		return fmt.Errorf("injected outbox failure")
	}
	s.outbox = append(s.outbox, event)
	return nil
}

func (s *MemoryStore) cloneLocked() *memoryData {
	clone := &memoryData{
		graphRevision: s.graphRevision,
		nextSeq:       s.nextSeq,
		nodes:         make(map[string]NodeRecord, len(s.nodes)),
		subgraphs:     make(map[string]SubgraphRecord, len(s.subgraphs)),
		edges:         make(map[string]ContextEdge, len(s.edges)),
		audit:         append([]AuditEvent(nil), s.audit...),
		outbox:        append([]OutboxEvent(nil), s.outbox...),
		failNext:      s.failNext,
	}
	for id, record := range s.nodes {
		record.Node = cloneNode(record.Node)
		clone.nodes[id] = record
	}
	for id, record := range s.subgraphs {
		record.Subgraph = cloneSubgraph(record.Subgraph)
		clone.subgraphs[id] = record
	}
	for key, edge := range s.edges {
		clone.edges[key] = edge
	}
	return clone
}

func (s *MemoryStore) replaceLocked(working *memoryData) {
	s.graphRevision = working.graphRevision
	s.nextSeq = working.nextSeq
	s.nodes = working.nodes
	s.subgraphs = working.subgraphs
	s.edges = working.edges
	s.audit = working.audit
	s.outbox = working.outbox
	s.failNext = working.failNext
}

func (s *MemoryStore) sortedNodesLocked() []NodeRecord {
	nodes := make([]NodeRecord, 0, len(s.nodes))
	for _, record := range s.nodes {
		nodes = append(nodes, record)
	}
	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].Sequence == nodes[j].Sequence {
			return nodes[i].Node.ID < nodes[j].Node.ID
		}
		return nodes[i].Sequence < nodes[j].Sequence
	})
	return nodes
}

func (s *MemoryStore) sortedEdgesLocked() []ContextEdge {
	edges := make([]ContextEdge, 0, len(s.edges))
	for _, edge := range s.edges {
		edges = append(edges, edge)
	}
	sort.Slice(edges, func(i, j int) bool { return edgeKey(edges[i]) < edgeKey(edges[j]) })
	return edges
}

func normalizeNode(node ContextNode) ContextNode {
	node.SubgraphIDs = uniqueStrings(node.SubgraphIDs)
	node.SourceRefs = uniqueStrings(node.SourceRefs)
	return node
}

func cloneNode(node ContextNode) ContextNode {
	node.SubgraphIDs = append([]string(nil), node.SubgraphIDs...)
	node.SourceRefs = append([]string(nil), node.SourceRefs...)
	return node
}

func cloneSubgraph(subgraph ContextSubgraph) ContextSubgraph {
	return subgraph
}

func edgeKey(edge ContextEdge) string {
	return edge.FromRef + "\x00" + edge.ToNodeID + "\x00" + edge.Kind
}

func uniqueStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func contains(values []string, needle string) bool {
	for _, value := range values {
		if value == needle {
			return true
		}
	}
	return false
}

func intersects(a, b []string) bool {
	for _, left := range a {
		if contains(b, left) {
			return true
		}
	}
	return false
}

func removeString(values []string, remove string) []string {
	out := values[:0]
	for _, value := range values {
		if value != remove {
			out = append(out, value)
		}
	}
	return append([]string(nil), out...)
}

func lowerAll(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(strings.ToLower(value))
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}

func keywordMatch(statement string, keywords []string) bool {
	if len(keywords) == 0 {
		return true
	}
	lower := strings.ToLower(statement)
	for _, keyword := range keywords {
		if !strings.Contains(lower, keyword) {
			return false
		}
	}
	return true
}

func mustJSON(value any) []byte {
	data, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return data
}
