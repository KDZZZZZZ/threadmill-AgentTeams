package contextgraph

import (
	"context"
	"sort"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

func (s *MemoryStore) CreateInitialSlice(ctx context.Context, principal auth.Principal, subgraphIDs []string) (ContextSlice, error) {
	return s.EnsureInitialSlice(ctx, principal, subgraphIDs)
}

func (s *MemoryStore) EnsureInitialSlice(ctx context.Context, principal auth.Principal, subgraphIDs []string) (ContextSlice, error) {
	if err := ctx.Err(); err != nil {
		return ContextSlice{}, err
	}
	if err := requireTool(principal, auth.ToolContextSubscribe, principal.ProjectID); err != nil {
		return ContextSlice{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if active := s.activeSubscriptionIDsLocked(principal.ProjectID, consumerInvocationID(principal)); len(active) > 0 {
		return s.materializeInvocationSliceLocked(principal, consumerInvocationID(principal)), nil
	}
	sub, err := s.createSubscription(s.now().UTC(), principal, SubscribeRequest{SubgraphIDs: subgraphIDs}, subscriptionSourceInitial)
	if err != nil {
		return ContextSlice{}, err
	}
	slice := s.materializeInvocationSliceLocked(principal, consumerInvocationID(principal))
	slice.SubscriptionIDs = []string{sub.ID}
	return slice, nil
}

func (s *MemoryStore) EffectiveSubgraphs(ctx context.Context, principal auth.Principal) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if consumerInvocationID(principal) == "" {
		return nil, kernel.InvalidArgument("consumer invocation is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.effectiveSubgraphsLocked(principal, consumerInvocationID(principal)), nil
}

func (s *MemoryStore) MaterializeRuntimeContext(ctx context.Context, principal auth.Principal) (ContextSlice, error) {
	if err := ctx.Err(); err != nil {
		return ContextSlice{}, err
	}
	if consumerInvocationID(principal) == "" {
		return ContextSlice{}, kernel.InvalidArgument("consumer invocation is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.materializeInvocationSliceLocked(principal, consumerInvocationID(principal)), nil
}

func (s *MemoryStore) materializeInvocationSliceLocked(principal auth.Principal, invocationID kernel.InvocationID) ContextSlice {
	effective := s.effectiveSubgraphsLocked(principal, invocationID)
	effectiveSet := make(map[string]struct{}, len(effective))
	for _, subgraphID := range effective {
		effectiveSet[subgraphID] = struct{}{}
	}
	var nodes []ContextNode
	seen := map[string]struct{}{}
	for _, record := range s.sortedNodesLocked() {
		if _, ok := seen[record.Node.ID]; ok || !canSeeNode(principal, s.subgraphs, record) {
			continue
		}
		if !intersects(record.Node.SubgraphIDs, effective) {
			continue
		}
		node := visibleNode(principal, s.subgraphs, record.Node)
		node.SubgraphIDs = node.SubgraphIDs[:0]
		for _, subgraphID := range record.Node.SubgraphIDs {
			if _, ok := effectiveSet[subgraphID]; ok {
				node.SubgraphIDs = append(node.SubgraphIDs, subgraphID)
			}
		}
		nodes = append(nodes, node)
		seen[record.Node.ID] = struct{}{}
	}
	return ContextSlice{
		Nodes:           nodes,
		SubscriptionIDs: s.activeSubscriptionIDsLocked(principal.ProjectID, invocationID),
		GraphRevision:   int64(s.graphRevision),
	}
}

func (s *MemoryStore) effectiveSubgraphsLocked(principal auth.Principal, invocationID kernel.InvocationID) []string {
	set := map[string]struct{}{}
	for _, record := range s.subscriptions {
		if record.ProjectID != principal.ProjectID || record.Subscription.ConsumerInvocationID != string(invocationID) || !record.Active {
			continue
		}
		for _, subgraphID := range record.Subscription.SubgraphIDs {
			subgraph, ok := s.subgraphs[subgraphID]
			if ok && canSeeSubgraph(principal, subgraph) {
				set[subgraphID] = struct{}{}
			}
		}
	}
	out := make([]string, 0, len(set))
	for subgraphID := range set {
		out = append(out, subgraphID)
	}
	sort.Strings(out)
	return out
}

func (s *MemoryStore) activeSubscriptionIDsLocked(projectID kernel.ProjectID, invocationID kernel.InvocationID) []string {
	var ids []string
	for id, record := range s.subscriptions {
		if record.ProjectID == projectID && record.Subscription.ConsumerInvocationID == string(invocationID) && record.Active {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}
