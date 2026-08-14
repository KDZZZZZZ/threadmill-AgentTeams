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
	key := invocationScopeKey{ProjectID: principal.ProjectID, InvocationID: consumerInvocationID(principal)}
	if initial, ok := s.initialSlices[key]; ok {
		return cloneContextSlice(initial), nil
	}
	var initialSubscription ContextSubscription
	for _, record := range s.subscriptions {
		if record.ProjectID == principal.ProjectID &&
			record.Subscription.ConsumerInvocationID == string(consumerInvocationID(principal)) &&
			record.Source == subscriptionSourceInitial && record.Active {
			initialSubscription = record.Subscription
			break
		}
	}
	if initialSubscription.ID == "" {
		var err error
		initialSubscription, err = s.createSubscription(s.now().UTC(), principal, SubscribeRequest{SubgraphIDs: subgraphIDs}, subscriptionSourceInitial)
		if err != nil {
			return ContextSlice{}, err
		}
	}
	slice := s.materializeInvocationSliceLocked(principal, consumerInvocationID(principal))
	slice.SubscriptionIDs = []string{initialSubscription.ID}
	s.initialSlices[key] = cloneContextSlice(slice)
	return cloneContextSlice(slice), nil
}

// InspectInitialSlice returns the immutable startup baseline captured by
// EnsureInitialSlice. It deliberately does not recompute the active
// subscription union; EndInvocation may expire every subscription while this
// snapshot remains available for provenance and GUI inspection.
func (s *MemoryStore) InspectInitialSlice(ctx context.Context, principal auth.Principal, invocationID kernel.InvocationID) (ContextSlice, error) {
	if err := ctx.Err(); err != nil {
		return ContextSlice{}, err
	}
	invocationID, err := authorizeInitialSliceInspection(principal, invocationID)
	if err != nil {
		return ContextSlice{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	initial, ok := s.initialSlices[invocationScopeKey{ProjectID: principal.ProjectID, InvocationID: invocationID}]
	if !ok {
		return ContextSlice{}, kernel.Error{Code: kernel.CodeNotFound, Message: "initial context slice not found"}
	}
	return cloneContextSlice(initial), nil
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

func authorizeInitialSliceInspection(principal auth.Principal, invocationID kernel.InvocationID) (kernel.InvocationID, error) {
	if principal.ProjectID == "" {
		return "", kernel.Forbidden("initial slice inspection requires project principal")
	}
	if invocationID == "" {
		invocationID = consumerInvocationID(principal)
	}
	switch principal.Kind {
	case auth.PrincipalAgent:
		if invocationID == "" || invocationID != principal.InvocationID {
			return "", kernel.Forbidden("agent initial slice inspection is limited to the authenticated invocation")
		}
	case auth.PrincipalOperator:
		if principal.Role != auth.RoleOperator || invocationID == "" {
			return "", kernel.Forbidden("operator initial slice inspection requires an invocation")
		}
	default:
		return "", kernel.Forbidden("initial slice inspection requires an authenticated principal")
	}
	return invocationID, nil
}

func cloneContextSlice(slice ContextSlice) ContextSlice {
	out := ContextSlice{
		SubscriptionIDs: append([]string(nil), slice.SubscriptionIDs...),
		GraphRevision:   slice.GraphRevision,
		Nodes:           make([]ContextNode, len(slice.Nodes)),
	}
	for i, node := range slice.Nodes {
		out.Nodes[i] = cloneNode(node)
	}
	return out
}
