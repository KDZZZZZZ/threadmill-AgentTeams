package contextgraph

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

const (
	subscriptionSourceExplicit = "explicit"
	subscriptionSourceInitial  = "initial_slice"
	subscriptionSourceSearch   = "search"
)

func (s *MemoryStore) Subscribe(ctx context.Context, principal auth.Principal, req SubscribeRequest) (ContextSubscription, error) {
	if err := ctx.Err(); err != nil {
		return ContextSubscription{}, err
	}
	if err := requireTool(principal, auth.ToolContextSubscribe, principal.ProjectID); err != nil {
		return ContextSubscription{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	working := s.cloneLocked()
	sub, err := working.createSubscription(s.now().UTC(), principal, req, subscriptionSourceExplicit)
	if err != nil {
		return ContextSubscription{}, err
	}
	s.replaceLocked(working)
	return sub, nil
}

func (s *MemoryStore) Unsubscribe(ctx context.Context, principal auth.Principal, subscriptionID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := requireTool(principal, auth.ToolContextUnsubscribe, principal.ProjectID); err != nil {
		return err
	}
	if strings.TrimSpace(subscriptionID) == "" {
		return kernel.InvalidArgument("subscription_id is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.subscriptions[subscriptionID]
	if !ok || record.ProjectID != principal.ProjectID || record.Subscription.ConsumerInvocationID != string(consumerInvocationID(principal)) {
		return subscriptionNotFound()
	}
	if !record.Active {
		return nil
	}
	now := s.now().UTC()
	record.Active = false
	record.CanceledAt = &now
	s.subscriptions[subscriptionID] = record
	s.audit = append(s.audit, AuditEvent{
		ID:        fmt.Sprintf("audit-%d", len(s.audit)+1),
		ProjectID: principal.ProjectID,
		ActorID:   principal.ActorPrincipalID,
		Action:    "context.subscription.cancel",
		SubjectID: subscriptionID,
		CreatedAt: now,
	})
	return nil
}

func (s *MemoryStore) EndInvocation(ctx context.Context, principal auth.Principal, invocationID kernel.InvocationID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if invocationID == "" {
		return kernel.InvalidArgument("invocation_id is required")
	}
	if principal.ProjectID == "" || principal.Kind != auth.PrincipalAgent {
		return kernel.Forbidden("invocation expiry requires agent principal")
	}
	if invocationID != principal.InvocationID {
		return kernel.Forbidden("invocation expiry is limited to the authenticated invocation")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now().UTC()
	for id, record := range s.subscriptions {
		if record.ProjectID != principal.ProjectID || record.Subscription.ConsumerInvocationID != string(invocationID) || !record.Active {
			continue
		}
		record.Active = false
		record.ExpiredAt = &now
		s.subscriptions[id] = record
	}
	return nil
}

func (s *MemoryStore) InspectSubscriptions(ctx context.Context, principal auth.Principal, invocationID kernel.InvocationID) ([]SubscriptionInspection, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
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
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []SubscriptionInspection
	for _, record := range s.subscriptions {
		if record.ProjectID != principal.ProjectID || record.Subscription.ConsumerInvocationID != string(invocationID) {
			continue
		}
		out = append(out, SubscriptionInspection{
			ID:                   record.Subscription.ID,
			ConsumerInvocationID: record.Subscription.ConsumerInvocationID,
			Source:               record.Source,
			SubgraphIDs:          append([]string(nil), record.Subscription.SubgraphIDs...),
			EventKinds:           append([]string(nil), record.Subscription.EventKinds...),
			Active:               record.Active,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (s *memoryData) createSubscription(now time.Time, principal auth.Principal, req SubscribeRequest, source string) (ContextSubscription, error) {
	invocationID, err := subscriptionConsumerInvocationID(principal, source)
	if err != nil {
		return ContextSubscription{}, err
	}
	if invocationID == "" {
		return ContextSubscription{}, kernel.InvalidArgument("consumer invocation is required")
	}
	subgraphIDs, err := s.visibleSubscriptionSubgraphs(principal, req.SubgraphIDs)
	if err != nil {
		return ContextSubscription{}, err
	}
	id := fmt.Sprintf("sub-%d", len(s.subscriptions)+1)
	for {
		if _, exists := s.subscriptions[id]; !exists {
			break
		}
		id = fmt.Sprintf("sub-%d", len(s.subscriptions)+2)
	}
	sub := ContextSubscription{
		ID:                   id,
		ConsumerInvocationID: string(invocationID),
		SubgraphIDs:          subgraphIDs,
		EventKinds:           uniqueStrings(req.EventKinds),
		PermissionSnapshot:   fmt.Sprintf("%s:%s:%s", principal.ProjectID, principal.TaskID, principal.Role),
	}
	s.subscriptions[id] = ContextSubscriptionRecord{
		Subscription: sub,
		ProjectID:    principal.ProjectID,
		TaskID:       subscriptionConsumerTaskID(principal, source),
		Role:         subscriptionConsumerRole(principal, source),
		Source:       source,
		Active:       true,
		CreatedAt:    now,
	}
	return sub, nil
}

func (s *MemoryStore) createSubscription(now time.Time, principal auth.Principal, req SubscribeRequest, source string) (ContextSubscription, error) {
	invocationID, err := subscriptionConsumerInvocationID(principal, source)
	if err != nil {
		return ContextSubscription{}, err
	}
	if invocationID == "" {
		return ContextSubscription{}, kernel.InvalidArgument("consumer invocation is required")
	}
	subgraphIDs, err := s.visibleSubscriptionSubgraphs(principal, req.SubgraphIDs)
	if err != nil {
		return ContextSubscription{}, err
	}
	id := fmt.Sprintf("sub-%d", len(s.subscriptions)+1)
	for {
		if _, exists := s.subscriptions[id]; !exists {
			break
		}
		id = fmt.Sprintf("sub-%d", len(s.subscriptions)+2)
	}
	sub := ContextSubscription{
		ID:                   id,
		ConsumerInvocationID: string(invocationID),
		SubgraphIDs:          subgraphIDs,
		EventKinds:           uniqueStrings(req.EventKinds),
		PermissionSnapshot:   fmt.Sprintf("%s:%s:%s", principal.ProjectID, principal.TaskID, principal.Role),
	}
	s.subscriptions[id] = ContextSubscriptionRecord{
		Subscription: sub,
		ProjectID:    principal.ProjectID,
		TaskID:       subscriptionConsumerTaskID(principal, source),
		Role:         subscriptionConsumerRole(principal, source),
		Source:       source,
		Active:       true,
		CreatedAt:    now,
	}
	return sub, nil
}

func (s *memoryData) visibleSubscriptionSubgraphs(principal auth.Principal, subgraphIDs []string) ([]string, error) {
	if len(subgraphIDs) == 0 {
		return nil, kernel.InvalidArgument("subgraph_ids are required")
	}
	out := make([]string, 0, len(subgraphIDs))
	for _, subgraphID := range uniqueStrings(subgraphIDs) {
		record, ok := s.subgraphs[subgraphID]
		if !ok || !canSeeSubgraph(principal, record) {
			return nil, kernel.Error{Code: kernel.CodeNotFound, Message: "subgraph not found"}
		}
		out = append(out, subgraphID)
	}
	return out, nil
}

func (s *MemoryStore) visibleSubscriptionSubgraphs(principal auth.Principal, subgraphIDs []string) ([]string, error) {
	if len(subgraphIDs) == 0 {
		return nil, kernel.InvalidArgument("subgraph_ids are required")
	}
	out := make([]string, 0, len(subgraphIDs))
	for _, subgraphID := range uniqueStrings(subgraphIDs) {
		record, ok := s.subgraphs[subgraphID]
		if !ok || !canSeeSubgraph(principal, record) {
			return nil, kernel.Error{Code: kernel.CodeNotFound, Message: "subgraph not found"}
		}
		out = append(out, subgraphID)
	}
	return out, nil
}

func consumerInvocationID(principal auth.Principal) kernel.InvocationID {
	return principal.InvocationID
}

func subscriptionConsumerInvocationID(principal auth.Principal, source string) (kernel.InvocationID, error) {
	if source == subscriptionSourceSearch {
		if principal.Role != auth.RoleContext || principal.Operation != "retrieve" {
			return "", kernel.Forbidden("search subscription requires context retrieve principal")
		}
		if principal.ConsumerInvocationID == "" {
			return "", kernel.InvalidArgument("search subscription requires trusted consumer invocation")
		}
		return principal.ConsumerInvocationID, nil
	}
	return consumerInvocationID(principal), nil
}

func subscriptionConsumerTaskID(principal auth.Principal, source string) kernel.TaskID {
	if source == subscriptionSourceSearch {
		return principal.ConsumerTaskID
	}
	return principal.TaskID
}

func subscriptionConsumerRole(principal auth.Principal, source string) string {
	if source == subscriptionSourceSearch {
		return string(principal.ConsumerRole)
	}
	return string(principal.Role)
}

func subscriptionNotFound() error {
	return kernel.Error{Code: kernel.CodeNotFound, Message: "subscription_not_found"}
}
