package contextgraph

import (
	"context"
	"fmt"
	"sort"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

func (s *MemoryStore) RecordContextDelta(ctx context.Context, principal auth.Principal, eventID, eventKind string, subgraphIDs []string) ([]ContextDelta, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if eventID == "" || eventKind == "" {
		return nil, kernel.InvalidArgument("event_id and event_kind are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var created []ContextDelta
	for _, record := range s.subscriptions {
		if record.ProjectID != principal.ProjectID || !record.Active || !recordMatchesDelta(record.Subscription, eventKind, subgraphIDs) {
			continue
		}
		visible := visibleDeltaSubgraphs(record, s.subgraphs, subgraphIDs)
		if len(visible) == 0 {
			continue
		}
		id := fmt.Sprintf("delta-%d", len(s.deltas)+1)
		delta := ContextDelta{
			ID:             id,
			ProjectID:      string(principal.ProjectID),
			SubscriptionID: record.Subscription.ID,
			InvocationID:   record.Subscription.ConsumerInvocationID,
			EventID:        eventID,
			EventKind:      eventKind,
			SubgraphIDs:    visible,
			GraphRevision:  int64(s.graphRevision),
		}
		s.deltas[id] = delta
		created = append(created, delta)
	}
	sort.Slice(created, func(i, j int) bool { return created[i].ID < created[j].ID })
	return created, nil
}

func (s *MemoryStore) PendingDeltas(ctx context.Context, principal auth.Principal) ([]ContextDelta, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	invocationID := consumerInvocationID(principal)
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []ContextDelta
	for id, delta := range s.deltas {
		if s.deltaAck[id] || delta.ProjectID != string(principal.ProjectID) || delta.InvocationID != string(invocationID) {
			continue
		}
		record, ok := s.subscriptions[delta.SubscriptionID]
		if !ok || !record.Active || record.ProjectID != principal.ProjectID || record.Subscription.ConsumerInvocationID != string(invocationID) {
			continue
		}
		visible := visibleDeltaSubgraphs(record, s.subgraphs, delta.SubgraphIDs)
		if len(visible) == 0 {
			continue
		}
		delta.SubgraphIDs = visible
		out = append(out, delta)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (s *MemoryStore) AckDelta(ctx context.Context, principal auth.Principal, deltaID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delta, ok := s.deltas[deltaID]
	if !ok || delta.ProjectID != string(principal.ProjectID) || delta.InvocationID != string(consumerInvocationID(principal)) {
		return kernel.Error{Code: kernel.CodeNotFound, Message: "delta not found"}
	}
	record, ok := s.subscriptions[delta.SubscriptionID]
	if !ok || record.ProjectID != principal.ProjectID || record.Subscription.ConsumerInvocationID != delta.InvocationID {
		return kernel.Error{Code: kernel.CodeNotFound, Message: "delta not found"}
	}
	s.deltaAck[deltaID] = true
	return nil
}

func (s *memoryData) appendContextDeltas(principal auth.Principal, eventID, eventKind string, subgraphIDs []string) []ContextDelta {
	var created []ContextDelta
	for _, record := range s.subscriptions {
		if record.ProjectID != principal.ProjectID || !record.Active || !recordMatchesDelta(record.Subscription, eventKind, subgraphIDs) {
			continue
		}
		visible := visibleDeltaSubgraphs(record, s.subgraphs, subgraphIDs)
		if len(visible) == 0 {
			continue
		}
		id := fmt.Sprintf("delta-%d", len(s.deltas)+1)
		delta := ContextDelta{
			ID:             id,
			ProjectID:      string(principal.ProjectID),
			SubscriptionID: record.Subscription.ID,
			InvocationID:   record.Subscription.ConsumerInvocationID,
			EventID:        eventID,
			EventKind:      eventKind,
			SubgraphIDs:    visible,
			GraphRevision:  int64(s.graphRevision),
		}
		s.deltas[id] = delta
		created = append(created, delta)
	}
	sort.Slice(created, func(i, j int) bool { return created[i].ID < created[j].ID })
	return created
}

func recordMatchesDelta(subscription ContextSubscription, eventKind string, subgraphIDs []string) bool {
	if len(subscription.EventKinds) > 0 && !contains(subscription.EventKinds, eventKind) {
		return false
	}
	return intersects(subscription.SubgraphIDs, subgraphIDs)
}

func visibleDeltaSubgraphs(record ContextSubscriptionRecord, subgraphs map[string]SubgraphRecord, changedSubgraphs []string) []string {
	var visible []string
	for _, subgraphID := range uniqueStrings(changedSubgraphs) {
		if !contains(record.Subscription.SubgraphIDs, subgraphID) {
			continue
		}
		subgraph, ok := subgraphs[subgraphID]
		if ok && canSubscriptionSeeSubgraph(record, subgraph) {
			visible = append(visible, subgraphID)
		}
	}
	return visible
}

func canSubscriptionSeeSubgraph(subscription ContextSubscriptionRecord, subgraph SubgraphRecord) bool {
	if subscription.ProjectID != subgraph.ProjectID {
		return false
	}
	if subgraph.Subgraph.Kind == string(SubgraphKindGeneral) {
		return true
	}
	return subscription.TaskID != "" && subgraph.TaskID == subscription.TaskID
}
