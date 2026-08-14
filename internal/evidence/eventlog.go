package evidence

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

type EventID string
type ArtifactID string
type Cursor int64

type Event struct {
	ID                EventID               `json:"id"`
	Sequence          Cursor                `json:"sequence"`
	StableKey         kernel.IdempotencyKey `json:"stable_key,omitempty"`
	Type              string                `json:"type"`
	ProjectID         kernel.ProjectID      `json:"project_id,omitempty"`
	TaskID            kernel.TaskID         `json:"task_id,omitempty"`
	WorkspaceRef      kernel.BindingRef     `json:"workspace_ref,omitempty"`
	PhaseEndpoint     kernel.EndpointID     `json:"phase_endpoint,omitempty"`
	AgentInvocationID kernel.InvocationID   `json:"agent_invocation_id,omitempty"`
	Payload           json.RawMessage       `json:"payload,omitempty"`
	ArtifactRefs      []ArtifactID          `json:"artifact_refs,omitempty"`
	GraphRevision     int64                 `json:"graph_revision,omitempty"`
	CreatedAt         time.Time             `json:"created_at"`
}

type AppendEvent struct {
	StableKey         kernel.IdempotencyKey
	Type              string
	ProjectID         kernel.ProjectID
	TaskID            kernel.TaskID
	WorkspaceRef      kernel.BindingRef
	PhaseEndpoint     kernel.EndpointID
	AgentInvocationID kernel.InvocationID
	Payload           any
	ArtifactRefs      []ArtifactID
	GraphRevision     int64
}

type EventLog struct {
	mu              sync.Mutex
	maxPayloadBytes int
	events          []Event
	byStableKey     map[kernel.IdempotencyKey]stableEventRecord
	now             func() time.Time
}

type stableEventRecord struct {
	event       Event
	requestHash string
}

func NewEventLog(maxPayloadBytes int) *EventLog {
	if maxPayloadBytes <= 0 {
		maxPayloadBytes = 64 * 1024
	}
	return &EventLog{
		maxPayloadBytes: maxPayloadBytes,
		byStableKey:     make(map[kernel.IdempotencyKey]stableEventRecord),
		now:             time.Now,
	}
}

func (l *EventLog) Append(ctx context.Context, appendEvent AppendEvent) (Event, error) {
	if err := ctx.Err(); err != nil {
		return Event{}, err
	}
	if appendEvent.Type == "" {
		return Event{}, kernel.InvalidArgument("event type is required")
	}
	if appendEvent.StableKey == "" {
		return Event{}, kernel.InvalidArgument("stable event key is required")
	}
	payload, err := encodePayload(appendEvent.Payload, l.maxPayloadBytes)
	if err != nil {
		return Event{}, err
	}
	requestHash, err := canonicalEventRequestHash(appendEvent, payload)
	if err != nil {
		return Event{}, err
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if existing, ok := l.byStableKey[appendEvent.StableKey]; ok {
		if existing.requestHash != requestHash {
			return Event{}, kernel.IdempotencyConflict()
		}
		return cloneEvent(existing.event), nil
	}

	event := Event{
		ID:                EventID(fmt.Sprintf("evt_%012d", len(l.events)+1)),
		Sequence:          Cursor(len(l.events) + 1),
		StableKey:         appendEvent.StableKey,
		Type:              appendEvent.Type,
		ProjectID:         appendEvent.ProjectID,
		TaskID:            appendEvent.TaskID,
		WorkspaceRef:      appendEvent.WorkspaceRef,
		PhaseEndpoint:     appendEvent.PhaseEndpoint,
		AgentInvocationID: appendEvent.AgentInvocationID,
		Payload:           payload,
		ArtifactRefs:      append([]ArtifactID(nil), appendEvent.ArtifactRefs...),
		GraphRevision:     appendEvent.GraphRevision,
		CreatedAt:         l.now().UTC(),
	}
	l.events = append(l.events, event)
	l.byStableKey[appendEvent.StableKey] = stableEventRecord{event: event, requestHash: requestHash}
	return cloneEvent(event), nil
}

func (l *EventLog) Replay(ctx context.Context, after Cursor, limit int) ([]Event, Cursor, error) {
	if err := ctx.Err(); err != nil {
		return nil, after, err
	}
	if limit <= 0 {
		limit = 100
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	var events []Event
	for _, event := range l.events {
		if event.Sequence <= after {
			continue
		}
		events = append(events, cloneEvent(event))
		if len(events) == limit {
			break
		}
	}
	next := after
	if len(events) > 0 {
		next = events[len(events)-1].Sequence
	}
	return events, next, nil
}

func (l *EventLog) ReplayTask(ctx context.Context, projectID kernel.ProjectID, taskID kernel.TaskID, after Cursor, limit int) ([]Event, Cursor, error) {
	if err := ctx.Err(); err != nil {
		return nil, after, err
	}
	if projectID == "" {
		return nil, after, kernel.InvalidArgument("project_id is required")
	}
	if taskID == "" {
		return nil, after, kernel.InvalidArgument("task_id is required")
	}
	if limit <= 0 {
		limit = 100
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	var events []Event
	next := after
	for _, event := range l.events {
		if event.Sequence <= after {
			continue
		}
		if event.ProjectID != projectID || event.TaskID != taskID {
			continue
		}
		events = append(events, cloneEvent(event))
		next = event.Sequence
		if len(events) == limit {
			break
		}
	}
	return events, next, nil
}

type ProjectionReader struct {
	events    EventStore
	artifacts ArtifactStore
}

func NewProjectionReader(events EventStore, artifacts ArtifactStore) *ProjectionReader {
	return &ProjectionReader{events: events, artifacts: artifacts}
}

func (r *ProjectionReader) ReadTask(ctx context.Context, principal Principal, taskID kernel.TaskID, after Cursor, limit int) ([]Event, Cursor, error) {
	if r == nil || r.events == nil {
		return nil, after, kernel.InvalidArgument("event log is required")
	}
	if err := requirePrincipal(principal); err != nil {
		return nil, after, err
	}
	if taskID == "" {
		return nil, after, kernel.InvalidArgument("task_id is required")
	}
	if principal.TaskID != taskID {
		return nil, after, kernel.Forbidden("principal cannot read task projection")
	}
	if limit <= 0 {
		limit = 100
	}
	var filtered []Event
	events, next, err := r.events.ReplayTask(ctx, principal.ProjectID, taskID, after, limit)
	if err != nil {
		return nil, after, err
	}
	for _, event := range events {
		event = cloneEvent(event)
		event.ArtifactRefs = r.visibleArtifactRefs(principal, event.ArtifactRefs)
		filtered = append(filtered, event)
	}
	return filtered, next, nil
}

func (r *ProjectionReader) visibleArtifactRefs(principal Principal, refs []ArtifactID) []ArtifactID {
	if len(refs) == 0 || r.artifacts == nil {
		return append([]ArtifactID(nil), refs...)
	}
	visible := make([]ArtifactID, 0, len(refs))
	for _, ref := range refs {
		if r.artifacts.CanRead(principal, ref) {
			visible = append(visible, ref)
		}
	}
	return visible
}

func encodePayload(payload any, maxBytes int) (json.RawMessage, error) {
	if payload == nil {
		return nil, nil
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal event payload: %w", err)
	}
	if len(raw) > maxBytes {
		return nil, kernel.InvalidArgument("event payload exceeds limit; use ArtifactRef for large objects")
	}
	return append(json.RawMessage(nil), raw...), nil
}

func hashBytes(bytes []byte) string {
	sum := sha256.Sum256(bytes)
	return hex.EncodeToString(sum[:])
}

func canonicalEventRequestHash(appendEvent AppendEvent, payload json.RawMessage) (string, error) {
	request := struct {
		Type              string              `json:"type"`
		ProjectID         kernel.ProjectID    `json:"project_id,omitempty"`
		TaskID            kernel.TaskID       `json:"task_id,omitempty"`
		WorkspaceRef      kernel.BindingRef   `json:"workspace_ref,omitempty"`
		PhaseEndpoint     kernel.EndpointID   `json:"phase_endpoint,omitempty"`
		AgentInvocationID kernel.InvocationID `json:"agent_invocation_id,omitempty"`
		Payload           json.RawMessage     `json:"payload,omitempty"`
		ArtifactRefs      []ArtifactID        `json:"artifact_refs,omitempty"`
		GraphRevision     int64               `json:"graph_revision,omitempty"`
	}{
		Type:              appendEvent.Type,
		ProjectID:         appendEvent.ProjectID,
		TaskID:            appendEvent.TaskID,
		WorkspaceRef:      appendEvent.WorkspaceRef,
		PhaseEndpoint:     appendEvent.PhaseEndpoint,
		AgentInvocationID: appendEvent.AgentInvocationID,
		Payload:           payload,
		ArtifactRefs:      append([]ArtifactID(nil), appendEvent.ArtifactRefs...),
		GraphRevision:     appendEvent.GraphRevision,
	}
	raw, err := json.Marshal(request)
	if err != nil {
		return "", fmt.Errorf("marshal canonical event request: %w", err)
	}
	return hashBytes(raw), nil
}

func cloneEvent(event Event) Event {
	event.Payload = append(json.RawMessage(nil), event.Payload...)
	event.ArtifactRefs = append([]ArtifactID(nil), event.ArtifactRefs...)
	return event
}
