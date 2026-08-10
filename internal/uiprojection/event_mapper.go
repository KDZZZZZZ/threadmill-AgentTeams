package uiprojection

import (
	"context"
	"encoding/json"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

type RawEvent struct {
	EventID       string              `json:"event_id"`
	Cursor        string              `json:"cursor"`
	Type          string              `json:"type"`
	ProjectID     kernel.ProjectID    `json:"project_id"`
	TaskID        kernel.TaskID       `json:"task_id,omitempty"`
	EndpointID    kernel.EndpointID   `json:"endpoint_id,omitempty"`
	InvocationID  kernel.InvocationID `json:"invocation_id,omitempty"`
	GraphRevision kernel.Revision     `json:"-"`
	OccurredAt    time.Time           `json:"occurred_at"`
	Payload       json.RawMessage     `json:"payload,omitempty"`
}

type UIEvent struct {
	EventID    string            `json:"event_id"`
	Cursor     string            `json:"cursor"`
	Type       string            `json:"type"`
	OccurredAt time.Time         `json:"occurred_at"`
	ProjectID  kernel.ProjectID  `json:"project_id"`
	TaskID     kernel.TaskID     `json:"task_id,omitempty"`
	EndpointID kernel.EndpointID `json:"endpoint_id,omitempty"`
	Payload    map[string]any    `json:"payload"`
}

type EventPage struct {
	Events     []UIEvent `json:"events"`
	NextCursor string    `json:"next_cursor"`
}

type EventQuery interface {
	ListEvents(context.Context, auth.Principal, kernel.ProjectID, string, int) (EventPage, error)
	SubscribeEvents(context.Context, auth.Principal, kernel.ProjectID, string) (<-chan UIEvent, error)
}

type EventMapper struct {
	permissions PermissionReader
}

func NewEventMapper(permissions PermissionReader) EventMapper {
	return EventMapper{permissions: permissions}
}

func (m EventMapper) Map(ctx context.Context, principal auth.Principal, event RawEvent) (UIEvent, bool, error) {
	if event.EventID == "" || event.Cursor == "" || event.Type == "" || event.ProjectID == "" || event.OccurredAt.IsZero() {
		return UIEvent{}, false, kernel.InvalidArgument("event_id, cursor, type, project_id, and occurred_at are required")
	}
	if m.permissions == nil {
		return UIEvent{}, false, kernel.Forbidden("UI event permission reader is not configured")
	}
	if err := requireEventProject(ctx, m.permissions, principal, event.ProjectID); err != nil {
		return UIEvent{}, false, err
	}
	if event.TaskID != "" {
		grant, err := m.permissions.TaskGrant(ctx, principal, event.ProjectID, event.TaskID)
		if err != nil {
			return UIEvent{}, false, err
		}
		if !grant.Visible {
			return UIEvent{}, false, nil
		}
	}

	eventType, ok := canonicalUIEventType(event.Type)
	if !ok {
		return UIEvent{}, false, nil
	}
	payload, err := projectEventPayload(eventType, event)
	if err != nil {
		return UIEvent{}, false, err
	}
	ui := UIEvent{
		EventID:    event.EventID,
		Cursor:     event.Cursor,
		Type:       eventType,
		OccurredAt: event.OccurredAt.UTC(),
		ProjectID:  event.ProjectID,
		TaskID:     event.TaskID,
		EndpointID: event.EndpointID,
		Payload:    payload,
	}
	return ui, true, nil
}

func canonicalUIEventType(raw string) (string, bool) {
	switch raw {
	case "capacity.updated",
		"graph.revision",
		"task.updated",
		"endpoint.updated",
		"invocation.updated",
		"subscription.updated",
		"context.delta",
		"task_memory_buffer.updated",
		"manager.interaction":
		return raw, true
	case "coordination.graph_revised":
		return "graph.revision", true
	case "CapacityAdjusted":
		return "capacity.updated", true
	case "PhaseActivated", "PhaseOutputSubmitted", "PhaseResultInvalidated":
		return "endpoint.updated", true
	case "AgentInvocationStarted", "AgentInvocationFinished", "AgentInvocationFailed", "PhaseInvocationStarted", "PhaseInvocationFailed", "PhaseInvocationStopped":
		return "invocation.updated", true
	case "ContextSubscriptionCreated", "ContextSubscriptionCancelled", "ContextSubscriptionExpired":
		return "subscription.updated", true
	case "ContextDeltaDelivered", "ContextDeltaConsumed", "ContextGraphCommitted":
		return "context.delta", true
	case "MemoryCandidateBuffered", "MemoryCandidateRejected", "CandidateBufferFrozen", "CandidateReviewAccepted", "CandidateReviewRejected":
		return "task_memory_buffer.updated", true
	case "TaskManagerDecisionSubmitted", "TaskManagerDecisionFinalized", "HumanDecisionRequested", "HumanDecisionRecorded", "OrchestrationProposalSubmitted", "OrchestrationProposalDecided":
		return "manager.interaction", true
	case "manager.message_recorded", "manager.decision_recorded":
		return "manager.interaction", true
	case "inspector.updated":
		return "invocation.updated", true
	default:
		return "", false
	}
}

type CapacityEventData struct {
	Capacity CapacityState `json:"capacity"`
}

type GraphRevisedEventData struct {
	Revision        kernel.Revision `json:"revision"`
	ManagerInputRef string          `json:"manager_input_ref,omitempty"`
	DecisionRef     string          `json:"decision_ref,omitempty"`
}

type TaskUpdatedEventData struct {
	TaskID kernel.TaskID `json:"task_id"`
	Status string        `json:"status"`
}

type EndpointUpdatedEventData struct {
	Endpoint            coordination.PhaseEndpointRef `json:"endpoint"`
	Generation          int                           `json:"generation"`
	State               string                        `json:"state"`
	LatestInvocationRef kernel.InvocationID           `json:"latest_invocation_ref,omitempty"`
}

type ManagerMessageEventData struct {
	ConversationID  string    `json:"conversation_id"`
	EntryID         string    `json:"entry_id"`
	Kind            string    `json:"kind"`
	CreatedAt       time.Time `json:"created_at"`
	ManagerInputRef string    `json:"manager_input_ref,omitempty"`
	Body            string    `json:"body,omitempty"`
}

type ManagerDecisionEventData struct {
	ConversationID string          `json:"conversation_id"`
	EntryID        string          `json:"entry_id"`
	DecisionRef    string          `json:"decision_ref"`
	GraphRevision  kernel.Revision `json:"graph_revision,omitempty"`
	Disposition    string          `json:"disposition,omitempty"`
}

type InspectorUpdatedEventData struct {
	Endpoint     coordination.PhaseEndpointRef `json:"endpoint"`
	Generation   int                           `json:"generation"`
	InvocationID kernel.InvocationID           `json:"invocation_id,omitempty"`
}

type InvocationUpdatedEventData struct {
	Endpoint     coordination.PhaseEndpointRef `json:"endpoint"`
	Generation   int                           `json:"generation,omitempty"`
	InvocationID kernel.InvocationID           `json:"invocation_id,omitempty"`
	Status       string                        `json:"status,omitempty"`
}

type SubscriptionUpdatedEventData struct {
	Endpoint       coordination.PhaseEndpointRef `json:"endpoint"`
	Generation     int                           `json:"generation,omitempty"`
	InvocationID   kernel.InvocationID           `json:"invocation_id,omitempty"`
	SubscriptionID string                        `json:"subscription_id,omitempty"`
	SubgraphIDs    []string                      `json:"subgraph_ids,omitempty"`
	Active         *bool                         `json:"active,omitempty"`
	Source         string                        `json:"source,omitempty"`
}

type ContextDeltaEventData struct {
	Endpoint        coordination.PhaseEndpointRef `json:"endpoint"`
	Generation      int                           `json:"generation,omitempty"`
	InvocationID    kernel.InvocationID           `json:"invocation_id,omitempty"`
	ContextSliceRef string                        `json:"context_slice_ref,omitempty"`
	Revision        string                        `json:"revision,omitempty"`
	SubgraphIDs     []string                      `json:"subgraph_ids,omitempty"`
}

type TaskMemoryBufferUpdatedEventData struct {
	TaskID                kernel.TaskID       `json:"task_id"`
	TaskMemoryBufferRef   string              `json:"task_memory_buffer_ref,omitempty"`
	CreatedByInvocationID kernel.InvocationID `json:"created_by_invocation_id,omitempty"`
	CandidateID           string              `json:"candidate_id,omitempty"`
}

type ManagerInteractionEventData struct {
	ConversationID  string          `json:"conversation_id,omitempty"`
	EntryID         string          `json:"entry_id,omitempty"`
	Kind            string          `json:"kind,omitempty"`
	CreatedAt       time.Time       `json:"created_at,omitempty"`
	ManagerInputRef string          `json:"manager_input_ref,omitempty"`
	DecisionRef     string          `json:"decision_ref,omitempty"`
	GraphRevision   kernel.Revision `json:"graph_revision,omitempty"`
	Body            string          `json:"body,omitempty"`
	Disposition     string          `json:"disposition,omitempty"`
}

func requireEventProject(ctx context.Context, permissions PermissionReader, principal auth.Principal, projectID kernel.ProjectID) error {
	if principal.Kind != auth.PrincipalOperator || principal.Role != auth.RoleOperator || principal.ProjectID != projectID {
		return kernel.Forbidden("UI events require an authenticated project operator")
	}
	allowed, err := permissions.CanReadProject(ctx, principal, projectID)
	if err != nil {
		return err
	}
	if !allowed {
		return kernel.Forbidden("operator is not allowed for project")
	}
	return nil
}

func decodeEventPayload(payload json.RawMessage, target any) error {
	if len(payload) == 0 {
		return kernel.InvalidArgument("structured UI event payload is required")
	}
	if err := json.Unmarshal(payload, target); err != nil {
		return kernel.InvalidArgument("structured UI event payload is invalid")
	}
	return nil
}

func projectEventPayload(eventType string, event RawEvent) (map[string]any, error) {
	var projected any
	switch eventType {
	case "capacity.updated":
		var payload CapacityState
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return nil, kernel.InvalidArgument("capacity event payload is invalid")
		}
		if payload.ProjectID == "" {
			var legacy CapacityEventData
			if err := decodeEventPayload(event.Payload, &legacy); err != nil {
				return nil, err
			}
			payload = legacy.Capacity
		}
		if payload.ProjectID == "" {
			payload.ProjectID = event.ProjectID
		}
		projected = payload
	case "graph.revision":
		var payload GraphRevisedEventData
		if err := decodeEventPayload(event.Payload, &payload); err != nil {
			return nil, err
		}
		if payload.Revision == 0 {
			payload.Revision = event.GraphRevision
		}
		projected = payload
	case "task.updated":
		var payload TaskUpdatedEventData
		if err := decodeEventPayload(event.Payload, &payload); err != nil {
			return nil, err
		}
		if payload.TaskID == "" {
			payload.TaskID = event.TaskID
		}
		projected = payload
	case "endpoint.updated":
		var payload EndpointUpdatedEventData
		if err := decodeEventPayload(event.Payload, &payload); err != nil {
			return nil, err
		}
		payload.Endpoint = fillEventEndpoint(payload.Endpoint, event)
		if payload.LatestInvocationRef == "" {
			payload.LatestInvocationRef = event.InvocationID
		}
		projected = payload
	case "invocation.updated":
		var payload InvocationUpdatedEventData
		if err := decodeEventPayload(event.Payload, &payload); err != nil {
			return nil, err
		}
		payload.Endpoint = fillEventEndpoint(payload.Endpoint, event)
		if payload.InvocationID == "" {
			payload.InvocationID = event.InvocationID
		}
		projected = payload
	case "subscription.updated":
		var payload SubscriptionUpdatedEventData
		if err := decodeEventPayload(event.Payload, &payload); err != nil {
			return nil, err
		}
		payload.Endpoint = fillEventEndpoint(payload.Endpoint, event)
		if payload.InvocationID == "" {
			payload.InvocationID = event.InvocationID
		}
		projected = payload
	case "context.delta":
		var payload ContextDeltaEventData
		if err := decodeEventPayload(event.Payload, &payload); err != nil {
			return nil, err
		}
		payload.Endpoint = fillEventEndpoint(payload.Endpoint, event)
		if payload.InvocationID == "" {
			payload.InvocationID = event.InvocationID
		}
		projected = payload
	case "task_memory_buffer.updated":
		var payload TaskMemoryBufferUpdatedEventData
		if err := decodeEventPayload(event.Payload, &payload); err != nil {
			return nil, err
		}
		if payload.TaskID == "" {
			payload.TaskID = event.TaskID
		}
		if payload.CreatedByInvocationID == "" {
			payload.CreatedByInvocationID = event.InvocationID
		}
		projected = payload
	case "manager.interaction":
		var payload ManagerInteractionEventData
		if err := decodeEventPayload(event.Payload, &payload); err != nil {
			return nil, err
		}
		projected = payload
	default:
		return nil, kernel.InvalidArgument("UI event type is not supported")
	}

	raw, err := json.Marshal(projected)
	if err != nil {
		return nil, kernel.InvalidArgument("structured UI event payload is invalid")
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil || out == nil {
		return nil, kernel.InvalidArgument("structured UI event payload must be a JSON object")
	}
	return out, nil
}

func fillEventEndpoint(endpoint coordination.PhaseEndpointRef, event RawEvent) coordination.PhaseEndpointRef {
	if endpoint.TaskID == "" {
		endpoint.TaskID = event.TaskID
	}
	if endpoint.EndpointID == "" {
		endpoint.EndpointID = event.EndpointID
	}
	return endpoint
}

func CursorExpired(cursor string) error {
	return kernel.Error{Code: kernel.ErrorCode("cursor_expired"), Message: "event cursor expired", Recoverable: true, Details: map[string]string{"cursor": cursor}}
}

func IsCursorExpired(err error) bool {
	return kernel.ErrorCodeOf(err) == kernel.ErrorCode("cursor_expired")
}
