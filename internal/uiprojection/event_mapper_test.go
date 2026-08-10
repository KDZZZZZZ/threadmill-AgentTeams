package uiprojection

import (
	"context"
	"encoding/json"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

func TestEventMapperProjectsOnlyWhitelistedStructuredEventsAfterACL(t *testing.T) {
	t.Parallel()
	projectID := kernel.ProjectID("project-1")
	taskID := kernel.TaskID("task-a")
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	permissions := &fakePermissions{project: true, grants: map[kernel.TaskID]TaskReadGrant{taskID: {Visible: true}}}
	mapper := NewEventMapper(permissions)
	payload := mustJSON(t, EndpointUpdatedEventData{
		Endpoint:            coordination.PhaseEndpointRef{TaskID: taskID, EndpointID: coordination.EndpointExecute},
		Generation:          2,
		State:               "running",
		LatestInvocationRef: "inv-1",
	})

	got, ok, err := mapper.Map(context.Background(), operator(projectID), RawEvent{
		EventID:    "evt-1",
		Cursor:     "42",
		Type:       "endpoint.updated",
		ProjectID:  projectID,
		TaskID:     taskID,
		EndpointID: coordination.EndpointExecute,
		OccurredAt: now,
		Payload:    payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ok || got.Type != "endpoint.updated" || got.Cursor != "42" || got.EventID != "evt-1" || got.TaskID != taskID || got.EndpointID != coordination.EndpointExecute {
		t.Fatalf("mapped event = %#v ok=%v", got, ok)
	}
	if got.OccurredAt != now || got.Payload["state"] != "running" || got.Payload["latest_invocation_ref"] != "inv-1" {
		t.Fatalf("event payload = %#v occurred_at=%s", got.Payload, got.OccurredAt)
	}
}

func TestEventMapperSupportsCanonicalOpenAPIEventTypes(t *testing.T) {
	t.Parallel()
	projectID := kernel.ProjectID("project-1")
	taskID := kernel.TaskID("task-a")
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	mapper := NewEventMapper(&fakePermissions{project: true, grants: map[kernel.TaskID]TaskReadGrant{taskID: {Visible: true}}})

	active := true
	tests := map[string]any{
		"capacity.updated":           CapacityState{ProjectID: projectID, Revision: 1, UpdatedAt: now},
		"graph.revision":             GraphRevisedEventData{Revision: 2},
		"task.updated":               TaskUpdatedEventData{TaskID: taskID, Status: "running"},
		"endpoint.updated":           EndpointUpdatedEventData{Endpoint: coordination.PhaseEndpointRef{TaskID: taskID, EndpointID: coordination.EndpointExecute}, Generation: 1, State: "running"},
		"invocation.updated":         InvocationUpdatedEventData{Endpoint: coordination.PhaseEndpointRef{TaskID: taskID, EndpointID: coordination.EndpointExecute}, InvocationID: "inv-1", Status: "running"},
		"subscription.updated":       SubscriptionUpdatedEventData{Endpoint: coordination.PhaseEndpointRef{TaskID: taskID, EndpointID: coordination.EndpointExecute}, SubscriptionID: "sub-1", Active: &active},
		"context.delta":              ContextDeltaEventData{Endpoint: coordination.PhaseEndpointRef{TaskID: taskID, EndpointID: coordination.EndpointExecute}, InvocationID: "inv-1", Revision: "ctx-2"},
		"task_memory_buffer.updated": TaskMemoryBufferUpdatedEventData{TaskID: taskID, TaskMemoryBufferRef: "memory-1", CandidateID: "candidate-1"},
		"manager.interaction":        ManagerInteractionEventData{ConversationID: "conv-1", EntryID: "entry-1", Kind: "decision", DecisionRef: "decision-1"},
	}
	for eventType, payload := range tests {
		got, ok, err := mapper.Map(context.Background(), operator(projectID), RawEvent{
			EventID:    "evt-" + eventType,
			Cursor:     "7",
			Type:       eventType,
			ProjectID:  projectID,
			TaskID:     taskID,
			EndpointID: coordination.EndpointExecute,
			OccurredAt: now,
			Payload:    mustJSON(t, payload),
		})
		if err != nil {
			t.Fatalf("%s err = %v", eventType, err)
		}
		if !ok || got.Type != eventType || len(got.Payload) == 0 {
			t.Fatalf("%s mapped got=%#v ok=%v", eventType, got, ok)
		}
	}
}

func TestEventMapperStripsUnapprovedPayloadFields(t *testing.T) {
	t.Parallel()
	projectID := kernel.ProjectID("project-1")
	taskID := kernel.TaskID("task-a")
	mapper := NewEventMapper(&fakePermissions{project: true, grants: map[kernel.TaskID]TaskReadGrant{taskID: {Visible: true}}})

	got, ok, err := mapper.Map(context.Background(), operator(projectID), RawEvent{
		EventID:    "evt-safe",
		Cursor:     "8",
		Type:       "endpoint.updated",
		ProjectID:  projectID,
		TaskID:     taskID,
		EndpointID: coordination.EndpointExecute,
		OccurredAt: time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC),
		Payload:    json.RawMessage(`{"generation":1,"state":"running","raw_tool_output":"secret","transcript":"private"}`),
	})
	if err != nil || !ok {
		t.Fatalf("Map() got=%#v ok=%v err=%v", got, ok, err)
	}
	if _, exists := got.Payload["raw_tool_output"]; exists {
		t.Fatalf("payload leaked raw tool output: %#v", got.Payload)
	}
	if _, exists := got.Payload["transcript"]; exists {
		t.Fatalf("payload leaked transcript: %#v", got.Payload)
	}
}

func TestUIEventAndEventPageJSONMatchOpenAPIShape(t *testing.T) {
	t.Parallel()
	event := UIEvent{
		EventID:    "evt-1",
		Cursor:     "1",
		Type:       "task.updated",
		OccurredAt: time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC),
		ProjectID:  "project-1",
		TaskID:     "task-a",
		EndpointID: coordination.EndpointExecute,
		Payload:    map[string]any{"status": "running"},
	}
	rawEvent, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	var eventObject map[string]any
	if err := json.Unmarshal(rawEvent, &eventObject); err != nil {
		t.Fatal(err)
	}
	if got, want := sortedKeys(eventObject), []string{"cursor", "endpoint_id", "event_id", "occurred_at", "payload", "project_id", "task_id", "type"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("UIEvent JSON keys = %v, want %v; body=%s", got, want, rawEvent)
	}

	rawPage, err := json.Marshal(EventPage{Events: []UIEvent{event}, NextCursor: "1"})
	if err != nil {
		t.Fatal(err)
	}
	var pageObject map[string]any
	if err := json.Unmarshal(rawPage, &pageObject); err != nil {
		t.Fatal(err)
	}
	if got, want := sortedKeys(pageObject), []string{"events", "next_cursor"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("EventPage JSON keys = %v, want %v; body=%s", got, want, rawPage)
	}
}

func TestEventMapperIgnoresUnknownBeforePayloadAndHiddenTaskBeforeProjection(t *testing.T) {
	t.Parallel()
	projectID := kernel.ProjectID("project-1")
	taskID := kernel.TaskID("task-hidden")
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	permissions := &fakePermissions{project: true, grants: map[kernel.TaskID]TaskReadGrant{taskID: {Visible: false}}}
	mapper := NewEventMapper(permissions)

	if got, ok, err := mapper.Map(context.Background(), operator(projectID), RawEvent{
		EventID:    "evt-raw",
		Cursor:     "99",
		Type:       "agent.raw_transcript",
		ProjectID:  projectID,
		TaskID:     taskID,
		OccurredAt: now,
		Payload:    json.RawMessage(`{"raw_tool_output":"secret"}`),
	}); err != nil || ok || got.EventID != "" {
		t.Fatalf("unknown raw event mapped got=%#v ok=%v err=%v", got, ok, err)
	}

	if got, ok, err := mapper.Map(context.Background(), operator(projectID), RawEvent{
		EventID:    "evt-hidden",
		Cursor:     "100",
		Type:       "endpoint.updated",
		ProjectID:  projectID,
		TaskID:     taskID,
		OccurredAt: now,
		Payload:    json.RawMessage(`{"endpoint":{"task_id":"task-hidden","endpoint_id":"execute"},"generation":1,"state":"running","raw_tool_output":"secret"}`),
	}); err != nil || ok || got.EventID != "" {
		t.Fatalf("hidden task event mapped got=%#v ok=%v err=%v", got, ok, err)
	}
}

func TestEventMapperRejectsCrossProjectOperator(t *testing.T) {
	t.Parallel()
	mapper := NewEventMapper(&fakePermissions{project: true})
	_, _, err := mapper.Map(context.Background(), operator("project-a"), RawEvent{
		EventID:    "evt-1",
		Cursor:     "1",
		Type:       "capacity.updated",
		ProjectID:  "project-b",
		OccurredAt: time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC),
		Payload:    mustJSON(t, CapacityEventData{Capacity: CapacityState{ProjectID: "project-b", UpdatedAt: time.Now().UTC()}}),
	})
	if !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("err = %v, want forbidden", err)
	}
}

func mustJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func sortedKeys(object map[string]any) []string {
	keys := make([]string, 0, len(object))
	for key := range object {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
