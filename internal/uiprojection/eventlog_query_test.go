package uiprojection

import (
	"context"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/evidence"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

func TestEventLogQueryListsMappedEventsWithStableCursorAndACL(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	projectID := kernel.ProjectID("project-1")
	taskID := kernel.TaskID("task-a")
	hiddenTaskID := kernel.TaskID("task-hidden")
	log := evidence.NewEventLog(4096)
	query := NewEventLogQuery(log, &fakePermissions{
		project: true,
		grants: map[kernel.TaskID]TaskReadGrant{
			taskID:       {Visible: true},
			hiddenTaskID: {Visible: false},
		},
	}, WithEventLogReplayBatchSize(2))

	appendUIEvent(t, query, "foreign", "task.updated", "project-2", taskID, map[string]any{"task_id": taskID, "status": "running"})
	appendUIEvent(t, query, "hidden", "task.updated", projectID, hiddenTaskID, map[string]any{"task_id": hiddenTaskID, "status": "running"})
	appendUIEvent(t, query, "unknown", "agent.raw_transcript", projectID, taskID, map[string]any{"raw_tool_output": "secret"})
	appendUIEvent(t, query, "visible-1", "task.updated", projectID, taskID, map[string]any{"task_id": taskID, "status": "running"})
	appendUIEvent(t, query, "visible-2", "endpoint.updated", projectID, taskID, EndpointUpdatedEventData{
		Endpoint:   coordination.PhaseEndpointRef{TaskID: taskID, EndpointID: coordination.EndpointExecute},
		Generation: 1,
		State:      "running",
	})

	page, err := query.ListEvents(ctx, operator(projectID), projectID, "", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 1 || page.Events[0].EventID != "evt_000000000004" || page.Events[0].Cursor != "4" {
		t.Fatalf("page events = %+v", page.Events)
	}
	if page.NextCursor != "4" {
		t.Fatalf("page cursor = %q, want cursor 4", page.NextCursor)
	}

	next, err := query.ListEvents(ctx, operator(projectID), projectID, page.NextCursor, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(next.Events) != 1 || next.Events[0].Cursor != "5" {
		t.Fatalf("next page = %+v", next.Events)
	}
	if next.NextCursor != "5" {
		t.Fatalf("next cursor = %q, want 5", next.NextCursor)
	}
}

func TestEventLogQueryCursorRetentionExpiryAndCurrentCursor(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	projectID := kernel.ProjectID("project-1")
	log := evidence.NewEventLog(4096)
	query := NewEventLogQuery(log, &fakePermissions{project: true}, WithEventLogRetentionFloor(2))
	appendUIEvent(t, query, "one", "capacity.updated", projectID, "", CapacityEventData{Capacity: CapacityState{ProjectID: projectID, UpdatedAt: time.Now().UTC()}})
	appendUIEvent(t, query, "two", "capacity.updated", projectID, "", CapacityEventData{Capacity: CapacityState{ProjectID: projectID, UpdatedAt: time.Now().UTC()}})

	if _, err := query.ListEvents(ctx, operator(projectID), projectID, "1", 10); !IsCursorExpired(err) {
		t.Fatalf("expired cursor err = %v", err)
	}
	cursor, err := query.CurrentCursor(ctx, projectID)
	if err != nil {
		t.Fatal(err)
	}
	if cursor != "2" {
		t.Fatalf("current cursor = %q, want 2", cursor)
	}
}

func TestEventLogQueryRequiresProjectACLBeforeReplay(t *testing.T) {
	t.Parallel()
	projectID := kernel.ProjectID("project-1")
	query := NewEventLogQuery(evidence.NewEventLog(4096), &fakePermissions{project: false})

	if _, err := query.ListEvents(context.Background(), operator(projectID), projectID, "", 10); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("ListEvents err = %v, want forbidden", err)
	}
	if _, err := query.SubscribeEvents(context.Background(), operator(projectID), projectID, ""); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("SubscribeEvents err = %v, want forbidden", err)
	}
}

func TestEventLogQuerySubscribeReplaysThenStreamsWithoutDuplicates(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	projectID := kernel.ProjectID("project-1")
	taskID := kernel.TaskID("task-a")
	log := evidence.NewEventLog(4096)
	query := NewEventLogQuery(log, &fakePermissions{project: true, grants: map[kernel.TaskID]TaskReadGrant{taskID: {Visible: true}}})
	appendUIEvent(t, query, "existing", "task.updated", projectID, taskID, map[string]any{"task_id": taskID, "status": "pending"})

	stream, err := query.SubscribeEvents(ctx, operator(projectID), projectID, "")
	if err != nil {
		t.Fatal(err)
	}
	first := readStreamEvent(t, stream)
	if first.EventID != "evt_000000000001" || first.Cursor != "1" {
		t.Fatalf("first event = %+v, want replayed cursor 1", first)
	}
	appendUIEvent(t, query, "live", "task.updated", projectID, taskID, map[string]any{"task_id": taskID, "status": "running"})
	second := readStreamEvent(t, stream)
	if second.EventID != "evt_000000000002" || second.Cursor != "2" {
		t.Fatalf("second event = %+v, want live cursor 2", second)
	}

	if _, err := query.Append(ctx, evidence.AppendEvent{StableKey: "live", Type: "task.updated", ProjectID: projectID, TaskID: taskID, Payload: map[string]any{"task_id": taskID, "status": "running"}}); err != nil {
		t.Fatal(err)
	}
	select {
	case duplicate := <-stream:
		t.Fatalf("duplicate idempotent event delivered: %+v", duplicate)
	case <-time.After(50 * time.Millisecond):
	}
}

func appendUIEvent(t *testing.T, query *EventLogQuery, key kernel.IdempotencyKey, eventType string, projectID kernel.ProjectID, taskID kernel.TaskID, payload any) {
	t.Helper()
	if _, err := query.Append(context.Background(), evidence.AppendEvent{
		StableKey: key,
		Type:      eventType,
		ProjectID: projectID,
		TaskID:    taskID,
		Payload:   payload,
	}); err != nil {
		t.Fatalf("append %s: %v", key, err)
	}
}

func readStreamEvent(t *testing.T, stream <-chan UIEvent) UIEvent {
	t.Helper()
	select {
	case event, ok := <-stream:
		if !ok {
			t.Fatal("stream closed")
		}
		return event
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for stream event")
		return UIEvent{}
	}
}
