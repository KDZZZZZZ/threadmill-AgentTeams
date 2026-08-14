package evidence

import (
	"context"
	"strings"
	"testing"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/objectstore"
)

func TestEventLogAppendOnlyStableKeyReplayAndPayloadLimit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	log := NewEventLog(32)

	first, err := log.Append(ctx, AppendEvent{
		StableKey:     "phase-output-1",
		Type:          "PhaseOutputSubmitted",
		ProjectID:     "project-1",
		TaskID:        "task-1",
		WorkspaceRef:  "ws-1",
		PhaseEndpoint: "plan",
		Payload:       map[string]string{"ok": "yes"},
	})
	if err != nil {
		t.Fatalf("append first event: %v", err)
	}
	again, err := log.Append(ctx, AppendEvent{
		StableKey:     "phase-output-1",
		Type:          "PhaseOutputSubmitted",
		ProjectID:     "project-1",
		TaskID:        "task-1",
		WorkspaceRef:  "ws-1",
		PhaseEndpoint: "plan",
		Payload:       map[string]string{"ok": "yes"},
	})
	if err != nil {
		t.Fatalf("idempotent append: %v", err)
	}
	if first.ID != again.ID || first.Sequence != again.Sequence {
		t.Fatalf("stable key did not return original event: first=%+v again=%+v", first, again)
	}

	if _, err := log.Append(ctx, AppendEvent{
		StableKey:     "phase-output-1",
		Type:          "PhaseOutputSubmitted",
		ProjectID:     "project-1",
		TaskID:        "task-1",
		WorkspaceRef:  "ws-1",
		PhaseEndpoint: "execute",
		Payload:       map[string]string{"ok": "yes"},
	}); !kernel.IsCode(err, kernel.CodeIdempotencyConflict) {
		t.Fatalf("stable key conflict error = %v", err)
	}

	if _, err := log.Append(ctx, AppendEvent{Type: "ToolOutputCaptured", Payload: "small"}); !kernel.IsCode(err, kernel.CodeInvalidRequest) {
		t.Fatalf("missing stable key error = %v", err)
	}

	if _, err := log.Append(ctx, AppendEvent{StableKey: "too-large", Type: "ToolOutputCaptured", Payload: strings.Repeat("x", 80)}); !kernel.IsCode(err, kernel.CodeInvalidRequest) {
		t.Fatalf("oversize payload error = %v", err)
	}

	second, err := log.Append(ctx, AppendEvent{StableKey: "verify-1", Type: "VerifyPassed", ProjectID: "project-1", TaskID: "task-1", Payload: map[string]string{"result": "pass"}})
	if err != nil {
		t.Fatalf("append second event: %v", err)
	}
	events, cursor, err := log.Replay(ctx, first.Sequence, 10)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if cursor != second.Sequence || len(events) != 1 || events[0].ID != second.ID {
		t.Fatalf("replay got events=%+v cursor=%d, want second event cursor", events, cursor)
	}
}

func TestArtifactRegistryDedupeHashACLAndProjection(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	registry := NewArtifactRegistry(objectstore.NewMemoryStore(), "artifacts")
	body := []byte("private transcript bytes")
	first, err := registry.Register(ctx, RegisterArtifact{
		Type:      ArtifactAgentTranscript,
		ProjectID: "project-1",
		Path:      "evidence/transcript.txt",
		Body:      body,
		TaskID:    "task-1",
	})
	if err != nil {
		t.Fatalf("register transcript: %v", err)
	}
	second, err := registry.Register(ctx, RegisterArtifact{
		Type:      ArtifactAgentTranscript,
		ProjectID: "project-1",
		Path:      "evidence/again.txt",
		Body:      body,
		TaskID:    "task-2",
	})
	if err != nil {
		t.Fatalf("register duplicate transcript: %v", err)
	}
	if first.ID != second.ID {
		t.Fatalf("duplicate content got different artifact IDs: %q != %q", first.ID, second.ID)
	}

	if _, _, err := registry.Open(ctx, Principal{}, first.ID); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("empty principal read error = %v", err)
	}
	if _, _, err := registry.Open(ctx, Principal{Role: RoleAuditor, ProjectID: "project-2", TaskID: "task-1"}, first.ID); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("cross-project read error = %v", err)
	}
	if _, _, err := registry.Open(ctx, Principal{Role: RoleAuditor, ProjectID: "project-1", TaskID: "task-3"}, first.ID); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("cross-task read error = %v", err)
	}
	if _, _, err := registry.Open(ctx, Principal{Role: RoleTaskManager, ProjectID: "project-1", TaskID: "task-1"}, first.ID); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("task manager transcript read error = %v", err)
	}
	artifact, got, err := registry.Open(ctx, Principal{Role: RoleAuditor, ProjectID: "project-1", TaskID: "task-1"}, first.ID)
	if err != nil {
		t.Fatalf("auditor open transcript: %v", err)
	}
	if artifact.ContentHash != hashBytes(body) || string(got) != string(body) {
		t.Fatalf("artifact hash/body mismatch: artifact=%+v body=%q", artifact, got)
	}
	if _, _, err := registry.Open(ctx, Principal{Role: RoleAuditor, ProjectID: "project-1", TaskID: "task-2"}, first.ID); err != nil {
		t.Fatalf("duplicate content did not grant second task: %v", err)
	}

	report, err := registry.Register(ctx, RegisterArtifact{
		Type:      ArtifactGeneratedReport,
		ProjectID: "project-1",
		Path:      "evidence/report.txt",
		Body:      body,
		TaskID:    "task-1",
	})
	if err != nil {
		t.Fatalf("register report with same bytes: %v", err)
	}
	if report.ID == first.ID || report.Type != ArtifactGeneratedReport {
		t.Fatalf("cross-type same bytes shared artifact identity: report=%+v transcript=%+v", report, first)
	}
	if _, _, err := registry.Open(ctx, Principal{Role: RoleTaskManager, ProjectID: "project-1", TaskID: "task-1"}, report.ID); err != nil {
		t.Fatalf("task manager should read generated report with transcript bytes: %v", err)
	}

	if _, err := registry.Register(ctx, RegisterArtifact{Type: ArtifactToolOutput, ProjectID: "project-1", TaskID: "task-1", Path: "sessions/raw.txt", Body: []byte("ok")}); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("sensitive path error = %v", err)
	}
	if _, err := registry.Register(ctx, RegisterArtifact{Type: ArtifactToolOutput, ProjectID: "project-1", TaskID: "task-1", Path: "evidence/raw.txt", Body: []byte("password=secret")}); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("sensitive content error = %v", err)
	}

	log := NewEventLog(1024)
	if _, err := log.Append(ctx, AppendEvent{StableKey: "artifact-event", Type: "AgentInvocationFinished", ProjectID: "project-1", TaskID: "task-1", ArtifactRefs: []ArtifactID{first.ID}}); err != nil {
		t.Fatalf("append artifact event: %v", err)
	}
	projection := NewProjectionReader(log, registry)
	taskManagerEvents, _, err := projection.ReadTask(ctx, Principal{Role: RoleTaskManager, ProjectID: "project-1", TaskID: "task-1"}, "task-1", 0, 10)
	if err != nil {
		t.Fatalf("task manager projection: %v", err)
	}
	if len(taskManagerEvents) != 1 || len(taskManagerEvents[0].ArtifactRefs) != 0 {
		t.Fatalf("task manager projection leaked transcript refs: %+v", taskManagerEvents)
	}
}

func TestProjectionReadTaskScansInterleavedEventsAndIsolatesSameTaskIDAcrossProjects(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	log := NewEventLog(1024)
	for _, event := range []AppendEvent{
		{StableKey: "other-1", Type: "PhaseOutputSubmitted", ProjectID: "project-1", TaskID: "task-other"},
		{StableKey: "match-1", Type: "PhaseOutputSubmitted", ProjectID: "project-1", TaskID: "task-1"},
		{StableKey: "same-task-other-project", Type: "PhaseOutputSubmitted", ProjectID: "project-2", TaskID: "task-1"},
		{StableKey: "other-2", Type: "PhaseOutputSubmitted", ProjectID: "project-1", TaskID: "task-other"},
		{StableKey: "match-2", Type: "VerifyPassed", ProjectID: "project-1", TaskID: "task-1"},
	} {
		if _, err := log.Append(ctx, event); err != nil {
			t.Fatalf("append %s: %v", event.StableKey, err)
		}
	}

	projection := NewProjectionReader(log, nil)
	events, cursor, err := projection.ReadTask(ctx, Principal{Role: RoleAuditor, ProjectID: "project-1", TaskID: "task-1"}, "task-1", 0, 2)
	if err != nil {
		t.Fatalf("read task projection: %v", err)
	}
	if len(events) != 2 || events[0].StableKey != "match-1" || events[1].StableKey != "match-2" {
		t.Fatalf("projection events = %+v, want both interleaved matches", events)
	}
	if cursor != events[1].Sequence {
		t.Fatalf("cursor = %d, want last scanned match %d", cursor, events[1].Sequence)
	}
	project2Events, _, err := projection.ReadTask(ctx, Principal{Role: RoleAuditor, ProjectID: "project-2", TaskID: "task-1"}, "task-1", 0, 10)
	if err != nil {
		t.Fatalf("cross-project same task projection: %v", err)
	}
	if len(project2Events) != 1 || project2Events[0].StableKey != "same-task-other-project" {
		t.Fatalf("cross-project same task events = %+v, want isolated project-2 event", project2Events)
	}
	if _, _, err := projection.ReadTask(ctx, Principal{Role: RoleAuditor, ProjectID: "project-1", TaskID: "task-other"}, "task-1", 0, 1); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("cross-task projection error = %v", err)
	}
}
