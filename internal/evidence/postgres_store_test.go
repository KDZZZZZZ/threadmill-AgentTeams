package evidence

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/objectstore"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/outbox"
	platformpostgres "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/postgres"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/migrations"
	_ "github.com/jackc/pgx/v5/stdlib"
)

const defaultEvidencePostgresDSN = "postgres://threadmill_test@127.0.0.1:5432/threadmill_test?sslmode=disable"

func TestPostgresEventStoreRealDatabaseIdempotencyReplayFiltersAndOutbox(t *testing.T) {
	ctx := context.Background()
	db := openEvidencePostgresSchema(t, ctx)

	events := NewPostgresEventStore(db, 64)
	first, err := events.AppendWithOutbox(ctx, AppendEvent{
		StableKey: "phase-output-real-pg",
		Type:      "PhaseOutputSubmitted",
		ProjectID: "project-real",
		TaskID:    "task-real",
		Payload:   map[string]string{"status": "done"},
	}, []outbox.Event{{
		ID:      "outbox-phase-output-real-pg",
		Topic:   "ui.events",
		Key:     "phase-output-real-pg",
		Payload: []byte(`{"event":"phase-output-real-pg"}`),
	}})
	if err != nil {
		t.Fatalf("append with outbox: %v", err)
	}
	again, err := events.AppendWithOutbox(ctx, AppendEvent{
		StableKey: "phase-output-real-pg",
		Type:      "PhaseOutputSubmitted",
		ProjectID: "project-real",
		TaskID:    "task-real",
		Payload:   map[string]string{"status": "done"},
	}, []outbox.Event{{
		ID:      "outbox-phase-output-real-pg-duplicate",
		Topic:   "ui.events",
		Key:     "phase-output-real-pg",
		Payload: []byte(`{"event":"duplicate"}`),
	}})
	if err != nil {
		t.Fatalf("idempotent append with outbox: %v", err)
	}
	if first.ID != again.ID || first.Sequence != again.Sequence {
		t.Fatalf("idempotent append returned different event: first=%+v again=%+v", first, again)
	}
	var outboxRows int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM platform_outbox_events`).Scan(&outboxRows); err != nil {
		t.Fatalf("count outbox rows: %v", err)
	}
	if outboxRows != 1 {
		t.Fatalf("outbox rows = %d, want exactly one side effect", outboxRows)
	}
	if _, err := events.Append(ctx, AppendEvent{
		StableKey: "phase-output-real-pg",
		Type:      "PhaseOutputSubmitted",
		ProjectID: "project-real",
		TaskID:    "task-real",
		Payload:   map[string]string{"status": "changed"},
	}); !kernel.IsCode(err, kernel.CodeIdempotencyConflict) {
		t.Fatalf("conflicting append = %v, want idempotency_conflict", err)
	}
	if _, err := events.Append(ctx, AppendEvent{
		StableKey: "too-large-real-pg",
		Type:      "ToolOutputCaptured",
		ProjectID: "project-real",
		TaskID:    "task-real",
		Payload:   strings.Repeat("x", 128),
	}); !kernel.IsCode(err, kernel.CodeInvalidRequest) {
		t.Fatalf("oversize payload = %v, want invalid_request", err)
	}
	second, err := events.Append(ctx, AppendEvent{
		StableKey: "verify-real-pg",
		Type:      "VerifyPassed",
		ProjectID: "project-real",
		TaskID:    "task-real",
		Payload:   map[string]string{"result": "pass"},
	})
	if err != nil {
		t.Fatalf("append second event: %v", err)
	}
	if _, err := events.Append(ctx, AppendEvent{StableKey: "foreign-real-pg", Type: "VerifyPassed", ProjectID: "project-other", TaskID: "task-other"}); err != nil {
		t.Fatalf("append foreign event: %v", err)
	}
	sameTaskOtherProject, err := events.Append(ctx, AppendEvent{StableKey: "same-task-other-project-real-pg", Type: "VerifyPassed", ProjectID: "project-other", TaskID: "task-real"})
	if err != nil {
		t.Fatalf("append same task id in other project: %v", err)
	}

	replayed, cursor, err := events.Replay(ctx, first.Sequence, 1)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(replayed) != 1 || replayed[0].ID != second.ID || cursor != second.Sequence {
		t.Fatalf("replay events=%+v cursor=%d, want second event", replayed, cursor)
	}
	reader := NewProjectionReader(events, nil)
	taskEvents, taskCursor, err := reader.ReadTask(ctx, Principal{Role: RoleAuditor, ProjectID: "project-real", TaskID: "task-real"}, "task-real", 0, 10)
	if err != nil {
		t.Fatalf("read task projection: %v", err)
	}
	if len(taskEvents) != 2 || taskCursor != second.Sequence {
		t.Fatalf("task projection events=%+v cursor=%d, want only matching project/task", taskEvents, taskCursor)
	}
	otherProjectEvents, _, err := reader.ReadTask(ctx, Principal{Role: RoleAuditor, ProjectID: "project-other", TaskID: "task-real"}, "task-real", 0, 10)
	if err != nil {
		t.Fatalf("read same task id in other project: %v", err)
	}
	if len(otherProjectEvents) != 1 || otherProjectEvents[0].ID != sameTaskOtherProject.ID {
		t.Fatalf("other project task events=%+v, want isolated other-project event", otherProjectEvents)
	}
}

func TestPostgresArtifactRegistryRealDatabaseMetadataGrantsAndConcurrentDedupe(t *testing.T) {
	ctx := context.Background()
	db := openEvidencePostgresSchema(t, ctx)
	registry := NewPostgresArtifactRegistry(db, objectstore.NewMemoryStore(), "artifacts")
	body := []byte("same artifact bytes")

	const writers = 12
	start := make(chan struct{})
	type outcome struct {
		id  ArtifactID
		err error
	}
	outcomes := make(chan outcome, writers)
	var ready sync.WaitGroup
	ready.Add(writers)
	for i := 0; i < writers; i++ {
		taskID := kernel.TaskID(fmt.Sprintf("task-%02d", i))
		go func() {
			ready.Done()
			<-start
			artifact, err := registry.Register(ctx, RegisterArtifact{
				Type:      ArtifactTestOutput,
				ProjectID: "project-real",
				TaskID:    taskID,
				Path:      "evidence/output.txt",
				Body:      body,
			})
			if err != nil {
				outcomes <- outcome{err: err}
				return
			}
			outcomes <- outcome{id: artifact.ID}
		}()
	}
	ready.Wait()
	close(start)
	var want ArtifactID
	for i := 0; i < writers; i++ {
		outcome := <-outcomes
		if outcome.err != nil {
			t.Fatalf("concurrent register: %v", outcome.err)
		}
		if want == "" {
			want = outcome.id
			continue
		}
		if outcome.id != want {
			t.Fatalf("concurrent duplicate got artifact ID %q, want %q", outcome.id, want)
		}
	}
	var artifactRows, grantRows int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM evidence_artifacts`).Scan(&artifactRows); err != nil {
		t.Fatalf("count artifact rows: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM evidence_artifact_grants`).Scan(&grantRows); err != nil {
		t.Fatalf("count grant rows: %v", err)
	}
	if artifactRows != 1 || grantRows != writers {
		t.Fatalf("artifact rows=%d grant rows=%d, want one metadata row and all task grants", artifactRows, grantRows)
	}
	artifact, got, err := registry.Open(ctx, Principal{Role: RoleAuditor, ProjectID: "project-real", TaskID: "task-00"}, want)
	if err != nil {
		t.Fatalf("open artifact: %v", err)
	}
	if artifact.ContentHash != hashBytes(body) || string(got) != string(body) {
		t.Fatalf("opened artifact hash/body mismatch: artifact=%+v body=%q", artifact, got)
	}

	transcript, err := registry.Register(ctx, RegisterArtifact{
		Type:      ArtifactAgentTranscript,
		ProjectID: "project-real",
		TaskID:    "task-manager-scope",
		Path:      "evidence/transcript.txt",
		Body:      []byte("private transcript"),
	})
	if err != nil {
		t.Fatalf("register transcript: %v", err)
	}
	if _, _, err := registry.Open(ctx, Principal{Role: RoleTaskManager, ProjectID: "project-real", TaskID: "task-manager-scope"}, transcript.ID); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("task manager transcript read = %v, want forbidden", err)
	}
	if _, _, err := registry.Open(ctx, Principal{Role: RoleAuditor, ProjectID: "project-real", TaskID: "task-manager-scope"}, transcript.ID); err != nil {
		t.Fatalf("auditor transcript read: %v", err)
	}
}

func TestPostgresArtifactRegistryRealDatabaseSeparatesSameBytesByTypeForACL(t *testing.T) {
	ctx := context.Background()
	db := openEvidencePostgresSchema(t, ctx)
	registry := NewPostgresArtifactRegistry(db, objectstore.NewMemoryStore(), "artifacts")
	body := []byte("same bytes used as report and transcript")

	report, err := registry.Register(ctx, RegisterArtifact{
		Type:      ArtifactGeneratedReport,
		ProjectID: "project-real",
		TaskID:    "task-real",
		Path:      "evidence/report.txt",
		Body:      body,
	})
	if err != nil {
		t.Fatalf("register report: %v", err)
	}
	transcript, err := registry.Register(ctx, RegisterArtifact{
		Type:      ArtifactAgentTranscript,
		ProjectID: "project-real",
		TaskID:    "task-real",
		Path:      "evidence/transcript.txt",
		Body:      body,
	})
	if err != nil {
		t.Fatalf("register transcript with same bytes: %v", err)
	}
	if report.ID == transcript.ID {
		t.Fatalf("same bytes across artifact types shared ID %q", report.ID)
	}
	var artifactRows int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM evidence_artifacts WHERE content_hash = $1`, hashBytes(body)).Scan(&artifactRows); err != nil {
		t.Fatalf("count same-hash artifacts: %v", err)
	}
	if artifactRows != 2 {
		t.Fatalf("same-hash artifact rows = %d, want one per type", artifactRows)
	}
	principal := Principal{Role: RoleTaskManager, ProjectID: "project-real", TaskID: "task-real"}
	if _, _, err := registry.Open(ctx, principal, report.ID); err != nil {
		t.Fatalf("task manager should read report: %v", err)
	}
	if _, _, err := registry.Open(ctx, principal, transcript.ID); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("task manager transcript read = %v, want forbidden", err)
	}
}

func TestEvidenceHardeningRollbackRejectsTypedHashDuplicatesWithoutChangingSchema(t *testing.T) {
	ctx := context.Background()
	db := openEvidencePostgresSchema(t, ctx)
	registry := NewPostgresArtifactRegistry(db, objectstore.NewMemoryStore(), "artifacts")
	body := []byte("same bytes make the legacy uniqueness constraint unsafe")

	for _, artifactType := range []ArtifactType{ArtifactGeneratedReport, ArtifactAgentTranscript} {
		if _, err := registry.Register(ctx, RegisterArtifact{
			Type:      artifactType,
			ProjectID: "project-rollback",
			TaskID:    "task-rollback",
			Path:      "evidence/rollback.txt",
			Body:      body,
		}); err != nil {
			t.Fatalf("register %s artifact: %v", artifactType, err)
		}
	}

	loaded, err := platformpostgres.LoadMigrations(migrations.FS, migrations.Dir)
	if err != nil {
		t.Fatalf("load migrations: %v", err)
	}
	err = platformpostgres.NewMigrator(db).Rollback(ctx, loaded, "3002")
	if err == nil || !strings.Contains(err.Error(), "evidence hardening rollback blocked") {
		t.Fatalf("rollback error = %v, want explicit unsafe rollback rejection", err)
	}

	var applied bool
	if err := db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = '3002')`).Scan(&applied); err != nil {
		t.Fatalf("read migration state after rejected rollback: %v", err)
	}
	if !applied {
		t.Fatal("rejected rollback removed migration state")
	}
	var compositeConstraint bool
	if err := db.QueryRowContext(ctx, `
SELECT EXISTS (
	SELECT 1
	FROM pg_constraint
	WHERE conrelid = 'evidence_artifacts'::regclass
		AND conname = 'evidence_artifacts_type_content_hash_key'
)`).Scan(&compositeConstraint); err != nil {
		t.Fatalf("read artifact constraint after rejected rollback: %v", err)
	}
	if !compositeConstraint {
		t.Fatal("rejected rollback changed the typed artifact uniqueness constraint")
	}
}

func TestPostgresArtifactRegistryWithRealMinIOWhenConfigured(t *testing.T) {
	endpoint := os.Getenv("THREADMILL_MINIO_ENDPOINT")
	accessKey := os.Getenv("THREADMILL_MINIO_ACCESS_KEY")
	secretKey := os.Getenv("THREADMILL_MINIO_SECRET_KEY")
	if endpoint == "" || accessKey == "" || secretKey == "" {
		t.Skip("THREADMILL_MINIO_ENDPOINT, THREADMILL_MINIO_ACCESS_KEY, and THREADMILL_MINIO_SECRET_KEY are required for real MinIO integration")
	}
	ctx := context.Background()
	db := openEvidencePostgresSchema(t, ctx)
	bucket := os.Getenv("THREADMILL_MINIO_BUCKET")
	if bucket == "" {
		bucket = "threadmill-evidence-test"
	}
	store, err := objectstore.NewMinIOStore(objectstore.MinIOConfig{
		Endpoint:  endpoint,
		AccessKey: accessKey,
		SecretKey: secretKey,
		Bucket:    bucket,
		Secure:    os.Getenv("THREADMILL_MINIO_SECURE") == "1",
	})
	if err != nil {
		t.Fatalf("create MinIO store: %v", err)
	}
	registry := NewPostgresArtifactRegistry(db, store, bucket)
	body := []byte("real minio artifact bytes")
	artifact, err := registry.Register(ctx, RegisterArtifact{
		Type:        ArtifactGeneratedReport,
		ProjectID:   "project-minio",
		TaskID:      "task-minio",
		Path:        "evidence/report.txt",
		ContentType: "text/plain",
		Body:        body,
	})
	if err != nil {
		t.Fatalf("register artifact in MinIO: %v", err)
	}
	opened, got, err := registry.Open(ctx, Principal{Role: RoleAuditor, ProjectID: "project-minio", TaskID: "task-minio"}, artifact.ID)
	if err != nil {
		t.Fatalf("open artifact from MinIO: %v", err)
	}
	if opened.ContentHash != hashBytes(body) || string(got) != string(body) {
		t.Fatalf("MinIO artifact mismatch: artifact=%+v body=%q", opened, got)
	}
}

func openEvidencePostgresSchema(t *testing.T, ctx context.Context) *sql.DB {
	t.Helper()
	dsn := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = defaultEvidencePostgresDSN
	}
	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open postgres admin connection: %v", err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	if err := admin.PingContext(ctx); err != nil {
		t.Fatalf("ping postgres using %s: %v", dsn, err)
	}
	schema := fmt.Sprintf("threadmill_evidence_it_%d_%d", os.Getpid(), time.Now().UnixNano())
	if _, err := admin.ExecContext(ctx, `CREATE SCHEMA `+quoteEvidenceIdent(schema)); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS `+quoteEvidenceIdent(schema)+` CASCADE`)
	})

	isolatedDSN, err := evidenceDSNWithSearchPath(dsn, schema)
	if err != nil {
		t.Fatalf("add search_path to dsn: %v", err)
	}
	db, err := sql.Open("pgx", isolatedDSN)
	if err != nil {
		t.Fatalf("open isolated postgres connection: %v", err)
	}
	db.SetMaxOpenConns(24)
	db.SetMaxIdleConns(24)
	t.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping isolated postgres: %v", err)
	}
	loaded, err := platformpostgres.LoadMigrations(migrations.FS, migrations.Dir)
	if err != nil {
		t.Fatalf("load migrations: %v", err)
	}
	if err := platformpostgres.NewMigrator(db).Apply(ctx, loaded); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	return db
}

func evidenceDSNWithSearchPath(dsn, schema string) (string, error) {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return "", err
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func quoteEvidenceIdent(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}
