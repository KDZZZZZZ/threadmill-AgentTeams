package mergequeue

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/evidence"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	platformpostgres "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/postgres"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/migrations"
	_ "github.com/jackc/pgx/v5/stdlib"
)

const mergeQueueDefaultPostgresDSN = "postgres://threadmill_test@127.0.0.1:5432/threadmill_test?sslmode=disable"

func TestPostgresStoreRealPostgresMergeQueueRuntime(t *testing.T) {
	ctx := context.Background()
	db := openMergeQueuePostgresSchema(t, ctx)
	repoA := seedRepo(t)
	repoB := seedRepo(t)
	storeA := NewPostgresStore(db)
	storeB := NewPostgresStore(db)
	storeA.SetClaimTTL(80 * time.Millisecond)
	storeB.SetClaimTTL(80 * time.Millisecond)

	candidateA := insertMergeQueueCandidateFixture(t, ctx, db, storeA, "project-a", "task-a", "candidate-a", repoA)
	candidateB := insertMergeQueueCandidateFixture(t, ctx, db, storeA, "project-a", "task-b", "candidate-b", repoA)
	candidateOtherRepo := insertMergeQueueCandidateFixture(t, ctx, db, storeA, "project-a", "task-c", "candidate-c", repoB)
	candidateOtherProject := insertMergeQueueCandidateFixture(t, ctx, db, storeA, "project-b", "task-z", "candidate-z", repoB)

	first, claimed, err := storeA.ClaimNext(ctx, repoA)
	if err != nil || !claimed || first.ID != candidateA.ID || first.Status != StatusMergeCheck {
		t.Fatalf("first ClaimNext(repoA) = %#v claimed=%v err=%v", first, claimed, err)
	}
	if _, claimed, err := storeB.ClaimNext(ctx, repoA); err != nil || claimed {
		t.Fatalf("second ClaimNext(repoA) while leased = claimed=%v err=%v, want no claim", claimed, err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	claimedIDs := make(chan CandidateID, 2)
	for _, store := range []*PostgresStore{storeA, storeB} {
		wg.Add(1)
		go func(store *PostgresStore) {
			defer wg.Done()
			candidate, got, err := store.ClaimNext(ctx, repoB)
			if err != nil {
				errs <- err
				return
			}
			if got {
				claimedIDs <- candidate.ID
			}
		}(store)
	}
	wg.Wait()
	close(errs)
	close(claimedIDs)
	for err := range errs {
		if err != nil {
			t.Fatalf("parallel ClaimNext(repoB): %v", err)
		}
	}
	claimedCount := 0
	var repoBClaim CandidateID
	for id := range claimedIDs {
		claimedCount++
		repoBClaim = id
	}
	if claimedCount != 1 || repoBClaim != candidateOtherRepo.ID {
		t.Fatalf("parallel repoB claims count=%d id=%s, want exactly %s", claimedCount, repoBClaim, candidateOtherRepo.ID)
	}

	time.Sleep(120 * time.Millisecond)
	reclaimed, claimed, err := storeB.ClaimNext(ctx, repoA)
	if err != nil || !claimed || reclaimed.ID != candidateA.ID {
		t.Fatalf("expired lease reclaim = %#v claimed=%v err=%v", reclaimed, claimed, err)
	}
	restarted := NewPostgresStore(db)
	if err := storeB.ReleaseClaim(ctx, repoA, candidateA.ID); err != nil {
		t.Fatalf("ReleaseClaim: %v", err)
	}
	if err := storeB.ReleaseClaim(ctx, repoA, candidateA.ID); err != nil {
		t.Fatalf("idempotent ReleaseClaim: %v", err)
	}
	afterRestart, claimed, err := restarted.ClaimNext(ctx, repoA)
	if err != nil || !claimed || afterRestart.ID != candidateA.ID || afterRestart.Status != StatusMergeCheck {
		t.Fatalf("restart claim existing in-flight candidate = %#v claimed=%v err=%v", afterRestart, claimed, err)
	}
	if _, err := storeA.fail(ctx, candidateA.ID, StatusMergeCheck, FailureVerifyFailed, []evidence.ArtifactID{"artifact-failure-a"}, mergeAudit(candidateA, "failed-a")); err != nil {
		t.Fatalf("fail candidateA: %v", err)
	}
	failed, err := storeB.Get(ctx, candidateA.ID)
	if err != nil {
		t.Fatal(err)
	}
	if failed.Status != StatusFailed || failed.FailureReason != FailureVerifyFailed || !containsArtifact(failed.EvidenceRefs, "artifact-failure-a") {
		t.Fatalf("failed candidate = %#v", failed)
	}

	if err := restarted.ReleaseClaim(ctx, repoA, candidateA.ID); err != nil {
		t.Fatalf("release failed candidate claim: %v", err)
	}
	next, claimed, err := restarted.ClaimNext(ctx, repoA)
	if err != nil || !claimed || next.ID != candidateB.ID {
		t.Fatalf("next after failed = %#v claimed=%v err=%v, want %s", next, claimed, err, candidateB.ID)
	}
	if candidateOtherProject.ProjectID != "project-b" {
		t.Fatalf("project-b fixture mutated: %#v", candidateOtherProject)
	}

	audits, err := restarted.pendingAudits(ctx)
	if err != nil {
		t.Fatal(err)
	}
	byProject := map[kernel.ProjectID]int{}
	for _, audit := range audits {
		byProject[audit.ProjectID]++
	}
	if byProject["project-a"] == 0 || byProject["project-b"] == 0 {
		t.Fatalf("pending audits not project-scoped: %#v", byProject)
	}
	if err := restarted.markAuditDelivered(ctx, mergeAudit(candidateA, "failed-a").StableKey); err != nil {
		t.Fatalf("markAuditDelivered: %v", err)
	}
	if delivered, err := mergeAuditDelivered(ctx, db, mergeAudit(candidateA, "failed-a").StableKey); err != nil || !delivered {
		t.Fatalf("delivered audit = %v err=%v", delivered, err)
	}
}

func insertMergeQueueCandidateFixture(t *testing.T, ctx context.Context, db *sql.DB, store *PostgresStore, projectID kernel.ProjectID, taskID kernel.TaskID, id CandidateID, repo string) Candidate {
	t.Helper()
	workspaceRef := kernel.BindingRef("ws_" + string(id))
	verifyRef := evidence.ArtifactID("artifact-verify-" + string(id))
	diffRef := evidence.ArtifactID("artifact-diff-" + string(id))
	failureRef := evidence.ArtifactID("artifact-failure-" + strings.TrimPrefix(string(id), "candidate-"))
	for _, ref := range []evidence.ArtifactID{verifyRef, diffRef, failureRef} {
		insertMergeQueueArtifact(t, ctx, db, ref, projectID, taskID)
	}
	insertMergeQueueWorkspace(t, ctx, db, workspaceRef, taskID)
	candidate, err := store.enqueue(ctx, EnqueueRequest{
		ID:                id,
		ProjectID:         projectID,
		TaskID:            taskID,
		WorkspaceRef:      workspaceRef,
		VerifyResultRef:   verifyRef,
		DiffArtifactRef:   diffRef,
		TargetRepository:  repo,
		TargetBranch:      "main",
		BaseRevision:      strings.Repeat("a", 40),
		MainRevision:      strings.Repeat("b", 40),
		CandidateRevision: strings.Repeat("c", 40),
		EvidenceRefs:      []evidence.ArtifactID{verifyRef, diffRef},
	}, mergeAudit(Candidate{ID: id, ProjectID: projectID, TaskID: taskID, WorkspaceRef: workspaceRef, MainRevision: strings.Repeat("b", 40)}, "queued"))
	if err != nil {
		t.Fatalf("enqueue %s: %v", id, err)
	}
	return candidate
}

func insertMergeQueueWorkspace(t *testing.T, ctx context.Context, db *sql.DB, id kernel.BindingRef, taskID kernel.TaskID) {
	t.Helper()
	_, err := db.ExecContext(ctx, `
INSERT INTO workspace_bindings(id, task_id, generation, kind, root, base_revision, current_revision, status)
VALUES ($1, $2, 1, 'git_worktree', $3, $4, $5, 'sealed')`,
		id, taskID, filepath.Join(os.TempDir(), string(id)), strings.Repeat("a", 40), strings.Repeat("c", 40))
	if err != nil {
		t.Fatalf("insert workspace %s: %v", id, err)
	}
}

func insertMergeQueueArtifact(t *testing.T, ctx context.Context, db *sql.DB, id evidence.ArtifactID, projectID kernel.ProjectID, taskID kernel.TaskID) {
	t.Helper()
	hash := fmt.Sprintf("%064x", sha256String(string(id)))
	_, err := db.ExecContext(ctx, `
INSERT INTO evidence_artifacts(id, type, path_or_blob_ref, content_hash, size_bytes, project_id, task_id)
VALUES ($1, 'test_output', $2, $3, 1, $4, $5)
ON CONFLICT (id) DO NOTHING`, id, "memory/"+string(id), hash, projectID, taskID)
	if err != nil {
		t.Fatalf("insert artifact %s: %v", id, err)
	}
}

func mergeAudit(candidate Candidate, suffix string) auditRecord {
	return auditRecord{
		StableKey:    kernel.IdempotencyKey("merge-test:" + string(candidate.ID) + ":" + suffix),
		Type:         "MergeCandidate" + suffix,
		ProjectID:    candidate.ProjectID,
		TaskID:       candidate.TaskID,
		WorkspaceRef: candidate.WorkspaceRef,
		Payload:      map[string]string{"candidate_id": string(candidate.ID), "main_revision": candidate.MainRevision},
		ArtifactRefs: []evidence.ArtifactID{},
	}
}

func mergeAuditDelivered(ctx context.Context, db *sql.DB, key kernel.IdempotencyKey) (bool, error) {
	var delivered bool
	err := db.QueryRowContext(ctx, `SELECT delivered FROM merge_audits WHERE stable_key = $1`, key).Scan(&delivered)
	return delivered, err
}

func openMergeQueuePostgresSchema(t *testing.T, ctx context.Context) *sql.DB {
	t.Helper()
	dsn := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = mergeQueueDefaultPostgresDSN
	}
	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open postgres admin connection: %v", err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	if err := admin.PingContext(ctx); err != nil {
		t.Fatalf("ping postgres using %s: %v", dsn, err)
	}
	schema := fmt.Sprintf("threadmill_mergequeue_it_%d_%d", os.Getpid(), time.Now().UnixNano())
	if _, err := admin.ExecContext(ctx, `CREATE SCHEMA `+quoteMergeQueueIdent(schema)); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS `+quoteMergeQueueIdent(schema)+` CASCADE`)
	})
	db, err := sql.Open("pgx", mergeQueueDSNWithSearchPath(dsn, schema))
	if err != nil {
		t.Fatalf("open isolated postgres: %v", err)
	}
	db.SetMaxOpenConns(12)
	db.SetMaxIdleConns(12)
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

func mergeQueueDSNWithSearchPath(dsn, schema string) string {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return dsn
	}
	query := parsed.Query()
	query.Set("options", "-c search_path="+schema)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func quoteMergeQueueIdent(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func sha256String(value string) uint64 {
	var out uint64
	for _, b := range []byte(value) {
		out = out*131 + uint64(b)
	}
	return out
}
