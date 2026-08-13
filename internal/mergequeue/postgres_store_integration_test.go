package mergequeue

import (
	"context"
	"database/sql"
	"errors"
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
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/objectstore"
	platformpostgres "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/postgres"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/workspace"
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
	storeA.SetClaimTTL(2 * time.Second)
	storeB.SetClaimTTL(2 * time.Second)

	candidateA := insertMergeQueueCandidateFixture(t, ctx, db, storeA, "project-a", "task-a", "candidate-a", repoA)
	candidateB := insertMergeQueueCandidateFixture(t, ctx, db, storeA, "project-a", "task-b", "candidate-b", repoA)
	candidateOtherRepo := insertMergeQueueCandidateFixture(t, ctx, db, storeA, "project-a", "task-c", "candidate-c", repoB)
	candidateOtherProject := insertMergeQueueCandidateFixture(t, ctx, db, storeA, "project-b", "task-z", "candidate-z", repoB)

	first, claimed, err := storeA.ClaimNext(ctx, repoA)
	if err != nil || !claimed || first.Candidate.ID != candidateA.ID || first.Candidate.Status != StatusMergeCheck {
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
				claimedIDs <- candidate.Candidate.ID
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

	storeA.SetClaimTTL(80 * time.Millisecond)
	storeB.SetClaimTTL(80 * time.Millisecond)
	first, err = storeA.RenewClaim(ctx, first)
	if err != nil {
		t.Fatalf("shorten repoA lease for expiry test: %v", err)
	}
	time.Sleep(120 * time.Millisecond)
	reclaimed, claimed, err := storeB.ClaimNext(ctx, repoA)
	if err != nil || !claimed || reclaimed.Candidate.ID != candidateA.ID {
		t.Fatalf("expired lease reclaim = %#v claimed=%v err=%v", reclaimed, claimed, err)
	}
	if _, err := storeA.fail(ctx, first, StatusMergeCheck, FailureVerifyFailed, []evidence.ArtifactID{"artifact-failure-a"}, mergeAudit(candidateA, "failed-a")); !kernel.IsCode(err, kernel.CodeLeaseConflict) {
		t.Fatalf("stale owner fail = %v, want lease_conflict", err)
	}
	if _, err := storeA.RenewClaim(ctx, first); !kernel.IsCode(err, kernel.CodeLeaseConflict) {
		t.Fatalf("stale owner renew = %v, want lease_conflict", err)
	}
	if err := storeA.ReleaseClaim(ctx, first); err != nil {
		t.Fatalf("stale owner release should be idempotent: %v", err)
	}
	if _, claimed, err := storeA.ClaimNext(ctx, repoA); err != nil || claimed {
		t.Fatalf("stale owner release deleted new claim: claimed=%v err=%v", claimed, err)
	}
	restarted := NewPostgresStore(db)
	if err := storeB.ReleaseClaim(ctx, reclaimed); err != nil {
		t.Fatalf("ReleaseClaim: %v", err)
	}
	if err := storeB.ReleaseClaim(ctx, reclaimed); err != nil {
		t.Fatalf("idempotent ReleaseClaim: %v", err)
	}
	afterRestart, claimed, err := restarted.ClaimNext(ctx, repoA)
	if err != nil || !claimed || afterRestart.Candidate.ID != candidateA.ID || afterRestart.Candidate.Status != StatusMergeCheck {
		t.Fatalf("restart claim existing in-flight candidate = %#v claimed=%v err=%v", afterRestart, claimed, err)
	}
	if _, err := restarted.fail(ctx, afterRestart, StatusMergeCheck, FailureVerifyFailed, []evidence.ArtifactID{"artifact-failure-a"}, mergeAudit(candidateA, "failed-a")); err != nil {
		t.Fatalf("fail candidateA: %v", err)
	}
	failed, err := storeB.Get(ctx, candidateA.ID)
	if err != nil {
		t.Fatal(err)
	}
	if failed.Status != StatusFailed || failed.FailureReason != FailureVerifyFailed || !containsArtifact(failed.EvidenceRefs, "artifact-failure-a") {
		t.Fatalf("failed candidate = %#v", failed)
	}

	if err := restarted.ReleaseClaim(ctx, afterRestart); err != nil {
		t.Fatalf("release failed candidate claim: %v", err)
	}
	next, claimed, err := restarted.ClaimNext(ctx, repoA)
	if err != nil || !claimed || next.Candidate.ID != candidateB.ID {
		t.Fatalf("next after failed = %#v claimed=%v err=%v, want %s", next, claimed, err, candidateB.ID)
	}
	if candidateOtherProject.ProjectID != "project-b" {
		t.Fatalf("project-b fixture mutated: %#v", candidateOtherProject)
	}

	audits, err := restarted.pendingAudits(ctx, "project-a", 64)
	if err != nil {
		t.Fatal(err)
	}
	byProject := map[kernel.ProjectID]int{}
	for _, audit := range audits {
		byProject[audit.ProjectID]++
	}
	if byProject["project-a"] == 0 || byProject["project-b"] != 0 {
		t.Fatalf("pending audits not isolated to requested project: %#v", byProject)
	}
	if err := restarted.markAuditDelivered(ctx, mergeAudit(candidateA, "failed-a").StableKey); err != nil {
		t.Fatalf("markAuditDelivered: %v", err)
	}
	if delivered, err := mergeAuditDelivered(ctx, db, mergeAudit(candidateA, "failed-a").StableKey); err != nil || !delivered {
		t.Fatalf("delivered audit = %v err=%v", delivered, err)
	}
}

func TestPostgresMergeFenceCoversIrreversibleActionPastLeaseExpiry(t *testing.T) {
	ctx := context.Background()
	db := openMergeQueuePostgresSchema(t, ctx)
	repo := seedRepo(t)
	storeA := NewPostgresStore(db)
	storeB := NewPostgresStore(db)
	storeA.SetClaimTTL(80 * time.Millisecond)
	storeB.SetClaimTTL(80 * time.Millisecond)

	candidate := insertMergeQueueCandidateFixture(t, ctx, db, storeA, "project-a", "task-fenced-action", "candidate-fenced-action", repo)
	claim, claimed, err := storeA.ClaimNext(ctx, repo)
	if err != nil || !claimed {
		t.Fatalf("ClaimNext = %#v claimed=%v err=%v", claim, claimed, err)
	}
	advanced, err := storeA.advance(ctx, claim, StatusMergeCheck, StatusTargetedVerify, nil, "")
	if err != nil {
		t.Fatalf("advance targeted verify: %v", err)
	}
	claim.Candidate = advanced

	op, err := storeA.beginMergeOperation(ctx, claim, mergeOperationRequest{
		ExpectedMainRevision:   strings.Repeat("b", 40),
		ExpectedMergedRevision: strings.Repeat("d", 40),
		Audit:                  mergeAudit(advanced, "merged-fenced-action"),
	})
	if err != nil {
		t.Fatalf("beginMergeOperation: %v", err)
	}
	time.Sleep(120 * time.Millisecond)
	if stolen, got, claimErr := storeB.ClaimNext(ctx, repo); claimErr != nil || got {
		t.Fatalf("claim during active merge operation = %#v claimed=%v err=%v", stolen, got, claimErr)
	}
	outcome, err := storeA.finalizeMergeOperation(ctx, op)
	if err != nil || outcome.Status != StatusMerged || outcome.MergedRevision != strings.Repeat("d", 40) {
		t.Fatalf("merge outcome = %#v err=%v", outcome, err)
	}
	stored, err := storeB.Get(ctx, candidate.ID)
	if err != nil || stored.Status != StatusMerged || stored.MergedRevision != strings.Repeat("d", 40) {
		t.Fatalf("stored candidate = %#v err=%v", stored, err)
	}
}

func TestPostgresReconcilerFencePreventsExpiredOwnerFromWritingMain(t *testing.T) {
	ctx := context.Background()
	db := openMergeQueuePostgresSchema(t, ctx)
	repo := seedRepo(t)
	binding := createMergeQueueGitBinding(t, repo, "task-fence", "workspace/fenced.txt", "fenced\n")
	insertMergeQueueWorkspaceBinding(t, ctx, db, binding)

	artifacts := evidence.NewArtifactRegistry(objectstore.NewMemoryStore(), "artifacts")
	verifyRef := registerArtifact(t, artifacts, "project-a", binding.TaskID, "verify fence")
	diffRef := registerArtifact(t, artifacts, "project-a", binding.TaskID, "diff fence")
	passRef := registerArtifact(t, artifacts, "project-a", binding.TaskID, "targeted verify passed")
	for _, ref := range []evidence.ArtifactID{verifyRef, diffRef, passRef} {
		insertMergeQueueArtifact(t, ctx, db, ref, "project-a", binding.TaskID)
	}

	storeA := NewPostgresStore(db)
	storeB := NewPostgresStore(db)
	storeA.SetClaimTTL(2 * time.Second)
	storeB.SetClaimTTL(2 * time.Second)
	verifier := &fakeVerifier{
		result:  TargetedVerifyResult{Passed: true, EvidenceRefs: []evidence.ArtifactID{passRef}},
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	reconciler := NewReconciler(storeA, staticWorkspaceReader{binding: binding}, verifier, GitBackend{TempParent: t.TempDir()}, artifacts, evidence.NewEventLog(64*1024))
	if _, err := reconciler.Enqueue(ctx, EnqueueRequest{
		ID:                "candidate-fence",
		ProjectID:         "project-a",
		TaskID:            binding.TaskID,
		WorkspaceRef:      binding.ID,
		VerifyResultRef:   verifyRef,
		DiffArtifactRef:   diffRef,
		TargetRepository:  repo,
		TargetBranch:      "main",
		BaseRevision:      binding.BaseRevision,
		MainRevision:      binding.BaseRevision,
		CandidateRevision: binding.CurrentRevision,
		EvidenceRefs:      []evidence.ArtifactID{verifyRef, diffRef},
	}); err != nil {
		t.Fatalf("enqueue fence candidate: %v", err)
	}

	type outcome struct {
		candidate Candidate
		claimed   bool
		err       error
	}
	done := make(chan outcome, 1)
	go func() {
		candidate, claimed, err := reconciler.ReconcileOne(ctx, repo)
		done <- outcome{candidate: candidate, claimed: claimed, err: err}
	}()
	select {
	case <-verifier.entered:
	case got := <-done:
		t.Fatalf("reconcile finished before verify: candidate:%#v claimed:%v err:%v", got.candidate, got.claimed, got.err)
	case <-time.After(15 * time.Second):
		t.Fatal("reconcile did not reach targeted verify")
	}
	time.Sleep(2100 * time.Millisecond)
	stolen, claimed, err := storeB.ClaimNext(ctx, repo)
	if err != nil || !claimed || stolen.Candidate.ID != "candidate-fence" || stolen.Token == "" {
		t.Fatalf("storeB takeover = %#v claimed=%v err=%v", stolen, claimed, err)
	}
	close(verifier.release)
	got := <-done
	if !got.claimed || !kernel.IsCode(got.err, kernel.CodeLeaseConflict) {
		t.Fatalf("expired owner reconcile = candidate:%#v claimed:%v err:%v, want lease_conflict", got.candidate, got.claimed, got.err)
	}
	if contents := fileAtBranchIfExists(t, repo, "main", "workspace/fenced.txt"); contents != "" {
		t.Fatalf("expired owner wrote main: %q", contents)
	}
	stored, err := storeB.Get(ctx, "candidate-fence")
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status == StatusMerged {
		t.Fatalf("expired owner merged candidate: %#v", stored)
	}
}

func TestPostgresReconcilerRecoversPendingMergeOperationWithExactRevision(t *testing.T) {
	ctx := context.Background()
	db := openMergeQueuePostgresSchema(t, ctx)
	repo := seedRepo(t)
	binding := createMergeQueueGitBinding(t, repo, "task-durable-op", "workspace/fenced.txt", "durable\n")
	insertMergeQueueWorkspaceBinding(t, ctx, db, binding)

	verifyRef := evidence.ArtifactID("artifact-verify-durable-op")
	diffRef := evidence.ArtifactID("artifact-diff-durable-op")
	passRef := evidence.ArtifactID("artifact-pass-durable-op")
	for _, ref := range []evidence.ArtifactID{verifyRef, diffRef, passRef} {
		insertMergeQueueArtifact(t, ctx, db, ref, "project-a", binding.TaskID)
	}

	store := NewPostgresStore(db)
	candidate, err := store.enqueue(ctx, EnqueueRequest{
		ID:                "candidate-durable-op",
		ProjectID:         "project-a",
		TaskID:            binding.TaskID,
		WorkspaceRef:      binding.ID,
		VerifyResultRef:   verifyRef,
		DiffArtifactRef:   diffRef,
		TargetRepository:  repo,
		TargetBranch:      "main",
		BaseRevision:      binding.BaseRevision,
		MainRevision:      binding.BaseRevision,
		CandidateRevision: binding.CurrentRevision,
		EvidenceRefs:      []evidence.ArtifactID{verifyRef, diffRef},
	}, mergeAudit(Candidate{ID: "candidate-durable-op", ProjectID: "project-a", TaskID: binding.TaskID, WorkspaceRef: binding.ID, MainRevision: binding.BaseRevision}, "queued"))
	if err != nil {
		t.Fatalf("enqueue durable op candidate: %v", err)
	}
	claim, claimed, err := store.ClaimNext(ctx, repo)
	if err != nil || !claimed {
		t.Fatalf("ClaimNext = %#v claimed=%v err=%v", claim, claimed, err)
	}
	advanced, err := store.advance(ctx, claim, StatusMergeCheck, StatusTargetedVerify, nil, "")
	if err != nil {
		t.Fatalf("advance targeted verify: %v", err)
	}
	claim.Candidate = advanced

	backend := GitBackend{TempParent: t.TempDir()}
	prepared, err := backend.Prepare(ctx, advanced, binding)
	if err != nil {
		t.Fatalf("prepare merge: %v", err)
	}
	defer backend.Cleanup(prepared)
	expectedMerged, err := backend.CreateMergeCommit(ctx, prepared, advanced)
	if err != nil {
		t.Fatalf("create merge commit: %v", err)
	}
	op, err := store.beginMergeOperation(ctx, claim, mergeOperationRequest{
		ExpectedMainRevision:   prepared.LatestMainRevision,
		ExpectedMergedRevision: expectedMerged,
		EvidenceRefs:           []evidence.ArtifactID{passRef},
		Audit:                  mergeSucceededAudit(advanced, expectedMerged, prepared.LatestMainRevision, []evidence.ArtifactID{passRef}),
	})
	if err != nil {
		t.Fatalf("begin merge operation: %v", err)
	}
	if err := backend.PushExact(ctx, prepared, op.ExpectedMergedRevision); err != nil {
		t.Fatalf("push exact merge commit: %v", err)
	}
	pushChange(t, repo, "after-durable.txt", "main advanced after push\n")
	if head := gitOut(t, repo, "rev-parse", "refs/heads/main"); head == op.ExpectedMergedRevision {
		t.Fatalf("test setup failed: main did not advance after exact merge commit")
	}

	artifacts := evidence.NewArtifactRegistry(objectstore.NewMemoryStore(), "artifacts")
	reconciler := NewReconciler(NewPostgresStore(db), staticWorkspaceReader{binding: binding}, &fakeVerifier{}, backend, artifacts, evidence.NewEventLog(64*1024))
	recovered, processed, err := reconciler.ReconcileOne(ctx, repo)
	if err != nil || !processed {
		t.Fatalf("recover pending operation = %#v processed=%v err=%v", recovered, processed, err)
	}
	if recovered.ID != candidate.ID || recovered.Status != StatusMerged || recovered.MergedRevision != op.ExpectedMergedRevision {
		t.Fatalf("recovered candidate = %#v, want exact merged revision %s", recovered, op.ExpectedMergedRevision)
	}
	stored, err := store.Get(ctx, candidate.ID)
	if err != nil || stored.MergedRevision != op.ExpectedMergedRevision {
		t.Fatalf("stored candidate = %#v err=%v", stored, err)
	}
}

func TestPostgresReconcilerFinalizesWhenPushReportsErrorAfterSideEffect(t *testing.T) {
	ctx := context.Background()
	db := openMergeQueuePostgresSchema(t, ctx)
	repo := seedRepo(t)
	binding := createMergeQueueGitBinding(t, repo, "task-push-side-effect", "workspace/pushed.txt", "pushed\n")
	insertMergeQueueWorkspaceBinding(t, ctx, db, binding)

	artifacts := evidence.NewArtifactRegistry(objectstore.NewMemoryStore(), "artifacts")
	verifyRef := registerArtifact(t, artifacts, "project-a", binding.TaskID, "verify side effect")
	diffRef := registerArtifact(t, artifacts, "project-a", binding.TaskID, "diff side effect")
	passRef := registerArtifact(t, artifacts, "project-a", binding.TaskID, "targeted verify passed")
	for _, ref := range []evidence.ArtifactID{verifyRef, diffRef, passRef} {
		insertMergeQueueArtifact(t, ctx, db, ref, "project-a", binding.TaskID)
	}

	store := NewPostgresStore(db)
	reconciler := NewReconciler(
		store,
		staticWorkspaceReader{binding: binding},
		&fakeVerifier{result: TargetedVerifyResult{Passed: true, EvidenceRefs: []evidence.ArtifactID{passRef}}},
		GitBackend{TempParent: t.TempDir(), pushErrorAfterSuccess: errors.New("simulated transport error after push side effect")},
		artifacts,
		evidence.NewEventLog(64*1024),
	)
	if _, err := reconciler.Enqueue(ctx, EnqueueRequest{
		ID:                "candidate-push-side-effect",
		ProjectID:         "project-a",
		TaskID:            binding.TaskID,
		WorkspaceRef:      binding.ID,
		VerifyResultRef:   verifyRef,
		DiffArtifactRef:   diffRef,
		TargetRepository:  repo,
		TargetBranch:      "main",
		BaseRevision:      binding.BaseRevision,
		MainRevision:      binding.BaseRevision,
		CandidateRevision: binding.CurrentRevision,
		EvidenceRefs:      []evidence.ArtifactID{verifyRef, diffRef},
	}); err != nil {
		t.Fatalf("enqueue side-effect candidate: %v", err)
	}

	merged, processed, err := reconciler.ReconcileOne(ctx, repo)
	if err != nil || !processed {
		t.Fatalf("reconcile side-effect push error = %#v processed=%v err=%v", merged, processed, err)
	}
	if merged.Status != StatusMerged || merged.MergedRevision == "" || merged.FailureReason != "" {
		t.Fatalf("merged candidate after side-effect push error = %#v", merged)
	}
	if got := fileAtBranch(t, repo, "main", "workspace/pushed.txt"); got != "pushed\n" {
		t.Fatalf("pushed file = %q", got)
	}
	op, active, err := store.pendingMergeOperation(ctx, repo)
	if err != nil {
		t.Fatalf("pending operation lookup: %v", err)
	}
	if active || op.Status == "aborted" || op.Status == "recovery_required" {
		t.Fatalf("operation left active after contained side-effect push error: %#v active=%v", op, active)
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

func insertMergeQueueWorkspaceBinding(t *testing.T, ctx context.Context, db *sql.DB, binding workspace.Binding) {
	t.Helper()
	_, err := db.ExecContext(ctx, `
INSERT INTO workspace_bindings(
	id, task_id, generation, kind, root, branch_name, base_revision, current_revision,
	allowed_dirs, observed_writes, phase_leases, status
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::text[], $10::jsonb, $11::jsonb, $12)`,
		binding.ID, binding.TaskID, binding.Generation, binding.Kind, binding.Root, binding.BranchName,
		binding.BaseRevision, binding.CurrentRevision, textArrayLiteral(binding.AllowedDirs),
		`{"files":["workspace/fenced.txt"]}`, `{"verify":"inv-verify"}`, binding.Status)
	if err != nil {
		t.Fatalf("insert workspace binding %s: %v", binding.ID, err)
	}
}

func createMergeQueueGitBinding(t *testing.T, repo string, taskID kernel.TaskID, file, body string) workspace.Binding {
	t.Helper()
	mainRevision := gitOut(t, repo, "rev-parse", "refs/heads/main")
	root := filepath.Join(t.TempDir(), "candidate")
	git(t, t.TempDir(), "clone", repo, root)
	branch := "threadmill/" + string(taskID) + "/001"
	git(t, root, "checkout", "-b", branch, mainRevision)
	write(t, filepath.Join(root, filepath.FromSlash(file)), body)
	git(t, root, "add", file)
	git(t, root, "-c", "user.email=test@example.com", "-c", "user.name=Test", "commit", "-m", "candidate")
	return workspace.Binding{
		ID:              kernel.BindingRef("ws_" + string(taskID)),
		TaskID:          taskID,
		Generation:      1,
		Kind:            workspace.KindGitWorktree,
		Root:            root,
		BranchName:      branch,
		BaseRevision:    mainRevision,
		CurrentRevision: gitOut(t, root, "rev-parse", "HEAD"),
		AllowedDirs:     []string{"workspace"},
		ObservedWrites:  workspace.WriteSet{Files: []string{file}},
		PhaseLeases:     map[workspace.Phase]kernel.InvocationID{workspace.PhaseVerify: "inv-verify"},
		Status:          workspace.StatusSealed,
	}
}

type staticWorkspaceReader struct {
	binding workspace.Binding
}

func (r staticWorkspaceReader) Get(_ context.Context, ref kernel.BindingRef) (workspace.Binding, error) {
	if ref != r.binding.ID {
		return workspace.Binding{}, kernel.Error{Code: kernel.CodeNotFound, Message: "workspace binding not found"}
	}
	return r.binding, nil
}

func fileAtBranchIfExists(t *testing.T, repo, branch, file string) string {
	t.Helper()
	out, err := gitOutput(context.Background(), repo, "show", branch+":"+file)
	if err != nil {
		return ""
	}
	return out
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
