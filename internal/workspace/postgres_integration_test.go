package workspace

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

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/postgres"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/migrations"
)

func TestPostgresWorkspaceRealLifecycleConcurrencyRestartAndAgentTools(t *testing.T) {
	databaseURL := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("THREADMILL_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	admin, err := postgres.Open(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	defer admin.Close(context.Background())

	schema := fmt.Sprintf("tm_workspace_%d", time.Now().UnixNano())
	if _, err := admin.SQL().ExecContext(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.SQL().ExecContext(context.Background(), `DROP SCHEMA IF EXISTS `+schema+` CASCADE`)
	})
	scopedURL, err := workspaceDatabaseURLWithSearchPath(databaseURL, schema)
	if err != nil {
		t.Fatal(err)
	}
	db, err := postgres.Open(ctx, scopedURL)
	if err != nil {
		t.Fatalf("open scoped postgres: %v", err)
	}
	defer db.Close(context.Background())
	loaded, err := postgres.LoadMigrations(migrations.FS, migrations.Dir)
	if err != nil {
		t.Fatalf("load migrations: %v", err)
	}
	if err := postgres.NewMigrator(db.SQL()).Apply(ctx, loaded); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	repository := seedBareRepo(t)
	worktreeParent := t.TempDir()
	req := CreateRequest{
		TaskID:         "task-real-workspace",
		Generation:     1,
		RepoPath:       repository,
		WorktreeParent: worktreeParent,
		AllowedDirs:    []string{"workspace"},
		DeclaredWrites: WriteSet{Files: []string{"workspace/app.go"}},
	}
	binding := assertConcurrentPostgresCreate(t, ctx, db.SQL(), req)
	var rowCount int
	if err := db.SQL().QueryRowContext(ctx, `SELECT count(*) FROM workspace_bindings WHERE task_id = $1 AND generation = $2`, req.TaskID, req.Generation).Scan(&rowCount); err != nil {
		t.Fatalf("count workspace rows: %v", err)
	}
	if rowCount != 1 {
		t.Fatalf("workspace row count = %d, want 1", rowCount)
	}
	if binding.Revision != 1 || binding.CurrentRevision == "" || binding.CreationFingerprint == "" {
		t.Fatalf("created binding lacks production identity: %+v", binding)
	}

	service := NewPostgresService(db.SQL())
	conflict := req
	conflict.DeclaredWrites = WriteSet{Files: []string{"workspace/other.go"}}
	if _, err := service.CreateGitWorktree(ctx, conflict); !kernel.IsCode(err, kernel.CodeIdempotencyConflict) {
		t.Fatalf("conflicting create error = %v, want idempotency_conflict", err)
	}

	plan, err := service.BindPhase(ctx, binding.ID, PhasePlan, "inv-plan-real", binding.Revision)
	if err != nil {
		t.Fatalf("bind plan: %v", err)
	}
	if _, err := service.CompletePhase(ctx, binding.ID, PhasePlan, "inv-plan-real", binding.Revision); !kernel.IsCode(err, kernel.CodeRevisionConflict) {
		t.Fatalf("stale complete error = %v, want revision_conflict", err)
	}
	idempotentPlan, err := service.BindPhase(ctx, binding.ID, PhasePlan, "inv-plan-real", binding.Revision)
	if err != nil || idempotentPlan.Revision != plan.Revision {
		t.Fatalf("idempotent plan bind = %+v err %v", idempotentPlan, err)
	}
	completedPlan, err := service.CompletePhase(ctx, binding.ID, PhasePlan, "inv-plan-real", plan.Revision)
	if err != nil {
		t.Fatalf("complete plan: %v", err)
	}

	// A new Service instance proves that lease history and revision authority
	// live in PostgreSQL rather than process memory.
	restarted := NewPostgresService(db.SQL())
	persisted, err := restarted.Get(ctx, binding.ID)
	if err != nil {
		t.Fatalf("get after restart: %v", err)
	}
	if persisted.PhaseLeases[PhasePlan] != "inv-plan-real" || persisted.Revision != completedPlan.Revision {
		t.Fatalf("restart lost phase state: %+v", persisted)
	}
	git(t, repository, "worktree", "remove", "--force", binding.Root)
	recovered, err := restarted.Materialize(ctx, binding.ID)
	if err != nil {
		t.Fatalf("materialize after worktree loss: %v", err)
	}
	if _, err := os.Stat(filepath.Join(recovered.Root, "README.md")); err != nil {
		t.Fatalf("recovered worktree missing README: %v", err)
	}

	_, err = restarted.BindPhase(ctx, binding.ID, PhaseExecute, "inv-execute-real", recovered.Revision)
	if err != nil {
		t.Fatalf("bind execute: %v", err)
	}
	tools := NewAgentTools(restarted, "git", "go")
	read, err := tools.Read(ctx, "inv-execute-real", PathRequest{Path: "README.md"})
	if err != nil || strings.TrimSpace(read.Content) != "seed" {
		t.Fatalf("read through invocation scope = %+v err %v", read, err)
	}
	listed, err := tools.List(ctx, "inv-execute-real", PathRequest{})
	if err != nil {
		t.Fatalf("list workspace: %v", err)
	}
	for _, entry := range listed.Entries {
		if strings.EqualFold(entry.Path, ".git") {
			t.Fatalf("protected .git entry leaked: %+v", listed.Entries)
		}
	}
	if _, err := tools.WritePlan(ctx, "inv-execute-real", WriteRequest{Path: "plan/escape.md", Content: "no"}); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("execute invocation writePlan error = %v, want forbidden", err)
	}
	if _, err := tools.Write(ctx, "inv-execute-real", WriteRequest{Path: "../outside.txt", Content: "no"}); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("parent traversal write error = %v, want forbidden", err)
	}
	write, err := tools.Write(ctx, "inv-execute-real", WriteRequest{Path: "workspace/app.go", Content: "package app\n"})
	if err != nil || write.WorkspaceRevision == "" {
		t.Fatalf("write through invocation scope = %+v err %v", write, err)
	}
	if _, err := tools.Run(ctx, "inv-execute-real", RunRequest{Command: []string{"git", "worktree", "list"}}); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("unsafe git subcommand error = %v, want forbidden", err)
	}
	if _, err := tools.Run(ctx, "inv-execute-real", RunRequest{Command: []string{"git", "status", "--short"}, WorkDir: "../"}); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("escaping workdir error = %v, want forbidden", err)
	}
	run, err := tools.Run(ctx, "inv-execute-real", RunRequest{Command: []string{"git", "status", "--short"}})
	if err != nil || run.ExitCode != 0 || !strings.Contains(run.Stdout, "workspace/") {
		t.Fatalf("real git run = %+v err %v", run, err)
	}
	diff, err := tools.Diff(ctx, "inv-execute-real", PathRequest{Path: "workspace"})
	if err != nil || !strings.Contains(diff.Patch, "workspace/app.go") || len(diff.ObservedWrites.Files) != 1 {
		t.Fatalf("real git diff = %+v err %v", diff, err)
	}
	secret := "workspace-secret-must-not-leak"
	t.Setenv("THREADMILL_WORKSPACE_TEST_SECRET", secret)
	hookPath := filepath.Join(repository, "hooks", "pre-commit")
	writeFile(t, hookPath, "#!/bin/sh\necho $THREADMILL_WORKSPACE_TEST_SECRET >&2\necho hook-ran > workspace/hook-ran\n")
	if err := os.Chmod(hookPath, 0o755); err != nil {
		t.Fatalf("chmod pre-commit hook: %v", err)
	}
	git(t, binding.Root, "config", "user.name", "Threadmill Test")
	git(t, binding.Root, "config", "user.email", "threadmill-test@invalid")
	if added, err := tools.Run(ctx, "inv-execute-real", RunRequest{Command: []string{"git", "add", "workspace/app.go"}}); err != nil || added.ExitCode != 0 {
		t.Fatalf("git add with scrubbed environment = %+v err %v", added, err)
	}
	committed, err := tools.Run(ctx, "inv-execute-real", RunRequest{Command: []string{"git", "commit", "-m", "candidate"}})
	if err != nil || committed.ExitCode != 0 {
		t.Fatalf("git commit with hooks disabled = %+v err %v", committed, err)
	}
	if strings.Contains(committed.Stdout, secret) || strings.Contains(committed.Stderr, secret) {
		t.Fatalf("workspace command leaked parent environment secret: %+v", committed)
	}
	if _, statErr := os.Stat(filepath.Join(binding.Root, "workspace", "hook-ran")); !os.IsNotExist(statErr) {
		t.Fatalf("workspace command executed repository hook, stat err = %v", statErr)
	}

	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(binding.Root, "workspace", "escape")); err == nil {
		if _, err := tools.Write(ctx, "inv-execute-real", WriteRequest{Path: "workspace/escape/out.txt", Content: "no"}); !kernel.IsCode(err, kernel.CodeForbidden) {
			t.Fatalf("symlink escape write error = %v, want forbidden", err)
		}
		if _, err := tools.Run(ctx, "inv-execute-real", RunRequest{Command: []string{"git", "status", "--short"}}); !kernel.IsCode(err, kernel.CodeForbidden) {
			t.Fatalf("run with escaping symlink error = %v, want forbidden", err)
		}
		if err := os.Remove(filepath.Join(binding.Root, "workspace", "escape")); err != nil {
			t.Fatalf("remove symlink fixture: %v", err)
		}
	}

	latest, err := restarted.Get(ctx, binding.ID)
	if err != nil {
		t.Fatal(err)
	}
	completedExecute, err := restarted.CompletePhase(ctx, binding.ID, PhaseExecute, "inv-execute-real", latest.Revision)
	if err != nil {
		t.Fatalf("complete execute: %v", err)
	}
	if _, err := tools.Read(ctx, "inv-execute-real", PathRequest{Path: "README.md"}); !kernel.IsCode(err, kernel.CodeStaleBinding) {
		t.Fatalf("revoked invocation read error = %v, want stale_binding", err)
	}

	_, err = restarted.BindPhase(ctx, binding.ID, PhaseVerify, "inv-verify-real", completedExecute.Revision)
	if err != nil {
		t.Fatalf("bind verify: %v", err)
	}
	verifyRun, err := tools.Run(ctx, "inv-verify-real", RunRequest{Command: []string{"git", "status", "--short"}})
	if err != nil || verifyRun.ExitCode != 0 {
		t.Fatalf("verify read-only git run = %+v err %v", verifyRun, err)
	}
	isolatedGitWrite, err := tools.Run(ctx, "inv-verify-real", RunRequest{Command: []string{"git", "diff", "--output=workspace/pwn"}})
	if err != nil || isolatedGitWrite.ExitCode != 0 {
		t.Fatalf("verify isolated git output = %+v err %v", isolatedGitWrite, err)
	}
	if _, statErr := os.Stat(filepath.Join(binding.Root, "workspace", "pwn")); !os.IsNotExist(statErr) {
		t.Fatalf("read-only git command wrote authoritative workspace, stat err = %v", statErr)
	}
	copyRun, err := tools.Run(ctx, "inv-verify-real", RunRequest{Command: []string{"go", "mod", "init", "example.com/read-only-check"}})
	if err != nil || copyRun.ExitCode != 0 {
		t.Fatalf("verify copied workspace run = %+v err %v", copyRun, err)
	}
	if _, statErr := os.Stat(filepath.Join(binding.Root, "go.mod")); !os.IsNotExist(statErr) {
		t.Fatalf("read-only phase command mutated authoritative workspace, stat err = %v", statErr)
	}
	latestVerify, err := restarted.Get(ctx, binding.ID)
	if err != nil {
		t.Fatal(err)
	}
	completedVerify, err := restarted.CompletePhase(ctx, binding.ID, PhaseVerify, "inv-verify-real", latestVerify.Revision)
	if err != nil {
		t.Fatalf("complete verify: %v", err)
	}
	sealed, err := restarted.Seal(ctx, binding.ID, completedVerify.Revision)
	if err != nil || sealed.Status != StatusSealed {
		t.Fatalf("seal persisted binding = %+v err %v", sealed, err)
	}

	assertConcurrentPostgresLeaseCAS(t, ctx, db.SQL(), repository, worktreeParent)
}

func assertConcurrentPostgresCreate(t *testing.T, ctx context.Context, db *sql.DB, req CreateRequest) Binding {
	t.Helper()
	const workers = 12
	start := make(chan struct{})
	results := make(chan Binding, workers)
	errs := make(chan error, workers)
	var ready sync.WaitGroup
	ready.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			ready.Done()
			<-start
			binding, err := NewPostgresService(db).CreateGitWorktree(ctx, req)
			if err != nil {
				errs <- err
				return
			}
			results <- binding
		}()
	}
	ready.Wait()
	close(start)
	var first Binding
	for i := 0; i < workers; i++ {
		select {
		case err := <-errs:
			t.Fatalf("concurrent create: %v", err)
		case result := <-results:
			if first.ID == "" {
				first = result
			} else if result.ID != first.ID || result.Root != first.Root || result.Revision != first.Revision {
				t.Fatalf("concurrent create diverged: first=%+v result=%+v", first, result)
			}
		}
	}
	return first
}

func assertConcurrentPostgresLeaseCAS(t *testing.T, ctx context.Context, db *sql.DB, repository, parent string) {
	t.Helper()
	service := NewPostgresService(db)
	binding, err := service.CreateGitWorktree(ctx, CreateRequest{TaskID: "task-cas", Generation: 1, RepoPath: repository, WorktreeParent: parent})
	if err != nil {
		t.Fatalf("create CAS binding: %v", err)
	}
	const workers = 10
	start := make(chan struct{})
	type result struct {
		invocation kernel.InvocationID
		binding    Binding
		err        error
	}
	results := make(chan result, workers)
	for i := 0; i < workers; i++ {
		go func(index int) {
			<-start
			invocation := kernel.InvocationID(fmt.Sprintf("inv-cas-%d", index))
			leased, err := NewPostgresService(db).BindPhase(ctx, binding.ID, PhasePlan, invocation, binding.Revision)
			results <- result{invocation: invocation, binding: leased, err: err}
		}(i)
	}
	close(start)
	successes := 0
	var winner result
	for i := 0; i < workers; i++ {
		candidate := <-results
		if candidate.err == nil {
			successes++
			winner = candidate
			continue
		}
		if !kernel.IsCode(candidate.err, kernel.CodeLeaseConflict) && !kernel.IsCode(candidate.err, kernel.CodeRevisionConflict) {
			t.Fatalf("concurrent lease error = %v", candidate.err)
		}
	}
	if successes != 1 {
		t.Fatalf("concurrent lease successes = %d, want 1", successes)
	}
	other, err := service.CreateGitWorktree(ctx, CreateRequest{TaskID: "task-cas-other", Generation: 1, RepoPath: repository, WorktreeParent: parent})
	if err != nil {
		t.Fatalf("create other CAS binding: %v", err)
	}
	if _, err := service.BindPhase(ctx, other.ID, PhasePlan, winner.invocation, other.Revision); !kernel.IsCode(err, kernel.CodeLeaseConflict) {
		t.Fatalf("duplicate active invocation error = %v, want lease_conflict", err)
	}
	if _, err := service.ReleasePhase(ctx, binding.ID, PhasePlan, winner.invocation, winner.binding.Revision); err != nil {
		t.Fatalf("release CAS winner: %v", err)
	}
}

func workspaceDatabaseURLWithSearchPath(raw, schema string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}
