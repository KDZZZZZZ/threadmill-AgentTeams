package workspace

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

var errInjected = errors.New("injected failure")

func TestGitWorktreeBindingPhaseReuseLeasesGuardWritesAndSeal(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo := seedBareRepo(t)
	parent := t.TempDir()
	service := NewService()

	first, err := service.CreateGitWorktree(ctx, CreateRequest{
		TaskID:         "task-1",
		Generation:     1,
		RepoPath:       repo,
		WorktreeParent: parent,
		AllowedDirs:    []string{"workspace"},
		DeclaredWrites: WriteSet{Files: []string{"workspace/app.go"}},
	})
	if err != nil {
		t.Fatalf("create first binding: %v", err)
	}
	again, err := service.CreateGitWorktree(ctx, CreateRequest{TaskID: "task-1", Generation: 1, RepoPath: repo, WorktreeParent: parent})
	if err != nil {
		t.Fatalf("reuse binding: %v", err)
	}
	if first.ID != again.ID || first.Root != again.Root {
		t.Fatalf("same round did not reuse binding: first=%+v again=%+v", first, again)
	}
	second, err := service.CreateGitWorktree(ctx, CreateRequest{TaskID: "task-1", Generation: 2, RepoPath: repo, WorktreeParent: parent})
	if err != nil {
		t.Fatalf("create second binding: %v", err)
	}
	if first.ID == second.ID || first.Root == second.Root {
		t.Fatalf("new generation reused old binding: first=%+v second=%+v", first, second)
	}

	leased, err := service.BindPhase(ctx, first.ID, PhasePlan, "inv-plan")
	if err != nil {
		t.Fatalf("lease plan: %v", err)
	}
	if leased.ActivePhase != PhasePlan || leased.ActiveInvocation != "inv-plan" {
		t.Fatalf("active lease projection missing: %+v", leased)
	}
	if _, err := service.BindPhase(ctx, first.ID, PhaseExecute, "inv-execute"); !kernel.IsCode(err, kernel.CodeLeaseConflict) {
		t.Fatalf("concurrent phase lease error = %v", err)
	}
	if _, err := service.CompletePhase(ctx, first.ID, PhasePlan, "inv-plan"); err != nil {
		t.Fatalf("complete plan: %v", err)
	}
	leased, err = service.BindPhase(ctx, first.ID, PhaseExecute, "inv-execute")
	if err != nil {
		t.Fatalf("lease execute after plan complete: %v", err)
	}
	if leased.ActivePhase != PhaseExecute || leased.PhaseLeases[PhasePlan] != "inv-plan" {
		t.Fatalf("phase chain projection = %+v", leased)
	}
	if _, err := service.ReleasePhase(ctx, first.ID, PhaseExecute, "inv-execute"); err != nil {
		t.Fatalf("release execute: %v", err)
	}
	if _, err := service.BindPhase(ctx, first.ID, PhaseExecute, "inv-execute-2"); err != nil {
		t.Fatalf("re-lease execute after release: %v", err)
	}
	if _, err := service.CompletePhase(ctx, first.ID, PhaseExecute, "inv-execute-2"); err != nil {
		t.Fatalf("complete execute: %v", err)
	}
	if _, err := service.BindPhase(ctx, first.ID, PhaseVerify, "inv-verify"); err != nil {
		t.Fatalf("lease verify after execute complete: %v", err)
	}

	assertAllowed(t, first, PhasePlan, "plan/declared-writes.json")
	assertDenied(t, first, PhasePlan, "workspace/app.go")
	assertAllowed(t, first, PhaseExecute, "workspace/app.go")
	assertDenied(t, first, PhaseExecute, "plan/notes.md")
	assertAllowed(t, first, PhaseVerify, "evidence/test.txt")
	assertDenied(t, first, PhaseVerify, "workspace/app.go")
	assertDenied(t, first, PhaseExecute, "../outside.txt")
	assertDenied(t, first, PhaseExecute, filepath.Join(first.Root, "workspace", "app.go"))

	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(first.Root, "workspace", "escape")); err == nil {
		assertDenied(t, first, PhaseExecute, "workspace/escape/out.txt")
		if err := os.Remove(filepath.Join(first.Root, "workspace", "escape")); err != nil {
			t.Fatalf("remove symlink fixture: %v", err)
		}
	}

	writeFile(t, filepath.Join(first.Root, "workspace", "app.go"), "package main\n")
	git(t, first.Root, "add", "workspace/app.go")
	git(t, first.Root, "-c", "user.email=test@example.com", "-c", "user.name=Test", "commit", "-m", "change")
	writeFile(t, filepath.Join(first.Root, "workspace", "staged.go"), "package main\n")
	git(t, first.Root, "add", "workspace/staged.go")
	writeFile(t, filepath.Join(first.Root, "workspace", "unstaged.go"), "package main\n")
	git(t, first.Root, "add", "workspace/unstaged.go")
	writeFile(t, filepath.Join(first.Root, "workspace", "unstaged.go"), "package main\n// changed\n")
	writeFile(t, filepath.Join(first.Root, "workspace", "untracked.go"), "package main\n")
	observed, err := service.RefreshObservedWrites(ctx, first.ID)
	if err != nil {
		t.Fatalf("refresh observed writes: %v", err)
	}
	wantObserved := []string{"workspace/app.go", "workspace/staged.go", "workspace/unstaged.go", "workspace/untracked.go"}
	if !reflect.DeepEqual(observed.ObservedWrites.Files, wantObserved) {
		t.Fatalf("observed writes = %+v", observed.ObservedWrites)
	}
	if observed.ActivePhase != PhaseVerify || observed.ActiveInvocation != "inv-verify" {
		t.Fatalf("refresh overwrote active lease: %+v", observed)
	}

	sealed, err := service.Seal(ctx, first.ID)
	if err != nil {
		t.Fatalf("seal binding: %v", err)
	}
	if _, err := ResolveWritePath(sealed, PhaseExecute, "workspace/after.txt"); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("sealed write error = %v", err)
	}
}

func TestCreateGitWorktreeFailsClosedAndRollsBack(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo := seedBareRepo(t)
	parent := t.TempDir()
	service := NewService()

	if _, err := service.CreateGitWorktree(ctx, CreateRequest{
		TaskID:         "task-invalid",
		Generation:     1,
		RepoPath:       repo,
		WorktreeParent: parent,
		AllowedDirs:    []string{"../escape"},
	}); !kernel.IsCode(err, kernel.CodeInvalidRequest) && !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("invalid allowed dirs error = %v", err)
	}

	_, err := service.CreateGitWorktree(ctx, CreateRequest{
		TaskID:         "task-rollback",
		Generation:     1,
		RepoPath:       repo,
		WorktreeParent: parent,
		AfterWorktreeAdd: func() error {
			return errInjected
		},
	})
	if err == nil {
		t.Fatal("expected injected create failure")
	}
	if _, statErr := os.Stat(filepath.Join(parent, "ws_task-rollback_001")); !os.IsNotExist(statErr) {
		t.Fatalf("worktree was not rolled back, stat err = %v", statErr)
	}
	out, branchErr := exec.Command("git", "--git-dir", repo, "branch", "--list", "threadmill/task-rollback/001").CombinedOutput()
	if branchErr != nil {
		t.Fatalf("list rollback branch: %v\n%s", branchErr, out)
	}
	if len(out) != 0 {
		t.Fatalf("branch was not rolled back: %s", out)
	}
}

func assertAllowed(t *testing.T, binding Binding, phase Phase, rel string) {
	t.Helper()
	if _, err := ResolveWritePath(binding, phase, rel); err != nil {
		t.Fatalf("ResolveWritePath(%s, %s) error = %v", phase, rel, err)
	}
}

func assertDenied(t *testing.T, binding Binding, phase Phase, rel string) {
	t.Helper()
	if _, err := ResolveWritePath(binding, phase, rel); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("ResolveWritePath(%s, %s) error = %v, want forbidden", phase, rel, err)
	}
}

func seedBareRepo(t *testing.T) string {
	t.Helper()
	work := filepath.Join(t.TempDir(), "seed")
	if err := os.MkdirAll(work, 0o755); err != nil {
		t.Fatalf("mkdir seed: %v", err)
	}
	git(t, work, "init")
	writeFile(t, filepath.Join(work, "README.md"), "seed\n")
	git(t, work, "add", "README.md")
	git(t, work, "-c", "user.email=test@example.com", "-c", "user.name=Test", "commit", "-m", "seed")
	bare := filepath.Join(t.TempDir(), "repo.git")
	git(t, work, "clone", "--bare", work, bare)
	return bare
}

func writeFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func git(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v in %s: %v\n%s", args, dir, err, out)
	}
}
