package mergequeue

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/evidence"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/objectstore"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/workspace"
)

func TestMergeQueueHappyPathWritesMainAndPreservesCandidateWorkspace(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	binding := h.workspace(t, "task-a", 1, "workspace/a.txt", "candidate-a\n")
	candidate := h.enqueue(t, "candidate-a", binding)
	beforeHead := gitOut(t, binding.Root, "rev-parse", "HEAD")

	merged, claimed, err := h.reconciler.ReconcileOne(context.Background(), h.repo)
	if err != nil || !claimed {
		t.Fatalf("reconcile = claimed:%v err:%v", claimed, err)
	}
	if merged.Status != StatusMerged || merged.MergedRevision == "" {
		t.Fatalf("merged candidate = %#v", merged)
	}
	if got := fileAtBranch(t, h.repo, "main", "workspace/a.txt"); got != "candidate-a\n" {
		t.Fatalf("merged file = %q", got)
	}
	if afterHead := gitOut(t, binding.Root, "rev-parse", "HEAD"); afterHead != beforeHead {
		t.Fatalf("candidate workspace changed from %s to %s", beforeHead, afterHead)
	}
	stored, err := h.store.Get(context.Background(), candidate.ID)
	if err != nil || stored.Status != StatusMerged {
		t.Fatalf("stored candidate = %#v err=%v", stored, err)
	}
	events, _, err := h.events.Replay(context.Background(), 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].Type != "MergeCandidateQueued" || events[1].Type != "MergeCandidateMerged" {
		t.Fatalf("merge events = %#v", events)
	}
}

func TestMergeQueueFailsWithEvidenceForPermissionConflictVerifyAndMainDrift(t *testing.T) {
	t.Run("permission", func(t *testing.T) {
		h := newHarness(t)
		binding := h.workspace(t, "task-a", 1, "outside.txt", "not allowed\n")
		h.enqueue(t, "candidate-permission", binding)
		failed, claimed, err := h.reconciler.ReconcileOne(context.Background(), h.repo)
		assertFailure(t, failed, claimed, err, FailurePermission)
	})

	t.Run("conflict", func(t *testing.T) {
		h := newHarness(t)
		binding := h.workspaceAllowed(t, "task-a", 1, "README.md", "candidate\n", []string{"README.md"})
		h.enqueue(t, "candidate-conflict", binding)
		pushChange(t, h.repo, "README.md", "main drift\n")
		failed, claimed, err := h.reconciler.ReconcileOne(context.Background(), h.repo)
		assertFailure(t, failed, claimed, err, FailureConflict)
	})

	t.Run("verify_failed", func(t *testing.T) {
		h := newHarness(t)
		binding := h.workspace(t, "task-a", 1, "workspace/a.txt", "candidate\n")
		h.enqueue(t, "candidate-verify", binding)
		failureEvidence := registerArtifact(t, h.artifacts, "project-a", "task-a", "targeted verification failure")
		h.verifier.result = TargetedVerifyResult{Passed: false, EvidenceRefs: []evidence.ArtifactID{failureEvidence}}
		failed, claimed, err := h.reconciler.ReconcileOne(context.Background(), h.repo)
		assertFailure(t, failed, claimed, err, FailureVerifyFailed)
		if !containsArtifact(failed.EvidenceRefs, failureEvidence) {
			t.Fatalf("failed targeted verify dropped verifier evidence: %#v", failed.EvidenceRefs)
		}
	})

	t.Run("targeted_verify_main_drift", func(t *testing.T) {
		h := newHarness(t)
		binding := h.workspace(t, "task-a", 1, "workspace/a.txt", "candidate\n")
		h.enqueue(t, "candidate-targeted-drift", binding)
		h.verifier.err = MainDrift("main-old", "main-new")
		failed, claimed, err := h.reconciler.ReconcileOne(context.Background(), h.repo)
		assertFailure(t, failed, claimed, err, FailureMainDrift)
	})

	t.Run("main_drift", func(t *testing.T) {
		h := newHarness(t)
		binding := h.workspace(t, "task-a", 1, "workspace/a.txt", "candidate\n")
		h.enqueue(t, "candidate-drift", binding)
		h.verifier.entered = make(chan struct{})
		h.verifier.release = make(chan struct{})
		type outcome struct {
			candidate Candidate
			claimed   bool
			err       error
		}
		done := make(chan outcome, 1)
		go func() {
			candidate, claimed, err := h.reconciler.ReconcileOne(context.Background(), h.repo)
			done <- outcome{candidate: candidate, claimed: claimed, err: err}
		}()
		<-h.verifier.entered
		pushChange(t, h.repo, "drift.txt", "external\n")
		close(h.verifier.release)
		got := <-done
		assertFailure(t, got.candidate, got.claimed, got.err, FailureMainDrift)
	})
}

func TestMergeQueueSerializesSameRepository(t *testing.T) {
	h := newHarness(t)
	first := h.workspace(t, "task-a", 1, "workspace/a.txt", "a\n")
	second := h.workspace(t, "task-b", 1, "workspace/b.txt", "b\n")
	h.enqueue(t, "candidate-a", first)
	h.enqueue(t, "candidate-b", second)
	h.verifier.entered = make(chan struct{})
	h.verifier.release = make(chan struct{})

	done := make(chan error, 1)
	go func() {
		_, claimed, err := h.reconciler.ReconcileOne(context.Background(), h.repo)
		if err == nil && !claimed {
			err = errors.New("first reconcile did not claim")
		}
		done <- err
	}()
	<-h.verifier.entered
	if _, claimed, err := h.reconciler.ReconcileOne(context.Background(), h.repo); err != nil || claimed {
		t.Fatalf("concurrent reconcile = claimed:%v err:%v, want no claim", claimed, err)
	}
	close(h.verifier.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	h.verifier.entered = nil
	h.verifier.release = nil
	if merged, claimed, err := h.reconciler.ReconcileOne(context.Background(), h.repo); err != nil || !claimed || merged.Status != StatusMerged {
		t.Fatalf("second serial reconcile = %#v claimed:%v err:%v", merged, claimed, err)
	}
	if h.verifier.maxActive.Load() != 1 {
		t.Fatalf("max concurrent targeted verifies = %d, want 1", h.verifier.maxActive.Load())
	}
}

func assertFailure(t *testing.T, candidate Candidate, claimed bool, err error, reason FailureReason) {
	t.Helper()
	if err != nil || !claimed {
		t.Fatalf("failure reconcile = claimed:%v err:%v", claimed, err)
	}
	if candidate.Status != StatusFailed || candidate.FailureReason != reason || len(candidate.EvidenceRefs) == 0 {
		t.Fatalf("failed candidate = %#v, want reason %s with evidence", candidate, reason)
	}
}

func containsArtifact(refs []evidence.ArtifactID, target evidence.ArtifactID) bool {
	for _, ref := range refs {
		if ref == target {
			return true
		}
	}
	return false
}

type harness struct {
	repo       string
	workspaces *workspace.Service
	store      *MemoryStore
	artifacts  *evidence.ArtifactRegistry
	events     *evidence.EventLog
	verifier   *fakeVerifier
	reconciler *Reconciler
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	repo := seedRepo(t)
	artifacts := evidence.NewArtifactRegistry(objectstore.NewMemoryStore(), "artifacts")
	verifyEvidence := registerArtifact(t, artifacts, "project-a", "task-a", "targeted verify passed")
	verifier := &fakeVerifier{result: TargetedVerifyResult{Passed: true, EvidenceRefs: []evidence.ArtifactID{verifyEvidence}}}
	store := NewMemoryStore()
	workspaces := workspace.NewService()
	events := evidence.NewEventLog(64 * 1024)
	return &harness{
		repo:       repo,
		workspaces: workspaces,
		store:      store,
		artifacts:  artifacts,
		events:     events,
		verifier:   verifier,
		reconciler: NewReconciler(store, workspaces, verifier, GitBackend{TempParent: t.TempDir()}, artifacts, events),
	}
}

func (h *harness) workspace(t *testing.T, taskID kernel.TaskID, generation int, file, body string) workspace.Binding {
	return h.workspaceAllowed(t, taskID, generation, file, body, []string{"workspace"})
}

func (h *harness) workspaceAllowed(t *testing.T, taskID kernel.TaskID, generation int, file, body string, allowedDirs []string) workspace.Binding {
	t.Helper()
	binding, err := h.workspaces.CreateGitWorktree(context.Background(), workspace.CreateRequest{
		TaskID:         taskID,
		Generation:     generation,
		RepoPath:       h.repo,
		WorktreeParent: t.TempDir(),
		AllowedDirs:    allowedDirs,
	})
	if err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(binding.Root, filepath.FromSlash(file)), body)
	git(t, binding.Root, "add", file)
	git(t, binding.Root, "-c", "user.email=test@example.com", "-c", "user.name=Test", "commit", "-m", "candidate")
	binding, err = h.workspaces.RefreshObservedWrites(context.Background(), binding.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []workspace.Phase{workspace.PhasePlan, workspace.PhaseExecute, workspace.PhaseVerify} {
		invocationID := kernel.InvocationID("inv-" + string(taskID) + "-" + string(phase))
		if _, err := h.workspaces.BindPhase(context.Background(), binding.ID, phase, invocationID); err != nil {
			t.Fatal(err)
		}
		binding, err = h.workspaces.CompletePhase(context.Background(), binding.ID, phase, invocationID)
		if err != nil {
			t.Fatal(err)
		}
	}
	binding, err = h.workspaces.Seal(context.Background(), binding.ID)
	if err != nil {
		t.Fatal(err)
	}
	return binding
}

func (h *harness) enqueue(t *testing.T, id CandidateID, binding workspace.Binding) Candidate {
	t.Helper()
	registerArtifact(t, h.artifacts, "project-a", binding.TaskID, "targeted verify passed")
	verifyRef := registerArtifact(t, h.artifacts, "project-a", binding.TaskID, "verify "+string(id))
	diffRef := registerArtifact(t, h.artifacts, "project-a", binding.TaskID, "diff "+string(id))
	candidate, err := h.reconciler.Enqueue(context.Background(), EnqueueRequest{
		ID:                id,
		ProjectID:         "project-a",
		TaskID:            binding.TaskID,
		WorkspaceRef:      binding.ID,
		VerifyResultRef:   verifyRef,
		DiffArtifactRef:   diffRef,
		TargetRepository:  h.repo,
		TargetBranch:      "main",
		BaseRevision:      binding.BaseRevision,
		MainRevision:      gitOut(t, h.repo, "rev-parse", "refs/heads/main"),
		CandidateRevision: binding.CurrentRevision,
		EvidenceRefs:      []evidence.ArtifactID{verifyRef, diffRef},
	})
	if err != nil {
		t.Fatal(err)
	}
	return candidate
}

type fakeVerifier struct {
	result    TargetedVerifyResult
	err       error
	entered   chan struct{}
	release   chan struct{}
	active    atomic.Int32
	maxActive atomic.Int32
	once      sync.Once
}

func (v *fakeVerifier) Verify(context.Context, TargetedVerifyRequest) (TargetedVerifyResult, error) {
	active := v.active.Add(1)
	defer v.active.Add(-1)
	for {
		current := v.maxActive.Load()
		if active <= current || v.maxActive.CompareAndSwap(current, active) {
			break
		}
	}
	if v.entered != nil {
		v.once.Do(func() { close(v.entered) })
	}
	if v.release != nil {
		<-v.release
	}
	return v.result, v.err
}

func seedRepo(t *testing.T) string {
	t.Helper()
	work := filepath.Join(t.TempDir(), "seed")
	if err := os.MkdirAll(work, 0o755); err != nil {
		t.Fatal(err)
	}
	git(t, work, "init", "-b", "main")
	write(t, filepath.Join(work, "README.md"), "seed\n")
	git(t, work, "add", "README.md")
	git(t, work, "-c", "user.email=test@example.com", "-c", "user.name=Test", "commit", "-m", "seed")
	bare := filepath.Join(t.TempDir(), "repo.git")
	git(t, work, "clone", "--bare", work, bare)
	return bare
}

func pushChange(t *testing.T, repo, file, body string) {
	t.Helper()
	clone := filepath.Join(t.TempDir(), "main-change")
	git(t, t.TempDir(), "clone", repo, clone)
	write(t, filepath.Join(clone, filepath.FromSlash(file)), body)
	git(t, clone, "add", file)
	git(t, clone, "-c", "user.email=test@example.com", "-c", "user.name=Test", "commit", "-m", "main change")
	git(t, clone, "push", "origin", "main")
}

func fileAtBranch(t *testing.T, repo, branch, file string) string {
	t.Helper()
	cmd := exec.Command("git", "show", branch+":"+file)
	cmd.Dir = repo
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git show %s:%s: %v\n%s", branch, file, err, out)
	}
	return string(out)
}

func registerArtifact(t *testing.T, registry *evidence.ArtifactRegistry, projectID kernel.ProjectID, taskID kernel.TaskID, body string) evidence.ArtifactID {
	t.Helper()
	artifact, err := registry.Register(context.Background(), evidence.RegisterArtifact{
		Type:      evidence.ArtifactTestOutput,
		ProjectID: projectID,
		TaskID:    taskID,
		Body:      []byte(body),
	})
	if err != nil {
		t.Fatal(err)
	}
	return artifact.ID
}

func write(t *testing.T, name, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(name, []byte(body), 0o644); err != nil {
		t.Fatal(err)
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

func gitOut(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v in %s: %v\n%s", args, dir, err, out)
	}
	return string(bytesTrimSpace(out))
}

func bytesTrimSpace(value []byte) []byte {
	start, end := 0, len(value)
	for start < end && (value[start] == ' ' || value[start] == '\n' || value[start] == '\r' || value[start] == '\t') {
		start++
	}
	for end > start && (value[end-1] == ' ' || value[end-1] == '\n' || value[end-1] == '\r' || value[end-1] == '\t') {
		end--
	}
	return value[start:end]
}
