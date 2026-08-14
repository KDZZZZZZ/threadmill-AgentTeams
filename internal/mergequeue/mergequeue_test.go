package mergequeue

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
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
	if calls := h.verifier.calls.Load(); calls != 0 {
		t.Fatalf("unchanged main invoked targeted verifier %d times, want 0", calls)
	}
	if !containsArtifact(events[1].ArtifactRefs, candidate.VerifyResultRef) || !containsArtifact(merged.EvidenceRefs, candidate.VerifyResultRef) {
		t.Fatalf("fast path dropped trusted verify evidence: merged=%#v event=%#v", merged.EvidenceRefs, events[1].ArtifactRefs)
	}
}

func TestMergeQueueFailsWithEvidenceForPermissionConflictVerifyAndMainDrift(t *testing.T) {
	t.Run("permission", func(t *testing.T) {
		h := newHarness(t)
		binding := h.workspace(t, "task-a", 1, "workspace/a.txt", "candidate\n")
		h.enqueue(t, "candidate-permission", binding)
		binding.Status = workspace.StatusPrepared
		if _, err := h.wsStore.UpdateCAS(context.Background(), binding, binding.Revision); err != nil {
			t.Fatal(err)
		}
		failed, claimed, err := h.reconciler.ReconcileOne(context.Background(), h.repo)
		assertFailure(t, failed, claimed, err, FailurePermission)
	})

	t.Run("conflict_rejected_by_verifier", func(t *testing.T) {
		h := newHarness(t)
		binding := h.workspaceAllowed(t, "task-a", 1, "README.md", "candidate\n", []string{"README.md"})
		h.enqueue(t, "candidate-conflict", binding)
		pushChange(t, h.repo, "README.md", "main drift\n")
		h.verifier.result = TargetedVerifyResult{Passed: false, EvidenceRefs: h.verifier.result.EvidenceRefs}
		failed, claimed, err := h.reconciler.ReconcileOne(context.Background(), h.repo)
		assertFailure(t, failed, claimed, err, FailureVerifyFailed)
	})

	t.Run("verify_failed", func(t *testing.T) {
		h := newHarness(t)
		binding := h.workspace(t, "task-a", 1, "workspace/a.txt", "candidate\n")
		h.enqueue(t, "candidate-verify", binding)
		pushChange(t, h.repo, "workspace/a.txt", "main advanced\n")
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
		pushChange(t, h.repo, "workspace/a.txt", "main advanced\n")
		h.verifier.err = MainDrift("main-old", "main-new")
		failed, claimed, err := h.reconciler.ReconcileOne(context.Background(), h.repo)
		assertFailure(t, failed, claimed, err, FailureMainDrift)
	})

	t.Run("verifier_terminal_proposal", func(t *testing.T) {
		h := newHarness(t)
		binding := h.workspace(t, "task-a", 1, "workspace/a.txt", "candidate\n")
		h.enqueue(t, "candidate-verifier-proposal", binding)
		pushChange(t, h.repo, "workspace/a.txt", "main advanced\n")
		proposalEvidence := registerArtifact(t, h.artifacts, "project-a", "task-a", "verifier proposal persisted")
		h.verifier.result = TargetedVerifyResult{EvidenceRefs: []evidence.ArtifactID{proposalEvidence}}
		h.verifier.err = kernel.TransitionRejected("targeted verifier submitted orchestration proposal")

		failed, claimed, err := h.reconciler.ReconcileOne(context.Background(), h.repo)
		assertFailure(t, failed, claimed, err, FailureVerifyFailed)
		if !containsArtifact(failed.EvidenceRefs, proposalEvidence) {
			t.Fatalf("verifier proposal evidence dropped: %#v", failed.EvidenceRefs)
		}
	})

	t.Run("targeted_evidence_acl", func(t *testing.T) {
		h := newHarness(t)
		binding := h.workspace(t, "task-a", 1, "workspace/a.txt", "candidate\n")
		h.enqueue(t, "candidate-targeted-acl", binding)
		pushChange(t, h.repo, "workspace/a.txt", "main advanced\n")
		foreignEvidence := registerArtifact(t, h.artifacts, "project-a", "task-other", "foreign targeted evidence")
		h.verifier.result = TargetedVerifyResult{Passed: true, EvidenceRefs: []evidence.ArtifactID{foreignEvidence}}

		failed, claimed, err := h.reconciler.ReconcileOne(context.Background(), h.repo)
		assertFailure(t, failed, claimed, err, FailureVerifyFailed)
		if containsArtifact(failed.EvidenceRefs, foreignEvidence) {
			t.Fatalf("cross-task verifier evidence persisted: %#v", failed.EvidenceRefs)
		}
	})

	t.Run("main_drift", func(t *testing.T) {
		h := newHarness(t)
		binding := h.workspace(t, "task-a", 1, "workspace/a.txt", "candidate\n")
		h.enqueue(t, "candidate-drift", binding)
		pushChange(t, h.repo, "workspace/a.txt", "main advanced\n")
		h.verifier.resolve = func(req TargetedVerifyRequest) error {
			return os.WriteFile(filepath.Join(req.WorkspaceRoot, "workspace", "a.txt"), []byte("resolved\n"), 0o644)
		}
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
	pushChange(t, h.repo, "workspace/a.txt", "main advanced\n")
	h.verifier.resolve = func(req TargetedVerifyRequest) error {
		return os.WriteFile(filepath.Join(req.WorkspaceRoot, "workspace", "a.txt"), []byte("resolved\n"), 0o644)
	}
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

func TestMergeQueueTargetedVerifierResolvesAuthorizedConflict(t *testing.T) {
	h := newHarness(t)
	binding := h.workspace(t, "task-a", 1, "workspace/a.txt", "candidate\n")
	h.enqueue(t, "candidate-resolved-conflict", binding)
	pushChange(t, h.repo, "workspace/a.txt", "main\n")
	h.verifier.resolve = func(req TargetedVerifyRequest) error {
		if req.Candidate.ID != "candidate-resolved-conflict" {
			return fmt.Errorf("unexpected candidate %s", req.Candidate.ID)
		}
		if !reflect.DeepEqual(req.ConflictPaths, []string{"workspace/a.txt"}) || !reflect.DeepEqual(req.AllowedWritePaths, req.ConflictPaths) {
			return fmt.Errorf("unexpected conflict authority: conflicts=%v allowed=%v", req.ConflictPaths, req.AllowedWritePaths)
		}
		pushURL, err := gitOutput(context.Background(), req.WorkspaceRoot, "remote", "get-url", "--push", "origin")
		if err != nil || pushURL != "threadmill-readonly://mergequeue" {
			return fmt.Errorf("verifier workspace push URL = %q err=%v", pushURL, err)
		}
		return os.WriteFile(filepath.Join(req.WorkspaceRoot, "workspace", "a.txt"), []byte("resolved\n"), 0o644)
	}

	merged, claimed, err := h.reconciler.ReconcileOne(context.Background(), h.repo)
	if err != nil || !claimed || merged.Status != StatusMerged {
		t.Fatalf("resolved conflict = %#v claimed=%v err=%v", merged, claimed, err)
	}
	if got := fileAtBranch(t, h.repo, "main", "workspace/a.txt"); got != "resolved\n" {
		t.Fatalf("resolved file = %q", got)
	}
	if calls := h.verifier.calls.Load(); calls != 1 {
		t.Fatalf("targeted verifier calls = %d, want 1", calls)
	}
}

func TestMergeQueueConflictExcludesPlanMetadataFromMergeCommit(t *testing.T) {
	h := newHarness(t)
	binding := h.workspace(t, "task-a", 1, "workspace/a.txt", "candidate\n")
	write(t, filepath.Join(binding.Root, "plan", "declared-writes.json"), `{"files":["workspace/a.txt"]}`+"\n")
	git(t, binding.Root, "add", "plan/declared-writes.json")
	git(t, binding.Root, "-c", "user.email=test@example.com", "-c", "user.name=Test", "commit", "-m", "plan metadata")
	var err error
	binding, err = h.workspaces.RefreshObservedWrites(context.Background(), binding.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(binding.ObservedWrites.Files, []string{"workspace/a.txt"}) {
		t.Fatalf("candidate observed writes = %v, want business write only", binding.ObservedWrites.Files)
	}
	h.enqueue(t, "candidate-conflict-with-plan-metadata", binding)
	pushChange(t, h.repo, "workspace/a.txt", "main\n")
	h.verifier.resolve = func(req TargetedVerifyRequest) error {
		return os.WriteFile(filepath.Join(req.WorkspaceRoot, "workspace", "a.txt"), []byte("resolved\n"), 0o644)
	}

	merged, claimed, err := h.reconciler.ReconcileOne(context.Background(), h.repo)
	if err != nil || !claimed || merged.Status != StatusMerged {
		t.Fatalf("resolved conflict with plan metadata = %#v claimed=%v err=%v", merged, claimed, err)
	}
	if got := fileAtBranch(t, h.repo, "main", "workspace/a.txt"); got != "resolved\n" {
		t.Fatalf("resolved file = %q", got)
	}
	if got := fileAtBranchIfExists(t, h.repo, "main", "plan/declared-writes.json"); got != "" {
		t.Fatalf("plan metadata leaked into merged branch: %q", got)
	}
}

func TestMergeQueueCleanMainDriftMergesWithoutPreAcceptanceVerify(t *testing.T) {
	h := newHarness(t)
	binding := h.workspace(t, "task-a", 1, "workspace/a.txt", "candidate\n")
	h.enqueue(t, "candidate-clean-drift", binding)
	pushChange(t, h.repo, "unrelated.txt", "main advanced\n")

	merged, claimed, err := h.reconciler.ReconcileOne(context.Background(), h.repo)
	if err != nil || !claimed || merged.Status != StatusMerged {
		t.Fatalf("clean drift = %#v claimed=%v err=%v", merged, claimed, err)
	}
	if calls := h.verifier.calls.Load(); calls != 0 {
		t.Fatalf("clean drift targeted verifier calls = %d, want 0", calls)
	}
}

func TestMergeQueueAcceptsCompletedExecuteWithoutCompletedVerify(t *testing.T) {
	h := newHarness(t)
	binding := h.workspace(t, "task-a", 1, "workspace/a.txt", "candidate\n")
	binding.PhaseLeases[workspace.PhaseVerify] = ""
	if _, err := h.wsStore.UpdateCAS(context.Background(), binding, binding.Revision); err != nil {
		t.Fatal(err)
	}
	h.enqueue(t, "candidate-execute-first", binding)

	merged, claimed, err := h.reconciler.ReconcileOne(context.Background(), h.repo)
	if err != nil || !claimed || merged.Status != StatusMerged {
		t.Fatalf("execute-first merge = %#v claimed=%v err=%v", merged, claimed, err)
	}
	if calls := h.verifier.calls.Load(); calls != 0 {
		t.Fatalf("clean execute-first merge invoked verifier %d times", calls)
	}
}

func TestMergeQueueRejectsUnresolvedAndOutOfScopeConflict(t *testing.T) {
	t.Run("unresolved", func(t *testing.T) {
		h := newHarness(t)
		binding := h.workspace(t, "task-a", 1, "workspace/a.txt", "candidate\n")
		h.enqueue(t, "candidate-unresolved-conflict", binding)
		pushChange(t, h.repo, "workspace/a.txt", "main\n")

		failed, claimed, err := h.reconciler.ReconcileOne(context.Background(), h.repo)
		assertFailure(t, failed, claimed, err, FailureVerifyFailed)
		if got := fileAtBranch(t, h.repo, "main", "workspace/a.txt"); got != "main\n" {
			t.Fatalf("unresolved conflict changed main: %q", got)
		}
	})

	t.Run("out_of_scope", func(t *testing.T) {
		h := newHarness(t)
		binding := h.workspace(t, "task-a", 1, "workspace/a.txt", "candidate\n")
		binding.DeclaredWrites.Files = []string{"workspace/other.txt"}
		if _, err := h.wsStore.UpdateCAS(context.Background(), binding, binding.Revision); err != nil {
			t.Fatal(err)
		}
		h.enqueue(t, "candidate-out-of-scope-conflict", binding)
		pushChange(t, h.repo, "workspace/a.txt", "main\n")

		failed, claimed, err := h.reconciler.ReconcileOne(context.Background(), h.repo)
		assertFailure(t, failed, claimed, err, FailurePermission)
		if calls := h.verifier.calls.Load(); calls != 0 {
			t.Fatalf("out-of-scope conflict invoked verifier %d times", calls)
		}
	})
}

func TestMergeQueueRejectsVerifierWriteOutsideAuthorizedCandidatePaths(t *testing.T) {
	h := newHarness(t)
	binding := h.workspace(t, "task-a", 1, "workspace/a.txt", "candidate\n")
	h.enqueue(t, "candidate-verifier-scope-escape", binding)
	pushChange(t, h.repo, "workspace/a.txt", "main advanced\n")
	h.verifier.resolve = func(req TargetedVerifyRequest) error {
		return os.WriteFile(filepath.Join(req.WorkspaceRoot, "workspace", "escape.txt"), []byte("escape\n"), 0o644)
	}

	failed, claimed, err := h.reconciler.ReconcileOne(context.Background(), h.repo)
	assertFailure(t, failed, claimed, err, FailurePermission)
	if got := fileAtBranchIfExists(t, h.repo, "main", "workspace/escape.txt"); got != "" {
		t.Fatalf("scope-escaping verifier wrote main: %q", got)
	}
}

func TestMergeQueueCleanDriftDoesNotInvokeVerifierRewrite(t *testing.T) {
	h := newHarness(t)
	binding := h.workspace(t, "task-a", 1, "workspace/a.txt", "candidate\n")
	h.enqueue(t, "candidate-clean-drift-rewrite", binding)
	pushChange(t, h.repo, "unrelated.txt", "main advanced\n")
	h.verifier.resolve = func(req TargetedVerifyRequest) error {
		if len(req.AllowedWritePaths) != 0 {
			return fmt.Errorf("clean drift unexpectedly granted writes: %v", req.AllowedWritePaths)
		}
		return os.WriteFile(filepath.Join(req.WorkspaceRoot, "workspace", "a.txt"), []byte("rewritten\n"), 0o644)
	}

	merged, claimed, err := h.reconciler.ReconcileOne(context.Background(), h.repo)
	if err != nil || !claimed || merged.Status != StatusMerged {
		t.Fatalf("clean drift = %#v claimed=%v err=%v", merged, claimed, err)
	}
	if calls := h.verifier.calls.Load(); calls != 0 {
		t.Fatalf("clean drift invoked verifier %d times", calls)
	}
	if got := fileAtBranchIfExists(t, h.repo, "main", "workspace/a.txt"); got != "candidate" {
		t.Fatalf("mechanical merge content = %q, want candidate", got)
	}
}

func TestMergeQueueRejectsVerifierCreatedCommit(t *testing.T) {
	h := newHarness(t)
	binding := h.workspace(t, "task-a", 1, "workspace/a.txt", "candidate\n")
	h.enqueue(t, "candidate-verifier-commit", binding)
	pushChange(t, h.repo, "workspace/a.txt", "main advanced\n")
	h.verifier.resolve = func(req TargetedVerifyRequest) error {
		if err := gitRun(context.Background(), req.WorkspaceRoot, "add", "--", "workspace/a.txt"); err != nil {
			return err
		}
		return gitRun(context.Background(), req.WorkspaceRoot, "-c", "user.email=verifier@example.com", "-c", "user.name=Verifier", "commit", "-m", "verifier must not commit")
	}

	failed, claimed, err := h.reconciler.ReconcileOne(context.Background(), h.repo)
	assertFailure(t, failed, claimed, err, FailurePermission)
	if got := fileAtBranchIfExists(t, h.repo, "main", "workspace/a.txt"); got != "main advanced" {
		t.Fatalf("verifier commit changed main: %q", got)
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
	wsStore    *workspace.MemoryStore
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
	wsStore := workspace.NewMemoryStore()
	workspaces := workspace.NewServiceWithStore(wsStore, workspace.NewLocalGitBackend())
	events := evidence.NewEventLog(64 * 1024)
	return &harness{
		repo:       repo,
		workspaces: workspaces,
		wsStore:    wsStore,
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
		DeclaredWrites: workspace.WriteSet{Files: []string{file}},
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
	calls     atomic.Int32
	once      sync.Once
	request   TargetedVerifyRequest
	resolve   func(TargetedVerifyRequest) error
}

func (v *fakeVerifier) Verify(ctx context.Context, req TargetedVerifyRequest) (TargetedVerifyResult, error) {
	v.calls.Add(1)
	v.request = req
	if v.resolve != nil {
		if err := v.resolve(req); err != nil {
			return TargetedVerifyResult{}, err
		}
	}
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
		select {
		case <-v.release:
		case <-ctx.Done():
			return TargetedVerifyResult{}, ctx.Err()
		}
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
