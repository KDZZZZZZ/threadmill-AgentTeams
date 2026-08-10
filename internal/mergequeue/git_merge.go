package mergequeue

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/workspace"
)

type backendFailure struct {
	reason FailureReason
	err    error
}

func (e backendFailure) Error() string { return e.err.Error() }
func (e backendFailure) Unwrap() error { return e.err }

type PreparedMerge struct {
	Root               string
	TargetRepository   string
	TargetBranch       string
	LatestMainRevision string
	CandidateRevision  string
	AlreadyMerged      bool
}

// GitBackend performs a mechanical merge in a disposable clone and is the
// only package component allowed to push the managed main branch.
type GitBackend struct {
	TempParent string
}

func (b GitBackend) Prepare(ctx context.Context, candidate Candidate, binding workspace.Binding) (prepared PreparedMerge, err error) {
	parent := b.TempParent
	if parent == "" {
		parent = os.TempDir()
	}
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return PreparedMerge{}, fmt.Errorf("create merge temp parent: %w", err)
	}
	root, err := os.MkdirTemp(parent, "threadmill-merge-")
	if err != nil {
		return PreparedMerge{}, fmt.Errorf("create merge temp: %w", err)
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.RemoveAll(root)
		}
	}()

	branch := candidate.TargetBranch
	if branch == "" {
		branch = "main"
	}
	if err := gitRun(ctx, parent, "clone", "--quiet", "--branch", branch, "--single-branch", candidate.TargetRepository, root); err != nil {
		return PreparedMerge{}, backendFailure{reason: FailureMainDrift, err: err}
	}
	latest, err := gitOutput(ctx, root, "rev-parse", "HEAD")
	if err != nil {
		return PreparedMerge{}, backendFailure{reason: FailureMainDrift, err: err}
	}
	if err := gitRun(ctx, root, "fetch", "--quiet", binding.Root, candidate.CandidateRevision); err != nil {
		return PreparedMerge{}, backendFailure{reason: FailureConflict, err: err}
	}
	alreadyMerged := gitRun(ctx, root, "merge-base", "--is-ancestor", "FETCH_HEAD", "HEAD") == nil
	if !alreadyMerged {
		if err := gitRun(ctx, root, "-c", "user.email=threadmill@local", "-c", "user.name=Threadmill Merge Queue", "merge", "--no-ff", "--no-commit", "FETCH_HEAD"); err != nil {
			return PreparedMerge{}, backendFailure{reason: FailureConflict, err: err}
		}
	}
	cleanup = false
	return PreparedMerge{
		Root:               root,
		TargetRepository:   candidate.TargetRepository,
		TargetBranch:       branch,
		LatestMainRevision: latest,
		CandidateRevision:  candidate.CandidateRevision,
		AlreadyMerged:      alreadyMerged,
	}, nil
}

func (b GitBackend) Merge(ctx context.Context, prepared PreparedMerge, candidate Candidate) (string, error) {
	latest, err := gitOutput(ctx, prepared.TargetRepository, "rev-parse", "refs/heads/"+prepared.TargetBranch)
	if err != nil {
		return "", backendFailure{reason: FailureMainDrift, err: err}
	}
	if latest != prepared.LatestMainRevision {
		return "", backendFailure{reason: FailureMainDrift, err: fmt.Errorf("main advanced from %s to %s", prepared.LatestMainRevision, latest)}
	}
	if prepared.AlreadyMerged {
		return latest, nil
	}
	message := fmt.Sprintf("Merge Threadmill candidate %s", candidate.ID)
	if err := gitRun(ctx, prepared.Root, "-c", "user.email=threadmill@local", "-c", "user.name=Threadmill Merge Queue", "commit", "-m", message); err != nil {
		return "", backendFailure{reason: FailureConflict, err: err}
	}
	merged, err := gitOutput(ctx, prepared.Root, "rev-parse", "HEAD")
	if err != nil {
		return "", backendFailure{reason: FailureConflict, err: err}
	}
	if err := gitRun(ctx, prepared.Root, "push", "--porcelain", "origin", "HEAD:refs/heads/"+prepared.TargetBranch); err != nil {
		return "", backendFailure{reason: FailureMainDrift, err: err}
	}
	return merged, nil
}

func (b GitBackend) Cleanup(prepared PreparedMerge) error {
	if strings.TrimSpace(prepared.Root) == "" {
		return nil
	}
	parent := b.TempParent
	if parent == "" {
		parent = os.TempDir()
	}
	absParent, err := filepath.Abs(parent)
	if err != nil {
		return err
	}
	absRoot, err := filepath.Abs(prepared.Root)
	if err != nil {
		return err
	}
	rel, err := filepath.Rel(absParent, absRoot)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return fmt.Errorf("refusing to clean merge workspace outside temp parent")
	}
	return os.RemoveAll(absRoot)
}

func failureReason(err error, fallback FailureReason) FailureReason {
	var failure backendFailure
	if errors.As(err, &failure) && validFailureReason(failure.reason) {
		return failure.reason
	}
	return fallback
}

func gitRun(ctx context.Context, dir string, args ...string) error {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
}

func gitOutput(ctx context.Context, dir string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return strings.TrimSpace(string(out)), nil
}
