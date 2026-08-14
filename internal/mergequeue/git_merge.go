package mergequeue

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/workspace"
)

type backendFailure struct {
	reason FailureReason
	err    error
}

func (e backendFailure) Error() string { return e.err.Error() }
func (e backendFailure) Unwrap() error { return e.err }

type pushCASDriftError struct {
	err error
}

func (e pushCASDriftError) Error() string { return e.err.Error() }
func (e pushCASDriftError) Unwrap() error { return e.err }

type ambiguousPushError struct {
	err error
}

func (e ambiguousPushError) Error() string { return e.err.Error() }
func (e ambiguousPushError) Unwrap() error { return e.err }

type PreparedMerge struct {
	Root                string
	TargetRepository    string
	TargetBranch        string
	LatestMainRevision  string
	CandidateRevision   string
	AlreadyMerged       bool
	NeedsResolution     bool
	ConflictPaths       []string
	AllowedWritePaths   []string
	candidateWritePaths []string
	verifierBaseline    map[string]preparedPathState
}

type preparedPathState struct {
	exists bool
	mode   os.FileMode
	sum    [sha256.Size]byte
	link   string
}

// GitBackend performs a mechanical merge in a disposable clone and is the
// only package component allowed to push the managed main branch.
type GitBackend struct {
	TempParent string

	pushErrorAfterSuccess error
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
	// The disposable verifier workspace has no configured push path. Merge
	// Queue later pushes through the explicit trusted TargetRepository.
	if err := gitRun(ctx, root, "remote", "set-url", "--push", "origin", "threadmill-readonly://mergequeue"); err != nil {
		return PreparedMerge{}, backendFailure{reason: FailurePermission, err: err}
	}
	latest, err := gitOutput(ctx, root, "rev-parse", "HEAD")
	if err != nil {
		return PreparedMerge{}, backendFailure{reason: FailureMainDrift, err: err}
	}
	if err := gitRun(ctx, root, "fetch", "--quiet", binding.Root, candidate.CandidateRevision); err != nil {
		return PreparedMerge{}, backendFailure{reason: FailureConflict, err: err}
	}
	alreadyMerged := gitRun(ctx, root, "merge-base", "--is-ancestor", "FETCH_HEAD", "HEAD") == nil
	allowedWritePaths, err := authorizedCandidatePaths(binding)
	if err != nil {
		return PreparedMerge{}, err
	}
	var conflictPaths []string
	if !alreadyMerged {
		mergeErr := gitRun(ctx, root, "-c", "user.email=threadmill@local", "-c", "user.name=Threadmill Merge Queue", "merge", "--no-ff", "--no-commit", "FETCH_HEAD")
		if mergeErr != nil {
			conflictPaths, err = unmergedPaths(ctx, root)
			if err != nil {
				return PreparedMerge{}, backendFailure{reason: FailureConflict, err: errors.Join(mergeErr, err)}
			}
			if len(conflictPaths) == 0 {
				return PreparedMerge{}, backendFailure{reason: FailureConflict, err: mergeErr}
			}
			if err := requirePathsWithinAuthority(conflictPaths, allowedWritePaths); err != nil {
				return PreparedMerge{}, err
			}
			// A Task workspace also contains Plan metadata and verifier scratch.
			// A conflicted Git merge initially brings the whole candidate commit
			// into the disposable clone, unlike the clean-drift path below which
			// checks out only declared business writes. Restore every non-authority
			// path to latest main before the verifier sees the workspace so those
			// phase-local artifacts can never leak into the merge commit.
			if err := restoreOutsideAuthority(ctx, root, latest, allowedWritePaths); err != nil {
				return PreparedMerge{}, err
			}
			baseline, err := snapshotPreparedPaths(root, allowedWritePaths)
			if err != nil {
				return PreparedMerge{}, err
			}
			cleanup = false
			return PreparedMerge{
				Root:                root,
				TargetRepository:    candidate.TargetRepository,
				TargetBranch:        branch,
				LatestMainRevision:  latest,
				CandidateRevision:   candidate.CandidateRevision,
				NeedsResolution:     true,
				ConflictPaths:       conflictPaths,
				AllowedWritePaths:   append([]string(nil), conflictPaths...),
				candidateWritePaths: allowedWritePaths,
				verifierBaseline:    baseline,
			}, nil
		}
		if err := gitRun(ctx, root, "reset", "--hard", "HEAD"); err != nil {
			return PreparedMerge{}, backendFailure{reason: FailureConflict, err: err}
		}
		args := append([]string{"checkout", "FETCH_HEAD", "--"}, binding.DeclaredWrites.Files...)
		if err := gitRun(ctx, root, args...); err != nil {
			return PreparedMerge{}, backendFailure{reason: FailureConflict, err: err}
		}
		alreadyMerged = gitRun(ctx, root, "diff", "--quiet", "HEAD", "--") == nil
	}
	baseline, err := snapshotPreparedPaths(root, allowedWritePaths)
	if err != nil {
		return PreparedMerge{}, err
	}
	cleanup = false
	return PreparedMerge{
		Root:                root,
		TargetRepository:    candidate.TargetRepository,
		TargetBranch:        branch,
		LatestMainRevision:  latest,
		CandidateRevision:   candidate.CandidateRevision,
		AlreadyMerged:       alreadyMerged,
		candidateWritePaths: allowedWritePaths,
		verifierBaseline:    baseline,
	}, nil
}

func restoreOutsideAuthority(ctx context.Context, root, mainRevision string, allowed []string) error {
	changed, err := changedPaths(ctx, root, mainRevision)
	if err != nil {
		return backendFailure{reason: FailureConflict, err: err}
	}
	for _, file := range changed {
		if pathInSet(file, allowed) {
			continue
		}
		if gitRun(ctx, root, "cat-file", "-e", mainRevision+":"+file) == nil {
			if err := gitRun(ctx, root, "restore", "--source", mainRevision, "--staged", "--worktree", "--", file); err != nil {
				return backendFailure{reason: FailureConflict, err: fmt.Errorf("restore non-candidate merge path %s: %w", file, err)}
			}
			continue
		}
		if err := gitRun(ctx, root, "rm", "-f", "--ignore-unmatch", "--", file); err != nil {
			return backendFailure{reason: FailureConflict, err: fmt.Errorf("remove non-candidate merge path %s: %w", file, err)}
		}
		if err := os.RemoveAll(filepath.Join(root, filepath.FromSlash(file))); err != nil {
			return backendFailure{reason: FailureConflict, err: fmt.Errorf("remove untracked non-candidate merge path %s: %w", file, err)}
		}
	}
	return nil
}

func (b GitBackend) CreateMergeCommit(ctx context.Context, prepared PreparedMerge, candidate Candidate) (string, error) {
	if err := stageAndValidatePreparedMerge(ctx, prepared); err != nil {
		return "", err
	}
	if prepared.AlreadyMerged {
		return prepared.LatestMainRevision, nil
	}
	message := fmt.Sprintf("Merge Threadmill candidate %s", candidate.ID)
	if err := gitRun(ctx, prepared.Root, "-c", "user.email=threadmill@local", "-c", "user.name=Threadmill Merge Queue", "commit", "-m", message); err != nil {
		return "", backendFailure{reason: FailureConflict, err: err}
	}
	merged, err := gitOutput(ctx, prepared.Root, "rev-parse", "HEAD")
	if err != nil {
		return "", backendFailure{reason: FailureConflict, err: err}
	}
	return merged, nil
}

func stageAndValidatePreparedMerge(ctx context.Context, prepared PreparedMerge) error {
	head, err := gitOutput(ctx, prepared.Root, "rev-parse", "HEAD")
	if err != nil {
		return backendFailure{reason: FailureConflict, err: err}
	}
	if head != prepared.LatestMainRevision {
		return backendFailure{reason: FailurePermission, err: fmt.Errorf("targeted verifier changed repository HEAD")}
	}
	if err := requireVerifierWriteBoundary(prepared); err != nil {
		return err
	}
	changed, err := changedPaths(ctx, prepared.Root, prepared.LatestMainRevision)
	if err != nil {
		return backendFailure{reason: FailureConflict, err: err}
	}
	if err := requirePathsWithinAuthority(changed, prepared.candidateWritePaths); err != nil {
		return err
	}
	if prepared.NeedsResolution {
		for _, file := range prepared.ConflictPaths {
			contents, readErr := os.ReadFile(filepath.Join(prepared.Root, filepath.FromSlash(file)))
			if readErr != nil && !os.IsNotExist(readErr) {
				return backendFailure{reason: FailureVerifyFailed, err: fmt.Errorf("inspect targeted conflict resolution %s: %w", file, readErr)}
			}
			if readErr == nil && hasConflictMarkers(contents) {
				return backendFailure{reason: FailureVerifyFailed, err: fmt.Errorf("targeted verifier left unresolved conflict %s", file)}
			}
		}
		args := append([]string{"add", "-A", "--"}, prepared.AllowedWritePaths...)
		if err := gitRun(ctx, prepared.Root, args...); err != nil {
			return backendFailure{reason: FailureConflict, err: err}
		}
		unresolved, err := unmergedPaths(ctx, prepared.Root)
		if err != nil {
			return backendFailure{reason: FailureConflict, err: err}
		}
		if len(unresolved) != 0 {
			return backendFailure{reason: FailureVerifyFailed, err: fmt.Errorf("targeted verifier left unresolved conflicts: %s", strings.Join(unresolved, ", "))}
		}
	}
	changed, err = changedPaths(ctx, prepared.Root, prepared.LatestMainRevision)
	if err != nil {
		return backendFailure{reason: FailureConflict, err: err}
	}
	if err := requirePathsWithinAuthority(changed, prepared.candidateWritePaths); err != nil {
		return err
	}
	return nil
}

func snapshotPreparedPaths(root string, paths []string) (map[string]preparedPathState, error) {
	states := make(map[string]preparedPathState, len(paths))
	for _, file := range cleanPathSet(paths) {
		state, err := preparedPathFingerprint(filepath.Join(root, filepath.FromSlash(file)))
		if err != nil {
			return nil, backendFailure{reason: FailurePermission, err: fmt.Errorf("snapshot prepared path %s: %w", file, err)}
		}
		states[file] = state
	}
	return states, nil
}

func requireVerifierWriteBoundary(prepared PreparedMerge) error {
	for file, before := range prepared.verifierBaseline {
		if pathInSet(file, prepared.AllowedWritePaths) {
			continue
		}
		after, err := preparedPathFingerprint(filepath.Join(prepared.Root, filepath.FromSlash(file)))
		if err != nil {
			return backendFailure{reason: FailurePermission, err: fmt.Errorf("inspect verifier path %s: %w", file, err)}
		}
		if before != after {
			return backendFailure{reason: FailurePermission, err: fmt.Errorf("targeted verifier changed read-only path %s", file)}
		}
	}
	return nil
}

func preparedPathFingerprint(file string) (preparedPathState, error) {
	info, err := os.Lstat(file)
	if os.IsNotExist(err) {
		return preparedPathState{}, nil
	}
	if err != nil {
		return preparedPathState{}, err
	}
	state := preparedPathState{exists: true, mode: info.Mode()}
	if info.Mode()&os.ModeSymlink != 0 {
		state.link, err = os.Readlink(file)
		return state, err
	}
	if !info.Mode().IsRegular() {
		return preparedPathState{}, fmt.Errorf("prepared path is not a regular file")
	}
	contents, err := os.ReadFile(file)
	if err != nil {
		return preparedPathState{}, err
	}
	state.sum = sha256.Sum256(contents)
	return state, nil
}

func hasConflictMarkers(contents []byte) bool {
	text := string(contents)
	return strings.Contains(text, "<<<<<<< ") && strings.Contains(text, "=======") && strings.Contains(text, ">>>>>>> ")
}

func authorizedCandidatePaths(binding workspace.Binding) ([]string, error) {
	declared := cleanPathSet(binding.DeclaredWrites.Files)
	observed := cleanPathSet(binding.ObservedWrites.Files)
	if len(declared) == 0 || len(observed) == 0 {
		return nil, backendFailure{reason: FailurePermission, err: fmt.Errorf("candidate declared and observed write paths are required")}
	}
	for _, file := range observed {
		if !pathInSet(file, declared) {
			return nil, backendFailure{reason: FailurePermission, err: fmt.Errorf("observed candidate path %s is outside declared writes", file)}
		}
	}
	return observed, nil
}

func requirePathsWithinAuthority(paths, allowed []string) error {
	for _, file := range cleanPathSet(paths) {
		if !pathInSet(file, allowed) {
			return backendFailure{reason: FailurePermission, err: fmt.Errorf("merge path %s is outside candidate write authority", file)}
		}
	}
	return nil
}

func pathInSet(file string, allowed []string) bool {
	file = filepath.ToSlash(filepath.Clean(file))
	for _, candidate := range allowed {
		if file == filepath.ToSlash(filepath.Clean(candidate)) {
			return true
		}
	}
	return false
}

func cleanPathSet(paths []string) []string {
	seen := make(map[string]struct{}, len(paths))
	out := make([]string, 0, len(paths))
	for _, file := range paths {
		file = strings.TrimSpace(filepath.ToSlash(filepath.Clean(file)))
		if file == "" || file == "." {
			continue
		}
		if _, ok := seen[file]; ok {
			continue
		}
		seen[file] = struct{}{}
		out = append(out, file)
	}
	sort.Strings(out)
	return out
}

func unmergedPaths(ctx context.Context, root string) ([]string, error) {
	out, err := gitOutputRaw(ctx, root, "diff", "--name-only", "--diff-filter=U", "-z")
	if err != nil {
		return nil, err
	}
	return splitGitPaths(out), nil
}

func changedPaths(ctx context.Context, root, baseRevision string) ([]string, error) {
	out, err := gitOutputRaw(ctx, root, "diff", "--name-only", "-z", baseRevision, "--")
	if err != nil {
		return nil, err
	}
	paths := splitGitPaths(out)
	untracked, err := gitOutputRaw(ctx, root, "ls-files", "--others", "--exclude-standard", "-z")
	if err != nil {
		return nil, err
	}
	return cleanPathSet(append(paths, splitGitPaths(untracked)...)), nil
}

func splitGitPaths(value string) []string {
	if value == "" {
		return nil
	}
	parts := strings.Split(value, "\x00")
	paths := make([]string, 0, len(parts))
	for _, part := range parts {
		if part != "" {
			paths = append(paths, part)
		}
	}
	return cleanPathSet(paths)
}

func (b GitBackend) PushExact(ctx context.Context, prepared PreparedMerge, expectedMergedRevision string) error {
	if strings.TrimSpace(expectedMergedRevision) == "" {
		return backendFailure{reason: FailureConflict, err: fmt.Errorf("expected merged revision is required")}
	}
	latest, err := gitOutput(ctx, prepared.TargetRepository, "rev-parse", "refs/heads/"+prepared.TargetBranch)
	if err != nil {
		return ambiguousPushError{err: err}
	}
	if latest != prepared.LatestMainRevision {
		return pushCASDriftError{err: fmt.Errorf("main advanced from %s to %s", prepared.LatestMainRevision, latest)}
	}
	if prepared.AlreadyMerged && expectedMergedRevision == latest {
		return nil
	}
	actual, err := gitOutput(ctx, prepared.Root, "rev-parse", "HEAD")
	if err != nil {
		return ambiguousPushError{err: err}
	}
	if actual != expectedMergedRevision {
		return ambiguousPushError{err: fmt.Errorf("prepared merge commit changed from %s to %s", expectedMergedRevision, actual)}
	}
	if err := gitRun(ctx, prepared.Root, "push", "--porcelain", prepared.TargetRepository, expectedMergedRevision+":refs/heads/"+prepared.TargetBranch); err != nil {
		if isCheckedOutBranchPushRefusal(err) {
			if directErr := pushToCheckedOutLocalRepository(ctx, prepared, expectedMergedRevision); directErr == nil {
				return nil
			}
		}
		return ambiguousPushError{err: err}
	}
	if b.pushErrorAfterSuccess != nil {
		return ambiguousPushError{err: b.pushErrorAfterSuccess}
	}
	return nil
}

func isCheckedOutBranchPushRefusal(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "branch is currently checked out") || strings.Contains(msg, "refusing to update checked out branch")
}

func pushToCheckedOutLocalRepository(ctx context.Context, prepared PreparedMerge, expectedMergedRevision string) error {
	latest, err := gitOutput(ctx, prepared.TargetRepository, "rev-parse", "refs/heads/"+prepared.TargetBranch)
	if err != nil {
		return err
	}
	if latest != prepared.LatestMainRevision {
		return pushCASDriftError{err: fmt.Errorf("main advanced from %s to %s", prepared.LatestMainRevision, latest)}
	}
	if err := gitRun(ctx, prepared.TargetRepository, "fetch", "--quiet", prepared.Root, expectedMergedRevision); err != nil {
		return err
	}
	if err := gitRun(ctx, prepared.TargetRepository, "reset", "--hard", expectedMergedRevision); err != nil {
		return err
	}
	return nil
}

func (b GitBackend) ContainsRevision(ctx context.Context, targetRepository, targetBranch, revision string) (bool, error) {
	if strings.TrimSpace(revision) == "" {
		return false, backendFailure{reason: FailureConflict, err: fmt.Errorf("revision is required")}
	}
	branch := targetBranch
	if branch == "" {
		branch = "main"
	}
	if err := gitRun(ctx, targetRepository, "cat-file", "-e", revision+"^{commit}"); err != nil {
		return false, nil
	}
	err := gitRun(ctx, targetRepository, "merge-base", "--is-ancestor", revision, "refs/heads/"+branch)
	if err == nil {
		return true, nil
	}
	if strings.Contains(err.Error(), "exit status 1") {
		return false, nil
	}
	if _, refErr := gitOutput(ctx, targetRepository, "rev-parse", "refs/heads/"+branch); refErr != nil {
		return false, backendFailure{reason: FailureMainDrift, err: refErr}
	}
	return false, nil
}

func (b GitBackend) Merge(ctx context.Context, prepared PreparedMerge, candidate Candidate) (string, error) {
	merged, err := b.CreateMergeCommit(ctx, prepared, candidate)
	if err != nil {
		return "", err
	}
	if err := b.PushExact(ctx, prepared, merged); err != nil {
		return "", err
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
	var drift pushCASDriftError
	if errors.As(err, &drift) {
		return FailureMainDrift
	}
	var failure backendFailure
	if errors.As(err, &failure) && validFailureReason(failure.reason) {
		return failure.reason
	}
	return fallback
}

func isPushCASDrift(err error) bool {
	var drift pushCASDriftError
	return errors.As(err, &drift)
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
	out, err := gitOutputRaw(ctx, dir, args...)
	return strings.TrimSpace(out), err
}

func gitOutputRaw(ctx context.Context, dir string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		return "", fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}
