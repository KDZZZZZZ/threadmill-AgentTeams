package workspace

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

// GitBackend owns local Git side effects. Binding state remains authoritative
// in BindingStore; this adapter is deliberately idempotent so a process can
// materialize the same persisted Binding again after a restart.
type GitBackend interface {
	ResolveRevision(context.Context, string, string) (string, error)
	Materialize(context.Context, Binding) (Materialization, error)
	Remove(context.Context, Binding, Materialization) error
	CurrentRevision(context.Context, Binding) (string, error)
	ObservedWrites(context.Context, Binding) ([]string, error)
	Diff(context.Context, Binding, string) (string, error)
}

type LocalGitBackend struct{}

type Materialization struct {
	Head            string
	WorktreeCreated bool
	BranchCreated   bool
}

func NewLocalGitBackend() *LocalGitBackend { return &LocalGitBackend{} }

func (LocalGitBackend) ResolveRevision(ctx context.Context, repositoryPath, revision string) (string, error) {
	if strings.TrimSpace(repositoryPath) == "" {
		return "", kernelInvalidRepository()
	}
	if revision == "" {
		revision = "HEAD"
	}
	return gitOutput(ctx, repositoryPath, "rev-parse", "--verify", revision+"^{commit}")
}

func (backend LocalGitBackend) Materialize(ctx context.Context, binding Binding) (Materialization, error) {
	if binding.Kind != KindGitWorktree {
		return Materialization{}, unsupportedWorkspaceKind(binding.Kind)
	}
	if strings.TrimSpace(binding.RepositoryPath) == "" || strings.TrimSpace(binding.Root) == "" || strings.TrimSpace(binding.BranchName) == "" {
		return Materialization{}, fmt.Errorf("materialize workspace: repository path, root, and branch are required")
	}
	if err := ensureMaterializationRoot(binding.Root); err != nil {
		return Materialization{}, err
	}
	repositoryAbs, err := filepath.Abs(binding.RepositoryPath)
	if err != nil {
		return Materialization{}, fmt.Errorf("resolve repository path: %w", err)
	}
	if _, err := gitOutput(ctx, repositoryAbs, "rev-parse", "--git-dir"); err != nil {
		return Materialization{}, fmt.Errorf("validate git repository: %w", err)
	}

	if info, statErr := os.Stat(binding.Root); statErr == nil {
		if !info.IsDir() {
			return Materialization{}, fmt.Errorf("workspace root is not a directory")
		}
		head, validErr := validateExistingWorktree(ctx, repositoryAbs, binding)
		if validErr == nil {
			if err := ensureWorkspaceDirectories(binding.Root); err != nil {
				return Materialization{}, err
			}
			if err := validateWorkspaceSymlinks(binding.Root); err != nil {
				return Materialization{}, err
			}
			return Materialization{Head: head}, nil
		}
		entries, readErr := os.ReadDir(binding.Root)
		if readErr != nil {
			return Materialization{}, fmt.Errorf("inspect workspace root: %w", readErr)
		}
		if len(entries) != 0 {
			return Materialization{}, fmt.Errorf("workspace root already exists but is not the requested worktree: %w", validErr)
		}
	} else if !os.IsNotExist(statErr) {
		return Materialization{}, fmt.Errorf("inspect workspace root: %w", statErr)
	}

	branchExists, err := gitRefExists(ctx, repositoryAbs, "refs/heads/"+binding.BranchName)
	if err != nil {
		return Materialization{}, err
	}
	materialized := Materialization{WorktreeCreated: true, BranchCreated: !branchExists}
	args := []string{"worktree", "add"}
	if branchExists {
		args = append(args, binding.Root, binding.BranchName)
	} else {
		args = append(args, "-b", binding.BranchName, binding.Root, binding.BaseRevision)
	}
	if err := gitRun(ctx, repositoryAbs, args...); err != nil {
		// A crashed process can leave a missing worktree registered. Pruning is
		// safe here because Git only removes stale administrative entries.
		_ = gitRun(ctx, repositoryAbs, "worktree", "prune")
		if retryErr := gitRun(ctx, repositoryAbs, args...); retryErr != nil {
			_ = backend.Remove(context.Background(), binding, materialized)
			return Materialization{}, retryErr
		}
	}
	if err := ensureWorkspaceDirectories(binding.Root); err != nil {
		_ = backend.Remove(context.Background(), binding, materialized)
		return Materialization{}, err
	}
	if err := validateWorkspaceSymlinks(binding.Root); err != nil {
		_ = backend.Remove(context.Background(), binding, materialized)
		return Materialization{}, err
	}
	materialized.Head, err = validateExistingWorktree(ctx, repositoryAbs, binding)
	if err != nil {
		_ = backend.Remove(context.Background(), binding, materialized)
		return Materialization{}, err
	}
	return materialized, nil
}

func (LocalGitBackend) Remove(ctx context.Context, binding Binding, materialized Materialization) error {
	if strings.TrimSpace(binding.RepositoryPath) == "" {
		return nil
	}
	var errs []error
	if materialized.WorktreeCreated {
		if err := gitRun(ctx, binding.RepositoryPath, "worktree", "remove", "--force", binding.Root); err != nil && !os.IsNotExist(err) {
			errs = append(errs, err)
		}
	}
	if materialized.BranchCreated {
		if err := gitRun(ctx, binding.RepositoryPath, "branch", "-D", binding.BranchName); err != nil {
			// The branch may not have been created if materialization failed early.
			exists, existsErr := gitRefExists(ctx, binding.RepositoryPath, "refs/heads/"+binding.BranchName)
			if existsErr != nil || exists {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

func (LocalGitBackend) CurrentRevision(ctx context.Context, binding Binding) (string, error) {
	return gitOutput(ctx, binding.Root, "rev-parse", "HEAD")
}

func (LocalGitBackend) ObservedWrites(ctx context.Context, binding Binding) ([]string, error) {
	return observedGitFiles(ctx, binding.Root, binding.BaseRevision)
}

func (LocalGitBackend) Diff(ctx context.Context, binding Binding, relativePath string) (string, error) {
	args := []string{"diff", "--binary", binding.BaseRevision, "--"}
	if relativePath != "" {
		args = append(args, filepath.ToSlash(relativePath))
	}
	tracked, err := gitOutput(ctx, binding.Root, args...)
	if err != nil {
		return "", err
	}
	untrackedArgs := []string{"ls-files", "--others", "--exclude-standard"}
	if relativePath != "" {
		untrackedArgs = append(untrackedArgs, "--", filepath.ToSlash(relativePath))
	}
	untracked, err := gitOutput(ctx, binding.Root, untrackedArgs...)
	if err != nil {
		return "", err
	}
	patches := []string{tracked}
	for _, file := range strings.Split(untracked, "\n") {
		file = strings.TrimSpace(file)
		if file == "" {
			continue
		}
		patch, diffErr := gitOutputWithExitCodes(ctx, binding.Root, []int{0, 1}, "diff", "--binary", "--no-index", "--", "/dev/null", file)
		if diffErr != nil {
			return "", diffErr
		}
		patches = append(patches, patch)
	}
	return strings.TrimSpace(strings.Join(nonEmptyStrings(patches), "\n")), nil
}

func validateExistingWorktree(ctx context.Context, repositoryAbs string, binding Binding) (string, error) {
	branch, err := gitOutput(ctx, binding.Root, "symbolic-ref", "--short", "HEAD")
	if err != nil {
		return "", err
	}
	if branch != binding.BranchName {
		return "", fmt.Errorf("workspace branch is %q, want %q", branch, binding.BranchName)
	}
	commonDir, err := gitOutput(ctx, binding.Root, "rev-parse", "--path-format=absolute", "--git-common-dir")
	if err != nil {
		return "", err
	}
	commonAbs, err := filepath.Abs(commonDir)
	if err != nil {
		return "", err
	}
	if !samePath(repositoryAbs, commonAbs) {
		return "", fmt.Errorf("workspace belongs to a different git repository")
	}
	return gitOutput(ctx, binding.Root, "rev-parse", "HEAD")
}

func ensureMaterializationRoot(root string) error {
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return fmt.Errorf("resolve workspace root: %w", err)
	}
	parent := filepath.Dir(rootAbs)
	if parent == rootAbs {
		return fmt.Errorf("workspace root cannot be a filesystem root")
	}
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return fmt.Errorf("create worktree parent: %w", err)
	}
	info, err := os.Lstat(parent)
	if err != nil {
		return fmt.Errorf("inspect worktree parent: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("worktree parent must not be a symbolic link")
	}
	return nil
}

func ensureWorkspaceDirectories(root string) error {
	for _, dir := range []string{"plan", "workspace", "evidence"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0o755); err != nil {
			return fmt.Errorf("create workspace dir %s: %w", dir, err)
		}
	}
	return nil
}

func validateWorkspaceSymlinks(root string) error {
	return filepath.WalkDir(root, func(current string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink == 0 {
			return nil
		}
		if err := ensureNoSymlinkEscape(root, current); err != nil {
			return fmt.Errorf("workspace contains an escaping symbolic link at %s: %w", filepath.ToSlash(current), err)
		}
		return nil
	})
}

func gitRefExists(ctx context.Context, repositoryPath, ref string) (bool, error) {
	cmd := exec.CommandContext(ctx, "git", "show-ref", "--verify", "--quiet", ref)
	cmd.Dir = repositoryPath
	err := cmd.Run()
	if err == nil {
		return true, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
		return false, nil
	}
	return false, fmt.Errorf("git show-ref --verify: %w", err)
}

func observedGitFiles(ctx context.Context, root, base string) ([]string, error) {
	commands := [][]string{
		{"diff", "--name-only", base, "HEAD"},
		{"diff", "--name-only", "--cached"},
		{"diff", "--name-only"},
		{"ls-files", "--others", "--exclude-standard"},
	}
	seen := map[string]struct{}{}
	for _, args := range commands {
		out, err := gitOutput(ctx, root, args...)
		if err != nil {
			return nil, err
		}
		for _, line := range strings.Split(out, "\n") {
			line = strings.TrimSpace(line)
			if line != "" {
				seen[filepath.ToSlash(line)] = struct{}{}
			}
		}
	}
	files := make([]string, 0, len(seen))
	for file := range seen {
		files = append(files, file)
	}
	sort.Strings(files)
	return files, nil
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
	return gitOutputWithExitCodes(ctx, dir, []int{0}, args...)
}

func gitOutputWithExitCodes(ctx context.Context, dir string, allowed []int, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) || !containsInt(allowed, exitErr.ExitCode()) {
			return "", fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
		}
	}
	return strings.TrimSpace(string(out)), nil
}

func containsInt(values []int, candidate int) bool {
	for _, value := range values {
		if value == candidate {
			return true
		}
	}
	return false
}

func nonEmptyStrings(values []string) []string {
	out := values[:0]
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			out = append(out, value)
		}
	}
	return out
}

func samePath(left, right string) bool {
	left = filepath.Clean(left)
	right = filepath.Clean(right)
	if isWindowsPathCaseInsensitive() {
		return strings.EqualFold(left, right)
	}
	return left == right
}

func isWindowsPathCaseInsensitive() bool {
	return filepath.Separator == '\\'
}

var _ GitBackend = (*LocalGitBackend)(nil)
