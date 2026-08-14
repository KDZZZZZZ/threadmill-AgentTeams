package workspace

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

// PathRequest is always relative to the Workspace Binding resolved from the
// trusted InvocationID. It never carries a TaskID, BindingRef, or phase lease.
type PathRequest struct {
	Path string `json:"path,omitempty"`
}

type WriteRequest struct {
	Path    string `json:"path"`
	Content string `json:"content"`
}

// RunRequest is argv-shaped so the Workspace implementation can enforce its
// allow policy without first interpreting a caller-provided shell string.
type RunRequest struct {
	Command []string `json:"command"`
	WorkDir string   `json:"work_dir,omitempty"`
}

type Entry struct {
	Path string `json:"path"`
	Kind string `json:"kind"`
	Size int64  `json:"size,omitempty"`
}

type ListResult struct {
	Entries           []Entry `json:"entries"`
	WorkspaceRevision string  `json:"workspace_revision"`
}

type ReadResult struct {
	Path              string `json:"path"`
	Content           string `json:"content"`
	WorkspaceRevision string `json:"workspace_revision"`
}

type WriteResult struct {
	Path              string `json:"path"`
	WorkspaceRevision string `json:"workspace_revision"`
}

type RunResult struct {
	Command           []string `json:"command"`
	ExitCode          int      `json:"exit_code"`
	Stdout            string   `json:"stdout,omitempty"`
	Stderr            string   `json:"stderr,omitempty"`
	WorkspaceRevision string   `json:"workspace_revision"`
}

type DiffResult struct {
	Patch             string   `json:"patch"`
	ObservedWrites    WriteSet `json:"observed_writes"`
	WorkspaceRevision string   `json:"workspace_revision"`
}

// AgentToolPort is the formal Runtime-to-Workspace seam. Implementations must
// resolve binding, phase, AllowedDirs, Declared Write Set, and lease solely
// from invocationID; MCP payloads cannot expand authority.
type AgentToolPort interface {
	List(context.Context, kernel.InvocationID, PathRequest) (ListResult, error)
	Read(context.Context, kernel.InvocationID, PathRequest) (ReadResult, error)
	WritePlan(context.Context, kernel.InvocationID, WriteRequest) (WriteResult, error)
	Write(context.Context, kernel.InvocationID, WriteRequest) (WriteResult, error)
	Run(context.Context, kernel.InvocationID, RunRequest) (RunResult, error)
	Diff(context.Context, kernel.InvocationID, PathRequest) (DiffResult, error)
}

const (
	maxWorkspaceReadBytes  = 4 << 20
	maxWorkspaceWriteBytes = 4 << 20
	maxCommandOutputBytes  = 4 << 20
	maxCommandCopyBytes    = 512 << 20
	maxCommandApplyBytes   = 32 << 20
)

// AgentTools is the invocation-scoped implementation of AgentToolPort. The
// caller supplies only an opaque InvocationID; task, phase, Binding, root,
// AllowedDirs, and the active lease are always loaded from BindingStore.
type AgentTools struct {
	service         *Service
	allowedCommands map[string]struct{}
}

func NewAgentTools(service *Service, allowedCommands ...string) *AgentTools {
	allowed := make(map[string]struct{}, len(allowedCommands))
	for _, command := range allowedCommands {
		command = normalizeExecutable(command)
		if command != "" {
			allowed[command] = struct{}{}
		}
	}
	return &AgentTools{service: service, allowedCommands: allowed}
}

func (t *AgentTools) List(ctx context.Context, invocationID kernel.InvocationID, req PathRequest) (ListResult, error) {
	var result ListResult
	err := t.withBinding(ctx, invocationID, func(_ context.Context, binding Binding) error {
		target, clean, err := resolveReadPath(binding, req.Path, true)
		if err != nil {
			return err
		}
		entries, err := os.ReadDir(target)
		if err != nil {
			return fmt.Errorf("list workspace path: %w", err)
		}
		for _, item := range entries {
			itemPath := item.Name()
			if clean != "" {
				itemPath = filepath.ToSlash(filepath.Join(clean, item.Name()))
			}
			if protectedWorkspacePath(itemPath) {
				continue
			}
			info, err := item.Info()
			if err != nil {
				return fmt.Errorf("inspect workspace entry: %w", err)
			}
			kind := "file"
			switch {
			case info.Mode()&os.ModeSymlink != 0:
				kind = "symlink"
			case info.IsDir():
				kind = "directory"
			}
			result.Entries = append(result.Entries, Entry{Path: itemPath, Kind: kind, Size: info.Size()})
		}
		result.WorkspaceRevision = binding.CurrentRevision
		return nil
	})
	return result, err
}

func (t *AgentTools) Read(ctx context.Context, invocationID kernel.InvocationID, req PathRequest) (ReadResult, error) {
	var result ReadResult
	err := t.withBinding(ctx, invocationID, func(_ context.Context, binding Binding) error {
		target, clean, err := resolveReadPath(binding, req.Path, false)
		if err != nil {
			return err
		}
		file, err := os.Open(target)
		if err != nil {
			return fmt.Errorf("open workspace file: %w", err)
		}
		defer file.Close()
		info, err := file.Stat()
		if err != nil {
			return fmt.Errorf("inspect workspace file: %w", err)
		}
		if !info.Mode().IsRegular() {
			return kernel.Forbidden("workspace.read only supports regular files")
		}
		if info.Size() > maxWorkspaceReadBytes {
			return kernel.Forbidden("workspace file exceeds read size limit")
		}
		content, err := io.ReadAll(io.LimitReader(file, maxWorkspaceReadBytes+1))
		if err != nil {
			return fmt.Errorf("read workspace file: %w", err)
		}
		if len(content) > maxWorkspaceReadBytes {
			return kernel.Forbidden("workspace file exceeds read size limit")
		}
		result = ReadResult{Path: clean, Content: string(content), WorkspaceRevision: binding.CurrentRevision}
		return nil
	})
	return result, err
}

func (t *AgentTools) WritePlan(ctx context.Context, invocationID kernel.InvocationID, req WriteRequest) (WriteResult, error) {
	return t.write(ctx, invocationID, PhasePlan, req)
}

func (t *AgentTools) Write(ctx context.Context, invocationID kernel.InvocationID, req WriteRequest) (WriteResult, error) {
	return t.write(ctx, invocationID, PhaseExecute, req)
}

func (t *AgentTools) write(ctx context.Context, invocationID kernel.InvocationID, requiredPhase Phase, req WriteRequest) (WriteResult, error) {
	var result WriteResult
	if len(req.Content) > maxWorkspaceWriteBytes {
		return result, kernel.Forbidden("workspace write exceeds size limit")
	}
	err := t.withBinding(ctx, invocationID, func(lockCtx context.Context, binding Binding) error {
		if binding.ActivePhase != requiredPhase {
			return kernel.Forbidden("workspace write tool is not allowed for the active phase")
		}
		target, err := ResolveWritePath(binding, requiredPhase, req.Path)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return fmt.Errorf("create workspace write directory: %w", err)
		}
		if err := ensureNoSymlinkEscape(binding.Root, target); err != nil {
			return err
		}
		temp, err := os.CreateTemp(filepath.Dir(target), ".threadmill-write-*")
		if err != nil {
			return fmt.Errorf("create workspace temporary file: %w", err)
		}
		tempName := temp.Name()
		defer os.Remove(tempName)
		if _, err := temp.WriteString(req.Content); err != nil {
			temp.Close()
			return fmt.Errorf("write workspace temporary file: %w", err)
		}
		if err := temp.Sync(); err != nil {
			temp.Close()
			return fmt.Errorf("sync workspace temporary file: %w", err)
		}
		if err := temp.Close(); err != nil {
			return fmt.Errorf("close workspace temporary file: %w", err)
		}
		if err := ensureNoSymlinkEscape(binding.Root, target); err != nil {
			return err
		}
		if err := os.Rename(tempName, target); err != nil {
			return fmt.Errorf("replace workspace file: %w", err)
		}
		updated, err := t.service.refreshLocked(lockCtx, binding)
		if err != nil {
			return err
		}
		result = WriteResult{Path: filepath.ToSlash(req.Path), WorkspaceRevision: updated.CurrentRevision}
		return nil
	})
	return result, err
}

func (t *AgentTools) Run(ctx context.Context, invocationID kernel.InvocationID, req RunRequest) (RunResult, error) {
	var result RunResult
	if err := validateCommand(t.allowedCommands, req.Command); err != nil {
		return result, err
	}
	err := t.withBinding(ctx, invocationID, func(lockCtx context.Context, binding Binding) error {
		if err := validateWorkspaceSymlinks(binding.Root); err != nil {
			return err
		}
		commandEnv, commandRoot, cleanupEnv, err := newWorkspaceCommandEnvironment()
		if err != nil {
			return err
		}
		defer cleanupEnv()
		_, cleanWorkDir, err := resolveReadPath(binding, req.WorkDir, true)
		if err != nil {
			return err
		}
		copyRoot, copyErr := copyWorkspaceForCommand(binding, commandRoot)
		if copyErr != nil {
			return copyErr
		}
		if normalizeExecutable(req.Command[0]) == "git" {
			if initErr := initializeReadOnlyGitCopy(lockCtx, copyRoot, commandEnv); initErr != nil {
				return initErr
			}
		}
		before, err := snapshotWorkspaceFiles(copyRoot)
		if err != nil {
			return err
		}
		workDir := filepath.Join(copyRoot, filepath.FromSlash(cleanWorkDir))
		cmd := exec.CommandContext(lockCtx, req.Command[0], req.Command[1:]...)
		cmd.Dir = workDir
		cmd.Env = commandEnv
		stdout := &limitedBuffer{limit: maxCommandOutputBytes}
		stderr := &limitedBuffer{limit: maxCommandOutputBytes}
		cmd.Stdout = stdout
		cmd.Stderr = stderr
		err = cmd.Run()
		exitCode := 0
		if err != nil {
			var exitErr *exec.ExitError
			if errors.As(err, &exitErr) {
				exitCode = exitErr.ExitCode()
			} else {
				return fmt.Errorf("run workspace command: %w", err)
			}
		}
		updated := binding
		if binding.ActivePhase == PhaseExecute && exitCode == 0 {
			after, snapshotErr := snapshotWorkspaceFiles(copyRoot)
			if snapshotErr != nil {
				return snapshotErr
			}
			if applyErr := applySandboxChanges(binding, binding.ActivePhase, copyRoot, before, after); applyErr != nil {
				return applyErr
			}
			var refreshErr error
			updated, refreshErr = t.service.refreshLocked(lockCtx, binding)
			if refreshErr != nil {
				return refreshErr
			}
			if err := validateObservedForPhase(updated, binding.ActivePhase); err != nil {
				return err
			}
		}
		result = RunResult{
			Command:           append([]string(nil), req.Command...),
			ExitCode:          exitCode,
			Stdout:            stdout.String(),
			Stderr:            stderr.String(),
			WorkspaceRevision: updated.CurrentRevision,
		}
		return nil
	})
	return result, err
}

func (t *AgentTools) Diff(ctx context.Context, invocationID kernel.InvocationID, req PathRequest) (DiffResult, error) {
	var result DiffResult
	err := t.withBinding(ctx, invocationID, func(lockCtx context.Context, binding Binding) error {
		clean := ""
		if req.Path != "" {
			var err error
			_, clean, err = resolveReadPath(binding, req.Path, false)
			if err != nil && !os.IsNotExist(err) {
				return err
			}
		}
		patch, err := t.service.git.Diff(lockCtx, binding, clean)
		if err != nil {
			return err
		}
		updated, err := t.service.refreshLocked(lockCtx, binding)
		if err != nil {
			return err
		}
		result = DiffResult{Patch: patch, ObservedWrites: updated.ObservedWrites, WorkspaceRevision: updated.CurrentRevision}
		return nil
	})
	return result, err
}

func (t *AgentTools) withBinding(ctx context.Context, invocationID kernel.InvocationID, fn func(context.Context, Binding) error) error {
	if t == nil || t.service == nil {
		return kernel.Error{Code: kernel.CodeInternalError, Message: "workspace agent tools service is required", Recoverable: false}
	}
	return t.service.withInvocation(ctx, invocationID, fn)
}

func (s *Service) refreshLocked(ctx context.Context, binding Binding) (Binding, error) {
	files, err := s.git.ObservedWrites(ctx, binding)
	if err != nil {
		return Binding{}, err
	}
	head, err := s.git.CurrentRevision(ctx, binding)
	if err != nil {
		return Binding{}, err
	}
	next := cloneBinding(binding)
	next.CurrentRevision = head
	next.ObservedWrites = WriteSet{Files: candidateObservedFiles(files)}
	if head == binding.CurrentRevision && writeSetsEqual(next.ObservedWrites, binding.ObservedWrites) {
		return binding, nil
	}
	return s.store.UpdateCAS(ctx, next, binding.Revision)
}

func candidateObservedFiles(files []string) []string {
	result := make([]string, 0, len(files))
	for _, file := range files {
		clean := filepath.ToSlash(filepath.Clean(file))
		if hasPathPrefix(clean, "plan") || hasPathPrefix(clean, "evidence") {
			continue
		}
		result = append(result, clean)
	}
	return normalizedStrings(result)
}

func resolveReadPath(binding Binding, requested string, allowRoot bool) (string, string, error) {
	clean := ""
	if strings.TrimSpace(requested) == "" {
		if !allowRoot {
			return "", "", kernel.InvalidArgument("path is required")
		}
	} else {
		var err error
		clean, err = cleanRelativePath(requested)
		if err != nil {
			return "", "", err
		}
		if protectedWorkspacePath(clean) {
			return "", "", kernel.Forbidden("workspace path is protected")
		}
	}
	target := filepath.Join(binding.Root, filepath.FromSlash(clean))
	if err := ensureNoSymlinkEscape(binding.Root, target); err != nil {
		return "", "", err
	}
	return target, clean, nil
}

func protectedWorkspacePath(relative string) bool {
	for _, component := range strings.Split(strings.ToLower(filepath.ToSlash(relative)), "/") {
		switch strings.TrimSpace(component) {
		case ".git", ".env", "credentials", "sessions", "logs", "tool results", "tool-results", "auth.json", "id_rsa", "id_ed25519":
			return true
		}
	}
	return false
}

func validateCommand(allowed map[string]struct{}, command []string) error {
	if len(command) == 0 || strings.TrimSpace(command[0]) == "" {
		return kernel.InvalidArgument("command argv is required")
	}
	executable := normalizeExecutable(command[0])
	if strings.ContainsAny(command[0], `/\\`) {
		return kernel.Forbidden("workspace command executable must not contain a path")
	}
	switch executable {
	case "cmd", "powershell", "pwsh", "sh", "bash", "zsh", "fish", "wsl", "python", "python3", "node":
		return kernel.Forbidden("shell and interpreter commands are not allowed by workspace.run")
	}
	if _, ok := allowed[executable]; !ok {
		return kernel.Forbidden("workspace command is not in the configured allowlist")
	}
	for _, arg := range command[1:] {
		if strings.ContainsRune(arg, 0) || strings.ContainsAny(arg, "\r\n") {
			return kernel.InvalidArgument("command arguments contain control characters")
		}
		normalized := strings.ReplaceAll(arg, "\\", "/")
		if filepath.IsAbs(arg) || normalized == ".." || strings.HasPrefix(normalized, "../") || strings.Contains(normalized, "/../") {
			return kernel.Forbidden("command arguments may not escape the workspace")
		}
		if arg == "-C" || strings.HasPrefix(arg, "--git-dir") || strings.HasPrefix(arg, "--work-tree") || strings.HasPrefix(arg, "--exec-path") {
			return kernel.Forbidden("command may not redirect repository scope")
		}
	}
	if executable == "git" {
		if len(command) < 2 || strings.HasPrefix(command[1], "-") {
			return kernel.Forbidden("git global options are not allowed")
		}
		switch command[1] {
		case "status", "diff", "rev-parse", "ls-files", "show":
		default:
			return kernel.Forbidden("git subcommand is not allowed in an agent workspace")
		}
	}
	return nil
}

func normalizeExecutable(command string) string {
	command = strings.ToLower(strings.TrimSpace(filepath.Base(command)))
	return strings.TrimSuffix(command, ".exe")
}

func copyWorkspaceForCommand(binding Binding, commandRoot string) (string, error) {
	copyRoot := filepath.Join(commandRoot, "workspace")
	if err := os.MkdirAll(copyRoot, 0o700); err != nil {
		return "", fmt.Errorf("create command workspace copy: %w", err)
	}
	var copiedBytes int64
	err := filepath.WalkDir(binding.Root, func(source string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(binding.Root, source)
		if err != nil {
			return err
		}
		if relative == "." {
			return nil
		}
		relativeSlash := filepath.ToSlash(relative)
		if protectedWorkspacePath(relativeSlash) {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		destination := filepath.Join(copyRoot, relative)
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			if err := ensureNoSymlinkEscape(binding.Root, source); err != nil {
				return err
			}
			resolved, err := filepath.EvalSymlinks(source)
			if err != nil {
				return err
			}
			targetRelative, err := filepath.Rel(binding.Root, resolved)
			if err != nil || targetRelative == ".." || strings.HasPrefix(targetRelative, ".."+string(filepath.Separator)) {
				return kernel.Forbidden("workspace copy rejected escaping symbolic link")
			}
			copyTarget := filepath.Join(copyRoot, targetRelative)
			linkTarget, err := filepath.Rel(filepath.Dir(destination), copyTarget)
			if err != nil {
				return err
			}
			if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
				return err
			}
			return os.Symlink(linkTarget, destination)
		}
		if entry.IsDir() {
			return os.MkdirAll(destination, info.Mode().Perm())
		}
		if !info.Mode().IsRegular() {
			return kernel.Forbidden("workspace copy contains an unsupported special file")
		}
		copiedBytes += info.Size()
		if copiedBytes > maxCommandCopyBytes {
			return kernel.Forbidden("workspace exceeds command copy limit")
		}
		if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
			return err
		}
		return copyRegularFile(source, destination, info.Mode().Perm())
	})
	if err != nil {
		return "", fmt.Errorf("copy workspace for command: %w", err)
	}
	return copyRoot, nil
}

func copyRegularFile(source, destination string, mode os.FileMode) error {
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer input.Close()
	output, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(output, input)
	closeErr := output.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}

func initializeReadOnlyGitCopy(ctx context.Context, root string, env []string) error {
	commands := [][]string{
		{"init", "--quiet"},
		{"add", "--all"},
		{"-c", "user.name=Threadmill", "-c", "user.email=threadmill@invalid", "commit", "--quiet", "--allow-empty", "-m", "read-only phase snapshot"},
	}
	for _, args := range commands {
		if err := gitRunWithEnvironment(ctx, root, env, args...); err != nil {
			return fmt.Errorf("initialize isolated git workspace: %w", err)
		}
	}
	return nil
}

func gitRunWithEnvironment(ctx context.Context, root string, env []string, args ...string) error {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = root
	cmd.Env = env
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return nil
}

func newWorkspaceCommandEnvironment() ([]string, string, func(), error) {
	runRoot, err := os.MkdirTemp("", "threadmill-command-env-*")
	if err != nil {
		return nil, "", nil, fmt.Errorf("create workspace command environment: %w", err)
	}
	cleanup := func() { _ = os.RemoveAll(runRoot) }
	hooks := filepath.Join(runRoot, "empty-hooks")
	if err := os.MkdirAll(hooks, 0o700); err != nil {
		cleanup()
		return nil, "", nil, fmt.Errorf("create empty git hooks directory: %w", err)
	}
	env := []string{
		"PATH=" + os.Getenv("PATH"),
		"GIT_TERMINAL_PROMPT=0",
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_CONFIG_GLOBAL=" + os.DevNull,
		"GIT_CONFIG_COUNT=1",
		"GIT_CONFIG_KEY_0=core.hooksPath",
		"GIT_CONFIG_VALUE_0=" + hooks,
		"TEMP=" + runRoot,
		"TMP=" + runRoot,
		"TMPDIR=" + runRoot,
		"GOCACHE=" + filepath.Join(runRoot, "go-cache"),
		"GOMODCACHE=" + filepath.Join(runRoot, "go-mod-cache"),
		"NO_COLOR=1",
	}
	if runtime.GOOS == "windows" {
		env = appendNonEmptyEnvironment(env,
			"SystemRoot", os.Getenv("SystemRoot"),
			"WINDIR", os.Getenv("WINDIR"),
			"SystemDrive", os.Getenv("SystemDrive"),
			"PATHEXT", os.Getenv("PATHEXT"),
		)
		userProfile := filepath.Join(runRoot, "profile")
		appData := filepath.Join(userProfile, "AppData", "Roaming")
		localAppData := filepath.Join(userProfile, "AppData", "Local")
		if err := os.MkdirAll(appData, 0o700); err != nil {
			cleanup()
			return nil, "", nil, err
		}
		if err := os.MkdirAll(localAppData, 0o700); err != nil {
			cleanup()
			return nil, "", nil, err
		}
		env = append(env, "USERPROFILE="+userProfile, "APPDATA="+appData, "LOCALAPPDATA="+localAppData)
	} else {
		home := filepath.Join(runRoot, "home")
		if err := os.MkdirAll(home, 0o700); err != nil {
			cleanup()
			return nil, "", nil, err
		}
		env = append(env, "HOME="+home)
	}
	return env, runRoot, cleanup, nil
}

type workspaceFileState struct {
	hash [sha256.Size]byte
	mode os.FileMode
	size int64
}

func snapshotWorkspaceFiles(root string) (map[string]workspaceFileState, error) {
	states := make(map[string]workspaceFileState)
	var total int64
	err := filepath.WalkDir(root, func(filePath string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(root, filePath)
		if err != nil {
			return err
		}
		if relative == "." {
			return nil
		}
		clean := filepath.ToSlash(relative)
		if protectedWorkspacePath(clean) {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("inspect command workspace path %q: %w", clean, err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return kernel.Forbidden("command workspace contains a symbolic link")
		}
		if entry.IsDir() {
			return nil
		}
		if !info.Mode().IsRegular() {
			return kernel.Forbidden("command workspace contains an unsupported special file")
		}
		total += info.Size()
		if total > maxCommandCopyBytes {
			return kernel.Forbidden("command workspace snapshot exceeds size limit")
		}
		state, err := hashWorkspaceFile(filePath, info)
		if err != nil {
			return fmt.Errorf("snapshot command workspace file %q: %w", clean, err)
		}
		states[clean] = state
		return nil
	})
	if err != nil {
		return nil, err
	}
	return states, nil
}

func hashWorkspaceFile(filePath string, info os.FileInfo) (workspaceFileState, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return workspaceFileState{}, err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return workspaceFileState{}, err
	}
	var sum [sha256.Size]byte
	copy(sum[:], hash.Sum(nil))
	return workspaceFileState{hash: sum, mode: info.Mode().Perm(), size: info.Size()}, nil
}

type sandboxChange struct {
	path            string
	target          string
	afterExists     bool
	afterContent    []byte
	afterMode       os.FileMode
	originalExists  bool
	originalContent []byte
	originalMode    os.FileMode
}

func applySandboxChanges(binding Binding, phase Phase, copyRoot string, before, after map[string]workspaceFileState) error {
	if !validPhase(phase) {
		return kernel.InvalidArgument("invalid phase")
	}
	paths := make(map[string]struct{}, len(before)+len(after))
	for filePath := range before {
		paths[filePath] = struct{}{}
	}
	for filePath := range after {
		paths[filePath] = struct{}{}
	}
	ordered := make([]string, 0, len(paths))
	for filePath := range paths {
		if beforeState, beforeOK := before[filePath]; beforeOK {
			if afterState, afterOK := after[filePath]; afterOK && beforeState == afterState {
				continue
			}
		}
		ordered = append(ordered, filePath)
	}
	sort.Strings(ordered)

	changes := make([]sandboxChange, 0, len(ordered))
	var applyBytes int64
	for _, filePath := range ordered {
		if protectedWorkspacePath(filePath) || !phaseAllows(binding, phase, filePath) {
			return kernel.Forbidden("workspace command wrote outside the active phase boundary")
		}
		target, err := ResolveWritePath(binding, phase, filePath)
		if err != nil {
			return err
		}
		change := sandboxChange{path: filePath, target: target}
		if afterState, ok := after[filePath]; ok {
			if afterState.size > maxWorkspaceWriteBytes {
				return kernel.Forbidden("workspace command output file exceeds write size limit")
			}
			content, err := os.ReadFile(filepath.Join(copyRoot, filepath.FromSlash(filePath)))
			if err != nil {
				return fmt.Errorf("read command workspace change %q: %w", filePath, err)
			}
			if sha256.Sum256(content) != afterState.hash {
				return commandRevisionConflict(filePath)
			}
			change.afterExists = true
			change.afterContent = content
			change.afterMode = afterState.mode
			applyBytes += int64(len(content))
		}
		if applyBytes > maxCommandApplyBytes {
			return kernel.Forbidden("workspace command changes exceed apply size limit")
		}

		currentState, currentContent, currentExists, err := readWorkspaceFile(target)
		if err != nil {
			return fmt.Errorf("validate authoritative workspace file %q: %w", filePath, err)
		}
		beforeState, beforeExists := before[filePath]
		if currentExists != beforeExists || (beforeExists && currentState != beforeState) {
			return commandRevisionConflict(filePath)
		}
		if len(currentContent) > maxWorkspaceWriteBytes {
			return kernel.Forbidden("workspace command cannot replace an oversized file")
		}
		change.originalExists = currentExists
		change.originalContent = currentContent
		change.originalMode = currentState.mode
		changes = append(changes, change)
	}

	applied := make([]sandboxChange, 0, len(changes))
	for _, change := range changes {
		if err := applySandboxChange(binding.Root, change); err != nil {
			return rollbackSandboxChanges(binding.Root, applied, err)
		}
		applied = append(applied, change)
	}
	return nil
}

func readWorkspaceFile(filePath string) (workspaceFileState, []byte, bool, error) {
	info, err := os.Lstat(filePath)
	if os.IsNotExist(err) {
		return workspaceFileState{}, nil, false, nil
	}
	if err != nil {
		return workspaceFileState{}, nil, false, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return workspaceFileState{}, nil, false, kernel.Forbidden("authoritative workspace target is not a regular file")
	}
	if info.Size() > maxWorkspaceWriteBytes {
		return workspaceFileState{mode: info.Mode().Perm(), size: info.Size()}, nil, true, nil
	}
	content, err := os.ReadFile(filePath)
	if err != nil {
		return workspaceFileState{}, nil, false, err
	}
	return workspaceFileState{
		hash: sha256.Sum256(content),
		mode: info.Mode().Perm(),
		size: info.Size(),
	}, content, true, nil
}

func applySandboxChange(root string, change sandboxChange) error {
	if err := ensureNoSymlinkEscape(root, change.target); err != nil {
		return err
	}
	if !change.afterExists {
		if err := os.Remove(change.target); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove workspace file %q: %w", change.path, err)
		}
		return nil
	}
	return atomicWorkspaceWrite(root, change.target, change.afterContent, change.afterMode)
}

func rollbackSandboxChanges(root string, applied []sandboxChange, cause error) error {
	var rollbackErr error
	for index := len(applied) - 1; index >= 0; index-- {
		change := applied[index]
		if change.originalExists {
			rollbackErr = errors.Join(rollbackErr, atomicWorkspaceWrite(root, change.target, change.originalContent, change.originalMode))
			continue
		}
		if err := os.Remove(change.target); err != nil && !os.IsNotExist(err) {
			rollbackErr = errors.Join(rollbackErr, err)
		}
	}
	if rollbackErr != nil {
		return errors.Join(cause, fmt.Errorf("rollback workspace command changes: %w", rollbackErr))
	}
	return cause
}

func atomicWorkspaceWrite(root, target string, content []byte, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return fmt.Errorf("create workspace command output directory: %w", err)
	}
	if err := ensureNoSymlinkEscape(root, target); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(target), ".threadmill-command-*")
	if err != nil {
		return fmt.Errorf("create workspace command output: %w", err)
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if _, err := temporary.Write(content); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Chmod(temporaryName, mode.Perm()); err != nil {
		return err
	}
	if err := ensureNoSymlinkEscape(root, target); err != nil {
		return err
	}
	if err := os.Rename(temporaryName, target); err != nil {
		return fmt.Errorf("replace workspace command output: %w", err)
	}
	return nil
}

func commandRevisionConflict(filePath string) error {
	return kernel.Error{
		Code:        kernel.CodeRevisionConflict,
		Message:     "workspace changed while command was running",
		Recoverable: true,
		Details:     map[string]string{"path": filePath},
	}
}

func appendNonEmptyEnvironment(env []string, pairs ...string) []string {
	for i := 0; i+1 < len(pairs); i += 2 {
		if pairs[i+1] != "" {
			env = append(env, pairs[i]+"="+pairs[i+1])
		}
	}
	return env
}

func validateObservedForPhase(binding Binding, phase Phase) error {
	for _, file := range binding.ObservedWrites.Files {
		if !phaseAllows(binding, phase, file) {
			return kernel.Forbidden("workspace command wrote outside the active phase boundary")
		}
	}
	return nil
}

type limitedBuffer struct {
	buffer bytes.Buffer
	limit  int
}

func (b *limitedBuffer) Write(data []byte) (int, error) {
	original := len(data)
	remaining := b.limit - b.buffer.Len()
	if remaining > 0 {
		if len(data) > remaining {
			data = data[:remaining]
		}
		_, _ = b.buffer.Write(data)
	}
	return original, nil
}

func (b *limitedBuffer) String() string { return b.buffer.String() }

var _ AgentToolPort = (*AgentTools)(nil)
