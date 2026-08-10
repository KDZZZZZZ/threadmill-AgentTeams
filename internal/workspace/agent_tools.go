package workspace

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
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
		workDir, cleanWorkDir, err := resolveReadPath(binding, req.WorkDir, true)
		if err != nil {
			return err
		}
		readOnlyPhase := binding.ActivePhase != PhaseExecute
		cleanup := func() {}
		if readOnlyPhase {
			if normalizeExecutable(req.Command[0]) == "git" {
				if !readOnlyGitSubcommand(req.Command) {
					return kernel.Forbidden("read-only phase may only run non-mutating git commands")
				}
			} else {
				copyRoot, copyErr := copyWorkspaceForReadOnlyRun(binding)
				if copyErr != nil {
					return copyErr
				}
				cleanup = func() { _ = os.RemoveAll(copyRoot) }
				workDir = filepath.Join(copyRoot, filepath.FromSlash(cleanWorkDir))
			}
		}
		defer cleanup()
		cmd := exec.CommandContext(lockCtx, req.Command[0], req.Command[1:]...)
		cmd.Dir = workDir
		cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0", "GIT_CONFIG_NOSYSTEM=1")
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
		if binding.ActivePhase == PhaseExecute {
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
	next.ObservedWrites = WriteSet{Files: files}
	if head == binding.CurrentRevision && writeSetsEqual(next.ObservedWrites, binding.ObservedWrites) {
		return binding, nil
	}
	return s.store.UpdateCAS(ctx, next, binding.Revision)
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
		case "status", "diff", "add", "commit", "rev-parse", "ls-files", "show":
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

func readOnlyGitSubcommand(command []string) bool {
	if len(command) < 2 {
		return false
	}
	switch command[1] {
	case "status", "diff", "rev-parse", "ls-files", "show":
		return true
	default:
		return false
	}
}

func copyWorkspaceForReadOnlyRun(binding Binding) (string, error) {
	copyRoot, err := os.MkdirTemp("", "threadmill-workspace-run-*")
	if err != nil {
		return "", fmt.Errorf("create read-only phase workspace copy: %w", err)
	}
	ok := false
	defer func() {
		if !ok {
			_ = os.RemoveAll(copyRoot)
		}
	}()
	const maxCopiedBytes int64 = 512 << 20
	var copiedBytes int64
	err = filepath.WalkDir(binding.Root, func(source string, entry os.DirEntry, walkErr error) error {
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
		if copiedBytes > maxCopiedBytes {
			return kernel.Forbidden("workspace exceeds read-only command copy limit")
		}
		if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
			return err
		}
		return copyRegularFile(source, destination, info.Mode().Perm())
	})
	if err != nil {
		return "", fmt.Errorf("copy workspace for read-only phase command: %w", err)
	}
	ok = true
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
