package workspace

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

type Kind string

const (
	KindGitWorktree Kind = "git_worktree"
)

type Phase string

const (
	PhasePlan    Phase = "plan"
	PhaseExecute Phase = "execute"
	PhaseVerify  Phase = "verify"
)

type Status string

const (
	StatusPrepared Status = "prepared"
	StatusSealed   Status = "sealed"
)

type WriteSet struct {
	Files     []string `json:"files,omitempty"`
	Modules   []string `json:"modules,omitempty"`
	Symbols   []string `json:"symbols,omitempty"`
	Contracts []string `json:"contracts,omitempty"`
	Tests     []string `json:"tests,omitempty"`
	Owners    []string `json:"owners,omitempty"`
}

type Binding struct {
	ID               kernel.BindingRef             `json:"id"`
	TaskID           kernel.TaskID                 `json:"task_id"`
	Generation       int                           `json:"generation"`
	Kind             Kind                          `json:"kind"`
	Root             string                        `json:"root"`
	BranchName       string                        `json:"branch_name,omitempty"`
	ContainerID      string                        `json:"container_id,omitempty"`
	VolumeRefs       []string                      `json:"volume_refs,omitempty"`
	BaseRevision     string                        `json:"base_revision"`
	CurrentRevision  string                        `json:"current_revision"`
	AllowedDirs      []string                      `json:"allowed_dirs"`
	DeclaredWrites   WriteSet                      `json:"declared_writes"`
	ObservedWrites   WriteSet                      `json:"observed_writes"`
	PhaseLeases      map[Phase]kernel.InvocationID `json:"phase_leases"`
	ActivePhase      Phase                         `json:"active_phase,omitempty"`
	ActiveInvocation kernel.InvocationID           `json:"active_invocation,omitempty"`
	Status           Status                        `json:"status"`
}

type CreateRequest struct {
	TaskID           kernel.TaskID
	Generation       int
	RepoPath         string
	WorktreeParent   string
	BaseRevision     string
	AllowedDirs      []string
	DeclaredWrites   WriteSet
	AfterWorktreeAdd func() error
}

type Service struct {
	mu       sync.Mutex
	bindings map[kernel.BindingRef]Binding
	byRound  map[roundKey]kernel.BindingRef
}

type roundKey struct {
	taskID     kernel.TaskID
	generation int
}

func NewService() *Service {
	return &Service{
		bindings: make(map[kernel.BindingRef]Binding),
		byRound:  make(map[roundKey]kernel.BindingRef),
	}
}

func (s *Service) CreateGitWorktree(ctx context.Context, req CreateRequest) (Binding, error) {
	if err := kernel.RequireID("task_id", req.TaskID); err != nil {
		return Binding{}, err
	}
	if req.Generation <= 0 {
		return Binding{}, kernel.InvalidArgument("generation must be positive")
	}
	if req.RepoPath == "" || req.WorktreeParent == "" {
		return Binding{}, kernel.InvalidArgument("repo_path and worktree_parent are required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	key := roundKey{taskID: req.TaskID, generation: req.Generation}
	if id, ok := s.byRound[key]; ok {
		return cloneBinding(s.bindings[id]), nil
	}

	base := req.BaseRevision
	if base == "" {
		resolved, err := gitOutput(ctx, req.RepoPath, "rev-parse", "HEAD")
		if err != nil {
			return Binding{}, err
		}
		base = resolved
	}
	id := kernel.BindingRef(fmt.Sprintf("ws_%s_%03d", sanitizeID(string(req.TaskID)), req.Generation))
	branch := fmt.Sprintf("threadmill/%s/%03d", sanitizeID(string(req.TaskID)), req.Generation)
	root := filepath.Join(req.WorktreeParent, string(id))
	if err := os.MkdirAll(req.WorktreeParent, 0o755); err != nil {
		return Binding{}, fmt.Errorf("create worktree parent: %w", err)
	}
	worktreeAdded := false
	if err := gitRun(ctx, req.RepoPath, "worktree", "add", "-b", branch, root, base); err != nil {
		return Binding{}, err
	}
	worktreeAdded = true
	rollback := func(cause error) (Binding, error) {
		if worktreeAdded {
			_ = gitRun(context.Background(), req.RepoPath, "worktree", "remove", "--force", root)
			_ = gitRun(context.Background(), req.RepoPath, "branch", "-D", branch)
		}
		return Binding{}, cause
	}
	if req.AfterWorktreeAdd != nil {
		if err := req.AfterWorktreeAdd(); err != nil {
			return rollback(fmt.Errorf("after worktree add: %w", err))
		}
	}
	current, err := gitOutput(ctx, root, "rev-parse", "HEAD")
	if err != nil {
		return rollback(err)
	}
	for _, dir := range []string{"plan", "workspace", "evidence"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0o755); err != nil {
			return rollback(fmt.Errorf("create workspace dir %s: %w", dir, err))
		}
	}
	allowed, err := normalizeAllowedDirs(req.AllowedDirs)
	if err != nil {
		return rollback(err)
	}
	binding := Binding{
		ID:              id,
		TaskID:          req.TaskID,
		Generation:      req.Generation,
		Kind:            KindGitWorktree,
		Root:            root,
		BranchName:      branch,
		BaseRevision:    currentOr(base, current),
		CurrentRevision: current,
		AllowedDirs:     allowed,
		DeclaredWrites:  cloneWriteSet(req.DeclaredWrites),
		PhaseLeases:     make(map[Phase]kernel.InvocationID),
		Status:          StatusPrepared,
	}
	s.bindings[id] = binding
	s.byRound[key] = id
	return cloneBinding(binding), nil
}

func (s *Service) BindPhase(ctx context.Context, id kernel.BindingRef, phase Phase, invocationID kernel.InvocationID) (Binding, error) {
	if err := ctx.Err(); err != nil {
		return Binding{}, err
	}
	if invocationID == "" {
		return Binding{}, kernel.InvalidArgument("invocation_id is required")
	}
	if !validPhase(phase) {
		return Binding{}, kernel.InvalidArgument("invalid phase")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	binding, ok := s.bindings[id]
	if !ok {
		return Binding{}, kernel.Error{Code: kernel.CodeNotFound, Message: "workspace binding not found"}
	}
	if binding.Status == StatusSealed {
		return Binding{}, kernel.Forbidden("workspace binding is sealed")
	}
	if binding.ActiveInvocation != "" && binding.ActiveInvocation != invocationID {
		return Binding{}, kernel.LeaseConflict("another phase holds the workspace write lease")
	}
	if completed := binding.PhaseLeases[phase]; completed != "" && completed != invocationID {
		return Binding{}, kernel.LeaseConflict("phase already completed by another invocation")
	}
	binding.ActivePhase = phase
	binding.ActiveInvocation = invocationID
	s.bindings[id] = binding
	return cloneBinding(binding), nil
}

func (s *Service) ReleasePhase(ctx context.Context, id kernel.BindingRef, phase Phase, invocationID kernel.InvocationID) (Binding, error) {
	return s.finishPhaseLease(ctx, id, phase, invocationID, false)
}

func (s *Service) CompletePhase(ctx context.Context, id kernel.BindingRef, phase Phase, invocationID kernel.InvocationID) (Binding, error) {
	return s.finishPhaseLease(ctx, id, phase, invocationID, true)
}

func (s *Service) finishPhaseLease(ctx context.Context, id kernel.BindingRef, phase Phase, invocationID kernel.InvocationID, complete bool) (Binding, error) {
	if err := ctx.Err(); err != nil {
		return Binding{}, err
	}
	if invocationID == "" {
		return Binding{}, kernel.InvalidArgument("invocation_id is required")
	}
	if !validPhase(phase) {
		return Binding{}, kernel.InvalidArgument("invalid phase")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	binding, ok := s.bindings[id]
	if !ok {
		return Binding{}, kernel.Error{Code: kernel.CodeNotFound, Message: "workspace binding not found"}
	}
	if binding.ActivePhase != phase || binding.ActiveInvocation != invocationID {
		return Binding{}, kernel.LeaseConflict("invocation does not hold active phase lease")
	}
	if complete {
		binding.PhaseLeases[phase] = invocationID
	}
	binding.ActivePhase = ""
	binding.ActiveInvocation = ""
	s.bindings[id] = binding
	return cloneBinding(binding), nil
}

func (s *Service) Get(_ context.Context, id kernel.BindingRef) (Binding, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	binding, ok := s.bindings[id]
	if !ok {
		return Binding{}, kernel.Error{Code: kernel.CodeNotFound, Message: "workspace binding not found"}
	}
	return cloneBinding(binding), nil
}

func (s *Service) Seal(ctx context.Context, id kernel.BindingRef) (Binding, error) {
	if err := ctx.Err(); err != nil {
		return Binding{}, err
	}
	s.mu.Lock()
	binding, ok := s.bindings[id]
	if !ok {
		s.mu.Unlock()
		return Binding{}, kernel.Error{Code: kernel.CodeNotFound, Message: "workspace binding not found"}
	}
	binding.Status = StatusSealed
	s.bindings[id] = binding
	s.mu.Unlock()
	return cloneBinding(binding), nil
}

func (s *Service) RefreshObservedWrites(ctx context.Context, id kernel.BindingRef) (Binding, error) {
	s.mu.Lock()
	binding, ok := s.bindings[id]
	s.mu.Unlock()
	if !ok {
		return Binding{}, kernel.Error{Code: kernel.CodeNotFound, Message: "workspace binding not found"}
	}
	files, err := observedGitFiles(ctx, binding.Root, binding.BaseRevision)
	if err != nil {
		return Binding{}, err
	}
	current, err := gitOutput(ctx, binding.Root, "rev-parse", "HEAD")
	if err != nil {
		return Binding{}, err
	}
	s.mu.Lock()
	currentBinding, ok := s.bindings[id]
	if !ok {
		s.mu.Unlock()
		return Binding{}, kernel.Error{Code: kernel.CodeNotFound, Message: "workspace binding not found"}
	}
	currentBinding.ObservedWrites = WriteSet{Files: files}
	currentBinding.CurrentRevision = current
	s.bindings[id] = currentBinding
	s.mu.Unlock()
	return cloneBinding(currentBinding), nil
}

func ResolveWritePath(binding Binding, phase Phase, relPath string) (string, error) {
	if binding.Status == StatusSealed {
		return "", kernel.Forbidden("workspace binding is sealed")
	}
	if !validPhase(phase) {
		return "", kernel.InvalidArgument("invalid phase")
	}
	clean, err := cleanRelativePath(relPath)
	if err != nil {
		return "", err
	}
	if !phaseAllows(binding, phase, clean) {
		return "", kernel.Forbidden("path is outside phase allowed dirs")
	}
	full := filepath.Join(binding.Root, filepath.FromSlash(clean))
	if err := ensureNoSymlinkEscape(binding.Root, full); err != nil {
		return "", err
	}
	return full, nil
}

func cleanRelativePath(p string) (string, error) {
	if p == "" {
		return "", kernel.InvalidArgument("path is required")
	}
	if filepath.IsAbs(p) || path.IsAbs(strings.ReplaceAll(p, "\\", "/")) {
		return "", kernel.Forbidden("absolute paths are not allowed")
	}
	clean := path.Clean(strings.ReplaceAll(p, "\\", "/"))
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
		return "", kernel.Forbidden("parent path traversal is not allowed")
	}
	return clean, nil
}

func ensureNoSymlinkEscape(root, target string) error {
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return fmt.Errorf("resolve workspace root: %w", err)
	}
	targetAbs, err := filepath.Abs(target)
	if err != nil {
		return fmt.Errorf("resolve workspace target: %w", err)
	}
	rel, err := filepath.Rel(rootAbs, targetAbs)
	if err != nil {
		return fmt.Errorf("compare path to root: %w", err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return kernel.Forbidden("path escapes workspace root")
	}
	rootInfo, err := os.Lstat(rootAbs)
	if err != nil {
		return fmt.Errorf("inspect workspace root: %w", err)
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 {
		return kernel.Forbidden("workspace root must not be a symbolic link")
	}

	current := rootAbs
	for _, component := range strings.Split(rel, string(filepath.Separator)) {
		if component == "" || component == "." {
			continue
		}
		current = filepath.Join(current, component)
		resolved, exists, err := resolveExistingLinksWithinRoot(rootAbs, current)
		if err != nil {
			return err
		}
		if !exists {
			return nil
		}
		current = resolved
	}
	return nil
}

func resolveExistingLinksWithinRoot(rootAbs, candidate string) (string, bool, error) {
	current := candidate
	for depth := 0; depth < 32; depth++ {
		info, err := os.Lstat(current)
		if os.IsNotExist(err) {
			return current, false, nil
		}
		if err != nil {
			return "", false, fmt.Errorf("inspect workspace path: %w", err)
		}
		if info.Mode()&os.ModeSymlink == 0 {
			return current, true, nil
		}
		destination, err := os.Readlink(current)
		if err != nil {
			return "", false, fmt.Errorf("read workspace symbolic link: %w", err)
		}
		if !filepath.IsAbs(destination) {
			destination = filepath.Join(filepath.Dir(current), destination)
		}
		destination, err = filepath.Abs(destination)
		if err != nil {
			return "", false, fmt.Errorf("resolve workspace symbolic link: %w", err)
		}
		rel, err := filepath.Rel(rootAbs, destination)
		if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
			return "", false, kernel.Forbidden("path escapes workspace root through symbolic link")
		}
		current = destination
	}
	return "", false, kernel.Forbidden("workspace symbolic link chain is too deep")
}

func phaseAllows(binding Binding, phase Phase, clean string) bool {
	switch phase {
	case PhasePlan:
		return hasPathPrefix(clean, "plan")
	case PhaseVerify:
		return hasPathPrefix(clean, "evidence")
	case PhaseExecute:
		for _, allowed := range binding.AllowedDirs {
			if hasPathPrefix(clean, allowed) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func hasPathPrefix(p, prefix string) bool {
	p = strings.Trim(path.Clean(p), "/")
	prefix = strings.Trim(path.Clean(prefix), "/")
	return p == prefix || strings.HasPrefix(p, prefix+"/")
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
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return strings.TrimSpace(string(out)), nil
}

func normalizeAllowedDirs(dirs []string) ([]string, error) {
	if len(dirs) == 0 {
		return []string{"workspace"}, nil
	}
	seen := map[string]struct{}{}
	var out []string
	for _, dir := range dirs {
		clean, err := cleanRelativePath(dir)
		if err != nil {
			return nil, fmt.Errorf("invalid allowed dir %q: %w", dir, err)
		}
		clean = strings.TrimSuffix(clean, "/")
		if _, ok := seen[clean]; !ok {
			seen[clean] = struct{}{}
			out = append(out, clean)
		}
	}
	sort.Strings(out)
	return out, nil
}

func validPhase(phase Phase) bool {
	return phase == PhasePlan || phase == PhaseExecute || phase == PhaseVerify
}

func sanitizeID(id string) string {
	id = strings.ToLower(id)
	var b strings.Builder
	for _, r := range id {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			b.WriteRune(r)
		} else {
			b.WriteByte('-')
		}
	}
	return strings.Trim(b.String(), "-")
}

func currentOr(base, current string) string {
	if current != "" {
		return current
	}
	return base
}

func cloneBinding(binding Binding) Binding {
	binding.AllowedDirs = append([]string(nil), binding.AllowedDirs...)
	binding.VolumeRefs = append([]string(nil), binding.VolumeRefs...)
	binding.DeclaredWrites = cloneWriteSet(binding.DeclaredWrites)
	binding.ObservedWrites = cloneWriteSet(binding.ObservedWrites)
	binding.PhaseLeases = cloneLeases(binding.PhaseLeases)
	return binding
}

func cloneWriteSet(set WriteSet) WriteSet {
	return WriteSet{
		Files:     append([]string(nil), set.Files...),
		Modules:   append([]string(nil), set.Modules...),
		Symbols:   append([]string(nil), set.Symbols...),
		Contracts: append([]string(nil), set.Contracts...),
		Tests:     append([]string(nil), set.Tests...),
		Owners:    append([]string(nil), set.Owners...),
	}
}

func cloneLeases(leases map[Phase]kernel.InvocationID) map[Phase]kernel.InvocationID {
	if len(leases) == 0 {
		return map[Phase]kernel.InvocationID{}
	}
	out := make(map[Phase]kernel.InvocationID, len(leases))
	for phase, invocation := range leases {
		out[phase] = invocation
	}
	return out
}
