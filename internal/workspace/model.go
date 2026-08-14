package workspace

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

type Kind string

const (
	KindGitWorktree Kind = "git_worktree"
	KindClone       Kind = "clone"
	KindContainer   Kind = "container"
	KindRemote      Kind = "remote"
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
	ID                  kernel.BindingRef             `json:"id"`
	Revision            kernel.Revision               `json:"binding_revision"`
	TaskID              kernel.TaskID                 `json:"task_id"`
	Generation          int                           `json:"generation"`
	Kind                Kind                          `json:"kind"`
	Root                string                        `json:"root"`
	BranchName          string                        `json:"branch_name,omitempty"`
	ContainerID         string                        `json:"container_id,omitempty"`
	VolumeRefs          []string                      `json:"volume_refs,omitempty"`
	BaseRevision        string                        `json:"base_revision"`
	CurrentRevision     string                        `json:"current_revision"`
	AllowedDirs         []string                      `json:"allowed_dirs"`
	DeclaredWrites      WriteSet                      `json:"declared_writes"`
	ObservedWrites      WriteSet                      `json:"observed_writes"`
	PhaseLeases         map[Phase]kernel.InvocationID `json:"phase_leases"`
	ActivePhase         Phase                         `json:"active_phase,omitempty"`
	ActiveInvocation    kernel.InvocationID           `json:"active_invocation,omitempty"`
	Status              Status                        `json:"status"`
	RepositoryPath      string                        `json:"-"`
	CreationFingerprint string                        `json:"-"`
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
	store BindingStore
	git   GitBackend
}

type roundKey struct {
	taskID     kernel.TaskID
	generation int
}

func NewService() *Service {
	return NewServiceWithStore(NewMemoryStore(), NewLocalGitBackend())
}

func NewServiceWithStore(store BindingStore, gitBackend GitBackend) *Service {
	return &Service{store: store, git: gitBackend}
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
	if err := s.ready(); err != nil {
		return Binding{}, err
	}
	repositoryPath, err := canonicalExistingPath(req.RepoPath)
	if err != nil {
		return Binding{}, fmt.Errorf("resolve repository path: %w", err)
	}
	worktreeParent, err := filepath.Abs(req.WorktreeParent)
	if err != nil {
		return Binding{}, fmt.Errorf("resolve worktree parent: %w", err)
	}
	allowed, err := normalizeAllowedDirs(req.AllowedDirs)
	if err != nil {
		return Binding{}, err
	}
	declared, err := normalizeWriteSet(req.DeclaredWrites, allowed)
	if err != nil {
		return Binding{}, err
	}
	var result Binding
	lockKey := roundLockKey(req.TaskID, req.Generation)
	err = s.store.WithLock(ctx, lockKey, func(lockCtx context.Context) error {
		existing, ok, getErr := s.store.GetByRound(lockCtx, req.TaskID, req.Generation)
		if getErr != nil {
			return getErr
		}
		if ok {
			base := existing.BaseRevision
			if req.BaseRevision != "" {
				base, getErr = s.git.ResolveRevision(lockCtx, repositoryPath, req.BaseRevision)
				if getErr != nil {
					return getErr
				}
			}
			fingerprint := createFingerprint(repositoryPath, worktreeParent, base, allowed, declared)
			if existing.CreationFingerprint != fingerprint {
				return kernel.IdempotencyConflict()
			}
			materialized, materializeErr := s.git.Materialize(lockCtx, existing)
			if materializeErr != nil {
				return materializeErr
			}
			if materialized.Head != existing.CurrentRevision {
				next := cloneBinding(existing)
				next.CurrentRevision = materialized.Head
				existing, materializeErr = s.store.UpdateCAS(lockCtx, next, existing.Revision)
				if materializeErr != nil {
					return materializeErr
				}
			}
			result = existing
			return nil
		}

		base, resolveErr := s.git.ResolveRevision(lockCtx, repositoryPath, req.BaseRevision)
		if resolveErr != nil {
			return resolveErr
		}
		id, branch := bindingIdentity(req.TaskID, req.Generation)
		binding := Binding{
			ID:                  id,
			Revision:            1,
			TaskID:              req.TaskID,
			Generation:          req.Generation,
			Kind:                KindGitWorktree,
			Root:                filepath.Join(worktreeParent, string(id)),
			BranchName:          branch,
			BaseRevision:        base,
			CurrentRevision:     base,
			AllowedDirs:         allowed,
			DeclaredWrites:      declared,
			PhaseLeases:         make(map[Phase]kernel.InvocationID),
			Status:              StatusPrepared,
			RepositoryPath:      repositoryPath,
			CreationFingerprint: createFingerprint(repositoryPath, worktreeParent, base, allowed, declared),
		}
		materialized, materializeErr := s.git.Materialize(lockCtx, binding)
		if materializeErr != nil {
			return materializeErr
		}
		binding.CurrentRevision = materialized.Head
		rollback := func(cause error) error {
			if rollbackErr := s.git.Remove(context.Background(), binding, materialized); rollbackErr != nil {
				return errors.Join(cause, fmt.Errorf("rollback workspace materialization: %w", rollbackErr))
			}
			return cause
		}
		if req.AfterWorktreeAdd != nil {
			if hookErr := req.AfterWorktreeAdd(); hookErr != nil {
				return rollback(fmt.Errorf("after worktree add: %w", hookErr))
			}
		}
		if insertErr := s.store.Insert(lockCtx, binding); insertErr != nil {
			return rollback(insertErr)
		}
		result = binding
		return nil
	})
	if err != nil {
		return Binding{}, err
	}
	return cloneBinding(result), nil
}

func (s *Service) BindPhase(ctx context.Context, id kernel.BindingRef, phase Phase, invocationID kernel.InvocationID, expectedRevision ...kernel.Revision) (Binding, error) {
	if invocationID == "" {
		return Binding{}, kernel.InvalidArgument("invocation_id is required")
	}
	if !validPhase(phase) {
		return Binding{}, kernel.InvalidArgument("invalid phase")
	}
	return s.mutate(ctx, id, firstRevision(expectedRevision), func(binding *Binding) (bool, error) {
		if binding.Status == StatusSealed {
			return false, kernel.Forbidden("workspace binding is sealed")
		}
		if binding.ActivePhase == phase && binding.ActiveInvocation == invocationID {
			return false, nil
		}
		if completed := binding.PhaseLeases[phase]; completed != "" {
			if completed == invocationID {
				return false, nil
			}
			return false, kernel.LeaseConflict("phase already completed by another invocation")
		}
		if binding.ActiveInvocation != "" {
			return false, kernel.LeaseConflict("another phase holds the workspace write lease")
		}
		if !phasePrerequisitesMet(*binding, phase) {
			return false, kernel.TransitionRejected("workspace phase prerequisites are not complete")
		}
		binding.ActivePhase = phase
		binding.ActiveInvocation = invocationID
		return true, nil
	})
}

// AuthorizeExecuteWrites promotes the Planner's persisted declared write set
// into the next execute lease. Runtime is the only caller: the Planner writes
// plan/declared-writes.json but cannot mutate Binding authority directly.
func (s *Service) AuthorizeExecuteWrites(ctx context.Context, id kernel.BindingRef, declared WriteSet, expectedRevision ...kernel.Revision) (Binding, error) {
	allowed, err := allowedDirsForDeclaredWrites(declared.Files)
	if err != nil {
		return Binding{}, err
	}
	normalized, err := normalizeWriteSet(declared, allowed)
	if err != nil {
		return Binding{}, err
	}
	return s.mutate(ctx, id, firstRevision(expectedRevision), func(binding *Binding) (bool, error) {
		if binding.Status == StatusSealed {
			return false, kernel.Forbidden("workspace binding is sealed")
		}
		if binding.ActiveInvocation != "" {
			return false, kernel.LeaseConflict("cannot change execute write authority while a phase lease is active")
		}
		if binding.PhaseLeases[PhasePlan] == "" {
			return false, kernel.TransitionRejected("planner phase must complete before execute write authority is promoted")
		}
		if binding.PhaseLeases[PhaseExecute] != "" {
			if stringSlicesEqual(binding.AllowedDirs, allowed) && writeSetsEqual(binding.DeclaredWrites, normalized) {
				return false, nil
			}
			return false, kernel.LeaseConflict("execute write authority cannot change after execute completion")
		}
		if stringSlicesEqual(binding.AllowedDirs, allowed) && writeSetsEqual(binding.DeclaredWrites, normalized) {
			return false, nil
		}
		binding.AllowedDirs = allowed
		binding.DeclaredWrites = normalized
		return true, nil
	})
}

func allowedDirsForDeclaredWrites(files []string) ([]string, error) {
	if len(files) == 0 {
		return nil, kernel.InvalidArgument("declared write set requires at least one file")
	}
	dirs := make([]string, 0, len(files))
	for _, file := range files {
		clean, err := cleanRelativePath(file)
		if err != nil {
			return nil, fmt.Errorf("invalid declared write %q: %w", file, err)
		}
		if protectedWorkspacePath(clean) || hasPathPrefix(clean, "plan") || hasPathPrefix(clean, "evidence") {
			return nil, kernel.Forbidden("declared execute write targets a protected phase path")
		}
		dirs = append(dirs, clean)
	}
	return normalizedStrings(dirs), nil
}

func (s *Service) ReleasePhase(ctx context.Context, id kernel.BindingRef, phase Phase, invocationID kernel.InvocationID, expectedRevision ...kernel.Revision) (Binding, error) {
	return s.finishPhaseLease(ctx, id, phase, invocationID, firstRevision(expectedRevision), false)
}

func (s *Service) CompletePhase(ctx context.Context, id kernel.BindingRef, phase Phase, invocationID kernel.InvocationID, expectedRevision ...kernel.Revision) (Binding, error) {
	return s.finishPhaseLease(ctx, id, phase, invocationID, firstRevision(expectedRevision), true)
}

func (s *Service) finishPhaseLease(ctx context.Context, id kernel.BindingRef, phase Phase, invocationID kernel.InvocationID, expected kernel.Revision, complete bool) (Binding, error) {
	if invocationID == "" {
		return Binding{}, kernel.InvalidArgument("invocation_id is required")
	}
	if !validPhase(phase) {
		return Binding{}, kernel.InvalidArgument("invalid phase")
	}
	return s.mutate(ctx, id, expected, func(binding *Binding) (bool, error) {
		if complete && binding.ActiveInvocation == "" && binding.PhaseLeases[phase] == invocationID {
			return false, nil
		}
		if binding.ActivePhase != phase || binding.ActiveInvocation != invocationID {
			return false, kernel.LeaseConflict("invocation does not hold active phase lease")
		}
		if complete {
			binding.PhaseLeases[phase] = invocationID
		}
		binding.ActivePhase = ""
		binding.ActiveInvocation = ""
		return true, nil
	})
}

func (s *Service) Get(ctx context.Context, id kernel.BindingRef) (Binding, error) {
	if err := s.ready(); err != nil {
		return Binding{}, err
	}
	return s.store.Get(ctx, id)
}

func (s *Service) GetByRound(ctx context.Context, taskID kernel.TaskID, generation int) (Binding, bool, error) {
	if err := s.ready(); err != nil {
		return Binding{}, false, err
	}
	if err := kernel.RequireID("task_id", taskID); err != nil {
		return Binding{}, false, err
	}
	if generation <= 0 {
		return Binding{}, false, kernel.InvalidArgument("generation must be positive")
	}
	binding, ok, err := s.store.GetByRound(ctx, taskID, generation)
	return cloneBinding(binding), ok, err
}

func (s *Service) GetLatestByTask(ctx context.Context, taskID kernel.TaskID) (Binding, bool, error) {
	if err := s.ready(); err != nil {
		return Binding{}, false, err
	}
	if err := kernel.RequireID("task_id", taskID); err != nil {
		return Binding{}, false, err
	}
	binding, ok, err := s.store.GetLatestByTask(ctx, taskID)
	return cloneBinding(binding), ok, err
}

// InheritPhasePrerequisites projects already-completed earlier phases into a
// fresh generation workspace. It never carries an active lease or marks the
// target phase complete. The source and target must be adjacent generations of
// the same Task and the target must start at the source's current Git revision.
func (s *Service) InheritPhasePrerequisites(ctx context.Context, targetID, sourceID kernel.BindingRef, phase Phase, activeInvocation ...kernel.InvocationID) (Binding, error) {
	if !validPhase(phase) {
		return Binding{}, kernel.InvalidArgument("invalid phase")
	}
	if len(activeInvocation) > 1 {
		return Binding{}, kernel.InvalidArgument("at most one active invocation may be supplied")
	}
	var replayInvocation kernel.InvocationID
	if len(activeInvocation) == 1 {
		replayInvocation = activeInvocation[0]
		if replayInvocation == "" {
			return Binding{}, kernel.InvalidArgument("active invocation cannot be empty")
		}
	}
	if targetID == "" || sourceID == "" || targetID == sourceID {
		return Binding{}, kernel.InvalidArgument("distinct source and target workspace bindings are required")
	}
	source, err := s.store.Get(ctx, sourceID)
	if err != nil {
		return Binding{}, err
	}
	if source.ActiveInvocation != "" {
		return Binding{}, kernel.LeaseConflict("previous workspace generation still has an active phase")
	}
	return s.mutate(ctx, targetID, kernel.LatestRevision, func(target *Binding) (bool, error) {
		if target.TaskID != source.TaskID || target.Generation != source.Generation+1 {
			return false, kernel.StaleBinding("workspace prerequisite source is not the previous Task generation")
		}
		if target.BaseRevision != source.CurrentRevision {
			return false, kernel.StaleBinding("new workspace is not based on the previous generation revision")
		}
		if target.Status == StatusSealed {
			return false, kernel.LeaseConflict("new workspace is not available for prerequisite inheritance")
		}
		replayingActive := target.ActiveInvocation != ""
		if replayingActive && (target.ActiveInvocation != replayInvocation || target.ActivePhase != phase) {
			return false, kernel.LeaseConflict("new workspace is not available for prerequisite inheritance")
		}
		changed := false
		for _, prerequisite := range phasePrerequisites(phase) {
			completed := source.PhaseLeases[prerequisite]
			if completed == "" {
				return false, kernel.TransitionRejected("previous workspace phase prerequisites are not complete")
			}
			if existing := target.PhaseLeases[prerequisite]; existing != "" {
				if existing != completed {
					return false, kernel.LeaseConflict("workspace prerequisite was inherited from a different invocation")
				}
				continue
			}
			if replayingActive {
				return false, kernel.LeaseConflict("active workspace is missing an inherited phase prerequisite")
			}
			target.PhaseLeases[prerequisite] = completed
			changed = true
		}
		return changed, nil
	})
}

// InheritPlanPrerequisite is the narrow fresh-round seam used when Merge
// Queue targeted verification has selected a newer main revision as the next
// execution baseline. Unlike InheritPhasePrerequisites, the target is not
// required to share the source's Git revision: it inherits only the audited
// fact that plan completed. Execute and verify completion never cross rounds.
func (s *Service) InheritPlanPrerequisite(ctx context.Context, targetID, sourceID kernel.BindingRef) (Binding, error) {
	if targetID == "" || sourceID == "" || targetID == sourceID {
		return Binding{}, kernel.InvalidArgument("distinct source and target workspace bindings are required")
	}
	source, err := s.store.Get(ctx, sourceID)
	if err != nil {
		return Binding{}, err
	}
	if source.ActiveInvocation != "" {
		return Binding{}, kernel.LeaseConflict("previous workspace generation still has an active phase")
	}
	planInvocation := source.PhaseLeases[PhasePlan]
	if planInvocation == "" {
		return Binding{}, kernel.TransitionRejected("previous workspace plan prerequisite is not complete")
	}
	return s.mutate(ctx, targetID, kernel.LatestRevision, func(target *Binding) (bool, error) {
		if target.TaskID != source.TaskID || target.Generation != source.Generation+1 {
			return false, kernel.StaleBinding("workspace prerequisite source is not the previous Task generation")
		}
		if target.Status == StatusSealed || target.ActiveInvocation != "" {
			return false, kernel.LeaseConflict("new workspace is not available for prerequisite inheritance")
		}
		if target.PhaseLeases[PhaseExecute] != "" || target.PhaseLeases[PhaseVerify] != "" {
			return false, kernel.LeaseConflict("new workspace already contains later phase completion")
		}
		changed := false
		if existing := target.PhaseLeases[PhasePlan]; existing != "" {
			if existing != planInvocation {
				return false, kernel.LeaseConflict("workspace plan prerequisite was inherited from a different invocation")
			}
		} else {
			target.PhaseLeases[PhasePlan] = planInvocation
			changed = true
		}
		// Planner output is immutable authority for the execute write boundary.
		// Carry that validated scope into the fresh latest-main round without
		// carrying any observed writes or later-phase completion.
		if !writeSetsEqual(target.DeclaredWrites, source.DeclaredWrites) || !stringSlicesEqual(target.AllowedDirs, source.AllowedDirs) {
			target.DeclaredWrites = cloneWriteSet(source.DeclaredWrites)
			target.AllowedDirs = append([]string(nil), source.AllowedDirs...)
			changed = true
		}
		return changed, nil
	})
}

// InheritMergedVerifyPrerequisites prepares a read-only verification round on
// the exact revision already written to the managed target branch. Unlike a
// retry round, the target intentionally does not share the execute candidate's
// commit: Merge Queue supplies the merged revision as its base. Only the
// audited plan/execute completion facts cross the boundary; observed writes,
// active leases, and verify completion never do.
func (s *Service) InheritMergedVerifyPrerequisites(ctx context.Context, targetID, sourceID kernel.BindingRef) (Binding, error) {
	if targetID == "" || sourceID == "" || targetID == sourceID {
		return Binding{}, kernel.InvalidArgument("distinct source and target workspace bindings are required")
	}
	source, err := s.store.Get(ctx, sourceID)
	if err != nil {
		return Binding{}, err
	}
	if source.ActiveInvocation != "" {
		return Binding{}, kernel.LeaseConflict("execute workspace still has an active phase")
	}
	planInvocation := source.PhaseLeases[PhasePlan]
	executeInvocation := source.PhaseLeases[PhaseExecute]
	if planInvocation == "" || executeInvocation == "" {
		return Binding{}, kernel.TransitionRejected("merged verification requires completed plan and execute prerequisites")
	}
	return s.mutate(ctx, targetID, kernel.LatestRevision, func(target *Binding) (bool, error) {
		if target.TaskID != source.TaskID || target.Generation != source.Generation+1 {
			return false, kernel.StaleBinding("merged verification workspace is not the next Task generation")
		}
		if target.Status == StatusSealed || target.ActiveInvocation != "" {
			return false, kernel.LeaseConflict("merged verification workspace is not available")
		}
		if target.PhaseLeases[PhaseVerify] != "" {
			return false, kernel.LeaseConflict("merged verification workspace is already complete")
		}
		changed := false
		for phase, invocationID := range map[Phase]kernel.InvocationID{
			PhasePlan: planInvocation, PhaseExecute: executeInvocation,
		} {
			if existing := target.PhaseLeases[phase]; existing != "" {
				if existing != invocationID {
					return false, kernel.LeaseConflict("merged verification prerequisite came from another execution lineage")
				}
				continue
			}
			target.PhaseLeases[phase] = invocationID
			changed = true
		}
		return changed, nil
	})
}

func (s *Service) Seal(ctx context.Context, id kernel.BindingRef, expectedRevision ...kernel.Revision) (Binding, error) {
	return s.mutate(ctx, id, firstRevision(expectedRevision), func(binding *Binding) (bool, error) {
		if binding.Status == StatusSealed {
			return false, nil
		}
		if binding.ActiveInvocation != "" {
			return false, kernel.LeaseConflict("cannot seal a workspace with an active phase lease")
		}
		binding.Status = StatusSealed
		return true, nil
	})
}

func (s *Service) RefreshObservedWrites(ctx context.Context, id kernel.BindingRef, expectedRevision ...kernel.Revision) (Binding, error) {
	return s.mutate(ctx, id, firstRevision(expectedRevision), func(binding *Binding) (bool, error) {
		files, err := s.git.ObservedWrites(ctx, *binding)
		if err != nil {
			return false, err
		}
		current, err := s.git.CurrentRevision(ctx, *binding)
		if err != nil {
			return false, err
		}
		nextWrites := WriteSet{Files: candidateObservedFiles(files)}
		if writeSetsEqual(binding.ObservedWrites, nextWrites) && binding.CurrentRevision == current {
			return false, nil
		}
		binding.ObservedWrites = nextWrites
		binding.CurrentRevision = current
		return true, nil
	})
}

func (s *Service) Materialize(ctx context.Context, id kernel.BindingRef) (Binding, error) {
	if err := s.ready(); err != nil {
		return Binding{}, err
	}
	var result Binding
	err := s.store.WithLock(ctx, bindingLockKey(id), func(lockCtx context.Context) error {
		binding, err := s.store.Get(lockCtx, id)
		if err != nil {
			return err
		}
		materialized, err := s.git.Materialize(lockCtx, binding)
		if err != nil {
			return err
		}
		if materialized.Head != binding.CurrentRevision {
			next := cloneBinding(binding)
			next.CurrentRevision = materialized.Head
			binding, err = s.store.UpdateCAS(lockCtx, next, binding.Revision)
			if err != nil {
				return err
			}
		}
		result = binding
		return nil
	})
	return result, err
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

func phasePrerequisitesMet(binding Binding, phase Phase) bool {
	switch phase {
	case PhasePlan:
		return true
	case PhaseExecute:
		return binding.PhaseLeases[PhasePlan] != ""
	case PhaseVerify:
		return binding.PhaseLeases[PhaseExecute] != ""
	default:
		return false
	}
}

func phasePrerequisites(phase Phase) []Phase {
	switch phase {
	case PhasePlan:
		return nil
	case PhaseExecute:
		return []Phase{PhasePlan}
	case PhaseVerify:
		return []Phase{PhasePlan, PhaseExecute}
	default:
		return nil
	}
}

func (s *Service) mutate(
	ctx context.Context,
	id kernel.BindingRef,
	expected kernel.Revision,
	apply func(*Binding) (bool, error),
) (Binding, error) {
	if err := s.ready(); err != nil {
		return Binding{}, err
	}
	if id == "" {
		return Binding{}, kernel.InvalidArgument("binding_ref is required")
	}
	var result Binding
	err := s.store.WithLock(ctx, bindingLockKey(id), func(lockCtx context.Context) error {
		binding, err := s.store.Get(lockCtx, id)
		if err != nil {
			return err
		}
		next := cloneBinding(binding)
		changed, err := apply(&next)
		if err != nil {
			return err
		}
		if !changed {
			result = binding
			return nil
		}
		if expected == kernel.LatestRevision {
			expected = binding.Revision
		}
		if err := kernel.CheckExpectedRevision(expected, binding.Revision); err != nil {
			return err
		}
		result, err = s.store.UpdateCAS(lockCtx, next, expected)
		return err
	})
	if err != nil {
		return Binding{}, err
	}
	return cloneBinding(result), nil
}

func firstRevision(values []kernel.Revision) kernel.Revision {
	if len(values) == 0 {
		return kernel.LatestRevision
	}
	return values[0]
}

func (s *Service) withInvocation(
	ctx context.Context,
	invocationID kernel.InvocationID,
	fn func(context.Context, Binding) error,
) error {
	if err := s.ready(); err != nil {
		return err
	}
	if invocationID == "" {
		return kernel.InvalidArgument("invocation_id is required")
	}
	binding, ok, err := s.store.GetByInvocation(ctx, invocationID)
	if err != nil {
		return err
	}
	if !ok {
		return kernel.StaleBinding("invocation has no active workspace phase lease")
	}
	return s.store.WithLock(ctx, bindingLockKey(binding.ID), func(lockCtx context.Context) error {
		current, active, err := s.store.GetByInvocation(lockCtx, invocationID)
		if err != nil {
			return err
		}
		if !active || current.ID != binding.ID || current.Status == StatusSealed {
			return kernel.StaleBinding("invocation workspace phase lease is no longer active")
		}
		return fn(lockCtx, current)
	})
}

func (s *Service) ready() error {
	if s == nil || s.store == nil || s.git == nil {
		return kernel.Error{Code: kernel.CodeInternalError, Message: "workspace service store and git backend are required", Recoverable: false}
	}
	return nil
}

func roundLockKey(taskID kernel.TaskID, generation int) string {
	return fmt.Sprintf("workspace:round:%s:%d", taskID, generation)
}

func bindingLockKey(id kernel.BindingRef) string {
	return "workspace:binding:" + string(id)
}

func bindingIdentity(taskID kernel.TaskID, generation int) (kernel.BindingRef, string) {
	sum := sha256.Sum256([]byte(taskID))
	hash := hex.EncodeToString(sum[:6])
	prefix := sanitizeID(string(taskID))
	if len(prefix) > 36 {
		prefix = prefix[:36]
	}
	id := kernel.BindingRef(fmt.Sprintf("ws_%s_%s_%03d", prefix, hash, generation))
	branch := fmt.Sprintf("threadmill/%s-%s/%03d", prefix, hash, generation)
	return id, branch
}

func createFingerprint(repositoryPath, worktreeParent, base string, allowed []string, declared WriteSet) string {
	payload, _ := json.Marshal(struct {
		RepositoryPath string   `json:"repository_path"`
		WorktreeParent string   `json:"worktree_parent"`
		Base           string   `json:"base"`
		Allowed        []string `json:"allowed"`
		Declared       WriteSet `json:"declared"`
	}{repositoryPath, filepath.Clean(worktreeParent), base, allowed, declared})
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func normalizeWriteSet(set WriteSet, allowed []string) (WriteSet, error) {
	files := make([]string, 0, len(set.Files))
	for _, file := range set.Files {
		clean, err := cleanRelativePath(file)
		if err != nil {
			return WriteSet{}, fmt.Errorf("invalid declared write %q: %w", file, err)
		}
		permitted := false
		for _, dir := range allowed {
			if hasPathPrefix(clean, dir) {
				permitted = true
				break
			}
		}
		if !permitted {
			return WriteSet{}, kernel.Forbidden("declared write is outside allowed dirs")
		}
		files = append(files, clean)
	}
	return WriteSet{
		Files:     normalizedStrings(files),
		Modules:   normalizedStrings(set.Modules),
		Symbols:   normalizedStrings(set.Symbols),
		Contracts: normalizedStrings(set.Contracts),
		Tests:     normalizedStrings(set.Tests),
		Owners:    normalizedStrings(set.Owners),
	}, nil
}

func normalizedStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func canonicalExistingPath(value string) (string, error) {
	abs, err := filepath.Abs(value)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return "", err
	}
	return filepath.Clean(resolved), nil
}

func writeSetsEqual(left, right WriteSet) bool {
	return stringSlicesEqual(left.Files, right.Files) &&
		stringSlicesEqual(left.Modules, right.Modules) &&
		stringSlicesEqual(left.Symbols, right.Symbols) &&
		stringSlicesEqual(left.Contracts, right.Contracts) &&
		stringSlicesEqual(left.Tests, right.Tests) &&
		stringSlicesEqual(left.Owners, right.Owners)
}

func stringSlicesEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func workspaceNotFound() error {
	return kernel.Error{Code: kernel.CodeNotFound, Message: "workspace binding not found", Recoverable: false}
}

func unsupportedWorkspaceKind(kind Kind) error {
	return kernel.Error{Code: kernel.CodeInvalidRequest, Message: "unsupported_workspace_kind: " + string(kind), Recoverable: false}
}

func kernelInvalidRepository() error {
	return kernel.InvalidArgument("repository_path is required")
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
