package workspace

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

// NativeSnapshot is the Workspace Service seam used by provider adapters that
// project a phase-owned workspace into native agent tools and later import the
// full returned file set. Authority still comes exclusively from the active
// invocation lease in BindingStore.
type NativeSnapshot struct {
	BindingRef        kernel.BindingRef    `json:"binding_ref"`
	InvocationID      kernel.InvocationID  `json:"invocation_id"`
	Phase             Phase                `json:"phase"`
	BindingRevision   kernel.Revision      `json:"binding_revision"`
	WorkspaceRevision string               `json:"workspace_revision"`
	Files             []NativeSnapshotFile `json:"files"`
}

type NativeSnapshotFile struct {
	Path    string `json:"path"`
	Mode    uint32 `json:"mode"`
	Content []byte `json:"content"`
	SHA256  string `json:"sha256"`
}

func (s *Service) ExportNativeSnapshot(ctx context.Context, invocationID kernel.InvocationID) (NativeSnapshot, error) {
	var result NativeSnapshot
	err := s.withInvocation(ctx, invocationID, func(_ context.Context, binding Binding) error {
		states, err := snapshotWorkspaceFiles(binding.Root)
		if err != nil {
			return err
		}
		files := make([]NativeSnapshotFile, 0, len(states))
		paths := make([]string, 0, len(states))
		for filePath := range states {
			paths = append(paths, filePath)
		}
		sort.Strings(paths)
		for _, filePath := range paths {
			content, err := os.ReadFile(filepath.Join(binding.Root, filepath.FromSlash(filePath)))
			if err != nil {
				return fmt.Errorf("read native workspace file %q: %w", filePath, err)
			}
			state := states[filePath]
			files = append(files, NativeSnapshotFile{
				Path:    filePath,
				Mode:    uint32(state.mode.Perm()),
				Content: content,
				SHA256:  hex.EncodeToString(state.hash[:]),
			})
		}
		result = NativeSnapshot{
			BindingRef:        binding.ID,
			InvocationID:      invocationID,
			Phase:             binding.ActivePhase,
			BindingRevision:   binding.Revision,
			WorkspaceRevision: binding.CurrentRevision,
			Files:             files,
		}
		return nil
	})
	return result, err
}

func (s *Service) ImportNativeSnapshot(ctx context.Context, invocationID kernel.InvocationID, snapshot NativeSnapshot) (Binding, error) {
	var result Binding
	err := s.withInvocation(ctx, invocationID, func(lockCtx context.Context, binding Binding) error {
		if snapshot.InvocationID != "" && snapshot.InvocationID != invocationID {
			return kernel.Forbidden("native workspace snapshot invocation does not match active lease")
		}
		if snapshot.BindingRef != "" && snapshot.BindingRef != binding.ID {
			return kernel.Forbidden("native workspace snapshot binding does not match active lease")
		}
		if snapshot.Phase != "" && snapshot.Phase != binding.ActivePhase {
			return kernel.Forbidden("native workspace snapshot phase does not match active lease")
		}
		if err := validateWorkspaceSymlinks(binding.Root); err != nil {
			return err
		}
		before, err := snapshotWorkspaceFiles(binding.Root)
		if err != nil {
			return err
		}
		copyRoot, cleanup, err := materializeNativeSnapshot(snapshot)
		if err != nil {
			return err
		}
		defer cleanup()
		after, err := snapshotWorkspaceFiles(copyRoot)
		if err != nil {
			return err
		}
		staleBinding := snapshot.BindingRevision != 0 && snapshot.BindingRevision != binding.Revision
		staleWorkspace := snapshot.WorkspaceRevision != "" && snapshot.WorkspaceRevision != binding.CurrentRevision
		if staleBinding || staleWorkspace {
			// Provider responses may be lost after a successful import. An exact
			// full-snapshot replay is idempotent; any content difference against a
			// newer authoritative revision is a conflict and must never overwrite it.
			if nativeWorkspaceStatesEqual(before, after) {
				updated, refreshErr := s.refreshLocked(lockCtx, binding)
				if refreshErr != nil {
					return refreshErr
				}
				result = updated
				return nil
			}
			if staleBinding {
				return kernel.RevisionConflict(snapshot.BindingRevision, binding.Revision)
			}
			return kernel.StaleBinding("native workspace snapshot was projected from a different workspace revision")
		}
		if binding.ActivePhase == PhaseVerify {
			// Ordinary verifier files are invocation-local scratch. Cross-phase
			// evidence is carried only by registered ArtifactRefs in PhaseOutput;
			// importing evidence/ into the Git candidate would change the revision
			// after verification and pollute execute-only ObservedWrites.
			if err := validateNativeSnapshotChanges(binding, binding.ActivePhase, before, after); err != nil {
				return err
			}
			result = binding
			return nil
		}
		if err := applySandboxChanges(binding, binding.ActivePhase, copyRoot, before, after); err != nil {
			return err
		}
		if _, err := s.git.Checkpoint(lockCtx, binding, string(binding.ActivePhase)+":"+string(invocationID)); err != nil {
			return fmt.Errorf("checkpoint native workspace snapshot: %w", err)
		}
		updated, err := s.refreshLocked(lockCtx, binding)
		if err != nil {
			return err
		}
		result = updated
		return nil
	})
	if err != nil {
		return Binding{}, err
	}
	return cloneBinding(result), nil
}

func nativeWorkspaceStatesEqual(left, right map[string]workspaceFileState) bool {
	if len(left) != len(right) {
		return false
	}
	for filePath, leftState := range left {
		if rightState, ok := right[filePath]; !ok || rightState != leftState {
			return false
		}
	}
	return true
}

func validateNativeSnapshotChanges(binding Binding, phase Phase, before, after map[string]workspaceFileState) error {
	paths := make(map[string]struct{}, len(before)+len(after))
	for filePath := range before {
		paths[filePath] = struct{}{}
	}
	for filePath := range after {
		paths[filePath] = struct{}{}
	}
	for filePath := range paths {
		beforeState, beforeOK := before[filePath]
		afterState, afterOK := after[filePath]
		if beforeOK == afterOK && (!beforeOK || beforeState == afterState) {
			continue
		}
		if protectedWorkspacePath(filePath) || !phaseAllows(binding, phase, filePath) {
			return kernel.Forbidden("workspace command wrote outside the active phase boundary")
		}
	}
	return nil
}

func materializeNativeSnapshot(snapshot NativeSnapshot) (string, func(), error) {
	root, err := os.MkdirTemp("", "threadmill-native-snapshot-*")
	if err != nil {
		return "", nil, fmt.Errorf("create native workspace snapshot root: %w", err)
	}
	cleanup := func() { _ = os.RemoveAll(root) }
	seen := make(map[string]struct{}, len(snapshot.Files))
	var total int64
	for _, file := range snapshot.Files {
		clean, err := cleanRelativePath(file.Path)
		if err != nil {
			cleanup()
			return "", nil, fmt.Errorf("invalid native workspace path %q: %w", file.Path, err)
		}
		if protectedWorkspacePath(clean) {
			cleanup()
			return "", nil, kernel.Forbidden("native workspace snapshot includes protected path")
		}
		if _, ok := seen[clean]; ok {
			cleanup()
			return "", nil, kernel.InvalidArgument("native workspace snapshot contains duplicate path")
		}
		seen[clean] = struct{}{}
		mode := os.FileMode(file.Mode)
		if mode&^os.ModePerm != 0 {
			cleanup()
			return "", nil, kernel.Forbidden("native workspace snapshot contains unsupported file mode")
		}
		if mode.Perm() == 0 {
			mode = 0o644
		}
		if int64(len(file.Content)) > maxWorkspaceWriteBytes {
			cleanup()
			return "", nil, kernel.Forbidden("native workspace snapshot file exceeds write size limit")
		}
		total += int64(len(file.Content))
		if total > maxCommandCopyBytes {
			cleanup()
			return "", nil, kernel.Forbidden("native workspace snapshot exceeds size limit")
		}
		sum := sha256.Sum256(file.Content)
		if strings.ToLower(strings.TrimSpace(file.SHA256)) != hex.EncodeToString(sum[:]) {
			cleanup()
			return "", nil, kernel.InvalidArgument("native workspace snapshot file hash mismatch")
		}
		target := filepath.Join(root, filepath.FromSlash(clean))
		if err := ensureNoSymlinkEscape(root, target); err != nil {
			cleanup()
			return "", nil, err
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			cleanup()
			return "", nil, fmt.Errorf("create native workspace snapshot directory: %w", err)
		}
		if err := os.WriteFile(target, file.Content, mode.Perm()); err != nil {
			cleanup()
			return "", nil, fmt.Errorf("write native workspace snapshot file %q: %w", clean, err)
		}
	}
	return root, cleanup, nil
}
