package runtime

import (
	"context"
	"errors"
	"fmt"
)

// DurableReconstructionAuthority adapts repository-owned logical records to
// the existing M4 rehydration interfaces. It never infers truth from a
// worker, Matrix room, or process-local cache.
type DurableReconstructionAuthority struct{ Store DurableReconstructionStore }

func (a DurableReconstructionAuthority) ReconstructWorkspace(ctx context.Context, waiting WaitingRecord, material ContinuationMaterial) (WorkspaceBinding, error) {
	if a.Store == nil {
		return WorkspaceBinding{}, errors.New("durable reconstruction store is required")
	}
	if material.WorkspaceRef != "" && material.WorkspaceRef != waiting.WorkspaceRef {
		return WorkspaceBinding{}, ErrReconstructionConflict
	}
	workspace, found, err := a.Store.GetWorkspace(ctx, waiting.WorkspaceRef, waiting.Key)
	if err != nil {
		return WorkspaceBinding{}, err
	}
	if !found {
		return WorkspaceBinding{}, errors.New("durable workspace authority is missing")
	}
	if !allowedDirsWithin(workspace.AllowedDirs, waiting.AllowedDirs) {
		return WorkspaceBinding{}, ErrReconstructionConflict
	}
	return WorkspaceBinding{Ref: workspace.Ref, Revision: fmt.Sprintf("%d", workspace.Revision), AllowedDirs: append([]string(nil), workspace.AllowedDirs...)}, nil
}
func (a DurableReconstructionAuthority) ReconstructContext(ctx context.Context, waiting WaitingRecord, material ContinuationMaterial) (RehydratedContext, error) {
	if a.Store == nil {
		return RehydratedContext{}, errors.New("durable reconstruction store is required")
	}
	if material.ContextSliceRef != "" && material.ContextSliceRef != waiting.ContextSliceRef {
		return RehydratedContext{}, ErrReconstructionConflict
	}
	slice, found, err := a.Store.GetContextSlice(ctx, waiting.ContextSliceRef, waiting.Key)
	if err != nil {
		return RehydratedContext{}, err
	}
	if !found {
		return RehydratedContext{}, errors.New("durable context authority is missing")
	}
	if material.ContextBaselineRef != "" && material.ContextBaselineRef != slice.BaselineRef {
		return RehydratedContext{}, ErrReconstructionConflict
	}
	return RehydratedContext{SliceRef: slice.Ref, BaselineRef: slice.BaselineRef}, nil
}
func (a DurableReconstructionAuthority) ReconstructTaskMemory(ctx context.Context, waiting WaitingRecord, material ContinuationMaterial) (RehydratedTaskMemory, error) {
	if a.Store == nil {
		return RehydratedTaskMemory{}, errors.New("durable reconstruction store is required")
	}
	if material.TaskMemoryBufferRef != "" && material.TaskMemoryBufferRef != waiting.TaskMemoryBufferRef {
		return RehydratedTaskMemory{}, ErrReconstructionConflict
	}
	memory, found, err := a.Store.GetTaskMemory(ctx, waiting.TaskMemoryBufferRef, waiting.Key)
	if err != nil {
		return RehydratedTaskMemory{}, err
	}
	if !found {
		return RehydratedTaskMemory{}, errors.New("durable task memory authority is missing")
	}
	return RehydratedTaskMemory{BufferRef: memory.Ref, View: memory.View}, nil
}

// WorkspaceRootResolver derives a fresh, process-local mount root. Its return
// value is intentionally not durable state.
type WorkspaceRootResolver interface {
	ResolveWorkspaceRoot(context.Context, DurableWorkspaceLease) (string, error)
}

// DurableWorkspaceLeaseAuthority provides the existing provisioning seam with
// a repository-fenced logical lease and a freshly derived local mount root.
type DurableWorkspaceLeaseAuthority struct {
	Store DurableReconstructionStore
	Roots WorkspaceRootResolver
}

func (a DurableWorkspaceLeaseAuthority) AcquireWorkspaceLease(ctx context.Context, plan RehydrationPlan) (WorkspaceLease, error) {
	if a.Store == nil || a.Roots == nil {
		return WorkspaceLease{}, errors.New("durable workspace lease authority dependencies are required")
	}
	key := WaitingKey{TaskID: plan.TaskID, InvocationID: plan.InvocationID, Generation: plan.Generation}
	lease, err := a.Store.AcquireWorkspaceLease(ctx, DurableWorkspaceLease{Ref: fmt.Sprintf("workspace-lease:%s:g%d:e%d", plan.InvocationID, plan.Generation, plan.NextExecutionEpoch), Key: key, WorkspaceRef: plan.Workspace.Ref, ExecutionEpoch: plan.NextExecutionEpoch})
	if err != nil {
		return WorkspaceLease{}, err
	}
	root, err := a.Roots.ResolveWorkspaceRoot(ctx, lease)
	if err != nil {
		return WorkspaceLease{}, err
	}
	if root == "" {
		return WorkspaceLease{}, errors.New("workspace root resolver returned an empty root")
	}
	return WorkspaceLease{TaskID: plan.TaskID, InvocationID: plan.InvocationID, Generation: plan.Generation, Ref: lease.Ref, WorkspaceRef: lease.WorkspaceRef, WorkspaceRoot: root, AllowedDirs: append([]string(nil), plan.Workspace.AllowedDirs...), Epoch: lease.ExecutionEpoch}, nil
}
func (a DurableWorkspaceLeaseAuthority) ReleaseWorkspaceLease(ctx context.Context, lease WorkspaceLease) error {
	if a.Store == nil {
		return errors.New("durable reconstruction store is required")
	}
	key := WaitingKey{TaskID: lease.TaskID, InvocationID: lease.InvocationID, Generation: lease.Generation}
	if key.TaskID == "" || key.InvocationID == "" || key.Generation <= 0 {
		return errors.New("workspace lease owner identity is required")
	}
	current, found, err := a.Store.ReadWorkspaceLease(ctx, lease.Ref, key, lease.Epoch)
	if err != nil {
		return err
	}
	if !found {
		return errors.New("workspace lease was not found")
	}
	_, swapped, err := a.Store.ReleaseWorkspaceLease(ctx, current, current.Revision)
	if err != nil {
		return err
	}
	if !swapped {
		return ErrWorkspaceLeaseConflict
	}
	return nil
}
