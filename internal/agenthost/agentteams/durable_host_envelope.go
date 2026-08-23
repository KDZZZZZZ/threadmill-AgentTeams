package agentteams

import (
	"context"
	"errors"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/phaseagent"
)

// TrustedHostBindingResolver is process-local composition: it resolves an
// already-issued execution binding, including its opaque token, without ever
// persisting that token in reconstruction records.
type TrustedHostBindingResolver interface {
	ResolveTrustedHostBinding(context.Context, phaseagent.ExecutionContext) (TrustedMCPBinding, error)
}

// WorkspaceMountResolver derives a local mount from a durable workspace
// descriptor. Its root is local execution-plane state and is never stored in
// RuntimeStateRepository.
type WorkspaceMountResolver interface {
	ResolveWorkspaceMount(context.Context, runtime.DurableWorkspace, phaseagent.ExecutionContext) (WorkspaceMount, error)
}

// DurableHostEnvelopeResolver is the production host-envelope composition
// seam. Task contract, workspace descriptor, context and task-memory come
// from one RuntimeStateRepository; binding/token and mount root are injected
// process-local capabilities.
type DurableHostEnvelopeResolver struct {
	Reconstruction runtime.DurableReconstructionStore
	Bindings       TrustedHostBindingResolver
	Mounts         WorkspaceMountResolver
}

var _ HostEnvelopeResolver = DurableHostEnvelopeResolver{}

func (r DurableHostEnvelopeResolver) ResolveHostEnvelope(ctx context.Context, execution phaseagent.ExecutionContext) (HostEnvelope, error) {
	if r.Reconstruction == nil || r.Bindings == nil || r.Mounts == nil {
		return HostEnvelope{}, errors.New("durable host envelope resolver dependencies are required")
	}
	start := execution.Invocation.Start
	if err := start.Validate(); err != nil {
		return HostEnvelope{}, err
	}
	key := runtime.WaitingKey{TaskID: start.Endpoint.TaskID, InvocationID: start.InvocationID, Generation: start.Generation}
	descriptor, found, err := r.Reconstruction.GetExecutionDescriptor(ctx, key)
	if err != nil {
		return HostEnvelope{}, err
	}
	if !found {
		return HostEnvelope{}, errors.New("durable execution descriptor is missing")
	}
	workspace, found, err := r.Reconstruction.GetWorkspace(ctx, descriptor.WorkspaceRef, key)
	if err != nil {
		return HostEnvelope{}, err
	}
	if !found {
		return HostEnvelope{}, errors.New("durable workspace descriptor is missing")
	}
	contextValue, found, err := r.Reconstruction.GetContextSlice(ctx, descriptor.ContextSliceRef, key)
	if err != nil {
		return HostEnvelope{}, err
	}
	if !found {
		return HostEnvelope{}, errors.New("durable context slice is missing")
	}
	memory, found, err := r.Reconstruction.GetTaskMemory(ctx, descriptor.TaskMemoryRef, key)
	if err != nil {
		return HostEnvelope{}, err
	}
	if !found {
		return HostEnvelope{}, errors.New("durable task memory is missing")
	}
	binding, err := r.Bindings.ResolveTrustedHostBinding(ctx, execution)
	if err != nil {
		return HostEnvelope{}, err
	}
	if binding.Binding.TaskID != key.TaskID || binding.Binding.InvocationID != key.InvocationID || binding.Binding.Generation != key.Generation || binding.Binding.BindingRef != start.BindingRef || binding.Binding.Endpoint != start.Endpoint {
		return HostEnvelope{}, errors.New("trusted host binding does not match durable logical identity")
	}
	mount, err := r.Mounts.ResolveWorkspaceMount(ctx, workspace, execution)
	if err != nil {
		return HostEnvelope{}, err
	}
	if !sameDirs(mount.AllowedDirs, workspace.AllowedDirs) {
		return HostEnvelope{}, errors.New("workspace mount expands durable allowed directories")
	}
	return HostEnvelope{BindingRef: start.BindingRef, TaskSpec: descriptor.TaskSpec, TaskContract: descriptor.TaskContract, PhaseInstruction: descriptor.PhaseInstruction, Workspace: mount, Context: MaterializedContext{Content: contextValue.Content}, TaskMemory: memory.View, MCPBinding: binding}, nil
}

// ResolveRehydratedPackageEnvelope is deliberately narrower than the normal
// host resolver. Package materialization happens before the fresh token is
// issued; it therefore projects only durable agent-visible material and never
// synthesizes a trusted MCP binding or local mount root.
func (r DurableHostEnvelopeResolver) ResolveRehydratedPackageEnvelope(ctx context.Context, plan runtime.RehydrationPlan) (HostEnvelope, error) {
	if r.Reconstruction == nil {
		return HostEnvelope{}, errors.New("durable reconstruction store is required")
	}
	key := runtime.WaitingKey{TaskID: plan.TaskID, InvocationID: plan.InvocationID, Generation: plan.Generation}
	descriptor, found, err := r.Reconstruction.GetExecutionDescriptor(ctx, key)
	if err != nil {
		return HostEnvelope{}, err
	}
	if !found {
		return HostEnvelope{}, errors.New("durable execution descriptor is missing")
	}
	if descriptor.WorkspaceRef != plan.Workspace.Ref || descriptor.ContextSliceRef != plan.Context.SliceRef || descriptor.TaskMemoryRef != plan.TaskMemory.BufferRef {
		return HostEnvelope{}, errors.New("durable execution descriptor does not match rehydration plan")
	}
	workspace, found, err := r.Reconstruction.GetWorkspace(ctx, descriptor.WorkspaceRef, key)
	if err != nil {
		return HostEnvelope{}, err
	}
	if !found {
		return HostEnvelope{}, errors.New("durable workspace descriptor is missing")
	}
	contextValue, found, err := r.Reconstruction.GetContextSlice(ctx, descriptor.ContextSliceRef, key)
	if err != nil {
		return HostEnvelope{}, err
	}
	if !found {
		return HostEnvelope{}, errors.New("durable context slice is missing")
	}
	memory, found, err := r.Reconstruction.GetTaskMemory(ctx, descriptor.TaskMemoryRef, key)
	if err != nil {
		return HostEnvelope{}, err
	}
	if !found {
		return HostEnvelope{}, errors.New("durable task memory is missing")
	}
	if !sameDirs(plan.Workspace.AllowedDirs, workspace.AllowedDirs) {
		return HostEnvelope{}, errors.New("rehydration plan expands durable workspace allowed directories")
	}
	return HostEnvelope{BindingRef: plan.NewBindingRef, TaskSpec: descriptor.TaskSpec, TaskContract: descriptor.TaskContract, PhaseInstruction: descriptor.PhaseInstruction, Workspace: WorkspaceMount{AllowedDirs: append([]string(nil), workspace.AllowedDirs...)}, Context: MaterializedContext{Content: contextValue.Content}, TaskMemory: memory.View}, nil
}

func sameDirs(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	seen := make(map[string]struct{}, len(want))
	for _, value := range want {
		seen[value] = struct{}{}
	}
	for _, value := range got {
		if _, ok := seen[value]; !ok {
			return false
		}
	}
	return true
}
