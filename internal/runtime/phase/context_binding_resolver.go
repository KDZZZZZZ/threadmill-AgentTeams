package phase

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/contextgraph"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	baseruntime "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime"
)

type BindingResolveRequest struct {
	Command      PhaseCommand
	InvocationID kernel.InvocationID
}

type BaseBindingSource interface {
	ResolvePhaseBinding(context.Context, BindingResolveRequest) (BindingSnapshot, []string, error)
	RefreshPhaseBinding(context.Context, ActiveInvocation) (BindingSnapshot, []string, error)
}

type ContextRuntime interface {
	EnsureInitialSlice(context.Context, auth.Principal, []string) (contextgraph.ContextSlice, error)
	InspectSubscriptions(context.Context, auth.Principal, kernel.InvocationID) ([]contextgraph.SubscriptionInspection, error)
	MaterializeRuntimeContext(context.Context, auth.Principal) (contextgraph.ContextSlice, error)
	ListTaskCandidates(context.Context, auth.Principal) (contextgraph.TaskMemoryBufferView, error)
	EndInvocation(context.Context, auth.Principal, kernel.InvocationID) error
}

type ContextBindingResolver struct {
	Base     BaseBindingSource
	Contexts ContextRuntime

	mu          sync.Mutex
	initialOnce map[string]*initialSliceCall
}

func NewContextBindingResolver(base BaseBindingSource, contexts ContextRuntime) *ContextBindingResolver {
	return &ContextBindingResolver{Base: base, Contexts: contexts}
}

func (r *ContextBindingResolver) Resolve(ctx context.Context, command PhaseCommand) (BindingSnapshot, error) {
	return r.ResolveForInvocation(ctx, BindingResolveRequest{
		Command:      command,
		InvocationID: deterministicInvocationID(command),
	})
}

func (r *ContextBindingResolver) ResolveForInvocation(ctx context.Context, req BindingResolveRequest) (BindingSnapshot, error) {
	if r == nil || r.Base == nil {
		return BindingSnapshot{}, kernel.InvalidArgument("base binding source is required")
	}
	if err := validateCommand(req.Command); err != nil {
		return BindingSnapshot{}, err
	}
	if req.InvocationID == "" {
		return BindingSnapshot{}, kernel.InvalidArgument("invocation_id is required")
	}
	if req.InvocationID != deterministicInvocationID(req.Command) {
		return BindingSnapshot{}, kernel.StaleBinding("invocation id does not match command")
	}
	binding, initialSubgraphIDs, err := r.Base.ResolvePhaseBinding(ctx, req)
	if err != nil {
		return BindingSnapshot{}, err
	}
	if err := validateBindingForCommand(binding, req.Command); err != nil {
		return BindingSnapshot{}, err
	}
	return r.bindContext(ctx, req.Command, req.InvocationID, binding, initialSubgraphIDs)
}

func (r *ContextBindingResolver) Refresh(ctx context.Context, active ActiveInvocation) (BindingSnapshot, error) {
	if r == nil || r.Base == nil {
		return BindingSnapshot{}, kernel.InvalidArgument("base binding source is required")
	}
	if err := validateCommand(active.Command); err != nil {
		return BindingSnapshot{}, err
	}
	if active.Invocation.ID == "" {
		return BindingSnapshot{}, kernel.InvalidArgument("invocation_id is required")
	}
	if active.Invocation.ID != deterministicInvocationID(active.Command) {
		return BindingSnapshot{}, kernel.StaleBinding("invocation id does not match command")
	}
	binding, initialSubgraphIDs, err := r.Base.RefreshPhaseBinding(ctx, active)
	if err != nil {
		return BindingSnapshot{}, err
	}
	if err := validateBindingForActive(binding, active); err != nil {
		return BindingSnapshot{}, err
	}
	if binding.ActorPrincipalID != active.Invocation.ActorPrincipalID {
		return BindingSnapshot{}, kernel.StaleBinding("binding actor principal does not match active invocation")
	}
	return r.bindContext(ctx, active.Command, active.Invocation.ID, binding, initialSubgraphIDs)
}

func (r *ContextBindingResolver) bindContext(ctx context.Context, command PhaseCommand, invocationID kernel.InvocationID, binding BindingSnapshot, initialSubgraphIDs []string) (BindingSnapshot, error) {
	if r.Contexts == nil {
		return BindingSnapshot{}, kernel.InvalidArgument("context runtime is required")
	}
	principal, err := phaseContextPrincipal(command, invocationID, binding)
	if err != nil {
		return BindingSnapshot{}, err
	}
	subscriptions, err := r.Contexts.InspectSubscriptions(ctx, principal, invocationID)
	if err != nil {
		return BindingSnapshot{}, err
	}
	if !hasActiveSubscription(subscriptions) && len(initialSubgraphIDs) > 0 {
		if _, err := r.ensureInitialSlice(ctx, principal, initialSubgraphIDs); err != nil {
			return BindingSnapshot{}, err
		}
	}
	slice, err := r.Contexts.MaterializeRuntimeContext(ctx, principal)
	if err != nil {
		return BindingSnapshot{}, err
	}
	memory, err := r.Contexts.ListTaskCandidates(ctx, principal)
	if err != nil {
		return BindingSnapshot{}, err
	}
	sliceJSON, err := stableJSON(slice)
	if err != nil {
		return BindingSnapshot{}, err
	}
	memoryJSON, err := stableJSON(memory)
	if err != nil {
		return BindingSnapshot{}, err
	}
	binding.ContextSlice = string(sliceJSON)
	binding.ContextSliceRef = stableBindingRef("context-slice", binding.ProjectID, command.Endpoint.TaskID, invocationID, sliceJSON)
	binding.TaskMemoryBuffer = string(memoryJSON)
	binding.TaskMemoryBufferRef = stableBindingRef("task-memory-buffer", binding.ProjectID, command.Endpoint.TaskID, invocationID, memoryJSON)
	return binding, nil
}

type ContextBindingLifecycle struct {
	Contexts ContextRuntime
}

func (l ContextBindingLifecycle) End(ctx context.Context, invocation baseruntime.Invocation) error {
	if l.Contexts == nil {
		return kernel.InvalidArgument("context runtime is required")
	}
	principal, err := invocationContextPrincipal(invocation)
	if err != nil {
		return err
	}
	return l.Contexts.EndInvocation(ctx, principal, invocation.ID)
}

func (l ContextBindingLifecycle) Complete(ctx context.Context, invocation baseruntime.Invocation) error {
	return l.End(ctx, invocation)
}

func phaseContextPrincipal(command PhaseCommand, invocationID kernel.InvocationID, binding BindingSnapshot) (auth.Principal, error) {
	role, err := phaseRole(command.Endpoint.EndpointID)
	if err != nil {
		return auth.Principal{}, err
	}
	projectID := binding.ProjectID
	if projectID == "" {
		return auth.Principal{}, kernel.InvalidArgument("project_id is required")
	}
	taskID := binding.TaskID
	if taskID == "" {
		taskID = command.Endpoint.TaskID
	}
	if taskID == "" {
		return auth.Principal{}, kernel.InvalidArgument("task_id is required")
	}
	actorID := binding.ActorPrincipalID
	if actorID == "" {
		return auth.Principal{}, kernel.InvalidArgument("actor_principal_id is required")
	}
	return auth.Principal{
		ActorPrincipalID: actorID,
		Kind:             auth.PrincipalAgent,
		ProjectID:        projectID,
		Role:             role,
		TaskID:           taskID,
		InvocationID:     invocationID,
		Tools: auth.ToolSet(
			auth.ToolContextSubscribe,
			auth.ToolAgentListTaskMemoryCandidates,
		),
	}, nil
}

func invocationContextPrincipal(invocation baseruntime.Invocation) (auth.Principal, error) {
	if invocation.ProjectID == "" {
		return auth.Principal{}, kernel.InvalidArgument("project_id is required")
	}
	if invocation.TaskID == "" {
		return auth.Principal{}, kernel.InvalidArgument("task_id is required")
	}
	if invocation.ID == "" {
		return auth.Principal{}, kernel.InvalidArgument("invocation_id is required")
	}
	if !invocation.Role.IsPhase() {
		return auth.Principal{}, kernel.InvalidArgument("phase role is required")
	}
	actorID := invocation.ActorPrincipalID
	if actorID == "" {
		return auth.Principal{}, kernel.InvalidArgument("actor_principal_id is required")
	}
	return auth.Principal{
		ActorPrincipalID: actorID,
		Kind:             auth.PrincipalAgent,
		ProjectID:        invocation.ProjectID,
		Role:             invocation.Role,
		TaskID:           invocation.TaskID,
		InvocationID:     invocation.ID,
		Tools: auth.ToolSet(
			auth.ToolContextSubscribe,
			auth.ToolAgentListTaskMemoryCandidates,
		),
	}, nil
}

type initialSliceCall struct {
	done  chan struct{}
	slice contextgraph.ContextSlice
	err   error
}

func (r *ContextBindingResolver) ensureInitialSlice(ctx context.Context, principal auth.Principal, subgraphIDs []string) (contextgraph.ContextSlice, error) {
	key := fmt.Sprintf("%s/%s", principal.ProjectID, principal.InvocationID)

	r.mu.Lock()
	if r.initialOnce == nil {
		r.initialOnce = make(map[string]*initialSliceCall)
	}
	if call, ok := r.initialOnce[key]; ok {
		r.mu.Unlock()
		select {
		case <-call.done:
			return call.slice, call.err
		case <-ctx.Done():
			return contextgraph.ContextSlice{}, ctx.Err()
		}
	}
	call := &initialSliceCall{done: make(chan struct{})}
	r.initialOnce[key] = call
	r.mu.Unlock()

	call.slice, call.err = r.Contexts.EnsureInitialSlice(ctx, principal, subgraphIDs)
	close(call.done)

	r.mu.Lock()
	delete(r.initialOnce, key)
	r.mu.Unlock()
	return call.slice, call.err
}

func hasActiveSubscription(subscriptions []contextgraph.SubscriptionInspection) bool {
	for _, subscription := range subscriptions {
		if subscription.Active {
			return true
		}
	}
	return false
}

func stableJSON(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return []byte("null"), nil
	}
	return raw, nil
}

func stableBindingRef(kind string, projectID kernel.ProjectID, taskID kernel.TaskID, invocationID kernel.InvocationID, raw []byte) string {
	sum := sha256.Sum256(raw)
	return fmt.Sprintf("%s://%s/%s/%s/%s", kind, projectID, taskID, invocationID, hex.EncodeToString(sum[:8]))
}
