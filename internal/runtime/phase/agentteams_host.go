package phase

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	adapter "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/adapters/agentteams"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

const (
	agentTeamsRuntimeConfigRefPrefix = "threadmill://phase-runtime-config/"
	agentTeamsEnvelopeRefPrefix      = "threadmill://phase-envelope/"
	agentTeamsInvocationRefPrefix    = "threadmill://phase-invocation/"
)

type PreparedInvocationWriter interface {
	SavePreparedInvocation(context.Context, string, adapter.PreparedInvocation) error
}

type AgentTeamsPhaseHostState struct {
	InvocationID    kernel.InvocationID
	InvocationRef   string
	Execution       adapter.AgentTeamsExecutionRef
	TerminationMode adapter.TerminateMode
}

type AgentTeamsPhaseHostStateStore interface {
	LoadAgentTeamsPhaseHostState(context.Context, kernel.InvocationID) (AgentTeamsPhaseHostState, bool, error)
	SaveAgentTeamsPhaseHostState(context.Context, AgentTeamsPhaseHostState) error
}

type AgentTeamsPhaseHostConfig struct {
	Adapter adapter.AgentTeamsHostAdapter
	Writer  PreparedInvocationWriter
	State   AgentTeamsPhaseHostStateStore
	RoomID  string
}

type AgentTeamsPhaseHost struct {
	adapter adapter.AgentTeamsHostAdapter
	writer  PreparedInvocationWriter
	state   AgentTeamsPhaseHostStateStore
	roomID  string
}

func NewAgentTeamsPhaseHost(cfg AgentTeamsPhaseHostConfig) (*AgentTeamsPhaseHost, error) {
	if cfg.Adapter == nil || cfg.Writer == nil || cfg.State == nil {
		return nil, kernel.InvalidArgument("AgentTeams phase host dependencies are required")
	}
	roomID := strings.TrimSpace(cfg.RoomID)
	if roomID == "" {
		return nil, kernel.InvalidArgument("AgentTeams room_id is required")
	}
	return &AgentTeamsPhaseHost{
		adapter: cfg.Adapter,
		writer:  cfg.Writer,
		state:   cfg.State,
		roomID:  roomID,
	}, nil
}

func (h *AgentTeamsPhaseHost) Dispatch(ctx context.Context, req DispatchRequest) error {
	return h.dispatch(ctx, req, false)
}

func (h *AgentTeamsPhaseHost) Rehydrate(ctx context.Context, req DispatchRequest) error {
	return h.dispatch(ctx, req, true)
}

func (h *AgentTeamsPhaseHost) Suspend(ctx context.Context, invocationID kernel.InvocationID) error {
	state, err := h.executionState(ctx, invocationID)
	if err != nil {
		return err
	}
	state.TerminationMode = adapter.TerminateReleaseWait
	if err := h.adapter.Terminate(ctx, state.Execution, string(adapter.TerminateReleaseWait)); err != nil {
		return err
	}
	return h.state.SaveAgentTeamsPhaseHostState(ctx, state)
}

func (h *AgentTeamsPhaseHost) Stop(ctx context.Context, req StopRequest) (StopResult, error) {
	state, err := h.executionState(ctx, req.Invocation.ID)
	if err != nil {
		return StopResult{}, err
	}
	state.TerminationMode = adapter.TerminateRecoverableStop
	if err := h.adapter.Terminate(ctx, state.Execution, string(adapter.TerminateRecoverableStop)); err != nil {
		return StopResult{}, err
	}
	if err := h.state.SaveAgentTeamsPhaseHostState(ctx, state); err != nil {
		return StopResult{}, err
	}
	result := StopResult{
		ResumeStateRef:    fmt.Sprintf("agentteams://resume-state/%s/%s", state.Execution.AgentTeamsTaskID, req.Invocation.ID),
		CheckpointRef:     req.Binding.CheckpointRef,
		WorkspaceRevision: req.Binding.WorkspaceRevision,
	}
	if req.Binding.CheckpointRef == "" {
		result.NonResumable = true
		result.ResumeStateRef = ""
		result.WorkspaceRevision = ""
	}
	return result, nil
}

func (h *AgentTeamsPhaseHost) Revoke(ctx context.Context, invocationID kernel.InvocationID) error {
	state, err := h.executionState(ctx, invocationID)
	if err != nil {
		return err
	}
	mode := state.TerminationMode
	if mode == "" {
		mode = adapter.TerminateCancel
		state.TerminationMode = mode
	}
	if err := h.adapter.Terminate(ctx, state.Execution, string(mode)); err != nil {
		return err
	}
	return h.state.SaveAgentTeamsPhaseHostState(ctx, state)
}

func (h *AgentTeamsPhaseHost) dispatch(ctx context.Context, req DispatchRequest, rehydrate bool) error {
	if err := validateAgentTeamsDispatch(req); err != nil {
		return err
	}
	invocationRef := agentTeamsInvocationRef(req.Invocation.ID)
	if !rehydrate {
		prepared, err := h.preparedInvocation(req, invocationRef)
		if err != nil {
			return err
		}
		if err := h.writer.SavePreparedInvocation(ctx, invocationRef, prepared); err != nil {
			return err
		}
	} else if _, ok, err := h.state.LoadAgentTeamsPhaseHostState(ctx, req.Invocation.ID); err != nil {
		return err
	} else if !ok {
		prepared, err := h.preparedInvocation(req, invocationRef)
		if err != nil {
			return err
		}
		if err := h.writer.SavePreparedInvocation(ctx, invocationRef, prepared); err != nil {
			return err
		}
	}
	execution, err := h.adapter.Dispatch(ctx, invocationRef)
	if err != nil {
		return err
	}
	state := AgentTeamsPhaseHostState{
		InvocationID:  req.Invocation.ID,
		InvocationRef: invocationRef,
		Execution:     execution,
	}
	if err := h.state.SaveAgentTeamsPhaseHostState(ctx, state); err != nil {
		terminateErr := h.adapter.Terminate(context.Background(), execution, string(adapter.TerminateCancel))
		return errors.Join(err, terminateErr)
	}
	return nil
}

func (h *AgentTeamsPhaseHost) executionState(ctx context.Context, invocationID kernel.InvocationID) (AgentTeamsPhaseHostState, error) {
	if err := kernel.RequireID("invocation_id", invocationID); err != nil {
		return AgentTeamsPhaseHostState{}, err
	}
	state, ok, err := h.state.LoadAgentTeamsPhaseHostState(ctx, invocationID)
	if err != nil {
		return AgentTeamsPhaseHostState{}, err
	}
	if ok {
		return state, nil
	}
	invocationRef := agentTeamsInvocationRef(invocationID)
	execution, err := h.adapter.Dispatch(ctx, invocationRef)
	if err != nil {
		return AgentTeamsPhaseHostState{}, err
	}
	state = AgentTeamsPhaseHostState{
		InvocationID:  invocationID,
		InvocationRef: invocationRef,
		Execution:     execution,
	}
	if err := h.state.SaveAgentTeamsPhaseHostState(ctx, state); err != nil {
		return AgentTeamsPhaseHostState{}, err
	}
	return state, nil
}

func (h *AgentTeamsPhaseHost) preparedInvocation(req DispatchRequest, invocationRef string) (adapter.PreparedInvocation, error) {
	spec, err := agentTeamsPhaseSpec(req)
	if err != nil {
		return adapter.PreparedInvocation{}, err
	}
	return adapter.PreparedInvocation{
		InvocationID:         req.Invocation.ID,
		ProjectID:            req.Invocation.ProjectID,
		Role:                 req.Invocation.Role,
		Operation:            req.Invocation.Operation,
		RoomID:               h.roomID,
		Spec:                 spec,
		RuntimeConfigRef:     agentTeamsRuntimeConfigRef(req.Invocation.ID),
		EnvelopeRef:          agentTeamsEnvelopeRef(req.Invocation.ID, invocationRef, req.Prompt.SHA256),
		RequiredCapabilities: requiredAgentTeamsCapabilities(req.Capability),
	}, nil
}

type agentTeamsPhaseSpecDocument struct {
	Kind                 string              `json:"kind"`
	InvocationID         kernel.InvocationID `json:"invocation_id"`
	ProjectID            kernel.ProjectID    `json:"project_id"`
	TaskID               kernel.TaskID       `json:"task_id"`
	EndpointID           kernel.EndpointID   `json:"endpoint_id"`
	Generation           uint64              `json:"generation"`
	BindingRef           kernel.BindingRef   `json:"binding_ref"`
	LeaseRef             kernel.LeaseID      `json:"lease_ref"`
	WorkspaceRef         string              `json:"workspace_ref"`
	WorkspaceRevision    string              `json:"workspace_revision"`
	ContextSliceRef      string              `json:"context_slice_ref,omitempty"`
	TaskMemoryBufferRef  string              `json:"task_memory_buffer_ref,omitempty"`
	CheckpointRef        string              `json:"checkpoint_ref,omitempty"`
	InputRevision        string              `json:"input_revision,omitempty"`
	RuntimeConfigRefHint string              `json:"runtime_config_ref_hint"`
	PromptSHA256         string              `json:"prompt_sha256"`
	Prompt               string              `json:"prompt"`
}

func agentTeamsPhaseSpec(req DispatchRequest) (string, error) {
	doc := agentTeamsPhaseSpecDocument{
		Kind:                 "threadmill.phase.agentteams.v1",
		InvocationID:         req.Invocation.ID,
		ProjectID:            req.Invocation.ProjectID,
		TaskID:               req.Invocation.TaskID,
		EndpointID:           req.Invocation.EndpointID,
		Generation:           req.Invocation.Generation,
		BindingRef:           req.Invocation.BindingRef,
		LeaseRef:             req.Invocation.LeaseID,
		WorkspaceRef:         req.Binding.WorkspaceRef,
		WorkspaceRevision:    req.Binding.WorkspaceRevision,
		ContextSliceRef:      req.Binding.ContextSliceRef,
		TaskMemoryBufferRef:  req.Binding.TaskMemoryBufferRef,
		CheckpointRef:        req.CheckpointRef,
		InputRevision:        req.Start.Inputs.InputRevision,
		RuntimeConfigRefHint: agentTeamsRuntimeConfigRef(req.Invocation.ID),
		PromptSHA256:         req.Prompt.SHA256,
		Prompt:               req.Prompt.Text,
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		return "", kernel.InvalidArgument("AgentTeams phase spec cannot be encoded")
	}
	return string(raw), nil
}

func validateAgentTeamsDispatch(req DispatchRequest) error {
	if err := req.Invocation.Validate(); err != nil {
		return err
	}
	if !req.Invocation.Role.IsPhase() {
		return kernel.Forbidden("AgentTeams phase host requires a phase invocation")
	}
	if req.Prompt.Text == "" || req.Prompt.SHA256 == "" {
		return kernel.InvalidArgument("AgentTeams phase host requires rendered prompt text and hash")
	}
	if req.Capability.InvocationID != req.Invocation.ID || req.Capability.Role != req.Invocation.Role {
		return kernel.Forbidden("AgentTeams phase host capability must match invocation")
	}
	return nil
}

func requiredAgentTeamsCapabilities(capability auth.Capability) []string {
	if capability.Tools == nil {
		return nil
	}
	for _, tool := range []auth.Tool{auth.ToolWorkspaceRun, auth.ToolWorkspaceWrite, auth.ToolWorkspaceWritePlan} {
		if _, ok := capability.Tools[tool]; ok {
			return []string{"shell"}
		}
	}
	return nil
}

func agentTeamsInvocationRef(invocationID kernel.InvocationID) string {
	return agentTeamsInvocationRefPrefix + string(invocationID)
}

func agentTeamsRuntimeConfigRef(invocationID kernel.InvocationID) string {
	return agentTeamsRuntimeConfigRefPrefix + string(invocationID)
}

func agentTeamsEnvelopeRef(invocationID kernel.InvocationID, invocationRef string, promptHash string) string {
	sum := sha256.Sum256([]byte(string(invocationID) + "\x00" + invocationRef + "\x00" + promptHash))
	return agentTeamsEnvelopeRefPrefix + string(invocationID) + "/" + hex.EncodeToString(sum[:16])
}

type MemoryPreparedInvocationWriter struct {
	mu    sync.RWMutex
	items map[string]adapter.PreparedInvocation
}

func NewMemoryPreparedInvocationWriter() *MemoryPreparedInvocationWriter {
	return &MemoryPreparedInvocationWriter{items: make(map[string]adapter.PreparedInvocation)}
}

func (w *MemoryPreparedInvocationWriter) SavePreparedInvocation(ctx context.Context, ref string, prepared adapter.PreparedInvocation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(ref) == "" {
		return kernel.InvalidArgument("prepared invocation ref is required")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.items == nil {
		w.items = make(map[string]adapter.PreparedInvocation)
	}
	if existing, ok := w.items[ref]; ok && !samePreparedInvocation(existing, prepared) {
		return kernel.IdempotencyConflict()
	}
	w.items[ref] = clonePreparedInvocation(prepared)
	return nil
}

func (w *MemoryPreparedInvocationWriter) LoadPreparedInvocation(ctx context.Context, ref string) (adapter.PreparedInvocation, error) {
	if err := ctx.Err(); err != nil {
		return adapter.PreparedInvocation{}, err
	}
	w.mu.RLock()
	defer w.mu.RUnlock()
	prepared, ok := w.items[ref]
	if !ok {
		return adapter.PreparedInvocation{}, kernel.Error{Code: kernel.CodeNotFound, Message: "prepared invocation not found"}
	}
	return clonePreparedInvocation(prepared), nil
}

type MemoryAgentTeamsPhaseHostStateStore struct {
	mu    sync.RWMutex
	items map[kernel.InvocationID]AgentTeamsPhaseHostState
}

func NewMemoryAgentTeamsPhaseHostStateStore() *MemoryAgentTeamsPhaseHostStateStore {
	return &MemoryAgentTeamsPhaseHostStateStore{items: make(map[kernel.InvocationID]AgentTeamsPhaseHostState)}
}

func (s *MemoryAgentTeamsPhaseHostStateStore) LoadAgentTeamsPhaseHostState(ctx context.Context, invocationID kernel.InvocationID) (AgentTeamsPhaseHostState, bool, error) {
	if err := ctx.Err(); err != nil {
		return AgentTeamsPhaseHostState{}, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	state, ok := s.items[invocationID]
	return state, ok, nil
}

func (s *MemoryAgentTeamsPhaseHostStateStore) SaveAgentTeamsPhaseHostState(ctx context.Context, state AgentTeamsPhaseHostState) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := kernel.RequireID("invocation_id", state.InvocationID); err != nil {
		return err
	}
	if strings.TrimSpace(state.InvocationRef) == "" {
		return kernel.InvalidArgument("AgentTeams invocation_ref is required")
	}
	if state.Execution.InvocationID == "" || state.Execution.AgentTeamsTaskID == "" || state.Execution.HostRef == "" {
		return kernel.InvalidArgument("AgentTeams execution ref is incomplete")
	}
	if state.Execution.InvocationID != state.InvocationID {
		return kernel.InvalidArgument("AgentTeams execution invocation_id does not match phase host state")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.items == nil {
		s.items = make(map[kernel.InvocationID]AgentTeamsPhaseHostState)
	}
	if existing, ok := s.items[state.InvocationID]; ok {
		if existing.InvocationRef != state.InvocationRef {
			return kernel.IdempotencyConflict()
		}
		if existing.Execution != state.Execution {
			if existing.TerminationMode != adapter.TerminateReleaseWait || state.TerminationMode != "" {
				return kernel.IdempotencyConflict()
			}
			s.items[state.InvocationID] = state
			return nil
		}
		if existing.TerminationMode != "" && state.TerminationMode == "" {
			state.TerminationMode = existing.TerminationMode
		}
	}
	s.items[state.InvocationID] = state
	return nil
}

func clonePreparedInvocation(prepared adapter.PreparedInvocation) adapter.PreparedInvocation {
	prepared.RequiredCapabilities = append([]string(nil), prepared.RequiredCapabilities...)
	return prepared
}

func samePreparedInvocation(left, right adapter.PreparedInvocation) bool {
	left = clonePreparedInvocation(left)
	right = clonePreparedInvocation(right)
	if left.InvocationID != right.InvocationID ||
		left.ProjectID != right.ProjectID ||
		left.Role != right.Role ||
		left.Operation != right.Operation ||
		left.RoomID != right.RoomID ||
		left.Spec != right.Spec ||
		left.RuntimeConfigRef != right.RuntimeConfigRef ||
		left.EnvelopeRef != right.EnvelopeRef {
		return false
	}
	if len(left.RequiredCapabilities) != len(right.RequiredCapabilities) {
		return false
	}
	for index := range left.RequiredCapabilities {
		if left.RequiredCapabilities[index] != right.RequiredCapabilities[index] {
			return false
		}
	}
	return true
}
