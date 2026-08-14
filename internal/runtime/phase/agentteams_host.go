package phase

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	adapter "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/adapters/agentteams"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	baseruntime "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime"
)

const (
	agentTeamsRuntimeConfigRefPrefix = "threadmill://phase-runtime-config/"
	agentTeamsEnvelopeRefPrefix      = "threadmill://phase-envelope/"
	agentTeamsInvocationRefPrefix    = "threadmill://phase-invocation/"
)

type PreparedInvocationWriter interface {
	SavePreparedInvocation(context.Context, string, adapter.PreparedInvocation) error
	LoadPreparedInvocation(context.Context, string) (adapter.PreparedInvocation, error)
}

// AgentTeamsPhaseAdapter is the Phase-only provider seam. FenceExecution is
// intentionally absent from the generic AgentTeams dispatcher used by Task
// Manager and Context Agent invocations.
type AgentTeamsPhaseAdapter interface {
	adapter.AgentTeamsHostAdapter
	FenceExecution(context.Context, adapter.AgentTeamsExecutionRef) error
	SyncExecutionWorkspace(context.Context, adapter.AgentTeamsExecutionRef) (adapter.ExecutionWorkspaceCheckpoint, error)
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
	Adapter AgentTeamsPhaseAdapter
	Writer  PreparedInvocationWriter
	State   AgentTeamsPhaseHostStateStore
	RoomID  string
}

type AgentTeamsPhaseHost struct {
	adapter AgentTeamsPhaseAdapter
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
	if _, err := h.adapter.SyncExecutionWorkspace(ctx, state.Execution); err != nil {
		return err
	}
	state.TerminationMode = adapter.TerminateReleaseWait
	if err := h.state.SaveAgentTeamsPhaseHostState(ctx, state); err != nil {
		return err
	}
	return h.adapter.Terminate(ctx, state.Execution, string(adapter.TerminateReleaseWait))
}

func (h *AgentTeamsPhaseHost) Stop(ctx context.Context, req StopRequest) (StopResult, error) {
	state, err := h.executionState(ctx, req.Invocation.ID)
	if err != nil {
		return StopResult{}, err
	}
	checkpoint, err := h.adapter.SyncExecutionWorkspace(ctx, state.Execution)
	if err != nil {
		return StopResult{}, err
	}
	state.TerminationMode = adapter.TerminateRecoverableStop
	if err := h.state.SaveAgentTeamsPhaseHostState(ctx, state); err != nil {
		return StopResult{}, err
	}
	if err := h.adapter.Terminate(ctx, state.Execution, string(adapter.TerminateRecoverableStop)); err != nil {
		return StopResult{}, err
	}
	return h.stopEvidence(req, state, checkpoint), nil
}

// SyncWorkspace is used by the internal Phase Runtime immediately before it
// asks Controller to refresh the authoritative binding and route output.
func (h *AgentTeamsPhaseHost) SyncWorkspace(ctx context.Context, invocationID kernel.InvocationID) (adapter.ExecutionWorkspaceCheckpoint, error) {
	state, err := h.executionState(ctx, invocationID)
	if err != nil {
		return adapter.ExecutionWorkspaceCheckpoint{}, err
	}
	return h.adapter.SyncExecutionWorkspace(ctx, state.Execution)
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
	if err := h.state.SaveAgentTeamsPhaseHostState(ctx, state); err != nil {
		return err
	}
	return h.adapter.Terminate(ctx, state.Execution, string(mode))
}

// Fence ends only the invocation's Threadmill authority. Production cleanup
// performs destructive MCP deletion and host-slot release after the provider
// has received this call's response and submitted its terminal result.
func (h *AgentTeamsPhaseHost) Fence(ctx context.Context, invocationID kernel.InvocationID) error {
	state, err := h.executionState(ctx, invocationID)
	if err != nil {
		return err
	}
	return h.adapter.FenceExecution(ctx, state.Execution)
}

func (h *AgentTeamsPhaseHost) dispatch(ctx context.Context, req DispatchRequest, rehydrate bool) error {
	if err := validateAgentTeamsDispatch(req); err != nil {
		return err
	}
	if !rehydrate {
		if current, ok, err := h.state.LoadAgentTeamsPhaseHostState(ctx, req.Invocation.ID); err != nil {
			return err
		} else if ok && current.TerminationMode == "" {
			execution, err := h.adapter.Dispatch(ctx, current.InvocationRef)
			if err != nil {
				return err
			}
			if execution != current.Execution {
				return kernel.IdempotencyConflict()
			}
			return nil
		}
	}
	invocationRef, err := agentTeamsInvocationRef(req, rehydrate)
	if err != nil {
		return err
	}
	prepared, err := h.preparedInvocation(req, invocationRef)
	if err != nil {
		return err
	}
	if !rehydrate {
		if persisted, loadErr := h.writer.LoadPreparedInvocation(ctx, invocationRef); loadErr == nil {
			prepared = persisted
		} else if !kernel.IsCode(loadErr, kernel.CodeNotFound) {
			return loadErr
		} else if err := h.writer.SavePreparedInvocation(ctx, invocationRef, prepared); err != nil {
			return err
		}
	} else if err := h.writer.SavePreparedInvocation(ctx, invocationRef, prepared); err != nil {
		return err
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
	return AgentTeamsPhaseHostState{}, kernel.Error{Code: kernel.CodeNotFound, Message: "AgentTeams phase host state not found"}
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

func (h *AgentTeamsPhaseHost) stopEvidence(req StopRequest, state AgentTeamsPhaseHostState, checkpoint adapter.ExecutionWorkspaceCheckpoint) StopResult {
	workspaceRevision := strings.TrimSpace(checkpoint.WorkspaceRevision)
	if workspaceRevision == "" {
		workspaceRevision = strings.TrimSpace(req.Binding.WorkspaceRevision)
	}
	if req.Binding.NonResumable || workspaceRevision == "" {
		return StopResult{NonResumable: true}
	}
	checkpointRef := strings.TrimSpace(req.Binding.CheckpointRef)
	if checkpointRef == "" {
		checkpointRef = deterministicWorkspaceCheckpointRef(req.Invocation.ID, workspaceRevision)
	}
	return StopResult{
		ResumeStateRef:    deterministicResumeStateRef(req.Invocation.ID, state.Execution.AgentTeamsTaskID, checkpointRef),
		CheckpointRef:     checkpointRef,
		WorkspaceRevision: workspaceRevision,
	}
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

func agentTeamsInvocationRef(req DispatchRequest, rehydrate bool) (string, error) {
	if !rehydrate {
		sum := sha256.Sum256([]byte(req.Invocation.ID))
		return agentTeamsInvocationRefPrefix + string(req.Invocation.ID) + "/" + hex.EncodeToString(sum[:16]), nil
	}
	spec, err := agentTeamsPhaseSpec(req)
	if err != nil {
		return "", err
	}
	payload := struct {
		InvocationID      kernel.InvocationID `json:"invocation_id"`
		PromptSHA256      string              `json:"prompt_sha256"`
		Spec              string              `json:"spec"`
		WorkspaceRevision string              `json:"workspace_revision"`
		InputRevision     string              `json:"input_revision"`
	}{
		InvocationID:      req.Invocation.ID,
		PromptSHA256:      req.Prompt.SHA256,
		Spec:              spec,
		WorkspaceRevision: req.Binding.WorkspaceRevision,
		InputRevision:     req.Start.Inputs.InputRevision,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", kernel.InvalidArgument("AgentTeams invocation ref payload cannot be encoded")
	}
	sum := sha256.Sum256(raw)
	return agentTeamsInvocationRefPrefix + string(req.Invocation.ID) + "/" + hex.EncodeToString(sum[:16]), nil
}

func invocationIDFromAgentTeamsRef(invocationRef string) kernel.InvocationID {
	value := strings.TrimPrefix(invocationRef, agentTeamsInvocationRefPrefix)
	if index := strings.IndexByte(value, '/'); index >= 0 {
		value = value[:index]
	}
	return kernel.InvocationID(value)
}

func agentTeamsRuntimeConfigRef(invocationID kernel.InvocationID) string {
	return agentTeamsRuntimeConfigRefPrefix + string(invocationID)
}

func agentTeamsEnvelopeRef(invocationID kernel.InvocationID, invocationRef string, promptHash string) string {
	sum := sha256.Sum256([]byte(string(invocationID) + "\x00" + invocationRef + "\x00" + promptHash))
	return agentTeamsEnvelopeRefPrefix + string(invocationID) + "/" + hex.EncodeToString(sum[:16])
}

func deterministicWorkspaceCheckpointRef(invocationID kernel.InvocationID, workspaceRevision string) string {
	sum := sha256.Sum256([]byte(string(invocationID) + "\x00" + workspaceRevision))
	return "agentteams://workspace-checkpoint/" + string(invocationID) + "/" + hex.EncodeToString(sum[:16])
}

func deterministicResumeStateRef(invocationID kernel.InvocationID, taskID string, checkpointRef string) string {
	sum := sha256.Sum256([]byte(string(invocationID) + "\x00" + taskID + "\x00" + checkpointRef))
	return "agentteams://resume-state/" + string(invocationID) + "/" + hex.EncodeToString(sum[:16])
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
	for existingRef, existing := range w.items {
		if existingRef != ref && existing.InvocationID == prepared.InvocationID {
			delete(w.items, existingRef)
		}
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

type PostgresAgentTeamsPhaseHostStore struct {
	db baseruntime.DBTX
}

func NewPostgresAgentTeamsPhaseHostStore(db baseruntime.DBTX) *PostgresAgentTeamsPhaseHostStore {
	return &PostgresAgentTeamsPhaseHostStore{db: db}
}

func NewPostgresAgentTeamsPhaseHostStoreFromSQL(db baseruntime.SQLDBTX) *PostgresAgentTeamsPhaseHostStore {
	return NewPostgresAgentTeamsPhaseHostStore(baseruntime.WrapSQLDBTX(db))
}

func (s *PostgresAgentTeamsPhaseHostStore) SavePreparedInvocation(ctx context.Context, ref string, prepared adapter.PreparedInvocation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s == nil || s.db == nil {
		return kernel.Error{Code: kernel.CodeInternalError, Message: "Postgres AgentTeams phase host store is not configured"}
	}
	if strings.TrimSpace(ref) == "" {
		return kernel.InvalidArgument("prepared invocation ref is required")
	}
	requiredCapabilities, err := json.Marshal(prepared.RequiredCapabilities)
	if err != nil {
		return kernel.InvalidArgument("prepared required capabilities cannot be encoded")
	}
	// A recoverable pre-dispatch retry may re-render Context. Keep the envelope
	// referenced by the current host state plus the newest candidate, but prune
	// every older failed candidate so a recovery loop cannot grow this table
	// without bound.
	if _, err := s.db.ExecContext(ctx, `
DELETE FROM phase_agentteams_prepared_invocations p
WHERE p.invocation_id = $1
  AND p.invocation_ref <> $2
  AND NOT EXISTS (
    SELECT 1
    FROM phase_agentteams_host_states h
    WHERE h.invocation_id = $1
      AND h.invocation_ref = p.invocation_ref
  )`, prepared.InvocationID, ref); err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx, `
INSERT INTO phase_agentteams_prepared_invocations (
  invocation_ref, invocation_id, project_id, role, operation, room_id, spec,
  runtime_config_ref, envelope_ref, required_capabilities
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb)
ON CONFLICT (invocation_ref) DO NOTHING`,
		ref,
		prepared.InvocationID,
		prepared.ProjectID,
		prepared.Role,
		prepared.Operation,
		prepared.RoomID,
		prepared.Spec,
		prepared.RuntimeConfigRef,
		prepared.EnvelopeRef,
		string(requiredCapabilities),
	)
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err == nil && affected == 1 {
		return nil
	}
	existing, err := s.loadPrepared(ctx, ref)
	if err != nil {
		return err
	}
	if !samePreparedInvocation(existing, prepared) {
		return kernel.IdempotencyConflict()
	}
	return nil
}

func (s *PostgresAgentTeamsPhaseHostStore) LoadPreparedInvocation(ctx context.Context, ref string) (adapter.PreparedInvocation, error) {
	if err := ctx.Err(); err != nil {
		return adapter.PreparedInvocation{}, err
	}
	if s == nil || s.db == nil {
		return adapter.PreparedInvocation{}, kernel.Error{Code: kernel.CodeInternalError, Message: "Postgres AgentTeams phase host store is not configured"}
	}
	return s.loadPrepared(ctx, ref)
}

func (s *PostgresAgentTeamsPhaseHostStore) LoadAgentTeamsPhaseHostState(ctx context.Context, invocationID kernel.InvocationID) (AgentTeamsPhaseHostState, bool, error) {
	if err := ctx.Err(); err != nil {
		return AgentTeamsPhaseHostState{}, false, err
	}
	if s == nil || s.db == nil {
		return AgentTeamsPhaseHostState{}, false, kernel.Error{Code: kernel.CodeInternalError, Message: "Postgres AgentTeams phase host store is not configured"}
	}
	if err := kernel.RequireID("invocation_id", invocationID); err != nil {
		return AgentTeamsPhaseHostState{}, false, err
	}
	return s.loadState(ctx, invocationID)
}

func (s *PostgresAgentTeamsPhaseHostStore) SaveAgentTeamsPhaseHostState(ctx context.Context, state AgentTeamsPhaseHostState) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s == nil || s.db == nil {
		return kernel.Error{Code: kernel.CodeInternalError, Message: "Postgres AgentTeams phase host store is not configured"}
	}
	if err := validateAgentTeamsPhaseHostState(state); err != nil {
		return err
	}
	existing, found, err := s.loadState(ctx, state.InvocationID)
	if err != nil {
		return err
	}
	if !found {
		result, err := s.db.ExecContext(ctx, `
INSERT INTO phase_agentteams_host_states (
  invocation_id, invocation_ref, agentteams_task_id, host_ref, termination_mode
) VALUES ($1, $2, $3, $4, NULLIF($5, ''))
ON CONFLICT (invocation_id) DO NOTHING`,
			state.InvocationID,
			state.InvocationRef,
			state.Execution.AgentTeamsTaskID,
			state.Execution.HostRef,
			string(state.TerminationMode),
		)
		if err != nil {
			return err
		}
		if affected, err := result.RowsAffected(); err == nil && affected == 1 {
			return nil
		}
		existing, found, err = s.loadState(ctx, state.InvocationID)
		if err != nil {
			return err
		}
		if !found {
			return kernel.Error{Code: kernel.CodeRevisionConflict, Message: "AgentTeams phase host state was not inserted", Recoverable: true}
		}
	}
	next, ok, err := nextAgentTeamsPhaseHostState(existing, state)
	if err != nil || !ok {
		return err
	}
	result, err := s.db.ExecContext(ctx, `
UPDATE phase_agentteams_host_states
SET invocation_ref = $2,
    agentteams_task_id = $3,
    host_ref = $4,
    termination_mode = NULLIF($5, ''),
    updated_at = now()
WHERE invocation_id = $1
  AND invocation_ref = $6
  AND agentteams_task_id = $7
  AND host_ref = $8
  AND COALESCE(termination_mode::text, '') = $9`,
		next.InvocationID,
		next.InvocationRef,
		next.Execution.AgentTeamsTaskID,
		next.Execution.HostRef,
		string(next.TerminationMode),
		existing.InvocationRef,
		existing.Execution.AgentTeamsTaskID,
		existing.Execution.HostRef,
		string(existing.TerminationMode),
	)
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err == nil && affected != 1 {
		return kernel.Error{Code: kernel.CodeRevisionConflict, Message: "AgentTeams phase host state changed", Recoverable: true}
	}
	return nil
}

func (s *PostgresAgentTeamsPhaseHostStore) loadPrepared(ctx context.Context, ref string) (adapter.PreparedInvocation, error) {
	var prepared adapter.PreparedInvocation
	var requiredCapabilities []byte
	err := s.db.QueryRowContext(ctx, `
SELECT invocation_id, project_id, role, operation, room_id, spec, runtime_config_ref,
       envelope_ref, required_capabilities
FROM phase_agentteams_prepared_invocations
WHERE invocation_ref = $1`, ref).Scan(
		&prepared.InvocationID,
		&prepared.ProjectID,
		&prepared.Role,
		&prepared.Operation,
		&prepared.RoomID,
		&prepared.Spec,
		&prepared.RuntimeConfigRef,
		&prepared.EnvelopeRef,
		&requiredCapabilities,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return adapter.PreparedInvocation{}, kernel.Error{Code: kernel.CodeNotFound, Message: "prepared invocation not found"}
	}
	if err != nil {
		return adapter.PreparedInvocation{}, err
	}
	if err := json.Unmarshal(requiredCapabilities, &prepared.RequiredCapabilities); err != nil {
		return adapter.PreparedInvocation{}, fmt.Errorf("decode prepared invocation capabilities: %w", err)
	}
	return prepared, nil
}

func (s *PostgresAgentTeamsPhaseHostStore) loadState(ctx context.Context, invocationID kernel.InvocationID) (AgentTeamsPhaseHostState, bool, error) {
	var state AgentTeamsPhaseHostState
	var terminationMode string
	err := s.db.QueryRowContext(ctx, `
SELECT invocation_id, invocation_ref, agentteams_task_id, host_ref, COALESCE(termination_mode::text, '')
FROM phase_agentteams_host_states
WHERE invocation_id = $1`, invocationID).Scan(
		&state.InvocationID,
		&state.InvocationRef,
		&state.Execution.AgentTeamsTaskID,
		&state.Execution.HostRef,
		&terminationMode,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return AgentTeamsPhaseHostState{}, false, nil
	}
	if err != nil {
		return AgentTeamsPhaseHostState{}, false, err
	}
	state.Execution.InvocationID = state.InvocationID
	state.TerminationMode = adapter.TerminateMode(terminationMode)
	return state, true, nil
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
	if err := validateAgentTeamsPhaseHostState(state); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.items == nil {
		s.items = make(map[kernel.InvocationID]AgentTeamsPhaseHostState)
	}
	if existing, ok := s.items[state.InvocationID]; ok {
		next, ok, err := nextAgentTeamsPhaseHostState(existing, state)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		s.items[state.InvocationID] = next
		return nil
	}
	s.items[state.InvocationID] = state
	return nil
}

func validateAgentTeamsPhaseHostState(state AgentTeamsPhaseHostState) error {
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
	switch state.TerminationMode {
	case "", adapter.TerminateReleaseWait, adapter.TerminateRecoverableStop, adapter.TerminateCancel:
		return nil
	default:
		return kernel.InvalidArgument("AgentTeams termination mode is invalid")
	}
}

func nextAgentTeamsPhaseHostState(existing, requested AgentTeamsPhaseHostState) (AgentTeamsPhaseHostState, bool, error) {
	if existing.InvocationID != requested.InvocationID {
		return AgentTeamsPhaseHostState{}, false, kernel.IdempotencyConflict()
	}
	if existing.InvocationRef != requested.InvocationRef || existing.Execution != requested.Execution {
		if existing.TerminationMode != adapter.TerminateReleaseWait || requested.TerminationMode != "" {
			return AgentTeamsPhaseHostState{}, false, kernel.IdempotencyConflict()
		}
		return requested, true, nil
	}
	if existing.TerminationMode != "" && requested.TerminationMode == "" {
		requested.TerminationMode = existing.TerminationMode
	}
	if existing.TerminationMode != "" && requested.TerminationMode != existing.TerminationMode {
		return AgentTeamsPhaseHostState{}, false, kernel.IdempotencyConflict()
	}
	if existing == requested {
		return existing, false, nil
	}
	return requested, true, nil
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
