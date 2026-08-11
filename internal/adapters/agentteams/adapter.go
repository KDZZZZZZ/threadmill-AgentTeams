package agentteams

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

type TerminateMode string

const (
	TerminateReleaseWait     TerminateMode = "release_wait"
	TerminateRecoverableStop TerminateMode = "recoverable_stop"
	TerminateCancel          TerminateMode = "cancel"
)

type AgentTeamsExecutionRef struct {
	InvocationID     kernel.InvocationID `json:"invocation_id"`
	AgentTeamsTaskID string              `json:"agentteams_task_id"`
	HostRef          string              `json:"host_ref"`
}

type UntrustedExecutionResult struct {
	AgentTeamsTaskID string   `json:"agentteams_task_id"`
	TaskStatus       string   `json:"task_status"`
	ResultStatus     string   `json:"result_status"`
	Summary          string   `json:"summary"`
	Deliverables     []string `json:"deliverables"`
	ResultDocument   []byte   `json:"result_document"`
	Effective        bool     `json:"effective"`
	ValidationErrors []string `json:"validation_errors"`
}

type ExecutionObservation struct {
	ID               string              `json:"id"`
	Cursor           string              `json:"cursor"`
	InvocationID     kernel.InvocationID `json:"invocation_id,omitempty"`
	AgentTeamsTaskID string              `json:"agentteams_task_id,omitempty"`
	HostRef          string              `json:"host_ref"`
	Kind             string              `json:"kind"`
	Payload          map[string]string   `json:"payload,omitempty"`
	ObservedAt       time.Time           `json:"observed_at"`
}

// AgentTeamsHostAdapter is internal to Agent Runtime. No Agent capability or
// MCP registry may expose it directly.
type AgentTeamsHostAdapter interface {
	Dispatch(context.Context, string) (AgentTeamsExecutionRef, error)
	Terminate(context.Context, AgentTeamsExecutionRef, string) error
	Collect(context.Context, AgentTeamsExecutionRef) (UntrustedExecutionResult, error)
	Observe(context.Context, string) ([]ExecutionObservation, error)
}

type executionState string

const (
	executionReserved   executionState = "reserved"
	executionDispatched executionState = "dispatched"
	executionTerminated executionState = "terminated"
)

type executionRecord struct {
	InvocationRef   string
	Attempt         int
	Execution       AgentTeamsExecutionRef
	Fingerprint     string
	State           executionState
	TerminationMode TerminateMode
}

type ExecutionStore interface {
	GetByInvocationRef(context.Context, string) (executionRecord, bool, error)
	GetByTaskID(context.Context, string) (executionRecord, bool, error)
	Reserve(context.Context, string, string, AgentTeamsExecutionRef) (executionRecord, bool, error)
	MarkDispatched(context.Context, string) error
	MarkTerminated(context.Context, string, TerminateMode) error
}

type MemoryExecutionStore struct {
	mu           sync.RWMutex
	byInvocation map[string]executionRecord
	byTaskID     map[string]executionRecord
}

func NewMemoryExecutionStore() *MemoryExecutionStore {
	return &MemoryExecutionStore{
		byInvocation: make(map[string]executionRecord),
		byTaskID:     make(map[string]executionRecord),
	}
}

func (s *MemoryExecutionStore) GetByInvocationRef(ctx context.Context, ref string) (executionRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return executionRecord{}, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	record, ok := s.byInvocation[ref]
	return record, ok, nil
}

func (s *MemoryExecutionStore) GetByTaskID(ctx context.Context, taskID string) (executionRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return executionRecord{}, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	record, ok := s.byTaskID[taskID]
	return record, ok, nil
}

func (s *MemoryExecutionStore) Reserve(
	ctx context.Context,
	invocationRef string,
	fingerprint string,
	execution AgentTeamsExecutionRef,
) (executionRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return executionRecord{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.byInvocation[invocationRef]; ok {
		if existing.Fingerprint != fingerprint {
			return executionRecord{}, false, kernel.IdempotencyConflict()
		}
		if existing.State != executionTerminated || existing.TerminationMode != TerminateReleaseWait {
			return existing, false, nil
		}
		execution.AgentTeamsTaskID = attemptedTaskID(invocationRef, existing.Attempt+1)
		execution.HostRef = strings.TrimSpace(execution.HostRef)
		if execution.HostRef == "" {
			return executionRecord{}, false, kernel.InvalidArgument("AgentTeams host_ref is required")
		}
		record := executionRecord{
			InvocationRef: invocationRef,
			Attempt:       existing.Attempt + 1,
			Execution:     execution,
			Fingerprint:   fingerprint,
			State:         executionReserved,
		}
		if _, ok := s.byTaskID[execution.AgentTeamsTaskID]; ok {
			return executionRecord{}, false, kernel.IdempotencyConflict()
		}
		s.byInvocation[invocationRef] = record
		s.byTaskID[execution.AgentTeamsTaskID] = record
		return record, true, nil
	}
	execution.AgentTeamsTaskID = attemptedTaskID(invocationRef, 1)
	execution.HostRef = strings.TrimSpace(execution.HostRef)
	if execution.HostRef == "" {
		return executionRecord{}, false, kernel.InvalidArgument("AgentTeams host_ref is required")
	}
	if existing, ok := s.byTaskID[execution.AgentTeamsTaskID]; ok && existing.InvocationRef != invocationRef {
		return executionRecord{}, false, kernel.IdempotencyConflict()
	}
	record := executionRecord{
		InvocationRef: invocationRef,
		Attempt:       1,
		Execution:     execution,
		Fingerprint:   fingerprint,
		State:         executionReserved,
	}
	s.byInvocation[invocationRef] = record
	s.byTaskID[execution.AgentTeamsTaskID] = record
	return record, true, nil
}

func (s *MemoryExecutionStore) MarkDispatched(ctx context.Context, taskID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.byTaskID[taskID]
	if !ok {
		return kernel.Error{Code: kernel.CodeNotFound, Message: "AgentTeams execution reservation not found"}
	}
	if record.State == executionTerminated {
		return kernel.Error{Code: kernel.CodeStaleCommand, Message: "terminated AgentTeams execution cannot be dispatched", Recoverable: true}
	}
	if record.State == executionDispatched {
		return nil
	}
	record.State = executionDispatched
	s.byTaskID[taskID] = record
	if latest, ok := s.byInvocation[record.InvocationRef]; ok && latest.Execution.AgentTeamsTaskID == taskID {
		s.byInvocation[record.InvocationRef] = record
	}
	return nil
}

func (s *MemoryExecutionStore) MarkTerminated(ctx context.Context, taskID string, mode TerminateMode) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.byTaskID[taskID]
	if !ok {
		return kernel.Error{Code: kernel.CodeNotFound, Message: "AgentTeams execution not found"}
	}
	if record.State == executionTerminated {
		if record.TerminationMode == mode {
			return nil
		}
		return kernel.IdempotencyConflict()
	}
	record.State = executionTerminated
	record.TerminationMode = mode
	if latest, ok := s.byInvocation[record.InvocationRef]; ok && latest.Execution.AgentTeamsTaskID == taskID {
		s.byInvocation[record.InvocationRef] = record
	}
	s.byTaskID[taskID] = record
	return nil
}

type Adapter struct {
	client          Client
	source          InvocationSource
	files           FileTransport
	store           ExecutionStore
	now             func() time.Time
	heartbeatMaxAge time.Duration
	projector       ObservationProjector
	dispatchMu      sync.Mutex
	dispatches      map[string]*dispatchCall
}

type dispatchCall struct {
	done   chan struct{}
	result AgentTeamsExecutionRef
	err    error
}

func NewAdapter(
	client Client,
	source InvocationSource,
	files FileTransport,
	store ExecutionStore,
	now func() time.Time,
	heartbeatMaxAge time.Duration,
) (*Adapter, error) {
	if client == nil || source == nil || files == nil || store == nil {
		return nil, kernel.InvalidArgument("AgentTeams adapter dependencies are required")
	}
	if now == nil {
		now = time.Now
	}
	if heartbeatMaxAge <= 0 {
		heartbeatMaxAge = 30 * time.Second
	}
	return &Adapter{
		client:          client,
		source:          source,
		files:           files,
		store:           store,
		now:             now,
		heartbeatMaxAge: heartbeatMaxAge,
		dispatches:      make(map[string]*dispatchCall),
	}, nil
}

func (a *Adapter) Dispatch(ctx context.Context, invocationRef string) (AgentTeamsExecutionRef, error) {
	if strings.TrimSpace(invocationRef) == "" {
		return AgentTeamsExecutionRef{}, kernel.InvalidArgument("invocation_ref is required")
	}
	return a.dispatchOnce(ctx, invocationRef, func() (AgentTeamsExecutionRef, error) {
		return a.dispatchReserved(ctx, invocationRef)
	})
}

func (a *Adapter) dispatchOnce(
	ctx context.Context,
	invocationRef string,
	fn func() (AgentTeamsExecutionRef, error),
) (AgentTeamsExecutionRef, error) {
	// MVP process-local singleflight only coalesces concurrent Dispatch calls in
	// this adapter instance. Cross-process or post-restart retries rely on
	// stable task IDs plus the AgentTeams taskflow idempotency contract; this is
	// not a global exactly-once fence for external calls.
	a.dispatchMu.Lock()
	if call, ok := a.dispatches[invocationRef]; ok {
		done := call.done
		a.dispatchMu.Unlock()
		select {
		case <-done:
			return call.result, call.err
		case <-ctx.Done():
			return AgentTeamsExecutionRef{}, ctx.Err()
		}
	}
	call := &dispatchCall{done: make(chan struct{})}
	a.dispatches[invocationRef] = call
	a.dispatchMu.Unlock()

	call.result, call.err = fn()
	close(call.done)

	a.dispatchMu.Lock()
	delete(a.dispatches, invocationRef)
	a.dispatchMu.Unlock()
	return call.result, call.err
}

func (a *Adapter) dispatchReserved(ctx context.Context, invocationRef string) (AgentTeamsExecutionRef, error) {
	prepared, err := a.source.LoadPreparedInvocation(ctx, invocationRef)
	if err != nil {
		return AgentTeamsExecutionRef{}, err
	}
	if err := validatePreparedInvocation(prepared); err != nil {
		return AgentTeamsExecutionRef{}, err
	}
	fingerprint, err := preparedFingerprint(prepared)
	if err != nil {
		return AgentTeamsExecutionRef{}, err
	}

	record, found, err := a.store.GetByInvocationRef(ctx, invocationRef)
	if err != nil {
		return AgentTeamsExecutionRef{}, err
	}
	if found {
		if record.Fingerprint != fingerprint {
			return AgentTeamsExecutionRef{}, kernel.IdempotencyConflict()
		}
		if record.State == executionDispatched {
			return record.Execution, nil
		}
		if record.State == executionTerminated {
			if record.TerminationMode != TerminateReleaseWait {
				return AgentTeamsExecutionRef{}, kernel.Error{Code: kernel.CodeStaleCommand, Message: "terminated AgentTeams execution cannot be redispatched", Recoverable: true}
			}
			host, err := a.selectHost(ctx, prepared)
			if err != nil {
				return AgentTeamsExecutionRef{}, err
			}
			proposed := AgentTeamsExecutionRef{
				InvocationID: prepared.InvocationID,
				HostRef:      host.Ref,
			}
			record, _, err = a.store.Reserve(ctx, invocationRef, fingerprint, proposed)
			if err != nil {
				return AgentTeamsExecutionRef{}, err
			}
			if record.State == executionDispatched {
				return record.Execution, nil
			}
		}
	} else {
		host, err := a.selectHost(ctx, prepared)
		if err != nil {
			return AgentTeamsExecutionRef{}, err
		}
		proposed := AgentTeamsExecutionRef{
			InvocationID: prepared.InvocationID,
			HostRef:      host.Ref,
		}
		record, _, err = a.store.Reserve(ctx, invocationRef, fingerprint, proposed)
		if err != nil {
			return AgentTeamsExecutionRef{}, err
		}
		if record.State == executionDispatched {
			return record.Execution, nil
		}
	}

	if err := a.client.PrepareHost(ctx, HostPreparation{
		HostRef:          record.Execution.HostRef,
		InvocationID:     prepared.InvocationID,
		Role:             prepared.Role,
		Operation:        prepared.Operation,
		RuntimeConfigRef: prepared.RuntimeConfigRef,
		EnvelopeRef:      prepared.EnvelopeRef,
	}); err != nil {
		return AgentTeamsExecutionRef{}, err
	}
	task, err := a.client.DelegateTask(ctx, DelegateTaskRequest{
		ProjectID: prepared.ProjectID,
		TaskID:    record.Execution.AgentTeamsTaskID,
		HostRef:   record.Execution.HostRef,
		RoomID:    prepared.RoomID,
		Spec:      prepared.Spec,
	})
	if err != nil {
		return AgentTeamsExecutionRef{}, err
	}
	if task.TaskID != record.Execution.AgentTeamsTaskID || task.HostRef != record.Execution.HostRef {
		identityErr := kernel.Error{Code: kernel.CodeInternalError, Message: "AgentTeams returned a mismatched execution identity"}
		return AgentTeamsExecutionRef{}, errors.Join(identityErr, a.fenceAndRelease(context.Background(), record.Execution))
	}
	if err := a.store.MarkDispatched(ctx, record.Execution.AgentTeamsTaskID); err != nil {
		return AgentTeamsExecutionRef{}, err
	}
	return record.Execution, nil
}

func (a *Adapter) Terminate(ctx context.Context, execution AgentTeamsExecutionRef, rawMode string) error {
	mode := TerminateMode(rawMode)
	if !validTerminateMode(mode) {
		return kernel.InvalidArgument("termination mode must be release_wait, recoverable_stop, or cancel")
	}
	record, ok, err := a.store.GetByTaskID(ctx, execution.AgentTeamsTaskID)
	if err != nil {
		return err
	}
	if !ok || !reflect.DeepEqual(record.Execution, execution) {
		return kernel.Error{Code: kernel.CodeNotFound, Message: "AgentTeams execution not found"}
	}
	if record.State == executionTerminated {
		if record.TerminationMode == mode {
			return nil
		}
		return kernel.IdempotencyConflict()
	}
	if err := a.client.RevokeInvocation(ctx, execution.HostRef, execution.InvocationID); err != nil {
		forceErr := a.client.ForceStopHost(ctx, execution.HostRef)
		var releaseErr error
		if forceErr == nil {
			releaseErr = a.client.ReleaseHostSlot(context.Background(), execution.AgentTeamsTaskID, execution.HostRef)
		}
		return errors.Join(err, forceErr, releaseErr)
	}
	if err := a.client.CancelTask(ctx, execution.AgentTeamsTaskID, terminationReason(mode)); err != nil {
		return err
	}
	if err := a.store.MarkTerminated(ctx, execution.AgentTeamsTaskID, mode); err != nil {
		_ = a.client.ReleaseHostSlot(context.Background(), execution.AgentTeamsTaskID, execution.HostRef)
		return err
	}
	return a.client.ReleaseHostSlot(ctx, execution.AgentTeamsTaskID, execution.HostRef)
}

func (a *Adapter) Collect(ctx context.Context, execution AgentTeamsExecutionRef) (UntrustedExecutionResult, error) {
	record, ok, err := a.store.GetByTaskID(ctx, execution.AgentTeamsTaskID)
	if err != nil {
		return UntrustedExecutionResult{}, err
	}
	if !ok || !reflect.DeepEqual(record.Execution, execution) {
		return UntrustedExecutionResult{}, kernel.Error{Code: kernel.CodeNotFound, Message: "AgentTeams execution not found"}
	}
	if err := a.files.PullExecution(ctx, execution.AgentTeamsTaskID); err != nil {
		return UntrustedExecutionResult{}, err
	}
	check, err := a.client.CheckTask(ctx, execution.AgentTeamsTaskID)
	if err != nil {
		return UntrustedExecutionResult{}, err
	}
	if isTerminalTaskStatus(check.Task.Status) {
		if err := a.fenceAndRelease(ctx, execution); err != nil {
			return UntrustedExecutionResult{}, err
		}
	}
	document, err := a.files.ReadResult(ctx, execution.AgentTeamsTaskID)
	if err != nil {
		return UntrustedExecutionResult{}, err
	}
	return UntrustedExecutionResult{
		AgentTeamsTaskID: execution.AgentTeamsTaskID,
		TaskStatus:       check.Task.Status,
		ResultStatus:     check.ResultStatus,
		Summary:          check.Summary,
		Deliverables:     append([]string(nil), check.Deliverables...),
		ResultDocument:   append([]byte(nil), document...),
		Effective:        check.Effective,
		ValidationErrors: append([]string(nil), check.ValidationErrors...),
	}, nil
}

func (a *Adapter) fenceAndRelease(ctx context.Context, execution AgentTeamsExecutionRef) error {
	if err := a.client.RevokeInvocation(ctx, execution.HostRef, execution.InvocationID); err != nil {
		if forceErr := a.client.ForceStopHost(ctx, execution.HostRef); forceErr != nil {
			return errors.Join(err, forceErr)
		}
	}
	return a.client.ReleaseHostSlot(ctx, execution.AgentTeamsTaskID, execution.HostRef)
}

func (a *Adapter) Observe(ctx context.Context, cursor string) ([]ExecutionObservation, error) {
	raw, err := a.client.ReadObservations(ctx, cursor)
	if err != nil {
		return nil, err
	}
	projected, err := a.projector.Project(ctx, raw, a.store)
	if err != nil {
		return nil, err
	}
	observations := projected.Observations
	if projected.IgnoredTaskEvents > 0 && needsCursorAdvance(observations, projected.NextCursor) {
		observations = append(observations, cursorAdvanceObservation(projected))
	}
	return observations, nil
}

func (a *Adapter) selectHost(ctx context.Context, prepared PreparedInvocation) (HostStatus, error) {
	hosts, err := a.client.ListHosts(ctx)
	if err != nil {
		return HostStatus{}, err
	}
	wantKind, err := hostKindForRole(prepared.Role)
	if err != nil {
		return HostStatus{}, err
	}
	now := a.now()
	candidates := make([]HostStatus, 0, len(hosts))
	for _, host := range hosts {
		if host.Kind != wantKind || host.Phase != "Running" || host.Capacity <= host.ActiveExecutions {
			continue
		}
		if host.LastHeartbeat.IsZero() || now.Sub(host.LastHeartbeat) > a.heartbeatMaxAge {
			continue
		}
		if !containsAll(host.Capabilities, prepared.RequiredCapabilities) {
			continue
		}
		candidates = append(candidates, host)
	}
	if len(candidates) == 0 {
		return HostStatus{}, kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "no healthy AgentTeams host has matching capacity", Recoverable: true}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].ActiveExecutions != candidates[j].ActiveExecutions {
			return candidates[i].ActiveExecutions < candidates[j].ActiveExecutions
		}
		return candidates[i].Ref < candidates[j].Ref
	})
	return candidates[0], nil
}

func validatePreparedInvocation(prepared PreparedInvocation) error {
	if err := kernel.RequireID("invocation_id", prepared.InvocationID); err != nil {
		return err
	}
	if err := kernel.RequireID("project_id", prepared.ProjectID); err != nil {
		return err
	}
	if _, err := hostKindForRole(prepared.Role); err != nil {
		return err
	}
	if prepared.Role == auth.RoleContext {
		switch prepared.Operation {
		case "retrieve", "curate", "review":
		default:
			return kernel.InvalidArgument("Context invocation operation is required")
		}
	} else if prepared.Operation != "" {
		return kernel.InvalidArgument("operation is only valid for Context Agent")
	}
	if strings.TrimSpace(prepared.RoomID) == "" || strings.TrimSpace(prepared.Spec) == "" {
		return kernel.InvalidArgument("AgentTeams room and execution spec are required")
	}
	if strings.TrimSpace(prepared.RuntimeConfigRef) == "" || strings.TrimSpace(prepared.EnvelopeRef) == "" {
		return kernel.InvalidArgument("Runtime config and signed envelope references are required")
	}
	return nil
}

func preparedFingerprint(prepared PreparedInvocation) (string, error) {
	canonical := prepared
	canonical.RequiredCapabilities = append([]string(nil), prepared.RequiredCapabilities...)
	sort.Strings(canonical.RequiredCapabilities)
	payload, err := json.Marshal(canonical)
	if err != nil {
		return "", kernel.InvalidArgument("prepared invocation cannot be canonicalized")
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func hostKindForRole(role auth.Role) (HostKind, error) {
	switch role {
	case auth.RoleTaskManager, auth.RoleContext:
		return HostManager, nil
	case auth.RolePlanner, auth.RoleExecutor, auth.RoleVerifier:
		return HostWorker, nil
	default:
		return "", kernel.Forbidden("role cannot run on AgentTeams host")
	}
}

func containsAll(values, required []string) bool {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	for _, value := range required {
		if _, ok := set[value]; !ok {
			return false
		}
	}
	return true
}

func validTerminateMode(mode TerminateMode) bool {
	return mode == TerminateReleaseWait || mode == TerminateRecoverableStop || mode == TerminateCancel
}

func terminationReason(mode TerminateMode) string {
	return "threadmill:" + string(mode)
}

func isTerminalTaskStatus(status string) bool {
	switch status {
	case "submitted", "succeeded", "success", "failed", "cancelled", "canceled":
		return true
	default:
		return false
	}
}

func needsCursorAdvance(observations []ExecutionObservation, nextCursor string) bool {
	if nextCursor == "" || len(observations) == 0 {
		return nextCursor != ""
	}
	return observations[len(observations)-1].Cursor != nextCursor
}
