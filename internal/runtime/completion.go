package runtime

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/artifacts"
	phasemcp "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/mcp/phase"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/phaseagent"
)

var (
	ErrCompletionConflict = errors.New("phase completion state changed concurrently")
	ErrConflictingOutput  = errors.New("a different phase output is already accepted")
	ErrStaleCompletion    = errors.New("phase output does not match the current execution")
)

type PhaseOutputKey struct {
	TaskID, InvocationID string
	Generation           int
}

// PhaseOutputRecord is the authoritative logical completion record. Physical
// epoch is retained only as acceptance evidence and is not part of PhaseOutput.
type PhaseOutputRecord struct {
	Key            PhaseOutputKey
	BindingRef     string
	InputRevision  string
	ExecutionEpoch ExecutionEpoch
	Output         phaseagent.PhaseOutput
	EventRecorded  bool
	Revision       int64
	AcceptedAt     time.Time
}

type PhaseOutputStore interface {
	PutIfAbsent(context.Context, PhaseOutputRecord) (PhaseOutputRecord, bool, error)
	Get(context.Context, PhaseOutputKey) (PhaseOutputRecord, bool, error)
	CompareAndSwap(context.Context, PhaseOutputKey, int64, PhaseOutputRecord) (PhaseOutputRecord, bool, error)
}

type InMemoryPhaseOutputStore struct {
	mu      sync.RWMutex
	records map[PhaseOutputKey]PhaseOutputRecord
}

func NewInMemoryPhaseOutputStore() *InMemoryPhaseOutputStore {
	return &InMemoryPhaseOutputStore{records: make(map[PhaseOutputKey]PhaseOutputRecord)}
}

func (s *InMemoryPhaseOutputStore) PutIfAbsent(_ context.Context, value PhaseOutputRecord) (PhaseOutputRecord, bool, error) {
	if value.Key.TaskID == "" || value.Key.InvocationID == "" || value.Key.Generation <= 0 || value.BindingRef == "" || value.InputRevision == "" || value.ExecutionEpoch <= 0 {
		return PhaseOutputRecord{}, false, errors.New("complete phase output authority is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if current, ok := s.records[value.Key]; ok {
		if current.BindingRef == value.BindingRef && current.InputRevision == value.InputRevision && current.ExecutionEpoch == value.ExecutionEpoch && reflect.DeepEqual(current.Output, value.Output) {
			return copyPhaseOutputRecord(current), false, nil
		}
		return copyPhaseOutputRecord(current), false, ErrConflictingOutput
	}
	value.Revision = 1
	value.AcceptedAt = time.Now().UTC()
	value = copyPhaseOutputRecord(value)
	s.records[value.Key] = value
	return copyPhaseOutputRecord(value), true, nil
}

func (s *InMemoryPhaseOutputStore) Get(_ context.Context, key PhaseOutputKey) (PhaseOutputRecord, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.records[key]
	return copyPhaseOutputRecord(value), ok, nil
}

func (s *InMemoryPhaseOutputStore) CompareAndSwap(_ context.Context, key PhaseOutputKey, expected int64, value PhaseOutputRecord) (PhaseOutputRecord, bool, error) {
	if value.Key != key {
		return PhaseOutputRecord{}, false, errors.New("phase output key cannot change")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.records[key]
	if !ok || current.Revision != expected {
		return copyPhaseOutputRecord(current), false, nil
	}
	value.Revision = current.Revision + 1
	value.AcceptedAt = current.AcceptedAt
	value = copyPhaseOutputRecord(value)
	s.records[key] = value
	return copyPhaseOutputRecord(value), true, nil
}

func copyPhaseOutputRecord(value PhaseOutputRecord) PhaseOutputRecord {
	value.Output.DeliveryRefs = append([]string(nil), value.Output.DeliveryRefs...)
	value.Output.EvidenceRefs = append([]string(nil), value.Output.EvidenceRefs...)
	return value
}

// CompletedTeamHarnessTaskCleaner is deliberately distinct from rollback
// cancellation even when both use the same underlying taskflow primitive.
type CompletedTeamHarnessTaskCleaner interface {
	CompleteTeamHarnessTask(context.Context, TeamHarnessTask) error
}

// TransientExecutionReleaser removes non-durable session/carrier resources
// after the Worker and its credentials have been reclaimed.
type TransientExecutionReleaser interface {
	ReleaseTransientExecution(context.Context, PhysicalExecution) error
}

// PhaseOutputCompletionCoordinator is the production Runtime implementation
// for one trusted rehydrated invocation. It accepts formal output, fences the
// logical invocation, and performs retryable normal carrier teardown.
type PhaseOutputCompletionCoordinator struct {
	Binding             phasemcp.InvocationBinding
	Delegate            phaseagent.Runtime
	Outputs             PhaseOutputStore
	Mutations           LifecycleMutationStore
	Waiting             WaitingStore
	PhysicalExecutions  PhysicalExecutionStore
	Events              artifacts.EventRecorder
	Tasks               CompletedTeamHarnessTaskCleaner
	Workers             WorkerProvisioner
	MCP                 MCPClientCleaner
	Credentials         MCPCredentialProvisioner
	Bindings            *phasemcp.BindingRegistry
	Leases              WorkspaceLeaseAcquirer
	TransientExecutions TransientExecutionReleaser
	PollInterval        time.Duration

	mu sync.Mutex
}

func (*PhaseOutputCompletionCoordinator) RecordsPhaseOutputSubmitted() bool { return true }

func (c *PhaseOutputCompletionCoordinator) SubmitPhaseOutput(ctx context.Context, output phaseagent.PhaseOutput) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.validate(); err != nil {
		return err
	}
	outputKey := PhaseOutputKey{TaskID: c.Binding.TaskID, InvocationID: c.Binding.InvocationID, Generation: c.Binding.Generation}
	existing, found, err := c.Outputs.Get(ctx, outputKey)
	if err != nil {
		return err
	}
	if found {
		if existing.BindingRef != c.Binding.BindingRef || existing.InputRevision != c.Binding.InputRevision || existing.ExecutionEpoch != ExecutionEpoch(c.Binding.ExecutionEpoch) || !reflect.DeepEqual(existing.Output, output) {
			return ErrConflictingOutput
		}
	} else if err := c.validateCurrentExecution(ctx); err != nil {
		return err
	}
	candidate := PhaseOutputRecord{
		Key:        outputKey,
		BindingRef: c.Binding.BindingRef, InputRevision: c.Binding.InputRevision,
		ExecutionEpoch: ExecutionEpoch(c.Binding.ExecutionEpoch), Output: output,
	}
	if c.Mutations != nil {
		waiting, found, getErr := c.Waiting.Get(ctx, WaitingKey{TaskID: c.Binding.TaskID, InvocationID: c.Binding.InvocationID, Generation: c.Binding.Generation})
		if getErr != nil || !found {
			return ErrStaleCompletion
		}
		_, _, _, err := c.Mutations.AcceptPhaseOutput(ctx, candidate, waiting.Key, waiting.Revision)
		if err != nil {
			return err
		}
		return c.finalize(ctx)
	}
	record, _, err := c.Outputs.PutIfAbsent(ctx, candidate)
	if err != nil {
		return err
	}
	if !record.EventRecorded {
		if c.Events != nil {
			if err := c.Events.Record(ctx, artifacts.Event{Type: artifacts.EventPhaseOutputSubmitted, TaskID: c.Binding.TaskID, InvocationID: c.Binding.InvocationID, ArtifactRefs: completionOutputRefs(output)}); err != nil {
				return err
			}
		}
		record.EventRecorded = true
		var swapped bool
		record, swapped, err = c.Outputs.CompareAndSwap(ctx, record.Key, record.Revision, record)
		if err != nil || !swapped {
			return ErrCompletionConflict
		}
	}
	return c.finalize(ctx)
}

func (c *PhaseOutputCompletionCoordinator) validateCurrentExecution(ctx context.Context) error {
	interval := c.PollInterval
	if interval <= 0 {
		interval = 25 * time.Millisecond
	}
	waitingKey := WaitingKey{TaskID: c.Binding.TaskID, InvocationID: c.Binding.InvocationID, Generation: c.Binding.Generation}
	physicalKey := PhysicalExecutionKey{TaskID: c.Binding.TaskID, InvocationID: c.Binding.InvocationID, Generation: c.Binding.Generation, ExecutionEpoch: ExecutionEpoch(c.Binding.ExecutionEpoch)}
	for {
		waiting, found, err := c.Waiting.Get(ctx, waitingKey)
		if err != nil || !found || waiting.ExecutionEpoch > ExecutionEpoch(c.Binding.ExecutionEpoch) {
			return ErrStaleCompletion
		}
		execution, physicalFound, physicalErr := c.PhysicalExecutions.Get(ctx, physicalKey)
		if physicalErr != nil || !physicalFound || execution.BindingRef != c.Binding.BindingRef || execution.InputRevision != c.Binding.InputRevision {
			return ErrStaleCompletion
		}
		if waiting.State == AwaitStateRunning && waiting.ExecutionEpoch == ExecutionEpoch(c.Binding.ExecutionEpoch) && waiting.PreviousBindingRef == c.Binding.BindingRef && waiting.InputRevision == c.Binding.InputRevision && execution.State == PhysicalExecutionRunning {
			return nil
		}
		if waiting.State != AwaitStateRehydrating || waiting.ExecutionEpoch+1 != ExecutionEpoch(c.Binding.ExecutionEpoch) || (execution.State != PhysicalExecutionAccepted && execution.State != PhysicalExecutionProvisioning) {
			return ErrStaleCompletion
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// RetryCompletion resumes cleanup after an accepted output without requiring
// an agent token to remain valid. It never accepts a new output.
func (c *PhaseOutputCompletionCoordinator) RetryCompletion(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.validate(); err != nil {
		return err
	}
	key := PhaseOutputKey{TaskID: c.Binding.TaskID, InvocationID: c.Binding.InvocationID, Generation: c.Binding.Generation}
	record, found, err := c.Outputs.Get(ctx, key)
	if err != nil || !found || !record.EventRecorded {
		return ErrStaleCompletion
	}
	return c.finalize(ctx)
}

func (c *PhaseOutputCompletionCoordinator) finalize(ctx context.Context) error {
	key := WaitingKey{TaskID: c.Binding.TaskID, InvocationID: c.Binding.InvocationID, Generation: c.Binding.Generation}
	waiting, found, err := c.Waiting.Get(ctx, key)
	if err != nil || !found {
		return ErrStaleCompletion
	}
	if waiting.State == AwaitStateRunning {
		if waiting.ExecutionEpoch != ExecutionEpoch(c.Binding.ExecutionEpoch) || waiting.PreviousBindingRef != c.Binding.BindingRef || waiting.InputRevision != c.Binding.InputRevision {
			return ErrStaleCompletion
		}
		terminal := waiting
		terminal.State = AwaitStateTerminal
		waiting, found, err = c.Waiting.CompareAndSwap(ctx, key, waiting.Revision, terminal)
		if err != nil || !found {
			return ErrCompletionConflict
		}
	} else if waiting.State != AwaitStateTerminal {
		return ErrStaleCompletion
	}

	physicalKey := PhysicalExecutionKey{TaskID: c.Binding.TaskID, InvocationID: c.Binding.InvocationID, Generation: c.Binding.Generation, ExecutionEpoch: ExecutionEpoch(c.Binding.ExecutionEpoch)}
	execution, found, err := c.PhysicalExecutions.Get(ctx, physicalKey)
	if err != nil || !found {
		return ErrStaleCompletion
	}
	if execution.BindingRef != c.Binding.BindingRef || execution.InputRevision != c.Binding.InputRevision {
		return ErrStaleCompletion
	}
	if execution.State == PhysicalExecutionTerminated {
		return nil
	}
	if execution.State == PhysicalExecutionRunning {
		next := execution
		next.State = PhysicalExecutionTearingDown
		execution, found, err = c.PhysicalExecutions.CompareAndSwap(ctx, physicalKey, execution.Revision, next)
		if err != nil || !found {
			return ErrCompletionConflict
		}
	} else if execution.State != PhysicalExecutionTearingDown {
		return ErrStaleCompletion
	}

	steps := []struct {
		done *bool
		run  func() error
	}{
		{&execution.Teardown.TeamHarnessTaskCancelled, func() error {
			return c.Tasks.CompleteTeamHarnessTask(ctx, TeamHarnessTask{ID: execution.TeamHarnessTaskID, AssignedTo: execution.TeamHarnessAssignedTo})
		}},
		{&execution.Teardown.WorkerDeleted, func() error {
			return c.Workers.DeleteWorker(ctx, ProvisionedWorker{ID: execution.WorkerID, Name: execution.WorkerName, MCPClientID: execution.MCPClientID, RuntimeGeneration: execution.DesiredRuntimeGeneration})
		}},
		{&execution.Teardown.MCPCleaned, func() error {
			return c.MCP.CleanupWorkerMCP(ctx, ProvisionedWorker{ID: execution.WorkerID, Name: execution.WorkerName, MCPClientID: execution.MCPClientID, RuntimeGeneration: execution.DesiredRuntimeGeneration})
		}},
		{&execution.Teardown.CredentialRevoked, func() error {
			return c.Credentials.RevokeMCPCredential(ctx, MCPCredentialBinding{Ref: execution.CredentialBindingRef, WorkerName: execution.WorkerName})
		}},
		{&execution.Teardown.TokenRevoked, func() error { c.Bindings.RevokeBinding(c.Binding); return nil }},
		{&execution.Teardown.LeaseReleased, func() error {
			return c.Leases.ReleaseWorkspaceLease(ctx, WorkspaceLease{Ref: execution.WorkspaceLeaseRef, Epoch: execution.ExecutionEpoch})
		}},
	}
	for _, step := range steps {
		if *step.done {
			continue
		}
		if err := step.run(); err != nil {
			return err
		}
		*step.done = true
		var swapped bool
		execution, swapped, err = c.PhysicalExecutions.CompareAndSwap(ctx, physicalKey, execution.Revision, execution)
		if err != nil || !swapped {
			return ErrCompletionConflict
		}
	}
	if c.TransientExecutions != nil {
		if err := c.TransientExecutions.ReleaseTransientExecution(ctx, execution); err != nil {
			return err
		}
	}
	execution.State = PhysicalExecutionTerminated
	_, found, err = c.PhysicalExecutions.CompareAndSwap(ctx, physicalKey, execution.Revision, execution)
	if err != nil || !found {
		return ErrCompletionConflict
	}
	return nil
}

func (c *PhaseOutputCompletionCoordinator) validate() error {
	if c == nil || c.Outputs == nil || c.Waiting == nil || c.PhysicalExecutions == nil || c.Tasks == nil || c.Workers == nil || c.MCP == nil || c.Credentials == nil || c.Bindings == nil || c.Leases == nil {
		return errors.New("phase completion dependencies are required")
	}
	if c.Binding.TaskID == "" || c.Binding.InvocationID == "" || c.Binding.Generation <= 0 || c.Binding.ExecutionEpoch <= 0 || c.Binding.BindingRef == "" || c.Binding.InputRevision == "" {
		return errors.New("phase completion binding is required")
	}
	return nil
}

func completionOutputRefs(output phaseagent.PhaseOutput) []artifacts.ArtifactRef {
	refs := make([]artifacts.ArtifactRef, 0, len(output.DeliveryRefs)+len(output.EvidenceRefs)+1)
	for _, ref := range output.DeliveryRefs {
		refs = append(refs, artifacts.ArtifactRef(ref))
	}
	if output.ReportRef != "" {
		refs = append(refs, artifacts.ArtifactRef(output.ReportRef))
	}
	for _, ref := range output.EvidenceRefs {
		refs = append(refs, artifacts.ArtifactRef(ref))
	}
	return refs
}

func (c *PhaseOutputCompletionCoordinator) AwaitInputs(ctx context.Context, request phaseagent.AwaitInputsRequest) (phaseagent.InputWaitResult, error) {
	if c.Delegate == nil {
		return phaseagent.InputWaitResult{}, errors.New("runtime delegate is unavailable")
	}
	return c.Delegate.AwaitInputs(ctx, request)
}
func (c *PhaseOutputCompletionCoordinator) ProposeOrchestration(ctx context.Context, proposal phaseagent.OrchestrationProposal) error {
	if c.Delegate == nil {
		return errors.New("runtime delegate is unavailable")
	}
	return c.Delegate.ProposeOrchestration(ctx, proposal)
}
func (c *PhaseOutputCompletionCoordinator) SubmitRequirement(ctx context.Context, requirement phaseagent.Requirement) error {
	if c.Delegate == nil {
		return errors.New("runtime delegate is unavailable")
	}
	return c.Delegate.SubmitRequirement(ctx, requirement)
}
func (c *PhaseOutputCompletionCoordinator) ListTaskMemoryCandidates(ctx context.Context) (phaseagent.TaskMemoryBufferView, error) {
	if c.Delegate == nil {
		return phaseagent.TaskMemoryBufferView{}, errors.New("runtime delegate is unavailable")
	}
	return c.Delegate.ListTaskMemoryCandidates(ctx)
}
func (c *PhaseOutputCompletionCoordinator) SubmitMemoryCandidate(ctx context.Context, candidate phaseagent.MemoryCandidate) (phaseagent.CandidateBufferedReceipt, error) {
	if c.Delegate == nil {
		return phaseagent.CandidateBufferedReceipt{}, errors.New("runtime delegate is unavailable")
	}
	return c.Delegate.SubmitMemoryCandidate(ctx, candidate)
}
