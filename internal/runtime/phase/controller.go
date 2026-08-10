package phase

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	baseruntime "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime/promptcatalog"
)

const defaultInvocationTTL = time.Hour

type PhaseEndpointRef = coordination.PhaseEndpointRef
type PhaseCommand = coordination.PhaseCommand

type InputRequirement struct {
	InputID           string           `json:"input_id"`
	FromEndpoint      PhaseEndpointRef `json:"from_endpoint"`
	RequiredArtifacts []string         `json:"required_artifacts"`
	RequiredBy        string           `json:"required_by"`
}

type InputDelivery struct {
	InputID        string           `json:"input_id"`
	FromEndpoint   PhaseEndpointRef `json:"from_endpoint"`
	PhaseOutputRef string           `json:"phase_output_ref"`
	ArtifactRefs   []string         `json:"artifact_refs"`
	SourceRevision string           `json:"source_revision"`
}

type PendingInput struct {
	InputID      string           `json:"input_id"`
	FromEndpoint PhaseEndpointRef `json:"from_endpoint"`
	RequiredBy   string           `json:"required_by"`
}

type PhaseInputSet struct {
	InputRevision string             `json:"input_revision"`
	Required      []InputRequirement `json:"required"`
	Delivered     []InputDelivery    `json:"delivered"`
	Pending       []PendingInput     `json:"pending"`
}

type StartPhaseInput struct {
	InvocationID kernel.InvocationID `json:"invocation_id"`
	Endpoint     PhaseEndpointRef    `json:"endpoint"`
	Generation   int                 `json:"generation"`
	BindingRef   kernel.BindingRef   `json:"binding_ref"`
	Inputs       PhaseInputSet       `json:"inputs"`
}

type AwaitInputsRequest struct {
	InputIDs []string `json:"input_ids,omitempty"`
}

type InputWaitResult struct {
	InputRevision  string          `json:"input_revision"`
	Delivered      []InputDelivery `json:"delivered"`
	Pending        []PendingInput  `json:"pending"`
	TerminalReason string          `json:"terminal_reason,omitempty"`
}

type InputsChanged struct {
	Inputs PhaseInputSet `json:"inputs"`
}

type ContextDelta struct {
	SubscriptionID string `json:"subscription_id"`
	SubgraphID     string `json:"subgraph_id"`
	Revision       int64  `json:"revision"`
	Changes        []any  `json:"changes,omitempty"`
}

type PhaseOutput struct {
	Phase        string   `json:"phase"`
	DeliveryRefs []string `json:"delivery_refs"`
	ReportRef    string   `json:"report_ref"`
	EvidenceRefs []string `json:"evidence_refs"`
}

type OutputReceipt struct {
	Output         PhaseOutput         `json:"output"`
	InvocationID   kernel.InvocationID `json:"invocation_id"`
	Endpoint       PhaseEndpointRef    `json:"endpoint"`
	Generation     int                 `json:"generation"`
	BindingRef     kernel.BindingRef   `json:"binding_ref"`
	LeaseRef       kernel.LeaseID      `json:"lease_ref"`
	InputRevision  string              `json:"input_revision"`
	WorkspaceRef   string              `json:"workspace_ref"`
	WorkspaceHead  string              `json:"workspace_head"`
	SubmittedAtUTC time.Time           `json:"submitted_at_utc"`
}

type BindingSnapshot struct {
	ProjectID           kernel.ProjectID
	ActorPrincipalID    kernel.ActorPrincipalID
	TaskID              kernel.TaskID
	EndpointID          kernel.EndpointID
	Generation          int
	BindingRef          kernel.BindingRef
	LeaseRef            kernel.LeaseID
	WorkspaceRef        string
	WorkspaceRevision   string
	ContextSliceRef     string
	ContextSlice        string
	TaskMemoryBufferRef string
	TaskMemoryBuffer    string
	TaskContract        string
	PhaseSpec           string
	Inputs              PhaseInputSet
	CheckpointRef       string
	NonResumable        bool
}

type ActiveInvocation struct {
	Invocation baseruntime.Invocation
	Command    PhaseCommand
	Binding    BindingSnapshot
	Inputs     PhaseInputSet
	Revoked    bool
}

type DispatchRequest struct {
	Invocation    baseruntime.Invocation
	Capability    auth.Capability
	Prompt        promptcatalog.Rendered
	Start         StartPhaseInput
	Binding       BindingSnapshot
	CheckpointRef string
}

type StopRequest struct {
	Invocation baseruntime.Invocation
	Command    PhaseCommand
	Binding    BindingSnapshot
}

type StopResult struct {
	ResumeStateRef    string
	CheckpointRef     string
	WorkspaceRevision string
	NonResumable      bool
}

type BindingResolver interface {
	Resolve(context.Context, PhaseCommand) (BindingSnapshot, error)
	Refresh(context.Context, ActiveInvocation) (BindingSnapshot, error)
}

type Assembler interface {
	Assemble(baseruntime.Invocation, promptcatalog.RenderData) (baseruntime.Assembly, error)
}

type InputRuntime interface {
	AwaitInputs(context.Context, ActiveInvocation, AwaitInputsRequest) (InputWaitResult, error)
}

type ArtifactRouter interface {
	Route(context.Context, ActiveInvocation, string) (string, error)
}

type Host interface {
	Dispatch(context.Context, DispatchRequest) error
	Rehydrate(context.Context, DispatchRequest) error
	Suspend(context.Context, kernel.InvocationID) error
	Stop(context.Context, StopRequest) (StopResult, error)
	// Revoke is keyed by invocation ID and must be idempotent across recovery
	// retries after stop evidence has already been persisted.
	Revoke(context.Context, kernel.InvocationID) error
}

type RecoveryStore interface {
	RecordActiveInvocation(context.Context, ActiveInvocation) error
	RecoverActiveInvocation(context.Context, PhaseCommand, BindingSnapshot) (ActiveInvocation, bool, error)
	RecordStopEvidence(context.Context, ActiveInvocation, PhaseCommand, StopResult) error
	GetStopEvidence(context.Context, kernel.InvocationID, string) (StopResult, bool, error)
	ClearActiveInvocation(context.Context, kernel.InvocationID) error
	ValidateResume(context.Context, PhaseCommand, BindingSnapshot) error
}

type Config struct {
	InvocationStore baseruntime.InvocationStore
	Assembler       Assembler
	BindingResolver BindingResolver
	InputRuntime    InputRuntime
	ArtifactRouter  ArtifactRouter
	Host            Host
	RecoveryStore   RecoveryStore
	Lifecycle       baseruntime.InvocationLifecycle
	Now             func() time.Time
	InvocationTTL   time.Duration
}

type Controller struct {
	store     baseruntime.InvocationStore
	assembler Assembler
	bindings  BindingResolver
	inputs    InputRuntime
	artifacts ArtifactRouter
	host      Host
	recovery  RecoveryStore
	lifecycle baseruntime.InvocationLifecycle
	now       func() time.Time
	ttl       time.Duration

	mu        sync.Mutex
	cond      *sync.Cond
	commands  map[string]commandRecord
	active    map[kernel.InvocationID]ActiveInvocation
	byLease   map[kernel.LeaseID]kernel.InvocationID
	receipts  map[kernel.InvocationID]OutputReceipt
	pending   map[kernel.InvocationID]OutputReceipt
	byCommand map[string]kernel.InvocationID
}

type commandRecord struct {
	fingerprint string
	inFlight    bool
	done        bool
	err         error
}

func NewController(cfg Config) *Controller {
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	ttl := cfg.InvocationTTL
	if ttl <= 0 {
		ttl = defaultInvocationTTL
	}
	lifecycle := cfg.Lifecycle
	if lifecycle == nil {
		lifecycle = baseruntime.NoopInvocationLifecycle{}
	}
	c := &Controller{
		store:     cfg.InvocationStore,
		assembler: cfg.Assembler,
		bindings:  cfg.BindingResolver,
		inputs:    cfg.InputRuntime,
		artifacts: cfg.ArtifactRouter,
		host:      cfg.Host,
		recovery:  cfg.RecoveryStore,
		lifecycle: lifecycle,
		now:       now,
		ttl:       ttl,
		commands:  make(map[string]commandRecord),
		active:    make(map[kernel.InvocationID]ActiveInvocation),
		byLease:   make(map[kernel.LeaseID]kernel.InvocationID),
		receipts:  make(map[kernel.InvocationID]OutputReceipt),
		pending:   make(map[kernel.InvocationID]OutputReceipt),
		byCommand: make(map[string]kernel.InvocationID),
	}
	c.cond = sync.NewCond(&c.mu)
	return c
}

func (c *Controller) Apply(ctx context.Context, command PhaseCommand) error {
	if err := validateCommand(command); err != nil {
		return err
	}
	fingerprint, err := hashJSON(command)
	if err != nil {
		return err
	}
	if replayed, done, err := c.recordCommandStart(command.ID, fingerprint); replayed {
		if done {
			return err
		}
	}
	switch command.Action {
	case coordination.CommandStart, coordination.CommandResume:
		err = c.applyStartLike(ctx, command)
	case coordination.CommandStop:
		err = c.applyStop(ctx, command)
	default:
		err = kernel.InvalidArgument("phase command action must be start, stop, or resume")
	}
	c.recordCommandFinish(command.ID, err)
	return err
}

func (c *Controller) AwaitInputs(ctx context.Context, invocationID kernel.InvocationID, req AwaitInputsRequest) (InputWaitResult, error) {
	active, err := c.activeInvocation(invocationID)
	if err != nil {
		return InputWaitResult{}, err
	}
	if err := validateAwaitRequest(active.Inputs, req); err != nil {
		return InputWaitResult{}, err
	}
	if err := c.host.Suspend(ctx, invocationID); err != nil {
		return InputWaitResult{}, err
	}
	if err := c.transitionToWaiting(ctx, invocationID); err != nil {
		return InputWaitResult{}, err
	}
	result, err := c.inputs.AwaitInputs(ctx, active, req)
	if err != nil {
		return InputWaitResult{}, err
	}
	updated := active
	updated.Inputs = mergeInputWaitResult(active.Inputs, result)
	c.mu.Lock()
	c.active[invocationID] = updated
	c.mu.Unlock()
	if result.TerminalReason == "" {
		if err := c.store.Transition(ctx, invocationID, baseruntime.InvocationWaiting, baseruntime.InvocationRunning); err != nil {
			return InputWaitResult{}, err
		}
		if err := c.rehydrate(ctx, updated); err != nil {
			return InputWaitResult{}, err
		}
	}
	return result, nil
}

func (c *Controller) OnInputsChanged(ctx context.Context, invocationID kernel.InvocationID, change InputsChanged) error {
	active, err := c.activeInvocation(invocationID)
	if err != nil {
		return err
	}
	active.Inputs = clonePhaseInputSet(change.Inputs)
	c.mu.Lock()
	c.active[invocationID] = active
	c.mu.Unlock()
	return c.rehydrate(ctx, active)
}

func (c *Controller) OnContextDelta(ctx context.Context, invocationID kernel.InvocationID, _ ContextDelta) error {
	active, err := c.activeInvocation(invocationID)
	if err != nil {
		return err
	}
	return c.rehydrate(ctx, active)
}

func (c *Controller) SubmitPhaseOutput(ctx context.Context, invocationID kernel.InvocationID, output PhaseOutput) (OutputReceipt, error) {
	active, err := c.activeInvocation(invocationID)
	if err != nil {
		return OutputReceipt{}, err
	}
	refreshed, err := c.bindings.Refresh(ctx, active)
	if err != nil {
		return OutputReceipt{}, err
	}
	if err := validateBindingForActive(refreshed, active); err != nil {
		return OutputReceipt{}, err
	}
	active.Binding = cloneBindingSnapshot(refreshed)
	if err := validatePhaseOutput(active, output); err != nil {
		return OutputReceipt{}, err
	}
	receipt, err := c.pendingOrRouteReceipt(ctx, active, output)
	if err != nil {
		return OutputReceipt{}, err
	}
	if err := c.host.Revoke(ctx, invocationID); err != nil {
		return OutputReceipt{}, err
	}
	if err := c.lifecycle.End(ctx, active.Invocation); err != nil {
		return OutputReceipt{}, err
	}
	if err := c.transitionToCompleted(ctx, invocationID); err != nil {
		return OutputReceipt{}, err
	}
	if c.recovery != nil {
		if err := c.recovery.ClearActiveInvocation(ctx, invocationID); err != nil {
			return OutputReceipt{}, err
		}
	}
	c.mu.Lock()
	c.receipts[invocationID] = receipt
	delete(c.pending, invocationID)
	c.byCommand[active.Command.ID] = invocationID
	delete(c.active, invocationID)
	delete(c.byLease, active.Command.LeaseRef)
	c.mu.Unlock()
	return receipt, nil
}

func (c *Controller) Output(ctx context.Context, invocationID kernel.InvocationID) (OutputReceipt, bool, error) {
	if err := ctx.Err(); err != nil {
		return OutputReceipt{}, false, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	receipt, ok := c.receipts[invocationID]
	return receipt, ok, nil
}

func (c *Controller) OutputByCommand(ctx context.Context, commandID string) (OutputReceipt, bool, error) {
	if err := ctx.Err(); err != nil {
		return OutputReceipt{}, false, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	invocationID, ok := c.byCommand[commandID]
	if !ok {
		return OutputReceipt{}, false, nil
	}
	receipt, ok := c.receipts[invocationID]
	return receipt, ok, nil
}

func (c *Controller) transitionToWaiting(ctx context.Context, invocationID kernel.InvocationID) error {
	invocation, ok, err := c.store.Get(ctx, invocationID)
	if err != nil {
		return err
	}
	if !ok {
		return kernel.Error{Code: kernel.CodeNotFound, Message: "invocation not found"}
	}
	switch invocation.Status {
	case baseruntime.InvocationRunning:
		return c.store.Transition(ctx, invocationID, baseruntime.InvocationRunning, baseruntime.InvocationWaiting)
	case baseruntime.InvocationWaiting:
		return nil
	default:
		return kernel.Error{Code: kernel.CodeStaleCommand, Message: "invocation is not running or waiting", Recoverable: true}
	}
}

func (c *Controller) pendingOrRouteReceipt(ctx context.Context, active ActiveInvocation, output PhaseOutput) (OutputReceipt, error) {
	c.mu.Lock()
	if receipt, ok := c.pending[active.Invocation.ID]; ok {
		c.mu.Unlock()
		return receipt, nil
	}
	c.mu.Unlock()

	for i, ref := range output.DeliveryRefs {
		routed, err := c.artifacts.Route(ctx, active, ref)
		if err != nil {
			return OutputReceipt{}, err
		}
		output.DeliveryRefs[i] = routed
	}
	routedReport, err := c.artifacts.Route(ctx, active, output.ReportRef)
	if err != nil {
		return OutputReceipt{}, err
	}
	output.ReportRef = routedReport
	for i, ref := range output.EvidenceRefs {
		routed, err := c.artifacts.Route(ctx, active, ref)
		if err != nil {
			return OutputReceipt{}, err
		}
		output.EvidenceRefs[i] = routed
	}
	receipt := OutputReceipt{
		Output:         clonePhaseOutput(output),
		InvocationID:   active.Invocation.ID,
		Endpoint:       active.Command.Endpoint,
		Generation:     active.Command.Generation,
		BindingRef:     active.Command.BindingRef,
		LeaseRef:       active.Command.LeaseRef,
		InputRevision:  active.Inputs.InputRevision,
		WorkspaceRef:   active.Binding.WorkspaceRef,
		WorkspaceHead:  active.Binding.WorkspaceRevision,
		SubmittedAtUTC: c.now().UTC(),
	}
	c.mu.Lock()
	if existing, ok := c.pending[active.Invocation.ID]; ok {
		c.mu.Unlock()
		return existing, nil
	}
	c.pending[active.Invocation.ID] = receipt
	c.mu.Unlock()
	return receipt, nil
}

func (c *Controller) applyStartLike(ctx context.Context, command PhaseCommand) error {
	binding, err := c.bindings.Resolve(ctx, command)
	if err != nil {
		return err
	}
	if err := validateBindingForCommand(binding, command); err != nil {
		return err
	}
	if command.Action == coordination.CommandStart && binding.CheckpointRef != "" {
		return kernel.Error{Code: kernel.CodeStaleCheckpoint, Message: "start command cannot use checkpoint-bound binding", Recoverable: true}
	}
	if command.Action == coordination.CommandResume {
		if binding.NonResumable || binding.CheckpointRef == "" {
			return kernel.Error{Code: kernel.CodeStaleCheckpoint, Message: "resume checkpoint is missing or non-resumable", Recoverable: true}
		}
		if c.recovery == nil {
			return kernel.Error{Code: kernel.CodeStaleCheckpoint, Message: "resume evidence store is required", Recoverable: true}
		}
		if err := c.recovery.ValidateResume(ctx, command, binding); err != nil {
			return err
		}
	}
	role, err := phaseRole(command.Endpoint.EndpointID)
	if err != nil {
		return err
	}
	now := c.now().UTC()
	invocation := baseruntime.Invocation{
		ID:                  deterministicInvocationID(command),
		ActorPrincipalID:    binding.ActorPrincipalID,
		ProjectID:           binding.ProjectID,
		TaskID:              command.Endpoint.TaskID,
		EndpointID:          command.Endpoint.EndpointID,
		Generation:          uint64(command.Generation),
		Role:                role,
		Status:              baseruntime.InvocationPrepared,
		BindingRef:          command.BindingRef,
		LeaseID:             command.LeaseRef,
		WorkspaceRef:        binding.WorkspaceRef,
		ContextSliceRef:     binding.ContextSliceRef,
		TaskMemoryBufferRef: binding.TaskMemoryBufferRef,
		CreatedAt:           now,
		ExpiresAt:           now.Add(c.ttl),
	}
	start := StartPhaseInput{
		InvocationID: invocation.ID,
		Endpoint:     command.Endpoint,
		Generation:   command.Generation,
		BindingRef:   command.BindingRef,
		Inputs:       clonePhaseInputSet(binding.Inputs),
	}
	renderData, err := c.renderData(invocation, start, binding)
	if err != nil {
		return err
	}
	assembly, err := c.assembler.Assemble(invocation, renderData)
	if err != nil {
		return err
	}
	invocation = assembly.Invocation
	if err := c.store.Create(ctx, invocation); err != nil {
		return err
	}
	active := ActiveInvocation{
		Invocation: invocation,
		Command:    command,
		Binding:    cloneBindingSnapshot(binding),
		Inputs:     clonePhaseInputSet(binding.Inputs),
	}
	if c.recovery != nil {
		if err := c.recovery.RecordActiveInvocation(ctx, active); err != nil {
			return err
		}
	}
	dispatch := DispatchRequest{
		Invocation:    invocation,
		Capability:    invocation.Capability(),
		Prompt:        assembly.Prompt,
		Start:         start,
		Binding:       cloneBindingSnapshot(binding),
		CheckpointRef: binding.CheckpointRef,
	}
	if err := c.host.Dispatch(ctx, dispatch); err != nil {
		if c.recovery != nil {
			_ = c.recovery.ClearActiveInvocation(ctx, invocation.ID)
		}
		_ = c.host.Revoke(ctx, invocation.ID)
		return err
	}
	if err := c.store.Transition(ctx, invocation.ID, baseruntime.InvocationPrepared, baseruntime.InvocationRunning); err != nil {
		_ = c.host.Revoke(ctx, invocation.ID)
		return err
	}
	active.Invocation.Status = baseruntime.InvocationRunning
	if c.recovery != nil {
		if err := c.recovery.RecordActiveInvocation(ctx, active); err != nil {
			_ = c.host.Revoke(ctx, invocation.ID)
			return err
		}
	}
	c.mu.Lock()
	c.active[invocation.ID] = active
	c.byLease[command.LeaseRef] = invocation.ID
	c.mu.Unlock()
	return nil
}

func (c *Controller) applyStop(ctx context.Context, command PhaseCommand) error {
	binding, err := c.bindings.Resolve(ctx, command)
	if err != nil {
		return err
	}
	if err := validateBindingForCommand(binding, command); err != nil {
		return err
	}
	active, err := c.activeByLease(command.LeaseRef)
	if err != nil {
		recovered, ok, recoverErr := c.recoverActiveByLease(ctx, command, binding)
		if recoverErr != nil {
			return recoverErr
		}
		if !ok {
			if stopped, stopErr := c.stopAlreadyApplied(ctx, command); stopErr != nil {
				return stopErr
			} else if stopped {
				return nil
			}
			return err
		}
		active = recovered
	}
	if active.Command.BindingRef != command.BindingRef || active.Command.Generation != command.Generation {
		return kernel.Error{Code: kernel.CodeStaleCommand, Message: "stop command does not match active invocation", Recoverable: true}
	}
	if active.Invocation.Status == baseruntime.InvocationStopped {
		if c.recovery != nil {
			if err := c.recovery.ClearActiveInvocation(ctx, active.Invocation.ID); err != nil {
				return err
			}
		}
		return nil
	}
	if c.recovery == nil {
		return kernel.IncompleteStopEvidence("stop evidence store is required")
	}
	if existing, ok, err := c.recovery.GetStopEvidence(ctx, active.Invocation.ID, command.ID); err != nil {
		return err
	} else if ok {
		if err := validateStopResult(existing); err != nil {
			return err
		}
		return c.finishPersistedStop(ctx, active)
	}
	result, err := c.host.Stop(ctx, StopRequest{Invocation: active.Invocation, Command: command, Binding: binding})
	if err != nil {
		return err
	}
	if err := validateStopResult(result); err != nil {
		return err
	}
	if err := c.recovery.RecordStopEvidence(ctx, active, command, result); err != nil {
		return err
	}
	return c.finishPersistedStop(ctx, active)
}

func (c *Controller) finishPersistedStop(ctx context.Context, active ActiveInvocation) error {
	if err := c.host.Revoke(ctx, active.Invocation.ID); err != nil {
		return err
	}
	if err := c.lifecycle.End(ctx, active.Invocation); err != nil {
		return err
	}
	if err := c.transitionToStopped(ctx, active.Invocation.ID); err != nil {
		return err
	}
	if c.recovery != nil {
		if err := c.recovery.ClearActiveInvocation(ctx, active.Invocation.ID); err != nil {
			return err
		}
	}
	c.mu.Lock()
	active.Revoked = true
	delete(c.active, active.Invocation.ID)
	delete(c.byLease, active.Command.LeaseRef)
	c.mu.Unlock()
	return nil
}

func (c *Controller) recoverActiveByLease(ctx context.Context, command PhaseCommand, binding BindingSnapshot) (ActiveInvocation, bool, error) {
	if c.recovery == nil {
		return ActiveInvocation{}, false, nil
	}
	active, ok, err := c.recovery.RecoverActiveInvocation(ctx, command, binding)
	if err != nil || !ok {
		return ActiveInvocation{}, ok, err
	}
	if err := validateBindingForActive(binding, active); err != nil {
		return ActiveInvocation{}, false, err
	}
	invocation, ok, err := c.store.Get(ctx, active.Invocation.ID)
	if err != nil {
		return ActiveInvocation{}, false, err
	}
	if !ok {
		return ActiveInvocation{}, false, kernel.Error{Code: kernel.CodeNotFound, Message: "invocation not found"}
	}
	active.Invocation = invocation
	switch invocation.Status {
	case baseruntime.InvocationCompleted, baseruntime.InvocationFailed:
		if err := c.recovery.ClearActiveInvocation(ctx, invocation.ID); err != nil {
			return ActiveInvocation{}, false, err
		}
		return ActiveInvocation{}, false, nil
	case baseruntime.InvocationStopped:
		if err := c.recovery.ClearActiveInvocation(ctx, invocation.ID); err != nil {
			return ActiveInvocation{}, false, err
		}
		return active, true, nil
	}
	c.mu.Lock()
	c.active[active.Invocation.ID] = active
	c.byLease[command.LeaseRef] = active.Invocation.ID
	c.mu.Unlock()
	return active, true, nil
}

func (c *Controller) stopAlreadyApplied(ctx context.Context, command PhaseCommand) (bool, error) {
	invocation, ok, err := c.store.GetByLease(ctx, command.LeaseRef)
	if err != nil {
		return false, err
	}
	if !ok || invocation.BindingRef != command.BindingRef || invocation.Generation != uint64(command.Generation) {
		return false, nil
	}
	return invocation.Status == baseruntime.InvocationStopped, nil
}

func (c *Controller) transitionToCompleted(ctx context.Context, invocationID kernel.InvocationID) error {
	invocation, ok, err := c.store.Get(ctx, invocationID)
	if err != nil {
		return err
	}
	if !ok {
		return kernel.Error{Code: kernel.CodeNotFound, Message: "invocation not found"}
	}
	switch invocation.Status {
	case baseruntime.InvocationRunning:
		return c.store.Transition(ctx, invocationID, baseruntime.InvocationRunning, baseruntime.InvocationCompleted)
	case baseruntime.InvocationCompleted:
		return nil
	default:
		return kernel.Error{Code: kernel.CodeStaleCommand, Message: "invocation is not running or completed", Recoverable: true}
	}
}

func (c *Controller) transitionToStopped(ctx context.Context, invocationID kernel.InvocationID) error {
	invocation, ok, err := c.store.Get(ctx, invocationID)
	if err != nil {
		return err
	}
	if !ok {
		return kernel.Error{Code: kernel.CodeNotFound, Message: "invocation not found"}
	}
	switch invocation.Status {
	case baseruntime.InvocationPrepared:
		return c.store.Transition(ctx, invocationID, baseruntime.InvocationPrepared, baseruntime.InvocationStopped)
	case baseruntime.InvocationRunning:
		return c.store.Transition(ctx, invocationID, baseruntime.InvocationRunning, baseruntime.InvocationStopped)
	case baseruntime.InvocationWaiting:
		return c.store.Transition(ctx, invocationID, baseruntime.InvocationWaiting, baseruntime.InvocationStopped)
	case baseruntime.InvocationStopped:
		return nil
	default:
		return kernel.Error{Code: kernel.CodeStaleCommand, Message: "invocation is not running or waiting", Recoverable: true}
	}
}

func (c *Controller) rehydrate(ctx context.Context, active ActiveInvocation) error {
	binding, err := c.bindings.Refresh(ctx, active)
	if err != nil {
		return err
	}
	if err := validateBindingForActive(binding, active); err != nil {
		return err
	}
	active.Binding = cloneBindingSnapshot(binding)
	start := StartPhaseInput{
		InvocationID: active.Invocation.ID,
		Endpoint:     active.Command.Endpoint,
		Generation:   active.Command.Generation,
		BindingRef:   active.Command.BindingRef,
		Inputs:       clonePhaseInputSet(active.Inputs),
	}
	renderData, err := c.renderData(active.Invocation, start, binding)
	if err != nil {
		return err
	}
	assembly, err := c.assembler.Assemble(active.Invocation, renderData)
	if err != nil {
		return err
	}
	req := DispatchRequest{
		Invocation:    assembly.Invocation,
		Capability:    assembly.Invocation.Capability(),
		Prompt:        assembly.Prompt,
		Start:         start,
		Binding:       binding,
		CheckpointRef: binding.CheckpointRef,
	}
	if err := c.host.Rehydrate(ctx, req); err != nil {
		return err
	}
	c.mu.Lock()
	c.active[active.Invocation.ID] = active
	c.mu.Unlock()
	return nil
}

func (c *Controller) renderData(invocation baseruntime.Invocation, start StartPhaseInput, binding BindingSnapshot) (promptcatalog.RenderData, error) {
	envelope, err := baseruntime.EnvelopeFromInvocation(invocation).JSON()
	if err != nil {
		return promptcatalog.RenderData{}, err
	}
	startPayload, err := json.Marshal(start)
	if err != nil {
		return promptcatalog.RenderData{}, err
	}
	return promptcatalog.RenderData{
		RuntimeEnvelope:    envelope,
		StartOrResumeInput: string(startPayload),
		TaskContract:       binding.TaskContract,
		PhaseSpec:          binding.PhaseSpec,
		WorkspaceBinding:   binding.WorkspaceRef,
		ContextSlice:       binding.ContextSlice,
		TaskMemoryBuffer:   binding.TaskMemoryBuffer,
	}, nil
}

func (c *Controller) recordCommandStart(id, fingerprint string) (replayed bool, done bool, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		existing, ok := c.commands[id]
		if !ok {
			c.commands[id] = commandRecord{fingerprint: fingerprint, inFlight: true}
			return false, false, nil
		}
		if existing.fingerprint != fingerprint {
			return true, true, kernel.Error{Code: kernel.CodeCommandConflict, Message: "command id was reused with different payload"}
		}
		if existing.inFlight {
			c.cond.Wait()
			continue
		}
		return true, existing.done, existing.err
	}
}

func (c *Controller) recordCommandFinish(id string, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	record := c.commands[id]
	record.inFlight = false
	record.err = err
	if err == nil {
		record.done = true
	}
	c.commands[id] = record
	c.cond.Broadcast()
}

func (c *Controller) activeInvocation(id kernel.InvocationID) (ActiveInvocation, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	active, ok := c.active[id]
	if !ok || active.Revoked {
		return ActiveInvocation{}, kernel.Error{Code: kernel.CodeStaleCommand, Message: "invocation is no longer active", Recoverable: true}
	}
	return cloneActiveInvocation(active), nil
}

func (c *Controller) activeByLease(lease kernel.LeaseID) (ActiveInvocation, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	invocationID, ok := c.byLease[lease]
	if !ok {
		return ActiveInvocation{}, kernel.Error{Code: kernel.CodeStaleCommand, Message: "no active invocation for lease", Recoverable: true}
	}
	active, ok := c.active[invocationID]
	if !ok || active.Revoked {
		return ActiveInvocation{}, kernel.Error{Code: kernel.CodeStaleCommand, Message: "invocation is no longer active", Recoverable: true}
	}
	return cloneActiveInvocation(active), nil
}

func validateCommand(command PhaseCommand) error {
	if command.ID == "" {
		return kernel.InvalidArgument("command id is required")
	}
	if err := kernel.RequireID("task_id", command.Endpoint.TaskID); err != nil {
		return err
	}
	if err := kernel.RequireID("endpoint_id", command.Endpoint.EndpointID); err != nil {
		return err
	}
	if command.Generation == 0 {
		return kernel.InvalidArgument("generation must be positive")
	}
	if err := kernel.RequireID("binding_ref", command.BindingRef); err != nil {
		return err
	}
	if err := kernel.RequireID("lease_ref", command.LeaseRef); err != nil {
		return err
	}
	return nil
}

func validateBindingForCommand(binding BindingSnapshot, command PhaseCommand) error {
	if binding.TaskID != command.Endpoint.TaskID ||
		binding.EndpointID != command.Endpoint.EndpointID ||
		binding.Generation != command.Generation ||
		binding.BindingRef != command.BindingRef {
		return kernel.StaleBinding("binding snapshot does not match command")
	}
	if binding.LeaseRef != command.LeaseRef {
		return kernel.LeaseConflict("binding lease does not match command")
	}
	if binding.ProjectID == "" || binding.ActorPrincipalID == "" || binding.WorkspaceRef == "" || binding.WorkspaceRevision == "" {
		return kernel.StaleBinding("binding snapshot is incomplete")
	}
	return nil
}

func validateBindingForActive(binding BindingSnapshot, active ActiveInvocation) error {
	if err := validateBindingForCommand(binding, active.Command); err != nil {
		return err
	}
	if binding.ProjectID != active.Invocation.ProjectID ||
		binding.TaskID != active.Invocation.TaskID ||
		binding.EndpointID != active.Invocation.EndpointID ||
		uint64(binding.Generation) != active.Invocation.Generation ||
		binding.BindingRef != active.Invocation.BindingRef ||
		binding.LeaseRef != active.Invocation.LeaseID {
		return kernel.StaleBinding("binding snapshot does not match active invocation")
	}
	return nil
}

func validateStopResult(result StopResult) error {
	if result.NonResumable {
		return nil
	}
	if result.ResumeStateRef == "" || result.WorkspaceRevision == "" || result.CheckpointRef == "" {
		return kernel.IncompleteStopEvidence("resumable stop requires resume_state_ref, workspace_revision, and checkpoint_ref")
	}
	return nil
}

func validateAwaitRequest(inputs PhaseInputSet, req AwaitInputsRequest) error {
	if len(req.InputIDs) == 0 {
		return nil
	}
	known := map[string]struct{}{}
	for _, pending := range inputs.Pending {
		known[pending.InputID] = struct{}{}
	}
	for _, id := range req.InputIDs {
		if _, ok := known[id]; !ok {
			return kernel.InvalidArgument("await input id is not pending")
		}
	}
	return nil
}

func validatePhaseOutput(active ActiveInvocation, output PhaseOutput) error {
	if output.Phase == "" || output.ReportRef == "" {
		return kernel.InvalidArgument("phase output requires phase and report_ref")
	}
	if err := validateCompletionInputs(active.Inputs); err != nil {
		return err
	}
	if output.Phase != string(active.Command.Endpoint.EndpointID) {
		return kernel.InvalidArgument("phase output phase does not match endpoint")
	}
	return nil
}

func validateCompletionInputs(inputs PhaseInputSet) error {
	delivered := make(map[string]InputDelivery, len(inputs.Delivered))
	for _, delivery := range inputs.Delivered {
		if delivery.InputID == "" || delivery.PhaseOutputRef == "" || delivery.SourceRevision == "" {
			return kernel.TransitionRejected("delivered completion input is missing identity, output ref, or source revision")
		}
		if delivery.FromEndpoint.TaskID == "" || delivery.FromEndpoint.EndpointID == "" {
			return kernel.TransitionRejected("delivered completion input is missing source endpoint")
		}
		delivered[delivery.InputID] = delivery
	}
	for _, req := range inputs.Required {
		if req.RequiredBy != "completion" {
			continue
		}
		delivery, ok := delivered[req.InputID]
		if !ok {
			return kernel.TransitionRejected("required completion input has not been delivered")
		}
		if req.FromEndpoint.TaskID == "" || req.FromEndpoint.EndpointID == "" || delivery.FromEndpoint != req.FromEndpoint {
			return kernel.TransitionRejected("completion input source endpoint does not match requirement")
		}
		artifacts := make(map[string]struct{}, len(delivery.ArtifactRefs))
		for _, ref := range delivery.ArtifactRefs {
			artifacts[ref] = struct{}{}
		}
		for _, required := range req.RequiredArtifacts {
			if _, ok := artifacts[required]; !ok {
				return kernel.TransitionRejected("completion input is missing required artifact")
			}
		}
	}
	return nil
}

func phaseRole(endpoint kernel.EndpointID) (auth.Role, error) {
	switch endpoint {
	case "plan":
		return auth.RolePlanner, nil
	case "execute":
		return auth.RoleExecutor, nil
	case "verify":
		return auth.RoleVerifier, nil
	default:
		return "", kernel.InvalidArgument("endpoint id must be plan, execute, or verify")
	}
}

func mergeInputWaitResult(current PhaseInputSet, result InputWaitResult) PhaseInputSet {
	next := clonePhaseInputSet(current)
	if result.InputRevision != "" {
		next.InputRevision = result.InputRevision
	}
	if result.Delivered != nil {
		next.Delivered = append(cloneInputDeliveries(current.Delivered), cloneInputDeliveries(result.Delivered)...)
		delivered := map[string]struct{}{}
		for _, delivery := range next.Delivered {
			delivered[delivery.InputID] = struct{}{}
		}
		filtered := make([]PendingInput, 0, len(current.Pending))
		for _, pending := range current.Pending {
			if _, ok := delivered[pending.InputID]; !ok {
				filtered = append(filtered, pending)
			}
		}
		next.Pending = filtered
	}
	if result.Pending != nil {
		next.Pending = clonePendingInputs(result.Pending)
	}
	return next
}

func hashJSON(value any) (string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func deterministicInvocationID(command PhaseCommand) kernel.InvocationID {
	sum := sha256.Sum256([]byte(command.ID))
	return kernel.InvocationID(fmt.Sprintf("inv_%s", hex.EncodeToString(sum[:8])))
}

func cloneActiveInvocation(active ActiveInvocation) ActiveInvocation {
	active.Binding = cloneBindingSnapshot(active.Binding)
	active.Inputs = clonePhaseInputSet(active.Inputs)
	return active
}

func cloneBindingSnapshot(binding BindingSnapshot) BindingSnapshot {
	binding.Inputs = clonePhaseInputSet(binding.Inputs)
	return binding
}

func clonePhaseInputSet(inputs PhaseInputSet) PhaseInputSet {
	inputs.Required = append([]InputRequirement(nil), inputs.Required...)
	inputs.Delivered = cloneInputDeliveries(inputs.Delivered)
	inputs.Pending = clonePendingInputs(inputs.Pending)
	return inputs
}

func cloneInputDeliveries(input []InputDelivery) []InputDelivery {
	out := append([]InputDelivery(nil), input...)
	for i := range out {
		out[i].ArtifactRefs = append([]string(nil), out[i].ArtifactRefs...)
	}
	return out
}

func clonePendingInputs(input []PendingInput) []PendingInput {
	return append([]PendingInput(nil), input...)
}

func clonePhaseOutput(output PhaseOutput) PhaseOutput {
	output.DeliveryRefs = append([]string(nil), output.DeliveryRefs...)
	output.EvidenceRefs = append([]string(nil), output.EvidenceRefs...)
	return output
}
