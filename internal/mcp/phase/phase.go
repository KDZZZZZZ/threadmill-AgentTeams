// Package phasemcp provides Threadmill's provider-neutral Phase Agent MCP
// tool service. It is transport-free: a future stdio/HTTP MCP server adapts
// requests to these methods after resolving an opaque execution token.
package phasemcp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/artifacts"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/executionreceipt"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/phaseagent"
)

type InvocationBinding struct {
	InvocationID   string                       `json:"invocation_id"`
	TaskID         string                       `json:"task_id"`
	Endpoint       phaseagent.PhaseEndpointRef  `json:"endpoint"`
	Generation     int                          `json:"generation"`
	ExecutionEpoch int64                        `json:"execution_epoch,omitempty"`
	Role           phaseagent.Phase             `json:"role"`
	BindingRef     string                       `json:"binding_ref"`
	InputRevision  string                       `json:"input_revision,omitempty"`
	WorkspaceRoot  string                       `json:"-"`
	AllowedDirs    []string                     `json:"allowed_dirs"`
	PermissionRef  string                       `json:"permission_ref"`
	Capabilities   phaseagent.PhaseCapabilities `json:"capabilities"`
}

// ExecutionBinding is passed to the host as an opaque token plus its local
// allowlist. The token contains no business identity; the registry is the
// authority for Invocation/Task/role/permission binding.
type ExecutionBinding struct {
	Token     string            `json:"token"`
	Binding   InvocationBinding `json:"binding"`
	ToolNames []string          `json:"tool_names"`
}

type BoundServices struct {
	Binding InvocationBinding
	Runtime phaseagent.Runtime
	Reader  phaseagent.ContextGraphReader
	Agent   phaseagent.ContextAgent
	Expires time.Time
}

type PackageConsumptionConfirmer interface {
	ConfirmPackageConsumption(context.Context, InvocationBinding, executionreceipt.Submission) (executionreceipt.Receipt, error)
}

// PhaseOutputEventAuthority marks a Runtime that emits the authoritative
// PhaseOutputSubmitted event as part of its completion transaction.
type PhaseOutputEventAuthority interface {
	RecordsPhaseOutputSubmitted() bool
}

type BindingRegistry struct {
	mu       sync.RWMutex
	bindings map[string]BoundServices
	now      func() time.Time
}

func NewBindingRegistry() *BindingRegistry {
	return &BindingRegistry{bindings: make(map[string]BoundServices), now: time.Now}
}

func (r *BindingRegistry) Issue(services BoundServices) (ExecutionBinding, error) {
	if services.Binding.InvocationID == "" || services.Binding.TaskID == "" || services.Binding.BindingRef == "" || services.Runtime == nil || services.Reader == nil || services.Agent == nil {
		return ExecutionBinding{}, ErrInvalidBinding
	}
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		return ExecutionBinding{}, err
	}
	token := hex.EncodeToString(bytes)
	r.mu.Lock()
	r.bindings[token] = services
	r.mu.Unlock()
	return ExecutionBinding{Token: token, Binding: services.Binding, ToolNames: ToolNames(services.Binding.Capabilities)}, nil
}

// IssueExecution creates a trusted MCP binding directly from the transient
// ExecutionContext supplied by Runner. Runtime integration supplies the
// already-authorized mount/permission projection; agents never construct this
// value or choose its Task/Invocation identity.
func (r *BindingRegistry) IssueExecution(execution phaseagent.ExecutionContext, allowedDirs []string, permissionRef string, expires time.Time) (ExecutionBinding, error) {
	return r.IssueExecutionWithWorkspace(execution, "", allowedDirs, permissionRef, expires)
}

// IssueExecutionWithWorkspace is used by Runtime integration after it has
// mounted the trusted workspace. WorkspaceRoot is server-side only.
func (r *BindingRegistry) IssueExecutionWithWorkspace(execution phaseagent.ExecutionContext, workspaceRoot string, allowedDirs []string, permissionRef string, expires time.Time) (ExecutionBinding, error) {
	start := execution.Invocation.Start
	if err := start.Validate(); err != nil {
		return ExecutionBinding{}, err
	}
	return r.Issue(BoundServices{
		Binding: InvocationBinding{
			InvocationID:  start.InvocationID,
			TaskID:        start.Endpoint.TaskID,
			Endpoint:      start.Endpoint,
			Generation:    start.Generation,
			Role:          execution.Role.Phase,
			BindingRef:    start.BindingRef,
			WorkspaceRoot: workspaceRoot,
			AllowedDirs:   append([]string(nil), allowedDirs...),
			PermissionRef: permissionRef,
			Capabilities:  execution.Role.Capabilities,
		},
		Runtime: execution.Runtime,
		Reader:  execution.ContextReader,
		Agent:   execution.ContextAgent,
		Expires: expires,
	})
}

func (r *BindingRegistry) Resolve(token string) (BoundServices, error) {
	r.mu.RLock()
	services, ok := r.bindings[token]
	r.mu.RUnlock()
	if !ok {
		return BoundServices{}, ErrInvalidToken
	}
	if !services.Expires.IsZero() && !r.now().Before(services.Expires) {
		return BoundServices{}, ErrExpiredToken
	}
	return services, nil
}

func (r *BindingRegistry) Revoke(token string) {
	r.mu.Lock()
	delete(r.bindings, token)
	r.mu.Unlock()
}

// RevokeBinding removes only tokens whose complete trusted execution identity
// matches binding. Normal completion can therefore revoke the current carrier
// without persisting raw token material.
func (r *BindingRegistry) RevokeBinding(binding InvocationBinding) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	revoked := 0
	for token, services := range r.bindings {
		candidate := services.Binding
		if candidate.TaskID == binding.TaskID && candidate.InvocationID == binding.InvocationID && candidate.Generation == binding.Generation && candidate.ExecutionEpoch == binding.ExecutionEpoch && candidate.BindingRef == binding.BindingRef && candidate.InputRevision == binding.InputRevision {
			delete(r.bindings, token)
			revoked++
		}
	}
	return revoked
}

var (
	ErrInvalidBinding             = errors.New("invalid trusted invocation binding")
	ErrInvalidToken               = errors.New("invalid execution token")
	ErrExpiredToken               = errors.New("expired execution token")
	ErrToolDenied                 = errors.New("tool is not allowed for this phase")
	ErrArtifactServiceUnavailable = errors.New("artifact service is not configured")
)

const (
	ToolAwaitInputs               = "runtime.awaitInputs"
	ToolSubmitPhaseOutput         = "agent.submitPhaseOutput"
	ToolProposeOrchestration      = "agent.proposeOrchestration"
	ToolSubmitRequirement         = "agent.submitRequirement"
	ToolListTaskMemoryCandidates  = "agent.listTaskMemoryCandidates"
	ToolSubmitMemoryCandidate     = "agent.submitMemoryCandidate"
	ToolListSubgraphs             = "context.listSubgraphs"
	ToolExplore                   = "context.explore"
	ToolSubscribe                 = "context.subscribe"
	ToolUnsubscribe               = "context.unsubscribe"
	ToolContextAgentRetrieve      = "contextAgent.retrieve"
	ToolRegisterArtifact          = "artifact.register"
	ToolConfirmPackageConsumption = "runtime.confirmPackageConsumption"
)

func ToolNames(c phaseagent.PhaseCapabilities) []string {
	tools := make([]string, 0, 12)
	if c.AllowAwaitInputs {
		tools = append(tools, ToolAwaitInputs)
	}
	if c.AllowOutputSubmission {
		tools = append(tools, ToolSubmitPhaseOutput)
	}
	if c.AllowOrchestrationProposal {
		tools = append(tools, ToolProposeOrchestration)
	}
	if c.AllowRequirementSubmission {
		tools = append(tools, ToolSubmitRequirement)
	}
	if c.AllowTaskMemoryRead {
		tools = append(tools, ToolListTaskMemoryCandidates)
	}
	if c.AllowTaskMemoryWrite {
		tools = append(tools, ToolSubmitMemoryCandidate)
	}
	if c.AllowContextRead {
		tools = append(tools, ToolListSubgraphs, ToolExplore)
	}
	if c.AllowContextSubscription {
		tools = append(tools, ToolSubscribe, ToolUnsubscribe)
	}
	if c.AllowContextRetrieval {
		tools = append(tools, ToolContextAgentRetrieve)
	}
	if c.AllowStructuredArtifactWrite || c.AllowEvidenceWrite {
		tools = append(tools, ToolRegisterArtifact)
	}
	return tools
}

type Handler struct {
	bindings  *BindingRegistry
	artifacts artifacts.Registrar
	recorder  artifacts.EventRecorder
	receipts  PackageConsumptionConfirmer
}

// NewHandler accepts optional Runtime-owned artifact and event seams. A
// handler without an artifact registrar remains usable for M2 tools but
// rejects artifact registration and formal output submission.
func NewHandler(bindings *BindingRegistry, services ...interface{}) (*Handler, error) {
	if bindings == nil {
		return nil, ErrInvalidBinding
	}
	h := &Handler{bindings: bindings}
	for _, service := range services {
		switch value := service.(type) {
		case artifacts.Registrar:
			h.artifacts = value
		case artifacts.EventRecorder:
			h.recorder = value
		case PackageConsumptionConfirmer:
			h.receipts = value
		}
	}
	return h, nil
}

func (h *Handler) Tools(token string) ([]string, error) {
	b, err := h.bindings.Resolve(token)
	if err != nil {
		return nil, err
	}
	tools := ToolNames(b.Binding.Capabilities)
	if b.Binding.ExecutionEpoch > 0 && b.Binding.InputRevision != "" && h.receipts != nil {
		tools = append(tools, ToolConfirmPackageConsumption)
	}
	return tools, nil
}

func (h *Handler) ConfirmPackageConsumption(ctx context.Context, token string, submission executionreceipt.Submission) (executionreceipt.Receipt, error) {
	b, err := h.bindings.Resolve(token)
	if err != nil {
		return executionreceipt.Receipt{}, err
	}
	if h.receipts == nil || b.Binding.ExecutionEpoch <= 0 || b.Binding.InputRevision == "" {
		return executionreceipt.Receipt{}, ErrToolDenied
	}
	return h.receipts.ConfirmPackageConsumption(ctx, b.Binding, submission)
}
func (h *Handler) AwaitInputs(ctx context.Context, token string, request phaseagent.AwaitInputsRequest) (phaseagent.InputWaitResult, error) {
	b, err := h.allowed(token, ToolAwaitInputs)
	if err != nil {
		return phaseagent.InputWaitResult{}, err
	}
	return b.Runtime.AwaitInputs(ctx, request)
}
func (h *Handler) SubmitPhaseOutput(ctx context.Context, token string, output phaseagent.PhaseOutput) error {
	b, err := h.allowed(token, ToolSubmitPhaseOutput)
	if err != nil {
		return err
	}
	if h.artifacts == nil {
		return ErrArtifactServiceUnavailable
	}
	if err := h.artifacts.ValidateReferences(ctx, artifactOwner(b.Binding), outputReferences(output)); err != nil {
		return err
	}
	if err := b.Runtime.SubmitPhaseOutput(ctx, output); err != nil {
		return err
	}
	ownedEvent := false
	if authority, ok := b.Runtime.(PhaseOutputEventAuthority); ok {
		ownedEvent = authority.RecordsPhaseOutputSubmitted()
	}
	if h.recorder != nil && !ownedEvent {
		return h.recorder.Record(ctx, artifacts.Event{Type: artifacts.EventPhaseOutputSubmitted, TaskID: b.Binding.TaskID, InvocationID: b.Binding.InvocationID, ArtifactRefs: outputReferences(output)})
	}
	return nil
}

// RegisterArtifact is the transport-neutral artifact.register tool. The
// trusted owner comes exclusively from the token registry. An agent can only
// choose a workspace-relative controlled path, kind, and optional media type.
func (h *Handler) RegisterArtifact(ctx context.Context, token, controlledPath string, kind artifacts.ArtifactType, mediaType string) (artifacts.ArtifactRef, error) {
	b, err := h.allowed(token, ToolRegisterArtifact)
	if err != nil {
		return "", err
	}
	if h.artifacts == nil {
		return "", ErrArtifactServiceUnavailable
	}
	return h.artifacts.Register(ctx, artifacts.RegisterRequest{Owner: artifactOwner(b.Binding), ControlledPath: controlledPath, Kind: kind, MediaType: mediaType})
}
func (h *Handler) ProposeOrchestration(ctx context.Context, token string, proposal phaseagent.OrchestrationProposal) error {
	b, err := h.allowed(token, ToolProposeOrchestration)
	if err != nil {
		return err
	}
	return b.Runtime.ProposeOrchestration(ctx, proposal)
}
func (h *Handler) SubmitRequirement(ctx context.Context, token string, requirement phaseagent.Requirement) error {
	b, err := h.allowed(token, ToolSubmitRequirement)
	if err != nil {
		return err
	}
	return b.Runtime.SubmitRequirement(ctx, requirement)
}
func (h *Handler) ListTaskMemoryCandidates(ctx context.Context, token string) (phaseagent.TaskMemoryBufferView, error) {
	b, err := h.allowed(token, ToolListTaskMemoryCandidates)
	if err != nil {
		return phaseagent.TaskMemoryBufferView{}, err
	}
	return b.Runtime.ListTaskMemoryCandidates(ctx)
}
func (h *Handler) SubmitMemoryCandidate(ctx context.Context, token string, candidate phaseagent.MemoryCandidate) (phaseagent.CandidateBufferedReceipt, error) {
	b, err := h.allowed(token, ToolSubmitMemoryCandidate)
	if err != nil {
		return phaseagent.CandidateBufferedReceipt{}, err
	}
	return b.Runtime.SubmitMemoryCandidate(ctx, candidate)
}
func (h *Handler) ListSubgraphs(ctx context.Context, token string, request phaseagent.ListSubgraphsRequest) ([]phaseagent.ContextSubgraph, error) {
	b, err := h.allowed(token, ToolListSubgraphs)
	if err != nil {
		return nil, err
	}
	return b.Reader.ListSubgraphs(ctx, request)
}
func (h *Handler) Explore(ctx context.Context, token string, request phaseagent.ExploreRequest) (phaseagent.ContextSliceDelta, error) {
	b, err := h.allowed(token, ToolExplore)
	if err != nil {
		return phaseagent.ContextSliceDelta{}, err
	}
	return b.Reader.Explore(ctx, request)
}
func (h *Handler) Subscribe(ctx context.Context, token string, request phaseagent.SubscribeRequest) (phaseagent.ContextSubscription, error) {
	b, err := h.allowed(token, ToolSubscribe)
	if err != nil {
		return phaseagent.ContextSubscription{}, err
	}
	return b.Reader.Subscribe(ctx, request)
}
func (h *Handler) Unsubscribe(ctx context.Context, token, subscriptionID string) error {
	b, err := h.allowed(token, ToolUnsubscribe)
	if err != nil {
		return err
	}
	return b.Reader.Unsubscribe(ctx, subscriptionID)
}
func (h *Handler) Retrieve(ctx context.Context, token string, request phaseagent.ContextRetrieveRequest) (phaseagent.ContextRetrieveResult, error) {
	b, err := h.allowed(token, ToolContextAgentRetrieve)
	if err != nil {
		return phaseagent.ContextRetrieveResult{}, err
	}
	return b.Agent.Retrieve(ctx, request)
}

func (h *Handler) allowed(token, tool string) (BoundServices, error) {
	b, err := h.bindings.Resolve(token)
	if err != nil {
		return BoundServices{}, err
	}
	for _, name := range ToolNames(b.Binding.Capabilities) {
		if name == tool {
			return b, nil
		}
	}
	return BoundServices{}, ErrToolDenied
}

func artifactOwner(binding InvocationBinding) artifacts.TrustedOwner {
	return artifacts.TrustedOwner{TaskID: binding.TaskID, InvocationID: binding.InvocationID, WorkspaceRoot: binding.WorkspaceRoot, AllowedDirs: binding.AllowedDirs}
}

func outputReferences(output phaseagent.PhaseOutput) []artifacts.ArtifactRef {
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
