// Package agentteams defines Threadmill's private contract for using
// AgentTeams as a PhaseExecutor host. It contains no TeamHarness, WorkerFlow,
// QwenPaw, MCP transport, or file-sync implementation.
package agentteams

import (
	"context"
	"errors"
	"fmt"

	phasemcp "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/mcp/phase"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/phaseagent"
)

// AgentTeamsPhaseExecutor adapts the provider-neutral PhaseExecutor port to
// an AgentTeams-specific execution host. It never returns a PhaseOutput:
// formal output remains an explicit call on phaseagent.Runtime.
type AgentTeamsPhaseExecutor struct {
	host      ExecutionHost
	envelopes HostEnvelopeResolver
}

var _ phaseagent.PhaseExecutor = (*AgentTeamsPhaseExecutor)(nil)

// NewAgentTeamsPhaseExecutor creates the M0 adapter shell. The Runtime-facing
// resolver supplies content already resolved from BindingRef; AgentTeams must
// never resolve or interpret BindingRef itself.
func NewAgentTeamsPhaseExecutor(host ExecutionHost, envelopes HostEnvelopeResolver) (*AgentTeamsPhaseExecutor, error) {
	if host == nil {
		return nil, contractError("execution host is required")
	}
	if envelopes == nil {
		return nil, contractError("host envelope resolver is required")
	}
	return &AgentTeamsPhaseExecutor{host: host, envelopes: envelopes}, nil
}

// Execute converts the transient Phase Agent execution into an internal host
// request and delegates exactly once. It does not perform task delegation,
// acknowledgement, result parsing, output submission, or lifecycle changes.
func (e *AgentTeamsPhaseExecutor) Execute(ctx context.Context, execution phaseagent.ExecutionContext) error {
	if e == nil || e.host == nil || e.envelopes == nil {
		return contractError("agentteams phase executor is not configured")
	}

	envelope, err := e.envelopes.ResolveHostEnvelope(ctx, execution)
	if err != nil {
		return &TransportError{Err: err}
	}
	request, err := buildHostExecutionRequest(execution, envelope)
	if err != nil {
		return err
	}
	outcome, err := e.host.Execute(ctx, request)
	if err != nil {
		var control *UnsupportedControlFlowError
		if errors.As(err, &control) {
			return control
		}
		return &TransportError{Err: err}
	}
	return outcomeError(outcome)
}

// ExecutionHost is the smallest AgentTeams-specific host seam required for
// M0/M1 fresh invocation. It intentionally does not expose TeamHarness
// actions such as delegate_task, ack_task, submit_task, or check_task.
// Concrete hosts will translate this one operation to those private details.
type ExecutionHost interface {
	Execute(ctx context.Context, request HostExecutionRequest) (HostExecutionOutcome, error)
}

// HostEnvelopeResolver is supplied by Threadmill Runtime integration. It
// resolves BindingRef before the adapter is entered and returns only the
// already-authorized materialization needed by the execution host.
type HostEnvelopeResolver interface {
	ResolveHostEnvelope(ctx context.Context, execution phaseagent.ExecutionContext) (HostEnvelope, error)
}

// HostExecutionRequest is private to the Runtime-to-AgentTeams adapter
// boundary. It is not a worker payload and must not be JSON-serialized as a
// substitute for phaseagent.ExecutionContext.
type HostExecutionRequest struct {
	InvocationID string                      `json:"invocation_id"`
	Endpoint     phaseagent.PhaseEndpointRef `json:"endpoint"`
	Generation   int                         `json:"generation"`
	Role         phaseagent.RoleSpec         `json:"role"`
	Inputs       phaseagent.PhaseInputSet    `json:"inputs"`
	Policy       EffectivePolicy             `json:"policy"`
	Envelope     HostEnvelope                `json:"-"`
}

// HostEnvelope is a Runtime-only, internal integration envelope. It holds
// resolved material rather than references for AgentTeams to interpret. The
// concrete host must project only permitted agent-visible content (for example
// a spec.md projection and mounted files) and must not expose trusted binding
// data, permissions, or MCP credentials to a worker as self-asserted fields.
type HostEnvelope struct {
	BindingRef       string                          `json:"-"`
	TaskSpec         string                          `json:"-"`
	TaskContract     string                          `json:"-"`
	PhaseInstruction string                          `json:"-"`
	Workspace        WorkspaceMount                  `json:"-"`
	Context          MaterializedContext             `json:"-"`
	TaskMemory       phaseagent.TaskMemoryBufferView `json:"-"`
	MCPBinding       TrustedMCPBinding               `json:"-"`
}

// WorkspaceMount describes an already prepared host mount. It neither creates
// a worktree nor grants a lease; Runtime/Workspace Service own both actions.
type WorkspaceMount struct {
	Root        string   `json:"root"`
	AllowedDirs []string `json:"allowed_dirs"`
	ReadOnly    bool     `json:"read_only"`
}

// MaterializedContext is the Runtime-approved context projection for a host.
// It deliberately has no ContextSliceRef, graph API, search request, or
// subscription-control identity; those remain behind Threadmill MCP seams.
type MaterializedContext struct {
	Content string `json:"content"`
}

// TrustedMCPBinding is owned by the provider-neutral Threadmill MCP layer.
// It is an opaque execution token plus server-side binding, never agent input.
type TrustedMCPBinding = phasemcp.ExecutionBinding

// EffectivePolicy is the adapter-facing projection of PhaseCapabilities. It
// documents the eventual host/MCP/filesystem policy but enforces nothing in
// M0. BlockedHostActions prevent TeamHarness project/task control surfaces
// from becoming implicit Phase Agent capabilities.
type EffectivePolicy struct {
	Phase                        phaseagent.Phase    `json:"phase"`
	AllowSourceRead              bool                `json:"allow_source_read"`
	AllowImplementationWrite     bool                `json:"allow_implementation_write"`
	AllowStructuredArtifactWrite bool                `json:"allow_structured_artifact_write"`
	AllowEvidenceWrite           bool                `json:"allow_evidence_write"`
	AllowToolExecution           bool                `json:"allow_tool_execution"`
	ToolPolicy                   EffectiveToolPolicy `json:"tool_policy"`
}

// EffectiveToolPolicy is the future MCP/host allowlist projection. It does
// not configure QwenPaw in M0.
type EffectiveToolPolicy struct {
	AllowContextRead           bool     `json:"allow_context_read"`
	AllowContextSubscription   bool     `json:"allow_context_subscription"`
	AllowContextRetrieval      bool     `json:"allow_context_retrieval"`
	AllowTaskMemoryRead        bool     `json:"allow_task_memory_read"`
	AllowTaskMemoryWrite       bool     `json:"allow_task_memory_write"`
	AllowAwaitInputs           bool     `json:"allow_await_inputs"`
	AllowRequirementSubmission bool     `json:"allow_requirement_submission"`
	AllowOutputSubmission      bool     `json:"allow_output_submission"`
	AllowOrchestrationProposal bool     `json:"allow_orchestration_proposal"`
	BlockedHostActions         []string `json:"blocked_host_actions"`
}

// HostExecutionStatus reports only physical execution observations. It must
// never be used for endpoint acceptance, verify pass, or Task completion.
type HostExecutionStatus string

const (
	HostExecutionCompleted       HostExecutionStatus = "completed"
	HostExecutionWaiting         HostExecutionStatus = "waiting"
	HostExecutionStopped         HostExecutionStatus = "stopped"
	HostExecutionCancelled       HostExecutionStatus = "cancelled"
	HostExecutionTransportFailed HostExecutionStatus = "transport_failed"
	HostExecutionFailed          HostExecutionStatus = "host_failed"
)

// HostExecutionOutcome is execution evidence. It deliberately contains no
// PhaseOutput, endpoint state, Task state, result.md contents, or artifacts.
type HostExecutionOutcome struct {
	ExecutionID  string              `json:"execution_id"`
	Status       HostExecutionStatus `json:"status"`
	Acknowledged bool                `json:"acknowledged"`
	Summary      string              `json:"summary"`
}

func buildHostExecutionRequest(execution phaseagent.ExecutionContext, envelope HostEnvelope) (HostExecutionRequest, error) {
	start := execution.Invocation.Start
	if err := start.Validate(); err != nil {
		return HostExecutionRequest{}, err
	}
	if envelope.BindingRef == "" {
		return HostExecutionRequest{}, contractError("resolved host envelope binding_ref is required")
	}
	if envelope.BindingRef != start.BindingRef {
		return HostExecutionRequest{}, contractError("resolved host envelope binding_ref does not match execution")
	}
	policy := MapCapabilities(execution.Role)
	return HostExecutionRequest{
		InvocationID: start.InvocationID,
		Endpoint:     start.Endpoint,
		Generation:   start.Generation,
		Role:         execution.Role,
		Inputs:       execution.Invocation.Inputs,
		Policy:       policy,
		Envelope:     envelope,
	}, nil
}

// MapCapabilities is a pure conversion from Threadmill's phase policy into
// the policy shape a future AgentTeams host will enforce.
func MapCapabilities(role phaseagent.RoleSpec) EffectivePolicy {
	c := role.Capabilities
	tools := EffectiveToolPolicy{
		AllowContextRead:           c.AllowContextRead,
		AllowContextSubscription:   c.AllowContextSubscription,
		AllowContextRetrieval:      c.AllowContextRetrieval,
		AllowTaskMemoryRead:        c.AllowTaskMemoryRead,
		AllowTaskMemoryWrite:       c.AllowTaskMemoryWrite,
		AllowAwaitInputs:           c.AllowAwaitInputs,
		AllowRequirementSubmission: c.AllowRequirementSubmission,
		AllowOutputSubmission:      c.AllowOutputSubmission,
		AllowOrchestrationProposal: c.AllowOrchestrationProposal,
		BlockedHostActions: []string{
			"plan_dag",
			"accept_task_result",
			"coordination_graph_write",
			"projectflow",
			"agent_mailbox",
			"context.search",
		},
	}
	return EffectivePolicy{
		Phase:                        role.Phase,
		AllowSourceRead:              c.AllowSourceRead,
		AllowImplementationWrite:     c.AllowImplementationWrite,
		AllowStructuredArtifactWrite: c.AllowStructuredArtifactWrite,
		AllowEvidenceWrite:           c.AllowEvidenceWrite,
		AllowToolExecution:           c.AllowToolExecution,
		ToolPolicy:                   tools,
	}
}

func outcomeError(outcome HostExecutionOutcome) error {
	switch outcome.Status {
	case HostExecutionCompleted:
		return nil
	case HostExecutionWaiting, HostExecutionStopped, HostExecutionCancelled:
		return &ControlOutcomeError{Outcome: outcome}
	case HostExecutionTransportFailed:
		return &TransportError{Err: fmt.Errorf("agentteams execution %q reported transport failure: %s", outcome.ExecutionID, outcome.Summary)}
	case HostExecutionFailed:
		return &AgentExecutionError{Outcome: outcome}
	default:
		return contractError("unknown host execution status")
	}
}

// ControlOutcomeError preserves waiting/stopped/cancelled as typed control
// evidence. Current phaseagent.Runner treats every error as failed, so Runtime
// lifecycle integration must handle this type before Runner maps it to an
// Invocation state. M0 intentionally does not change the PhaseExecutor port.
type ControlOutcomeError struct{ Outcome HostExecutionOutcome }

func (e *ControlOutcomeError) Error() string {
	return "agentteams execution control outcome: " + string(e.Outcome.Status)
}

// TransportError identifies adapter/host transport or configuration failure.
type TransportError struct{ Err error }

func (e *TransportError) Error() string { return "agentteams transport error: " + e.Err.Error() }
func (e *TransportError) Unwrap() error { return e.Err }

// AgentExecutionError identifies a terminal failure reported by the host; it
// remains physical execution evidence, not a PhaseOutput or graph decision.
type AgentExecutionError struct{ Outcome HostExecutionOutcome }

func (e *AgentExecutionError) Error() string {
	return "agentteams host execution failed: " + e.Outcome.Summary
}

// UnsupportedControlFlowError signals a host lifecycle that M1 deliberately
// does not implement (waiting or stopped). It must not be converted to a
// successful completion. The current Runner still classifies it as failed;
// Runtime lifecycle integration will handle it in M4/M5.
type UnsupportedControlFlowError struct {
	Flow string
}

func (e *UnsupportedControlFlowError) Error() string {
	return "unsupported_control_flow: " + e.Flow
}

type contractError string

func (e contractError) Error() string { return string(e) }
