package phase

import (
	"strings"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

const (
	OrchestrationSplit          = "split"
	OrchestrationDependency     = "dependency"
	OrchestrationReplan         = "replan"
	OrchestrationRetry          = "retry"
	OrchestrationSerialParallel = "serial_parallel"
)

// OrchestrationIntent is the only proposal payload accepted from a Phase
// Agent. Identity, endpoint, invocation, and revision fields are deliberately
// absent; Agent Runtime derives those trusted fields from the active binding.
type OrchestrationIntent struct {
	OrchestrationAdvice string   `json:"orchestration_advice"`
	DeliverySpecAdvice  string   `json:"delivery_spec_advice,omitempty"`
	ReportSpecAdvice    string   `json:"report_spec_advice,omitempty"`
	Rationale           string   `json:"rationale"`
	EvidenceRefs        []string `json:"evidence_refs,omitempty"`
}

// OrchestrationProposal is the Phase Agent's bounded orchestration intent.
// It is not a Coordination Graph mutation. Runtime binds and validates the
// source invocation authority before routing the proposal to Task Manager.
type OrchestrationProposal struct {
	ProposalID               string              `json:"proposal_id"`
	ClientRef                string              `json:"client_ref"`
	FromEndpoint             PhaseEndpointRef    `json:"from_endpoint"`
	FromInvocationID         kernel.InvocationID `json:"from_invocation_id"`
	BasedOnGraphRevision     kernel.Revision     `json:"based_on_graph_revision"`
	BasedOnWorkspaceRevision string              `json:"based_on_workspace_revision"`
	BasedOnInputRevision     string              `json:"based_on_input_revision"`
	OrchestrationAdvice      string              `json:"orchestration_advice"`
	DeliverySpecAdvice       string              `json:"delivery_spec_advice"`
	ReportSpecAdvice         string              `json:"report_spec_advice"`
	Rationale                string              `json:"rationale"`
	EvidenceRefs             []string            `json:"evidence_refs"`
}

func ValidateOrchestrationIntent(intent OrchestrationIntent) error {
	switch intent.OrchestrationAdvice {
	case OrchestrationSplit, OrchestrationDependency, OrchestrationReplan, OrchestrationRetry, OrchestrationSerialParallel:
	default:
		return kernel.InvalidArgument("orchestration_advice must be split, dependency, replan, retry, or serial_parallel")
	}
	if strings.TrimSpace(intent.Rationale) == "" {
		return kernel.InvalidArgument("rationale is required")
	}
	if strings.TrimSpace(intent.DeliverySpecAdvice) == "" {
		return kernel.InvalidArgument("delivery_spec_advice is required")
	}
	if strings.TrimSpace(intent.ReportSpecAdvice) == "" {
		return kernel.InvalidArgument("report_spec_advice is required")
	}
	return nil
}
