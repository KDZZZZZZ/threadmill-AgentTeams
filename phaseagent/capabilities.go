package phaseagent

// PhaseCapabilities describes the policy envelope that Runtime will later
// enforce through tool allowlists, directory controls, and leases. It grants no
// filesystem or tool access by itself.
type PhaseCapabilities struct {
	AllowSourceRead              bool `json:"allow_source_read"`
	AllowImplementationWrite     bool `json:"allow_implementation_write"`
	AllowStructuredArtifactWrite bool `json:"allow_structured_artifact_write"`
	AllowEvidenceWrite           bool `json:"allow_evidence_write"`
	AllowToolExecution           bool `json:"allow_tool_execution"`
	AllowContextRead             bool `json:"allow_context_read"`
	AllowContextSubscription     bool `json:"allow_context_subscription"`
	AllowContextRetrieval        bool `json:"allow_context_retrieval"`
	AllowTaskMemoryRead          bool `json:"allow_task_memory_read"`
	AllowTaskMemoryWrite         bool `json:"allow_task_memory_write"`
	AllowAwaitInputs             bool `json:"allow_await_inputs"`
	AllowRequirementSubmission   bool `json:"allow_requirement_submission"`
	AllowOutputSubmission        bool `json:"allow_output_submission"`
	AllowOrchestrationProposal   bool `json:"allow_orchestration_proposal"`
}

// CapabilitiesFor returns the baseline policy description for a fixed phase.
// All phases can read context, work with the current Task memory buffer, submit
// formal output, and propose orchestration. Only execute writes implementation;
// verify writes evidence but not the candidate implementation.
func CapabilitiesFor(phase Phase) (PhaseCapabilities, error) {
	if err := phase.Validate(); err != nil {
		return PhaseCapabilities{}, err
	}

	base := PhaseCapabilities{
		AllowSourceRead:            true,
		AllowToolExecution:         true,
		AllowContextRead:           true,
		AllowContextSubscription:   true,
		AllowContextRetrieval:      true,
		AllowTaskMemoryRead:        true,
		AllowTaskMemoryWrite:       true,
		AllowAwaitInputs:           true,
		AllowRequirementSubmission: true,
		AllowOutputSubmission:      true,
		AllowOrchestrationProposal: true,
	}
	switch phase {
	case PhasePlan:
		base.AllowStructuredArtifactWrite = true
	case PhaseExecute:
		base.AllowImplementationWrite = true
		base.AllowStructuredArtifactWrite = true
		base.AllowEvidenceWrite = true
	case PhaseVerify:
		base.AllowStructuredArtifactWrite = true
		base.AllowEvidenceWrite = true
	}
	return base, nil
}
