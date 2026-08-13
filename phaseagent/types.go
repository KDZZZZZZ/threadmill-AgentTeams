package phaseagent

// InputRequirement describes one declared incoming dependency of an endpoint.
// RequiredBy is "start" or "completion" as specified by the endpoint contract.
type InputRequirement struct {
	InputID           string           `json:"input_id"`
	FromEndpoint      PhaseEndpointRef `json:"from_endpoint"`
	RequiredArtifacts []string         `json:"required_artifacts"`
	RequiredBy        string           `json:"required_by"`
}

// InputDelivery is the formal output delivered by an upstream endpoint.
type InputDelivery struct {
	InputID        string           `json:"input_id"`
	FromEndpoint   PhaseEndpointRef `json:"from_endpoint"`
	PhaseOutputRef string           `json:"phase_output_ref"`
	ArtifactRefs   []string         `json:"artifact_refs"`
	SourceRevision string           `json:"source_revision"`
}

// PendingInput identifies a declared completion input that has not arrived.
type PendingInput struct {
	InputID      string           `json:"input_id"`
	FromEndpoint PhaseEndpointRef `json:"from_endpoint"`
	RequiredBy   string           `json:"required_by"`
}

// PhaseInputSet is the read-only projection of declared incoming edges made
// available to a Phase Agent by its Runtime.
type PhaseInputSet struct {
	InputRevision string             `json:"input_revision"`
	Required      []InputRequirement `json:"required"`
	Delivered     []InputDelivery    `json:"delivered"`
	Pending       []PendingInput     `json:"pending"`
}

// AwaitInputsRequest asks the Runtime to wait for declared pending inputs.
// An omitted InputIDs slice means all current pending inputs.
type AwaitInputsRequest struct {
	InputIDs []string `json:"input_ids,omitempty"`
}

// InputWaitResult is returned when declared inputs arrive or cannot arrive.
type InputWaitResult struct {
	InputRevision  string          `json:"input_revision"`
	Delivered      []InputDelivery `json:"delivered"`
	Pending        []PendingInput  `json:"pending"`
	TerminalReason string          `json:"terminal_reason,omitempty"`
}

// PhaseOutput is the structured formal output submitted by a Phase Agent.
// Runtime-owned binding fields intentionally do not appear here.
type PhaseOutput struct {
	Phase        Phase    `json:"phase"`
	DeliveryRefs []string `json:"delivery_refs"`
	ReportRef    string   `json:"report_ref"`
	EvidenceRefs []string `json:"evidence_refs"`
}

// OrchestrationProposal expresses an intent for the Task Manager to consider;
// it is not a command that modifies the Coordination Graph.
type OrchestrationProposal struct {
	ProposalID               string           `json:"proposal_id"`
	ClientRef                string           `json:"client_ref"`
	FromEndpoint             PhaseEndpointRef `json:"from_endpoint"`
	FromInvocationID         string           `json:"from_invocation_id"`
	BasedOnGraphRevision     int64            `json:"based_on_graph_revision"`
	BasedOnWorkspaceRevision string           `json:"based_on_workspace_revision"`
	BasedOnInputRevision     string           `json:"based_on_input_revision"`
	OrchestrationAdvice      string           `json:"orchestration_advice"`
	DeliverySpecAdvice       string           `json:"delivery_spec_advice"`
	ReportSpecAdvice         string           `json:"report_spec_advice"`
	Rationale                string           `json:"rationale"`
	EvidenceRefs             []string         `json:"evidence_refs"`
}

// Requirement is a newly discovered unit of work for the Task Manager to
// normalize into a Task Contract. A Requirement is not itself schedulable.
type Requirement struct {
	Text         string   `json:"text"`
	Goal         string   `json:"goal,omitempty"`
	Constraints  []string `json:"constraints,omitempty"`
	EvidenceRefs []string `json:"evidence_refs,omitempty"`
}

// MemoryCandidate is a reusable knowledge candidate. The Context Manager,
// rather than the Phase Agent, decides whether it becomes persistent context.
type MemoryCandidate struct {
	Statement   string   `json:"statement"`
	Kind        string   `json:"kind"`
	SourceRefs  []string `json:"source_refs"`
	SubgraphIDs []string `json:"subgraph_ids"`
}

// TaskMemoryCandidateView is the same-Task read-only view of one buffered
// candidate. It intentionally excludes TaskID and creation/audit context.
type TaskMemoryCandidateView struct {
	CandidateID string          `json:"candidate_id"`
	Candidate   MemoryCandidate `json:"candidate"`
}

// TaskMemoryBufferView is an append-only Task working-memory snapshot. It is
// neither a Context Graph response nor a ContextNode collection.
type TaskMemoryBufferView struct {
	Candidates []TaskMemoryCandidateView `json:"candidates"`
}

// CandidateBufferedReceipt acknowledges append-only buffering of a candidate;
// it neither approves the candidate nor writes to the Context Graph.
type CandidateBufferedReceipt struct {
	CandidateID string `json:"candidate_id"`
}
