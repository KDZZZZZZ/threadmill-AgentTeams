package phaseagent

import "context"

// Runtime is the Phase Agent's outbound boundary to its host Runtime.
//
// Implementations validate the current invocation, input revisions, contracts,
// permissions, and delivery requirements before routing a request to their
// respective owners. This interface neither grants a Phase Agent graph-writing
// authority nor exposes Task Manager, Context Graph, Artifact Store, MCP, or
// AgentTeams details.
type Runtime interface {
	// AwaitInputs waits only for inputs already declared in the invocation's
	// PhaseInputSet. An omitted InputIDs slice requests all pending inputs. Its
	// later rehydration retains the logical generation and is not checkpoint
	// resume or a PhaseCommand.resume action.
	AwaitInputs(ctx context.Context, request AwaitInputsRequest) (InputWaitResult, error)

	// SubmitPhaseOutput submits a formal phase result. Submission is not a
	// decision that an endpoint is satisfied or that a Task is done.
	SubmitPhaseOutput(ctx context.Context, output PhaseOutput) error

	// ProposeOrchestration submits an intent for the Task Manager to accept,
	// revise, or reject; it does not modify the Coordination Graph directly.
	ProposeOrchestration(ctx context.Context, proposal OrchestrationProposal) error

	// SubmitRequirement submits newly discovered work for later normalization;
	// the submitted Requirement is not directly schedulable.
	SubmitRequirement(ctx context.Context, requirement Requirement) error

	// ListTaskMemoryCandidates reads the current Task's append-only candidate
	// buffer. Task identity is bound by Runtime call context, never by Agent input.
	ListTaskMemoryCandidates(ctx context.Context) (TaskMemoryBufferView, error)

	// SubmitMemoryCandidate appends a candidate to the current Task's buffer and
	// returns its buffered ID. It does not review the candidate or mutate the
	// Context Graph.
	SubmitMemoryCandidate(ctx context.Context, candidate MemoryCandidate) (CandidateBufferedReceipt, error)
}
