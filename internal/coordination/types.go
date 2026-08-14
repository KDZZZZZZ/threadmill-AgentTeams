package coordination

import (
	"context"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

type EndpointID = kernel.EndpointID

const (
	EndpointPlan    EndpointID = "plan"
	EndpointExecute EndpointID = "execute"
	EndpointVerify  EndpointID = "verify"
)

type TaskOutcome string

const (
	TaskActive   TaskOutcome = "active"
	TaskDone     TaskOutcome = "done"
	TaskCanceled TaskOutcome = "canceled"
	TaskFailed   TaskOutcome = "failed"
)

type EndpointState string

const (
	EndpointPending   EndpointState = "pending"
	EndpointSubmitted EndpointState = "submitted"
	EndpointSatisfied EndpointState = "satisfied"
	EndpointRejected  EndpointState = "rejected"
)

type RunPolicy string

const (
	RunEnabled RunPolicy = "enabled"
	RunHeld    RunPolicy = "held"
)

type EdgeSignal string

const (
	SignalPhaseSatisfied EdgeSignal = "phase_satisfied"
	SignalTaskDone       EdgeSignal = "task_done"
)

type RequiredBy string

const (
	RequiredByStart      RequiredBy = "start"
	RequiredByCompletion RequiredBy = "completion"
)

type OnFalse string

const (
	OnFalseBlock  OnFalse = "block"
	OnFalseReplan OnFalse = "replan"
	OnFalseCancel OnFalse = "cancel"
)

type BlockerState string

const (
	BlockerActive   BlockerState = "active"
	BlockerResolved BlockerState = "resolved"
	BlockerDenied   BlockerState = "denied"
	BlockerObsolete BlockerState = "obsolete"
)

type Verdict string

const (
	VerdictSubmitted   Verdict = "submitted"
	VerdictSatisfied   Verdict = "satisfied"
	VerdictRejected    Verdict = "rejected"
	VerdictInvalidated Verdict = "invalidated"
)

type CommandAction string

const (
	CommandStart  CommandAction = "start"
	CommandStop   CommandAction = "stop"
	CommandResume CommandAction = "resume"
)

type TransitionTarget string

const (
	TargetPhaseEndpoint TransitionTarget = "phase_endpoint"
	TargetBlocker       TransitionTarget = "blocker"
	TargetTask          TransitionTarget = "task"
)

type PhaseEndpointRef struct {
	TaskID     kernel.TaskID `json:"task_id"`
	EndpointID EndpointID    `json:"endpoint_id"`
}

type Task struct {
	ID          kernel.TaskID `json:"id"`
	ContractRef string        `json:"contract_ref"`
	Outcome     TaskOutcome   `json:"outcome"`
}

type PhaseEndpoint struct {
	Ref        PhaseEndpointRef  `json:"ref"`
	SpecRef    string            `json:"spec_ref"`
	BindingRef kernel.BindingRef `json:"binding_ref"`
	Generation int               `json:"generation"`
	State      EndpointState     `json:"state"`
	RunPolicy  RunPolicy         `json:"run_policy"`
}

type Edge struct {
	From          PhaseEndpointRef `json:"from"`
	To            PhaseEndpointRef `json:"to"`
	Signal        EdgeSignal       `json:"signal"`
	RequiredBy    RequiredBy       `json:"required_by"`
	ArtifactKinds []string         `json:"artifact_kinds"`
	OnFalse       OnFalse          `json:"on_false"`
}

type Blocker struct {
	ID         string           `json:"id"`
	Target     PhaseEndpointRef `json:"target"`
	RequiredBy RequiredBy       `json:"required_by"`
	OnFalse    OnFalse          `json:"on_false"`
	State      BlockerState     `json:"state"`
}

type PhaseResult struct {
	ID         string            `json:"id"`
	Endpoint   PhaseEndpointRef  `json:"endpoint"`
	BindingRef kernel.BindingRef `json:"binding_ref"`
	OutputRef  string            `json:"output_ref"`
	Verdict    Verdict           `json:"verdict"`
}

type PhaseCommand struct {
	ID         string            `json:"id"`
	Endpoint   PhaseEndpointRef  `json:"endpoint"`
	Generation int               `json:"generation"`
	BindingRef kernel.BindingRef `json:"binding_ref"`
	LeaseRef   kernel.LeaseID    `json:"lease_ref"`
	Action     CommandAction     `json:"action"`
	CauseRef   string            `json:"cause_ref"`
}

type GraphSnapshot struct {
	Revision  kernel.Revision `json:"revision"`
	Tasks     []Task          `json:"tasks"`
	Endpoints []PhaseEndpoint `json:"endpoints"`
	Edges     []Edge          `json:"edges"`
	Blockers  []Blocker       `json:"blockers"`
	Results   []PhaseResult   `json:"results"`
}

type PendingSubgraph struct {
	RequestID    kernel.IdempotencyKey `json:"request_id"`
	BaseRevision kernel.Revision       `json:"base_revision"`
	Tasks        []Task                `json:"tasks,omitempty"`
	Endpoints    []PhaseEndpoint       `json:"endpoints"`
	Edges        []Edge                `json:"edges"`
	Blockers     []Blocker             `json:"blockers"`
}

type GraphTransition struct {
	TargetKind        TransitionTarget  `json:"target_kind"`
	Endpoint          PhaseEndpointRef  `json:"endpoint,omitempty"`
	BlockerID         string            `json:"blocker_id,omitempty"`
	TaskID            kernel.TaskID     `json:"task_id,omitempty"`
	Action            string            `json:"action"`
	Generation        int               `json:"generation,omitempty"`
	Result            PhaseResult       `json:"result,omitempty"`
	NewBindingRef     kernel.BindingRef `json:"new_binding_ref,omitempty"`
	ExecuteBindingRef kernel.BindingRef `json:"execute_binding_ref,omitempty"`
	VerifyBindingRef  kernel.BindingRef `json:"verify_binding_ref,omitempty"`
	NewSpecRef        string            `json:"new_spec_ref,omitempty"`
	CheckpointRef     string            `json:"checkpoint_ref,omitempty"`
	NonResumable      bool              `json:"non_resumable,omitempty"`
	EvidenceRefs      []string          `json:"evidence_refs,omitempty"`
}

type TaskManagerGraph interface {
	Snapshot(ctx context.Context, revision kernel.Revision) (GraphSnapshot, error)
	ReplacePending(ctx context.Context, next PendingSubgraph) (kernel.Revision, error)
	Transition(ctx context.Context, expectedRevision kernel.Revision, transitionRef string) (kernel.Revision, error)
}

type PhaseController interface {
	Apply(ctx context.Context, command PhaseCommand) error
}

type Store interface {
	Latest(ctx context.Context, projectID kernel.ProjectID) (GraphSnapshot, error)
	Snapshot(ctx context.Context, projectID kernel.ProjectID, revision kernel.Revision) (GraphSnapshot, error)
	ReplacePending(ctx context.Context, projectID kernel.ProjectID, next PendingSubgraph) (GraphSnapshot, error)
	TransitionWithDecisionRef(ctx context.Context, projectID kernel.ProjectID, expectedRevision kernel.Revision, decisionRef string, transition GraphTransition) (GraphSnapshot, error)
}

type DecisionKind string

const (
	DecisionReplacePending DecisionKind = "replace_pending"
	DecisionTransition     DecisionKind = "transition"
)

type DecisionLog interface {
	AuthorizeReplacePending(ctx context.Context, projectID kernel.ProjectID, decisionRef kernel.IdempotencyKey) error
	ResolveTransition(ctx context.Context, projectID kernel.ProjectID, transitionRef string) (GraphTransition, error)
}
