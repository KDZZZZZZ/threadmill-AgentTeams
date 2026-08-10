package taskmanager

import (
	"context"
	"fmt"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

type DeliveryPolicy string

const (
	DeliveryPolicyNonCodeArtifact  DeliveryPolicy = "non_code_artifact"
	DeliveryPolicyCodeMerge        DeliveryPolicy = "code_merge"
	DeliveryPolicyHumanAcceptance  DeliveryPolicy = "human_acceptance"
	DeliveryPolicyExternalDelivery DeliveryPolicy = "external_delivery"
)

type ReplyStatus string

const (
	ReplyAccepted ReplyStatus = "accepted"
	ReplyRejected ReplyStatus = "rejected"
	ReplyConflict ReplyStatus = "conflict"
	ReplyDeferred ReplyStatus = "deferred"
)

type DecisionKind string

const (
	DecisionKindReplacePending DecisionKind = "replace_pending"
	DecisionKindTransition     DecisionKind = "transition"
	DecisionKindTerminal       DecisionKind = "terminal"
)

type RequirementInput struct {
	InputRef    string
	TaskID      kernel.TaskID
	ContractRef string
	Requirement Requirement
}

type Requirement struct {
	Text         string   `json:"text"`
	Goal         string   `json:"goal,omitempty"`
	Constraints  []string `json:"constraints,omitempty"`
	EvidenceRefs []string `json:"evidence_refs,omitempty"`
}

// ManagerDecisionInput is trusted Runtime context for a Task Manager decision
// about one persisted ManagerInputRef. The natural-language browser message is
// deliberately absent: it cannot be interpreted as a graph command by this
// application service. The Task Manager Agent supplies TaskManagerDecision as
// its separate structured output.
type ManagerDecisionInput struct {
	InputRef     string
	Endpoint     coordination.PhaseEndpointRef
	SeenRevision kernel.Revision
}

type PhaseStoppedInput struct {
	InputRef            string
	Endpoint            coordination.PhaseEndpointRef
	CommandID           string
	LeaseRef            kernel.LeaseID
	Generation          int
	EvidenceRefs        []string
	CheckpointRef       string
	NonResumable        bool
	NewBindingRef       kernel.BindingRef
	NewSpecRef          string
	Replacement         Replacement
	ReleaseAfterReplace bool
}

type Replacement struct {
	Endpoints []coordination.PhaseEndpoint
	Edges     []coordination.Edge
	Blockers  []coordination.Blocker
}

type DeliveryInput struct {
	InputRef string
	TaskID   kernel.TaskID
	Evidence DeliveryEvidence
}

type DeliveryEvidence struct {
	ArtifactRefs       []string
	EvidenceRefs       []string
	ExternalDelivered  bool
	HumanAccepted      bool
	LatestMainVerified bool
	MergeSucceeded     bool
	MergeCommitRef     string
}

type TaskManagerDecision struct {
	Action       string   `json:"action"`
	TargetRef    string   `json:"target_ref,omitempty"`
	EvidenceRefs []string `json:"evidence_refs,omitempty"`
	Reason       string   `json:"reason"`
}

type DecisionSubmission struct {
	ProjectID        kernel.ProjectID
	InputRef         string
	ExpectedRevision kernel.Revision
	Decision         TaskManagerDecision
	Kind             DecisionKind
	Transition       coordination.GraphTransition
}

type TaskContract struct {
	TaskID         kernel.TaskID
	ContractRef    string
	DeliveryPolicy DeliveryPolicy
	PhaseSpecs     map[coordination.EndpointID]string
}

type ProjectionRequest struct {
	ProjectionID   string
	TaskID         kernel.TaskID
	SourceRevision kernel.Revision
	Requirement    Requirement
}

type ManagerReplyEvent struct {
	InputRef      string
	Status        ReplyStatus
	DecisionRef   string
	GraphRevision kernel.Revision
	Reason        string
}

type Result struct {
	InputRef      string
	Status        ReplyStatus
	DecisionRef   string
	GraphRevision kernel.Revision
}

type DecisionStore interface {
	SubmitDecision(context.Context, DecisionSubmission) (string, error)
}

type ContractStore interface {
	ResolveRequirementContract(context.Context, RequirementInput) (TaskContract, error)
	TaskContract(context.Context, kernel.TaskID) (TaskContract, error)
}

type TaskContextProjector interface {
	RegisterTaskSubgraph(context.Context, kernel.TaskID) error
	EnqueueProjection(context.Context, ProjectionRequest) error
	RetryProjection(context.Context, string) error
}

type ReplyRecorder interface {
	AppendManagerReply(context.Context, ManagerReplyEvent) error
}

type MemoryFinalizer interface {
	FinalizeTaskMemory(context.Context, kernel.TaskID, string) error
}

type Options struct {
	ProjectID      kernel.ProjectID
	Graph          coordination.TaskManagerGraph
	Decisions      DecisionStore
	Contracts      ContractStore
	TaskContext    TaskContextProjector
	Replies        ReplyRecorder
	MemoryFinalize MemoryFinalizer
}

type Manager struct {
	projectID      kernel.ProjectID
	graph          coordination.TaskManagerGraph
	decisions      DecisionStore
	contracts      ContractStore
	taskContext    TaskContextProjector
	replies        ReplyRecorder
	memoryFinalize MemoryFinalizer

	finalizeBatches map[kernel.TaskID]string
	finalized       map[kernel.TaskID]bool
}

func NewManager(options Options) *Manager {
	return &Manager{
		projectID:       options.ProjectID,
		graph:           options.Graph,
		decisions:       options.Decisions,
		contracts:       options.Contracts,
		taskContext:     options.TaskContext,
		replies:         options.Replies,
		memoryFinalize:  options.MemoryFinalize,
		finalizeBatches: make(map[kernel.TaskID]string),
		finalized:       make(map[kernel.TaskID]bool),
	}
}

func (m *Manager) HandleRequirement(ctx context.Context, input RequirementInput) (Result, error) {
	if err := validateRequirementInput(input); err != nil {
		return Result{}, err
	}
	contract, err := m.contracts.ResolveRequirementContract(ctx, input)
	if err != nil {
		return Result{}, err
	}
	snapshot, err := m.graph.Snapshot(ctx, kernel.LatestRevision)
	if err != nil {
		return Result{}, err
	}
	if err := validateTaskContract(contract); err != nil {
		decisionRef, submitErr := m.submitTerminalDecision(ctx, input.InputRef, snapshot.Revision, TaskManagerDecision{
			Action:    "defer",
			TargetRef: string(input.TaskID),
			Reason:    err.Error(),
		})
		if submitErr != nil {
			return Result{}, submitErr
		}
		return m.reply(ctx, ManagerReplyEvent{InputRef: input.InputRef, Status: ReplyDeferred, DecisionRef: decisionRef, GraphRevision: snapshot.Revision, Reason: err.Error()}, nil)
	}
	subgraph := requirementSubgraph(contract, snapshot.Revision)
	decision := TaskManagerDecision{
		Action:    string(DecisionKindReplacePending),
		TargetRef: string(contract.TaskID),
		Reason:    "create task from persisted requirement input",
	}
	ref, err := m.replacePending(ctx, input.InputRef, snapshot.Revision, decision, subgraph)
	if err != nil {
		if kernel.IsCode(err, kernel.CodeRevisionConflict) {
			return m.reply(ctx, ManagerReplyEvent{InputRef: input.InputRef, Status: ReplyConflict, DecisionRef: ref, GraphRevision: snapshot.Revision, Reason: "graph revision conflict"}, err)
		}
		return Result{}, err
	}
	revision := latestRevision(ctx, m.graph)
	if m.taskContext != nil {
		if err := m.taskContext.RegisterTaskSubgraph(ctx, contract.TaskID); err != nil {
			return m.reply(ctx, ManagerReplyEvent{InputRef: input.InputRef, Status: ReplyDeferred, DecisionRef: ref, GraphRevision: revision, Reason: "graph applied; task context registration requires retry"}, err)
		}
		projection := ProjectionRequest{
			ProjectionID:   projectionID(contract.TaskID, revision),
			TaskID:         contract.TaskID,
			SourceRevision: revision,
			Requirement:    input.Requirement,
		}
		if err := m.taskContext.EnqueueProjection(ctx, projection); err != nil {
			return m.reply(ctx, ManagerReplyEvent{InputRef: input.InputRef, Status: ReplyDeferred, DecisionRef: ref, GraphRevision: revision, Reason: "graph applied; task context projection requires retry"}, err)
		}
	}
	return Result{InputRef: input.InputRef, Status: ReplyAccepted, DecisionRef: ref, GraphRevision: revision}, nil
}

func (m *Manager) HandleManagerDecision(ctx context.Context, input ManagerDecisionInput, decision TaskManagerDecision) (Result, error) {
	if input.InputRef == "" {
		return Result{}, kernel.InvalidArgument("input_ref is required")
	}
	if decision.Action == "" || decision.Reason == "" {
		return Result{}, kernel.InvalidArgument("manager decision action and reason are required")
	}
	snapshot, err := m.graph.Snapshot(ctx, kernel.LatestRevision)
	if err != nil {
		return Result{}, err
	}
	if input.SeenRevision != 0 && input.SeenRevision != snapshot.Revision {
		decisionRef, submitErr := m.submitTerminalDecision(ctx, input.InputRef, snapshot.Revision, TaskManagerDecision{
			Action: "reject",
			Reason: "manager input was based on a stale graph revision",
		})
		if submitErr != nil {
			return Result{}, submitErr
		}
		return m.reply(ctx, ManagerReplyEvent{InputRef: input.InputRef, Status: ReplyConflict, DecisionRef: decisionRef, GraphRevision: snapshot.Revision, Reason: "manager input was based on a stale graph revision"}, kernel.RevisionConflict(input.SeenRevision, snapshot.Revision))
	}
	switch decision.Action {
	case "reject", "defer", "no_change":
		if decision.TargetRef != "" {
			return Result{}, kernel.InvalidArgument("terminal manager decision must omit target_ref")
		}
		decisionRef, submitErr := m.submitTerminalDecision(ctx, input.InputRef, snapshot.Revision, decision)
		if submitErr != nil {
			return Result{}, submitErr
		}
		status := ReplyAccepted
		if decision.Action == "reject" {
			status = ReplyRejected
		} else if decision.Action == "defer" {
			status = ReplyDeferred
		}
		return m.reply(ctx, ManagerReplyEvent{InputRef: input.InputRef, Status: status, DecisionRef: decisionRef, GraphRevision: snapshot.Revision, Reason: decision.Reason}, nil)
	case "held", "released":
		// These are the only direct endpoint-control decisions handled here.
		// stopped remains a Runtime-authenticated boundary input and replacement
		// remains the canonical replacePending path.
	default:
		return Result{}, kernel.InvalidArgument("manager decision must be reject, defer, no_change, held, or released")
	}

	endpoint, ok := findEndpoint(snapshot, input.Endpoint)
	if !ok {
		return Result{}, kernel.Error{Code: kernel.CodeNotFound, Message: "manager decision target endpoint was not found"}
	}
	wantTarget := targetRef(input.Endpoint)
	if decision.TargetRef != wantTarget {
		return Result{}, kernel.InvalidArgument("manager decision target_ref does not match Runtime-selected endpoint")
	}
	if decision.Action == "held" && endpoint.RunPolicy == coordination.RunHeld {
		return Result{}, kernel.TransitionRejected("endpoint is already held")
	}
	if decision.Action == "released" && endpoint.RunPolicy != coordination.RunHeld {
		return Result{}, kernel.TransitionRejected("endpoint is not held")
	}
	decisionRef, err := m.transition(ctx, input.InputRef, snapshot.Revision, decision, coordination.GraphTransition{
		TargetKind: coordination.TargetPhaseEndpoint,
		Endpoint:   input.Endpoint,
		Action:     decision.Action,
		Generation: endpoint.Generation,
	})
	if err != nil {
		return Result{}, err
	}
	snapshot, err = m.graph.Snapshot(ctx, kernel.LatestRevision)
	if err != nil {
		return Result{}, err
	}
	return m.reply(ctx, ManagerReplyEvent{InputRef: input.InputRef, Status: ReplyAccepted, DecisionRef: decisionRef, GraphRevision: snapshot.Revision, Reason: decision.Reason}, nil)
}

func (m *Manager) HandlePhaseStopped(ctx context.Context, input PhaseStoppedInput) (Result, error) {
	if err := validateStoppedInput(input); err != nil {
		return Result{}, err
	}
	snapshot, err := m.graph.Snapshot(ctx, kernel.LatestRevision)
	if err != nil {
		return Result{}, err
	}
	endpoint, ok := findEndpoint(snapshot, input.Endpoint)
	if !ok {
		return m.stoppedRejected(ctx, input, snapshot.Revision, "stopped endpoint not found", kernel.Error{Code: kernel.CodeNotFound, Message: "stopped endpoint not found"})
	}
	if endpoint.Generation != input.Generation {
		return m.stoppedRejected(ctx, input, snapshot.Revision, "stopped event generation does not match current endpoint", kernel.TransitionRejected("stopped event generation does not match current endpoint"))
	}
	if endpoint.RunPolicy != coordination.RunHeld {
		return m.stoppedRejected(ctx, input, snapshot.Revision, "stopped event requires a held endpoint", kernel.TransitionRejected("stopped event requires a held endpoint"))
	}
	decisionRef, err := m.transition(ctx, input.InputRef, snapshot.Revision, TaskManagerDecision{
		Action:       "stopped",
		TargetRef:    targetRef(input.Endpoint),
		EvidenceRefs: input.EvidenceRefs,
		Reason:       "runtime-authenticated phase stopped event",
	}, coordination.GraphTransition{
		TargetKind:    coordination.TargetPhaseEndpoint,
		Endpoint:      input.Endpoint,
		Action:        "stopped",
		Generation:    input.Generation,
		NewBindingRef: input.NewBindingRef,
		NewSpecRef:    input.NewSpecRef,
		CheckpointRef: input.CheckpointRef,
		NonResumable:  input.NonResumable,
		EvidenceRefs:  input.EvidenceRefs,
	})
	if err != nil {
		return Result{}, err
	}
	snapshot, err = m.graph.Snapshot(ctx, kernel.LatestRevision)
	if err != nil {
		return Result{}, err
	}
	endpoint, _ = findEndpoint(snapshot, input.Endpoint)
	if len(input.Replacement.Endpoints) > 0 {
		replacement := normalizeReplacement(input.Replacement, endpoint, snapshot.Revision)
		decisionRef, err = m.replacePending(ctx, input.InputRef, snapshot.Revision, TaskManagerDecision{
			Action:    string(DecisionKindReplacePending),
			TargetRef: targetRef(input.Endpoint),
			Reason:    "apply replacement after authenticated stopped event",
		}, replacement)
		if err != nil {
			return Result{}, err
		}
		snapshot, err = m.graph.Snapshot(ctx, kernel.LatestRevision)
		if err != nil {
			return Result{}, err
		}
		endpoint, _ = findEndpoint(snapshot, input.Endpoint)
	}
	if input.ReleaseAfterReplace {
		decisionRef, err = m.transition(ctx, input.InputRef, snapshot.Revision, TaskManagerDecision{
			Action:    "released",
			TargetRef: targetRef(input.Endpoint),
			Reason:    "release endpoint after authenticated stopped replacement",
		}, coordination.GraphTransition{
			TargetKind: coordination.TargetPhaseEndpoint,
			Endpoint:   input.Endpoint,
			Action:     "released",
			Generation: endpoint.Generation,
		})
		if err != nil {
			return Result{}, err
		}
		snapshot, err = m.graph.Snapshot(ctx, kernel.LatestRevision)
		if err != nil {
			return Result{}, err
		}
	}
	return Result{InputRef: input.InputRef, Status: ReplyAccepted, DecisionRef: decisionRef, GraphRevision: snapshot.Revision}, nil
}

func (m *Manager) HandleDelivery(ctx context.Context, input DeliveryInput) (Result, error) {
	snapshot, err := m.graph.Snapshot(ctx, kernel.LatestRevision)
	if err != nil {
		return Result{}, err
	}
	if _, ok := findTask(snapshot, input.TaskID); !ok {
		return Result{}, kernel.Error{Code: kernel.CodeNotFound, Message: "delivery task not found"}
	}
	contract, err := m.contracts.TaskContract(ctx, input.TaskID)
	if err != nil {
		return Result{}, err
	}
	if contract.DeliveryPolicy == "" {
		decisionRef, submitErr := m.submitTerminalDecision(ctx, input.InputRef, snapshot.Revision, TaskManagerDecision{
			Action:    "defer",
			TargetRef: string(input.TaskID),
			Reason:    "task contract is missing delivery policy",
		})
		if submitErr != nil {
			return Result{}, submitErr
		}
		return m.reply(ctx, ManagerReplyEvent{InputRef: input.InputRef, Status: ReplyDeferred, DecisionRef: decisionRef, GraphRevision: snapshot.Revision, Reason: "task contract is missing delivery policy"}, nil)
	}
	if !deliverySatisfied(contract.DeliveryPolicy, input.Evidence) {
		decisionRef, submitErr := m.submitTerminalDecision(ctx, input.InputRef, snapshot.Revision, TaskManagerDecision{
			Action:    "defer",
			TargetRef: string(input.TaskID),
			Reason:    "delivery policy evidence is incomplete",
		})
		if submitErr != nil {
			return Result{}, submitErr
		}
		return m.reply(ctx, ManagerReplyEvent{InputRef: input.InputRef, Status: ReplyDeferred, DecisionRef: decisionRef, GraphRevision: snapshot.Revision, Reason: "delivery policy evidence is incomplete"}, nil)
	}
	verify, ok := findEndpoint(snapshot, coordination.PhaseEndpointRef{TaskID: input.TaskID, EndpointID: coordination.EndpointVerify})
	if !ok || verify.State != coordination.EndpointSatisfied {
		decisionRef, submitErr := m.submitTerminalDecision(ctx, input.InputRef, snapshot.Revision, TaskManagerDecision{
			Action:    "defer",
			TargetRef: string(input.TaskID),
			Reason:    "verify endpoint is not satisfied",
		})
		if submitErr != nil {
			return Result{}, submitErr
		}
		return m.reply(ctx, ManagerReplyEvent{InputRef: input.InputRef, Status: ReplyDeferred, DecisionRef: decisionRef, GraphRevision: snapshot.Revision, Reason: "verify endpoint is not satisfied"}, nil)
	}
	decisionRef, err := m.transition(ctx, input.InputRef, snapshot.Revision, TaskManagerDecision{
		Action:    string(coordination.TaskDone),
		TargetRef: string(input.TaskID),
		Reason:    "delivery policy and verify evidence satisfied",
	}, coordination.GraphTransition{
		TargetKind: coordination.TargetTask,
		TaskID:     input.TaskID,
		Action:     string(coordination.TaskDone),
	})
	if err != nil {
		return Result{}, err
	}
	snapshot, err = m.graph.Snapshot(ctx, kernel.LatestRevision)
	if err != nil {
		return Result{}, err
	}
	result := Result{InputRef: input.InputRef, Status: ReplyAccepted, DecisionRef: decisionRef, GraphRevision: snapshot.Revision}
	_, _ = m.RetryFinalize(ctx, input.TaskID)
	return result, nil
}

func (m *Manager) RetryProjection(ctx context.Context, projectionID string) error {
	if m.taskContext == nil {
		return nil
	}
	return m.taskContext.RetryProjection(ctx, projectionID)
}

func (m *Manager) RetryFinalize(ctx context.Context, taskID kernel.TaskID) (Result, error) {
	if m.memoryFinalize == nil {
		return Result{Status: ReplyAccepted}, nil
	}
	if m.finalized[taskID] {
		return Result{Status: ReplyAccepted}, nil
	}
	batchID := m.finalizeBatches[taskID]
	if batchID == "" {
		batchID = fmt.Sprintf("task-memory-freeze:%s", taskID)
		m.finalizeBatches[taskID] = batchID
	}
	if err := m.memoryFinalize.FinalizeTaskMemory(ctx, taskID, batchID); err != nil {
		return Result{Status: ReplyDeferred}, err
	}
	m.finalized[taskID] = true
	return Result{Status: ReplyAccepted}, nil
}

func (m *Manager) replacePending(ctx context.Context, inputRef string, revision kernel.Revision, decision TaskManagerDecision, subgraph coordination.PendingSubgraph) (string, error) {
	ref, err := m.decisions.SubmitDecision(ctx, DecisionSubmission{
		ProjectID:        m.projectID,
		InputRef:         inputRef,
		ExpectedRevision: revision,
		Decision:         decision,
		Kind:             DecisionKindReplacePending,
	})
	if err != nil {
		return "", err
	}
	subgraph.RequestID = kernel.IdempotencyKey(ref)
	if _, err := m.graph.ReplacePending(ctx, subgraph); err != nil {
		return ref, err
	}
	return ref, nil
}

func (m *Manager) transition(ctx context.Context, inputRef string, revision kernel.Revision, decision TaskManagerDecision, transition coordination.GraphTransition) (string, error) {
	ref, err := m.decisions.SubmitDecision(ctx, DecisionSubmission{
		ProjectID:        m.projectID,
		InputRef:         inputRef,
		ExpectedRevision: revision,
		Decision:         decision,
		Kind:             DecisionKindTransition,
		Transition:       transition,
	})
	if err != nil {
		return "", err
	}
	if _, err := m.graph.Transition(ctx, revision, ref); err != nil {
		return ref, err
	}
	return ref, nil
}

func (m *Manager) submitTerminalDecision(ctx context.Context, inputRef string, revision kernel.Revision, decision TaskManagerDecision) (string, error) {
	return m.decisions.SubmitDecision(ctx, DecisionSubmission{
		ProjectID:        m.projectID,
		InputRef:         inputRef,
		ExpectedRevision: revision,
		Decision:         decision,
		Kind:             DecisionKindTerminal,
	})
}

func (m *Manager) stoppedRejected(ctx context.Context, input PhaseStoppedInput, revision kernel.Revision, reason string, err error) (Result, error) {
	decisionRef, submitErr := m.submitTerminalDecision(ctx, input.InputRef, revision, TaskManagerDecision{
		Action:       "reject",
		TargetRef:    targetRef(input.Endpoint),
		EvidenceRefs: input.EvidenceRefs,
		Reason:       reason,
	})
	if submitErr != nil {
		return Result{}, submitErr
	}
	return Result{InputRef: input.InputRef, Status: ReplyRejected, DecisionRef: decisionRef, GraphRevision: revision}, err
}

func (m *Manager) reply(ctx context.Context, reply ManagerReplyEvent, err error) (Result, error) {
	if m.replies != nil {
		if appendErr := m.replies.AppendManagerReply(ctx, reply); appendErr != nil && err == nil {
			err = appendErr
		}
	}
	return Result{InputRef: reply.InputRef, Status: reply.Status, DecisionRef: reply.DecisionRef, GraphRevision: reply.GraphRevision}, err
}

func validateRequirementInput(input RequirementInput) error {
	if input.InputRef == "" {
		return kernel.InvalidArgument("input_ref is required")
	}
	if err := kernel.RequireID("requirement_input.task_id", input.TaskID); err != nil {
		return err
	}
	if input.ContractRef == "" {
		return kernel.InvalidArgument("requirement_input.contract_ref is required")
	}
	return nil
}

func validateTaskContract(contract TaskContract) error {
	if err := kernel.RequireID("task_contract.task_id", contract.TaskID); err != nil {
		return err
	}
	if contract.ContractRef == "" {
		return kernel.InvalidArgument("task contract_ref is required")
	}
	switch contract.DeliveryPolicy {
	case DeliveryPolicyNonCodeArtifact, DeliveryPolicyCodeMerge, DeliveryPolicyHumanAcceptance, DeliveryPolicyExternalDelivery:
	default:
		return kernel.InvalidArgument("task contract delivery policy is required")
	}
	for _, endpointID := range []coordination.EndpointID{coordination.EndpointPlan, coordination.EndpointExecute, coordination.EndpointVerify} {
		if contract.PhaseSpecs[endpointID] == "" {
			return kernel.InvalidArgument(fmt.Sprintf("task contract %s spec_ref is required", endpointID))
		}
	}
	return nil
}

func validateStoppedInput(input PhaseStoppedInput) error {
	if input.InputRef == "" {
		return kernel.InvalidArgument("input_ref is required")
	}
	if input.CommandID == "" {
		return kernel.InvalidArgument("stopped command_id is required")
	}
	if err := kernel.RequireID("stopped lease_ref", input.LeaseRef); err != nil {
		return err
	}
	if input.Generation <= 0 {
		return kernel.InvalidArgument("stopped generation is required")
	}
	if len(input.EvidenceRefs) == 0 {
		return kernel.IncompleteStopEvidence("stopped event requires evidence_refs")
	}
	if input.NewBindingRef == "" {
		return kernel.InvalidArgument("stopped new_binding_ref is required")
	}
	if input.CheckpointRef == "" && !input.NonResumable {
		return kernel.IncompleteStopEvidence("stopped event requires checkpoint_ref or non_resumable")
	}
	return nil
}

func requirementSubgraph(contract TaskContract, revision kernel.Revision) coordination.PendingSubgraph {
	taskID := contract.TaskID
	return coordination.PendingSubgraph{
		BaseRevision: revision,
		Tasks: []coordination.Task{{
			ID:          taskID,
			ContractRef: contract.ContractRef,
			Outcome:     coordination.TaskActive,
		}},
		Endpoints: []coordination.PhaseEndpoint{
			newEndpoint(contract, coordination.EndpointPlan),
			newEndpoint(contract, coordination.EndpointExecute),
			newEndpoint(contract, coordination.EndpointVerify),
		},
	}
}

func newEndpoint(contract TaskContract, endpointID coordination.EndpointID) coordination.PhaseEndpoint {
	return coordination.PhaseEndpoint{
		Ref:        coordination.PhaseEndpointRef{TaskID: contract.TaskID, EndpointID: endpointID},
		SpecRef:    contract.PhaseSpecs[endpointID],
		BindingRef: kernel.BindingRef(fmt.Sprintf("binding://%s/%s/1", contract.TaskID, endpointID)),
		Generation: 1,
		State:      coordination.EndpointPending,
		RunPolicy:  coordination.RunEnabled,
	}
}

func normalizeReplacement(replacement Replacement, current coordination.PhaseEndpoint, revision kernel.Revision) coordination.PendingSubgraph {
	endpoints := append([]coordination.PhaseEndpoint(nil), replacement.Endpoints...)
	for i := range endpoints {
		if endpoints[i].Ref == current.Ref {
			endpoints[i].Generation = current.Generation
			endpoints[i].BindingRef = current.BindingRef
			endpoints[i].SpecRef = current.SpecRef
			endpoints[i].State = current.State
			endpoints[i].RunPolicy = current.RunPolicy
		}
	}
	return coordination.PendingSubgraph{
		BaseRevision: revision,
		Endpoints:    endpoints,
		Edges:        append([]coordination.Edge(nil), replacement.Edges...),
		Blockers:     append([]coordination.Blocker(nil), replacement.Blockers...),
	}
}

func deliverySatisfied(policy DeliveryPolicy, evidence DeliveryEvidence) bool {
	switch policy {
	case DeliveryPolicyNonCodeArtifact:
		return len(evidence.ArtifactRefs) > 0
	case DeliveryPolicyHumanAcceptance:
		return evidence.HumanAccepted
	case DeliveryPolicyCodeMerge:
		return evidence.LatestMainVerified && evidence.MergeSucceeded && evidence.MergeCommitRef != ""
	case DeliveryPolicyExternalDelivery:
		return evidence.ExternalDelivered && len(evidence.EvidenceRefs) > 0
	default:
		return false
	}
}

func findEndpoint(snapshot coordination.GraphSnapshot, ref coordination.PhaseEndpointRef) (coordination.PhaseEndpoint, bool) {
	for _, endpoint := range snapshot.Endpoints {
		if endpoint.Ref == ref {
			return endpoint, true
		}
	}
	return coordination.PhaseEndpoint{}, false
}

func findTask(snapshot coordination.GraphSnapshot, id kernel.TaskID) (coordination.Task, bool) {
	for _, task := range snapshot.Tasks {
		if task.ID == id {
			return task, true
		}
	}
	return coordination.Task{}, false
}

func targetRef(ref coordination.PhaseEndpointRef) string {
	return fmt.Sprintf("%s/%s", ref.TaskID, ref.EndpointID)
}

func projectionID(taskID kernel.TaskID, revision kernel.Revision) string {
	return fmt.Sprintf("task-context:%s:%d", taskID, revision)
}

func latestRevision(ctx context.Context, graph coordination.TaskManagerGraph) kernel.Revision {
	snapshot, err := graph.Snapshot(ctx, kernel.LatestRevision)
	if err != nil {
		return 0
	}
	return snapshot.Revision
}
