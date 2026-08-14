package app

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/contextgraph"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/evidence"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/mergequeue"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	phasepkg "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime/phase"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/taskmanager"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/transport/httpapi"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/transport/mcpapi"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/uiprojection"
)

// productionTaskManagerRuntime binds every MCP mutation to the immutable input
// row selected when the invocation was created. InputRef, graph revision,
// endpoint and decision reference never come from Agent JSON.
type productionTaskManagerRuntime struct {
	db            *sql.DB
	projectID     kernel.ProjectID
	graphStore    *coordination.PostgresStore
	decisions     productionTaskManagerDecisionStore
	idempotency   kernel.IdempotencyStore
	workspaces    productionTaskWorkspaceProvisioner
	contexts      productionTaskContextProjector
	memory        contextgraph.TaskMemoryFinalizer
	merge         productionTaskMergeScheduler
	mergeEvidence productionTaskMergeEvidenceReader
	followups     productionTaskManagerFollowupDispatcher
	events        evidence.EventStore
	now           func() time.Time
}

type productionTaskManagerDecisionStore interface {
	SubmitDecision(context.Context, taskmanager.DecisionSubmission) (string, error)
	PersistRequirementContract(context.Context, taskmanager.RequirementInput, taskmanager.TaskContract) error
	TaskContract(context.Context, kernel.TaskID) (taskmanager.TaskContract, error)
}

// These ports deliberately expose only Runtime-owned provisioning and
// projection operations. They are never added to the Task Manager Agent's
// tool set.
type productionTaskWorkspaceProvisioner interface {
	EnsureTaskWorkspace(context.Context, productionTaskWorkspaceRequest) (kernel.BindingRef, error)
}

type productionTaskContextProjector interface {
	EnsureTaskContext(context.Context, productionTaskContextRequest) error
}

type productionTaskManagerFollowupDispatcher interface {
	DispatchTaskManagerFollowup(context.Context, productionInput) (persistedProductionInput, error)
}

type productionTaskMergeScheduler interface {
	EnqueueExecutedTask(context.Context, productionPhaseEvaluationBoundary) error
}

type productionTaskMergeEvidenceReader interface {
	CodeMergeEvidence(context.Context, kernel.TaskID, string) (taskmanager.DeliveryEvidence, []string, bool, error)
}

type productionTaskWorkspaceRequest struct {
	TaskID     kernel.TaskID
	Generation int
	// BaseRevision is set only for a Manager-approved fresh round. Generation
	// zero asks the provisioner to allocate/reuse the next unused Task round.
	BaseRevision string
}

type productionTaskContextRequest struct {
	InvocationID  kernel.InvocationID
	InputRef      string
	TaskID        kernel.TaskID
	GraphRevision kernel.Revision
	Requirement   taskmanager.Requirement
	Contract      taskmanager.TaskContract
}

type productionTaskManagerBinding struct {
	InvocationID     kernel.InvocationID
	InputRef         string
	ConversationID   string
	SeenRevision     kernel.Revision
	SelectedTaskID   kernel.TaskID
	SelectedEndpoint coordination.EndpointID
	TargetKind       string
	TargetRef        string
	InputKind        string
	InputBody        string
	InputPayload     json.RawMessage
	DecisionRef      string
	DecisionKind     taskmanager.DecisionKind
	DecisionAction   string
	MutationApplied  bool
	AppliedRevision  kernel.Revision
}

type productionPendingTaskResource struct {
	Input       taskmanager.RequirementInput
	Contract    taskmanager.TaskContract
	Generations []int
	IsNew       bool
}

type productionPendingPlan struct {
	Subgraph  coordination.PendingSubgraph
	Resources []productionPendingTaskResource
}

type productionPhaseOutputBoundary struct {
	OutputRef string                 `json:"output_ref"`
	Receipt   phasepkg.OutputReceipt `json:"receipt"`
}

type productionPhaseEvaluationBoundary struct {
	SourceInputRef string                        `json:"source_input_ref"`
	Output         productionPhaseOutputBoundary `json:"output"`
	Endpoint       coordination.PhaseEndpointRef `json:"endpoint"`
	Generation     int                           `json:"generation"`
	BindingRef     kernel.BindingRef             `json:"binding_ref"`
}

type productionTaskCompletionBoundary struct {
	SourceInputRef string                        `json:"source_input_ref"`
	TaskID         kernel.TaskID                 `json:"task_id"`
	VerifyEndpoint coordination.PhaseEndpointRef `json:"verify_endpoint"`
	VerifyOutput   productionPhaseOutputBoundary `json:"verify_output"`
}

type productionPhaseStoppedBoundary struct {
	CommandID     string                        `json:"command_id"`
	Endpoint      coordination.PhaseEndpointRef `json:"endpoint"`
	Generation    int                           `json:"generation"`
	BindingRef    kernel.BindingRef             `json:"binding_ref"`
	LeaseRef      kernel.LeaseID                `json:"lease_ref"`
	CheckpointRef string                        `json:"checkpoint_ref"`
	NonResumable  bool                          `json:"non_resumable"`
}

type productionPhaseFailedBoundary struct {
	CommandID     string                        `json:"command_id"`
	CommandAction coordination.CommandAction    `json:"command_action"`
	Endpoint      coordination.PhaseEndpointRef `json:"endpoint"`
	Generation    int                           `json:"generation"`
	BindingRef    kernel.BindingRef             `json:"binding_ref"`
	LeaseRef      kernel.LeaseID                `json:"lease_ref"`
}

type productionStopReleaseBoundary struct {
	SourceInputRef string                         `json:"source_input_ref"`
	Stopped        productionPhaseStoppedBoundary `json:"stopped"`
	NewGeneration  int                            `json:"new_generation"`
	NewBindingRef  kernel.BindingRef              `json:"new_binding_ref"`
}

func newProductionTaskManagerRuntime(db *sql.DB, projectID kernel.ProjectID, graphStore *coordination.PostgresStore, now func() time.Time) (*productionTaskManagerRuntime, error) {
	if db == nil || graphStore == nil || kernel.IsZeroID(projectID) {
		return nil, kernel.InvalidArgument("production Task Manager database, project, and graph are required")
	}
	if now == nil {
		now = time.Now
	}
	return &productionTaskManagerRuntime{db: db, projectID: projectID, graphStore: graphStore, decisions: taskmanager.NewPostgresStore(db, projectID, graphStore), idempotency: kernel.NewMemoryIdempotencyStore(), now: now}, nil
}

func (p *productionTaskManagerRuntime) setProductionDependencies(workspaces productionTaskWorkspaceProvisioner, contexts productionTaskContextProjector, followups productionTaskManagerFollowupDispatcher) error {
	if workspaces == nil || contexts == nil || followups == nil {
		return kernel.InvalidArgument("production Task Manager workspace, context, and follow-up dependencies are required")
	}
	p.workspaces = workspaces
	p.contexts = contexts
	p.followups = followups
	return nil
}

func (p *productionTaskManagerRuntime) setProductionMemoryFinalizer(finalizer contextgraph.TaskMemoryFinalizer) error {
	if finalizer == nil {
		return kernel.InvalidArgument("production Task Manager memory finalizer is required")
	}
	p.memory = finalizer
	return nil
}

func (p *productionTaskManagerRuntime) setProductionMergeQueue(scheduler productionTaskMergeScheduler, evidence productionTaskMergeEvidenceReader) error {
	if scheduler == nil || evidence == nil {
		return kernel.InvalidArgument("production Task Manager merge queue dependencies are required")
	}
	p.merge = scheduler
	p.mergeEvidence = evidence
	return nil
}

func (p *productionTaskManagerRuntime) setProductionEventStore(events evidence.EventStore) error {
	if events == nil {
		return kernel.InvalidArgument("production Task Manager event store is required")
	}
	p.events = events
	return nil
}

func (p *productionTaskManagerRuntime) Snapshot(ctx context.Context, caller auth.Principal, scope auth.BoundScope, revision kernel.Revision) (mcpapi.TaskManagerSnapshot, error) {
	if _, err := p.binding(ctx, caller, scope); err != nil {
		return mcpapi.TaskManagerSnapshot{}, err
	}
	snapshot, err := p.graph(caller).Snapshot(ctx, revision)
	if err != nil {
		return mcpapi.TaskManagerSnapshot{}, err
	}
	deliveries, err := p.taskManagerDeliveryStates(ctx, snapshot)
	if err != nil {
		return mcpapi.TaskManagerSnapshot{}, err
	}
	return mcpapi.TaskManagerSnapshot{GraphSnapshot: snapshot, Deliveries: deliveries}, nil
}

func (p *productionTaskManagerRuntime) taskManagerDeliveryStates(ctx context.Context, snapshot coordination.GraphSnapshot) ([]mcpapi.TaskManagerDeliveryState, error) {
	activeTasks := make(map[kernel.TaskID]struct{}, len(snapshot.Tasks))
	for _, task := range snapshot.Tasks {
		activeTasks[task.ID] = struct{}{}
	}
	rows, err := p.db.QueryContext(ctx, `
SELECT c.task_id,
       c.delivery_policy,
       COALESCE(latest.id, ''),
       COALESCE(latest.status, ''),
       COALESCE(delivery.status, ''),
       COALESCE(latest.failure_reason, ''),
       COALESCE(latest.failure_evidence_ref, ''),
       COALESCE(replan.target_ref, ''),
       CASE
         WHEN c.delivery_policy <> $2 THEN TRUE
         ELSE EXISTS (
           SELECT 1
           FROM merge_candidates merged
           JOIN production_merge_deliveries delivered
             ON delivered.project_id=merged.project_id
            AND delivered.candidate_id=merged.id
           WHERE merged.project_id=c.project_id
             AND merged.task_id=c.task_id
             AND merged.status='merged'
             AND delivered.status='delivered'
         )
       END AS ready_for_verify
FROM taskmanager_contracts c
LEFT JOIN LATERAL (
  SELECT candidate.id, candidate.status, candidate.failure_reason,
         candidate.failure_evidence_ref
  FROM merge_candidates candidate
  WHERE candidate.project_id=c.project_id AND candidate.task_id=c.task_id
  ORDER BY candidate.updated_at DESC, candidate.id DESC
  LIMIT 1
) latest ON TRUE
LEFT JOIN production_merge_deliveries delivery
  ON delivery.project_id=c.project_id AND delivery.candidate_id=latest.id
LEFT JOIN LATERAL (
  SELECT manager_input.target_ref
  FROM production_manager_inputs manager_input
  WHERE manager_input.project_id=c.project_id
    AND manager_input.input_kind='phase_orchestration'
    AND manager_input.target_kind='phase_orchestration'
    AND manager_input.selected_task_id=c.task_id
    AND manager_input.selected_endpoint_id=$3
    AND manager_input.status='completed'
    AND manager_input.payload->>'source_kind'=$4
    AND manager_input.payload->>'candidate_id'=latest.id
    AND manager_input.payload->>'orchestration_advice'=$5
    AND manager_input.target_ref=manager_input.payload->>'proposal_id'
  ORDER BY manager_input.created_at DESC, manager_input.input_ref DESC
  LIMIT 1
) replan ON TRUE
WHERE c.project_id=$1
ORDER BY c.task_id`, p.projectID, taskmanager.DeliveryPolicyCodeMerge, coordination.EndpointVerify, productionTargetedVerifyProposalSource, phasepkg.OrchestrationReplan)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	deliveries := make([]mcpapi.TaskManagerDeliveryState, 0, len(activeTasks))
	for rows.Next() {
		var state mcpapi.TaskManagerDeliveryState
		if err := rows.Scan(
			&state.TaskID,
			&state.DeliveryPolicy,
			&state.LatestCandidateID,
			&state.LatestCandidateStatus,
			&state.LatestDeliveryStatus,
			&state.LatestFailureReason,
			&state.LatestFailureEvidenceRef,
			&state.LatestReplanProposalRef,
			&state.ReadyForVerify,
		); err != nil {
			return nil, err
		}
		if _, exists := activeTasks[state.TaskID]; exists {
			state.ReopenRoundAvailable = productionReopenRoundAvailable(snapshot, state)
			deliveries = append(deliveries, state)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return deliveries, nil
}

// productionReopenRoundAvailable is a read-only recovery hint. It never
// grants graph authority: trustedTargetedVerifyReopenRound still revalidates
// the candidate, verifier invocation, binding, workspace, delivery and
// proposal before registering a decision. Keeping the hint in the snapshot
// lets Manager choose a recoverable task without searching unrelated memory.
func productionReopenRoundAvailable(snapshot coordination.GraphSnapshot, state mcpapi.TaskManagerDeliveryState) bool {
	if state.DeliveryPolicy != taskmanager.DeliveryPolicyCodeMerge || state.LatestReplanProposalRef == "" || state.ReadyForVerify {
		return false
	}
	if (state.LatestCandidateStatus != string(mergequeue.StatusTargetedVerify) && state.LatestCandidateStatus != string(mergequeue.StatusFailed)) ||
		(state.LatestDeliveryStatus != "queued" && state.LatestDeliveryStatus != "failed") {
		return false
	}
	taskActive := false
	for _, task := range snapshot.Tasks {
		if task.ID == state.TaskID {
			taskActive = task.Outcome == coordination.TaskActive
			break
		}
	}
	if !taskActive {
		return false
	}
	var plan, execute, verify *coordination.PhaseEndpoint
	for i := range snapshot.Endpoints {
		endpoint := &snapshot.Endpoints[i]
		if endpoint.Ref.TaskID != state.TaskID {
			continue
		}
		switch endpoint.Ref.EndpointID {
		case coordination.EndpointPlan:
			plan = endpoint
		case coordination.EndpointExecute:
			execute = endpoint
		case coordination.EndpointVerify:
			verify = endpoint
		}
	}
	if plan == nil || execute == nil || verify == nil || plan.State != coordination.EndpointSatisfied ||
		execute.RunPolicy == coordination.RunHeld || verify.RunPolicy == coordination.RunHeld {
		return false
	}
	executeComplete := execute.State == coordination.EndpointSatisfied || execute.State == coordination.EndpointRejected
	verifyReopenable := verify.State == coordination.EndpointPending || verify.State == coordination.EndpointSatisfied || verify.State == coordination.EndpointRejected
	return executeComplete && verifyReopenable
}

func (p *productionTaskManagerRuntime) SubmitTaskManagerDecision(ctx context.Context, caller auth.Principal, scope auth.BoundScope, decision taskmanager.TaskManagerDecision) (string, error) {
	binding, err := p.binding(ctx, caller, scope)
	if err != nil {
		return "", err
	}
	if binding.DecisionRef != "" {
		matches, err := p.decisionMatches(ctx, binding, decision)
		if err != nil {
			return "", err
		}
		if !matches {
			return "", kernel.IdempotencyConflict()
		}
		if binding.DecisionKind == taskmanager.DecisionKindTerminal && !binding.MutationApplied {
			if err := p.complete(ctx, caller.InvocationID, binding.DecisionRef, binding.SeenRevision); err != nil {
				return "", err
			}
		}
		if err := p.appendDecisionAcceptedEvent(ctx, binding, binding.DecisionRef); err != nil {
			return "", err
		}
		return binding.DecisionRef, nil
	}
	snapshot, err := p.graph(caller).Snapshot(ctx, kernel.LatestRevision)
	if err != nil {
		return "", err
	}
	// The immutable boundary revision remains audit input, but it cannot become
	// a permanent deadlock after the Agent follows the contract and re-reads the
	// latest snapshot. The Agent never submits an expected revision: Runtime
	// binds the decision and its one allowed mutation to this authoritative
	// snapshot revision, then persists that base in taskmanager_decisions.
	binding.SeenRevision = snapshot.Revision
	kind, transition, err := p.trustedDecisionMutation(ctx, binding, snapshot, decision)
	if err != nil {
		return "", err
	}
	decisionRef, err := p.decisions.SubmitDecision(ctx, taskmanager.DecisionSubmission{ProjectID: p.projectID, InputRef: binding.InputRef, ExpectedRevision: binding.SeenRevision, Decision: decision, Kind: kind, Transition: transition})
	if err != nil {
		return "", err
	}
	if err := p.persistDecisionAcceptance(ctx, binding, decisionRef, kind, decision, snapshot.Revision); err != nil {
		return "", err
	}
	binding.DecisionRef, binding.DecisionKind, binding.DecisionAction = decisionRef, kind, decision.Action
	if kind == taskmanager.DecisionKindTerminal {
		if err := p.complete(ctx, caller.InvocationID, decisionRef, snapshot.Revision); err != nil {
			return "", err
		}
	}
	if err := p.appendDecisionAcceptedEvent(ctx, binding, decisionRef); err != nil {
		return "", err
	}
	return decisionRef, nil
}

func (p *productionTaskManagerRuntime) ReplacePending(ctx context.Context, caller auth.Principal, scope auth.BoundScope, intent mcpapi.PendingSubgraphIntent) (kernel.Revision, error) {
	binding, err := p.binding(ctx, caller, scope)
	if err != nil {
		return 0, err
	}
	if binding.DecisionKind != taskmanager.DecisionKindReplacePending || binding.DecisionRef == "" || binding.DecisionAction != "replace_pending" {
		return 0, kernel.Forbidden("replacePending requires this invocation's persisted replace_pending decision")
	}
	recoveredRevision, recovered, err := p.recoverAppliedRevision(ctx, binding)
	if err != nil {
		return 0, err
	}
	base, err := p.graph(caller).Snapshot(ctx, binding.SeenRevision)
	if err != nil && recovered && kernel.IsCode(err, kernel.CodeNotFound) {
		// PostgreSQL stores the first real mutation as revision 2; revision 1
		// is a synthetic empty snapshot. After that mutation commits, replay
		// normalizes against the recovered snapshot and verifies the exact
		// replacement scope below.
		base, err = p.graphStore.Snapshot(ctx, p.projectID, recoveredRevision)
	}
	if err != nil {
		return 0, err
	}
	plan, err := p.normalizePending(ctx, binding, base, intent)
	if err != nil {
		return 0, err
	}
	if err := p.ensurePendingPrerequisites(ctx, plan); err != nil {
		return 0, fmt.Errorf("ensure pending prerequisites: %w", err)
	}
	if recovered {
		if err := p.verifyAppliedPending(ctx, recoveredRevision, plan.Subgraph); err != nil {
			return 0, fmt.Errorf("verify recovered pending subgraph: %w", err)
		}
		if err := p.ensurePendingContext(ctx, binding, recoveredRevision, plan); err != nil {
			return 0, fmt.Errorf("ensure recovered pending context: %w", err)
		}
		if !binding.MutationApplied {
			if err := p.complete(ctx, caller.InvocationID, binding.DecisionRef, recoveredRevision); err != nil {
				return 0, err
			}
		}
		if err := p.appendGraphRevisionEvents(ctx, binding, recoveredRevision, plan.Subgraph.Endpoints); err != nil {
			return 0, err
		}
		return recoveredRevision, nil
	}
	revision, err := p.graph(caller).ReplacePending(ctx, plan.Subgraph)
	if err != nil {
		return 0, err
	}
	if err := p.ensurePendingContext(ctx, binding, revision, plan); err != nil {
		return 0, fmt.Errorf("ensure pending context: %w", err)
	}
	if err := p.complete(ctx, caller.InvocationID, binding.DecisionRef, revision); err != nil {
		return 0, err
	}
	if err := p.appendGraphRevisionEvents(ctx, binding, revision, plan.Subgraph.Endpoints); err != nil {
		return 0, err
	}
	return revision, nil
}

func (p *productionTaskManagerRuntime) Transition(ctx context.Context, caller auth.Principal, scope auth.BoundScope) (kernel.Revision, error) {
	binding, err := p.binding(ctx, caller, scope)
	if err != nil {
		return 0, err
	}
	if binding.DecisionKind != taskmanager.DecisionKindTransition || binding.DecisionRef == "" {
		return 0, kernel.Forbidden("transition requires this invocation's persisted transition decision")
	}
	if revision, found, err := p.recoverAppliedRevision(ctx, binding); err != nil {
		return 0, err
	} else if found {
		if err := p.finishTransition(ctx, binding, revision); err != nil {
			return 0, err
		}
		if err := p.appendTransitionEvents(ctx, binding, revision); err != nil {
			return 0, err
		}
		return revision, nil
	}
	if binding.DecisionAction == "reopen_round" {
		if err := p.ensureTargetedVerifyReopenWorkspace(ctx, binding); err != nil {
			return 0, err
		}
	}
	revision, err := p.graph(caller).Transition(ctx, binding.SeenRevision, binding.DecisionRef)
	if err != nil {
		return 0, err
	}
	if err := p.finishTransition(ctx, binding, revision); err != nil {
		return 0, err
	}
	if err := p.appendTransitionEvents(ctx, binding, revision); err != nil {
		return 0, err
	}
	return revision, nil
}

// RecoverPersistedTaskManagerDecision mechanically retries a transition that
// the Task Manager already decided and persisted before its bounded provider
// execution failed. Re-dispatching the model here is both unnecessary and
// unsafe: harmless wording variation would conflict with the immutable
// decision even though the graph mutation is still pending.
func (p *productionTaskManagerRuntime) RecoverPersistedTaskManagerDecision(ctx context.Context, invocationID kernel.InvocationID) (bool, error) {
	if invocationID == "" {
		return false, kernel.InvalidArgument("Task Manager invocation_id is required")
	}
	caller := auth.Principal{
		ActorPrincipalID: kernel.ActorPrincipalID("production-task-manager:" + string(p.projectID)),
		Kind:             auth.PrincipalAgent,
		ProjectID:        p.projectID,
		InvocationID:     invocationID,
		Role:             auth.RoleTaskManager,
		Tools: auth.ToolSet(
			auth.ToolCoordinationSnapshot,
			auth.ToolTaskManagerSubmitDecision,
			auth.ToolCoordinationTransition,
		),
	}
	scope := auth.BoundScope{ProjectID: p.projectID, InvocationID: invocationID}
	binding, err := p.binding(ctx, caller, scope)
	if err != nil {
		return false, err
	}
	if binding.DecisionRef == "" || binding.DecisionKind != taskmanager.DecisionKindTransition {
		return false, nil
	}
	_, err = p.Transition(ctx, caller, scope)
	return true, err
}

func (p *productionTaskManagerRuntime) normalizePending(ctx context.Context, binding productionTaskManagerBinding, snapshot coordination.GraphSnapshot, intent mcpapi.PendingSubgraphIntent) (productionPendingPlan, error) {
	if len(intent.Endpoints) == 0 {
		return productionPendingPlan{}, kernel.InvalidArgument("pending subgraph endpoints are required")
	}
	taskPolicies := make(map[kernel.TaskID]taskmanager.DeliveryPolicy, len(intent.TaskPolicies))
	for _, policy := range intent.TaskPolicies {
		if err := kernel.RequireID("task_id", policy.TaskID); err != nil {
			return productionPendingPlan{}, err
		}
		if !validProductionDeliveryPolicy(policy.DeliveryPolicy) {
			return productionPendingPlan{}, kernel.InvalidArgument("task delivery_policy is unsupported")
		}
		if _, duplicate := taskPolicies[policy.TaskID]; duplicate {
			return productionPendingPlan{}, kernel.InvalidArgument("duplicate task delivery policy")
		}
		taskPolicies[policy.TaskID] = policy.DeliveryPolicy
	}
	existingTasks := make(map[kernel.TaskID]coordination.Task, len(snapshot.Tasks))
	for _, task := range snapshot.Tasks {
		existingTasks[task.ID] = task
	}
	existingEndpoints := make(map[coordination.PhaseEndpointRef]coordination.PhaseEndpoint, len(snapshot.Endpoints))
	for _, endpoint := range snapshot.Endpoints {
		existingEndpoints[endpoint.Ref] = endpoint
	}
	byTask := make(map[kernel.TaskID]map[coordination.EndpointID]mcpapi.PendingEndpointIntent)
	for _, endpoint := range intent.Endpoints {
		if err := validateProductionEndpointRef(endpoint.Ref); err != nil {
			return productionPendingPlan{}, err
		}
		endpoints := byTask[endpoint.Ref.TaskID]
		if endpoints == nil {
			endpoints = make(map[coordination.EndpointID]mcpapi.PendingEndpointIntent, 3)
			byTask[endpoint.Ref.TaskID] = endpoints
		}
		if _, duplicate := endpoints[endpoint.Ref.EndpointID]; duplicate {
			return productionPendingPlan{}, kernel.InvalidGraph("duplicate endpoint in pending subgraph")
		}
		endpoints[endpoint.Ref.EndpointID] = endpoint
	}
	for taskID := range taskPolicies {
		if _, known := byTask[taskID]; !known {
			return productionPendingPlan{}, kernel.InvalidArgument("task delivery policy does not match a pending task")
		}
	}
	taskIDs := make([]kernel.TaskID, 0, len(byTask))
	for taskID := range byTask {
		taskIDs = append(taskIDs, taskID)
	}
	sort.Slice(taskIDs, func(i, j int) bool { return taskIDs[i] < taskIDs[j] })
	plan := productionPendingPlan{Subgraph: coordination.PendingSubgraph{
		RequestID:    kernel.IdempotencyKey(binding.DecisionRef),
		BaseRevision: binding.SeenRevision,
		Edges:        append([]coordination.Edge(nil), intent.Edges...),
	}}
	for _, blocker := range intent.Blockers {
		plan.Subgraph.Blockers = append(plan.Subgraph.Blockers, coordination.Blocker{
			ID: blocker.ID, Target: blocker.Target, RequiredBy: blocker.RequiredBy,
			OnFalse: blocker.OnFalse, State: coordination.BlockerActive,
		})
	}
	for _, taskID := range taskIDs {
		endpointIntents := byTask[taskID]
		existingTask, exists := existingTasks[taskID]
		contractRef := canonicalProductionContractRef(p.projectID, binding.InputRef, taskID)
		var storedContract taskmanager.TaskContract
		if exists {
			if existingTask.Outcome != coordination.TaskActive {
				return productionPendingPlan{}, kernel.TransitionRejected("pending replacement requires an active task")
			}
			contractRef = existingTask.ContractRef
			if p.decisions == nil {
				return productionPendingPlan{}, kernel.Error{Code: kernel.CodeInternalError, Message: "production Task Manager decision store is not configured", Recoverable: true}
			}
			var err error
			storedContract, err = p.decisions.TaskContract(ctx, taskID)
			if err != nil {
				return productionPendingPlan{}, err
			}
			if storedContract.ContractRef != existingTask.ContractRef {
				return productionPendingPlan{}, kernel.StaleBinding("stored TaskContract does not match graph task contract_ref")
			}
			if policy, ok := taskPolicies[taskID]; ok && policy != storedContract.DeliveryPolicy {
				return productionPendingPlan{}, kernel.InvalidArgument("ReplacePending cannot rewrite task delivery policy")
			}
		} else {
			if len(endpointIntents) != 3 {
				return productionPendingPlan{}, kernel.InvalidArgument(fmt.Sprintf("task %s must include exactly plan, execute, and verify endpoints", taskID))
			}
			if _, ok := taskPolicies[taskID]; !ok {
				return productionPendingPlan{}, kernel.InvalidArgument("new task requires an explicit delivery_policy")
			}
		}
		canonicalTask := coordination.Task{ID: taskID, ContractRef: contractRef, Outcome: coordination.TaskActive}
		plan.Subgraph.Tasks = append(plan.Subgraph.Tasks, canonicalTask)
		deliveryPolicy := taskPolicies[taskID]
		if exists {
			deliveryPolicy = storedContract.DeliveryPolicy
		} else if deliveryPolicy == "" {
			deliveryPolicy = taskmanager.DeliveryPolicyCodeMerge
		}
		contract := taskmanager.TaskContract{
			TaskID: taskID, ContractRef: contractRef, DeliveryPolicy: deliveryPolicy,
			PhaseSpecs: make(map[coordination.EndpointID]string, 3),
		}
		if exists {
			contract = storedContract
		}
		// Endpoint generations are control-plane retry counters, not Task
		// workspace rounds. Existing Tasks already own their workspace lineage;
		// pre-provisioning a round whose number happens to match a pending
		// endpoint generation can skip or depend on workspace rounds that never
		// existed. Phase Runtime resolves/reuses the correct Task round when the
		// endpoint actually starts. Only a newly introduced Task needs its first
		// workspace provisioned before the graph mutation becomes visible.
		generationSet := make(map[int]struct{})
		endpointIDs := []coordination.EndpointID{coordination.EndpointPlan, coordination.EndpointExecute, coordination.EndpointVerify}
		if exists {
			endpointIDs = make([]coordination.EndpointID, 0, len(endpointIntents))
			for endpointID := range endpointIntents {
				endpointIDs = append(endpointIDs, endpointID)
			}
			sort.Slice(endpointIDs, func(i, j int) bool { return endpointIDs[i] < endpointIDs[j] })
		}
		for _, endpointID := range endpointIDs {
			candidate := endpointIntents[endpointID]
			if candidate.RunPolicy != coordination.RunEnabled && candidate.RunPolicy != coordination.RunHeld {
				return productionPendingPlan{}, kernel.InvalidArgument("endpoint.run_policy must be enabled or held")
			}
			canonical := coordination.PhaseEndpoint{
				Ref: candidate.Ref, SpecRef: canonicalProductionSpecRef(p.projectID, binding.InputRef, candidate.Ref), Generation: 1,
				BindingRef: canonicalProductionBindingRef(candidate.Ref, 1),
				State:      coordination.EndpointPending, RunPolicy: candidate.RunPolicy,
			}
			if exists {
				current, ok := existingEndpoints[candidate.Ref]
				if !ok {
					return productionPendingPlan{}, kernel.InvalidGraph("existing task is missing the selected endpoint")
				}
				if current.State != coordination.EndpointPending {
					return productionPendingPlan{}, kernel.ScopeNotPending("only pending endpoints can be replaced")
				}
				if candidate.RunPolicy != current.RunPolicy {
					return productionPendingPlan{}, kernel.InvalidArgument("ReplacePending cannot rewrite endpoint run policy")
				}
				canonical = current
				rejected, err := p.pendingEndpointDispatchRejected(ctx, current)
				if err != nil {
					return productionPendingPlan{}, err
				}
				if rejected {
					// The Agent submits only endpoint intent. Runtime derives the new
					// generation from its own rejected command, released lease, and
					// DispatchRejected observation; the caller cannot forge a retry.
					canonical.Generation++
					canonical.BindingRef = canonicalProductionBindingRef(canonical.Ref, canonical.Generation)
				}
			}
			if !exists {
				contract.PhaseSpecs[endpointID] = canonical.SpecRef
			}
			if !exists {
				generationSet[canonical.Generation] = struct{}{}
			}
			plan.Subgraph.Endpoints = append(plan.Subgraph.Endpoints, canonical)
		}
		requirement, err := trustedProductionRequirement(binding, taskID, contractRef)
		if err != nil {
			return productionPendingPlan{}, err
		}
		generations := make([]int, 0, len(generationSet))
		for generation := range generationSet {
			generations = append(generations, generation)
		}
		sort.Ints(generations)
		plan.Resources = append(plan.Resources, productionPendingTaskResource{Input: requirement, Contract: contract, Generations: generations, IsNew: !exists})
	}
	if err := validateProductionEdges(plan.Subgraph.Edges, snapshot.Endpoints, plan.Subgraph.Endpoints); err != nil {
		return productionPendingPlan{}, err
	}
	if err := validateProductionBlockers(plan.Subgraph.Blockers, plan.Subgraph.Endpoints); err != nil {
		return productionPendingPlan{}, err
	}
	return plan, nil
}

func (p *productionTaskManagerRuntime) pendingEndpointDispatchRejected(ctx context.Context, endpoint coordination.PhaseEndpoint) (bool, error) {
	// normalizePending is also used as a pure authority-normalization step in
	// unit tests and recovery validation. Without a durable observation store
	// there is no trusted DispatchRejected fact from which Runtime may derive a
	// new generation, so preserve the current endpoint instead of dereferencing
	// a missing database or inventing a retry.
	if p.db == nil {
		return false, nil
	}
	var rejected bool
	err := p.db.QueryRowContext(ctx, `
SELECT EXISTS (
  SELECT 1
  FROM coordination_phase_commands c
  JOIN coordination_phase_leases l
    ON l.project_id=c.project_id AND l.lease_ref=c.lease_ref
  JOIN coordination_runtime_observations o
    ON o.project_id=c.project_id AND o.event_id=c.completed_event_ref
  WHERE c.project_id=$1
    AND c.task_id=$2 AND c.endpoint_id=$3
    AND c.generation=$4 AND c.binding_ref=$5
    AND c.action IN ('start','resume')
    AND c.not_executable=true
    AND c.observed_event_ref IS NULL
    AND l.state='released'
    AND o.kind='DispatchRejected' AND o.folded=true
    AND o.command_id=c.command_id AND o.lease_ref=c.lease_ref
)`, p.projectID, endpoint.Ref.TaskID, endpoint.Ref.EndpointID, endpoint.Generation, endpoint.BindingRef).Scan(&rejected)
	return rejected, err
}

func validProductionDeliveryPolicy(policy taskmanager.DeliveryPolicy) bool {
	switch policy {
	case taskmanager.DeliveryPolicyNonCodeArtifact, taskmanager.DeliveryPolicyCodeMerge,
		taskmanager.DeliveryPolicyHumanAcceptance, taskmanager.DeliveryPolicyExternalDelivery:
		return true
	default:
		return false
	}
}

func (p *productionTaskManagerRuntime) ensurePendingPrerequisites(ctx context.Context, plan productionPendingPlan) error {
	if p.workspaces == nil || p.contexts == nil {
		return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "production Task Manager workspace and context provisioning is not configured", Recoverable: true}
	}
	for _, resource := range plan.Resources {
		if resource.IsNew {
			if err := p.decisions.PersistRequirementContract(ctx, resource.Input, resource.Contract); err != nil {
				return err
			}
		} else {
			stored, err := p.decisions.TaskContract(ctx, resource.Contract.TaskID)
			if err != nil {
				return err
			}
			if !sameProductionJSON(stored, resource.Contract) {
				return kernel.StaleBinding("persisted task contract does not match pending endpoint specs")
			}
		}
		for _, generation := range resource.Generations {
			workspaceRef, err := p.workspaces.EnsureTaskWorkspace(ctx, productionTaskWorkspaceRequest{TaskID: resource.Contract.TaskID, Generation: generation})
			if err != nil {
				return err
			}
			if kernel.IsZeroID(workspaceRef) {
				return kernel.Error{Code: kernel.CodeInternalError, Message: "workspace provisioner returned an empty binding", Recoverable: true}
			}
		}
	}
	return nil
}

func (p *productionTaskManagerRuntime) ensurePendingContext(ctx context.Context, binding productionTaskManagerBinding, revision kernel.Revision, plan productionPendingPlan) error {
	if p.contexts == nil {
		return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "production Task Manager context projection is not configured", Recoverable: true}
	}
	for _, resource := range plan.Resources {
		if err := p.contexts.EnsureTaskContext(ctx, productionTaskContextRequest{
			InvocationID: binding.InvocationID, InputRef: resource.Input.InputRef, TaskID: resource.Contract.TaskID, GraphRevision: revision,
			Requirement: resource.Input.Requirement, Contract: resource.Contract,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (p *productionTaskManagerRuntime) verifyAppliedPending(ctx context.Context, revision kernel.Revision, expected coordination.PendingSubgraph) error {
	snapshot, err := p.graphStore.Snapshot(ctx, p.projectID, revision)
	if err != nil {
		return err
	}
	tasks := make(map[kernel.TaskID]coordination.Task, len(snapshot.Tasks))
	for _, task := range snapshot.Tasks {
		tasks[task.ID] = task
	}
	endpoints := make(map[coordination.PhaseEndpointRef]coordination.PhaseEndpoint, len(snapshot.Endpoints))
	for _, endpoint := range snapshot.Endpoints {
		endpoints[endpoint.Ref] = endpoint
	}
	scope := make(map[coordination.PhaseEndpointRef]struct{}, len(expected.Endpoints))
	for _, task := range expected.Tasks {
		if tasks[task.ID] != task {
			return fmt.Errorf("task %s differs from recovered snapshot: %w", task.ID, kernel.IdempotencyConflict())
		}
	}
	for _, endpoint := range expected.Endpoints {
		scope[endpoint.Ref] = struct{}{}
		if endpoints[endpoint.Ref] != endpoint {
			return fmt.Errorf("endpoint %s/%s differs from recovered snapshot: %w", endpoint.Ref.TaskID, endpoint.Ref.EndpointID, kernel.IdempotencyConflict())
		}
	}
	actualEdges := make([]coordination.Edge, 0, len(expected.Edges))
	for _, edge := range snapshot.Edges {
		if _, ok := scope[edge.To]; ok {
			actualEdges = append(actualEdges, edge)
		}
	}
	actualBlockers := make([]coordination.Blocker, 0, len(expected.Blockers))
	for _, blocker := range snapshot.Blockers {
		if _, ok := scope[blocker.Target]; ok {
			actualBlockers = append(actualBlockers, blocker)
		}
	}
	sort.Slice(actualEdges, func(i, j int) bool { return productionEdgeKey(actualEdges[i]) < productionEdgeKey(actualEdges[j]) })
	expectedEdges := append([]coordination.Edge(nil), expected.Edges...)
	sort.Slice(expectedEdges, func(i, j int) bool { return productionEdgeKey(expectedEdges[i]) < productionEdgeKey(expectedEdges[j]) })
	sort.Slice(actualBlockers, func(i, j int) bool { return actualBlockers[i].ID < actualBlockers[j].ID })
	expectedBlockers := append([]coordination.Blocker(nil), expected.Blockers...)
	sort.Slice(expectedBlockers, func(i, j int) bool { return expectedBlockers[i].ID < expectedBlockers[j].ID })
	if !sameProductionSlice(actualEdges, expectedEdges) || !sameProductionSlice(actualBlockers, expectedBlockers) {
		return fmt.Errorf("edges or blockers differ from recovered snapshot: %w", kernel.IdempotencyConflict())
	}
	return nil
}

func (p *productionTaskManagerRuntime) finishTransition(ctx context.Context, binding productionTaskManagerBinding, revision kernel.Revision) error {
	needsFollowup := productionTransitionNeedsFollowup(binding)
	if needsFollowup {
		if p.followups == nil {
			return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "production Task Manager follow-up dispatcher is not configured", Recoverable: true}
		}
		followup, err := p.transitionFollowup(ctx, binding, revision)
		if err != nil {
			return err
		}
		if followup.RequestID != "" {
			if err := dispatchPersistedTaskManagerFollowup(ctx, p.followups, followup); err != nil {
				return err
			}
		}
	}
	if binding.DecisionAction == string(coordination.TaskDone) {
		if p.memory == nil {
			return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "production Task Manager memory finalizer is not configured", Recoverable: true}
		}
		taskID := kernel.TaskID(binding.TargetRef)
		principal := auth.Principal{
			ActorPrincipalID: kernel.ActorPrincipalID("production-task-manager:" + string(p.projectID)),
			Kind:             auth.PrincipalAgent, ProjectID: p.projectID, InvocationID: binding.InvocationID,
			Role: auth.RoleTaskManager, TaskID: taskID, Tools: auth.ToolSet(auth.ToolContextFinalizeTaskMemory),
		}
		if _, err := p.memory.FinalizeTaskMemory(ctx, principal, taskID); err != nil {
			return err
		}
	}
	if binding.DecisionAction == "reopen_round" {
		if err := p.ensureTargetedVerifyReopenContext(ctx, binding, revision); err != nil {
			return err
		}
	}
	if binding.MutationApplied {
		return nil
	}
	return p.complete(ctx, binding.InvocationID, binding.DecisionRef, revision)
}

func productionTransitionNeedsFollowup(binding productionTaskManagerBinding) bool {
	return binding.DecisionAction == "submitted" || binding.DecisionAction == "stopped" ||
		(binding.DecisionAction == "satisfied" &&
			(binding.SelectedEndpoint == coordination.EndpointExecute || binding.SelectedEndpoint == coordination.EndpointVerify))
}

// ReconcileCompletedTransitionFollowups closes the crash gap after a graph
// transition has committed but before its Runtime-owned delivery side effect.
// In particular, a satisfied code_merge Execute must enqueue its immutable
// candidate before the existing Verify endpoint is allowed to run. The query
// selects only authoritative, already-applied bindings whose output has no
// durable merge delivery; EnqueueExecutedTask remains the idempotency owner.
func (p *productionTaskManagerRuntime) ReconcileCompletedTransitionFollowups(ctx context.Context) error {
	if p == nil {
		return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "production Task Manager runtime is not configured", Recoverable: true}
	}
	rows, err := p.db.QueryContext(ctx, `
SELECT b.invocation_id
FROM production_taskmanager_bindings b
JOIN production_manager_inputs i
  ON i.project_id=b.project_id AND i.input_ref=b.input_ref
JOIN taskmanager_contracts c
  ON c.project_id=b.project_id AND c.task_id=i.selected_task_id
LEFT JOIN production_merge_deliveries d
  ON d.project_id=b.project_id AND d.task_id=i.selected_task_id AND d.verify_result_ref=i.target_ref
WHERE b.project_id=$1
  AND b.mutation_applied=TRUE
  AND b.decision_action='satisfied'
  AND i.selected_endpoint_id=$2
  AND c.delivery_policy=$3
  AND d.candidate_id IS NULL
ORDER BY b.completed_at, b.invocation_id
LIMIT 32`, p.projectID, coordination.EndpointExecute, taskmanager.DeliveryPolicyCodeMerge)
	if err != nil {
		return err
	}
	defer rows.Close()
	var invocations []kernel.InvocationID
	for rows.Next() {
		var invocationID kernel.InvocationID
		if err := rows.Scan(&invocationID); err != nil {
			return err
		}
		invocations = append(invocations, invocationID)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	var reconcileErr error
	for _, invocationID := range invocations {
		caller := auth.Principal{
			ActorPrincipalID: kernel.ActorPrincipalID("production-task-manager:" + string(p.projectID)),
			Kind:             auth.PrincipalAgent, ProjectID: p.projectID, InvocationID: invocationID,
			Role: auth.RoleTaskManager,
		}
		binding, err := p.binding(ctx, caller, auth.BoundScope{ProjectID: p.projectID, InvocationID: invocationID})
		if err == nil {
			err = p.finishTransition(ctx, binding, binding.AppliedRevision)
		}
		reconcileErr = errors.Join(reconcileErr, err)
	}
	return reconcileErr
}

func (p *productionTaskManagerRuntime) ensureTargetedVerifyReopenWorkspace(ctx context.Context, binding productionTaskManagerBinding) error {
	if p.workspaces == nil {
		return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "production Task Manager workspace provisioning is not configured", Recoverable: true}
	}
	taskID, err := p.persistedReopenRoundTarget(ctx, binding)
	if err != nil {
		return err
	}
	boundary, err := p.targetedVerifyReopenBoundary(ctx, binding, taskID)
	if err != nil {
		return err
	}
	if boundary.SourceKind != productionTargetedVerifyProposalSource || boundary.FromEndpoint.TaskID == "" || strings.TrimSpace(boundary.BasedOnWorkspaceRevision) == "" {
		return kernel.Forbidden("reopen_round workspace requires a trusted targeted verify boundary")
	}
	workspaceRef, err := p.workspaces.EnsureTaskWorkspace(ctx, productionTaskWorkspaceRequest{
		TaskID: boundary.FromEndpoint.TaskID, BaseRevision: boundary.BasedOnWorkspaceRevision,
	})
	if err != nil {
		return err
	}
	if workspaceRef == "" {
		return kernel.Error{Code: kernel.CodeInternalError, Message: "reopen_round workspace provisioner returned an empty binding", Recoverable: true}
	}
	return nil
}

func (p *productionTaskManagerRuntime) ensureTargetedVerifyReopenContext(ctx context.Context, binding productionTaskManagerBinding, revision kernel.Revision) error {
	if p.contexts == nil || p.decisions == nil {
		return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "production Task Manager context projection is not configured", Recoverable: true}
	}
	taskID, err := p.persistedReopenRoundTarget(ctx, binding)
	if err != nil {
		return err
	}
	contract, err := p.decisions.TaskContract(ctx, taskID)
	if err != nil {
		return err
	}
	requirement, err := trustedProductionRequirement(binding, taskID, contract.ContractRef)
	if err != nil {
		return err
	}
	return p.contexts.EnsureTaskContext(ctx, productionTaskContextRequest{
		InvocationID: binding.InvocationID, InputRef: requirement.InputRef, TaskID: taskID,
		GraphRevision: revision, Requirement: requirement.Requirement, Contract: contract,
	})
}

func (p *productionTaskManagerRuntime) persistedReopenRoundTarget(ctx context.Context, binding productionTaskManagerBinding) (kernel.TaskID, error) {
	if binding.DecisionRef == "" || binding.DecisionAction != "reopen_round" {
		return "", kernel.Forbidden("reopen_round target requires a persisted reopen decision")
	}
	var targetRef string
	err := p.db.QueryRowContext(ctx, `
SELECT decision->>'target_ref'
FROM taskmanager_decisions
WHERE project_id=$1 AND decision_ref=$2 AND input_ref=$3`, p.projectID, binding.DecisionRef, binding.InputRef).Scan(&targetRef)
	if errors.Is(err, sql.ErrNoRows) {
		return "", kernel.Error{Code: kernel.CodeInternalError, Message: "persisted reopen_round decision is missing", Recoverable: true}
	}
	if err != nil {
		return "", err
	}
	taskID := kernel.TaskID(targetRef)
	if err := validateProductionTaskID(taskID); err != nil {
		return "", trustedBoundaryError("persisted reopen_round target is invalid")
	}
	return taskID, nil
}

// A follow-up that is already durable may wait for the current Task Manager
// invocation to release the only matching AgentTeams slot. Treat that exact
// state as accepted business work: the production reconcile loop retries
// pending manager inputs after the current provider task terminates. Other
// executor errors remain visible because they may mean nothing was persisted.
func dispatchPersistedTaskManagerFollowup(ctx context.Context, dispatcher productionTaskManagerFollowupDispatcher, followup productionInput) error {
	stored, err := dispatcher.DispatchTaskManagerFollowup(ctx, followup)
	if err == nil {
		return nil
	}
	if stored.InputRef != "" && stored.InvocationID != "" && stored.Status == "pending" && productionTerminalDeliveryWaitsForCapacity(err) {
		return nil
	}
	return err
}

func (p *productionTaskManagerRuntime) transitionFollowup(ctx context.Context, binding productionTaskManagerBinding, revision kernel.Revision) (productionInput, error) {
	snapshot, err := p.graphStore.Snapshot(ctx, p.projectID, revision)
	if err != nil {
		return productionInput{}, err
	}
	ref, err := trustedEndpoint(binding)
	if err != nil {
		return productionInput{}, err
	}
	endpoint, err := productionSnapshotEndpoint(snapshot, ref)
	if err != nil {
		return productionInput{}, err
	}
	requestID := stableProductionSuffix(p.projectID, "taskmanager-followup", binding.InputRef, binding.DecisionRef, binding.DecisionAction, revision)
	input := productionInput{
		Kind: "phase_orchestration", RequestID: requestID, ConversationID: binding.ConversationID,
		SeenRevision: revision, SelectedEndpoint: &ref,
	}
	switch binding.DecisionAction {
	case "submitted":
		var output productionPhaseOutputBoundary
		if err := json.Unmarshal(binding.InputPayload, &output); err != nil {
			return productionInput{}, trustedBoundaryError("phase_output payload is invalid")
		}
		if output.OutputRef == "" || output.OutputRef != binding.TargetRef || output.Receipt.Endpoint != ref {
			return productionInput{}, trustedBoundaryError("phase_output identity changed before follow-up dispatch")
		}
		boundary := productionPhaseEvaluationBoundary{
			SourceInputRef: binding.InputRef, Output: output, Endpoint: ref,
			Generation: endpoint.Generation, BindingRef: endpoint.BindingRef,
		}
		payload, err := json.Marshal(boundary)
		if err != nil {
			return productionInput{}, err
		}
		input.Body = fmt.Sprintf("evaluate phase output %s for %s/%s", output.OutputRef, ref.TaskID, ref.EndpointID)
		input.Payload = payload
		input.TargetKind = "phase_evaluation"
		input.TargetRef = output.OutputRef
	case "satisfied":
		var evaluation productionPhaseEvaluationBoundary
		if err := json.Unmarshal(binding.InputPayload, &evaluation); err != nil {
			return productionInput{}, trustedBoundaryError("phase_evaluation payload is invalid")
		}
		if evaluation.Endpoint != ref || evaluation.Output.Receipt.Endpoint != ref || evaluation.Output.OutputRef == "" {
			return productionInput{}, trustedBoundaryError("satisfied phase identity changed before follow-up dispatch")
		}
		contract, err := p.decisions.TaskContract(ctx, ref.TaskID)
		if err != nil {
			return productionInput{}, err
		}
		if contract.DeliveryPolicy == taskmanager.DeliveryPolicyCodeMerge && ref.EndpointID == coordination.EndpointExecute {
			if p.merge == nil {
				return productionInput{}, kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "production Merge Queue is not configured", Recoverable: true}
			}
			if err := p.merge.EnqueueExecutedTask(ctx, evaluation); err != nil {
				return productionInput{}, err
			}
			// Verify is intentionally not a Merge Queue prerequisite. The queue
			// writes the execute candidate first, then publishes the exact merged
			// revision that makes the existing Verify endpoint runnable.
			return productionInput{}, nil
		}
		if ref.EndpointID != coordination.EndpointVerify {
			return productionInput{}, nil
		}
		boundary := productionTaskCompletionBoundary{
			SourceInputRef: binding.InputRef, TaskID: ref.TaskID, VerifyEndpoint: ref, VerifyOutput: evaluation.Output,
		}
		payload, err := json.Marshal(boundary)
		if err != nil {
			return productionInput{}, err
		}
		input.Body = fmt.Sprintf("evaluate Task %s completion after verify satisfied", ref.TaskID)
		input.Payload = payload
		input.TargetKind = "task_completion"
		input.TargetRef = string(ref.TaskID)
	case "stopped":
		var stopped productionPhaseStoppedBoundary
		if err := json.Unmarshal(binding.InputPayload, &stopped); err != nil {
			return productionInput{}, trustedBoundaryError("phase_stopped payload is invalid")
		}
		if stopped.CommandID == "" || stopped.CommandID != binding.TargetRef || stopped.Endpoint != ref {
			return productionInput{}, trustedBoundaryError("phase_stopped identity changed before follow-up dispatch")
		}
		wantBinding := canonicalProductionBindingRef(ref, endpoint.Generation)
		if endpoint.BindingRef != wantBinding {
			return productionInput{}, kernel.StaleBinding("stopped transition did not install the Runtime binding")
		}
		boundary := productionStopReleaseBoundary{
			SourceInputRef: binding.InputRef, Stopped: stopped,
			NewGeneration: endpoint.Generation, NewBindingRef: endpoint.BindingRef,
		}
		payload, err := json.Marshal(boundary)
		if err != nil {
			return productionInput{}, err
		}
		input.Body = fmt.Sprintf("release stopped phase %s/%s after command %s", ref.TaskID, ref.EndpointID, stopped.CommandID)
		input.Payload = payload
		input.TargetKind = "stop_release"
		input.TargetRef = stopped.CommandID
	default:
		return productionInput{}, kernel.InvalidArgument("transition does not require a follow-up")
	}
	return input, nil
}

func (p *productionTaskManagerRuntime) appendDecisionAcceptedEvent(ctx context.Context, binding productionTaskManagerBinding, decisionRef string) error {
	if p.events == nil {
		return nil
	}
	_, err := p.events.Append(ctx, evidence.AppendEvent{
		StableKey:     kernel.IdempotencyKey(stableProductionEventKey(p.projectID, "manager-decision", decisionRef)),
		Type:          "manager.interaction",
		ProjectID:     p.projectID,
		GraphRevision: int64(binding.SeenRevision),
		Payload: uiprojection.ManagerInteractionEventData{
			ConversationID:  binding.ConversationID,
			EntryID:         "decision:" + decisionRef,
			Kind:            "decision",
			ManagerInputRef: binding.InputRef,
			DecisionRef:     decisionRef,
			GraphRevision:   binding.SeenRevision,
			Disposition:     "accepted",
		},
	})
	return err
}

func (p *productionTaskManagerRuntime) appendGraphRevisionEvents(ctx context.Context, binding productionTaskManagerBinding, revision kernel.Revision, endpoints []coordination.PhaseEndpoint) error {
	if p.events == nil {
		return nil
	}
	for _, endpoint := range endpoints {
		if err := p.appendEndpointUpdatedEvent(ctx, binding, revision, endpoint); err != nil {
			return err
		}
	}
	return p.appendGraphRevisionEvent(ctx, binding, revision)
}

func (p *productionTaskManagerRuntime) appendGraphRevisionEvent(ctx context.Context, binding productionTaskManagerBinding, revision kernel.Revision) error {
	_, err := p.events.Append(ctx, evidence.AppendEvent{
		StableKey:     kernel.IdempotencyKey(stableProductionEventKey(p.projectID, "graph", binding.InputRef, binding.DecisionRef, revision)),
		Type:          "graph.revision",
		ProjectID:     p.projectID,
		GraphRevision: int64(revision),
		Payload: uiprojection.GraphRevisedEventData{
			Revision:        revision,
			ManagerInputRef: binding.InputRef,
			DecisionRef:     binding.DecisionRef,
		},
	})
	return err
}

func (p *productionTaskManagerRuntime) appendTransitionEvents(ctx context.Context, binding productionTaskManagerBinding, revision kernel.Revision) error {
	if p.events == nil {
		return nil
	}
	snapshot, err := p.graphStore.Snapshot(ctx, p.projectID, revision)
	if err != nil {
		return err
	}
	if binding.DecisionAction == "reopen_round" {
		taskID, err := p.persistedReopenRoundTarget(ctx, binding)
		if err != nil {
			return err
		}
		for _, endpointID := range []coordination.EndpointID{coordination.EndpointExecute, coordination.EndpointVerify} {
			endpoint, err := productionSnapshotEndpoint(snapshot, coordination.PhaseEndpointRef{
				TaskID: taskID, EndpointID: endpointID,
			})
			if err != nil {
				return err
			}
			if err := p.appendEndpointUpdatedEvent(ctx, binding, revision, endpoint); err != nil {
				return err
			}
		}
		return p.appendGraphRevisionEvent(ctx, binding, revision)
	}
	ref, err := trustedEndpoint(binding)
	if err != nil {
		return err
	}
	endpoint, err := productionSnapshotEndpoint(snapshot, ref)
	if err != nil {
		return err
	}
	if err := p.appendEndpointUpdatedEvent(ctx, binding, revision, endpoint); err != nil {
		return err
	}
	if err := p.appendPhaseInvocationUpdatedEvent(ctx, binding, revision, endpoint); err != nil {
		return err
	}
	return p.appendGraphRevisionEvent(ctx, binding, revision)
}

func (p *productionTaskManagerRuntime) appendEndpointUpdatedEvent(ctx context.Context, binding productionTaskManagerBinding, revision kernel.Revision, endpoint coordination.PhaseEndpoint) error {
	_, err := p.events.Append(ctx, evidence.AppendEvent{
		StableKey:     kernel.IdempotencyKey(stableProductionEventKey(p.projectID, "endpoint", binding.DecisionRef, revision, endpoint.Ref.TaskID, endpoint.Ref.EndpointID)),
		Type:          "endpoint.updated",
		ProjectID:     p.projectID,
		TaskID:        endpoint.Ref.TaskID,
		PhaseEndpoint: endpoint.Ref.EndpointID,
		GraphRevision: int64(revision),
		Payload: uiprojection.EndpointUpdatedEventData{
			Endpoint:   endpoint.Ref,
			Generation: endpoint.Generation,
			State:      string(endpoint.State),
		},
	})
	return err
}

func (p *productionTaskManagerRuntime) appendPhaseInvocationUpdatedEvent(ctx context.Context, binding productionTaskManagerBinding, revision kernel.Revision, endpoint coordination.PhaseEndpoint) error {
	invocationID := productionPhaseInvocationForEvent(binding)
	if invocationID == "" {
		return nil
	}
	_, err := p.events.Append(ctx, evidence.AppendEvent{
		StableKey:         kernel.IdempotencyKey(stableProductionEventKey(p.projectID, "invocation", binding.DecisionRef, revision, invocationID)),
		Type:              "invocation.updated",
		ProjectID:         p.projectID,
		TaskID:            endpoint.Ref.TaskID,
		PhaseEndpoint:     endpoint.Ref.EndpointID,
		AgentInvocationID: invocationID,
		GraphRevision:     int64(revision),
		Payload: uiprojection.InvocationUpdatedEventData{
			Endpoint:     endpoint.Ref,
			Generation:   endpoint.Generation,
			InvocationID: invocationID,
			Status:       productionPhaseInvocationStatusForEvent(binding.DecisionAction),
		},
	})
	return err
}

func productionPhaseInvocationForEvent(binding productionTaskManagerBinding) kernel.InvocationID {
	switch binding.InputKind {
	case "phase_output":
		var output productionPhaseOutputBoundary
		if err := json.Unmarshal(binding.InputPayload, &output); err == nil {
			return output.Receipt.InvocationID
		}
	case "phase_orchestration":
		var evaluation productionPhaseEvaluationBoundary
		if err := json.Unmarshal(binding.InputPayload, &evaluation); err == nil {
			return evaluation.Output.Receipt.InvocationID
		}
	case "phase_failed":
		var failed productionPhaseFailedBoundary
		if err := json.Unmarshal(binding.InputPayload, &failed); err == nil && failed.CommandID != "" {
			return deterministicPhaseInvocationID(failed.CommandID)
		}
	}
	return ""
}

func productionPhaseInvocationStatusForEvent(action string) string {
	switch action {
	case "rejected", "reopened", "failed":
		return "failed"
	case "stopped":
		return "stopped"
	default:
		return "completed"
	}
}

func stableProductionEventKey(parts ...any) string {
	return "production-event:" + stableProductionSuffix(parts...)
}

func (p *productionTaskManagerRuntime) graph(caller auth.Principal) coordination.TaskManagerGraph {
	return coordination.NewTaskManagerGraph(caller, p.graphStore, p.graphStore, p.idempotency)
}

func (p *productionTaskManagerRuntime) binding(ctx context.Context, caller auth.Principal, scope auth.BoundScope) (productionTaskManagerBinding, error) {
	if caller.Role != auth.RoleTaskManager || caller.ProjectID != p.projectID || caller.InvocationID == "" || scope.ProjectID != caller.ProjectID || scope.InvocationID != caller.InvocationID {
		return productionTaskManagerBinding{}, kernel.Forbidden("Task Manager invocation scope mismatch")
	}
	var binding productionTaskManagerBinding
	var taskID, endpointID, targetKind, targetRef, decisionRef, decisionKind, decisionAction sql.NullString
	var payloadRaw string
	var appliedRevision sql.NullInt64
	err := p.db.QueryRowContext(ctx, `SELECT b.invocation_id, b.input_ref, i.conversation_id, COALESCE(d.expected_revision, i.observed_graph_revision),
i.selected_task_id, i.selected_endpoint_id, i.target_kind, i.target_ref,
i.input_kind, i.payload::text,
COALESCE((SELECT e.body FROM production_conversation_entries e
  WHERE e.project_id=i.project_id AND e.manager_input_ref=i.input_ref AND e.entry_kind=i.input_kind
  ORDER BY e.sequence LIMIT 1), ''),
b.decision_ref, b.decision_kind, b.decision_action, b.mutation_applied, b.applied_graph_revision
FROM production_taskmanager_bindings b
JOIN production_manager_inputs i ON i.project_id=b.project_id AND i.input_ref=b.input_ref
LEFT JOIN taskmanager_decisions d ON d.project_id=b.project_id AND d.decision_ref=b.decision_ref AND d.input_ref=b.input_ref
WHERE b.project_id=$1 AND b.invocation_id=$2`, p.projectID, caller.InvocationID).
		Scan(&binding.InvocationID, &binding.InputRef, &binding.ConversationID, &binding.SeenRevision, &taskID, &endpointID, &targetKind, &targetRef, &binding.InputKind, &payloadRaw, &binding.InputBody, &decisionRef, &decisionKind, &decisionAction, &binding.MutationApplied, &appliedRevision)
	if errors.Is(err, sql.ErrNoRows) {
		return productionTaskManagerBinding{}, kernel.Forbidden("Task Manager invocation is not bound to a production input")
	}
	if err != nil {
		return productionTaskManagerBinding{}, err
	}
	binding.SelectedTaskID = kernel.TaskID(taskID.String)
	binding.SelectedEndpoint = coordination.EndpointID(endpointID.String)
	binding.TargetKind, binding.TargetRef = targetKind.String, targetRef.String
	binding.InputPayload = json.RawMessage(payloadRaw)
	binding.DecisionRef, binding.DecisionKind, binding.DecisionAction = decisionRef.String, taskmanager.DecisionKind(decisionKind.String), decisionAction.String
	if appliedRevision.Valid {
		binding.AppliedRevision = kernel.Revision(appliedRevision.Int64)
	}
	return binding, nil
}

func (p *productionTaskManagerRuntime) decisionMatches(ctx context.Context, binding productionTaskManagerBinding, decision taskmanager.TaskManagerDecision) (bool, error) {
	var storedRaw string
	err := p.db.QueryRowContext(ctx, `SELECT decision::text FROM taskmanager_decisions
WHERE project_id=$1 AND decision_ref=$2 AND input_ref=$3`, p.projectID, binding.DecisionRef, binding.InputRef).Scan(&storedRaw)
	if errors.Is(err, sql.ErrNoRows) {
		return false, kernel.Error{Code: kernel.CodeInternalError, Message: "persisted Task Manager decision is missing", Recoverable: true}
	}
	if err != nil {
		return false, err
	}
	var stored taskmanager.TaskManagerDecision
	if err := json.Unmarshal([]byte(storedRaw), &stored); err != nil {
		return false, kernel.Error{Code: kernel.CodeInternalError, Message: "persisted Task Manager decision is invalid", Recoverable: true}
	}
	storedCanonical, err := json.Marshal(stored)
	if err != nil {
		return false, err
	}
	replayedCanonical, err := json.Marshal(decision)
	if err != nil {
		return false, kernel.InvalidArgument("manager decision must be JSON serializable")
	}
	return bytes.Equal(storedCanonical, replayedCanonical), nil
}

// recoverAppliedRevision closes the only non-atomic gap between the
// coordination transaction and the production binding transaction. The graph
// revision is accepted only at the invocation's exact next revision and with
// the decision reference derived by the coordination store.
func (p *productionTaskManagerRuntime) recoverAppliedRevision(ctx context.Context, binding productionTaskManagerBinding) (kernel.Revision, bool, error) {
	if binding.MutationApplied {
		if binding.AppliedRevision <= 0 {
			return 0, false, kernel.Error{Code: kernel.CodeInternalError, Message: "applied Task Manager mutation has no graph revision", Recoverable: true}
		}
		return binding.AppliedRevision, true, nil
	}
	if binding.DecisionRef == "" || (binding.DecisionKind != taskmanager.DecisionKindReplacePending && binding.DecisionKind != taskmanager.DecisionKindTransition) {
		return 0, false, nil
	}
	want := binding.SeenRevision.Next()
	var revision kernel.Revision
	err := p.db.QueryRowContext(ctx, `SELECT revision FROM coordination_graph_revisions
WHERE project_id=$1 AND revision=$2 AND decision_ref=$3`, p.projectID, want, binding.DecisionRef).Scan(&revision)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return revision, true, nil
}

func (p *productionTaskManagerRuntime) trustedDecisionMutation(ctx context.Context, binding productionTaskManagerBinding, snapshot coordination.GraphSnapshot, decision taskmanager.TaskManagerDecision) (taskmanager.DecisionKind, coordination.GraphTransition, error) {
	if decision.Action == string(coordination.TaskDone) {
		taskID, evidenceRefs, err := p.trustedTaskCompletionBoundary(ctx, binding, snapshot, decision)
		if err != nil {
			return "", coordination.GraphTransition{}, err
		}
		return taskmanager.DecisionKindTransition, coordination.GraphTransition{
			TargetKind: coordination.TargetTask, TaskID: taskID, Action: string(coordination.TaskDone), EvidenceRefs: evidenceRefs,
		}, nil
	}
	if decision.Action == "reopen_round" {
		return p.trustedTargetedVerifyReopenRound(ctx, binding, snapshot, decision)
	}
	return trustedDecisionMutation(binding, snapshot, decision)
}

func (p *productionTaskManagerRuntime) trustedTargetedVerifyReopenRound(ctx context.Context, binding productionTaskManagerBinding, snapshot coordination.GraphSnapshot, decision taskmanager.TaskManagerDecision) (taskmanager.DecisionKind, coordination.GraphTransition, error) {
	if p == nil || p.db == nil {
		return "", coordination.GraphTransition{}, kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "targeted verify authority store is not configured", Recoverable: true}
	}
	taskID := kernel.TaskID(decision.TargetRef)
	boundary, err := p.targetedVerifyReopenBoundary(ctx, binding, taskID)
	if err != nil {
		return "", coordination.GraphTransition{}, err
	}
	proposal := boundary.OrchestrationProposal
	if boundary.SourceKind != productionTargetedVerifyProposalSource || boundary.CandidateID == "" || proposal.ProposalID == "" ||
		proposal.FromEndpoint.TaskID == "" || proposal.FromEndpoint.EndpointID != coordination.EndpointVerify || proposal.FromInvocationID == "" {
		return "", coordination.GraphTransition{}, kernel.Forbidden("reopen_round proposal identity is not a trusted targeted verify boundary")
	}
	if taskID == "" || taskID != proposal.FromEndpoint.TaskID {
		return "", coordination.GraphTransition{}, kernel.InvalidArgument("reopen_round target_ref must be the Runtime-selected task ID")
	}
	var candidateProject kernel.ProjectID
	var candidateTask kernel.TaskID
	var candidateStatus string
	var invocationProject kernel.ProjectID
	var invocationTask kernel.TaskID
	var invocationEndpoint coordination.EndpointID
	var invocationRole auth.Role
	var invocationBinding kernel.BindingRef
	var invocationWorkspace string
	var failureEvidenceRef string
	var latestCandidate bool
	err = p.db.QueryRowContext(ctx, `
SELECT c.project_id, c.task_id, c.status, COALESCE(c.failure_evidence_ref,''),
       c.id = (SELECT latest.id FROM merge_candidates latest
               WHERE latest.project_id=c.project_id AND latest.task_id=c.task_id
               ORDER BY latest.created_at DESC, latest.id DESC LIMIT 1),
       r.project_id, COALESCE(r.task_id,''), COALESCE(r.endpoint_id,''), r.role,
       COALESCE(r.binding_ref,''), COALESCE(r.workspace_ref,'')
FROM merge_candidates c
JOIN runtime_invocations r ON r.invocation_id=$2
WHERE c.id=$1`, boundary.CandidateID, proposal.FromInvocationID).Scan(
		&candidateProject, &candidateTask, &candidateStatus, &failureEvidenceRef, &latestCandidate,
		&invocationProject, &invocationTask, &invocationEndpoint, &invocationRole,
		&invocationBinding, &invocationWorkspace,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return "", coordination.GraphTransition{}, kernel.Forbidden("targeted verify proposal has no persisted candidate invocation authority")
	}
	if err != nil {
		return "", coordination.GraphTransition{}, err
	}
	if candidateProject != p.projectID || candidateTask != proposal.FromEndpoint.TaskID || !latestCandidate ||
		(candidateStatus != string(mergequeue.StatusTargetedVerify) && candidateStatus != string(mergequeue.StatusFailed)) ||
		invocationProject != p.projectID || invocationTask != candidateTask || invocationEndpoint != coordination.EndpointVerify || invocationRole != auth.RoleVerifier ||
		invocationBinding != productionTargetedVerifyBindingRef(mergequeue.TargetedVerifyRequest{Candidate: mergequeue.Candidate{ID: boundary.CandidateID, TaskID: candidateTask}, LatestMainRevision: proposal.BasedOnWorkspaceRevision}) ||
		strings.TrimSpace(invocationWorkspace) == "" {
		return "", coordination.GraphTransition{}, kernel.Forbidden("targeted verify proposal does not match persisted merge authority")
	}
	var deliveryTask kernel.TaskID
	var deliveryStatus string
	if err := p.db.QueryRowContext(ctx, `
SELECT task_id, status
FROM production_merge_deliveries
WHERE project_id=$1 AND candidate_id=$2`, p.projectID, boundary.CandidateID).Scan(&deliveryTask, &deliveryStatus); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", coordination.GraphTransition{}, kernel.Forbidden("targeted verify proposal is not attached to a production merge delivery")
		}
		return "", coordination.GraphTransition{}, err
	}
	if deliveryTask != candidateTask || (deliveryStatus != "queued" && deliveryStatus != "failed") {
		return "", coordination.GraphTransition{}, kernel.Forbidden("targeted verify proposal requires an active or failed production merge delivery")
	}
	var execute, verify coordination.PhaseEndpoint
	for _, endpoint := range snapshot.Endpoints {
		if endpoint.Ref.TaskID != candidateTask {
			continue
		}
		switch endpoint.Ref.EndpointID {
		case coordination.EndpointExecute:
			execute = endpoint
		case coordination.EndpointVerify:
			verify = endpoint
		}
	}
	if execute.Ref.TaskID == "" || verify.Ref.TaskID == "" {
		return "", coordination.GraphTransition{}, kernel.InvalidGraph("reopen_round task is missing execute or verify endpoint")
	}
	if execute.State != coordination.EndpointSatisfied && execute.State != coordination.EndpointRejected {
		return "", coordination.GraphTransition{}, kernel.TransitionRejected("targeted verify reopen_round requires a completed execute round")
	}
	if verify.State != coordination.EndpointPending && verify.State != coordination.EndpointSatisfied && verify.State != coordination.EndpointRejected {
		return "", coordination.GraphTransition{}, kernel.TransitionRejected("targeted verify reopen_round requires pending, satisfied, or rejected verify")
	}
	refs := uniqueProductionStrings(append(append([]string(nil), proposal.EvidenceRefs...),
		failureEvidenceRef, "merge-candidate:"+string(boundary.CandidateID), "targeted-verify-proposal:"+proposal.ProposalID))
	return taskmanager.DecisionKindTransition, coordination.GraphTransition{
		TargetKind:        coordination.TargetTask,
		TaskID:            candidateTask,
		Action:            "reopen_round",
		ExecuteBindingRef: canonicalProductionBindingRef(execute.Ref, execute.Generation+1),
		VerifyBindingRef:  canonicalProductionBindingRef(verify.Ref, verify.Generation+1),
		EvidenceRefs:      refs,
	}, nil
}

// targetedVerifyReopenBoundary accepts either the live internal targeted
// verifier proposal or a later operator Manager message. The latter carries
// no candidate authority of its own: Runtime resolves the latest failed
// candidate and a completed, persisted internal replan proposal for that
// exact task before the decision may be registered.
func (p *productionTaskManagerRuntime) targetedVerifyReopenBoundary(ctx context.Context, binding productionTaskManagerBinding, taskID kernel.TaskID) (productionTargetedVerifyProposalBoundary, error) {
	if taskID == "" {
		return productionTargetedVerifyProposalBoundary{}, kernel.InvalidArgument("reopen_round target_ref is required")
	}
	switch binding.InputKind {
	case "phase_orchestration":
		if binding.TargetKind != "phase_orchestration" {
			return productionTargetedVerifyProposalBoundary{}, kernel.Forbidden("reopen_round requires a targeted verifier orchestration proposal")
		}
		boundary, err := decodeProductionTargetedVerifyProposalBoundary(binding.InputPayload)
		if err != nil {
			return productionTargetedVerifyProposalBoundary{}, err
		}
		proposal := boundary.OrchestrationProposal
		if proposal.ProposalID != binding.TargetRef || proposal.FromEndpoint.TaskID != binding.SelectedTaskID ||
			binding.SelectedEndpoint != coordination.EndpointVerify || proposal.FromEndpoint.TaskID != taskID {
			return productionTargetedVerifyProposalBoundary{}, kernel.Forbidden("reopen_round proposal identity is not bound to the current targeted verifier input")
		}
		return boundary, nil
	case "manager":
		if err := trustedManagerControlIntent(binding, httpapi.ManagerIntentOrchestrate); err != nil {
			return productionTargetedVerifyProposalBoundary{}, err
		}
		return p.latestPersistedTargetedVerifyReopenBoundary(ctx, taskID)
	default:
		return productionTargetedVerifyProposalBoundary{}, kernel.Forbidden("reopen_round requires a targeted verifier proposal or an orchestrate Manager input backed by one")
	}
}

func decodeProductionTargetedVerifyProposalBoundary(payload []byte) (productionTargetedVerifyProposalBoundary, error) {
	var boundary productionTargetedVerifyProposalBoundary
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&boundary); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return productionTargetedVerifyProposalBoundary{}, trustedBoundaryError("targeted verify orchestration payload is invalid")
	}
	return boundary, nil
}

func (p *productionTaskManagerRuntime) latestPersistedTargetedVerifyReopenBoundary(ctx context.Context, taskID kernel.TaskID) (productionTargetedVerifyProposalBoundary, error) {
	var candidateID mergequeue.CandidateID
	err := p.db.QueryRowContext(ctx, `
SELECT c.id
FROM merge_candidates c
JOIN production_merge_deliveries d
  ON d.project_id=c.project_id AND d.candidate_id=c.id AND d.task_id=c.task_id
WHERE c.project_id=$1 AND c.task_id=$2
  AND c.status IN ('targeted_verify','failed')
  AND d.status IN ('queued','failed')
ORDER BY c.created_at DESC, c.id DESC
LIMIT 1`, p.projectID, taskID).Scan(&candidateID)
	if errors.Is(err, sql.ErrNoRows) {
		return productionTargetedVerifyProposalBoundary{}, kernel.Forbidden("reopen_round has no current failed merge candidate authority")
	}
	if err != nil {
		return productionTargetedVerifyProposalBoundary{}, err
	}
	var payload string
	err = p.db.QueryRowContext(ctx, `
SELECT i.payload::text
FROM production_manager_inputs i
WHERE i.project_id=$1
  AND i.input_kind='phase_orchestration'
  AND i.target_kind='phase_orchestration'
  AND i.selected_task_id=$2
  AND i.selected_endpoint_id=$3
  AND i.status='completed'
  AND i.payload->>'source_kind'=$4
  AND i.payload->>'candidate_id'=$5
  AND i.payload->>'orchestration_advice'=$6
  AND i.target_ref=i.payload->>'proposal_id'
ORDER BY i.created_at DESC, i.input_ref DESC
LIMIT 1`, p.projectID, taskID, coordination.EndpointVerify, productionTargetedVerifyProposalSource, candidateID, phasepkg.OrchestrationReplan).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return productionTargetedVerifyProposalBoundary{}, kernel.Forbidden("reopen_round has no completed trusted targeted verifier replan proposal")
	}
	if err != nil {
		return productionTargetedVerifyProposalBoundary{}, err
	}
	boundary, err := decodeProductionTargetedVerifyProposalBoundary([]byte(payload))
	if err != nil {
		return productionTargetedVerifyProposalBoundary{}, err
	}
	if boundary.CandidateID != candidateID || boundary.FromEndpoint.TaskID != taskID {
		return productionTargetedVerifyProposalBoundary{}, kernel.Forbidden("persisted targeted verifier proposal does not match the latest failed candidate")
	}
	return boundary, nil
}

func trustedDecisionMutation(binding productionTaskManagerBinding, snapshot coordination.GraphSnapshot, decision taskmanager.TaskManagerDecision) (taskmanager.DecisionKind, coordination.GraphTransition, error) {
	switch decision.Action {
	case "replace_pending":
		if decision.TargetRef != "" {
			return "", coordination.GraphTransition{}, kernel.InvalidArgument("replace_pending target_ref must be omitted")
		}
		if binding.InputKind != "requirement" && binding.InputKind != "manager" && binding.InputKind != "human" && !(binding.InputKind == "phase_orchestration" && binding.TargetKind == "phase_orchestration") {
			return "", coordination.GraphTransition{}, kernel.Forbidden("replace_pending requires a trusted requirement, manager, human, or orchestration proposal boundary")
		}
		return taskmanager.DecisionKindReplacePending, coordination.GraphTransition{}, nil
	case "reject", "defer", "no_change":
		if decision.TargetRef != "" {
			return "", coordination.GraphTransition{}, kernel.InvalidArgument("terminal decision target_ref must be omitted")
		}
		if binding.InputKind != "requirement" && binding.InputKind != "manager" && binding.InputKind != "human" && !(binding.InputKind == "phase_orchestration" && binding.TargetKind == "phase_orchestration") {
			return "", coordination.GraphTransition{}, kernel.Forbidden("terminal decision is not allowed for a phase lifecycle boundary")
		}
		return taskmanager.DecisionKindTerminal, coordination.GraphTransition{}, nil
	case "held":
		if binding.InputKind != "manager" && binding.InputKind != "human" && !(binding.InputKind == "phase_orchestration" && binding.TargetKind == "phase_orchestration") {
			return "", coordination.GraphTransition{}, kernel.Forbidden("held requires a trusted manager, human, or orchestration proposal boundary")
		}
		if binding.InputKind == "manager" {
			if err := trustedManagerControlIntent(binding, httpapi.ManagerIntentHold); err != nil {
				return "", coordination.GraphTransition{}, err
			}
		}
		ref, err := trustedEndpoint(binding)
		if err != nil {
			return "", coordination.GraphTransition{}, err
		}
		want := fmt.Sprintf("%s/%s", ref.TaskID, ref.EndpointID)
		if decision.TargetRef != want {
			return "", coordination.GraphTransition{}, kernel.InvalidArgument("decision target_ref does not match Runtime-selected endpoint")
		}
		endpoint, err := productionSnapshotEndpoint(snapshot, ref)
		if err != nil {
			return "", coordination.GraphTransition{}, err
		}
		return taskmanager.DecisionKindTransition, coordination.GraphTransition{TargetKind: coordination.TargetPhaseEndpoint, Endpoint: ref, Action: decision.Action, Generation: endpoint.Generation}, nil
	case "submitted":
		endpoint, output, err := trustedPhaseOutputBoundary(binding, snapshot, decision)
		if err != nil {
			return "", coordination.GraphTransition{}, err
		}
		return taskmanager.DecisionKindTransition, coordination.GraphTransition{
			TargetKind: coordination.TargetPhaseEndpoint, Endpoint: endpoint.Ref, Action: decision.Action, Generation: endpoint.Generation,
			Result:       canonicalProductionPhaseResult(endpoint, output.OutputRef, coordination.VerdictSubmitted),
			EvidenceRefs: trustedPhaseOutputEvidence(output),
		}, nil
	case "satisfied", "rejected":
		endpoint, evaluation, err := trustedPhaseEvaluationBoundary(binding, snapshot, decision)
		if err != nil {
			return "", coordination.GraphTransition{}, err
		}
		verdict := coordination.VerdictSatisfied
		if decision.Action == "rejected" {
			verdict = coordination.VerdictRejected
		}
		return taskmanager.DecisionKindTransition, coordination.GraphTransition{
			TargetKind: coordination.TargetPhaseEndpoint, Endpoint: endpoint.Ref, Action: decision.Action, Generation: endpoint.Generation,
			Result:       canonicalProductionPhaseResult(endpoint, evaluation.Output.OutputRef, verdict),
			EvidenceRefs: trustedPhaseOutputEvidence(evaluation.Output),
		}, nil
	case "stopped":
		endpoint, stopped, err := trustedPhaseStoppedBoundary(binding, snapshot, decision)
		if err != nil {
			return "", coordination.GraphTransition{}, err
		}
		return taskmanager.DecisionKindTransition, coordination.GraphTransition{
			TargetKind: coordination.TargetPhaseEndpoint, Endpoint: endpoint.Ref, Action: decision.Action, Generation: endpoint.Generation,
			NewBindingRef: canonicalProductionBindingRef(endpoint.Ref, endpoint.Generation+1),
			CheckpointRef: stopped.CheckpointRef, NonResumable: stopped.NonResumable,
			EvidenceRefs: []string{"phase-stop:" + stableProductionSuffix(stopped.CommandID, stopped.LeaseRef, stopped.CheckpointRef, stopped.NonResumable)},
		}, nil
	case "released":
		if binding.InputKind == "manager" || binding.InputKind == "human" {
			if binding.InputKind == "manager" {
				if err := trustedManagerControlIntent(binding, httpapi.ManagerIntentResume); err != nil {
					return "", coordination.GraphTransition{}, err
				}
			}
			ref, err := trustedEndpoint(binding)
			if err != nil {
				return "", coordination.GraphTransition{}, err
			}
			want := fmt.Sprintf("%s/%s", ref.TaskID, ref.EndpointID)
			if decision.TargetRef != want {
				return "", coordination.GraphTransition{}, kernel.InvalidArgument("decision target_ref does not match Runtime-selected endpoint")
			}
			endpoint, err := productionSnapshotEndpoint(snapshot, ref)
			if err != nil {
				return "", coordination.GraphTransition{}, err
			}
			if endpoint.RunPolicy != coordination.RunHeld {
				return "", coordination.GraphTransition{}, kernel.TransitionRejected("released transition requires held endpoint")
			}
			// The Coordination Graph store still rejects release while this
			// generation owns an active lease. This branch restores the public
			// Manager/human resume intent without exposing GraphRuntime control.
			return taskmanager.DecisionKindTransition, coordination.GraphTransition{
				TargetKind: coordination.TargetPhaseEndpoint,
				Endpoint:   endpoint.Ref,
				Action:     decision.Action,
				Generation: endpoint.Generation,
			}, nil
		}
		endpoint, _, err := trustedStopReleaseBoundary(binding, snapshot, decision)
		if err != nil {
			return "", coordination.GraphTransition{}, err
		}
		return taskmanager.DecisionKindTransition, coordination.GraphTransition{TargetKind: coordination.TargetPhaseEndpoint, Endpoint: endpoint.Ref, Action: decision.Action, Generation: endpoint.Generation}, nil
	case "reopened":
		endpoint, failed, err := trustedPhaseFailedBoundary(binding, snapshot, decision, false)
		if err != nil {
			return "", coordination.GraphTransition{}, err
		}
		return taskmanager.DecisionKindTransition, coordination.GraphTransition{
			TargetKind: coordination.TargetPhaseEndpoint, Endpoint: endpoint.Ref, Action: decision.Action, Generation: endpoint.Generation,
			NewBindingRef: canonicalProductionBindingRef(endpoint.Ref, endpoint.Generation+1),
			EvidenceRefs:  []string{"phase-failure:" + stableProductionSuffix(failed.CommandID, failed.LeaseRef, failed.BindingRef)},
		}, nil
	case "failed":
		_, failed, err := trustedPhaseFailedBoundary(binding, snapshot, decision, true)
		if err != nil {
			return "", coordination.GraphTransition{}, err
		}
		return taskmanager.DecisionKindTransition, coordination.GraphTransition{
			TargetKind: coordination.TargetTask, TaskID: failed.Endpoint.TaskID, Action: string(coordination.TaskFailed),
			EvidenceRefs: []string{"phase-failure:" + stableProductionSuffix(failed.CommandID, failed.LeaseRef, failed.BindingRef)},
		}, nil
	default:
		return "", coordination.GraphTransition{}, kernel.Forbidden("decision action requires a Runtime-authenticated phase or delivery boundary")
	}
}

func trustedManagerControlIntent(binding productionTaskManagerBinding, expected httpapi.ManagerMessageIntent) error {
	var request httpapi.ManagerMessageRequest
	decoder := json.NewDecoder(bytes.NewReader(binding.InputPayload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return trustedBoundaryError("manager control payload is invalid")
	}
	intent := request.Intent
	if intent == "" {
		intent = httpapi.ManagerIntentOrchestrate
	}
	if intent != expected {
		return kernel.Forbidden(fmt.Sprintf("%s decision requires explicit manager intent %s", expected, expected))
	}
	return nil
}

func trustedPhaseFailedBoundary(binding productionTaskManagerBinding, snapshot coordination.GraphSnapshot, decision taskmanager.TaskManagerDecision, taskTarget bool) (coordination.PhaseEndpoint, productionPhaseFailedBoundary, error) {
	if binding.InputKind != "phase_failed" || binding.TargetKind != "phase_failed" || binding.TargetRef == "" {
		return coordination.PhaseEndpoint{}, productionPhaseFailedBoundary{}, kernel.Forbidden("reopened or failed requires a Runtime-authenticated phase_failed boundary")
	}
	ref, err := trustedEndpoint(binding)
	if err != nil {
		return coordination.PhaseEndpoint{}, productionPhaseFailedBoundary{}, err
	}
	want := fmt.Sprintf("%s/%s", ref.TaskID, ref.EndpointID)
	if taskTarget {
		want = string(ref.TaskID)
	}
	if decision.TargetRef != want {
		return coordination.PhaseEndpoint{}, productionPhaseFailedBoundary{}, kernel.InvalidArgument("decision target_ref does not match Runtime-selected failed Phase")
	}
	endpoint, err := productionSnapshotEndpoint(snapshot, ref)
	if err != nil {
		return coordination.PhaseEndpoint{}, productionPhaseFailedBoundary{}, err
	}
	var failed productionPhaseFailedBoundary
	if err := json.Unmarshal(binding.InputPayload, &failed); err != nil {
		return coordination.PhaseEndpoint{}, productionPhaseFailedBoundary{}, trustedBoundaryError("phase_failed payload is invalid")
	}
	if failed.CommandID == "" || failed.CommandID != binding.TargetRef || failed.Endpoint != ref || kernel.IsZeroID(failed.LeaseRef) ||
		(failed.CommandAction != coordination.CommandStart && failed.CommandAction != coordination.CommandResume) {
		return coordination.PhaseEndpoint{}, productionPhaseFailedBoundary{}, trustedBoundaryError("phase_failed command identity does not match its persisted binding")
	}
	if failed.Generation != endpoint.Generation || failed.BindingRef != endpoint.BindingRef {
		return coordination.PhaseEndpoint{}, productionPhaseFailedBoundary{}, kernel.StaleBinding("phase_failed does not match the graph snapshot")
	}
	return endpoint, failed, nil
}

func (p *productionTaskManagerRuntime) trustedTaskCompletionBoundary(ctx context.Context, binding productionTaskManagerBinding, snapshot coordination.GraphSnapshot, decision taskmanager.TaskManagerDecision) (kernel.TaskID, []string, error) {
	if binding.InputKind != "phase_orchestration" || binding.TargetKind != "task_completion" || binding.TargetRef == "" {
		return "", nil, kernel.Forbidden("done requires a Runtime-authenticated task_completion boundary")
	}
	taskID := kernel.TaskID(binding.TargetRef)
	if decision.TargetRef != string(taskID) || binding.SelectedTaskID != taskID || binding.SelectedEndpoint != coordination.EndpointVerify {
		return "", nil, kernel.InvalidArgument("done target does not match the Runtime-selected Task")
	}
	var task coordination.Task
	for _, candidate := range snapshot.Tasks {
		if candidate.ID == taskID {
			task = candidate
			break
		}
	}
	if task.ID == "" {
		return "", nil, kernel.Error{Code: kernel.CodeNotFound, Message: "task completion target not found", Recoverable: true}
	}
	if task.Outcome != coordination.TaskActive {
		return "", nil, kernel.TransitionRejected("task completion target is not active")
	}
	verifyRef := coordination.PhaseEndpointRef{TaskID: taskID, EndpointID: coordination.EndpointVerify}
	verify, err := productionSnapshotEndpoint(snapshot, verifyRef)
	if err != nil {
		return "", nil, err
	}
	if verify.State != coordination.EndpointSatisfied {
		return "", nil, kernel.TransitionRejected("task done requires a satisfied verify endpoint")
	}
	var completion productionTaskCompletionBoundary
	if err := json.Unmarshal(binding.InputPayload, &completion); err != nil {
		return "", nil, trustedBoundaryError("task_completion payload is invalid")
	}
	if completion.TaskID != taskID || completion.VerifyEndpoint != verifyRef || completion.VerifyOutput.OutputRef == "" || completion.VerifyOutput.Receipt.Endpoint != verifyRef {
		return "", nil, trustedBoundaryError("task_completion identity does not match its persisted binding")
	}
	if completion.VerifyOutput.Receipt.Generation != verify.Generation || completion.VerifyOutput.Receipt.BindingRef != verify.BindingRef {
		return "", nil, kernel.StaleBinding("task_completion output does not match the verify endpoint")
	}
	contract, err := p.decisions.TaskContract(ctx, taskID)
	if err != nil {
		return "", nil, err
	}
	switch contract.DeliveryPolicy {
	case taskmanager.DeliveryPolicyNonCodeArtifact:
		if len(completion.VerifyOutput.Receipt.Output.DeliveryRefs) == 0 {
			return "", nil, kernel.TransitionRejected("non_code_artifact completion requires a verified delivery artifact")
		}
	case taskmanager.DeliveryPolicyCodeMerge:
		if p.mergeEvidence == nil {
			return "", nil, kernel.TransitionRejected("code_merge completion requires a trusted Merge Queue success boundary")
		}
		delivery, mergeEvidenceRefs, ok, err := p.mergeEvidence.CodeMergeEvidence(ctx, taskID, completion.VerifyOutput.Receipt.WorkspaceHead)
		if err != nil {
			return "", nil, err
		}
		if !ok || !taskmanagerDeliverySatisfied(contract.DeliveryPolicy, delivery) {
			return "", nil, kernel.TransitionRejected("code_merge completion requires a trusted Merge Queue success boundary")
		}
		evidenceRefs := append([]string(nil), mergeEvidenceRefs...)
		if delivery.MergeCommitRef != "" {
			evidenceRefs = append(evidenceRefs, delivery.MergeCommitRef)
		}
		return taskID, uniqueProductionStrings(evidenceRefs), nil
	case taskmanager.DeliveryPolicyHumanAcceptance:
		return "", nil, kernel.TransitionRejected("human_acceptance completion requires a trusted human decision boundary")
	case taskmanager.DeliveryPolicyExternalDelivery:
		return "", nil, kernel.TransitionRejected("external_delivery completion requires a trusted external delivery boundary")
	default:
		return "", nil, kernel.InvalidArgument("task delivery policy is unsupported")
	}
	evidenceRefs := append([]string(nil), completion.VerifyOutput.Receipt.Output.EvidenceRefs...)
	evidenceRefs = append(evidenceRefs, completion.VerifyOutput.Receipt.Output.DeliveryRefs...)
	if completion.VerifyOutput.Receipt.Output.ReportRef != "" {
		evidenceRefs = append(evidenceRefs, completion.VerifyOutput.Receipt.Output.ReportRef)
	}
	return taskID, uniqueProductionStrings(evidenceRefs), nil
}

func taskmanagerDeliverySatisfied(policy taskmanager.DeliveryPolicy, evidence taskmanager.DeliveryEvidence) bool {
	switch policy {
	case taskmanager.DeliveryPolicyNonCodeArtifact:
		return len(evidence.ArtifactRefs) > 0
	case taskmanager.DeliveryPolicyHumanAcceptance:
		return evidence.HumanAccepted
	case taskmanager.DeliveryPolicyCodeMerge:
		return evidence.LatestMainVerified && evidence.MergeSucceeded && evidence.MergeCommitRef != ""
	case taskmanager.DeliveryPolicyExternalDelivery:
		return evidence.ExternalDelivered && len(evidence.EvidenceRefs) > 0
	default:
		return false
	}
}

func trustedPhaseOutputBoundary(binding productionTaskManagerBinding, snapshot coordination.GraphSnapshot, decision taskmanager.TaskManagerDecision) (coordination.PhaseEndpoint, productionPhaseOutputBoundary, error) {
	if binding.InputKind != "phase_output" || binding.TargetKind != "phase_output" || binding.TargetRef == "" {
		return coordination.PhaseEndpoint{}, productionPhaseOutputBoundary{}, kernel.Forbidden("submitted requires a Runtime-authenticated phase_output boundary")
	}
	ref, err := trustedDecisionEndpoint(binding, decision)
	if err != nil {
		return coordination.PhaseEndpoint{}, productionPhaseOutputBoundary{}, err
	}
	endpoint, err := productionSnapshotEndpoint(snapshot, ref)
	if err != nil {
		return coordination.PhaseEndpoint{}, productionPhaseOutputBoundary{}, err
	}
	var output productionPhaseOutputBoundary
	if err := json.Unmarshal(binding.InputPayload, &output); err != nil {
		return coordination.PhaseEndpoint{}, productionPhaseOutputBoundary{}, trustedBoundaryError("phase_output payload is invalid")
	}
	if output.OutputRef == "" || output.OutputRef != binding.TargetRef || output.Receipt.Endpoint != ref {
		return coordination.PhaseEndpoint{}, productionPhaseOutputBoundary{}, trustedBoundaryError("phase_output identity does not match its persisted binding")
	}
	if output.Receipt.Generation != endpoint.Generation || output.Receipt.BindingRef != endpoint.BindingRef {
		return coordination.PhaseEndpoint{}, productionPhaseOutputBoundary{}, kernel.StaleBinding("phase_output does not match the graph snapshot")
	}
	return endpoint, output, nil
}

func trustedPhaseEvaluationBoundary(binding productionTaskManagerBinding, snapshot coordination.GraphSnapshot, decision taskmanager.TaskManagerDecision) (coordination.PhaseEndpoint, productionPhaseEvaluationBoundary, error) {
	if binding.InputKind != "phase_orchestration" || binding.TargetKind != "phase_evaluation" || binding.TargetRef == "" {
		return coordination.PhaseEndpoint{}, productionPhaseEvaluationBoundary{}, kernel.Forbidden("satisfied or rejected requires a Runtime-authenticated phase_evaluation boundary")
	}
	ref, err := trustedDecisionEndpoint(binding, decision)
	if err != nil {
		return coordination.PhaseEndpoint{}, productionPhaseEvaluationBoundary{}, err
	}
	endpoint, err := productionSnapshotEndpoint(snapshot, ref)
	if err != nil {
		return coordination.PhaseEndpoint{}, productionPhaseEvaluationBoundary{}, err
	}
	var evaluation productionPhaseEvaluationBoundary
	if err := json.Unmarshal(binding.InputPayload, &evaluation); err != nil {
		return coordination.PhaseEndpoint{}, productionPhaseEvaluationBoundary{}, trustedBoundaryError("phase_evaluation payload is invalid")
	}
	if evaluation.Output.OutputRef != binding.TargetRef || evaluation.Endpoint != ref || evaluation.Output.Receipt.Endpoint != ref {
		return coordination.PhaseEndpoint{}, productionPhaseEvaluationBoundary{}, trustedBoundaryError("phase_evaluation identity does not match its persisted binding")
	}
	if evaluation.Generation != endpoint.Generation || evaluation.BindingRef != endpoint.BindingRef {
		return coordination.PhaseEndpoint{}, productionPhaseEvaluationBoundary{}, kernel.StaleBinding("phase_evaluation does not match the graph snapshot")
	}
	if evaluation.Output.Receipt.Generation != endpoint.Generation || evaluation.Output.Receipt.BindingRef != endpoint.BindingRef {
		return coordination.PhaseEndpoint{}, productionPhaseEvaluationBoundary{}, kernel.StaleBinding("phase_evaluation output does not match the graph snapshot")
	}
	return endpoint, evaluation, nil
}

func trustedPhaseStoppedBoundary(binding productionTaskManagerBinding, snapshot coordination.GraphSnapshot, decision taskmanager.TaskManagerDecision) (coordination.PhaseEndpoint, productionPhaseStoppedBoundary, error) {
	if binding.InputKind != "phase_stopped" || binding.TargetKind != "phase_stopped" || binding.TargetRef == "" {
		return coordination.PhaseEndpoint{}, productionPhaseStoppedBoundary{}, kernel.Forbidden("stopped requires a Runtime-authenticated phase_stopped boundary")
	}
	ref, err := trustedDecisionEndpoint(binding, decision)
	if err != nil {
		return coordination.PhaseEndpoint{}, productionPhaseStoppedBoundary{}, err
	}
	endpoint, err := productionSnapshotEndpoint(snapshot, ref)
	if err != nil {
		return coordination.PhaseEndpoint{}, productionPhaseStoppedBoundary{}, err
	}
	var stopped productionPhaseStoppedBoundary
	if err := json.Unmarshal(binding.InputPayload, &stopped); err != nil {
		return coordination.PhaseEndpoint{}, productionPhaseStoppedBoundary{}, trustedBoundaryError("phase_stopped payload is invalid")
	}
	if stopped.CommandID == "" || stopped.CommandID != binding.TargetRef || stopped.Endpoint != ref || kernel.IsZeroID(stopped.LeaseRef) {
		return coordination.PhaseEndpoint{}, productionPhaseStoppedBoundary{}, trustedBoundaryError("phase_stopped command or lease identity does not match its persisted binding")
	}
	if stopped.Generation != endpoint.Generation || stopped.BindingRef != endpoint.BindingRef {
		return coordination.PhaseEndpoint{}, productionPhaseStoppedBoundary{}, kernel.StaleBinding("phase_stopped does not match the graph snapshot")
	}
	if stopped.CheckpointRef == "" && !stopped.NonResumable {
		return coordination.PhaseEndpoint{}, productionPhaseStoppedBoundary{}, kernel.IncompleteStopEvidence("phase_stopped requires checkpoint_ref or non_resumable")
	}
	return endpoint, stopped, nil
}

func trustedStopReleaseBoundary(binding productionTaskManagerBinding, snapshot coordination.GraphSnapshot, decision taskmanager.TaskManagerDecision) (coordination.PhaseEndpoint, productionStopReleaseBoundary, error) {
	if binding.InputKind != "phase_orchestration" || binding.TargetKind != "stop_release" || binding.TargetRef == "" {
		return coordination.PhaseEndpoint{}, productionStopReleaseBoundary{}, kernel.Forbidden("released requires a Runtime-authenticated stop_release boundary")
	}
	ref, err := trustedDecisionEndpoint(binding, decision)
	if err != nil {
		return coordination.PhaseEndpoint{}, productionStopReleaseBoundary{}, err
	}
	endpoint, err := productionSnapshotEndpoint(snapshot, ref)
	if err != nil {
		return coordination.PhaseEndpoint{}, productionStopReleaseBoundary{}, err
	}
	var release productionStopReleaseBoundary
	if err := json.Unmarshal(binding.InputPayload, &release); err != nil {
		return coordination.PhaseEndpoint{}, productionStopReleaseBoundary{}, trustedBoundaryError("stop_release payload is invalid")
	}
	if release.SourceInputRef == "" || release.Stopped.CommandID != binding.TargetRef || release.Stopped.Endpoint != ref || kernel.IsZeroID(release.Stopped.LeaseRef) || release.NewGeneration != endpoint.Generation || release.NewBindingRef != endpoint.BindingRef || release.NewBindingRef != canonicalProductionBindingRef(ref, release.NewGeneration) {
		return coordination.PhaseEndpoint{}, productionStopReleaseBoundary{}, trustedBoundaryError("stop_release identity does not match the graph snapshot")
	}
	if release.Stopped.CheckpointRef == "" && !release.Stopped.NonResumable {
		return coordination.PhaseEndpoint{}, productionStopReleaseBoundary{}, kernel.IncompleteStopEvidence("stop_release requires checkpoint_ref or non_resumable")
	}
	return endpoint, release, nil
}

func trustedDecisionEndpoint(binding productionTaskManagerBinding, decision taskmanager.TaskManagerDecision) (coordination.PhaseEndpointRef, error) {
	ref, err := trustedEndpoint(binding)
	if err != nil {
		return coordination.PhaseEndpointRef{}, err
	}
	if decision.TargetRef != fmt.Sprintf("%s/%s", ref.TaskID, ref.EndpointID) {
		return coordination.PhaseEndpointRef{}, kernel.InvalidArgument("decision target_ref does not match Runtime-selected endpoint")
	}
	return ref, nil
}

func productionSnapshotEndpoint(snapshot coordination.GraphSnapshot, ref coordination.PhaseEndpointRef) (coordination.PhaseEndpoint, error) {
	for _, endpoint := range snapshot.Endpoints {
		if endpoint.Ref == ref {
			return endpoint, nil
		}
	}
	return coordination.PhaseEndpoint{}, kernel.Error{Code: kernel.CodeNotFound, Message: "Runtime-selected endpoint not found", Recoverable: true}
}

func canonicalProductionPhaseResult(endpoint coordination.PhaseEndpoint, outputRef string, verdict coordination.Verdict) coordination.PhaseResult {
	return coordination.PhaseResult{
		ID:       "phase-result:" + stableProductionSuffix(endpoint.Ref.TaskID, endpoint.Ref.EndpointID, endpoint.Generation, endpoint.BindingRef, outputRef, verdict),
		Endpoint: endpoint.Ref, BindingRef: endpoint.BindingRef, OutputRef: outputRef, Verdict: verdict,
	}
}

func trustedPhaseOutputEvidence(output productionPhaseOutputBoundary) []string {
	refs := append([]string(nil), output.Receipt.Output.DeliveryRefs...)
	refs = append(refs, output.Receipt.Output.ReportRef)
	refs = append(refs, output.Receipt.Output.EvidenceRefs...)
	refs = append(refs, output.OutputRef)
	return uniqueProductionStrings(refs)
}

func trustedBoundaryError(message string) error {
	return kernel.Error{Code: kernel.CodeInternalError, Message: message, Recoverable: true}
}

func trustedEndpoint(binding productionTaskManagerBinding) (coordination.PhaseEndpointRef, error) {
	if binding.SelectedTaskID != "" && binding.SelectedEndpoint != "" {
		return coordination.PhaseEndpointRef{TaskID: binding.SelectedTaskID, EndpointID: binding.SelectedEndpoint}, nil
	}
	if binding.TargetKind == "endpoint" || binding.TargetKind == "phase_endpoint" {
		parts := strings.Split(binding.TargetRef, "/")
		if len(parts) == 2 && parts[0] != "" && parts[1] != "" {
			return coordination.PhaseEndpointRef{TaskID: kernel.TaskID(parts[0]), EndpointID: coordination.EndpointID(parts[1])}, nil
		}
	}
	return coordination.PhaseEndpointRef{}, kernel.Forbidden("input is not bound to a Runtime-selected endpoint")
}

func trustedProductionRequirement(binding productionTaskManagerBinding, taskID kernel.TaskID, contractRef string) (taskmanager.RequirementInput, error) {
	requirement := taskmanager.Requirement{}
	switch binding.InputKind {
	case "requirement":
		var req httpapi.RequirementCreateRequest
		if err := json.Unmarshal(binding.InputPayload, &req); err != nil {
			return taskmanager.RequirementInput{}, trustedBoundaryError("requirement payload is invalid")
		}
		body, err := trustedProductionBody(binding.InputBody, req.Body)
		if err != nil {
			return taskmanager.RequirementInput{}, err
		}
		requirement.Text = body
		requirement.Goal = strings.TrimSpace(req.Motivation)
		requirement.Constraints = uniqueProductionStrings(append(append([]string(nil), req.Constraints...), req.Acceptance...))
		if req.Source != nil && strings.TrimSpace(req.Source.ExternalRef) != "" {
			requirement.EvidenceRefs = []string{strings.TrimSpace(req.Source.ExternalRef)}
		}
	case "manager":
		var req httpapi.ManagerMessageRequest
		if err := json.Unmarshal(binding.InputPayload, &req); err != nil {
			return taskmanager.RequirementInput{}, trustedBoundaryError("manager payload is invalid")
		}
		body, err := trustedProductionBody(binding.InputBody, req.Body)
		if err != nil {
			return taskmanager.RequirementInput{}, err
		}
		requirement.Text, requirement.Goal = body, body
	case "human":
		var req httpapi.HumanDecisionRequest
		if err := json.Unmarshal(binding.InputPayload, &req); err != nil {
			return taskmanager.RequirementInput{}, trustedBoundaryError("human payload is invalid")
		}
		body, err := trustedProductionBody(binding.InputBody, req.Decision+": "+req.Reason)
		if err != nil {
			return taskmanager.RequirementInput{}, err
		}
		requirement.Text, requirement.Goal = body, strings.TrimSpace(req.Reason)
		for _, evidence := range req.EvidenceRefs {
			if strings.TrimSpace(evidence.ArtifactID) != "" {
				requirement.EvidenceRefs = append(requirement.EvidenceRefs, strings.TrimSpace(evidence.ArtifactID))
			}
		}
		requirement.EvidenceRefs = uniqueProductionStrings(requirement.EvidenceRefs)
	case "phase_orchestration":
		if binding.TargetKind != "phase_orchestration" {
			return taskmanager.RequirementInput{}, kernel.Forbidden("follow-up phase input cannot define a task requirement")
		}
		var proposal struct {
			Rationale           string   `json:"rationale"`
			OrchestrationAdvice string   `json:"orchestration_advice"`
			EvidenceRefs        []string `json:"evidence_refs"`
		}
		if err := json.Unmarshal(binding.InputPayload, &proposal); err != nil {
			return taskmanager.RequirementInput{}, trustedBoundaryError("phase orchestration payload is invalid")
		}
		body := strings.TrimSpace(binding.InputBody)
		if body == "" {
			body = strings.TrimSpace(proposal.Rationale)
		}
		requirement.Text, requirement.Goal = body, strings.TrimSpace(proposal.OrchestrationAdvice)
		requirement.EvidenceRefs = uniqueProductionStrings(proposal.EvidenceRefs)
	default:
		return taskmanager.RequirementInput{}, kernel.Forbidden("input kind cannot define a task requirement")
	}
	if strings.TrimSpace(requirement.Text) == "" {
		return taskmanager.RequirementInput{}, kernel.InvalidArgument("trusted requirement body is required")
	}
	return taskmanager.RequirementInput{
		InputRef: "requirement-input:" + stableProductionSuffix(binding.InputRef, taskID),
		TaskID:   taskID, ContractRef: contractRef, Requirement: requirement,
	}, nil
}

func trustedProductionBody(stored, payload string) (string, error) {
	stored = strings.TrimSpace(stored)
	payload = strings.TrimSpace(payload)
	if stored == "" || payload == "" || stored != payload {
		return "", trustedBoundaryError("persisted input body does not match its payload")
	}
	return stored, nil
}

func validateProductionTaskID(taskID kernel.TaskID) error {
	if err := kernel.RequireID("task.id", taskID); err != nil {
		return err
	}
	raw := string(taskID)
	if raw != strings.TrimSpace(raw) || strings.ContainsAny(raw, "/\\?# \t\r\n") {
		return kernel.InvalidArgument("task.id must be a canonical URI path segment")
	}
	return nil
}

func validateProductionEndpointRef(ref coordination.PhaseEndpointRef) error {
	if err := validateProductionTaskID(ref.TaskID); err != nil {
		return err
	}
	switch ref.EndpointID {
	case coordination.EndpointPlan, coordination.EndpointExecute, coordination.EndpointVerify:
		return nil
	default:
		return kernel.InvalidArgument("endpoint_id must be one of plan, execute, verify")
	}
}

func canonicalProductionBindingRef(ref coordination.PhaseEndpointRef, generation int) kernel.BindingRef {
	return kernel.BindingRef(fmt.Sprintf("binding://%s/%s/%d", ref.TaskID, ref.EndpointID, generation))
}

func canonicalProductionContractRef(projectID kernel.ProjectID, inputRef string, taskID kernel.TaskID) string {
	return "task-contract:" + stableProductionSuffix(projectID, inputRef, taskID)
}

func canonicalProductionSpecRef(projectID kernel.ProjectID, inputRef string, ref coordination.PhaseEndpointRef) string {
	return "phase-spec:" + stableProductionSuffix(projectID, inputRef, ref.TaskID, ref.EndpointID)
}

func validateProductionEdges(edges []coordination.Edge, existing, replacement []coordination.PhaseEndpoint) error {
	known := make(map[coordination.PhaseEndpointRef]struct{}, len(existing)+len(replacement))
	scope := make(map[coordination.PhaseEndpointRef]struct{}, len(replacement))
	for _, endpoint := range existing {
		known[endpoint.Ref] = struct{}{}
	}
	for _, endpoint := range replacement {
		known[endpoint.Ref] = struct{}{}
		scope[endpoint.Ref] = struct{}{}
	}
	seen := make(map[string]struct{}, len(edges))
	for _, edge := range edges {
		if err := validateProductionEndpointRef(edge.From); err != nil {
			return err
		}
		if err := validateProductionEndpointRef(edge.To); err != nil {
			return err
		}
		if _, ok := known[edge.From]; !ok {
			return kernel.InvalidGraph("edge source references unknown endpoint")
		}
		if _, ok := scope[edge.To]; !ok {
			return kernel.InvalidArgument("pending subgraph edges must target an endpoint in scope")
		}
		if edge.From.TaskID == edge.To.TaskID {
			return kernel.InvalidGraph("fixed plan/execute/verify order is not an editable edge")
		}
		if edge.Signal != coordination.SignalPhaseSatisfied && edge.Signal != coordination.SignalTaskDone {
			return kernel.InvalidArgument("edge.signal is not allowed")
		}
		if edge.RequiredBy != coordination.RequiredByStart && edge.RequiredBy != coordination.RequiredByCompletion {
			return kernel.InvalidArgument("edge.required_by is not allowed")
		}
		if edge.OnFalse != coordination.OnFalseBlock && edge.OnFalse != coordination.OnFalseReplan && edge.OnFalse != coordination.OnFalseCancel {
			return kernel.InvalidArgument("edge.on_false is not allowed")
		}
		for _, artifactKind := range edge.ArtifactKinds {
			if strings.TrimSpace(artifactKind) == "" {
				return kernel.InvalidArgument("edge.artifact_kinds cannot contain empty values")
			}
		}
		key := productionEdgeKey(edge)
		if _, duplicate := seen[key]; duplicate {
			return kernel.InvalidGraph("duplicate edge in pending subgraph")
		}
		seen[key] = struct{}{}
	}
	return nil
}

func validateProductionBlockers(blockers []coordination.Blocker, replacement []coordination.PhaseEndpoint) error {
	scope := make(map[coordination.PhaseEndpointRef]struct{}, len(replacement))
	for _, endpoint := range replacement {
		scope[endpoint.Ref] = struct{}{}
	}
	seen := make(map[string]struct{}, len(blockers))
	for _, blocker := range blockers {
		if strings.TrimSpace(blocker.ID) == "" {
			return kernel.InvalidArgument("blocker.id is required")
		}
		if _, duplicate := seen[blocker.ID]; duplicate {
			return kernel.InvalidGraph("duplicate blocker in pending subgraph")
		}
		seen[blocker.ID] = struct{}{}
		if err := validateProductionEndpointRef(blocker.Target); err != nil {
			return err
		}
		if _, ok := scope[blocker.Target]; !ok {
			return kernel.InvalidArgument("pending subgraph blockers must target an endpoint in scope")
		}
		if blocker.RequiredBy != coordination.RequiredByStart && blocker.RequiredBy != coordination.RequiredByCompletion {
			return kernel.InvalidArgument("blocker.required_by is not allowed")
		}
		if blocker.OnFalse != coordination.OnFalseBlock && blocker.OnFalse != coordination.OnFalseReplan && blocker.OnFalse != coordination.OnFalseCancel {
			return kernel.InvalidArgument("blocker.on_false is not allowed")
		}
		if blocker.State != coordination.BlockerActive && blocker.State != coordination.BlockerResolved && blocker.State != coordination.BlockerDenied && blocker.State != coordination.BlockerObsolete {
			return kernel.InvalidArgument("blocker.state is not allowed")
		}
	}
	return nil
}

func productionEdgeKey(edge coordination.Edge) string {
	return fmt.Sprintf("%s/%s>%s/%s:%s:%s", edge.From.TaskID, edge.From.EndpointID, edge.To.TaskID, edge.To.EndpointID, edge.Signal, edge.RequiredBy)
}

func sameProductionJSON(left, right any) bool {
	leftRaw, leftErr := json.Marshal(left)
	rightRaw, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftRaw, rightRaw)
}

func sameProductionSlice[T any](left, right []T) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if !sameProductionJSON(left[i], right[i]) {
			return false
		}
	}
	return true
}

func uniqueProductionStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func (p *productionTaskManagerRuntime) complete(ctx context.Context, invocationID kernel.InvocationID, decisionRef string, revision kernel.Revision) error {
	if revision <= 0 {
		return kernel.InvalidArgument("applied graph revision is required")
	}
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := p.now().UTC()
	result, err := tx.ExecContext(ctx, `UPDATE production_taskmanager_bindings
SET mutation_applied=TRUE, applied_graph_revision=COALESCE(applied_graph_revision,$3), completed_at=COALESCE(completed_at,$4)
WHERE invocation_id=$1 AND decision_ref=$2 AND (applied_graph_revision IS NULL OR applied_graph_revision=$3)`, invocationID, decisionRef, revision, now)
	if err != nil {
		return err
	}
	if affected, _ := result.RowsAffected(); affected == 0 {
		return kernel.IdempotencyConflict()
	}
	if _, err := tx.ExecContext(ctx, `UPDATE production_manager_inputs SET status='completed', updated_at=$2 WHERE invocation_id=$1`, invocationID, now); err != nil {
		return err
	}
	// AgentTeams may start the agent before ingress records the dispatch ack.
	// Completing from prepared is the fail-safe catch-up for that race; the
	// later dispatch ack cannot move a completed invocation back to running.
	if _, err := tx.ExecContext(ctx, `UPDATE runtime_invocations SET status='completed' WHERE invocation_id=$1 AND status IN ('prepared','running','waiting')`, invocationID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE production_conversation_entries SET graph_revision=$1 WHERE project_id=$2 AND decision_ref=$3`, revision, p.projectID, decisionRef); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (p *productionTaskManagerRuntime) persistDecisionAcceptance(ctx context.Context, binding productionTaskManagerBinding, decisionRef string, kind taskmanager.DecisionKind, decision taskmanager.TaskManagerDecision, revision kernel.Revision) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE production_taskmanager_bindings
SET decision_ref=$2, decision_kind=$3, decision_action=$4
WHERE invocation_id=$1 AND (decision_ref IS NULL OR decision_ref=$2)`, binding.InvocationID, decisionRef, kind, decision.Action)
	if err != nil {
		return err
	}
	if affected, _ := result.RowsAffected(); affected == 0 {
		return kernel.IdempotencyConflict()
	}
	body, _ := json.Marshal(decision)
	if _, err := tx.ExecContext(ctx, `INSERT INTO production_conversation_entries(project_id, conversation_id, entry_id, entry_kind, manager_input_ref, decision_ref, graph_revision, body, disposition, created_at)
VALUES ($1,$2,$3,'decision',$4,$5,$6,$7,$8,$9)
ON CONFLICT (project_id, conversation_id, entry_id) DO NOTHING`, p.projectID, binding.ConversationID, "decision:"+decisionRef, binding.InputRef, decisionRef, revision, string(body), "accepted", p.now().UTC()); err != nil {
		return err
	}
	return tx.Commit()
}

var _ mcpapi.TaskManagerAgentRuntime = (*productionTaskManagerRuntime)(nil)
