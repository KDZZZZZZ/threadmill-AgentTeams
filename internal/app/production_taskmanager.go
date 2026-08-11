package app

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/evidence"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
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
	db          *sql.DB
	projectID   kernel.ProjectID
	graphStore  *coordination.PostgresStore
	decisions   *taskmanager.PostgresStore
	idempotency kernel.IdempotencyStore
	workspaces  productionTaskWorkspaceProvisioner
	contexts    productionTaskContextProjector
	followups   productionTaskManagerFollowupDispatcher
	cleaner     productionTaskManagerExecutionCleaner
	events      evidence.EventStore
	now         func() time.Time
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

type productionTaskWorkspaceRequest struct {
	TaskID     kernel.TaskID
	Generation int
}

type productionTaskContextRequest struct {
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

type productionPhaseStoppedBoundary struct {
	CommandID     string                        `json:"command_id"`
	Endpoint      coordination.PhaseEndpointRef `json:"endpoint"`
	Generation    int                           `json:"generation"`
	BindingRef    kernel.BindingRef             `json:"binding_ref"`
	LeaseRef      kernel.LeaseID                `json:"lease_ref"`
	CheckpointRef string                        `json:"checkpoint_ref"`
	NonResumable  bool                          `json:"non_resumable"`
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

func (p *productionTaskManagerRuntime) setTaskManagerExecutionCleaner(cleaner productionTaskManagerExecutionCleaner) error {
	if cleaner == nil {
		return kernel.InvalidArgument("production Task Manager execution cleaner is required")
	}
	p.cleaner = cleaner
	return nil
}

func (p *productionTaskManagerRuntime) setProductionEventStore(events evidence.EventStore) error {
	if events == nil {
		return kernel.InvalidArgument("production Task Manager event store is required")
	}
	p.events = events
	return nil
}

func (p *productionTaskManagerRuntime) Snapshot(ctx context.Context, caller auth.Principal, scope auth.BoundScope, revision kernel.Revision) (coordination.GraphSnapshot, error) {
	if _, err := p.binding(ctx, caller, scope); err != nil {
		return coordination.GraphSnapshot{}, err
	}
	return p.graph(caller).Snapshot(ctx, revision)
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
		if binding.DecisionKind == taskmanager.DecisionKindTerminal {
			if err := p.cleanupTaskManagerExecution(ctx); err != nil {
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
	if snapshot.Revision != binding.SeenRevision {
		return "", kernel.RevisionConflict(binding.SeenRevision, snapshot.Revision)
	}
	kind, transition, err := trustedDecisionMutation(binding, snapshot, decision)
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
	plan, err := p.normalizePending(binding, base, intent)
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
		if err := p.ensurePendingContext(ctx, recoveredRevision, plan); err != nil {
			return 0, fmt.Errorf("ensure recovered pending context: %w", err)
		}
		if !binding.MutationApplied {
			if err := p.complete(ctx, caller.InvocationID, binding.DecisionRef, recoveredRevision); err != nil {
				return 0, err
			}
		} else if err := p.cleanupTaskManagerExecution(ctx); err != nil {
			return 0, err
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
	if err := p.ensurePendingContext(ctx, revision, plan); err != nil {
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

func (p *productionTaskManagerRuntime) normalizePending(binding productionTaskManagerBinding, snapshot coordination.GraphSnapshot, intent mcpapi.PendingSubgraphIntent) (productionPendingPlan, error) {
	if len(intent.Endpoints) == 0 {
		return productionPendingPlan{}, kernel.InvalidArgument("pending subgraph endpoints are required")
	}
	existingTasks := make(map[kernel.TaskID]coordination.Task, len(snapshot.Tasks))
	for _, task := range snapshot.Tasks {
		existingTasks[task.ID] = task
	}
	existingEndpoints := make(map[coordination.PhaseEndpointRef]coordination.PhaseEndpoint, len(snapshot.Endpoints))
	for _, endpoint := range snapshot.Endpoints {
		existingEndpoints[endpoint.Ref] = endpoint
	}
	semanticTasks := make(map[kernel.TaskID]coordination.Task, len(intent.Tasks))
	for _, task := range intent.Tasks {
		if err := validateProductionTaskID(task.ID); err != nil {
			return productionPendingPlan{}, err
		}
		if _, duplicate := semanticTasks[task.ID]; duplicate {
			return productionPendingPlan{}, kernel.InvalidGraph("duplicate task in pending subgraph")
		}
		semanticTasks[task.ID] = task
	}
	byTask := make(map[kernel.TaskID]map[coordination.EndpointID]coordination.PhaseEndpoint)
	for _, endpoint := range intent.Endpoints {
		if err := validateProductionEndpointRef(endpoint.Ref); err != nil {
			return productionPendingPlan{}, err
		}
		endpoints := byTask[endpoint.Ref.TaskID]
		if endpoints == nil {
			endpoints = make(map[coordination.EndpointID]coordination.PhaseEndpoint, 3)
			byTask[endpoint.Ref.TaskID] = endpoints
		}
		if _, duplicate := endpoints[endpoint.Ref.EndpointID]; duplicate {
			return productionPendingPlan{}, kernel.InvalidGraph("duplicate endpoint in pending subgraph")
		}
		endpoints[endpoint.Ref.EndpointID] = endpoint
	}
	for taskID := range semanticTasks {
		if _, scoped := byTask[taskID]; !scoped {
			return productionPendingPlan{}, kernel.InvalidArgument("pending task must include exactly plan, execute, and verify endpoints")
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
		Blockers:     append([]coordination.Blocker(nil), intent.Blockers...),
	}}
	for _, taskID := range taskIDs {
		endpointIntents := byTask[taskID]
		if len(endpointIntents) != 3 {
			return productionPendingPlan{}, kernel.InvalidArgument(fmt.Sprintf("task %s must include exactly plan, execute, and verify endpoints", taskID))
		}
		semanticTask, supplied := semanticTasks[taskID]
		existingTask, exists := existingTasks[taskID]
		if !exists && !supplied {
			return productionPendingPlan{}, kernel.InvalidArgument("new pending task requires contract_ref")
		}
		contractRef := strings.TrimSpace(semanticTask.ContractRef)
		if exists {
			if existingTask.Outcome != coordination.TaskActive {
				return productionPendingPlan{}, kernel.TransitionRejected("pending replacement requires an active task")
			}
			if supplied && contractRef != "" && contractRef != existingTask.ContractRef {
				return productionPendingPlan{}, kernel.StaleBinding("existing task contract_ref is immutable")
			}
			contractRef = existingTask.ContractRef
		}
		if contractRef == "" {
			return productionPendingPlan{}, kernel.InvalidArgument("task.contract_ref is required")
		}
		canonicalTask := coordination.Task{ID: taskID, ContractRef: contractRef, Outcome: coordination.TaskActive}
		plan.Subgraph.Tasks = append(plan.Subgraph.Tasks, canonicalTask)
		contract := taskmanager.TaskContract{
			TaskID: taskID, ContractRef: contractRef, DeliveryPolicy: taskmanager.DeliveryPolicyCodeMerge,
			PhaseSpecs: make(map[coordination.EndpointID]string, 3),
		}
		generationSet := make(map[int]struct{})
		for _, endpointID := range []coordination.EndpointID{coordination.EndpointPlan, coordination.EndpointExecute, coordination.EndpointVerify} {
			candidate, ok := endpointIntents[endpointID]
			if !ok {
				return productionPendingPlan{}, kernel.InvalidArgument(fmt.Sprintf("task %s is missing %s endpoint", taskID, endpointID))
			}
			candidate.SpecRef = strings.TrimSpace(candidate.SpecRef)
			if candidate.SpecRef == "" {
				return productionPendingPlan{}, kernel.InvalidArgument("endpoint.spec_ref is required")
			}
			if candidate.RunPolicy != coordination.RunEnabled && candidate.RunPolicy != coordination.RunHeld {
				return productionPendingPlan{}, kernel.InvalidArgument("endpoint.run_policy must be enabled or held")
			}
			canonical := coordination.PhaseEndpoint{
				Ref: candidate.Ref, SpecRef: candidate.SpecRef, Generation: 1,
				BindingRef: canonicalProductionBindingRef(candidate.Ref, 1),
				State:      coordination.EndpointPending, RunPolicy: candidate.RunPolicy,
			}
			if exists {
				current, ok := existingEndpoints[candidate.Ref]
				if !ok {
					return productionPendingPlan{}, kernel.InvalidGraph("existing task is missing a fixed endpoint")
				}
				if current.State != coordination.EndpointPending {
					return productionPendingPlan{}, kernel.ScopeNotPending("only pending endpoints can be replaced")
				}
				if candidate.SpecRef != current.SpecRef {
					return productionPendingPlan{}, kernel.StaleBinding("existing endpoint spec_ref is immutable")
				}
				if candidate.RunPolicy != current.RunPolicy {
					return productionPendingPlan{}, kernel.InvalidArgument("ReplacePending cannot rewrite endpoint run policy")
				}
				canonical = current
			}
			contract.PhaseSpecs[endpointID] = canonical.SpecRef
			generationSet[canonical.Generation] = struct{}{}
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

func (p *productionTaskManagerRuntime) ensurePendingContext(ctx context.Context, revision kernel.Revision, plan productionPendingPlan) error {
	if p.contexts == nil {
		return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "production Task Manager context projection is not configured", Recoverable: true}
	}
	for _, resource := range plan.Resources {
		if err := p.contexts.EnsureTaskContext(ctx, productionTaskContextRequest{
			InputRef: resource.Input.InputRef, TaskID: resource.Contract.TaskID, GraphRevision: revision,
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
	if binding.DecisionAction == "submitted" || binding.DecisionAction == "stopped" {
		if p.followups == nil {
			return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "production Task Manager follow-up dispatcher is not configured", Recoverable: true}
		}
		followup, err := p.transitionFollowup(ctx, binding, revision)
		if err != nil {
			return err
		}
		if _, err := p.followups.DispatchTaskManagerFollowup(ctx, followup); err != nil {
			return err
		}
	}
	if binding.MutationApplied {
		return p.cleanupTaskManagerExecution(ctx)
	}
	return p.complete(ctx, binding.InvocationID, binding.DecisionRef, revision)
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

func (p *productionTaskManagerRuntime) cleanupTaskManagerExecution(ctx context.Context) error {
	if p.cleaner == nil {
		return nil
	}
	return p.cleaner.CleanupCompletedTaskManagerInvocations(ctx)
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
	}
	return ""
}

func productionPhaseInvocationStatusForEvent(action string) string {
	switch action {
	case "rejected":
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
	err := p.db.QueryRowContext(ctx, `SELECT b.invocation_id, b.input_ref, i.conversation_id, i.observed_graph_revision,
i.selected_task_id, i.selected_endpoint_id, i.target_kind, i.target_ref,
i.input_kind, i.payload::text,
COALESCE((SELECT e.body FROM production_conversation_entries e
  WHERE e.project_id=i.project_id AND e.manager_input_ref=i.input_ref AND e.entry_kind=i.input_kind
  ORDER BY e.sequence LIMIT 1), ''),
b.decision_ref, b.decision_kind, b.decision_action, b.mutation_applied, b.applied_graph_revision
FROM production_taskmanager_bindings b
JOIN production_manager_inputs i ON i.project_id=b.project_id AND i.input_ref=b.input_ref
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
	graphDecisionRef := binding.DecisionRef
	if binding.DecisionKind == taskmanager.DecisionKindTransition {
		var transitionRaw string
		err := p.db.QueryRowContext(ctx, `SELECT transition_payload::text FROM taskmanager_decisions
WHERE project_id=$1 AND decision_ref=$2 AND input_ref=$3`, p.projectID, binding.DecisionRef, binding.InputRef).Scan(&transitionRaw)
		if errors.Is(err, sql.ErrNoRows) {
			return 0, false, kernel.Error{Code: kernel.CodeInternalError, Message: "persisted Task Manager transition is missing", Recoverable: true}
		}
		if err != nil {
			return 0, false, err
		}
		var transition coordination.GraphTransition
		if err := json.Unmarshal([]byte(transitionRaw), &transition); err != nil {
			return 0, false, kernel.Error{Code: kernel.CodeInternalError, Message: "persisted Task Manager transition is invalid", Recoverable: true}
		}
		canonical, err := json.Marshal(transition)
		if err != nil {
			return 0, false, err
		}
		graphDecisionRef = "transition:" + hashProductionBytes(canonical)
	}
	want := binding.SeenRevision.Next()
	var revision kernel.Revision
	err := p.db.QueryRowContext(ctx, `SELECT revision FROM coordination_graph_revisions
WHERE project_id=$1 AND revision=$2 AND decision_ref=$3`, p.projectID, want, graphDecisionRef).Scan(&revision)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return revision, true, nil
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
		if binding.InputKind != "manager" && binding.InputKind != "human" {
			return "", coordination.GraphTransition{}, kernel.Forbidden("held requires a trusted manager or human boundary")
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
		endpoint, _, err := trustedStopReleaseBoundary(binding, snapshot, decision)
		if err != nil {
			return "", coordination.GraphTransition{}, err
		}
		return taskmanager.DecisionKindTransition, coordination.GraphTransition{TargetKind: coordination.TargetPhaseEndpoint, Endpoint: endpoint.Ref, Action: decision.Action, Generation: endpoint.Generation}, nil
	default:
		return "", coordination.GraphTransition{}, kernel.Forbidden("decision action requires a Runtime-authenticated phase or delivery boundary")
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
	return p.cleanupTaskManagerExecution(ctx)
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
