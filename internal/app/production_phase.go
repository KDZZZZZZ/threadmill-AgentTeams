package app

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/adapters/agentteams"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/contextgraph"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/evidence"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/config"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/objectstore"
	runtimepkg "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime"
	phasepkg "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime/phase"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/taskmanager"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/transport/mcpapi"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/workspace"
)

type productionPhaseBundleOptions struct {
	Config      config.Config
	DB          *sql.DB
	ProjectID   kernel.ProjectID
	Graph       *coordination.PostgresStore
	Assembler   *runtimepkg.Assembler
	Adapter     phasepkg.AgentTeamsPhaseAdapter
	Ingress     *productionIngress
	ObjectStore objectstore.Store
	Workspaces  *workspace.Service
	Now         func() time.Time
}

type productionInvocationSource struct {
	taskManager agentteams.InvocationSource
	context     agentteams.InvocationSource
	phase       agentteams.InvocationSource
}

func (s productionInvocationSource) LoadPreparedInvocation(ctx context.Context, invocationRef string) (agentteams.PreparedInvocation, error) {
	if strings.HasPrefix(invocationRef, "threadmill://phase-invocation/") {
		if s.phase == nil {
			return agentteams.PreparedInvocation{}, kernel.Error{Code: kernel.CodeNotFound, Message: "phase invocation source is not configured"}
		}
		return s.phase.LoadPreparedInvocation(ctx, invocationRef)
	}
	if strings.HasPrefix(invocationRef, "context-invocation:") {
		if s.context == nil {
			return agentteams.PreparedInvocation{}, kernel.Error{Code: kernel.CodeNotFound, Message: "Context Agent invocation source is not configured"}
		}
		return s.context.LoadPreparedInvocation(ctx, invocationRef)
	}
	if s.taskManager == nil {
		return agentteams.PreparedInvocation{}, kernel.Error{Code: kernel.CodeNotFound, Message: "task manager invocation source is not configured"}
	}
	return s.taskManager.LoadPreparedInvocation(ctx, invocationRef)
}

func buildProductionPhaseSeams(options productionPhaseBundleOptions) (productionPhaseSeams, error) {
	if options.DB == nil || options.Graph == nil || options.Assembler == nil || options.Adapter == nil || options.Ingress == nil || options.ObjectStore == nil {
		return productionPhaseSeams{}, kernel.InvalidArgument("production phase database, graph, assembler, adapter, ingress, and object store are required")
	}
	if kernel.IsZeroID(options.ProjectID) {
		return productionPhaseSeams{}, kernel.InvalidArgument("project_id is required")
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	contexts := contextgraph.NewPostgresStore(options.DB, now)
	taskResolver := productionTaskEndpointResolver{projectID: options.ProjectID, graph: options.Graph}
	contexts.SetTaskEndpointResolver(taskResolver)
	workspaces := options.Workspaces
	if workspaces == nil {
		workspaces = workspace.NewPostgresService(options.DB)
	}
	invocations := runtimepkg.NewPostgresInvocationStoreFromSQL(options.DB)
	contracts := taskmanager.NewPostgresStore(options.DB, options.ProjectID, options.Graph)
	artifacts := evidence.NewPostgresArtifactRegistry(options.DB, options.ObjectStore, options.Config.ObjectStoreBucket)
	phaseBindings := &productionPhaseBindingSource{
		db: options.DB, projectID: options.ProjectID, graph: options.Graph, contracts: contracts,
		workspaces: workspaces, contexts: contexts, repositoryPath: options.Config.RepositoryPath,
		worktreeParent: options.Config.WorktreeParent, now: now,
	}
	resolver := phasepkg.NewContextBindingResolver(phaseBindings, contexts)
	hostStore := phasepkg.NewPostgresAgentTeamsPhaseHostStoreFromSQL(options.DB)
	host, err := phasepkg.NewAgentTeamsPhaseHost(phasepkg.AgentTeamsPhaseHostConfig{
		Adapter: options.Adapter,
		Writer:  hostStore,
		State:   hostStore,
		RoomID:  options.Config.AgentTeamsRoomID,
	})
	if err != nil {
		return productionPhaseSeams{}, err
	}
	phaseRuntime := &productionPhaseRuntime{
		db: options.DB, projectID: options.ProjectID, graph: options.Graph, ingress: options.Ingress,
		invocations: invocations, workspaces: workspaces, artifacts: artifacts, source: phaseBindings, workspaceSync: host, now: now,
	}
	recovery := phasepkg.NewPostgresRecoveryStoreFromSQL(options.DB, invocations)
	controller := phasepkg.NewController(phasepkg.Config{
		InvocationStore: invocations,
		Assembler:       options.Assembler,
		BindingResolver: resolver,
		InputRuntime:    productionPhaseInputs{source: phaseBindings},
		ArtifactRouter:  productionPhaseArtifactRouter{registry: artifacts},
		Host:            host,
		RecoveryStore:   recovery,
		Lifecycle: productionPhaseLifecycle{
			workspaces: workspaces,
			contexts:   phasepkg.ContextBindingLifecycle{Contexts: contexts},
		},
		Observations: productionPhaseObservationWriter{
			projectID: options.ProjectID, graph: options.Graph, ingress: options.Ingress, now: now,
		},
		Now: now,
	})
	phaseRuntime.controller = controller
	phaseRuntime.recovery = recovery
	return productionPhaseSeams{
		Controller:     controller,
		Failures:       controller,
		Runtime:        phaseRuntime,
		Orchestration:  phaseRuntime,
		TaskWorkspaces: phaseBindings,
		TaskContexts:   phaseBindings,
		Host:           host,
		WorkspaceSync:  host,
		Recovery:       recovery,
		Contexts:       contexts,
		TaskSubgraphs:  contexts,
		Contracts:      contracts,
		ArtifactRouter: productionPhaseArtifactRouter{registry: artifacts},
		Readiness: productionPhaseReadiness{
			db: options.DB, repositoryPath: options.Config.RepositoryPath, worktreeParent: options.Config.WorktreeParent,
			projectID: options.ProjectID, graph: options.Graph, contexts: contexts, artifacts: artifacts,
		},
	}, nil
}

type productionPhaseBindingSource struct {
	db             *sql.DB
	projectID      kernel.ProjectID
	graph          *coordination.PostgresStore
	contracts      *taskmanager.PostgresStore
	workspaces     *workspace.Service
	contexts       *contextgraph.PostgresStore
	repositoryPath string
	worktreeParent string
	now            func() time.Time
}

func (s *productionPhaseBindingSource) ResolvePhaseBinding(ctx context.Context, req phasepkg.BindingResolveRequest) (phasepkg.BindingSnapshot, []string, error) {
	return s.resolve(ctx, req.Command, req.InvocationID, req.Command.Action != coordination.CommandStop)
}

func (s *productionPhaseBindingSource) RefreshPhaseBinding(ctx context.Context, active phasepkg.ActiveInvocation) (phasepkg.BindingSnapshot, []string, error) {
	return s.resolve(ctx, active.Command, active.Invocation.ID, false)
}

func (s *productionPhaseBindingSource) AbortResolvedPhaseBinding(ctx context.Context, req phasepkg.BindingResolveRequest, binding phasepkg.BindingSnapshot) error {
	if req.Command.Action == coordination.CommandStop || binding.WorkspaceRef == "" || req.InvocationID == "" {
		return nil
	}
	return s.releaseWorkspaceLease(ctx, req.Command.Endpoint, req.InvocationID, kernel.BindingRef(binding.WorkspaceRef))
}

func (s *productionPhaseBindingSource) releaseWorkspaceLease(ctx context.Context, endpoint coordination.PhaseEndpointRef, invocationID kernel.InvocationID, workspaceRef kernel.BindingRef) error {
	if workspaceRef == "" || invocationID == "" {
		return nil
	}
	current, err := s.workspaces.Get(ctx, workspaceRef)
	if err != nil {
		if kernel.IsCode(err, kernel.CodeNotFound) {
			return nil
		}
		return err
	}
	phase := workspacePhase(endpoint.EndpointID)
	if current.ActivePhase != phase || current.ActiveInvocation != invocationID {
		return nil
	}
	_, err = s.workspaces.ReleasePhase(ctx, current.ID, phase, invocationID, current.Revision)
	return err
}

func (s *productionPhaseBindingSource) resolve(ctx context.Context, command coordination.PhaseCommand, invocationID kernel.InvocationID, lease bool) (binding phasepkg.BindingSnapshot, initialSubgraphIDs []string, err error) {
	snapshot, err := s.graph.Latest(ctx, s.projectID)
	if err != nil {
		return phasepkg.BindingSnapshot{}, nil, err
	}
	task, endpoint, err := s.authorizeEndpoint(ctx, snapshot, command)
	if err != nil {
		return phasepkg.BindingSnapshot{}, nil, err
	}
	contract, err := s.contracts.TaskContract(ctx, command.Endpoint.TaskID)
	if err != nil {
		return phasepkg.BindingSnapshot{}, nil, err
	}
	if contract.ContractRef != task.ContractRef || contract.PhaseSpecs[endpoint.Ref.EndpointID] != endpoint.SpecRef {
		return phasepkg.BindingSnapshot{}, nil, kernel.StaleBinding("phase command does not match task contract")
	}
	workspaceBinding, err := s.materializeWorkspace(ctx, command, invocationID, lease)
	if err != nil {
		return phasepkg.BindingSnapshot{}, nil, err
	}
	rollbackLease := lease && command.Action != coordination.CommandStop
	defer func() {
		if err != nil && rollbackLease {
			_ = s.releaseWorkspaceLease(ctx, command.Endpoint, invocationID, workspaceBinding.ID)
		}
	}()
	checkpointRef, nonResumable, err := s.resumeCheckpoint(ctx, command)
	if err != nil {
		return phasepkg.BindingSnapshot{}, nil, err
	}
	if command.Action != coordination.CommandStop {
		if err := s.persistAuthority(ctx, command, workspaceBinding.ID, contract.ContractRef, endpoint.SpecRef, checkpointRef, nonResumable); err != nil {
			return phasepkg.BindingSnapshot{}, nil, err
		}
	} else if err := s.validatePersistedAuthority(ctx, command, workspaceBinding.ID, contract.ContractRef, endpoint.SpecRef); err != nil {
		return phasepkg.BindingSnapshot{}, nil, err
	}
	inputs, err := s.inputs(ctx, snapshot, command)
	if err != nil {
		return phasepkg.BindingSnapshot{}, nil, err
	}
	if command.Action != coordination.CommandStop && hasPendingStartInput(inputs.Pending) {
		return phasepkg.BindingSnapshot{}, nil, kernel.TransitionRejected("phase start inputs are not fully materialized")
	}
	contractJSON, err := stableProductionJSON(contract)
	if err != nil {
		return phasepkg.BindingSnapshot{}, nil, err
	}
	specJSON, err := stableProductionJSON(map[string]any{"spec_ref": endpoint.SpecRef, "endpoint_id": endpoint.Ref.EndpointID})
	if err != nil {
		return phasepkg.BindingSnapshot{}, nil, err
	}
	initial, err := s.initialSubgraphs(ctx, command.Endpoint.TaskID)
	if err != nil {
		return phasepkg.BindingSnapshot{}, nil, err
	}
	workspaceJSON, err := stableProductionJSON(struct {
		WorkspaceRef      kernel.BindingRef  `json:"workspace_ref"`
		WorkspaceRevision string             `json:"workspace_revision"`
		Phase             workspace.Phase    `json:"phase"`
		AllowedDirs       []string           `json:"allowed_dirs"`
		DeclaredWrites    workspace.WriteSet `json:"declared_writes"`
	}{
		WorkspaceRef: workspaceBinding.ID, WorkspaceRevision: workspaceBinding.CurrentRevision,
		Phase: workspacePhase(command.Endpoint.EndpointID), AllowedDirs: append([]string(nil), workspaceBinding.AllowedDirs...),
		DeclaredWrites: workspaceBinding.DeclaredWrites,
	})
	if err != nil {
		return phasepkg.BindingSnapshot{}, nil, err
	}
	rollbackLease = false
	return phasepkg.BindingSnapshot{
		ProjectID:         s.projectID,
		ActorPrincipalID:  phaseActorPrincipal(s.projectID, command.Endpoint, command.Generation),
		TaskID:            command.Endpoint.TaskID,
		EndpointID:        command.Endpoint.EndpointID,
		Generation:        command.Generation,
		BindingRef:        command.BindingRef,
		LeaseRef:          command.LeaseRef,
		WorkspaceRef:      string(workspaceBinding.ID),
		WorkspaceRevision: workspaceBinding.CurrentRevision,
		WorkspaceBinding:  string(workspaceJSON),
		TaskContract:      string(contractJSON),
		PhaseSpec:         string(specJSON),
		Inputs:            inputs,
		CheckpointRef:     checkpointRef,
		NonResumable:      nonResumable,
	}, initial, nil
}

func (s *productionPhaseBindingSource) EnsureTaskWorkspace(ctx context.Context, req productionTaskWorkspaceRequest) (kernel.BindingRef, error) {
	if err := kernel.RequireID("task_id", req.TaskID); err != nil {
		return "", err
	}
	if req.Generation < 0 {
		return "", kernel.InvalidArgument("generation cannot be negative")
	}
	generation := req.Generation
	baseRevision := strings.TrimSpace(req.BaseRevision)
	if generation == 0 {
		latest, ok, err := s.workspaces.GetLatestByTask(ctx, req.TaskID)
		if err != nil {
			return "", err
		}
		if ok && baseRevision != "" && latest.BaseRevision == baseRevision &&
			latest.ActiveInvocation == "" && latest.PhaseLeases[workspace.PhasePlan] != "" &&
			latest.PhaseLeases[workspace.PhaseExecute] == "" && latest.PhaseLeases[workspace.PhaseVerify] == "" {
			// Crash-safe replay of the same Manager-approved fresh round.
			return latest.ID, nil
		} else if ok {
			generation = latest.Generation + 1
		} else {
			generation = 1
		}
	}
	if baseRevision == "" && generation > 1 {
		previous, ok, err := s.workspaces.GetByRound(ctx, req.TaskID, generation-1)
		if err != nil {
			return "", err
		}
		if !ok || previous.CurrentRevision == "" {
			return "", kernel.StaleBinding("previous Task workspace generation is unavailable")
		}
		baseRevision = previous.CurrentRevision
	}
	binding, err := s.workspaces.CreateGitWorktree(ctx, workspace.CreateRequest{
		TaskID: req.TaskID, Generation: generation, RepoPath: s.repositoryPath, WorktreeParent: s.worktreeParent,
		BaseRevision: baseRevision,
	})
	if err != nil {
		return "", err
	}
	binding, err = s.workspaces.Materialize(ctx, binding.ID)
	if err != nil {
		return "", err
	}
	// A Manager-approved reopened round starts from the exact latest-main
	// revision observed by targeted verify. Project the completed plan marker
	// into that fresh round so workspaceForStart selects it for execute instead
	// of treating it as an unused speculative shell.
	if strings.TrimSpace(req.BaseRevision) != "" && generation > 1 {
		previous, ok, err := s.workspaces.GetByRound(ctx, req.TaskID, generation-1)
		if err != nil {
			return "", err
		}
		if !ok {
			return "", kernel.StaleBinding("previous Task workspace generation is unavailable")
		}
		if previous.PhaseLeases[workspace.PhasePlan] == "" {
			return "", kernel.TransitionRejected("reopen_round requires a completed plan workspace prerequisite")
		}
		binding, err = s.workspaces.InheritPlanPrerequisite(ctx, binding.ID, previous.ID)
		if err != nil {
			return "", err
		}
	}
	return binding.ID, nil
}

// EnsureMergedVerifyWorkspace materializes the exact revision Merge Queue
// wrote to main and projects only the completed plan/execute prerequisites
// into that read-only verification round. This is the sole bridge from the
// internal merge lifecycle back to normal Phase scheduling.
func (s *productionPhaseBindingSource) EnsureMergedVerifyWorkspace(ctx context.Context, taskID kernel.TaskID, sourceRef kernel.BindingRef, mergedRevision string) (kernel.BindingRef, error) {
	if err := kernel.RequireID("task_id", taskID); err != nil {
		return "", err
	}
	if sourceRef == "" || strings.TrimSpace(mergedRevision) == "" {
		return "", kernel.InvalidArgument("source workspace and merged revision are required")
	}
	source, err := s.workspaces.Get(ctx, sourceRef)
	if err != nil {
		return "", err
	}
	if source.TaskID != taskID {
		return "", kernel.StaleBinding("merge source workspace belongs to another task")
	}
	if latest, ok, err := s.workspaces.GetLatestByTask(ctx, taskID); err != nil {
		return "", err
	} else if ok && latest.Generation == source.Generation+1 && latest.BaseRevision == mergedRevision &&
		latest.CurrentRevision == mergedRevision && latest.PhaseLeases[workspace.PhasePlan] != "" &&
		latest.PhaseLeases[workspace.PhaseExecute] != "" && latest.PhaseLeases[workspace.PhaseVerify] == "" {
		return latest.ID, nil
	}
	target, err := s.workspaces.CreateGitWorktree(ctx, workspace.CreateRequest{
		TaskID: taskID, Generation: source.Generation + 1,
		RepoPath: s.repositoryPath, WorktreeParent: s.worktreeParent, BaseRevision: mergedRevision,
	})
	if err != nil {
		return "", err
	}
	target, err = s.workspaces.InheritMergedVerifyPrerequisites(ctx, target.ID, source.ID)
	if err != nil {
		return "", err
	}
	return target.ID, nil
}

func (s *productionPhaseBindingSource) EnsureTaskContext(ctx context.Context, req productionTaskContextRequest) error {
	if err := kernel.RequireID("invocation_id", req.InvocationID); err != nil {
		return err
	}
	if err := kernel.RequireID("task_id", req.TaskID); err != nil {
		return err
	}
	principal := auth.Principal{
		ActorPrincipalID: kernel.ActorPrincipalID("production-task-manager:" + string(s.projectID)),
		Kind:             auth.PrincipalAgent,
		ProjectID:        s.projectID,
		InvocationID:     req.InvocationID,
		Role:             auth.RoleTaskManager,
		TaskID:           req.TaskID,
		Tools:            auth.ToolSet(auth.ToolContextRegisterTaskSubgraph, auth.ToolContextProjectTaskContext),
	}
	binding, err := s.contexts.RegisterTaskSubgraph(ctx, principal, req.TaskID)
	if err != nil {
		return err
	}
	endpointIDs := make([]string, 0, len(req.Contract.PhaseSpecs))
	for endpointID := range req.Contract.PhaseSpecs {
		endpointIDs = append(endpointIDs, string(endpointID))
	}
	sort.Strings(endpointIDs)
	endpoints := make([]contextgraph.PhaseEndpointRef, 0, len(endpointIDs))
	for _, endpointID := range endpointIDs {
		endpoints = append(endpoints, contextgraph.PhaseEndpointRef{TaskID: req.TaskID, EndpointID: coordination.EndpointID(endpointID)})
	}
	for _, projection := range productionTaskContextProjections(s.projectID, req, binding.SubgraphID, endpoints) {
		if _, err := s.contexts.ProjectTaskContext(ctx, principal, contextgraph.ProjectTaskContextRequest{Projection: projection}); err != nil {
			return err
		}
	}
	return nil
}

// productionTaskContextProjections keeps Context Graph statements atomic. A
// requirement or contract is authoritative as a whole in its source store,
// but copying that whole object into one ContextNode makes retrieval coarse and
// prevents later agents from citing the exact constraint or decision they
// reused. Each stable semantic unit therefore becomes one independently
// addressable task projection with the same source revision.
func productionTaskContextProjections(projectID kernel.ProjectID, req productionTaskContextRequest, subgraphID string, endpoints []contextgraph.PhaseEndpointRef) []contextgraph.TaskContextProjection {
	allRecipients := []contextgraph.TaskContextRecipient{{TaskID: string(req.TaskID), EndpointRefs: append([]contextgraph.PhaseEndpointRef(nil), endpoints...)}}
	requirementRefs := []string{"requirement:" + req.InputRef}
	requirementRefs = uniqueProductionStrings(append(requirementRefs, req.Requirement.EvidenceRefs...))
	contractRef := "contract:" + req.Contract.ContractRef
	sourceRevision := fmt.Sprint(req.GraphRevision)
	projections := make([]contextgraph.TaskContextProjection, 0, 8+len(req.Requirement.Constraints)+len(req.Contract.PhaseSpecs))
	appendProjection := func(category string, statement string, sourceRefs []string, recipients []contextgraph.TaskContextRecipient) {
		statement = strings.TrimSpace(statement)
		if statement == "" {
			return
		}
		projections = append(projections, contextgraph.TaskContextProjection{
			// Projection identity follows the semantic unit, not its position in a
			// sorted input list. Adding a new constraint must never make an existing
			// NodeRef silently change meaning.
			ProjectionID:   stableRuntimeRef("task-context", projectID, req.TaskID, category, statement),
			SourceRevision: sourceRevision,
			Statement:      statement,
			Kind:           string(contextgraph.NodeKindDirective),
			SourceRefs:     append([]string(nil), sourceRefs...),
			SubgraphIDs:    []string{subgraphID},
			Recipients:     cloneProductionContextRecipients(recipients),
		})
	}

	for _, statement := range productionAtomicContextStatements(req.Requirement.Text) {
		appendProjection("requirement", statement, requirementRefs, allRecipients)
	}
	for _, statement := range productionAtomicContextStatements(req.Requirement.Goal) {
		appendProjection("goal", statement, requirementRefs, allRecipients)
	}
	for _, constraint := range req.Requirement.Constraints {
		for _, statement := range productionAtomicContextStatements(constraint) {
			appendProjection("constraint", statement, requirementRefs, allRecipients)
		}
	}
	appendProjection("delivery-policy", fmt.Sprintf("交付策略要求 %s。", req.Contract.DeliveryPolicy), []string{contractRef}, allRecipients)
	return projections
}

func productionAtomicContextStatements(value string) []string {
	parts := strings.FieldsFunc(value, func(r rune) bool {
		switch r {
		case '\n', '\r', '。', '！', '？', '；', ';':
			return true
		default:
			return false
		}
	})
	return uniqueProductionStrings(parts)
}

func cloneProductionContextRecipients(input []contextgraph.TaskContextRecipient) []contextgraph.TaskContextRecipient {
	cloned := make([]contextgraph.TaskContextRecipient, len(input))
	for index, recipient := range input {
		cloned[index] = contextgraph.TaskContextRecipient{TaskID: recipient.TaskID, EndpointRefs: append([]contextgraph.PhaseEndpointRef(nil), recipient.EndpointRefs...)}
	}
	return cloned
}

func (s *productionPhaseBindingSource) authorizeEndpoint(ctx context.Context, snapshot coordination.GraphSnapshot, command coordination.PhaseCommand) (coordination.Task, coordination.PhaseEndpoint, error) {
	var task coordination.Task
	for _, candidate := range snapshot.Tasks {
		if candidate.ID == command.Endpoint.TaskID {
			task = candidate
			break
		}
	}
	if task.ID == "" {
		return coordination.Task{}, coordination.PhaseEndpoint{}, kernel.Error{Code: kernel.CodeNotFound, Message: "phase task not found", Recoverable: true}
	}
	if task.Outcome != coordination.TaskActive {
		return coordination.Task{}, coordination.PhaseEndpoint{}, kernel.TransitionRejected("phase task is not active")
	}
	for _, endpoint := range snapshot.Endpoints {
		if endpoint.Ref == command.Endpoint {
			if endpoint.Generation != command.Generation || endpoint.BindingRef != command.BindingRef {
				return coordination.Task{}, coordination.PhaseEndpoint{}, kernel.StaleBinding("phase command does not match current endpoint")
			}
			if endpoint.RunPolicy == coordination.RunHeld && command.Action != coordination.CommandStop {
				return coordination.Task{}, coordination.PhaseEndpoint{}, kernel.TransitionRejected("held phase cannot start or resume")
			}
			return task, endpoint, nil
		}
	}
	return coordination.Task{}, coordination.PhaseEndpoint{}, kernel.Error{Code: kernel.CodeNotFound, Message: "phase endpoint not found", Recoverable: true}
}

func (s *productionPhaseBindingSource) materializeWorkspace(ctx context.Context, command coordination.PhaseCommand, invocationID kernel.InvocationID, lease bool) (workspace.Binding, error) {
	var binding workspace.Binding
	var err error
	switch {
	case command.Action == coordination.CommandStop:
		binding, err = s.workspaceForStop(ctx, command)
	case command.Action == coordination.CommandResume && command.Generation > 1:
		binding, err = s.workspaceForResume(ctx, command)
	default:
		binding, err = s.workspaceForStart(ctx, command, invocationID)
	}
	if err != nil {
		return workspace.Binding{}, err
	}
	binding, err = s.workspaces.Materialize(ctx, binding.ID)
	if err != nil {
		return workspace.Binding{}, err
	}
	phase := workspacePhase(command.Endpoint.EndpointID)
	alreadyBound := binding.ActivePhase == phase && binding.ActiveInvocation == invocationID
	if lease && command.Action == coordination.CommandStart && phase == workspace.PhaseExecute && !alreadyBound {
		binding, err = s.promotePlannerWriteAuthority(ctx, binding)
		if err != nil {
			return workspace.Binding{}, err
		}
	}
	if !lease || command.Action == coordination.CommandStop {
		return binding, nil
	}
	return s.workspaces.BindPhase(ctx, binding.ID, phase, invocationID, binding.Revision)
}

// workspaceForStart resolves the Task-owned round independently from the
// endpoint control generation. Plan, execute, and verify reuse the same round;
// a higher endpoint generation only creates a new Workspace when the latest
// round has already completed the phase being retried.
func (s *productionPhaseBindingSource) workspaceForStart(ctx context.Context, command coordination.PhaseCommand, invocationID kernel.InvocationID) (workspace.Binding, error) {
	phase := workspacePhase(command.Endpoint.EndpointID)
	latest, ok, err := s.workspaces.GetLatestByTask(ctx, command.Endpoint.TaskID)
	if err != nil {
		return workspace.Binding{}, err
	}
	if !ok {
		return s.workspaces.CreateGitWorktree(ctx, workspace.CreateRequest{
			TaskID: command.Endpoint.TaskID, Generation: 1,
			RepoPath: s.repositoryPath, WorktreeParent: s.worktreeParent,
		})
	}
	for generation := latest.Generation; generation > 0; generation-- {
		candidate := latest
		if generation != latest.Generation {
			candidate, ok, err = s.workspaces.GetByRound(ctx, command.Endpoint.TaskID, generation)
			if err != nil {
				return workspace.Binding{}, err
			}
			if !ok {
				continue
			}
		}
		if candidate.ActiveInvocation != "" &&
			(candidate.ActiveInvocation != invocationID || candidate.ActivePhase != phase) {
			return workspace.Binding{}, kernel.LeaseConflict("another phase holds the Task workspace write lease")
		}
		if !productionWorkspacePrerequisitesMet(candidate, phase) {
			// Older binaries could pre-create the next round from repository
			// HEAD before a failed endpoint was reopened. Such a never-leased,
			// unchanged shell is not an execution lineage and must not hide the
			// previous usable Task round.
			if productionWorkspaceRoundUnused(candidate) {
				continue
			}
			return workspace.Binding{}, kernel.TransitionRejected("latest Task workspace phase prerequisites are not complete")
		}
		if candidate.PhaseLeases[phase] == "" {
			return candidate, nil
		}
		latest = candidate
		break
	}
	next, err := s.workspaces.CreateGitWorktree(ctx, workspace.CreateRequest{
		TaskID: command.Endpoint.TaskID, Generation: latest.Generation + 1,
		RepoPath: s.repositoryPath, WorktreeParent: s.worktreeParent, BaseRevision: latest.CurrentRevision,
	})
	if err != nil {
		return workspace.Binding{}, err
	}
	return s.workspaces.InheritPhasePrerequisites(ctx, next.ID, latest.ID, phase, invocationID)
}

func productionWorkspacePrerequisitesMet(binding workspace.Binding, phase workspace.Phase) bool {
	switch phase {
	case workspace.PhasePlan:
		return true
	case workspace.PhaseExecute:
		return binding.PhaseLeases[workspace.PhasePlan] != ""
	case workspace.PhaseVerify:
		return binding.PhaseLeases[workspace.PhasePlan] != "" && binding.PhaseLeases[workspace.PhaseExecute] != ""
	default:
		return false
	}
}

func productionWorkspaceRoundUnused(binding workspace.Binding) bool {
	return binding.ActiveInvocation == "" && len(binding.PhaseLeases) == 0 &&
		binding.BaseRevision == binding.CurrentRevision && productionWriteSetEmpty(binding.DeclaredWrites) &&
		productionWriteSetEmpty(binding.ObservedWrites)
}

func productionWriteSetEmpty(set workspace.WriteSet) bool {
	return len(set.Files) == 0 && len(set.Modules) == 0 && len(set.Symbols) == 0 &&
		len(set.Contracts) == 0 && len(set.Tests) == 0 && len(set.Owners) == 0
}

func (s *productionPhaseBindingSource) promotePlannerWriteAuthority(ctx context.Context, binding workspace.Binding) (workspace.Binding, error) {
	raw, err := os.ReadFile(filepath.Join(binding.Root, "plan", "declared-writes.json"))
	if err != nil {
		if os.IsNotExist(err) {
			// A fresh Manager-approved round may deliberately start from a newer
			// main revision that does not contain Task-local plan artifacts. In
			// that case Workspace Service has already inherited the completed-plan
			// marker and its normalized write authority from the prior round.
			if binding.Generation > 1 && binding.PhaseLeases[workspace.PhasePlan] != "" {
				return binding, nil
			}
			return workspace.Binding{}, kernel.TransitionRejected("execute requires plan/declared-writes.json from the completed planner phase")
		}
		return workspace.Binding{}, fmt.Errorf("read planner declared write set: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var declared workspace.WriteSet
	if err := decoder.Decode(&declared); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return workspace.Binding{}, kernel.InvalidArgument("plan/declared-writes.json is invalid")
	}
	return s.workspaces.AuthorizeExecuteWrites(ctx, binding.ID, declared, binding.Revision)
}

func (s *productionPhaseBindingSource) workspaceForStop(ctx context.Context, command coordination.PhaseCommand) (workspace.Binding, error) {
	var workspaceRef string
	err := s.db.QueryRowContext(ctx, `
SELECT workspace_ref
FROM production_phase_bindings
WHERE project_id=$1 AND task_id=$2 AND endpoint_id=$3 AND generation=$4 AND graph_binding_ref=$5`,
		s.projectID, command.Endpoint.TaskID, command.Endpoint.EndpointID, command.Generation, command.BindingRef).Scan(&workspaceRef)
	if errors.Is(err, sql.ErrNoRows) {
		err = s.db.QueryRowContext(ctx, `SELECT workspace_ref FROM runtime_invocations WHERE lease_id=$1 AND project_id=$2`, command.LeaseRef, s.projectID).Scan(&workspaceRef)
	}
	if err != nil {
		return workspace.Binding{}, err
	}
	return s.workspaces.Get(ctx, kernel.BindingRef(workspaceRef))
}

func (s *productionPhaseBindingSource) workspaceForResume(ctx context.Context, command coordination.PhaseCommand) (workspace.Binding, error) {
	var workspaceRef string
	err := s.db.QueryRowContext(ctx, `
SELECT workspace_ref
FROM production_phase_bindings
WHERE project_id=$1 AND task_id=$2 AND endpoint_id=$3 AND generation=$4
ORDER BY created_at DESC
LIMIT 1`,
		s.projectID, command.Endpoint.TaskID, command.Endpoint.EndpointID, command.Generation-1).Scan(&workspaceRef)
	if errors.Is(err, sql.ErrNoRows) {
		return workspace.Binding{}, kernel.Error{Code: kernel.CodeStaleCheckpoint, Message: "resume workspace binding is not persisted", Recoverable: true}
	}
	if err != nil {
		return workspace.Binding{}, err
	}
	return s.workspaces.Get(ctx, kernel.BindingRef(workspaceRef))
}

func (s *productionPhaseBindingSource) persistAuthority(ctx context.Context, command coordination.PhaseCommand, workspaceRef kernel.BindingRef, contractRef, specRef, checkpointRef string, nonResumable bool) error {
	_, err := s.db.ExecContext(ctx, `
INSERT INTO production_phase_bindings (
  project_id, task_id, endpoint_id, generation, graph_binding_ref, workspace_ref,
  actor_principal_id, contract_ref, spec_ref, checkpoint_ref, non_resumable
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
ON CONFLICT (project_id, task_id, endpoint_id, generation, graph_binding_ref) DO NOTHING`,
		s.projectID, command.Endpoint.TaskID, command.Endpoint.EndpointID, command.Generation, command.BindingRef,
		workspaceRef, phaseActorPrincipal(s.projectID, command.Endpoint, command.Generation), contractRef, specRef,
		checkpointRef, nonResumable)
	if err != nil {
		return err
	}
	var storedWorkspace, storedContract, storedSpec, storedCheckpoint string
	var storedNonResumable bool
	err = s.db.QueryRowContext(ctx, `
SELECT workspace_ref, contract_ref, spec_ref, checkpoint_ref, non_resumable
FROM production_phase_bindings
WHERE project_id=$1 AND task_id=$2 AND endpoint_id=$3 AND generation=$4 AND graph_binding_ref=$5`,
		s.projectID, command.Endpoint.TaskID, command.Endpoint.EndpointID, command.Generation, command.BindingRef).
		Scan(&storedWorkspace, &storedContract, &storedSpec, &storedCheckpoint, &storedNonResumable)
	if err != nil {
		return err
	}
	if storedWorkspace != string(workspaceRef) || storedContract != contractRef || storedSpec != specRef || storedCheckpoint != checkpointRef || storedNonResumable != nonResumable {
		return kernel.IdempotencyConflict()
	}
	return nil
}

func (s *productionPhaseBindingSource) validatePersistedAuthority(ctx context.Context, command coordination.PhaseCommand, workspaceRef kernel.BindingRef, contractRef, specRef string) error {
	var storedWorkspace, storedContract, storedSpec string
	err := s.db.QueryRowContext(ctx, `
SELECT workspace_ref, contract_ref, spec_ref
FROM production_phase_bindings
WHERE project_id=$1 AND task_id=$2 AND endpoint_id=$3 AND generation=$4 AND graph_binding_ref=$5`,
		s.projectID, command.Endpoint.TaskID, command.Endpoint.EndpointID, command.Generation, command.BindingRef).
		Scan(&storedWorkspace, &storedContract, &storedSpec)
	if err != nil {
		return err
	}
	if storedWorkspace != string(workspaceRef) || storedContract != contractRef || storedSpec != specRef {
		return kernel.StaleBinding("stop command does not match persisted phase binding")
	}
	return nil
}

func (s *productionPhaseBindingSource) initialSubgraphs(ctx context.Context, taskID kernel.TaskID) ([]string, error) {
	principal := auth.Principal{
		ActorPrincipalID: kernel.ActorPrincipalID("task-manager://production-phase-context"),
		Kind:             auth.PrincipalAgent,
		ProjectID:        s.projectID,
		Role:             auth.RoleTaskManager,
		TaskID:           taskID,
		Tools:            auth.ToolSet(auth.ToolContextRegisterTaskSubgraph),
	}
	binding, err := s.contexts.RegisterTaskSubgraph(ctx, principal, taskID)
	if err != nil {
		return nil, err
	}
	return []string{binding.SubgraphID}, nil
}

func (s *productionPhaseBindingSource) inputs(ctx context.Context, snapshot coordination.GraphSnapshot, command coordination.PhaseCommand) (phasepkg.PhaseInputSet, error) {
	var required []phasepkg.InputRequirement
	var delivered []phasepkg.InputDelivery
	var pending []phasepkg.PendingInput
	outputs, err := s.phaseOutputs(ctx)
	if err != nil {
		return phasepkg.PhaseInputSet{}, err
	}
	results := make(map[coordination.PhaseEndpointRef]coordination.PhaseResult)
	for _, result := range snapshot.Results {
		if result.Verdict == coordination.VerdictSatisfied {
			results[result.Endpoint] = result
		}
	}
	for _, edge := range snapshot.Edges {
		if edge.To != command.Endpoint {
			continue
		}
		inputID := phaseInputID(edge)
		req := phasepkg.InputRequirement{
			InputID:      inputID,
			FromEndpoint: edge.From,
			RequiredBy:   string(edge.RequiredBy),
		}
		result, ok := results[edge.From]
		output := outputs[result.OutputRef]
		if ok && result.OutputRef != "" && output.OutputRef != "" {
			requiredArtifacts, satisfied, err := s.resolveRequiredArtifacts(ctx, edge.ArtifactKinds, output)
			if err != nil {
				return phasepkg.PhaseInputSet{}, err
			}
			req.RequiredArtifacts = requiredArtifacts
			required = append(required, req)
			if satisfied {
				delivered = append(delivered, phasepkg.InputDelivery{
					InputID: inputID, FromEndpoint: edge.From, PhaseOutputRef: result.OutputRef,
					ArtifactRefs: append([]string(nil), output.ArtifactRefs...), SourceRevision: fmt.Sprint(snapshot.Revision),
				})
				continue
			}
			pending = append(pending, phasepkg.PendingInput{InputID: inputID, FromEndpoint: edge.From, RequiredBy: string(edge.RequiredBy)})
			continue
		}
		required = append(required, req)
		pending = append(pending, phasepkg.PendingInput{InputID: inputID, FromEndpoint: edge.From, RequiredBy: string(edge.RequiredBy)})
	}
	sort.Slice(required, func(i, j int) bool { return required[i].InputID < required[j].InputID })
	sort.Slice(delivered, func(i, j int) bool { return delivered[i].InputID < delivered[j].InputID })
	sort.Slice(pending, func(i, j int) bool { return pending[i].InputID < pending[j].InputID })
	inputs := phasepkg.PhaseInputSet{Required: required, Delivered: delivered, Pending: pending}
	inputs.InputRevision = phaseInputRevision(snapshot.Revision, inputs)
	return inputs, nil
}

func hasPendingStartInput(pending []phasepkg.PendingInput) bool {
	for _, item := range pending {
		if item.RequiredBy == string(coordination.RequiredByStart) {
			return true
		}
	}
	return false
}

func (s *productionPhaseBindingSource) resolveRequiredArtifacts(ctx context.Context, kinds []string, output productionPhaseOutputRecord) ([]string, bool, error) {
	if len(kinds) == 0 {
		return nil, true, nil
	}
	artifactSet := make(map[string]struct{}, len(output.ArtifactRefs))
	for _, ref := range output.ArtifactRefs {
		if strings.HasPrefix(ref, "art_") {
			artifactSet[ref] = struct{}{}
		}
	}
	required := make([]string, 0, len(output.ArtifactRefs))
	seen := map[string]struct{}{}
	add := func(ref string) {
		if ref == "" {
			return
		}
		if _, ok := seen[ref]; ok {
			return
		}
		seen[ref] = struct{}{}
		required = append(required, ref)
	}
	for _, kind := range kinds {
		switch {
		case kind == "phase_output":
			if len(artifactSet) == 0 {
				return nil, false, nil
			}
			for _, ref := range output.ArtifactRefs {
				if _, ok := artifactSet[ref]; ok {
					add(ref)
				}
			}
		case strings.HasPrefix(kind, "art_"):
			if _, ok := artifactSet[kind]; !ok {
				return nil, false, nil
			}
			add(kind)
		default:
			matched, err := s.artifactsByType(ctx, output.ArtifactRefs, kind)
			if err != nil {
				return nil, false, err
			}
			if len(matched) == 0 {
				return nil, false, nil
			}
			for _, ref := range matched {
				if _, ok := artifactSet[ref]; ok {
					add(ref)
				}
			}
		}
	}
	sort.Strings(required)
	return required, true, nil
}

func (s *productionPhaseBindingSource) artifactsByType(ctx context.Context, refs []string, artifactType string) ([]string, error) {
	if len(refs) == 0 {
		return nil, nil
	}
	out := make([]string, 0, len(refs))
	for _, ref := range refs {
		var typ string
		err := s.db.QueryRowContext(ctx, `
SELECT a.type
FROM evidence_artifacts a
JOIN evidence_artifact_grants g ON g.artifact_id = a.id
WHERE a.id=$1 AND g.project_id=$2`, ref, s.projectID).Scan(&typ)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if typ == artifactType {
			out = append(out, ref)
		}
	}
	return out, nil
}

func (s *productionPhaseBindingSource) phaseOutputs(ctx context.Context) (map[string]productionPhaseOutputRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT output_ref, artifact_refs::text
FROM production_phase_outputs
WHERE project_id=$1`, s.projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]productionPhaseOutputRecord{}
	for rows.Next() {
		var record productionPhaseOutputRecord
		var refsRaw string
		if err := rows.Scan(&record.OutputRef, &refsRaw); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(refsRaw), &record.ArtifactRefs); err != nil {
			return nil, err
		}
		out[record.OutputRef] = record
	}
	return out, rows.Err()
}

func (s *productionPhaseBindingSource) resumeCheckpoint(ctx context.Context, command coordination.PhaseCommand) (string, bool, error) {
	if command.Action != coordination.CommandResume {
		return "", false, nil
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT run_command_id, stop_result
FROM phase_recovery_obligations
WHERE stop_result IS NOT NULL
ORDER BY updated_at DESC, run_command_id DESC`)
	if err != nil {
		return "", false, err
	}
	defer rows.Close()
	for rows.Next() {
		var runCommandID string
		var payload []byte
		if err := rows.Scan(&runCommandID, &payload); err != nil {
			return "", false, err
		}
		var projectID kernel.ProjectID
		var taskID kernel.TaskID
		var endpointID kernel.EndpointID
		var generation uint64
		err := s.db.QueryRowContext(ctx, `
SELECT project_id, COALESCE(task_id,''), COALESCE(endpoint_id,''), COALESCE(generation,0)
FROM runtime_invocations
WHERE invocation_id=$1`, deterministicPhaseInvocationID(runCommandID)).
			Scan(&projectID, &taskID, &endpointID, &generation)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return "", false, err
		}
		if projectID != s.projectID || taskID != command.Endpoint.TaskID || endpointID != command.Endpoint.EndpointID || generation+1 != uint64(command.Generation) {
			continue
		}
		var result phasepkg.StopResult
		if err := json.Unmarshal(payload, &result); err != nil {
			return "", false, err
		}
		return result.CheckpointRef, result.NonResumable, nil
	}
	if err := rows.Err(); err != nil {
		return "", false, err
	}
	return "", false, nil
}

type productionPhaseInputs struct {
	source *productionPhaseBindingSource
}

func (p productionPhaseInputs) AwaitInputs(ctx context.Context, active phasepkg.ActiveInvocation, req phasepkg.AwaitInputsRequest) (phasepkg.InputWaitResult, error) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		result, done, err := p.read(ctx, active, req)
		if err != nil || done {
			return result, err
		}
		select {
		case <-ctx.Done():
			return phasepkg.InputWaitResult{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (p productionPhaseInputs) read(ctx context.Context, active phasepkg.ActiveInvocation, req phasepkg.AwaitInputsRequest) (phasepkg.InputWaitResult, bool, error) {
	if !active.Invocation.ExpiresAt.IsZero() && !p.source.now().Before(active.Invocation.ExpiresAt) {
		return phasepkg.InputWaitResult{InputRevision: active.Inputs.InputRevision, Delivered: active.Inputs.Delivered, Pending: active.Inputs.Pending, TerminalReason: "lease_expired"}, true, nil
	}
	snapshot, err := p.source.graph.Latest(ctx, active.Invocation.ProjectID)
	if err != nil {
		return phasepkg.InputWaitResult{}, false, err
	}
	for _, endpoint := range snapshot.Endpoints {
		if endpoint.Ref == active.Command.Endpoint && endpoint.Generation != active.Command.Generation {
			return phasepkg.InputWaitResult{InputRevision: active.Inputs.InputRevision, Delivered: active.Inputs.Delivered, Pending: active.Inputs.Pending, TerminalReason: "input_stale"}, true, nil
		}
	}
	inputs, err := p.source.inputs(ctx, snapshot, active.Command)
	if err != nil {
		return phasepkg.InputWaitResult{}, false, err
	}
	result := phasepkg.InputWaitResult{InputRevision: inputs.InputRevision, Delivered: inputs.Delivered, Pending: inputs.Pending}
	if reason := sourceTerminalReason(snapshot, active.Command); reason != "" {
		result.TerminalReason = reason
		return result, true, nil
	}
	if active.Inputs.InputRevision != "" && inputs.InputRevision != active.Inputs.InputRevision {
		result.TerminalReason = "input_stale"
		return result, true, nil
	}
	if len(req.InputIDs) == 0 {
		return result, len(inputs.Pending) == 0, nil
	}
	delivered := map[string]struct{}{}
	for _, item := range inputs.Delivered {
		delivered[item.InputID] = struct{}{}
	}
	for _, id := range req.InputIDs {
		if _, ok := delivered[id]; !ok {
			return result, false, nil
		}
	}
	return result, true, nil
}

func sourceTerminalReason(snapshot coordination.GraphSnapshot, command coordination.PhaseCommand) string {
	tasks := make(map[kernel.TaskID]coordination.TaskOutcome, len(snapshot.Tasks))
	for _, task := range snapshot.Tasks {
		tasks[task.ID] = task.Outcome
	}
	endpoints := make(map[coordination.PhaseEndpointRef]coordination.EndpointState, len(snapshot.Endpoints))
	for _, endpoint := range snapshot.Endpoints {
		endpoints[endpoint.Ref] = endpoint.State
	}
	for _, edge := range snapshot.Edges {
		if edge.To != command.Endpoint {
			continue
		}
		switch tasks[edge.From.TaskID] {
		case coordination.TaskCanceled:
			return "source_cancelled"
		case coordination.TaskFailed:
			return "source_failed"
		}
		if endpoints[edge.From] == coordination.EndpointRejected {
			return "source_failed"
		}
	}
	return ""
}

type productionPhaseArtifactRouter struct {
	registry *evidence.PostgresArtifactRegistry
}

func (r productionPhaseArtifactRouter) Route(ctx context.Context, active phasepkg.ActiveInvocation, ref string) (string, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" || strings.ContainsAny(ref, `/\`) || strings.Contains(ref, "..") || !strings.HasPrefix(ref, "art_") {
		return "", kernel.Forbidden("phase output must reference an existing artifact id")
	}
	principal := evidence.Principal{Role: evidence.RolePhaseAgent, ProjectID: active.Invocation.ProjectID, TaskID: active.Invocation.TaskID}
	if _, _, err := r.registry.Open(ctx, principal, evidence.ArtifactID(ref)); err != nil {
		return "", err
	}
	return ref, nil
}

type productionPhaseLifecycle struct {
	workspaces *workspace.Service
	contexts   phasepkg.ContextBindingLifecycle
}

func (l productionPhaseLifecycle) Complete(ctx context.Context, invocation runtimepkg.Invocation) error {
	err := l.finishWorkspace(ctx, invocation, true)
	return errors.Join(err, l.contexts.Complete(ctx, invocation))
}

func (l productionPhaseLifecycle) End(ctx context.Context, invocation runtimepkg.Invocation) error {
	err := l.finishWorkspace(ctx, invocation, false)
	return errors.Join(err, l.contexts.End(ctx, invocation))
}

func (l productionPhaseLifecycle) finishWorkspace(ctx context.Context, invocation runtimepkg.Invocation, complete bool) error {
	if invocation.WorkspaceRef == "" || invocation.ID == "" || !invocation.Role.IsPhase() {
		return nil
	}
	phase := workspacePhase(invocation.EndpointID)
	current, err := l.workspaces.Get(ctx, kernel.BindingRef(invocation.WorkspaceRef))
	if err != nil {
		if kernel.IsCode(err, kernel.CodeNotFound) {
			return nil
		}
		return err
	}
	if current.ActiveInvocation != invocation.ID {
		if complete && current.PhaseLeases[phase] == invocation.ID {
			return nil
		}
		if current.ActiveInvocation == "" {
			return nil
		}
		return kernel.LeaseConflict("workspace lease is held by another invocation")
	}
	if complete {
		_, err = l.workspaces.CompletePhase(ctx, current.ID, phase, invocation.ID, current.Revision)
	} else {
		_, err = l.workspaces.ReleasePhase(ctx, current.ID, phase, invocation.ID, current.Revision)
	}
	return err
}

type productionPhaseRuntime struct {
	controller    *phasepkg.Controller
	db            *sql.DB
	projectID     kernel.ProjectID
	graph         *coordination.PostgresStore
	ingress       *productionIngress
	invocations   runtimepkg.InvocationStore
	workspaces    *workspace.Service
	artifacts     *evidence.PostgresArtifactRegistry
	source        *productionPhaseBindingSource
	recovery      phasepkg.RecoveryStore
	workspaceSync interface {
		SyncWorkspace(context.Context, kernel.InvocationID) (agentteams.ExecutionWorkspaceCheckpoint, error)
	}
	now func() time.Time
}

func (p *productionPhaseRuntime) AwaitInputs(ctx context.Context, invocationID kernel.InvocationID, req phasepkg.AwaitInputsRequest) (phasepkg.InputWaitResult, error) {
	return p.controller.AwaitInputs(ctx, invocationID, req)
}

func (p *productionPhaseRuntime) SubmitPhaseOutput(ctx context.Context, invocationID kernel.InvocationID, output phasepkg.PhaseOutput) (phasepkg.OutputReceipt, error) {
	if err := p.syncOutputWorkspace(ctx, invocationID, output); err != nil {
		return phasepkg.OutputReceipt{}, err
	}
	requestID, err := p.persistOutputIntent(ctx, invocationID, output)
	if err != nil {
		return phasepkg.OutputReceipt{}, err
	}
	receipt, deliver, abandoned, err := p.resolveOutputIntentReceipt(ctx, invocationID, requestID, output)
	if err != nil {
		return phasepkg.OutputReceipt{}, err
	}
	if abandoned {
		return phasepkg.OutputReceipt{}, kernel.IdempotencyConflict()
	}
	if !deliver {
		return phasepkg.OutputReceipt{}, kernel.Error{Code: kernel.CodeStaleCommand, Message: "phase output receipt is not ready", Recoverable: true}
	}
	if err := p.deliverOutputReceipt(ctx, receipt, requestID, output); err != nil {
		return receipt, err
	}
	return receipt, nil
}

func (p *productionPhaseRuntime) syncOutputWorkspace(ctx context.Context, invocationID kernel.InvocationID, output phasepkg.PhaseOutput) error {
	if recovered, ok, err := p.recoveredOutputReceipt(ctx, invocationID); err != nil {
		return err
	} else if ok {
		if !productionPhaseReceiptMatchesOutput(recovered, output) {
			return kernel.IdempotencyConflict()
		}
		return nil
	}
	if p.workspaceSync == nil {
		return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "phase native workspace synchronization is not configured", Recoverable: true}
	}
	checkpoint, err := p.workspaceSync.SyncWorkspace(ctx, invocationID)
	if err != nil {
		return fmt.Errorf("phase native workspace synchronization: %w", err)
	}
	if strings.TrimSpace(checkpoint.WorkspaceRevision) == "" {
		return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "phase native workspace synchronization returned no revision", Recoverable: true}
	}
	return nil
}

func (p *productionPhaseRuntime) persistOutputIntent(ctx context.Context, invocationID kernel.InvocationID, output phasepkg.PhaseOutput) (string, error) {
	payload, err := json.Marshal(productionPhaseOutputIntent{InvocationID: invocationID, Output: output})
	if err != nil {
		return "", err
	}
	requestID := stableProductionSuffix(invocationID, "phase_output", hashProductionBytes(payload))
	if p.invocations == nil {
		return "", kernel.InvalidArgument("phase output intent requires invocation store")
	}
	invocation, err := p.phaseOutputIntentInvocation(ctx, invocationID)
	if err != nil {
		return "", err
	}
	endpoint := coordination.PhaseEndpointRef{TaskID: invocation.TaskID, EndpointID: invocation.EndpointID}
	input := productionInput{
		Kind: "phase_output", RequestID: requestID, ConversationID: "runtime:" + string(endpoint.TaskID),
		Body: "phase output intent " + string(invocationID), Payload: payload, SeenRevision: 1,
		SelectedEndpoint: &endpoint, TargetKind: "phase_output", TargetRef: "",
	}
	return requestID, (productionPhaseTerminalOutbox{db: p.db, projectID: p.projectID, ingress: p.ingress, now: p.now}).enqueueIntent(ctx, productionPhaseTerminalDelivery{
		Input: input, InvocationID: invocationID, Endpoint: endpoint, Generation: int(invocation.Generation), CommandAction: coordination.CommandStart,
	})
}

func (p *productionPhaseRuntime) deliverOutputReceipt(ctx context.Context, receipt phasepkg.OutputReceipt, requestID string, intentOutput phasepkg.PhaseOutput) error {
	outbox := productionPhaseTerminalOutbox{db: p.db, projectID: p.projectID, ingress: p.ingress, now: p.now}
	if exists, err := outbox.finalExists(ctx, "phase_output", requestID); err != nil {
		return err
	} else if exists {
		return nil
	}
	outputRef, err := p.persistOutput(ctx, receipt)
	if err != nil {
		return err
	}
	snapshot, err := p.graph.Latest(ctx, p.projectID)
	if err != nil {
		return err
	}
	payload, _ := json.Marshal(struct {
		OutputRef string                 `json:"output_ref"`
		Receipt   phasepkg.OutputReceipt `json:"receipt"`
	}{outputRef, receipt})
	input := productionInput{
		Kind: "phase_output", RequestID: requestID,
		ConversationID: "runtime:" + string(receipt.Endpoint.TaskID),
		Body:           fmt.Sprintf("phase output %s %s", receipt.Endpoint.EndpointID, outputRef),
		Payload:        payload, SeenRevision: snapshot.Revision, SelectedEndpoint: &receipt.Endpoint,
		TargetKind: "phase_output", TargetRef: outputRef,
	}
	return outbox.promote(ctx, productionPhaseTerminalDelivery{
		Input: input, InvocationID: receipt.InvocationID, CommandID: receipt.CommandID,
		CommandAction: receipt.CommandAction, Endpoint: receipt.Endpoint, Generation: receipt.Generation, IntentOutput: intentOutput,
	})
}

func (p *productionPhaseRuntime) ReplayTerminalDeliveries(ctx context.Context) error {
	outbox := productionPhaseTerminalOutbox{db: p.db, projectID: p.projectID, ingress: p.ingress, now: p.now}
	return errors.Join(
		p.recoverActivePhaseInvocations(ctx),
		p.recoverPersistedOutputReceipts(ctx),
		p.replayOutputIntents(ctx, outbox),
		outbox.replay(ctx),
	)
}

// recoverActivePhaseInvocations reconstructs the Controller's process-local
// active index before replaying output intents. AgentTeams workers and their
// MCP calls can outlive a Threadmill process restart; the persisted invocation,
// command, and recovery obligation therefore remain authoritative even though
// the new Controller has an empty in-memory active map.
func (p *productionPhaseRuntime) recoverActivePhaseInvocations(ctx context.Context) error {
	if p.controller == nil {
		return nil
	}
	abandoned, err := p.unrecoverablePhaseInvocations(ctx)
	if err != nil {
		return err
	}
	var errs []error
	for _, command := range abandoned {
		if err := p.controller.AbandonInvocation(ctx, command); err != nil && !kernel.IsCode(err, kernel.CodeStaleCommand) {
			errs = append(errs, err)
		}
	}
	rows, err := p.db.QueryContext(ctx, `
SELECT c.command_id, c.task_id, c.endpoint_id, c.generation, c.binding_ref,
       c.lease_ref, c.action, COALESCE(c.cause_ref, '')
FROM phase_recovery_obligations o
JOIN coordination_phase_commands c
  ON c.project_id=$1
 AND c.command_id=o.run_command_id
JOIN runtime_invocations r
  ON r.project_id=c.project_id
 AND r.task_id=c.task_id
 AND r.endpoint_id=c.endpoint_id
 AND r.generation=c.generation
 AND r.binding_ref=c.binding_ref
 AND r.lease_id=c.lease_ref
JOIN coordination_phase_leases l
  ON l.project_id=c.project_id
 AND l.lease_ref=c.lease_ref
WHERE o.active=TRUE
  AND o.output_receipt IS NULL
  AND r.status IN ('prepared','running','waiting')
  AND c.accepted_at IS NOT NULL
  AND c.not_executable=FALSE
  AND l.state='active'
  AND l.expires_at > now()
ORDER BY c.command_id`, p.projectID)
	if err != nil {
		return errors.Join(errors.Join(errs...), err)
	}
	var commands []coordination.PhaseCommand
	for rows.Next() {
		var command coordination.PhaseCommand
		if err := rows.Scan(
			&command.ID, &command.Endpoint.TaskID, &command.Endpoint.EndpointID,
			&command.Generation, &command.BindingRef, &command.LeaseRef,
			&command.Action, &command.CauseRef,
		); err != nil {
			rows.Close()
			return err
		}
		commands = append(commands, command)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, command := range commands {
		if err := p.controller.Apply(ctx, command); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// unrecoverablePhaseInvocations returns active Runtime invocations whose
// persisted command can no longer authorize execution. These are cleanup
// obligations, not graph transitions: command rejection or lease expiry is
// already authoritative in the Coordination Graph.
func (p *productionPhaseRuntime) unrecoverablePhaseInvocations(ctx context.Context) ([]coordination.PhaseCommand, error) {
	rows, err := p.db.QueryContext(ctx, `
SELECT c.command_id, c.task_id, c.endpoint_id, c.generation, c.binding_ref,
       c.lease_ref, c.action, COALESCE(c.cause_ref, '')
FROM phase_recovery_obligations o
JOIN coordination_phase_commands c
  ON c.project_id=$1
 AND c.command_id=o.run_command_id
JOIN runtime_invocations r
  ON r.project_id=c.project_id
 AND r.task_id=c.task_id
 AND r.endpoint_id=c.endpoint_id
 AND r.generation=c.generation
 AND r.binding_ref=c.binding_ref
 AND r.lease_id=c.lease_ref
LEFT JOIN coordination_phase_leases l
  ON l.project_id=c.project_id
 AND l.lease_ref=c.lease_ref
WHERE o.active=TRUE
  AND o.output_receipt IS NULL
  AND r.status IN ('prepared','running','waiting')
  AND c.action IN ('start','resume')
  AND (
    c.not_executable=TRUE
    OR l.lease_ref IS NULL
    OR l.state <> 'active'
    OR l.expires_at <= now()
  )
ORDER BY c.command_id`, p.projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	commands := make([]coordination.PhaseCommand, 0)
	for rows.Next() {
		var command coordination.PhaseCommand
		if err := rows.Scan(
			&command.ID, &command.Endpoint.TaskID, &command.Endpoint.EndpointID,
			&command.Generation, &command.BindingRef, &command.LeaseRef,
			&command.Action, &command.CauseRef,
		); err != nil {
			return nil, err
		}
		commands = append(commands, command)
	}
	return commands, rows.Err()
}

type productionPhaseOutputIntent struct {
	InvocationID kernel.InvocationID  `json:"invocation_id"`
	Output       phasepkg.PhaseOutput `json:"output"`
}

func (p *productionPhaseRuntime) recoverPersistedOutputReceipts(ctx context.Context) error {
	if p.controller == nil {
		return nil
	}
	rows, err := p.db.QueryContext(ctx, `
SELECT r.invocation_id, o.output_receipt
FROM phase_recovery_obligations o
JOIN coordination_phase_commands c
  ON c.project_id=$1
 AND c.command_id=o.run_command_id
JOIN runtime_invocations r
  ON r.project_id=c.project_id
 AND r.task_id=c.task_id
 AND r.endpoint_id=c.endpoint_id
 AND r.generation=c.generation
 AND r.binding_ref=c.binding_ref
 AND r.lease_id=c.lease_ref
WHERE o.active=TRUE
  AND o.output_receipt IS NOT NULL
  AND r.status IN ('running','completed')
ORDER BY o.updated_at, o.run_command_id`, p.projectID)
	if err != nil {
		return err
	}
	type persistedOutput struct {
		invocationID kernel.InvocationID
		receipt      phasepkg.OutputReceipt
	}
	var pending []persistedOutput
	for rows.Next() {
		var invocationID kernel.InvocationID
		var raw []byte
		if err := rows.Scan(&invocationID, &raw); err != nil {
			rows.Close()
			return err
		}
		var receipt phasepkg.OutputReceipt
		if err := json.Unmarshal(raw, &receipt); err != nil {
			rows.Close()
			return err
		}
		if receipt.InvocationID != invocationID {
			rows.Close()
			return kernel.Error{Code: kernel.CodeInternalError, Message: "persisted phase output receipt invocation identity changed", Recoverable: true}
		}
		pending = append(pending, persistedOutput{invocationID: invocationID, receipt: receipt})
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}
	var errs []error
	for _, item := range pending {
		if _, err := p.controller.SubmitPhaseOutput(ctx, item.invocationID, item.receipt.Output); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (p *productionPhaseRuntime) replayOutputIntents(ctx context.Context, outbox productionPhaseTerminalOutbox) error {
	rows, err := p.db.QueryContext(ctx, `SELECT payload, request_id FROM production_phase_terminal_obligations WHERE project_id=$1 AND input_kind='phase_output' AND status='intent' ORDER BY updated_at, obligation_id`, p.projectID)
	if err != nil {
		return err
	}
	defer rows.Close()
	var errs []error
	for rows.Next() {
		var payload []byte
		var requestID string
		if err := rows.Scan(&payload, &requestID); err != nil {
			return err
		}
		var intent productionPhaseOutputIntent
		if err := json.Unmarshal(payload, &intent); err != nil {
			errs = append(errs, err)
			continue
		}
		invocation, err := p.phaseOutputIntentInvocation(ctx, intent.InvocationID)
		if err != nil {
			if kernel.IsCode(err, kernel.CodeForbidden) || kernel.IsCode(err, kernel.CodeStaleCommand) {
				if abandonErr := outbox.abandonIntent(ctx, "phase_output", requestID, err); abandonErr != nil {
					errs = append(errs, abandonErr)
				}
				continue
			}
			errs = append(errs, err)
			continue
		}
		if invocation.Status == runtimepkg.InvocationFailed || invocation.Status == runtimepkg.InvocationStopped {
			cause := kernel.Error{Code: kernel.CodeStaleCommand, Message: "phase output intent arrived after invocation termination", Recoverable: false}
			if abandonErr := outbox.abandonIntent(ctx, "phase_output", requestID, cause); abandonErr != nil {
				errs = append(errs, abandonErr)
			}
			continue
		}
		receipt, deliver, abandoned, err := p.resolveOutputIntentReceipt(ctx, intent.InvocationID, requestID, intent.Output)
		if abandoned {
			continue
		}
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if !deliver {
			continue
		}
		if err := p.deliverOutputReceipt(ctx, receipt, requestID, intent.Output); err != nil {
			errs = append(errs, err)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return errors.Join(errs...)
}

func (p *productionPhaseRuntime) phaseOutputIntentInvocation(ctx context.Context, invocationID kernel.InvocationID) (runtimepkg.Invocation, error) {
	if p.invocations == nil {
		return runtimepkg.Invocation{}, kernel.InvalidArgument("phase output intent requires invocation store")
	}
	invocation, ok, err := p.invocations.Get(ctx, invocationID)
	if err != nil {
		return runtimepkg.Invocation{}, err
	}
	if !ok {
		return runtimepkg.Invocation{}, kernel.Error{Code: kernel.CodeStaleCommand, Message: "phase output intent requires an existing invocation", Recoverable: true}
	}
	if invocation.ProjectID != p.projectID {
		return runtimepkg.Invocation{}, kernel.Forbidden("phase output invocation belongs to another project")
	}
	if !invocation.Role.IsPhase() {
		return runtimepkg.Invocation{}, kernel.Forbidden("phase output intent requires a phase invocation")
	}
	return invocation, nil
}

func (p *productionPhaseRuntime) resolveOutputIntentReceipt(ctx context.Context, invocationID kernel.InvocationID, requestID string, output phasepkg.PhaseOutput) (phasepkg.OutputReceipt, bool, bool, error) {
	if p.controller == nil {
		recovered, ok, err := p.recoveredOutputReceipt(ctx, invocationID)
		if err != nil {
			return phasepkg.OutputReceipt{}, false, false, err
		}
		if ok && productionPhaseReceiptMatchesOutput(recovered, output) {
			return recovered, true, false, nil
		}
		return phasepkg.OutputReceipt{}, false, false, kernel.Error{Code: kernel.CodeStaleCommand, Message: "phase output controller is not available and no matching receipt was recovered", Recoverable: true}
	}
	receipt, err := p.controller.SubmitPhaseOutput(ctx, invocationID, output)
	if err == nil {
		return receipt, true, false, nil
	}
	if productionPhaseTerminalError(err) {
		if abandonErr := p.abandonOutputIntent(ctx, invocationID, requestID, output, err); abandonErr != nil {
			return phasepkg.OutputReceipt{}, false, false, abandonErr
		}
		return phasepkg.OutputReceipt{}, false, true, err
	}
	recovered, ok, recoverErr := p.recoveredOutputReceipt(ctx, invocationID)
	if recoverErr != nil {
		return phasepkg.OutputReceipt{}, false, false, recoverErr
	}
	if ok && productionPhaseReceiptMatchesOutput(recovered, output) {
		return recovered, true, false, nil
	}
	// The MCP request context may be cancelled by the provider as soon as its
	// streaming turn closes. Preserve the exact internal stage on a fresh,
	// bounded context so operators and the GUI can distinguish binding,
	// artifact, receipt and lifecycle failures without exposing those details
	// through the Agent-facing transport error.
	if recordErr := p.recordOutputIntentFailure(ctx, requestID, err); recordErr != nil {
		return phasepkg.OutputReceipt{}, false, false, errors.Join(err, recordErr)
	}
	return phasepkg.OutputReceipt{}, false, false, err
}

func (p *productionPhaseRuntime) recordOutputIntentFailure(ctx context.Context, requestID string, cause error) error {
	if p.db == nil || requestID == "" || cause == nil {
		return nil
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_, err := p.db.ExecContext(cleanupCtx, `
UPDATE production_phase_terminal_obligations
SET attempts=attempts+1, last_error=$4, updated_at=$5
WHERE project_id=$1 AND input_kind=$2 AND request_id=$3 AND status='intent'`,
		p.projectID, "phase_output", requestID, cause.Error(), p.now().UTC())
	return err
}

func (p *productionPhaseRuntime) abandonOutputIntent(ctx context.Context, invocationID kernel.InvocationID, requestID string, output phasepkg.PhaseOutput, cause error) error {
	if receipt, ok, err := p.recoveredOutputReceipt(ctx, invocationID); err != nil {
		return err
	} else if ok && productionPhaseReceiptMatchesOutput(receipt, output) {
		return nil
	}
	return (productionPhaseTerminalOutbox{db: p.db, projectID: p.projectID, ingress: p.ingress, now: p.now}).abandonIntent(ctx, "phase_output", requestID, cause)
}

func productionPhaseReceiptMatchesOutput(receipt phasepkg.OutputReceipt, output phasepkg.PhaseOutput) bool {
	outputRaw, err := json.Marshal(output)
	if err != nil {
		return false
	}
	if receipt.OutputFingerprint != "" {
		return receipt.OutputFingerprint == hashProductionBytes(outputRaw)
	}
	receiptOutput, err := json.Marshal(receipt.Output)
	if err != nil {
		return false
	}
	return string(receiptOutput) == string(outputRaw)
}

func productionPhaseTerminalError(err error) bool {
	var kernelErr kernel.Error
	if errors.As(err, &kernelErr) {
		return !kernelErr.Recoverable
	}
	var kernelErrPtr *kernel.Error
	if errors.As(err, &kernelErrPtr) && kernelErrPtr != nil {
		return !kernelErrPtr.Recoverable
	}
	return false
}

func (p *productionPhaseRuntime) recoveredOutputReceipt(ctx context.Context, invocationID kernel.InvocationID) (phasepkg.OutputReceipt, bool, error) {
	if p.recovery == nil {
		return phasepkg.OutputReceipt{}, false, nil
	}
	return p.recovery.GetOutputReceipt(ctx, invocationID, "")
}

func (p *productionPhaseRuntime) SubmitOrchestrationIntent(ctx context.Context, principal auth.Principal, scope auth.BoundScope, intent phasepkg.OrchestrationIntent) (phasepkg.OrchestrationProposal, error) {
	if err := phasepkg.ValidateOrchestrationIntent(intent); err != nil {
		return phasepkg.OrchestrationProposal{}, err
	}
	if principal.ProjectID != p.projectID || scope.ProjectID != p.projectID || principal.InvocationID == "" || scope.InvocationID != principal.InvocationID || !principal.Role.IsPhase() {
		return phasepkg.OrchestrationProposal{}, kernel.Forbidden("phase orchestration scope mismatch")
	}
	invocation, ok, err := p.invocations.Get(ctx, principal.InvocationID)
	if err != nil {
		return phasepkg.OrchestrationProposal{}, err
	}
	if !ok || invocation.Status != runtimepkg.InvocationRunning ||
		invocation.Role != principal.Role || invocation.TaskID != principal.TaskID || invocation.ID != principal.InvocationID {
		return phasepkg.OrchestrationProposal{}, kernel.Forbidden("phase orchestration requires an active invocation")
	}
	snapshot, err := p.graph.Latest(ctx, p.projectID)
	if err != nil {
		return phasepkg.OrchestrationProposal{}, err
	}
	workspaceRevision := invocation.WorkspaceRef
	if invocation.WorkspaceRef != "" {
		if binding, err := p.workspaces.Get(ctx, kernel.BindingRef(invocation.WorkspaceRef)); err == nil {
			workspaceRevision = binding.CurrentRevision
		}
	}
	command := coordination.PhaseCommand{
		Endpoint:   coordination.PhaseEndpointRef{TaskID: invocation.TaskID, EndpointID: invocation.EndpointID},
		Generation: int(invocation.Generation),
		BindingRef: invocation.BindingRef,
		LeaseRef:   invocation.LeaseID,
		Action:     coordination.CommandStart,
	}
	inputs, err := p.source.inputs(ctx, snapshot, command)
	if err != nil {
		return phasepkg.OrchestrationProposal{}, err
	}
	active := phasepkg.ActiveInvocation{Invocation: invocation, Command: command, Inputs: inputs}
	for i, ref := range intent.EvidenceRefs {
		routed, err := (productionPhaseArtifactRouter{registry: p.artifacts}).Route(ctx, active, ref)
		if err != nil {
			return phasepkg.OrchestrationProposal{}, err
		}
		intent.EvidenceRefs[i] = routed
	}
	proposal := phasepkg.OrchestrationProposal{
		ProposalID:       stableRuntimeRef("phase-orchestration", principal.InvocationID, snapshot.Revision, intent.OrchestrationAdvice),
		ClientRef:        stableRuntimeRef("phase-orchestration-client", principal.InvocationID, intent.Rationale),
		FromEndpoint:     coordination.PhaseEndpointRef{TaskID: invocation.TaskID, EndpointID: invocation.EndpointID},
		FromInvocationID: principal.InvocationID, BasedOnGraphRevision: snapshot.Revision,
		BasedOnWorkspaceRevision: workspaceRevision, BasedOnInputRevision: inputs.InputRevision, OrchestrationAdvice: intent.OrchestrationAdvice,
		DeliverySpecAdvice: intent.DeliverySpecAdvice, ReportSpecAdvice: intent.ReportSpecAdvice,
		Rationale: intent.Rationale, EvidenceRefs: append([]string(nil), intent.EvidenceRefs...),
	}
	payload, _ := json.Marshal(proposal)
	_, err = p.ingress.persistAndDispatch(ctx, productionInput{
		Kind: "phase_orchestration", RequestID: proposal.ProposalID,
		ConversationID: "runtime:" + string(invocation.TaskID), Body: proposal.Rationale, Payload: payload,
		SeenRevision: snapshot.Revision, SelectedEndpoint: &proposal.FromEndpoint,
		TargetKind: "phase_orchestration", TargetRef: proposal.ProposalID,
	})
	return proposal, err
}

func (p *productionPhaseRuntime) persistOutput(ctx context.Context, receipt phasepkg.OutputReceipt) (string, error) {
	outputRaw, _ := json.Marshal(receipt.Output)
	var existingRef string
	if err := p.db.QueryRowContext(ctx, `SELECT output_ref FROM production_phase_outputs WHERE project_id=$1 AND invocation_id=$2 AND output=$3::jsonb`, p.projectID, receipt.InvocationID, string(outputRaw)).Scan(&existingRef); err == nil {
		return existingRef, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	var existingCount int
	if err := p.db.QueryRowContext(ctx, `SELECT count(*) FROM production_phase_outputs WHERE project_id=$1 AND invocation_id=$2`, p.projectID, receipt.InvocationID).Scan(&existingCount); err != nil {
		return "", err
	}
	if existingCount > 0 {
		return "", kernel.IdempotencyConflict()
	}
	payload, err := json.Marshal(receipt)
	if err != nil {
		return "", err
	}
	artifact, err := p.artifacts.Register(ctx, evidence.RegisterArtifact{
		Type: evidence.ArtifactGeneratedReport, ProjectID: p.projectID, TaskID: receipt.Endpoint.TaskID,
		AgentInvocationID: receipt.InvocationID, ContentType: "application/json", Body: payload,
	})
	if err != nil {
		return "", err
	}
	refs := append([]string(nil), receipt.Output.DeliveryRefs...)
	refs = append(refs, receipt.Output.ReportRef)
	refs = append(refs, receipt.Output.EvidenceRefs...)
	refs = append(refs, string(artifact.ID))
	sort.Strings(refs)
	refsRaw, _ := json.Marshal(refs)
	result, err := p.db.ExecContext(ctx, `
INSERT INTO production_phase_outputs (
  output_ref, project_id, task_id, endpoint_id, generation, binding_ref, lease_ref,
  invocation_id, input_revision, output, artifact_refs
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11::jsonb)
ON CONFLICT (output_ref) DO NOTHING`,
		string(artifact.ID), p.projectID, receipt.Endpoint.TaskID, receipt.Endpoint.EndpointID, receipt.Generation,
		receipt.BindingRef, receipt.LeaseRef, receipt.InvocationID, receipt.InputRevision, string(outputRaw), string(refsRaw))
	if err != nil {
		return "", err
	}
	if affected, _ := result.RowsAffected(); affected == 0 {
		var existing string
		if err := p.db.QueryRowContext(ctx, `SELECT output::text FROM production_phase_outputs WHERE output_ref=$1`, string(artifact.ID)).Scan(&existing); err != nil {
			return "", err
		}
		if existing != string(outputRaw) {
			return "", kernel.IdempotencyConflict()
		}
	}
	return string(artifact.ID), nil
}

type productionPhaseObservationWriter struct {
	projectID kernel.ProjectID
	graph     coordination.PhaseObservationWriter
	ingress   *productionIngress
	now       func() time.Time
}

func (w productionPhaseObservationWriter) RecordPhaseInvocationStarted(ctx context.Context, projectID kernel.ProjectID, command coordination.PhaseCommand) error {
	return w.graph.RecordPhaseInvocationStarted(ctx, projectID, command)
}

func (w productionPhaseObservationWriter) RecordPhaseOutputSubmitted(ctx context.Context, projectID kernel.ProjectID, command coordination.PhaseCommand) error {
	return w.graph.RecordPhaseOutputSubmitted(ctx, projectID, command)
}

func (w productionPhaseObservationWriter) RecordPhaseInvocationFailed(ctx context.Context, projectID kernel.ProjectID, command coordination.PhaseCommand) error {
	snapshot, err := w.ingress.graph.Latest(ctx, projectID)
	if err != nil {
		return err
	}
	payload, _ := json.Marshal(productionPhaseFailedBoundary{
		CommandID: command.ID, CommandAction: command.Action, Endpoint: command.Endpoint,
		Generation: command.Generation, BindingRef: command.BindingRef, LeaseRef: command.LeaseRef,
	})
	input := productionInput{
		Kind: "phase_failed", RequestID: stableProductionSuffix(command.ID, "failed"),
		ConversationID: "runtime:" + string(command.Endpoint.TaskID),
		Body:           fmt.Sprintf("phase failed %s/%s", command.Endpoint.TaskID, command.Endpoint.EndpointID),
		Payload:        payload, SeenRevision: snapshot.Revision, SelectedEndpoint: &command.Endpoint,
		TargetKind: "phase_failed", TargetRef: command.ID,
	}
	outbox := productionPhaseTerminalOutbox{db: w.ingress.db, projectID: projectID, ingress: w.ingress, now: w.now}
	if _, err := outbox.enqueue(ctx, productionPhaseTerminalDelivery{
		Input: input, InvocationID: deterministicPhaseInvocationID(command.ID), CommandID: command.ID,
		CommandAction: command.Action, Endpoint: command.Endpoint, Generation: command.Generation,
	}); err != nil {
		return err
	}
	if err := w.graph.RecordPhaseInvocationFailed(ctx, projectID, command); err != nil {
		return err
	}
	obligationID := stableRuntimeRef("phase-terminal", projectID, input.Kind, input.RequestID)
	return outbox.deliver(ctx, obligationID)
}

func (w productionPhaseObservationWriter) RecordPhaseInvocationStopped(ctx context.Context, projectID kernel.ProjectID, command coordination.PhaseCommand, checkpointRef string, nonResumable bool) error {
	snapshot, err := w.ingress.graph.Latest(ctx, projectID)
	if err != nil {
		return err
	}
	// Only an operator/Task Manager hold creates a graph-control obligation.
	// Runtime-driven stops (for example an expired execution lease) merely end
	// the current invocation; the still-enabled endpoint remains eligible for a
	// fresh start and must not be sent through the held-only "stopped" graph
	// transition.
	held := false
	for _, endpoint := range snapshot.Endpoints {
		if endpoint.Ref == command.Endpoint &&
			endpoint.Generation == command.Generation &&
			endpoint.BindingRef == command.BindingRef {
			held = endpoint.RunPolicy == coordination.RunHeld
			break
		}
	}
	if !held {
		return w.graph.RecordPhaseInvocationStopped(ctx, projectID, command, checkpointRef, nonResumable)
	}
	payload, _ := json.Marshal(struct {
		CommandID     string                        `json:"command_id"`
		Endpoint      coordination.PhaseEndpointRef `json:"endpoint"`
		Generation    int                           `json:"generation"`
		BindingRef    kernel.BindingRef             `json:"binding_ref"`
		LeaseRef      kernel.LeaseID                `json:"lease_ref"`
		CheckpointRef string                        `json:"checkpoint_ref"`
		NonResumable  bool                          `json:"non_resumable"`
	}{command.ID, command.Endpoint, command.Generation, command.BindingRef, command.LeaseRef, checkpointRef, nonResumable})
	input := productionInput{
		Kind: "phase_stopped", RequestID: stableProductionSuffix(command.ID, checkpointRef, nonResumable),
		ConversationID: "runtime:" + string(command.Endpoint.TaskID),
		Body:           fmt.Sprintf("phase stopped %s/%s", command.Endpoint.TaskID, command.Endpoint.EndpointID),
		Payload:        payload, SeenRevision: snapshot.Revision, SelectedEndpoint: &command.Endpoint,
		TargetKind: "phase_stopped", TargetRef: command.ID,
	}
	outbox := productionPhaseTerminalOutbox{db: w.ingress.db, projectID: projectID, ingress: w.ingress, now: w.now}
	if _, err := outbox.enqueue(ctx, productionPhaseTerminalDelivery{
		Input: input, InvocationID: deterministicPhaseInvocationID(command.ID), CommandID: command.ID,
		CommandAction: command.Action, Endpoint: command.Endpoint, Generation: command.Generation,
	}); err != nil {
		return err
	}
	if err := w.graph.RecordPhaseInvocationStopped(ctx, projectID, command, checkpointRef, nonResumable); err != nil {
		return err
	}
	obligationID := stableRuntimeRef("phase-terminal", projectID, input.Kind, input.RequestID)
	return outbox.deliver(ctx, obligationID)
}

type productionPhaseTerminalDelivery struct {
	Input         productionInput
	InvocationID  kernel.InvocationID
	CommandID     string
	CommandAction coordination.CommandAction
	Endpoint      coordination.PhaseEndpointRef
	Generation    int
	IntentOutput  phasepkg.PhaseOutput
}

type productionPhaseTerminalOutbox struct {
	db        *sql.DB
	projectID kernel.ProjectID
	ingress   *productionIngress
	now       func() time.Time
}

func (o productionPhaseTerminalOutbox) enqueueAndDeliver(ctx context.Context, delivery productionPhaseTerminalDelivery) error {
	obligationID, err := o.enqueue(ctx, delivery)
	if err != nil {
		return err
	}
	return o.deliver(ctx, obligationID)
}

func (o productionPhaseTerminalOutbox) promoteAndDeliver(ctx context.Context, delivery productionPhaseTerminalDelivery) error {
	obligationID, err := o.promoteFinal(ctx, delivery)
	if err != nil {
		return err
	}
	return o.deliver(ctx, obligationID)
}

func (o productionPhaseTerminalOutbox) promote(ctx context.Context, delivery productionPhaseTerminalDelivery) error {
	_, err := o.promoteFinal(ctx, delivery)
	return err
}

func (o productionPhaseTerminalOutbox) promoteFinal(ctx context.Context, delivery productionPhaseTerminalDelivery) (string, error) {
	obligationID := stableRuntimeRef("phase-terminal", o.projectID, delivery.Input.Kind, delivery.Input.RequestID)
	payloadHash := hashProductionBytes(delivery.Input.Payload)
	identityHash, err := o.identityHash(delivery, payloadHash)
	if err != nil {
		return "", err
	}
	intentPayloadHash, intentIdentityHash, intentFound, err := o.intentHashesForReceiptDelivery(ctx, obligationID, delivery)
	if err != nil {
		return "", err
	}
	now := o.timestamp()
	affected := int64(0)
	if intentFound {
		result, err := o.db.ExecContext(ctx, `
UPDATE production_phase_terminal_obligations
SET invocation_id=$2, command_id=$3, command_action=$4, task_id=$5, endpoint_id=$6,
    generation=$7, conversation_id=$8, body=$9, payload=$10::jsonb, payload_hash=$11,
    identity_hash=$12, seen_revision=$13, target_kind=$14, target_ref=$15,
    dispatch_request_id=request_id, status='pending', last_error='', updated_at=$16
WHERE obligation_id=$1 AND project_id=$17 AND status='intent'
  AND intent_payload_hash=$18 AND intent_identity_hash=$19`,
			obligationID, delivery.InvocationID, delivery.CommandID, delivery.CommandAction,
			delivery.Endpoint.TaskID, delivery.Endpoint.EndpointID, delivery.Generation, delivery.Input.ConversationID,
			delivery.Input.Body, string(delivery.Input.Payload), payloadHash, identityHash, delivery.Input.SeenRevision,
			delivery.Input.TargetKind, delivery.Input.TargetRef, now, o.projectID, intentPayloadHash, intentIdentityHash)
		if err != nil {
			return "", err
		}
		affected, _ = result.RowsAffected()
	}
	if affected == 0 {
		if exists, err := o.finalExistsByID(ctx, obligationID); err != nil {
			return "", err
		} else if exists {
			return obligationID, nil
		}
		if _, err := o.enqueue(ctx, delivery); err != nil {
			return "", err
		}
	}
	return obligationID, nil
}

func (o productionPhaseTerminalOutbox) abandonIntent(ctx context.Context, kind, requestID string, cause error) error {
	if o.db == nil {
		return kernel.InvalidArgument("production phase terminal outbox database is required")
	}
	now := o.timestamp()
	_, err := o.db.ExecContext(ctx, `
UPDATE production_phase_terminal_obligations
SET status='abandoned', attempts=attempts+1, last_error=$4, updated_at=$5
WHERE project_id=$1 AND input_kind=$2 AND request_id=$3 AND status='intent'`,
		o.projectID, kind, requestID, cause.Error(), now)
	return err
}

func (o productionPhaseTerminalOutbox) enqueueIntent(ctx context.Context, delivery productionPhaseTerminalDelivery) error {
	if o.db == nil {
		return kernel.InvalidArgument("production phase terminal outbox database is required")
	}
	if delivery.Input.Kind != "phase_output" || delivery.InvocationID == "" || len(delivery.Input.Payload) == 0 || delivery.Input.RequestID == "" {
		return kernel.InvalidArgument("production phase output intent identity is required")
	}
	obligationID := stableRuntimeRef("phase-terminal", o.projectID, delivery.Input.Kind, delivery.Input.RequestID)
	payloadHash := hashProductionBytes(delivery.Input.Payload)
	identityHash, err := o.intentIdentityHash(delivery, payloadHash)
	if err != nil {
		return err
	}
	now := o.timestamp()
	_, err = o.db.ExecContext(ctx, `
INSERT INTO production_phase_terminal_obligations (
  obligation_id, project_id, invocation_id, command_id, command_action, task_id, endpoint_id,
  generation, input_kind, request_id, dispatch_request_id, conversation_id, body, payload, payload_hash,
  identity_hash, intent_payload_hash, intent_identity_hash, seen_revision, target_kind, target_ref, status, created_at, updated_at
) VALUES ($1,$2,$3,'',$4,$5,$6,$7,$8,$9,$9,$10,$11,$12::jsonb,$13,$14,$13,$14,$15,$16,'','intent',$17,$17)
ON CONFLICT (obligation_id) DO NOTHING`,
		obligationID, o.projectID, delivery.InvocationID, delivery.CommandAction, delivery.Endpoint.TaskID,
		delivery.Endpoint.EndpointID, positiveGeneration(delivery.Generation), delivery.Input.Kind, delivery.Input.RequestID,
		delivery.Input.ConversationID, delivery.Input.Body, string(delivery.Input.Payload), payloadHash,
		identityHash, positiveRevision(delivery.Input.SeenRevision), delivery.Input.TargetKind, now)
	if err != nil {
		return err
	}
	return o.checkExisting(ctx, obligationID, payloadHash, identityHash, true)
}

func (o productionPhaseTerminalOutbox) enqueue(ctx context.Context, delivery productionPhaseTerminalDelivery) (string, error) {
	if o.now == nil {
		o.now = time.Now
	}
	if o.db == nil || o.ingress == nil {
		return "", kernel.InvalidArgument("production phase terminal outbox dependencies are required")
	}
	if delivery.Input.SelectedEndpoint == nil || len(delivery.Input.Payload) == 0 || delivery.Input.RequestID == "" || delivery.Input.TargetKind == "" || delivery.Input.TargetRef == "" {
		return "", kernel.InvalidArgument("production phase terminal delivery identity is required")
	}
	obligationID := stableRuntimeRef("phase-terminal", o.projectID, delivery.Input.Kind, delivery.Input.RequestID)
	payloadHash := hashProductionBytes(delivery.Input.Payload)
	identityHash, err := o.identityHash(delivery, payloadHash)
	if err != nil {
		return "", err
	}
	now := o.timestamp()
	_, err = o.db.ExecContext(ctx, `
INSERT INTO production_phase_terminal_obligations (
  obligation_id, project_id, invocation_id, command_id, command_action, task_id, endpoint_id,
  generation, input_kind, request_id, dispatch_request_id, conversation_id, body, payload, payload_hash,
  identity_hash, intent_payload_hash, intent_identity_hash, seen_revision, target_kind, target_ref, created_at, updated_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$10,$11,$12,$13::jsonb,$14,$15,'','',$16,$17,$18,$19,$19)
ON CONFLICT (obligation_id) DO NOTHING`,
		obligationID, o.projectID, delivery.InvocationID, delivery.CommandID, delivery.CommandAction,
		delivery.Endpoint.TaskID, delivery.Endpoint.EndpointID, delivery.Generation, delivery.Input.Kind,
		delivery.Input.RequestID, delivery.Input.ConversationID, delivery.Input.Body, string(delivery.Input.Payload),
		payloadHash, identityHash, delivery.Input.SeenRevision, delivery.Input.TargetKind, delivery.Input.TargetRef, now)
	if err != nil {
		return "", err
	}
	if err := o.checkExisting(ctx, obligationID, payloadHash, identityHash, false); err != nil {
		return "", err
	}
	return obligationID, nil
}

func (o productionPhaseTerminalOutbox) checkExisting(ctx context.Context, obligationID, payloadHash, identityHash string, allowIntent bool) error {
	var storedPayloadHash, storedIdentityHash, storedIntentPayloadHash, storedIntentIdentityHash, status string
	if err := o.db.QueryRowContext(ctx, `SELECT payload_hash, identity_hash, intent_payload_hash, intent_identity_hash, status FROM production_phase_terminal_obligations WHERE obligation_id=$1`, obligationID).Scan(&storedPayloadHash, &storedIdentityHash, &storedIntentPayloadHash, &storedIntentIdentityHash, &status); err != nil {
		return err
	}
	if allowIntent {
		if storedIntentPayloadHash == payloadHash && storedIntentIdentityHash == identityHash {
			return nil
		}
		if status == "intent" && storedPayloadHash == payloadHash && storedIdentityHash == identityHash {
			return nil
		}
	}
	if storedPayloadHash != payloadHash || storedIdentityHash != identityHash {
		return kernel.IdempotencyConflict()
	}
	return nil
}

func (o productionPhaseTerminalOutbox) deliver(ctx context.Context, obligationID string) error {
	delivery, status, err := o.load(ctx, obligationID)
	if err != nil {
		return err
	}
	if status == "delivered" {
		return nil
	}
	stored, err := o.ingress.persistAndDispatch(ctx, delivery.Input)
	if err != nil {
		rotated, rotateErr := o.rotateFailedDispatch(ctx, obligationID, delivery.Input.RequestID, err)
		if rotateErr != nil {
			_ = o.recordFailure(ctx, obligationID, errors.Join(err, rotateErr))
			return errors.Join(err, rotateErr)
		}
		if rotated {
			return nil
		}
		_ = o.recordFailure(ctx, obligationID, err)
		return err
	}
	return o.markDelivered(ctx, obligationID, stored.InputRef)
}

func (o productionPhaseTerminalOutbox) deliverExistingFinal(ctx context.Context, kind, requestID string) (bool, error) {
	obligationID := stableRuntimeRef("phase-terminal", o.projectID, kind, requestID)
	return o.deliverExistingFinalByID(ctx, obligationID)
}

func (o productionPhaseTerminalOutbox) deliverExistingFinalByID(ctx context.Context, obligationID string) (bool, error) {
	exists, err := o.finalExistsByID(ctx, obligationID)
	if !exists || err != nil {
		return exists, err
	}
	return true, o.deliver(ctx, obligationID)
}

func (o productionPhaseTerminalOutbox) finalExists(ctx context.Context, kind, requestID string) (bool, error) {
	obligationID := stableRuntimeRef("phase-terminal", o.projectID, kind, requestID)
	return o.finalExistsByID(ctx, obligationID)
}

func (o productionPhaseTerminalOutbox) finalExistsByID(ctx context.Context, obligationID string) (bool, error) {
	var status string
	if err := o.db.QueryRowContext(ctx, `SELECT status FROM production_phase_terminal_obligations WHERE obligation_id=$1 AND project_id=$2`, obligationID, o.projectID).Scan(&status); errors.Is(err, sql.ErrNoRows) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	if status != "pending" && status != "delivered" {
		return false, nil
	}
	return true, nil
}

func (o productionPhaseTerminalOutbox) replay(ctx context.Context) error {
	rows, err := o.db.QueryContext(ctx, `SELECT obligation_id FROM production_phase_terminal_obligations WHERE project_id=$1 AND status='pending' ORDER BY updated_at, obligation_id`, o.projectID)
	if err != nil {
		return err
	}
	defer rows.Close()
	var errs []error
	for rows.Next() {
		var obligationID string
		if err := rows.Scan(&obligationID); err != nil {
			return err
		}
		if err := o.deliver(ctx, obligationID); err != nil {
			errs = append(errs, err)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return errors.Join(errs...)
}

func (o productionPhaseTerminalOutbox) load(ctx context.Context, obligationID string) (productionPhaseTerminalDelivery, string, error) {
	var delivery productionPhaseTerminalDelivery
	var endpointID coordination.EndpointID
	var payload []byte
	var status string
	err := o.db.QueryRowContext(ctx, `
SELECT invocation_id, command_id, command_action, task_id, endpoint_id, generation,
       input_kind, dispatch_request_id, conversation_id, body, payload, seen_revision, target_kind, target_ref, status
FROM production_phase_terminal_obligations
WHERE obligation_id=$1 AND project_id=$2`, obligationID, o.projectID).Scan(
		&delivery.InvocationID, &delivery.CommandID, &delivery.CommandAction, &delivery.Endpoint.TaskID, &endpointID,
		&delivery.Generation, &delivery.Input.Kind, &delivery.Input.RequestID, &delivery.Input.ConversationID,
		&delivery.Input.Body, &payload, &delivery.Input.SeenRevision, &delivery.Input.TargetKind, &delivery.Input.TargetRef, &status)
	if err != nil {
		return productionPhaseTerminalDelivery{}, "", err
	}
	delivery.Endpoint.EndpointID = endpointID
	delivery.Input.Payload = payload
	delivery.Input.SelectedEndpoint = &delivery.Endpoint
	return delivery, status, nil
}

func (o productionPhaseTerminalOutbox) rotateFailedDispatch(ctx context.Context, obligationID, dispatchRequestID string, cause error) (bool, error) {
	tx, err := o.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var logicalRequestID, currentDispatchRequestID, inputKind string
	var attempts int
	err = tx.QueryRowContext(ctx, `
SELECT request_id, dispatch_request_id, input_kind, attempts
FROM production_phase_terminal_obligations
WHERE obligation_id=$1 AND project_id=$2 AND status='pending'
FOR UPDATE`, obligationID, o.projectID).Scan(&logicalRequestID, &currentDispatchRequestID, &inputKind, &attempts)
	if errors.Is(err, sql.ErrNoRows) {
		return false, tx.Commit()
	}
	if err != nil {
		return false, err
	}
	if currentDispatchRequestID != dispatchRequestID {
		return true, tx.Commit()
	}
	var inputStatus string
	err = tx.QueryRowContext(ctx, `
SELECT status
FROM production_manager_inputs
WHERE project_id=$1 AND input_kind=$2 AND request_id=$3`, o.projectID, inputKind, currentDispatchRequestID).Scan(&inputStatus)
	if errors.Is(err, sql.ErrNoRows) {
		return false, tx.Commit()
	}
	if err != nil {
		return false, err
	}
	if inputStatus != "failed" {
		return false, tx.Commit()
	}
	nextDispatchRequestID := stableProductionSuffix(logicalRequestID, "dispatch-retry", attempts+1)
	result, err := tx.ExecContext(ctx, `
UPDATE production_phase_terminal_obligations
SET dispatch_request_id=$2, attempts=attempts+1, last_error=$3, updated_at=$4
WHERE obligation_id=$1 AND project_id=$5 AND status='pending' AND dispatch_request_id=$6`,
		obligationID, nextDispatchRequestID, cause.Error(), o.timestamp(), o.projectID, currentDispatchRequestID)
	if err != nil {
		return false, err
	}
	if affected, err := result.RowsAffected(); err != nil {
		return false, err
	} else if affected != 1 {
		return false, kernel.Error{Code: kernel.CodeRevisionConflict, Message: "phase terminal dispatch retry changed concurrently", Recoverable: true}
	}
	return true, tx.Commit()
}

func (o productionPhaseTerminalOutbox) recordFailure(ctx context.Context, obligationID string, cause error) error {
	now := o.timestamp()
	_, err := o.db.ExecContext(ctx, `UPDATE production_phase_terminal_obligations SET attempts=attempts+1, last_error=$2, updated_at=$3 WHERE obligation_id=$1`, obligationID, cause.Error(), now)
	return err
}

func (o productionPhaseTerminalOutbox) markDelivered(ctx context.Context, obligationID, inputRef string) error {
	now := o.timestamp()
	_, err := o.db.ExecContext(ctx, `UPDATE production_phase_terminal_obligations SET status='delivered', manager_input_ref=$2, last_error='', updated_at=$3 WHERE obligation_id=$1`, obligationID, inputRef, now)
	return err
}

func (o productionPhaseTerminalOutbox) timestamp() time.Time {
	if o.now != nil {
		return o.now().UTC()
	}
	return time.Now().UTC()
}

func positiveGeneration(generation int) int {
	if generation > 0 {
		return generation
	}
	return 1
}

func positiveRevision(revision kernel.Revision) kernel.Revision {
	if revision > 0 {
		return revision
	}
	return 1
}

func (o productionPhaseTerminalOutbox) identityHash(delivery productionPhaseTerminalDelivery, payloadHash string) (string, error) {
	raw, err := stableProductionJSON(map[string]any{
		"project_id":      o.projectID,
		"invocation_id":   delivery.InvocationID,
		"command_id":      delivery.CommandID,
		"command_action":  delivery.CommandAction,
		"endpoint":        delivery.Endpoint,
		"generation":      delivery.Generation,
		"input_kind":      delivery.Input.Kind,
		"request_id":      delivery.Input.RequestID,
		"conversation_id": delivery.Input.ConversationID,
		"body":            delivery.Input.Body,
		"payload_hash":    payloadHash,
		"seen_revision":   delivery.Input.SeenRevision,
		"target_kind":     delivery.Input.TargetKind,
		"target_ref":      delivery.Input.TargetRef,
	})
	if err != nil {
		return "", err
	}
	return hashProductionBytes(raw), nil
}

func (o productionPhaseTerminalOutbox) intentIdentityHash(delivery productionPhaseTerminalDelivery, payloadHash string) (string, error) {
	raw, err := stableProductionJSON(map[string]any{
		"project_id":    o.projectID,
		"invocation_id": delivery.InvocationID,
		"input_kind":    delivery.Input.Kind,
		"request_id":    delivery.Input.RequestID,
		"payload_hash":  payloadHash,
		"target_kind":   delivery.Input.TargetKind,
	})
	if err != nil {
		return "", err
	}
	return hashProductionBytes(raw), nil
}

func (o productionPhaseTerminalOutbox) intentHashesForReceiptDelivery(ctx context.Context, obligationID string, delivery productionPhaseTerminalDelivery) (string, string, bool, error) {
	var intentPayload []byte
	var intentPayloadHash, intentIdentityHash, status string
	if err := o.db.QueryRowContext(ctx, `SELECT payload, intent_payload_hash, intent_identity_hash, status FROM production_phase_terminal_obligations WHERE obligation_id=$1 AND project_id=$2`, obligationID, o.projectID).Scan(&intentPayload, &intentPayloadHash, &intentIdentityHash, &status); errors.Is(err, sql.ErrNoRows) {
		return "", "", false, nil
	} else if err != nil {
		return "", "", false, err
	}
	if status != "intent" {
		return "", "", false, nil
	}
	var intent productionPhaseOutputIntent
	if err := json.Unmarshal(intentPayload, &intent); err != nil {
		return "", "", false, err
	}
	canonicalIntentPayload, err := json.Marshal(intent)
	if err != nil {
		return "", "", false, err
	}
	if intent.InvocationID != delivery.InvocationID {
		return "", "", false, kernel.IdempotencyConflict()
	}
	var envelope struct {
		Receipt phasepkg.OutputReceipt `json:"receipt"`
	}
	if err := json.Unmarshal(delivery.Input.Payload, &envelope); err != nil {
		return "", "", false, err
	}
	if envelope.Receipt.InvocationID != delivery.InvocationID || envelope.Receipt.Endpoint != delivery.Endpoint || envelope.Receipt.Generation != delivery.Generation {
		return "", "", false, kernel.IdempotencyConflict()
	}
	intentOutputRaw, err := json.Marshal(intent.Output)
	if err != nil {
		return "", "", false, err
	}
	if envelope.Receipt.OutputFingerprint != "" {
		if envelope.Receipt.OutputFingerprint != hashProductionBytes(intentOutputRaw) {
			return "", "", false, kernel.IdempotencyConflict()
		}
	} else if !productionPhaseReceiptMatchesOutput(envelope.Receipt, intent.Output) {
		return "", "", false, kernel.IdempotencyConflict()
	}
	expectedRequestID := stableProductionSuffix(delivery.InvocationID, "phase_output", hashProductionBytes(canonicalIntentPayload))
	if delivery.Input.RequestID != expectedRequestID || intentPayloadHash != hashProductionBytes(canonicalIntentPayload) {
		return "", "", false, kernel.IdempotencyConflict()
	}
	return intentPayloadHash, intentIdentityHash, true, nil
}

type productionPhaseReadiness struct {
	db             *sql.DB
	repositoryPath string
	worktreeParent string
	projectID      kernel.ProjectID
	graph          *coordination.PostgresStore
	contexts       *contextgraph.PostgresStore
	artifacts      *evidence.PostgresArtifactRegistry
}

func (r productionPhaseReadiness) Check(ctx context.Context) error {
	if r.db == nil || r.graph == nil || r.contexts == nil || r.artifacts == nil {
		return kernel.InvalidArgument("production phase readiness dependencies are required")
	}
	if err := r.db.PingContext(ctx); err != nil {
		return err
	}
	if _, err := os.Stat(r.repositoryPath); err != nil {
		return fmt.Errorf("phase repository path: %w", err)
	}
	if _, err := os.Stat(r.worktreeParent); err != nil {
		return fmt.Errorf("phase worktree parent: %w", err)
	}
	for _, table := range []string{"production_phase_bindings", "production_phase_outputs", "production_phase_terminal_obligations", "phase_agentteams_prepared_invocations", "phase_agentteams_host_states", "context_subscriptions"} {
		var exists bool
		if err := r.db.QueryRowContext(ctx, `SELECT to_regclass($1) IS NOT NULL`, table).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("phase readiness table %s is missing", table)
		}
	}
	if _, err := r.contexts.InspectSubscriptions(ctx, auth.Principal{Kind: auth.PrincipalOperator, ProjectID: r.projectID, Role: auth.RoleOperator}, "readiness-probe"); err != nil && !kernel.IsCode(err, kernel.CodeNotFound) {
		return err
	}
	_, err := r.graph.Latest(ctx, r.projectID)
	return err
}

type productionTaskEndpointResolver struct {
	projectID kernel.ProjectID
	graph     *coordination.PostgresStore
}

func (r productionTaskEndpointResolver) TaskExists(ctx context.Context, projectID kernel.ProjectID, taskID kernel.TaskID) (bool, error) {
	if projectID != r.projectID {
		return false, nil
	}
	snapshot, err := r.graph.Latest(ctx, projectID)
	if err != nil {
		return false, err
	}
	for _, task := range snapshot.Tasks {
		if task.ID == taskID {
			return true, nil
		}
	}
	return false, nil
}

func (r productionTaskEndpointResolver) TaskDone(ctx context.Context, projectID kernel.ProjectID, taskID kernel.TaskID) (bool, error) {
	if projectID != r.projectID {
		return false, nil
	}
	snapshot, err := r.graph.Latest(ctx, projectID)
	if err != nil {
		return false, err
	}
	for _, task := range snapshot.Tasks {
		if task.ID == taskID {
			return task.Outcome == coordination.TaskDone, nil
		}
	}
	return false, nil
}

func (r productionTaskEndpointResolver) EndpointExists(ctx context.Context, projectID kernel.ProjectID, endpoint contextgraph.PhaseEndpointRef) (bool, error) {
	if projectID != r.projectID {
		return false, nil
	}
	snapshot, err := r.graph.Latest(ctx, projectID)
	if err != nil {
		return false, err
	}
	for _, candidate := range snapshot.Endpoints {
		if string(candidate.Ref.TaskID) == string(endpoint.TaskID) && string(candidate.Ref.EndpointID) == string(endpoint.EndpointID) {
			return true, nil
		}
	}
	return false, nil
}

type productionPhaseOutputRecord struct {
	OutputRef    string
	ArtifactRefs []string
}

func workspacePhase(endpoint kernel.EndpointID) workspace.Phase {
	switch endpoint {
	case coordination.EndpointPlan:
		return workspace.PhasePlan
	case coordination.EndpointVerify:
		return workspace.PhaseVerify
	default:
		return workspace.PhaseExecute
	}
}

func phaseActorPrincipal(projectID kernel.ProjectID, endpoint coordination.PhaseEndpointRef, generation int) kernel.ActorPrincipalID {
	return kernel.ActorPrincipalID(fmt.Sprintf("phase-agent://%s/%s/%s/%d", projectID, endpoint.TaskID, endpoint.EndpointID, generation))
}

func phaseInputID(edge coordination.Edge) string {
	return fmt.Sprintf("%s/%s->%s/%s:%s", edge.From.TaskID, edge.From.EndpointID, edge.To.TaskID, edge.To.EndpointID, edge.RequiredBy)
}

func phaseInputRevision(revision kernel.Revision, inputs phasepkg.PhaseInputSet) string {
	raw, _ := json.Marshal(struct {
		Revision  kernel.Revision             `json:"revision"`
		Required  []phasepkg.InputRequirement `json:"required"`
		Delivered []phasepkg.InputDelivery    `json:"delivered"`
		Pending   []phasepkg.PendingInput     `json:"pending"`
	}{revision, inputs.Required, inputs.Delivered, inputs.Pending})
	sum := sha256.Sum256(raw)
	return "inputs:" + hex.EncodeToString(sum[:16])
}

func stableRuntimeRef(prefix string, parts ...any) string {
	return prefix + ":" + stableProductionSuffix(parts...)
}

func deterministicPhaseInvocationID(commandID string) kernel.InvocationID {
	sum := sha256.Sum256([]byte(commandID))
	return kernel.InvocationID(fmt.Sprintf("inv_%s", hex.EncodeToString(sum[:8])))
}

func stableProductionJSON(value any) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return raw, nil
}

var _ phasepkg.BaseBindingSource = (*productionPhaseBindingSource)(nil)
var _ agentteams.InvocationSource = productionInvocationSource{}
var _ phasepkg.InputRuntime = productionPhaseInputs{}
var _ phasepkg.ArtifactRouter = productionPhaseArtifactRouter{}
var _ runtimepkg.InvocationLifecycle = productionPhaseLifecycle{}
var _ mcpapi.PhaseRuntime = (*productionPhaseRuntime)(nil)
var _ mcpapi.OrchestrationProposalRuntime = (*productionPhaseRuntime)(nil)
var _ coordination.PhaseObservationWriter = productionPhaseObservationWriter{}
var _ contextgraph.TaskEndpointResolver = productionTaskEndpointResolver{}
