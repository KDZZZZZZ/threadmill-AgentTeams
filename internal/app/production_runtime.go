package app

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/adapters/agentteams"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/contextgraph"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/evidence"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/mergequeue"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/config"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/objectstore"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/postgres"
	runtimepkg "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime"
	phasepkg "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime/phase"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime/promptcatalog"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/scheduler"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/transport/mcpapi"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/workspace"
)

const productionReconcileInterval = 2 * time.Second

// productionPhaseSeams is the bounded handoff point for the full phase
// Controller. Nil fields are replaced by one explicit fail-closed boundary;
// callers may inject the authoritative phase implementation without changing
// ingress, AgentTeams, or GraphRuntime wiring.
type productionPhaseSeams struct {
	Controller     coordination.PhaseController
	Failures       productionPhaseFailureReporter
	Runtime        mcpapi.PhaseRuntime
	Orchestration  mcpapi.OrchestrationProposalRuntime
	Readiness      productionReadinessProbe
	TaskWorkspaces productionTaskWorkspaceProvisioner
	TaskContexts   productionTaskContextProjector

	// The fields below are internal assembly parts, never external ports. They
	// let production wire the Merge Queue's latest-main Verifier through the
	// same persisted invocation/controller/AgentTeams carrier as normal phases
	// without rebuilding a second, subtly different runtime stack.
	Host          phasepkg.Host
	WorkspaceSync interface {
		SyncWorkspace(context.Context, kernel.InvocationID) (agentteams.ExecutionWorkspaceCheckpoint, error)
	}
	Recovery       phasepkg.RecoveryStore
	Contexts       phasepkg.ContextRuntime
	TaskSubgraphs  targetedVerifyTaskSubgraphRegistrar
	Contracts      targetedVerifyContractStore
	ArtifactRouter phasepkg.ArtifactRouter
}

func buildProductionRuntimeDependencies(ctx context.Context, cfg config.Config, db *postgres.DB, phaseSeams productionPhaseSeams) (productionRuntimeDependencies, error) {
	if db == nil || db.SQL() == nil {
		return productionRuntimeDependencies{}, errors.New("production runtime database is required")
	}
	projectID := kernel.ProjectID(cfg.ProjectID)
	sqlDB := db.SQL()
	if err := validateProductionWorkspacePaths(cfg.RepositoryPath, cfg.WorktreeParent); err != nil {
		return productionRuntimeDependencies{}, err
	}
	store, err := objectstore.NewMinIOStore(objectstore.MinIOConfig{Endpoint: cfg.ObjectStoreEndpoint, AccessKey: cfg.ObjectStoreAccessKey, SecretKey: cfg.ObjectStoreSecretKey, Bucket: cfg.ObjectStoreBucket, Secure: cfg.ObjectStoreSecure})
	if err != nil {
		return productionRuntimeDependencies{}, err
	}
	evidenceStore, err := productionEvidenceObjectStore(cfg, store)
	if err != nil {
		return productionRuntimeDependencies{}, err
	}
	sharedStore, err := objectstore.NewMinIOStore(objectstore.MinIOConfig{Endpoint: cfg.ObjectStoreEndpoint, AccessKey: cfg.AgentTeamsSharedAccessKey, SecretKey: cfg.AgentTeamsSharedSecretKey, Bucket: cfg.AgentTeamsSharedBucket, Secure: cfg.ObjectStoreSecure})
	if err != nil {
		return productionRuntimeDependencies{}, err
	}
	availableTools := make(map[auth.Tool]struct{})
	for _, tool := range auth.CanonicalTools() {
		availableTools[tool] = struct{}{}
	}
	catalog, err := promptcatalog.Load(cfg.RuntimeAssetsRoot, availableTools)
	if err != nil {
		return productionRuntimeDependencies{}, fmt.Errorf("load production runtime assets: %w", err)
	}
	assembler, err := runtimepkg.NewAssembler(catalog)
	if err != nil {
		return productionRuntimeDependencies{}, err
	}
	coordStore := coordination.NewPostgresStore(sqlDB)
	contextStore := contextgraph.NewPostgresStore(sqlDB, time.Now)
	contextStore.SetTaskEndpointResolver(productionTaskEndpointResolver{projectID: projectID, graph: coordStore})
	artifactRegistry := evidence.NewPostgresArtifactRegistry(sqlDB, evidenceStore, cfg.ObjectStoreBucket)
	eventStore := evidence.NewPostgresEventStore(sqlDB, 1<<20)
	ingress, err := newProductionIngress(sqlDB, projectID, cfg.AgentTeamsRoomID, assembler, coordStore, time.Now)
	if err != nil {
		return productionRuntimeDependencies{}, err
	}
	taskManagerRuntime, err := newProductionTaskManagerRuntime(sqlDB, projectID, coordStore, time.Now)
	if err != nil {
		return productionRuntimeDependencies{}, err
	}
	if err := taskManagerRuntime.setProductionEventStore(eventStore); err != nil {
		return productionRuntimeDependencies{}, err
	}
	contextRuntime, err := newProductionContextRuntime(sqlDB, projectID, cfg.AgentTeamsRoomID, assembler, contextStore, time.Now)
	if err != nil {
		return productionRuntimeDependencies{}, err
	}
	invocationStore := runtimepkg.NewPostgresInvocationStoreFromSQL(sqlDB)
	workspaces := workspace.NewPostgresService(sqlDB)

	controller, err := agentteams.NewAgentTeamsControllerClient(cfg.AgentTeamsControllerURL, cfg.AgentTeamsControllerBearer, nil)
	if err != nil {
		return productionRuntimeDependencies{}, err
	}
	containers := make(agentteams.StaticContainerResolver, len(cfg.AgentTeamsContainers))
	for host, container := range cfg.AgentTeamsContainers {
		containers[host] = container
	}
	managerWorkers := productionManagerWorkerAliases(containers)
	const dedicatedTaskflowHostRef = "threadmill-dispatcher"
	taskflowHostRef := ""
	if _, ok := containers[dedicatedTaskflowHostRef]; ok {
		taskflowHostRef = dedicatedTaskflowHostRef
	}
	if len(managerWorkers) > 0 {
		if taskflowHostRef == "" {
			return productionRuntimeDependencies{}, errors.New("dedicated AgentTeams manager worker requires a taskflow dispatcher host")
		}
	}
	taskflow, err := agentteams.NewQwenPawDockerTaskflow("", "")
	if err != nil {
		return productionRuntimeDependencies{}, err
	}
	mcpResolver, err := newProductionMCPResolver(auth.NewPostgresStore(sqlDB), invocationStore, cfg.ContainerMCPURL, cfg.RuntimeTokenKey)
	if err != nil {
		return productionRuntimeDependencies{}, err
	}
	phaseHostStore := phasepkg.NewPostgresAgentTeamsPhaseHostStoreFromSQL(sqlDB)
	client, err := agentteams.NewProductionClient(agentteams.ProductionClientOptions{
		Controller:      controller,
		Slots:           agentteams.NewHostSlotStore(sqlDB),
		MCPResolver:     mcpResolver,
		QwenPaw:         agentteams.DockerQwenPawProvider{Containers: containers},
		Taskflow:        taskflow,
		SharedFiles:     taskflow,
		Containers:      containers,
		ManagerWorkers:  managerWorkers,
		TaskflowHostRef: taskflowHostRef,
	})
	if err != nil {
		return productionRuntimeDependencies{}, err
	}
	workspaceProjector := productionExecutionWorkspaceProjector{invocations: invocationStore, workspaces: workspaces}
	targetedRegistry := newProductionTargetedVerifyRegistry()
	targetedProjector := &productionTargetedVerifyWorkspaceProjector{registry: targetedRegistry}
	projectorRouter := productionTargetedVerifyProjectorRouter{Regular: workspaceProjector, Targeted: targetedProjector}
	files, err := agentteams.NewSharedObjectFileTransport(sharedStore, cfg.AgentTeamsSharedBucket, cfg.AgentTeamsSharedPrefix, projectorRouter)
	if err != nil {
		return productionRuntimeDependencies{}, err
	}
	adapter, err := agentteams.NewAdapter(client, productionInvocationSource{taskManager: ingress, context: contextRuntime, phase: phaseHostStore}, files, agentteams.NewPostgresExecutionStore(sqlDB), time.Now, 30*time.Second)
	if err != nil {
		return productionRuntimeDependencies{}, err
	}
	if err := ingress.setDispatcher(adapter); err != nil {
		return productionRuntimeDependencies{}, err
	}
	if err := contextRuntime.setDispatcher(adapter); err != nil {
		return productionRuntimeDependencies{}, err
	}
	taskManagerCleanup, err := newProductionTaskManagerExecutionCleanup(sqlDB, adapter, contextStore, time.Now)
	if err != nil {
		return productionRuntimeDependencies{}, err
	}
	if err := ingress.setTaskManagerExecutionCleaner(taskManagerCleanup); err != nil {
		return productionRuntimeDependencies{}, err
	}
	if err := ingress.setPersistedDecisionRecoverer(taskManagerRuntime); err != nil {
		return productionRuntimeDependencies{}, err
	}
	if phaseSeams.Controller == nil || phaseSeams.Runtime == nil || phaseSeams.Orchestration == nil || phaseSeams.Readiness == nil {
		phaseSeams, err = buildProductionPhaseSeams(productionPhaseBundleOptions{
			Config: cfg, DB: sqlDB, ProjectID: projectID, Graph: coordStore, Assembler: assembler,
			Adapter: adapter, Ingress: ingress, ObjectStore: evidenceStore, Workspaces: workspaces, Now: time.Now,
		})
		if err != nil {
			return productionRuntimeDependencies{}, err
		}
	}
	targetedConfigured := phaseSeams.Host != nil && phaseSeams.WorkspaceSync != nil && phaseSeams.Recovery != nil && phaseSeams.Contexts != nil && phaseSeams.Contracts != nil && phaseSeams.ArtifactRouter != nil
	if !targetedConfigured {
		return productionRuntimeDependencies{}, errors.New("production targeted verify requires the complete internal Phase runtime assembly")
	}
	targetedBundle, err := buildProductionTargetedVerifyBundle(productionTargetedVerifyBundleOptions{
		ProjectID: projectID, Graph: coordStore, Proposals: ingress,
		Contracts: phaseSeams.Contracts, Invocations: invocationStore,
		Assembler: assembler, Host: phaseSeams.Host, Recovery: phaseSeams.Recovery,
		Contexts: phaseSeams.Contexts, TaskSubgraphs: phaseSeams.TaskSubgraphs,
		ArtifactRouter:  phaseSeams.ArtifactRouter,
		OutputValidator: productionTargetedVerifyResultAcceptor{projectID: projectID, artifacts: artifactRegistry},
		Registry:        targetedRegistry, Projector: targetedProjector,
		WorkspaceSync: phaseSeams.WorkspaceSync, Now: time.Now,
	})
	if err != nil {
		return productionRuntimeDependencies{}, err
	}
	phaseRuntimeRouter := productionTargetedVerifyRuntimeRouter{
		Regular: phaseSeams.Runtime, RegularProposal: phaseSeams.Orchestration,
		Targeted: targetedBundle.Runtime, Registry: targetedBundle.Registry,
	}
	mergeQueue := &productionMergeQueue{
		db: sqlDB, projectID: projectID, repositoryPath: cfg.RepositoryPath,
		graph: coordStore, workspaces: workspaces, artifacts: artifactRegistry,
	}
	provider, ok := phaseSeams.TaskWorkspaces.(productionMergedVerifyWorkspaceProvisioner)
	if !ok {
		return productionRuntimeDependencies{}, errors.New("production code_merge requires merged-revision Verify workspace provisioning")
	}
	mergeQueue.verifySpaces = provider
	mergeQueue.queue = mergequeue.NewReconciler(
		mergequeue.NewPostgresStore(sqlDB), workspaces,
		productionAgentTeamsMergeVerifier(projectID, targetedBundle.Bindings, targetedBundle.Runtime, artifactRegistry),
		mergequeue.GitBackend{TempParent: cfg.WorktreeParent}, artifactRegistry, eventStore,
	)
	if err := taskManagerRuntime.setProductionMergeQueue(mergeQueue, mergeQueue); err != nil {
		return productionRuntimeDependencies{}, err
	}
	phaseBoundary := &productionUnavailablePhaseBoundary{invocations: invocationStore}
	phaseConfigured := phaseSeams.Controller != nil && phaseSeams.Failures != nil && phaseSeams.Runtime != nil && phaseSeams.Orchestration != nil && phaseSeams.Readiness != nil
	if phaseSeams.Controller == nil {
		phaseSeams.Controller = phaseBoundary
	}
	if phaseSeams.Failures == nil {
		phaseSeams.Failures = phaseBoundary
	}
	if phaseSeams.Runtime == nil {
		phaseSeams.Runtime = phaseBoundary
	}
	if phaseSeams.Orchestration == nil {
		phaseSeams.Orchestration = phaseBoundary
	}
	if phaseSeams.Readiness == nil {
		phaseSeams.Readiness = phaseBoundary
	}
	if phaseSeams.TaskWorkspaces != nil || phaseSeams.TaskContexts != nil {
		if err := taskManagerRuntime.setProductionDependencies(phaseSeams.TaskWorkspaces, phaseSeams.TaskContexts, ingress); err != nil {
			return productionRuntimeDependencies{}, err
		}
	}
	if err := taskManagerRuntime.setProductionMemoryFinalizer(contextRuntime); err != nil {
		return productionRuntimeDependencies{}, err
	}
	baseSelection := scheduler.New(scheduler.BudgetPolicy{VerifyLevel: scheduler.VerifyStandard, ExplorationLevel: scheduler.ExplorationTargeted})
	graphRuntime, err := coordination.NewRuntime(coordination.RuntimeOptions{
		ProjectID: projectID, Store: coordStore, PhaseController: phaseSeams.Controller,
		Selection:  productionMergeAwareSelection{db: sqlDB, projectID: projectID, inner: baseSelection},
		Scheduling: scheduler.NewPostgresSchedulingStateProvider(sqlDB, projectID),
	})
	if err != nil {
		return productionRuntimeDependencies{}, err
	}
	loop := newProductionRuntimeLoop(client, scheduler.NewPostgresCapacityLedger(sqlDB, projectID), graphRuntime, phaseSeams.Readiness, phaseConfigured, time.Now)
	phaseFailures, err := newProductionPhaseExecutionMonitor(sqlDB, projectID, adapter, phaseSeams.Failures, time.Now)
	if err != nil {
		return productionRuntimeDependencies{}, err
	}
	targetedFailures, err := newProductionTargetedVerifyExecutionMonitor(sqlDB, projectID, targetedBundle.Registry, adapter, targetedBundle.Runtime, time.Now)
	if err != nil {
		return productionRuntimeDependencies{}, err
	}
	loop.phaseFailures = func(ctx context.Context) error {
		return errors.Join(phaseFailures.Reconcile(ctx), targetedFailures.Reconcile(ctx))
	}
	loop.executionCleanup = func(ctx context.Context) error {
		return errors.Join(
			taskManagerCleanup.CleanupTaskManagerInvocations(ctx),
			ingress.RetryFailedTaskManagerInputs(ctx),
			taskManagerRuntime.ReconcileCompletedTransitionFollowups(ctx),
			contextRuntime.Reconcile(ctx),
		)
	}
	mergeWorker := newProductionAsyncReconciler(mergeQueue.Reconcile)
	loop.mergeQueue = mergeWorker.Poll
	loop.mergeQueueWait = mergeWorker.Wait
	if runtime, ok := phaseSeams.Runtime.(*productionPhaseRuntime); ok {
		loop.terminalRetry = runtime.ReplayTerminalDeliveries
	}
	loop.Start(ctx)
	workspaceTools := workspace.NewAgentTools(workspaces)
	objectProbe, err := newProductionMinIOReadiness(cfg.ObjectStoreEndpoint, cfg.ObjectStoreSecure)
	if err != nil {
		loop.Close(context.Background())
		return productionRuntimeDependencies{}, err
	}
	return productionRuntimeDependencies{
		requirements: ingress, human: ingress, manager: ingress,
		phaseController: phaseSeams.Controller, phase: phaseRuntimeRouter,
		requirement: productionRequirementSubmitter{ingress: ingress}, orchestration: phaseRuntimeRouter,
		taskManager: taskManagerRuntime, workspace: workspaceTools,
		contextRetrieve: contextRuntime, contextSearcher: productionContextSearcher{searcher: contextStore, runtime: contextRuntime}, memoryFinalizer: contextRuntime,
		objectStoreDriver: store, objectStore: objectProbe, agentTeams: client, runtime: loop, background: loop,
	}, nil
}

func productionManagerWorkerAliases(containers agentteams.StaticContainerResolver) map[string]string {
	aliases := map[string]string{}
	for _, logicalHost := range []string{"default", "context"} {
		if container := containers[logicalHost]; strings.HasPrefix(container, "agentteams-worker-") {
			aliases[logicalHost] = strings.TrimPrefix(container, "agentteams-worker-")
		}
	}
	return aliases
}

func validateProductionWorkspacePaths(repositoryPath, worktreeParent string) error {
	for _, path := range []struct {
		name  string
		value string
	}{{"repository", repositoryPath}, {"worktree parent", worktreeParent}} {
		info, err := os.Stat(path.value)
		if err != nil {
			return fmt.Errorf("production %s path is unavailable: %w", path.name, err)
		}
		if !info.IsDir() {
			return fmt.Errorf("production %s path is not a directory", path.name)
		}
	}
	return nil
}

type productionMCPResolver struct {
	authStore   auth.Store
	invocations runtimepkg.InvocationStore
	url         string
	key         []byte
	now         func() time.Time
	tokenMu     sync.Mutex
}

func newProductionMCPResolver(authStore auth.Store, invocations runtimepkg.InvocationStore, mcpURL string, key []byte) (*productionMCPResolver, error) {
	parsed, err := url.Parse(strings.TrimSpace(mcpURL))
	if authStore == nil || invocations == nil || err != nil || parsed.Scheme == "" || parsed.Host == "" || len(key) != 32 {
		return nil, kernel.InvalidArgument("production invocation MCP resolver requires store, invocation source, URL, and 32-byte key")
	}
	return &productionMCPResolver{authStore: authStore, invocations: invocations, url: parsed.String(), key: append([]byte(nil), key...), now: time.Now}, nil
}

func (r *productionMCPResolver) ResolveInvocationMCP(ctx context.Context, preparation agentteams.HostPreparation) (agentteams.InvocationMCPMaterial, error) {
	invocation, ok, err := r.invocations.Get(ctx, preparation.InvocationID)
	if err != nil {
		return agentteams.InvocationMCPMaterial{}, err
	}
	if !ok || invocation.Role != preparation.Role || invocation.Operation != preparation.Operation {
		return agentteams.InvocationMCPMaterial{}, kernel.Forbidden("AgentTeams host preparation does not match persisted invocation")
	}
	if invocation.Status != runtimepkg.InvocationPrepared && invocation.Status != runtimepkg.InvocationRunning && invocation.Status != runtimepkg.InvocationWaiting {
		return agentteams.InvocationMCPMaterial{}, kernel.Forbidden("invocation is not active for MCP preparation")
	}
	if !invocation.ExpiresAt.After(r.now()) {
		return agentteams.InvocationMCPMaterial{}, kernel.Forbidden("invocation expired before MCP preparation")
	}
	token := r.token(invocation.ID)
	record := auth.TokenRecord{TokenHash: auth.HashOpaqueSecret(token), ActorPrincipalID: invocation.ActorPrincipalID, Capability: invocation.Capability(), ExpiresAt: invocation.ExpiresAt}
	r.tokenMu.Lock()
	defer r.tokenMu.Unlock()
	existing, found, err := r.authStore.TokenByHash(ctx, record.TokenHash)
	if err != nil {
		return agentteams.InvocationMCPMaterial{}, err
	}
	if found {
		// A deterministic bearer is a one-lifetime credential. Reusing an
		// active identical row is idempotent; a revoked row can never be
		// overwritten back to active, including after a process restart.
		if existing.RevokedAt != nil {
			return agentteams.InvocationMCPMaterial{}, kernel.Forbidden("invocation MCP bearer was revoked and cannot be reactivated")
		}
		if !sameProductionTokenRecord(existing, record) {
			return agentteams.InvocationMCPMaterial{}, kernel.Forbidden("invocation MCP bearer conflicts with persisted authority")
		}
	} else if err := r.authStore.PutToken(ctx, record); err != nil {
		return agentteams.InvocationMCPMaterial{}, err
	}
	tools := make([]string, 0, len(invocation.EffectiveTools))
	for _, tool := range invocation.EffectiveTools {
		tools = append(tools, string(tool))
	}
	sort.Strings(tools)
	return agentteams.InvocationMCPMaterial{URL: r.url, BearerToken: token, TokenIdentifier: string(invocation.ID), ExpectedTools: tools}, nil
}

func sameProductionTokenRecord(left, right auth.TokenRecord) bool {
	if left.ActorPrincipalID != right.ActorPrincipalID || !left.ExpiresAt.Equal(right.ExpiresAt) {
		return false
	}
	leftCapability, rightCapability := left.Capability, right.Capability
	if leftCapability.ProjectID != rightCapability.ProjectID || leftCapability.TaskID != rightCapability.TaskID ||
		leftCapability.InvocationID != rightCapability.InvocationID || leftCapability.ConsumerInvocationID != rightCapability.ConsumerInvocationID ||
		leftCapability.ConsumerTaskID != rightCapability.ConsumerTaskID || leftCapability.ConsumerRole != rightCapability.ConsumerRole ||
		leftCapability.Role != rightCapability.Role || leftCapability.Operation != rightCapability.Operation ||
		!leftCapability.ExpiresAt.Equal(rightCapability.ExpiresAt) || len(leftCapability.Tools) != len(rightCapability.Tools) {
		return false
	}
	for tool := range leftCapability.Tools {
		if _, ok := rightCapability.Tools[tool]; !ok {
			return false
		}
	}
	return true
}

// RevokeInvocationMCP invalidates leaked copies of the deterministic bearer.
// The AgentTeams client remains responsible for removing the MCP client from
// the container before releasing the host slot.
func (r *productionMCPResolver) RevokeInvocationMCP(ctx context.Context, invocationID kernel.InvocationID) error {
	if err := kernel.RequireID("invocation_id", invocationID); err != nil {
		return err
	}
	r.tokenMu.Lock()
	defer r.tokenMu.Unlock()
	err := r.authStore.RevokeToken(ctx, auth.HashOpaqueSecret(r.token(invocationID)), r.now().UTC())
	// Revocation is idempotent at the authority boundary. PrepareHost can fail
	// before the deterministic bearer row is inserted; cleanup must still be
	// able to close the reserved execution and release its slot. A missing row
	// means there is no credential to revoke, never that one should be created.
	if kernel.IsCode(err, kernel.CodeUnauthorized) {
		return nil
	}
	return err
}

func (r *productionMCPResolver) token(invocationID kernel.InvocationID) string {
	mac := hmac.New(sha256.New, r.key)
	_, _ = mac.Write([]byte(invocationID))
	return "tm_" + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

type productionUnavailablePhaseBoundary struct {
	invocations runtimepkg.InvocationStore
}

func (p *productionUnavailablePhaseBoundary) FailInvocation(context.Context, coordination.PhaseCommand) error {
	return phaseUnavailable("phase failure runtime is not configured")
}

func (p *productionUnavailablePhaseBoundary) Apply(ctx context.Context, command coordination.PhaseCommand) error {
	invocation, ok, err := p.invocations.GetByLease(ctx, command.LeaseRef)
	if err != nil {
		return err
	}
	if !ok {
		return phaseUnavailable("phase invocation binding is not materialized")
	}
	if invocation.TaskID != command.Endpoint.TaskID || invocation.EndpointID != kernel.EndpointID(command.Endpoint.EndpointID) || invocation.Generation != uint64(command.Generation) || invocation.BindingRef != command.BindingRef {
		return kernel.StaleBinding("phase command does not match persisted invocation binding")
	}
	return phaseUnavailable("phase AgentTeams host is not configured")
}

func (p *productionUnavailablePhaseBoundary) AwaitInputs(ctx context.Context, invocationID kernel.InvocationID, _ phasepkg.AwaitInputsRequest) (phasepkg.InputWaitResult, error) {
	if _, err := p.requirePhaseInvocation(ctx, invocationID); err != nil {
		return phasepkg.InputWaitResult{}, err
	}
	return phasepkg.InputWaitResult{}, phaseUnavailable("phase input runtime is not configured")
}

func (p *productionUnavailablePhaseBoundary) SubmitPhaseOutput(ctx context.Context, invocationID kernel.InvocationID, _ phasepkg.PhaseOutput) (phasepkg.OutputReceipt, error) {
	if _, err := p.requirePhaseInvocation(ctx, invocationID); err != nil {
		return phasepkg.OutputReceipt{}, err
	}
	return phasepkg.OutputReceipt{}, phaseUnavailable("phase output runtime is not configured")
}

func (p *productionUnavailablePhaseBoundary) SubmitOrchestrationIntent(ctx context.Context, principal auth.Principal, _ auth.BoundScope, _ phasepkg.OrchestrationIntent) (phasepkg.OrchestrationProposal, error) {
	if _, err := p.requirePhaseInvocation(ctx, principal.InvocationID); err != nil {
		return phasepkg.OrchestrationProposal{}, err
	}
	return phasepkg.OrchestrationProposal{}, phaseUnavailable("phase orchestration routing is not configured")
}

func (p *productionUnavailablePhaseBoundary) Check(context.Context) error {
	return phaseUnavailable("phase runtime is not configured")
}

func (p *productionUnavailablePhaseBoundary) requirePhaseInvocation(ctx context.Context, invocationID kernel.InvocationID) (runtimepkg.Invocation, error) {
	invocation, ok, err := p.invocations.Get(ctx, invocationID)
	if err != nil {
		return runtimepkg.Invocation{}, err
	}
	if !ok || !invocation.Role.IsPhase() {
		return runtimepkg.Invocation{}, kernel.Forbidden("phase runtime requires a persisted phase invocation")
	}
	return invocation, nil
}

func phaseUnavailable(message string) error {
	return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: message, Recoverable: true}
}

type productionHostLister interface {
	ListHosts(context.Context) ([]agentteams.HostStatus, error)
}

type productionCapacityObserver interface {
	Observe(context.Context, int, int) (scheduler.Capacity, error)
}

type productionRuntimeLoop struct {
	hosts            productionHostLister
	capacity         productionCapacityObserver
	runtime          coordination.RuntimeRunner
	phase            productionReadinessProbe
	terminalRetry    func(context.Context) error
	executionCleanup func(context.Context) error
	phaseFailures    func(context.Context) error
	mergeQueue       func(context.Context) error
	mergeQueueWait   func()
	phaseReady       bool
	now              func() time.Time
	cancel           context.CancelFunc
	done             chan struct{}
	mu               sync.RWMutex
	lastErr          error
	reconciled       bool
}

func newProductionRuntimeLoop(hosts productionHostLister, capacity productionCapacityObserver, runtime coordination.RuntimeRunner, phase productionReadinessProbe, phaseReady bool, now func() time.Time) *productionRuntimeLoop {
	if now == nil {
		now = time.Now
	}
	return &productionRuntimeLoop{hosts: hosts, capacity: capacity, runtime: runtime, phase: phase, phaseReady: phaseReady, now: now, done: make(chan struct{}), lastErr: errors.New("runtime has not reconciled")}
}

func (l *productionRuntimeLoop) Start(parent context.Context) {
	ctx, cancel := context.WithCancel(parent)
	l.cancel = cancel
	go l.run(ctx)
}

func (l *productionRuntimeLoop) run(ctx context.Context) {
	defer func() {
		if l.mergeQueueWait != nil {
			l.mergeQueueWait()
		}
		close(l.done)
	}()
	l.step(ctx)
	ticker := time.NewTicker(productionReconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			l.step(ctx)
		}
	}
}

// productionAsyncReconciler keeps long external workflows (notably
// latest-main verification and conflict resolution) out of the two-second
// graph-control loop. Poll starts at most one run, reports the most recently
// completed infrastructure error for readiness, and lets the next poll retry.
// The run receives the loop's lifetime context so Close cancels it and Wait
// joins it before the production host reports shutdown complete.
type productionAsyncReconciler struct {
	run func(context.Context) error

	mu      sync.Mutex
	running bool
	lastErr error
	wg      sync.WaitGroup
}

func newProductionAsyncReconciler(run func(context.Context) error) *productionAsyncReconciler {
	return &productionAsyncReconciler{run: run}
}

func (r *productionAsyncReconciler) Poll(ctx context.Context) error {
	if r == nil || r.run == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	lastErr := r.lastErr
	if r.running {
		r.mu.Unlock()
		return lastErr
	}
	r.running = true
	r.wg.Add(1)
	r.mu.Unlock()
	go func() {
		defer r.wg.Done()
		err := r.run(ctx)
		r.mu.Lock()
		r.lastErr = err
		r.running = false
		r.mu.Unlock()
	}()
	return lastErr
}

func (r *productionAsyncReconciler) Wait() {
	if r != nil {
		r.wg.Wait()
	}
}

func (l *productionRuntimeLoop) step(ctx context.Context) {
	var stepErr error
	if l.executionCleanup != nil {
		stepErr = errors.Join(stepErr, l.executionCleanup(ctx))
	}
	if l.phaseFailures != nil {
		stepErr = errors.Join(stepErr, l.phaseFailures(ctx))
	}
	if l.mergeQueue != nil {
		stepErr = errors.Join(stepErr, l.mergeQueue(ctx))
	}
	hosts, hostErr := l.hosts.ListHosts(ctx)
	if hostErr != nil {
		stepErr = errors.Join(stepErr, hostErr)
	} else {
		healthy, active := 0, 0
		for _, host := range hosts {
			if !productionPhaseHostReady(host, l.now()) {
				continue
			}
			healthy += host.Capacity
			active += host.ActiveExecutions
		}
		if active > healthy {
			active = healthy
		}
		_, observeErr := l.capacity.Observe(ctx, healthy, active)
		stepErr = errors.Join(stepErr, observeErr)
	}
	if l.terminalRetry != nil {
		retryErr := l.terminalRetry(ctx)
		if !productionTerminalDeliveryWaitsForCapacity(retryErr) {
			stepErr = errors.Join(stepErr, retryErr)
		}
	}
	// Graph lifecycle control is independent of provider cleanup, readiness,
	// and terminal-delivery recovery. In particular, a held endpoint must still
	// emit stop while an unrelated Context invocation is unhealthy.
	stepErr = errors.Join(stepErr, l.runtime.Reconcile(ctx))
	l.mu.Lock()
	l.lastErr = stepErr
	l.reconciled = stepErr == nil
	l.mu.Unlock()
}

// A durable terminal obligation that cannot acquire a Task Manager host is
// pending business work, not a broken runtime dependency. The outbox retains
// the exact obligation and retries it on the next reconcile tick. Readiness
// must still fail for persistence, protocol, or mixed failures so operators do
// not lose infrastructure faults behind one retryable capacity error.
func productionTerminalDeliveryWaitsForCapacity(err error) bool {
	if err == nil {
		return false
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		children := joined.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if child == nil || !productionTerminalDeliveryWaitsForCapacity(child) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		if cause := wrapped.Unwrap(); cause != nil {
			return productionTerminalDeliveryWaitsForCapacity(cause)
		}
	}
	return kernel.IsCode(err, kernel.CodeExecutorUnavailable)
}

func productionPhaseHostReady(host agentteams.HostStatus, now time.Time) bool {
	if host.Kind != agentteams.HostWorker {
		return false
	}
	phase := strings.ToLower(strings.TrimSpace(host.Phase))
	if phase != "running" && phase != "sleeping" {
		return false
	}
	// Sleeping is an intentional zero-cost carrier state. It remains available
	// capacity because Dispatch selects it and PrepareHost wakes it before MCP
	// installation. Requiring a live heartbeat from a sleeping process would
	// make the next phase permanently unschedulable after the first idle sleep.
	if phase == "running" && (host.LastHeartbeat.IsZero() || now.Sub(host.LastHeartbeat) > 30*time.Second) {
		return false
	}
	for _, capability := range host.Capabilities {
		if capability == "shell" {
			return true
		}
	}
	return false
}

func (l *productionRuntimeLoop) Check(ctx context.Context) error {
	if !l.phaseReady {
		return l.phase.Check(ctx)
	}
	if err := l.phase.Check(ctx); err != nil {
		return err
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	if !l.reconciled {
		return l.lastErr
	}
	return nil
}

func (l *productionRuntimeLoop) Close(ctx context.Context) error {
	if l.cancel != nil {
		l.cancel()
	}
	select {
	case <-l.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type productionHTTPReadinessProbe struct {
	url    string
	client *http.Client
}

func newProductionMinIOReadiness(endpoint string, secure bool) (*productionHTTPReadinessProbe, error) {
	base := strings.TrimSpace(endpoint)
	if !strings.Contains(base, "://") {
		scheme := "http"
		if secure {
			scheme = "https"
		}
		base = scheme + "://" + base
	}
	parsed, err := url.Parse(base)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return nil, kernel.InvalidArgument("object store readiness endpoint is invalid")
	}
	parsed.Path = "/minio/health/ready"
	parsed.RawQuery, parsed.Fragment, parsed.User = "", "", nil
	return &productionHTTPReadinessProbe{url: parsed.String(), client: &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

func (p *productionHTTPReadinessProbe) Check(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.url, nil)
	if err != nil {
		return err
	}
	response, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("object store readiness returned HTTP %d", response.StatusCode)
	}
	return nil
}

var _ agentteams.InvocationMCPResolver = (*productionMCPResolver)(nil)
var _ coordination.PhaseController = (*productionUnavailablePhaseBoundary)(nil)
var _ mcpapi.PhaseRuntime = (*productionUnavailablePhaseBoundary)(nil)
var _ mcpapi.OrchestrationProposalRuntime = (*productionUnavailablePhaseBoundary)(nil)
