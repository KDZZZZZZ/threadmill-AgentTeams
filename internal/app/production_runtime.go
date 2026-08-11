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
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
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
	Runtime        mcpapi.PhaseRuntime
	Orchestration  mcpapi.OrchestrationProposalRuntime
	Readiness      productionReadinessProbe
	TaskWorkspaces productionTaskWorkspaceProvisioner
	TaskContexts   productionTaskContextProjector
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
	sharedStore, err := objectstore.NewMinIOStore(objectstore.MinIOConfig{Endpoint: cfg.ObjectStoreEndpoint, AccessKey: cfg.ObjectStoreAccessKey, SecretKey: cfg.ObjectStoreSecretKey, Bucket: cfg.AgentTeamsSharedBucket, Secure: cfg.ObjectStoreSecure})
	if err != nil {
		return productionRuntimeDependencies{}, err
	}
	availableTools := make(map[auth.Tool]struct{})
	for _, tool := range auth.CanonicalTools() {
		availableTools[tool] = struct{}{}
	}
	catalog, err := promptcatalog.Load(cfg.RepositoryPath, availableTools)
	if err != nil {
		return productionRuntimeDependencies{}, fmt.Errorf("load production runtime assets: %w", err)
	}
	assembler, err := runtimepkg.NewAssembler(catalog)
	if err != nil {
		return productionRuntimeDependencies{}, err
	}
	coordStore := coordination.NewPostgresStore(sqlDB)
	ingress, err := newProductionIngress(sqlDB, projectID, cfg.AgentTeamsRoomID, assembler, coordStore, time.Now)
	if err != nil {
		return productionRuntimeDependencies{}, err
	}
	taskManagerRuntime, err := newProductionTaskManagerRuntime(sqlDB, projectID, coordStore, time.Now)
	if err != nil {
		return productionRuntimeDependencies{}, err
	}
	invocationStore := runtimepkg.NewPostgresInvocationStoreFromSQL(sqlDB)

	controller, err := agentteams.NewAgentTeamsControllerClient(cfg.AgentTeamsControllerURL, cfg.AgentTeamsControllerBearer, nil)
	if err != nil {
		return productionRuntimeDependencies{}, err
	}
	containers := make(agentteams.StaticContainerResolver, len(cfg.AgentTeamsContainers))
	for host, container := range cfg.AgentTeamsContainers {
		containers[host] = container
	}
	managerWorkers := map[string]string{}
	if container := containers["default"]; strings.HasPrefix(container, "agentteams-worker-") {
		managerWorkers["default"] = strings.TrimPrefix(container, "agentteams-worker-")
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
		Controller:     controller,
		Slots:          agentteams.NewHostSlotStore(sqlDB),
		MCPResolver:    mcpResolver,
		QwenPaw:        agentteams.DockerQwenPawProvider{Containers: containers},
		Taskflow:       taskflow,
		Containers:     containers,
		ManagerWorkers: managerWorkers,
	})
	if err != nil {
		return productionRuntimeDependencies{}, err
	}
	files, err := agentteams.NewSharedObjectFileTransport(sharedStore, cfg.AgentTeamsSharedBucket, cfg.AgentTeamsSharedPrefix)
	if err != nil {
		return productionRuntimeDependencies{}, err
	}
	adapter, err := agentteams.NewAdapter(client, productionInvocationSource{taskManager: ingress, phase: phaseHostStore}, files, agentteams.NewPostgresExecutionStore(sqlDB), time.Now, 30*time.Second)
	if err != nil {
		return productionRuntimeDependencies{}, err
	}
	if err := ingress.setDispatcher(adapter); err != nil {
		return productionRuntimeDependencies{}, err
	}
	if phaseSeams.Controller == nil || phaseSeams.Runtime == nil || phaseSeams.Orchestration == nil || phaseSeams.Readiness == nil {
		phaseSeams, err = buildProductionPhaseSeams(productionPhaseBundleOptions{
			Config: cfg, DB: sqlDB, ProjectID: projectID, Graph: coordStore, Assembler: assembler,
			Adapter: adapter, Ingress: ingress, ObjectStore: store, Now: time.Now,
		})
		if err != nil {
			return productionRuntimeDependencies{}, err
		}
	}
	phaseBoundary := &productionUnavailablePhaseBoundary{invocations: invocationStore}
	phaseConfigured := phaseSeams.Controller != nil && phaseSeams.Runtime != nil && phaseSeams.Orchestration != nil && phaseSeams.Readiness != nil
	if phaseSeams.Controller == nil {
		phaseSeams.Controller = phaseBoundary
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
	graphRuntime, err := coordination.NewRuntime(coordination.RuntimeOptions{
		ProjectID: projectID, Store: coordStore, PhaseController: phaseSeams.Controller,
		Selection:  scheduler.New(scheduler.BudgetPolicy{VerifyLevel: scheduler.VerifyStandard, ExplorationLevel: scheduler.ExplorationTargeted}),
		Scheduling: scheduler.NewPostgresSchedulingStateProvider(sqlDB, projectID),
	})
	if err != nil {
		return productionRuntimeDependencies{}, err
	}
	loop := newProductionRuntimeLoop(client, scheduler.NewPostgresCapacityLedger(sqlDB, projectID), graphRuntime, phaseSeams.Readiness, phaseConfigured, time.Now)
	if runtime, ok := phaseSeams.Runtime.(*productionPhaseRuntime); ok {
		loop.terminalRetry = runtime.ReplayTerminalDeliveries
	}
	loop.Start(ctx)
	workspaceTools := workspace.NewAgentTools(workspace.NewPostgresService(sqlDB))
	objectProbe, err := newProductionMinIOReadiness(cfg.ObjectStoreEndpoint, cfg.ObjectStoreSecure)
	if err != nil {
		loop.Close(context.Background())
		return productionRuntimeDependencies{}, err
	}
	return productionRuntimeDependencies{
		requirements: ingress, human: ingress, manager: ingress,
		phaseController: phaseSeams.Controller, phase: phaseSeams.Runtime,
		requirement: productionRequirementSubmitter{ingress: ingress}, orchestration: phaseSeams.Orchestration,
		taskManager: taskManagerRuntime, workspace: workspaceTools,
		objectStoreDriver: store, objectStore: objectProbe, agentTeams: client, runtime: loop, background: loop,
	}, nil
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
	return r.authStore.RevokeToken(ctx, auth.HashOpaqueSecret(r.token(invocationID)), r.now().UTC())
}

func (r *productionMCPResolver) token(invocationID kernel.InvocationID) string {
	mac := hmac.New(sha256.New, r.key)
	_, _ = mac.Write([]byte(invocationID))
	return "tm_" + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

type productionUnavailablePhaseBoundary struct {
	invocations runtimepkg.InvocationStore
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
	hosts         productionHostLister
	capacity      productionCapacityObserver
	runtime       coordination.RuntimeRunner
	phase         productionReadinessProbe
	terminalRetry func(context.Context) error
	phaseReady    bool
	now           func() time.Time
	cancel        context.CancelFunc
	done          chan struct{}
	mu            sync.RWMutex
	lastErr       error
	reconciled    bool
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
	defer close(l.done)
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

func (l *productionRuntimeLoop) step(ctx context.Context) {
	hosts, err := l.hosts.ListHosts(ctx)
	if err == nil {
		healthy, active := 0, 0
		for _, host := range hosts {
			if host.Kind != agentteams.HostWorker || host.Phase != "Running" || host.LastHeartbeat.IsZero() || l.now().Sub(host.LastHeartbeat) > 30*time.Second {
				continue
			}
			healthy += host.Capacity
			active += host.ActiveExecutions
		}
		if active > healthy {
			active = healthy
		}
		_, err = l.capacity.Observe(ctx, healthy, active)
	}
	if err == nil {
		if l.terminalRetry != nil {
			err = l.terminalRetry(ctx)
		}
	}
	if err == nil {
		err = l.runtime.Reconcile(ctx)
	}
	l.mu.Lock()
	l.lastErr = err
	l.reconciled = err == nil
	l.mu.Unlock()
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
