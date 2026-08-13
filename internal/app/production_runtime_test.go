package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/adapters/agentteams"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	runtimepkg "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/scheduler"
)

func TestProductionMCPResolverDerivesRestartStableScopedToken(t *testing.T) {
	now := time.Date(2026, time.August, 11, 13, 0, 0, 0, time.UTC)
	invocations := runtimepkg.NewMemoryInvocationStore()
	invocation := validProductionInvocation(now, auth.RoleTaskManager)
	if err := invocations.Create(context.Background(), invocation); err != nil {
		t.Fatal(err)
	}
	authStore := auth.NewMemoryStore()
	resolver, err := newProductionMCPResolver(authStore, invocations, "http://threadmill.internal/mcp", []byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	prep := agentteams.HostPreparation{InvocationID: invocation.ID, Role: invocation.Role}
	first, err := resolver.ResolveInvocationMCP(context.Background(), prep)
	if err != nil {
		t.Fatal(err)
	}
	second, err := resolver.ResolveInvocationMCP(context.Background(), prep)
	if err != nil {
		t.Fatal(err)
	}
	if first.BearerToken == "" || first.BearerToken != second.BearerToken || first.TokenIdentifier != string(invocation.ID) {
		t.Fatalf("MCP material is not stable and invocation-bound: first=%#v second=%#v", first, second)
	}
	principal, err := auth.NewAuthenticator(authStore, func() time.Time { return now }).AuthenticateAgentToken(context.Background(), first.BearerToken)
	if err != nil {
		t.Fatalf("derived token does not authenticate: %v", err)
	}
	if principal.InvocationID != invocation.ID || principal.Role != auth.RoleTaskManager || !principal.HasTool(auth.ToolCoordinationTransition) {
		t.Fatalf("derived token principal = %#v", principal)
	}
	resolver.now = func() time.Time { return now.Add(time.Minute) }
	if err := resolver.RevokeInvocationMCP(context.Background(), invocation.ID); err != nil {
		t.Fatalf("RevokeInvocationMCP() error = %v", err)
	}
	if _, err := resolver.ResolveInvocationMCP(context.Background(), prep); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("ResolveInvocationMCP() after revoke error = %v, want forbidden", err)
	}
	if _, err := auth.NewAuthenticator(authStore, func() time.Time { return now.Add(2 * time.Minute) }).AuthenticateAgentToken(context.Background(), first.BearerToken); !kernel.IsCode(err, kernel.CodeUnauthorized) {
		t.Fatalf("revoked leaked token error = %v, want unauthorized", err)
	}
}

func TestUnavailablePhaseBoundaryValidatesBindingThenFailsClosed(t *testing.T) {
	now := time.Date(2026, time.August, 11, 13, 0, 0, 0, time.UTC)
	invocations := runtimepkg.NewMemoryInvocationStore()
	invocation := validProductionInvocation(now, auth.RoleExecutor)
	invocation.TaskID = "task-a"
	invocation.EndpointID = "execute"
	invocation.Generation = 2
	invocation.BindingRef = "binding-a"
	invocation.LeaseID = "lease-a"
	if err := invocations.Create(context.Background(), invocation); err != nil {
		t.Fatal(err)
	}
	boundary := &productionUnavailablePhaseBoundary{invocations: invocations}
	err := boundary.Apply(context.Background(), coordination.PhaseCommand{Endpoint: coordination.PhaseEndpointRef{TaskID: "task-a", EndpointID: coordination.EndpointExecute}, Generation: 2, BindingRef: "binding-a", LeaseRef: "lease-a", Action: coordination.CommandStart})
	if !kernel.IsCode(err, kernel.CodeExecutorUnavailable) {
		t.Fatalf("matching binding error = %v, want executor_unavailable", err)
	}
	err = boundary.Apply(context.Background(), coordination.PhaseCommand{Endpoint: coordination.PhaseEndpointRef{TaskID: "task-a", EndpointID: coordination.EndpointExecute}, Generation: 2, BindingRef: "other", LeaseRef: "lease-a", Action: coordination.CommandStart})
	if !kernel.IsCode(err, kernel.CodeStaleBinding) {
		t.Fatalf("mismatched binding error = %v, want stale_binding", err)
	}
}

func TestProductionMCPResolverRejectsCompletedInvocation(t *testing.T) {
	now := time.Date(2026, time.August, 11, 13, 0, 0, 0, time.UTC)
	invocations := runtimepkg.NewMemoryInvocationStore()
	invocation := validProductionInvocation(now, auth.RoleTaskManager)
	invocation.Status = runtimepkg.InvocationCompleted
	if err := invocations.Create(context.Background(), invocation); err != nil {
		t.Fatal(err)
	}
	resolver, err := newProductionMCPResolver(auth.NewMemoryStore(), invocations, "http://threadmill.internal/mcp", []byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	resolver.now = func() time.Time { return now }
	if _, err := resolver.ResolveInvocationMCP(context.Background(), agentteams.HostPreparation{InvocationID: invocation.ID, Role: invocation.Role}); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("completed invocation resolver error = %v, want forbidden", err)
	}
}

func TestProductionRuntimeLoopObservesWorkersBeforeReconcile(t *testing.T) {
	now := time.Date(2026, time.August, 11, 13, 0, 0, 0, time.UTC)
	capacity := &recordingProductionCapacity{}
	runner := &recordingProductionRunner{}
	loop := newProductionRuntimeLoop(staticProductionHosts{hosts: []agentteams.HostStatus{
		{Ref: "worker-a", Kind: agentteams.HostWorker, Phase: "Running", LastHeartbeat: now, Capacity: 1, ActiveExecutions: 1},
		{Ref: "manager-a", Kind: agentteams.HostManager, Phase: "Running", LastHeartbeat: now, Capacity: 1},
	}}, capacity, runner, productionTestProbe{}, true, func() time.Time { return now })
	loop.step(context.Background())
	if capacity.healthy != 1 || capacity.active != 1 || runner.calls != 1 {
		t.Fatalf("runtime step capacity=(%d,%d) reconcile=%d", capacity.healthy, capacity.active, runner.calls)
	}
	if err := loop.Check(context.Background()); err != nil {
		t.Fatalf("runtime readiness = %v, want ready", err)
	}
}

func TestProductionRuntimeLoopRetriesTerminalBeforeReconcile(t *testing.T) {
	now := time.Date(2026, time.August, 11, 13, 0, 0, 0, time.UTC)
	runner := &recordingProductionRunner{}
	retryErr := errors.New("terminal retry unavailable")
	retryCalls := 0
	loop := newProductionRuntimeLoop(staticProductionHosts{}, &recordingProductionCapacity{}, runner, productionTestProbe{}, true, func() time.Time { return now })
	loop.terminalRetry = func(context.Context) error {
		retryCalls++
		if retryCalls == 1 {
			return retryErr
		}
		return nil
	}

	loop.step(context.Background())
	if retryCalls != 1 || runner.calls != 0 {
		t.Fatalf("failed terminal retry calls=%d reconcile=%d, want retry only", retryCalls, runner.calls)
	}
	if err := loop.Check(context.Background()); !errors.Is(err, retryErr) {
		t.Fatalf("runtime readiness after retry failure = %v, want retry err", err)
	}

	loop.step(context.Background())
	if retryCalls != 2 || runner.calls != 1 {
		t.Fatalf("recovered terminal retry calls=%d reconcile=%d, want retry then reconcile", retryCalls, runner.calls)
	}
	if err := loop.Check(context.Background()); err != nil {
		t.Fatalf("runtime readiness after retry recovery = %v, want ready", err)
	}
}

func TestProductionRuntimeLoopCloseCancelsTerminalRetry(t *testing.T) {
	loop := newProductionRuntimeLoop(staticProductionHosts{}, &recordingProductionCapacity{}, &recordingProductionRunner{}, productionTestProbe{}, true, time.Now)
	entered := make(chan struct{})
	loop.terminalRetry = func(ctx context.Context) error {
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	}
	loop.Start(context.Background())
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("terminal retry was not entered")
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := loop.Close(closeCtx); err != nil {
		t.Fatalf("Close() = %v, want cancelled terminal retry to exit", err)
	}
}

func validProductionInvocation(now time.Time, role auth.Role) runtimepkg.Invocation {
	return runtimepkg.Invocation{
		ID: "invocation-a", ActorPrincipalID: "agent-a", ProjectID: "project-a", Role: role,
		Status: runtimepkg.InvocationPrepared, PromptHashes: map[string]string{"prompt": "hash"},
		SkillHashes:    map[string]string{"skill": "hash"},
		EffectiveTools: []auth.Tool{auth.ToolCoordinationSnapshot, auth.ToolCoordinationTransition},
		CreatedAt:      now, ExpiresAt: now.Add(time.Hour),
	}
}

type staticProductionHosts struct {
	hosts []agentteams.HostStatus
	err   error
}

func (s staticProductionHosts) ListHosts(context.Context) ([]agentteams.HostStatus, error) {
	return s.hosts, s.err
}

type recordingProductionCapacity struct{ healthy, active int }

func (r *recordingProductionCapacity) Observe(_ context.Context, healthy, active int) (scheduler.Capacity, error) {
	r.healthy, r.active = healthy, active
	return scheduler.Capacity{Healthy: healthy, Active: active}, nil
}

type recordingProductionRunner struct {
	calls int
	err   error
}

func (r *recordingProductionRunner) Reconcile(context.Context) error {
	r.calls++
	return r.err
}

func TestProductionRuntimeLoopReportsPhaseDependency(t *testing.T) {
	phaseErr := errors.New("phase unavailable")
	loop := newProductionRuntimeLoop(staticProductionHosts{}, &recordingProductionCapacity{}, &recordingProductionRunner{}, productionTestProbe{err: phaseErr}, false, time.Now)
	if err := loop.Check(context.Background()); !errors.Is(err, phaseErr) {
		t.Fatalf("runtime Check() = %v, want phase dependency error", err)
	}
}

func TestProductionInvocationSourceRoutesPhaseRefsToPhaseStore(t *testing.T) {
	phaseSource := &recordingInvocationSource{prepared: agentteams.PreparedInvocation{InvocationID: "phase-inv", ProjectID: "project-a", Role: auth.RoleExecutor, RoomID: "room", Spec: "phase", RuntimeConfigRef: "runtime", EnvelopeRef: "envelope"}}
	taskManagerSource := &recordingInvocationSource{prepared: agentteams.PreparedInvocation{InvocationID: "tm-inv", ProjectID: "project-a", Role: auth.RoleTaskManager, RoomID: "room", Spec: "tm", RuntimeConfigRef: "runtime", EnvelopeRef: "envelope"}}
	source := productionInvocationSource{taskManager: taskManagerSource, phase: phaseSource}

	if prepared, err := source.LoadPreparedInvocation(context.Background(), "threadmill://phase-invocation/phase-inv/hash"); err != nil || prepared.InvocationID != "phase-inv" {
		t.Fatalf("phase source prepared=%#v err=%v", prepared, err)
	}
	if prepared, err := source.LoadPreparedInvocation(context.Background(), "tm-inv"); err != nil || prepared.InvocationID != "tm-inv" {
		t.Fatalf("task manager source prepared=%#v err=%v", prepared, err)
	}
	if phaseSource.calls != 1 || taskManagerSource.calls != 1 {
		t.Fatalf("source calls phase=%d taskManager=%d", phaseSource.calls, taskManagerSource.calls)
	}
}

func TestValidateProductionWorkspacePathsFailsClosed(t *testing.T) {
	repository := t.TempDir()
	worktrees := t.TempDir()
	if err := validateProductionWorkspacePaths(repository, worktrees); err != nil {
		t.Fatalf("existing workspace paths rejected: %v", err)
	}
	if err := validateProductionWorkspacePaths(repository, worktrees+"-missing"); err == nil {
		t.Fatal("missing worktree parent accepted")
	}
}

type recordingInvocationSource struct {
	calls    int
	prepared agentteams.PreparedInvocation
	err      error
}

func (s *recordingInvocationSource) LoadPreparedInvocation(context.Context, string) (agentteams.PreparedInvocation, error) {
	s.calls++
	return s.prepared, s.err
}
