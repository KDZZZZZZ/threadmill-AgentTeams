package app

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/adapters/agentteams"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/evidence"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/mergequeue"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	runtimepkg "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime"
	phasepkg "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime/phase"
)

func TestProductionTargetedVerifyMonitorRequiresObservedActivityBeforeIdleFailure(t *testing.T) {
	now := time.Date(2026, 8, 12, 7, 0, 0, 0, time.UTC)
	target := productionTargetedVerifyExecutionTarget{
		execution: agentteams.AgentTeamsExecutionRef{AgentTeamsTaskID: "targeted-provider-a"},
		startedAt: now.Add(-2 * time.Minute),
	}
	monitor := &productionTargetedVerifyExecutionMonitor{
		activeAt: make(map[string]time.Time), idleSince: make(map[string]time.Time),
		cleaned: make(map[string]struct{}),
	}
	if monitor.executionAbandoned(target, agentteams.HostActivity{Status: "idle"}, now) {
		t.Fatal("cold idle targeted verify without prior activity was abandoned")
	}
	target.startedAt = now.Add(-productionPhaseExecutionColdQuiescenceGap - time.Second)
	if !monitor.executionAbandoned(target, agentteams.HostActivity{Status: "idle"}, now) {
		t.Fatal("cold idle targeted verify beyond the recovery grace was not abandoned")
	}
	target.startedAt = now.Add(-2 * time.Minute)
	if monitor.executionAbandoned(target, agentteams.HostActivity{Status: "running", RunningTaskCount: 1}, now) {
		t.Fatal("running targeted verify was abandoned")
	}
	if monitor.executionAbandoned(target, agentteams.HostActivity{Status: "idle"}, now.Add(10*time.Second)) {
		t.Fatal("recently idle targeted verify was abandoned before quiescence gap")
	}
	if monitor.executionAbandoned(target, agentteams.HostActivity{Status: "idle"}, now.Add(time.Minute)) {
		t.Fatal("one-minute pause between targeted verifier turns was treated as abandoned")
	}
	if !monitor.executionAbandoned(target, agentteams.HostActivity{Status: "idle"}, now.Add(productionPhaseExecutionQuiescenceGap+11*time.Second)) {
		t.Fatal("continuously idle targeted verify with prior activity was not abandoned")
	}

	historical := productionTargetedVerifyExecutionTarget{
		execution: agentteams.AgentTeamsExecutionRef{AgentTeamsTaskID: "targeted-provider-history"},
		startedAt: now.Add(-10 * time.Minute),
	}
	activity := agentteams.HostActivity{
		Status: "idle", LastRunAt: now.Add(-productionPhaseExecutionQuiescenceGap), LastFinishAt: now.Add(-productionPhaseExecutionQuiescenceGap - time.Second),
	}
	if !monitor.executionAbandoned(historical, activity, now) {
		t.Fatal("historical run timestamp kept an already quiescent targeted verify alive")
	}
}

func TestProductionTargetedVerifyMonitorFailsOnlyRegisteredProjectDispatchedExecutions(t *testing.T) {
	databaseURL := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("THREADMILL_TEST_DATABASE_URL is required for the real PostgreSQL integration test")
	}
	ctx := context.Background()
	db := openProductionPhasePostgres(t, ctx, databaseURL)
	defer db.Close()
	projectID := kernel.ProjectID("project-targeted-monitor")
	registry := newProductionTargetedVerifyRegistry()
	targeted := registerTargetedMonitorInvocation(t, ctx, db, registry, projectID, "candidate-targeted", runtimepkg.InvocationRunning, true, auth.RoleVerifier)
	_ = registerTargetedMonitorInvocation(t, ctx, db, registry, kernel.ProjectID("project-targeted-other"), "candidate-other-project", runtimepkg.InvocationRunning, true, auth.RoleVerifier)
	_ = registerTargetedMonitorInvocation(t, ctx, db, registry, projectID, "candidate-not-dispatched", runtimepkg.InvocationRunning, false, auth.RoleVerifier)
	_ = registerTargetedMonitorInvocation(t, ctx, db, registry, projectID, "candidate-wrong-role", runtimepkg.InvocationRunning, true, auth.RoleExecutor)
	provider := &recordingPhaseExecutionProvider{terminal: false, activity: agentteams.HostActivity{Status: "running", RunningTaskCount: 1}}
	runtime := &recordingTargetedVerifyFailureRuntime{}
	monitor, err := newProductionTargetedVerifyExecutionMonitor(db, projectID, registry, provider, runtime, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := monitor.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if len(runtime.failed) != 0 {
		t.Fatalf("provider in_progress while QwenPaw active failed invocations = %#v", runtime.failed)
	}
	provider.terminal = true
	if err := monitor.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if len(runtime.failed) != 1 || runtime.failed[0] != targeted {
		t.Fatalf("failed targeted invocations = %#v, want only %q", runtime.failed, targeted)
	}
	if len(provider.executions) != 2 || provider.executions[0].InvocationID != targeted || provider.executions[1].InvocationID != targeted {
		t.Fatalf("provider probes = %#v, want only %q", provider.executions, targeted)
	}
}

func TestProductionTargetedVerifyMonitorReclaimsOnlyExpiredReservedExecutions(t *testing.T) {
	databaseURL := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("THREADMILL_TEST_DATABASE_URL is required for the real PostgreSQL integration test")
	}
	ctx := context.Background()
	db := openProductionPhasePostgres(t, ctx, databaseURL)
	defer db.Close()
	projectID := kernel.ProjectID("project-targeted-reserved-expired")
	registry := newProductionTargetedVerifyRegistry()
	expired := registerTargetedMonitorInvocation(t, ctx, db, registry, projectID, "candidate-expired-reserved", runtimepkg.InvocationPrepared, true, auth.RoleVerifier)
	fresh := registerTargetedMonitorInvocation(t, ctx, db, registry, projectID, "candidate-fresh-reserved", runtimepkg.InvocationPrepared, true, auth.RoleVerifier)
	if _, err := db.ExecContext(ctx, `
UPDATE agentteams_execution_refs
SET state='reserved'
WHERE invocation_id IN ($1,$2)`, expired, fresh); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
UPDATE runtime_invocations
SET expires_at = CASE WHEN invocation_id=$1 THEN now()-interval '1 minute' ELSE now()+interval '1 hour' END
WHERE invocation_id IN ($1,$2)`, expired, fresh); err != nil {
		t.Fatal(err)
	}
	provider := &recordingPhaseExecutionProvider{terminal: false, activity: agentteams.HostActivity{Status: "running", RunningTaskCount: 1}}
	runtime := &recordingTargetedVerifyFailureRuntime{}
	monitor, err := newProductionTargetedVerifyExecutionMonitor(db, projectID, registry, provider, runtime, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := monitor.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if len(runtime.failed) != 1 || runtime.failed[0] != expired {
		t.Fatalf("failed targeted invocations = %#v, want only expired reserved %q", runtime.failed, expired)
	}
	if len(provider.terminated) != 1 || provider.terminated[0].InvocationID != expired {
		t.Fatalf("terminated executions = %#v, want only expired reserved %q", provider.terminated, expired)
	}
	if len(provider.executions) != 0 {
		t.Fatalf("reserved cleanup probed provider terminal/activity: %#v", provider.executions)
	}
}

func TestProductionTargetedVerifyMonitorReclaimsExpiredUnregisteredTargetedInvocations(t *testing.T) {
	databaseURL := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("THREADMILL_TEST_DATABASE_URL is required for the real PostgreSQL integration test")
	}
	ctx := context.Background()
	db := openProductionPhasePostgres(t, ctx, databaseURL)
	defer db.Close()
	projectID := kernel.ProjectID("project-targeted-unregistered-expired")
	expiredOrphan := createUnregisteredTargetedInvocation(t, ctx, db, projectID, "expired-orphan", runtimepkg.InvocationPrepared, false, true)
	expiredReserved := createUnregisteredTargetedInvocation(t, ctx, db, projectID, "expired-reserved", runtimepkg.InvocationPrepared, true, true)
	freshOrphan := createUnregisteredTargetedInvocation(t, ctx, db, projectID, "fresh-orphan", runtimepkg.InvocationPrepared, false, false)
	regularExpired := createUnregisteredTargetedInvocation(t, ctx, db, projectID, "regular-expired", runtimepkg.InvocationPrepared, false, true)
	if _, err := db.ExecContext(ctx, `UPDATE runtime_invocations SET binding_ref='binding:regular-expired' WHERE invocation_id=$1`, regularExpired); err != nil {
		t.Fatal(err)
	}
	provider := &postgresTerminatingPhaseExecutionProvider{
		recordingPhaseExecutionProvider: recordingPhaseExecutionProvider{terminal: false, activity: agentteams.HostActivity{Status: "running", RunningTaskCount: 1}},
		executionStore:                  agentteams.NewPostgresExecutionStore(db),
		slots:                           agentteams.NewHostSlotStore(db),
	}
	runtime := &productionTargetedVerifyPhaseRuntime{
		projectID:   projectID,
		invocations: runtimepkg.NewPostgresInvocationStoreFromSQL(db),
		registry:    newProductionTargetedVerifyRegistry(),
	}
	monitor, err := newProductionTargetedVerifyExecutionMonitor(db, projectID, newProductionTargetedVerifyRegistry(), provider, runtime, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := monitor.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	assertRuntimeStatus(t, ctx, db, expiredOrphan, runtimepkg.InvocationFailed)
	assertRuntimeStatus(t, ctx, db, expiredReserved, runtimepkg.InvocationFailed)
	assertRuntimeStatus(t, ctx, db, freshOrphan, runtimepkg.InvocationPrepared)
	assertRuntimeStatus(t, ctx, db, regularExpired, runtimepkg.InvocationPrepared)
	if len(provider.terminated) != 1 || provider.terminated[0].InvocationID != expiredReserved {
		t.Fatalf("terminated executions = %#v, want only reserved expired targeted %q", provider.terminated, expiredReserved)
	}
	if len(provider.executions) != 0 {
		t.Fatalf("unregistered expired cleanup probed provider terminal/activity: %#v", provider.executions)
	}
	var state, mode string
	var revoked, released bool
	if err := db.QueryRowContext(ctx, `
SELECT state, COALESCE(termination_mode,''), mcp_revoked_at IS NOT NULL, host_slot_released_at IS NOT NULL
FROM agentteams_execution_refs
WHERE invocation_id=$1`, expiredReserved).Scan(&state, &mode, &revoked, &released); err != nil {
		t.Fatal(err)
	}
	if state != "terminated" || mode != string(agentteams.TerminateCancel) || !revoked || !released {
		t.Fatalf("expired reserved execution state=%q mode=%q revoked=%v released=%v, want terminated/cancel/revoked/released", state, mode, revoked, released)
	}
}

func TestProductionTargetedVerifyMonitorRetriesTerminalRuntimeReservedExecutionCleanup(t *testing.T) {
	databaseURL := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("THREADMILL_TEST_DATABASE_URL is required for the real PostgreSQL integration test")
	}
	ctx := context.Background()
	db := openProductionPhasePostgres(t, ctx, databaseURL)
	defer db.Close()
	projectID := kernel.ProjectID("project-targeted-terminal-reserved-retry")
	expiredReserved := createUnregisteredTargetedInvocation(t, ctx, db, projectID, "terminal-reserved-retry", runtimepkg.InvocationPrepared, true, true)
	provider := &failOncePostgresTerminatingPhaseExecutionProvider{
		delegate: postgresTerminatingPhaseExecutionProvider{
			recordingPhaseExecutionProvider: recordingPhaseExecutionProvider{terminal: false, activity: agentteams.HostActivity{Status: "running", RunningTaskCount: 1}},
			executionStore:                  agentteams.NewPostgresExecutionStore(db),
			slots:                           agentteams.NewHostSlotStore(db),
		},
		err: kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "AgentTeams host not found", Recoverable: true},
	}
	registry := newProductionTargetedVerifyRegistry()
	runtime := &productionTargetedVerifyPhaseRuntime{
		projectID:   projectID,
		invocations: runtimepkg.NewPostgresInvocationStoreFromSQL(db),
		registry:    registry,
	}
	monitor, err := newProductionTargetedVerifyExecutionMonitor(db, projectID, registry, provider, runtime, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := monitor.Reconcile(ctx); !kernel.IsCode(err, kernel.CodeExecutorUnavailable) {
		t.Fatalf("first Reconcile error = %v, want executor_unavailable from provider cleanup", err)
	}
	assertRuntimeStatus(t, ctx, db, expiredReserved, runtimepkg.InvocationFailed)
	assertAgentTeamsExecutionState(t, ctx, db, expiredReserved, "reserved", "", false, false)
	// Merge Queue may restore ownership after the first cleanup attempt. A
	// terminal Runtime invocation with a live reservation must still be retried.
	registry.byInvocation[expiredReserved] = "historical-targeted-command"
	if err := monitor.Reconcile(ctx); err != nil {
		t.Fatalf("second Reconcile should retry terminal-runtime reserved cleanup: %v", err)
	}
	assertRuntimeStatus(t, ctx, db, expiredReserved, runtimepkg.InvocationFailed)
	assertAgentTeamsExecutionState(t, ctx, db, expiredReserved, "terminated", string(agentteams.TerminateCancel), true, true)
	if len(provider.delegate.terminated) != 2 || provider.delegate.terminated[0].InvocationID != expiredReserved || provider.delegate.terminated[1].InvocationID != expiredReserved {
		t.Fatalf("provider terminate attempts = %#v, want two retries for %q", provider.delegate.terminated, expiredReserved)
	}
}

func TestProductionTargetedVerifyMonitorCleansRuntimeTerminalOnlyWhenProviderSafe(t *testing.T) {
	databaseURL := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("THREADMILL_TEST_DATABASE_URL is required for the real PostgreSQL integration test")
	}
	ctx := context.Background()
	db := openProductionPhasePostgres(t, ctx, databaseURL)
	defer db.Close()
	projectID := kernel.ProjectID("project-targeted-cleanup")
	registry := newProductionTargetedVerifyRegistry()
	completed := registerTargetedMonitorInvocation(t, ctx, db, registry, projectID, "candidate-completed", runtimepkg.InvocationCompleted, true, auth.RoleVerifier)
	running := registerTargetedMonitorInvocation(t, ctx, db, registry, projectID, "candidate-running", runtimepkg.InvocationRunning, true, auth.RoleVerifier)
	registry.MarkTerminal(completed)
	provider := &recordingPhaseExecutionProvider{terminal: false, activity: agentteams.HostActivity{Status: "running", RunningTaskCount: 1}}
	runtime := &recordingTargetedVerifyFailureRuntime{}
	monitor, err := newProductionTargetedVerifyExecutionMonitor(db, projectID, registry, provider, runtime, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := monitor.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if len(provider.terminated) != 0 {
		t.Fatalf("terminal targeted verify was terminated while provider still active: %#v", provider.terminated)
	}
	if len(provider.finalized) != 1 || provider.finalized[0].InvocationID != completed {
		t.Fatalf("provider finalizations = %#v, want completed %q", provider.finalized, completed)
	}
	provider.terminal = true
	if err := monitor.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if len(provider.terminated) != 1 || provider.terminated[0].InvocationID != completed {
		t.Fatalf("terminated executions = %#v, want completed %q", provider.terminated, completed)
	}
	if len(runtime.failed) != 1 || runtime.failed[0] != running {
		t.Fatalf("active running invocation failure = %#v, want provider-terminal running %q", runtime.failed, running)
	}
	if !registry.OwnsInvocation(completed) {
		t.Fatal("cleanup forgot completed targeted invocation before Merge Queue can read the receipt")
	}
}

func registerTargetedMonitorInvocation(t *testing.T, ctx context.Context, db *sql.DB, registry *productionTargetedVerifyRegistry, projectID kernel.ProjectID, candidate string, status runtimepkg.InvocationStatus, dispatched bool, role auth.Role) kernel.InvocationID {
	t.Helper()
	req := mergequeue.TargetedVerifyRequest{
		Candidate: mergequeue.Candidate{
			ID:                mergequeue.CandidateID(candidate),
			ProjectID:         projectID,
			TaskID:            kernel.TaskID("task-" + candidate),
			TargetRepository:  "repo",
			TargetBranch:      "main",
			CandidateRevision: "candidate-revision",
			VerifyResultRef:   evidence.ArtifactID("art-verify-" + candidate),
			DiffArtifactRef:   evidence.ArtifactID("art-diff-" + candidate),
		},
		WorkspaceRoot:      "C:/tmp/" + candidate,
		LatestMainRevision: "latest-main-revision",
	}
	command := coordination.PhaseCommand{
		ID: "cmd:targeted-monitor:" + candidate,
		Endpoint: coordination.PhaseEndpointRef{
			TaskID:     req.Candidate.TaskID,
			EndpointID: coordination.EndpointVerify,
		},
		Generation: 1,
		BindingRef: kernel.BindingRef("binding:" + candidate),
		LeaseRef:   kernel.LeaseID("lease:" + candidate),
		Action:     coordination.CommandStart,
		CauseRef:   "test",
	}
	binding := phasepkg.BindingSnapshot{
		ProjectID:        projectID,
		ActorPrincipalID: kernel.ActorPrincipalID("actor:" + candidate),
		TaskID:           req.Candidate.TaskID,
		EndpointID:       coordination.EndpointVerify,
		Generation:       command.Generation,
		BindingRef:       command.BindingRef,
		LeaseRef:         command.LeaseRef,
		WorkspaceRef:     req.WorkspaceRoot,
	}
	if err := registry.RegisterBinding(ctx, req, command, binding); err != nil {
		t.Fatalf("register targeted binding: %v", err)
	}
	invocationID := deterministicPhaseInvocationID(command.ID)
	created := time.Now().UTC().Add(-time.Minute)
	if err := runtimepkg.NewPostgresInvocationStoreFromSQL(db).Create(ctx, runtimepkg.Invocation{
		ID: invocationID, ActorPrincipalID: kernel.ActorPrincipalID("actor:" + candidate), ProjectID: projectID,
		TaskID: req.Candidate.TaskID, EndpointID: coordination.EndpointVerify, Generation: 1,
		Role: role, Status: status, BindingRef: command.BindingRef, LeaseID: command.LeaseRef,
		PromptHashes: map[string]string{"prompt": "hash"}, SkillHashes: map[string]string{"skill": "hash"},
		EffectiveTools: []auth.Tool{auth.ToolAgentSubmitPhaseOutput}, CreatedAt: created, ExpiresAt: created.Add(time.Hour),
	}); err != nil {
		t.Fatalf("create runtime invocation: %v", err)
	}
	if dispatched {
		invocationRef := "threadmill://targeted/" + string(invocationID)
		taskID := "agentteams-" + candidate
		if _, err := db.ExecContext(ctx, `
INSERT INTO phase_agentteams_host_states(invocation_id, invocation_ref, agentteams_task_id, host_ref)
VALUES ($1,$2,$3,'phase-a')`, invocationID, invocationRef, taskID); err != nil {
			t.Fatalf("insert host state: %v", err)
		}
		if _, err := db.ExecContext(ctx, `
INSERT INTO agentteams_execution_refs(invocation_ref, invocation_id, agentteams_task_id, host_ref, dispatch_fingerprint, state, attempt, created_at, updated_at)
VALUES ($1,$2,$3,'phase-a','fingerprint','dispatched',1,$4,$4)`, invocationRef, invocationID, taskID, created); err != nil {
			t.Fatalf("insert execution ref: %v", err)
		}
	}
	return invocationID
}

func createUnregisteredTargetedInvocation(t *testing.T, ctx context.Context, db *sql.DB, projectID kernel.ProjectID, suffix string, status runtimepkg.InvocationStatus, reservedExecution bool, expired bool) kernel.InvocationID {
	t.Helper()
	invocationID := kernel.InvocationID("inv-unregistered-" + suffix)
	created := time.Now().UTC().Add(-2 * time.Hour)
	expiresAt := time.Now().UTC().Add(time.Hour)
	if expired {
		expiresAt = time.Now().UTC().Add(-time.Minute)
	}
	if err := runtimepkg.NewPostgresInvocationStoreFromSQL(db).Create(ctx, runtimepkg.Invocation{
		ID: invocationID, ActorPrincipalID: kernel.ActorPrincipalID("actor:" + suffix), ProjectID: projectID,
		TaskID: kernel.TaskID("task-" + suffix), EndpointID: coordination.EndpointVerify, Generation: 1,
		Role: auth.RoleVerifier, Status: status,
		BindingRef: kernel.BindingRef("targeted-verify-binding:" + suffix), LeaseID: kernel.LeaseID("targeted-verify-lease:" + suffix),
		PromptHashes: map[string]string{"prompt": "hash"}, SkillHashes: map[string]string{"skill": "hash"},
		EffectiveTools: []auth.Tool{auth.ToolAgentSubmitPhaseOutput}, CreatedAt: created, ExpiresAt: expiresAt,
	}); err != nil {
		t.Fatalf("create unregistered targeted invocation: %v", err)
	}
	if reservedExecution {
		invocationRef := "threadmill://targeted/" + string(invocationID)
		taskID := "agentteams-" + suffix
		hostRef := "phase-" + suffix
		if _, err := db.ExecContext(ctx, `
INSERT INTO phase_agentteams_host_states(invocation_id, invocation_ref, agentteams_task_id, host_ref)
VALUES ($1,$2,$3,$4)`, invocationID, invocationRef, taskID, hostRef); err != nil {
			t.Fatalf("insert unregistered host state: %v", err)
		}
		if _, err := db.ExecContext(ctx, `
INSERT INTO agentteams_execution_refs(invocation_ref, invocation_id, agentteams_task_id, host_ref, dispatch_fingerprint, state, attempt, created_at, updated_at)
VALUES ($1,$2,$3,$4,'fingerprint','reserved',1,$5,$5)`, invocationRef, invocationID, taskID, hostRef, created); err != nil {
			t.Fatalf("insert unregistered execution ref: %v", err)
		}
		if err := agentteams.NewHostSlotStore(db).Claim(ctx, hostRef, invocationID, "mcp-"+suffix, []byte("token-hash-"+suffix), "token-"+suffix); err != nil {
			t.Fatalf("claim unregistered host slot: %v", err)
		}
	}
	return invocationID
}

func assertRuntimeStatus(t *testing.T, ctx context.Context, db *sql.DB, invocationID kernel.InvocationID, want runtimepkg.InvocationStatus) {
	t.Helper()
	var got string
	if err := db.QueryRowContext(ctx, `SELECT status FROM runtime_invocations WHERE invocation_id=$1`, invocationID).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if runtimepkg.InvocationStatus(got) != want {
		t.Fatalf("runtime invocation %q status=%q, want %q", invocationID, got, want)
	}
}

func assertAgentTeamsExecutionState(t *testing.T, ctx context.Context, db *sql.DB, invocationID kernel.InvocationID, wantState, wantMode string, wantRevoked, wantReleased bool) {
	t.Helper()
	var state, mode string
	var revoked, released bool
	if err := db.QueryRowContext(ctx, `
SELECT state, COALESCE(termination_mode,''), mcp_revoked_at IS NOT NULL, host_slot_released_at IS NOT NULL
FROM agentteams_execution_refs
WHERE invocation_id=$1`, invocationID).Scan(&state, &mode, &revoked, &released); err != nil {
		t.Fatal(err)
	}
	if state != wantState || mode != wantMode || revoked != wantRevoked || released != wantReleased {
		t.Fatalf("execution %q state=%q mode=%q revoked=%v released=%v, want %q/%q/%v/%v", invocationID, state, mode, revoked, released, wantState, wantMode, wantRevoked, wantReleased)
	}
}

type postgresTerminatingPhaseExecutionProvider struct {
	recordingPhaseExecutionProvider
	executionStore *agentteams.PostgresExecutionStore
	slots          *agentteams.HostSlotStore
}

func (p *postgresTerminatingPhaseExecutionProvider) Terminate(ctx context.Context, execution agentteams.AgentTeamsExecutionRef, mode string) error {
	p.terminated = append(p.terminated, execution)
	if err := p.slots.MarkRevoked(ctx, execution.AgentTeamsTaskID, execution.HostRef); err != nil && !kernel.IsCode(err, kernel.CodeNotFound) {
		return err
	}
	if err := p.slots.Release(ctx, execution.AgentTeamsTaskID, execution.HostRef); err != nil && !kernel.IsCode(err, kernel.CodeNotFound) {
		return err
	}
	return p.executionStore.MarkTerminated(ctx, execution.AgentTeamsTaskID, agentteams.TerminateMode(mode))
}

type failOncePostgresTerminatingPhaseExecutionProvider struct {
	delegate postgresTerminatingPhaseExecutionProvider
	err      error
	calls    int
}

func (p *failOncePostgresTerminatingPhaseExecutionProvider) ExecutionTerminal(ctx context.Context, execution agentteams.AgentTeamsExecutionRef) (bool, error) {
	return p.delegate.ExecutionTerminal(ctx, execution)
}

func (p *failOncePostgresTerminatingPhaseExecutionProvider) ExecutionActivity(ctx context.Context, execution agentteams.AgentTeamsExecutionRef) (agentteams.HostActivity, error) {
	return p.delegate.ExecutionActivity(ctx, execution)
}

func (p *failOncePostgresTerminatingPhaseExecutionProvider) Terminate(ctx context.Context, execution agentteams.AgentTeamsExecutionRef, mode string) error {
	p.calls++
	if p.calls == 1 {
		p.delegate.terminated = append(p.delegate.terminated, execution)
		return p.err
	}
	return p.delegate.Terminate(ctx, execution, mode)
}

func (p *failOncePostgresTerminatingPhaseExecutionProvider) FinalizeExecution(ctx context.Context, execution agentteams.AgentTeamsExecutionRef, summary string) error {
	return p.delegate.FinalizeExecution(ctx, execution, summary)
}

type recordingTargetedVerifyFailureRuntime struct {
	failed []kernel.InvocationID
}

func (r *recordingTargetedVerifyFailureRuntime) FailTargetedInvocation(_ context.Context, invocationID kernel.InvocationID) error {
	r.failed = append(r.failed, invocationID)
	return nil
}
