package phase

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	adapter "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/adapters/agentteams"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	platformpostgres "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/postgres"
	baseruntime "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime/promptcatalog"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/migrations"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestAgentTeamsPhaseHostDispatchPersistsPreparedBeforeDispatch(t *testing.T) {
	host, service, writer, _ := newAgentTeamsPhaseHostHarness(t)
	req := validAgentTeamsDispatchRequest("inv-dispatch")

	if err := host.Dispatch(context.Background(), req); err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}

	invocationRef, err := agentTeamsInvocationRef(req, false)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := service.calls[0], "dispatch:"+invocationRef; got != want {
		t.Fatalf("first adapter call = %q, want %q", got, want)
	}
	prepared, err := writer.LoadPreparedInvocation(context.Background(), invocationRef)
	if err != nil {
		t.Fatalf("prepared invocation was not saved: %v", err)
	}
	if prepared.InvocationID != req.Invocation.ID || prepared.ProjectID != req.Invocation.ProjectID || prepared.Role != auth.RoleExecutor {
		t.Fatalf("prepared identity = %#v", prepared)
	}
	if prepared.RuntimeConfigRef == "" || prepared.EnvelopeRef == "" || strings.Contains(prepared.RuntimeConfigRef, "secret") || strings.Contains(prepared.EnvelopeRef, "secret") {
		t.Fatalf("prepared refs must be stable non-secret values: %#v", prepared)
	}
	if got := prepared.RequiredCapabilities; len(got) != 1 || got[0] != "shell" {
		t.Fatalf("required AgentTeams capabilities = %#v, want shell", got)
	}
	var spec agentTeamsPhaseSpecDocument
	if err := json.Unmarshal([]byte(prepared.Spec), &spec); err != nil {
		t.Fatalf("prepared spec is not JSON: %v", err)
	}
	if spec.Prompt != req.Prompt.Text || spec.WorkspaceRevision != req.Binding.WorkspaceRevision {
		t.Fatalf("prepared spec = %#v", spec)
	}
}

func TestAgentTeamsPhaseHostDispatchReplayKeepsOriginalExecutionAcrossRerender(t *testing.T) {
	host, service, writer, state := newAgentTeamsPhaseHostHarness(t)
	req := validAgentTeamsDispatchRequest("inv-dispatch-replay")
	if err := host.Dispatch(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	original, ok, err := state.LoadAgentTeamsPhaseHostState(context.Background(), req.Invocation.ID)
	if err != nil || !ok {
		t.Fatalf("load original state = ok %v err %v", ok, err)
	}

	replayed := req
	replayed.Prompt.Text = "a newly rendered prompt must not create another provider execution"
	replayed.Prompt.SHA256 = "new-render-hash"
	replayed.Binding.WorkspaceRevision = "new-binding-revision"
	replayed.Start.Inputs.InputRevision = "new-input-revision"
	if err := host.Dispatch(context.Background(), replayed); err != nil {
		t.Fatalf("Dispatch() replay error = %v", err)
	}
	current, ok, err := state.LoadAgentTeamsPhaseHostState(context.Background(), req.Invocation.ID)
	if err != nil || !ok || current != original {
		t.Fatalf("replayed state = %#v, ok %v err %v; want %#v", current, ok, err, original)
	}
	if service.nextAttempt != 2 {
		t.Fatalf("provider attempts = %d, want exactly one execution", service.nextAttempt-1)
	}
	prepared, err := writer.LoadPreparedInvocation(context.Background(), original.InvocationRef)
	if err != nil {
		t.Fatal(err)
	}
	var spec agentTeamsPhaseSpecDocument
	if err := json.Unmarshal([]byte(prepared.Spec), &spec); err != nil {
		t.Fatal(err)
	}
	if spec.Prompt != req.Prompt.Text || spec.WorkspaceRevision != req.Binding.WorkspaceRevision {
		t.Fatalf("immutable prepared invocation changed on replay: %#v", spec)
	}
}

func TestAgentTeamsPhaseHostSuspendThenRehydrateCreatesNewAttempt(t *testing.T) {
	host, service, writer, state := newAgentTeamsPhaseHostHarness(t)
	req := validAgentTeamsDispatchRequest("inv-await")
	if err := host.Dispatch(context.Background(), req); err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}
	if err := host.Suspend(context.Background(), req.Invocation.ID); err != nil {
		t.Fatalf("Suspend() error = %v", err)
	}
	rehydrate := req
	rehydrate.Prompt.Text = "execute the bounded phase with fresh context"
	rehydrate.Prompt.SHA256 = "rendered-sha-fresh"
	rehydrate.Start.Inputs.InputRevision = "inputs-fresh"
	rehydrate.Binding.ContextSliceRef = "context-slice-fresh"
	rehydrate.Binding.WorkspaceRevision = "workspace-rev-fresh"
	if err := host.Rehydrate(context.Background(), rehydrate); err != nil {
		t.Fatalf("Rehydrate() error = %v", err)
	}

	current, ok, err := state.LoadAgentTeamsPhaseHostState(context.Background(), req.Invocation.ID)
	if err != nil || !ok {
		t.Fatalf("load phase host state = ok %v err %v", ok, err)
	}
	if current.Execution.AgentTeamsTaskID != "agentteams-task-2" {
		t.Fatalf("rehydrated task id = %q, want second attempt", current.Execution.AgentTeamsTaskID)
	}
	prepared, err := writer.LoadPreparedInvocation(context.Background(), current.InvocationRef)
	if err != nil {
		t.Fatalf("load rehydrated prepared invocation: %v", err)
	}
	var spec agentTeamsPhaseSpecDocument
	if err := json.Unmarshal([]byte(prepared.Spec), &spec); err != nil {
		t.Fatalf("rehydrated spec is not JSON: %v", err)
	}
	if spec.Prompt != rehydrate.Prompt.Text || spec.InputRevision != "inputs-fresh" || spec.WorkspaceRevision != "workspace-rev-fresh" {
		t.Fatalf("rehydrated spec = %#v, want fresh prompt/input/workspace", spec)
	}
	if got := service.terminationModes["agentteams-task-1"]; got != adapter.TerminateReleaseWait {
		t.Fatalf("first attempt termination = %q, want release_wait", got)
	}
}

func TestAgentTeamsPhaseHostStopThenRevokeUsesRecoverableStopMode(t *testing.T) {
	host, service, _, state := newAgentTeamsPhaseHostHarness(t)
	req := validAgentTeamsDispatchRequest("inv-stop")
	if err := host.Dispatch(context.Background(), req); err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}
	result, err := host.Stop(context.Background(), StopRequest{
		Invocation: req.Invocation,
		Binding:    req.Binding,
	})
	if err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if result.NonResumable || result.CheckpointRef != req.Binding.CheckpointRef || result.WorkspaceRevision != req.Binding.WorkspaceRevision || result.ResumeStateRef == "" {
		t.Fatalf("stop result = %#v", result)
	}
	if err := host.Revoke(context.Background(), req.Invocation.ID); err != nil {
		t.Fatalf("Revoke() after stop error = %v", err)
	}
	saved, ok, err := state.LoadAgentTeamsPhaseHostState(context.Background(), req.Invocation.ID)
	if err != nil || !ok {
		t.Fatalf("load phase host state = ok %v err %v", ok, err)
	}
	if saved.TerminationMode != adapter.TerminateRecoverableStop {
		t.Fatalf("persisted termination mode = %q, want recoverable_stop", saved.TerminationMode)
	}
	if got := service.terminateCalls["agentteams-task-1"]; got != 2 {
		t.Fatalf("terminate calls after stop+revoke = %d, want idempotent second call", got)
	}
}

func TestAgentTeamsPhaseHostStopWithoutCheckpointDerivesStableCheckpoint(t *testing.T) {
	host, service, _, _ := newAgentTeamsPhaseHostHarness(t)
	req := validAgentTeamsDispatchRequest("inv-stop-derived")
	req.Binding.CheckpointRef = ""
	service.syncWorkspaceRevision = "workspace-rev-synced"
	if err := host.Dispatch(context.Background(), req); err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}
	result, err := host.Stop(context.Background(), StopRequest{Invocation: req.Invocation, Binding: req.Binding})
	if err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if result.NonResumable || result.CheckpointRef == "" || result.ResumeStateRef == "" || result.WorkspaceRevision != service.syncWorkspaceRevision {
		t.Fatalf("derived resumable stop result = %#v", result)
	}
	wantCheckpoint := deterministicWorkspaceCheckpointRef(req.Invocation.ID, service.syncWorkspaceRevision)
	if result.CheckpointRef != wantCheckpoint {
		t.Fatalf("checkpoint ref = %q, want synced workspace checkpoint %q", result.CheckpointRef, wantCheckpoint)
	}
	again, err := host.Stop(context.Background(), StopRequest{Invocation: req.Invocation, Binding: req.Binding})
	if err != nil {
		t.Fatalf("repeat Stop() error = %v", err)
	}
	if again != result {
		t.Fatalf("derived stop evidence changed: %#v then %#v", result, again)
	}
}

func TestAgentTeamsPhaseHostStopExplicitNonResumableHardStops(t *testing.T) {
	host, _, _, _ := newAgentTeamsPhaseHostHarness(t)
	req := validAgentTeamsDispatchRequest("inv-stop-hard")
	req.Binding.NonResumable = true
	if err := host.Dispatch(context.Background(), req); err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}
	result, err := host.Stop(context.Background(), StopRequest{Invocation: req.Invocation, Binding: req.Binding})
	if err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if !result.NonResumable || result.CheckpointRef != "" || result.ResumeStateRef != "" || result.WorkspaceRevision != "" {
		t.Fatalf("explicit hard stop result = %#v", result)
	}
}

func TestAgentTeamsPhaseHostNormalRevokeCancelsExecution(t *testing.T) {
	host, service, _, state := newAgentTeamsPhaseHostHarness(t)
	req := validAgentTeamsDispatchRequest("inv-revoke")
	if err := host.Dispatch(context.Background(), req); err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}
	if err := host.Revoke(context.Background(), req.Invocation.ID); err != nil {
		t.Fatalf("Revoke() error = %v", err)
	}
	saved, ok, err := state.LoadAgentTeamsPhaseHostState(context.Background(), req.Invocation.ID)
	if err != nil || !ok {
		t.Fatalf("load phase host state = ok %v err %v", ok, err)
	}
	if saved.TerminationMode != adapter.TerminateCancel {
		t.Fatalf("normal revoke mode = %q, want cancel", saved.TerminationMode)
	}
	if got := service.terminationModes["agentteams-task-1"]; got != adapter.TerminateCancel {
		t.Fatalf("adapter termination mode = %q, want cancel", got)
	}
}

func TestAgentTeamsPhaseHostTerminationIntentPersistsBeforeExternalTerminate(t *testing.T) {
	host, service, _, state := newAgentTeamsPhaseHostHarness(t)
	req := validAgentTeamsDispatchRequest("inv-crash-order")
	if err := host.Dispatch(context.Background(), req); err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}
	service.failTerminate = fmt.Errorf("external terminate failed after intent persisted")
	if _, err := host.Stop(context.Background(), StopRequest{Invocation: req.Invocation, Binding: req.Binding}); err == nil {
		t.Fatal("Stop() error = nil, want external failure")
	}
	saved, ok, err := state.LoadAgentTeamsPhaseHostState(context.Background(), req.Invocation.ID)
	if err != nil || !ok {
		t.Fatalf("load phase host state = ok %v err %v", ok, err)
	}
	if saved.TerminationMode != adapter.TerminateRecoverableStop {
		t.Fatalf("persisted termination intent after external failure = %q, want recoverable_stop", saved.TerminationMode)
	}
	service.failTerminate = nil
	if err := host.Revoke(context.Background(), req.Invocation.ID); err != nil {
		t.Fatalf("retry Revoke() after persisted intent error = %v", err)
	}
	if got := service.terminationModes["agentteams-task-1"]; got != adapter.TerminateRecoverableStop {
		t.Fatalf("retried external termination mode = %q, want recoverable_stop", got)
	}
}

func TestAgentTeamsPhaseHostRestartReusesPersistentState(t *testing.T) {
	host, service, writer, state := newAgentTeamsPhaseHostHarness(t)
	req := validAgentTeamsDispatchRequest("inv-restart")
	if err := host.Dispatch(context.Background(), req); err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}

	restarted, err := NewAgentTeamsPhaseHost(AgentTeamsPhaseHostConfig{
		Adapter: service,
		Writer:  writer,
		State:   state,
		RoomID:  "!threadmill:example.test",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.Suspend(context.Background(), req.Invocation.ID); err != nil {
		t.Fatalf("restarted Suspend() error = %v", err)
	}
	if service.dispatchCalls != 1 {
		t.Fatalf("restart suspend redispatched unexpectedly: dispatch calls = %d", service.dispatchCalls)
	}
}

func TestPostgresAgentTeamsPhaseHostStoreRealPostgresRestart(t *testing.T) {
	dsn := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}
	if dsn == "" {
		t.Skip("THREADMILL_TEST_DATABASE_URL or DATABASE_URL is required for real PostgreSQL integration")
	}
	ctx := context.Background()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping postgres: %v", err)
	}
	schema := fmt.Sprintf("threadmill_phase_host_it_%d", time.Now().UnixNano())
	if _, err := db.ExecContext(ctx, `CREATE SCHEMA `+quoteIdent(schema)); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	defer db.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS `+quoteIdent(schema)+` CASCADE`)
	if _, err := db.ExecContext(ctx, `SET search_path TO `+quoteIdent(schema)); err != nil {
		t.Fatalf("set search_path: %v", err)
	}
	loaded, err := platformpostgres.LoadMigrations(migrations.FS, ".")
	if err != nil {
		t.Fatalf("load migrations: %v", err)
	}
	if err := platformpostgres.NewMigrator(db).Apply(ctx, loaded); err != nil {
		t.Fatalf("apply full migrations: %v", err)
	}

	store := NewPostgresAgentTeamsPhaseHostStoreFromSQL(db)
	service := newFakeAgentTeamsPhaseAdapter()
	host, err := NewAgentTeamsPhaseHost(AgentTeamsPhaseHostConfig{
		Adapter: service,
		Writer:  store,
		State:   store,
		RoomID:  "!threadmill:example.test",
	})
	if err != nil {
		t.Fatal(err)
	}
	req := validAgentTeamsDispatchRequest("inv-pg-restart")
	if err := host.Dispatch(ctx, req); err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}
	restartedStore := NewPostgresAgentTeamsPhaseHostStoreFromSQL(db)
	restarted, err := NewAgentTeamsPhaseHost(AgentTeamsPhaseHostConfig{
		Adapter: service,
		Writer:  restartedStore,
		State:   restartedStore,
		RoomID:  "!threadmill:example.test",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.Suspend(ctx, req.Invocation.ID); err != nil {
		t.Fatalf("restarted Suspend() error = %v", err)
	}
	refreshed := req
	refreshed.Prompt.Text = "postgres restart fresh prompt"
	refreshed.Prompt.SHA256 = "pg-fresh-sha"
	refreshed.Start.Inputs.InputRevision = "pg-inputs-2"
	if err := restarted.Rehydrate(ctx, refreshed); err != nil {
		t.Fatalf("restarted Rehydrate() error = %v", err)
	}
	state, ok, err := NewPostgresAgentTeamsPhaseHostStoreFromSQL(db).LoadAgentTeamsPhaseHostState(ctx, req.Invocation.ID)
	if err != nil || !ok {
		t.Fatalf("load postgres host state = ok %v err %v", ok, err)
	}
	if state.Execution.AgentTeamsTaskID != "agentteams-task-2" {
		t.Fatalf("postgres rehydrate task = %q, want second attempt", state.Execution.AgentTeamsTaskID)
	}
	prepared, err := NewPostgresAgentTeamsPhaseHostStoreFromSQL(db).LoadPreparedInvocation(ctx, state.InvocationRef)
	if err != nil {
		t.Fatalf("load postgres prepared invocation: %v", err)
	}
	var spec agentTeamsPhaseSpecDocument
	if err := json.Unmarshal([]byte(prepared.Spec), &spec); err != nil {
		t.Fatalf("postgres prepared spec JSON: %v", err)
	}
	if spec.Prompt != refreshed.Prompt.Text || spec.InputRevision != "pg-inputs-2" {
		t.Fatalf("postgres rehydrated spec = %#v", spec)
	}
}

func newAgentTeamsPhaseHostHarness(t *testing.T) (*AgentTeamsPhaseHost, *fakeAgentTeamsPhaseAdapter, *MemoryPreparedInvocationWriter, *MemoryAgentTeamsPhaseHostStateStore) {
	t.Helper()
	service := newFakeAgentTeamsPhaseAdapter()
	writer := NewMemoryPreparedInvocationWriter()
	state := NewMemoryAgentTeamsPhaseHostStateStore()
	host, err := NewAgentTeamsPhaseHost(AgentTeamsPhaseHostConfig{
		Adapter: service,
		Writer:  writer,
		State:   state,
		RoomID:  "!threadmill:example.test",
	})
	if err != nil {
		t.Fatal(err)
	}
	return host, service, writer, state
}

func TestMemoryPreparedInvocationWriterReplacesUndispatchedEnvelope(t *testing.T) {
	writer := NewMemoryPreparedInvocationWriter()
	first := adapter.PreparedInvocation{InvocationID: "inv-retry", Spec: "first"}
	second := adapter.PreparedInvocation{InvocationID: "inv-retry", Spec: "second"}
	if err := writer.SavePreparedInvocation(context.Background(), "ref-first", first); err != nil {
		t.Fatal(err)
	}
	if err := writer.SavePreparedInvocation(context.Background(), "ref-second", second); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.LoadPreparedInvocation(context.Background(), "ref-first"); !kernel.IsCode(err, kernel.CodeNotFound) {
		t.Fatalf("superseded prepared envelope error = %v, want not_found", err)
	}
	loaded, err := writer.LoadPreparedInvocation(context.Background(), "ref-second")
	if err != nil || loaded.Spec != "second" {
		t.Fatalf("latest prepared envelope = %#v err %v", loaded, err)
	}
}

func TestPostgresPreparedInvocationWriterPrunesFailedRetryEnvelopes(t *testing.T) {
	db := openAgentTeamsPhasePostgres(t)
	ctx := context.Background()
	store := NewPostgresAgentTeamsPhaseHostStoreFromSQL(db)
	invocationID := kernel.InvocationID("inv-pg-prune-retries")
	base := adapter.PreparedInvocation{
		InvocationID: invocationID,
		ProjectID:    "project-pg-prune-retries",
		Role:         auth.RolePlanner,
		RoomID:       "!threadmill:example.test",
	}
	active := base
	active.Spec = "active"
	active.EnvelopeRef = "envelope-active"
	active.RuntimeConfigRef = "runtime-active"
	if err := store.SavePreparedInvocation(ctx, "ref-active", active); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveAgentTeamsPhaseHostState(ctx, AgentTeamsPhaseHostState{
		InvocationID:  invocationID,
		InvocationRef: "ref-active",
		Execution: adapter.AgentTeamsExecutionRef{
			InvocationID:     invocationID,
			AgentTeamsTaskID: "task-active",
			HostRef:          "phase-a",
		},
	}); err != nil {
		t.Fatal(err)
	}
	for _, candidate := range []string{"retry-1", "retry-2", "retry-3"} {
		prepared := base
		prepared.Spec = candidate
		prepared.EnvelopeRef = "envelope-" + candidate
		prepared.RuntimeConfigRef = "runtime-" + candidate
		if err := store.SavePreparedInvocation(ctx, "ref-"+candidate, prepared); err != nil {
			t.Fatal(err)
		}
	}
	var refs []string
	rows, err := db.QueryContext(ctx, `SELECT invocation_ref FROM phase_agentteams_prepared_invocations WHERE invocation_id=$1 ORDER BY invocation_ref`, invocationID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var ref string
		if err := rows.Scan(&ref); err != nil {
			t.Fatal(err)
		}
		refs = append(refs, ref)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want := []string{"ref-active", "ref-retry-3"}
	if !reflect.DeepEqual(refs, want) {
		t.Fatalf("prepared refs = %v, want %v", refs, want)
	}
}

func openAgentTeamsPhasePostgres(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}
	if dsn == "" {
		t.Skip("THREADMILL_TEST_DATABASE_URL or DATABASE_URL is required for real PostgreSQL integration")
	}
	ctx := context.Background()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		t.Fatalf("ping postgres: %v", err)
	}
	schema := fmt.Sprintf("threadmill_phase_host_prune_it_%d", time.Now().UnixNano())
	if _, err := db.ExecContext(ctx, `CREATE SCHEMA `+quoteIdent(schema)); err != nil {
		db.Close()
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS `+quoteIdent(schema)+` CASCADE`)
		_ = db.Close()
	})
	if _, err := db.ExecContext(ctx, `SET search_path TO `+quoteIdent(schema)); err != nil {
		t.Fatalf("set search_path: %v", err)
	}
	loaded, err := platformpostgres.LoadMigrations(migrations.FS, ".")
	if err != nil {
		t.Fatalf("load migrations: %v", err)
	}
	if err := platformpostgres.NewMigrator(db).Apply(ctx, loaded); err != nil {
		t.Fatalf("apply full migrations: %v", err)
	}
	return db
}

func validAgentTeamsDispatchRequest(invocationID string) DispatchRequest {
	now := time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)
	invocation := baseruntime.Invocation{
		ID:               kernel.InvocationID(invocationID),
		ActorPrincipalID: "actor-a",
		ProjectID:        "project-a",
		TaskID:           "task-a",
		EndpointID:       "execute",
		Generation:       1,
		Role:             auth.RoleExecutor,
		Status:           baseruntime.InvocationPrepared,
		BindingRef:       "binding-a",
		LeaseID:          "lease-a",
		WorkspaceRef:     "workspace-a",
		PromptHashes:     map[string]string{"shared": "prompt-hash"},
		SkillHashes:      map[string]string{"phase-runtime": "skill-hash"},
		EffectiveTools:   []auth.Tool{auth.ToolWorkspaceRun, auth.ToolAgentSubmitPhaseOutput},
		CreatedAt:        now,
		ExpiresAt:        now.Add(time.Hour),
	}
	binding := BindingSnapshot{
		ProjectID:           invocation.ProjectID,
		ActorPrincipalID:    invocation.ActorPrincipalID,
		TaskID:              invocation.TaskID,
		EndpointID:          invocation.EndpointID,
		Generation:          int(invocation.Generation),
		BindingRef:          invocation.BindingRef,
		LeaseRef:            invocation.LeaseID,
		WorkspaceRef:        invocation.WorkspaceRef,
		WorkspaceRevision:   "workspace-rev-a",
		ContextSliceRef:     "context-slice-a",
		TaskMemoryBufferRef: "memory-buffer-a",
		Inputs: PhaseInputSet{
			InputRevision: "inputs-a",
		},
		CheckpointRef: "checkpoint-a",
	}
	return DispatchRequest{
		Invocation: invocation,
		Capability: invocation.Capability(),
		Prompt: promptcatalog.Rendered{
			Text:         "execute the bounded phase",
			PromptHashes: invocation.PromptHashes,
			SkillHashes:  invocation.SkillHashes,
			SHA256:       "rendered-sha",
		},
		Start: StartPhaseInput{
			InvocationID: invocation.ID,
			Endpoint: PhaseEndpointRef{
				TaskID:     invocation.TaskID,
				EndpointID: invocation.EndpointID,
			},
			Generation: int(invocation.Generation),
			BindingRef: invocation.BindingRef,
			Inputs:     binding.Inputs,
		},
		Binding:       binding,
		CheckpointRef: binding.CheckpointRef,
	}
}

type fakeAgentTeamsPhaseAdapter struct {
	dispatchCalls         int
	nextAttempt           int
	current               map[string]adapter.AgentTeamsExecutionRef
	states                map[string]string
	terminationModes      map[string]adapter.TerminateMode
	terminateCalls        map[string]int
	calls                 []string
	failTerminate         error
	syncWorkspaceRevision string
}

func newFakeAgentTeamsPhaseAdapter() *fakeAgentTeamsPhaseAdapter {
	return &fakeAgentTeamsPhaseAdapter{
		nextAttempt:      1,
		current:          make(map[string]adapter.AgentTeamsExecutionRef),
		states:           make(map[string]string),
		terminationModes: make(map[string]adapter.TerminateMode),
		terminateCalls:   make(map[string]int),
	}
}

func (a *fakeAgentTeamsPhaseAdapter) Dispatch(_ context.Context, invocationRef string) (adapter.AgentTeamsExecutionRef, error) {
	a.dispatchCalls++
	a.calls = append(a.calls, "dispatch:"+invocationRef)
	if existing, ok := a.current[invocationRef]; ok {
		if a.states[existing.AgentTeamsTaskID] == "terminated" {
			if a.terminationModes[existing.AgentTeamsTaskID] != adapter.TerminateReleaseWait {
				return adapter.AgentTeamsExecutionRef{}, kernel.Error{Code: kernel.CodeStaleCommand, Message: "terminated execution cannot be redispatched"}
			}
		} else {
			return existing, nil
		}
	}
	execution := adapter.AgentTeamsExecutionRef{
		InvocationID:     invocationIDFromAgentTeamsRef(invocationRef),
		AgentTeamsTaskID: fmt.Sprintf("agentteams-task-%d", a.nextAttempt),
		HostRef:          "worker-a",
	}
	a.nextAttempt++
	a.current[invocationRef] = execution
	a.states[execution.AgentTeamsTaskID] = "dispatched"
	return execution, nil
}

func (a *fakeAgentTeamsPhaseAdapter) Terminate(_ context.Context, execution adapter.AgentTeamsExecutionRef, rawMode string) error {
	mode := adapter.TerminateMode(rawMode)
	a.calls = append(a.calls, "terminate:"+execution.AgentTeamsTaskID+":"+rawMode)
	a.terminateCalls[execution.AgentTeamsTaskID]++
	if a.failTerminate != nil {
		return a.failTerminate
	}
	if a.states[execution.AgentTeamsTaskID] == "terminated" {
		if a.terminationModes[execution.AgentTeamsTaskID] == mode {
			return nil
		}
		return kernel.IdempotencyConflict()
	}
	a.states[execution.AgentTeamsTaskID] = "terminated"
	a.terminationModes[execution.AgentTeamsTaskID] = mode
	return nil
}

func (a *fakeAgentTeamsPhaseAdapter) FenceExecution(_ context.Context, execution adapter.AgentTeamsExecutionRef) error {
	a.calls = append(a.calls, "fence:"+string(execution.InvocationID))
	return nil
}

func (a *fakeAgentTeamsPhaseAdapter) SyncExecutionWorkspace(_ context.Context, execution adapter.AgentTeamsExecutionRef) (adapter.ExecutionWorkspaceCheckpoint, error) {
	a.calls = append(a.calls, "sync:"+execution.AgentTeamsTaskID)
	return adapter.ExecutionWorkspaceCheckpoint{WorkspaceRevision: a.syncWorkspaceRevision}, nil
}

func (a *fakeAgentTeamsPhaseAdapter) Collect(context.Context, adapter.AgentTeamsExecutionRef) (adapter.UntrustedExecutionResult, error) {
	return adapter.UntrustedExecutionResult{}, nil
}

func (a *fakeAgentTeamsPhaseAdapter) Observe(context.Context, string) ([]adapter.ExecutionObservation, error) {
	return nil, nil
}
