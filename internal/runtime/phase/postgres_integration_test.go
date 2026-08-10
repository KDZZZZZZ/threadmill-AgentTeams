package phase

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	platformpostgres "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/postgres"
	baseruntime "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/migrations"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestPostgresRuntimeStoresAgainstRealDatabase(t *testing.T) {
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
	schema := fmt.Sprintf(
		"threadmill_runtime_it_%d_%s",
		time.Now().UnixNano(),
		strings.NewReplacer("/", "_", "-", "_").Replace(strings.ToLower(t.Name())),
	)
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

	invocations := baseruntime.NewPostgresInvocationStoreFromSQL(db)
	start := validCommand("cmd-real-start", coordination.CommandStart, "binding-real-1", "lease-real-1", 1)
	binding := realPostgresBinding("binding-real-1", "lease-real-1", 1)
	invocation := realPostgresInvocation(start, binding)
	if err := invocations.Create(ctx, invocation); err != nil {
		t.Fatalf("create invocation: %v", err)
	}
	if err := invocations.Create(ctx, invocation); err != nil {
		t.Fatalf("idempotent create: %v", err)
	}
	conflict := invocation
	conflict.BindingRef = "binding-conflict"
	if err := invocations.Create(ctx, conflict); !kernel.IsCode(err, kernel.CodeIdempotencyConflict) {
		t.Fatalf("conflicting create = %v, want idempotency_conflict", err)
	}
	got, ok, err := invocations.Get(ctx, invocation.ID)
	if err != nil || !ok {
		t.Fatalf("get invocation = %#v %v %v, want row", got, ok, err)
	}
	if got.ID != invocation.ID || got.ConsumerInvocationID != invocation.ConsumerInvocationID {
		t.Fatalf("get invocation lost canonical fields: %#v", got)
	}
	if err := invocations.Transition(ctx, invocation.ID, baseruntime.InvocationPrepared, baseruntime.InvocationRunning); err != nil {
		t.Fatalf("transition prepared->running: %v", err)
	}
	byLease, ok, err := invocations.GetByLease(ctx, invocation.LeaseID)
	if err != nil || !ok {
		t.Fatalf("get by lease = %#v %v %v, want row", byLease, ok, err)
	}
	if byLease.Status != baseruntime.InvocationRunning {
		t.Fatalf("get by lease status = %s, want running", byLease.Status)
	}
	if err := invocations.Transition(ctx, invocation.ID, baseruntime.InvocationPrepared, baseruntime.InvocationStopped); !kernel.IsCode(err, kernel.CodeRevisionConflict) {
		t.Fatalf("stale CAS transition = %v, want revision_conflict", err)
	}

	firstRecovery := NewPostgresRecoveryStoreFromSQL(db, invocations)
	active := ActiveInvocation{Invocation: invocation, Command: start, Binding: binding, Inputs: binding.Inputs}
	if err := firstRecovery.RecordActiveInvocation(ctx, active); err != nil {
		t.Fatalf("record active: %v", err)
	}
	stop := validCommand("cmd-real-stop", coordination.CommandStop, "binding-real-1", "lease-real-1", 1)
	rebuiltRecovery := NewPostgresRecoveryStoreFromSQL(db, invocations)
	recovered, ok, err := rebuiltRecovery.RecoverActiveInvocation(ctx, stop, binding)
	if err != nil || !ok {
		t.Fatalf("recover active after rebuild = %#v %v %v, want active", recovered, ok, err)
	}
	if recovered.Invocation.ID != invocation.ID || recovered.Command.ID != stop.ID {
		t.Fatalf("recovered active = %#v, want invocation plus stop command", recovered)
	}
	stopResult := StopResult{ResumeStateRef: "resume-real-1", CheckpointRef: "checkpoint-real-1", WorkspaceRevision: "main-real-stop"}
	if err := rebuiltRecovery.RecordStopEvidence(ctx, recovered, stop, stopResult); err != nil {
		t.Fatalf("record stop evidence: %v", err)
	}
	if err := rebuiltRecovery.RecordStopEvidence(ctx, recovered, stop, stopResult); err != nil {
		t.Fatalf("duplicate same stop evidence: %v", err)
	}
	conflictingStop := stopResult
	conflictingStop.CheckpointRef = "checkpoint-conflict"
	if err := rebuiltRecovery.RecordStopEvidence(ctx, recovered, stop, conflictingStop); !kernel.IsCode(err, kernel.CodeIdempotencyConflict) {
		t.Fatalf("duplicate conflicting stop evidence = %v, want idempotency_conflict", err)
	}
	persisted, ok, err := NewPostgresRecoveryStoreFromSQL(db, invocations).GetStopEvidence(ctx, invocation.ID, stop.ID)
	if err != nil || !ok {
		t.Fatalf("get stop evidence after rebuild = %#v %v %v, want evidence", persisted, ok, err)
	}
	if persisted.CheckpointRef != stopResult.CheckpointRef || persisted.WorkspaceRevision != stopResult.WorkspaceRevision {
		t.Fatalf("persisted stop evidence = %#v, want %#v", persisted, stopResult)
	}
	resume := validCommand("cmd-real-resume", coordination.CommandResume, "binding-real-2", "lease-real-2", 2)
	resumeBinding := realPostgresBinding("binding-real-2", "lease-real-2", 2)
	resumeBinding.CheckpointRef = stopResult.CheckpointRef
	if err := rebuiltRecovery.ValidateResume(ctx, resume, resumeBinding); err != nil {
		t.Fatalf("validate resume: %v", err)
	}
	resumeBinding.CheckpointRef = "checkpoint-missing"
	if err := rebuiltRecovery.ValidateResume(ctx, resume, resumeBinding); !kernel.IsCode(err, kernel.CodeStaleCheckpoint) {
		t.Fatalf("resume mismatch = %v, want stale_checkpoint", err)
	}
	otherStart := validCommand("cmd-real-other-start", coordination.CommandStart, "binding-real-other-1", "lease-real-other-1", 1)
	otherBinding := realPostgresBinding("binding-real-other-1", "lease-real-other-1", 1)
	otherBinding.ProjectID = "project-other"
	otherInvocation := realPostgresInvocation(otherStart, otherBinding)
	if err := invocations.Create(ctx, otherInvocation); err != nil {
		t.Fatalf("create other-project invocation: %v", err)
	}
	otherActive := ActiveInvocation{Invocation: otherInvocation, Command: otherStart, Binding: otherBinding, Inputs: otherBinding.Inputs}
	if err := rebuiltRecovery.RecordActiveInvocation(ctx, otherActive); err != nil {
		t.Fatalf("record other-project active: %v", err)
	}
	otherStop := validCommand("cmd-real-other-stop", coordination.CommandStop, "binding-real-other-1", "lease-real-other-1", 1)
	crossProjectCheckpoint := "checkpoint-cross-project"
	if err := rebuiltRecovery.RecordStopEvidence(ctx, otherActive, otherStop, StopResult{
		ResumeStateRef:    "resume-other",
		CheckpointRef:     crossProjectCheckpoint,
		WorkspaceRevision: "main-other-stop",
	}); err != nil {
		t.Fatalf("record other-project stop evidence: %v", err)
	}
	resumeBinding.CheckpointRef = crossProjectCheckpoint
	if err := rebuiltRecovery.ValidateResume(ctx, resume, resumeBinding); !kernel.IsCode(err, kernel.CodeStaleCheckpoint) {
		t.Fatalf("cross-project checkpoint = %v, want stale_checkpoint", err)
	}
	if err := rebuiltRecovery.ClearActiveInvocation(ctx, invocation.ID); err != nil {
		t.Fatalf("clear active: %v", err)
	}
	if _, ok, err := NewPostgresRecoveryStoreFromSQL(db, invocations).RecoverActiveInvocation(ctx, stop, binding); err != nil || ok {
		t.Fatalf("recover after clear ok=%v err=%v, want no active", ok, err)
	}
}

func realPostgresBinding(binding kernel.BindingRef, lease kernel.LeaseID, generation int) BindingSnapshot {
	return BindingSnapshot{
		ProjectID:           "project-real",
		ActorPrincipalID:    "agent-real-executor",
		TaskID:              "task-a",
		EndpointID:          "execute",
		Generation:          generation,
		BindingRef:          binding,
		LeaseRef:            lease,
		WorkspaceRef:        "workspace-real",
		WorkspaceRevision:   fmt.Sprintf("main-real-%d", generation),
		ContextSliceRef:     "ctx-real",
		TaskMemoryBufferRef: "mem-real",
		Inputs:              PhaseInputSet{InputRevision: "inputs-real"},
	}
}

func realPostgresInvocation(command PhaseCommand, binding BindingSnapshot) baseruntime.Invocation {
	now := time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC)
	return baseruntime.Invocation{
		ID:                   deterministicInvocationID(command),
		ActorPrincipalID:     binding.ActorPrincipalID,
		ProjectID:            binding.ProjectID,
		TaskID:               command.Endpoint.TaskID,
		EndpointID:           command.Endpoint.EndpointID,
		ConsumerInvocationID: "",
		Generation:           uint64(command.Generation),
		Role:                 auth.RoleExecutor,
		Status:               baseruntime.InvocationPrepared,
		BindingRef:           command.BindingRef,
		LeaseID:              command.LeaseRef,
		WorkspaceRef:         binding.WorkspaceRef,
		ContextSliceRef:      binding.ContextSliceRef,
		TaskMemoryBufferRef:  binding.TaskMemoryBufferRef,
		PromptHashes:         map[string]string{"shared": "prompt-real"},
		SkillHashes:          map[string]string{"phase-runtime": "skill-real"},
		EffectiveTools:       []auth.Tool{auth.ToolRuntimeAwaitInputs, auth.ToolAgentSubmitPhaseOutput},
		CreatedAt:            now,
		ExpiresAt:            now.Add(time.Hour),
	}
}

func quoteIdent(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}
