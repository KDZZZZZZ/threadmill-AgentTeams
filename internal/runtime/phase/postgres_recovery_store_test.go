package phase

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	baseruntime "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime"
)

func TestPostgresRecoveryStoreRecoversActiveStopEvidenceClearAndResume(t *testing.T) {
	ctx := context.Background()
	db := newFakeRecoveryDB()
	invocations := baseruntime.NewMemoryInvocationStore()
	store := NewPostgresRecoveryStore(db, invocations)
	start := validCommand("cmd-pg-start", coordination.CommandStart, "binding-1", "lease-1", 1)
	binding := postgresRecoveryBinding()
	invocation := postgresRecoveryInvocation(start, binding)
	if err := invocations.Create(ctx, invocation); err != nil {
		t.Fatalf("create invocation: %v", err)
	}
	if err := invocations.Transition(ctx, invocation.ID, baseruntime.InvocationPrepared, baseruntime.InvocationRunning); err != nil {
		t.Fatalf("mark running: %v", err)
	}
	active := ActiveInvocation{Invocation: invocation, Command: start, Binding: binding, Inputs: binding.Inputs}
	if err := store.RecordActiveInvocation(ctx, active); err != nil {
		t.Fatalf("record active: %v", err)
	}

	stop := validCommand("cmd-pg-stop", coordination.CommandStop, "binding-1", "lease-1", 1)
	recovered, ok, err := store.RecoverActiveInvocation(ctx, stop, binding)
	if err != nil || !ok {
		t.Fatalf("recover active = %#v %v %v, want active", recovered, ok, err)
	}
	if recovered.Invocation.ID != invocation.ID || recovered.Command.ID != stop.ID {
		t.Fatalf("recovered active = %#v, want invocation with stop command authority", recovered)
	}
	result := StopResult{ResumeStateRef: "resume-state-1", CheckpointRef: "checkpoint-1", WorkspaceRevision: "main-rev-stop"}
	if err := store.RecordStopEvidence(ctx, recovered, stop, result); err != nil {
		t.Fatalf("record stop: %v", err)
	}
	if err := store.RecordStopEvidence(ctx, recovered, stop, result); err != nil {
		t.Fatalf("idempotent stop: %v", err)
	}
	conflict := result
	conflict.CheckpointRef = "other-checkpoint"
	if err := store.RecordStopEvidence(ctx, recovered, stop, conflict); !kernel.IsCode(err, kernel.CodeIdempotencyConflict) {
		t.Fatalf("duplicate stop conflict = %v, want idempotency_conflict", err)
	}
	got, ok, err := store.GetStopEvidence(ctx, invocation.ID, stop.ID)
	if err != nil || !ok {
		t.Fatalf("get stop evidence = %#v %v %v, want result", got, ok, err)
	}
	if got.CheckpointRef != result.CheckpointRef || got.ResumeStateRef != result.ResumeStateRef {
		t.Fatalf("stop evidence = %#v, want %#v", got, result)
	}
	resume := validCommand("cmd-pg-resume", coordination.CommandResume, "binding-2", "lease-2", 2)
	resumeBinding := binding
	resumeBinding.Generation = 2
	resumeBinding.BindingRef = "binding-2"
	resumeBinding.LeaseRef = "lease-2"
	resumeBinding.CheckpointRef = "checkpoint-1"
	if err := store.ValidateResume(ctx, resume, resumeBinding); err != nil {
		t.Fatalf("validate resume: %v", err)
	}
	resumeBinding.CheckpointRef = "missing-checkpoint"
	if err := store.ValidateResume(ctx, resume, resumeBinding); !kernel.IsCode(err, kernel.CodeStaleCheckpoint) {
		t.Fatalf("mismatched resume = %v, want stale_checkpoint", err)
	}
	if err := store.ClearActiveInvocation(ctx, invocation.ID); err != nil {
		t.Fatalf("clear active: %v", err)
	}
	if _, ok, err := store.RecoverActiveInvocation(ctx, stop, binding); err != nil || ok {
		t.Fatalf("recover after clear ok=%v err=%v, want no active", ok, err)
	}
}

func TestPostgresRecoveryStoreRejectsStopEvidenceWithoutActiveObligation(t *testing.T) {
	store := NewPostgresRecoveryStore(newFakeRecoveryDB(), baseruntime.NewMemoryInvocationStore())
	start := validCommand("cmd-pg-missing-active", coordination.CommandStart, "binding-1", "lease-1", 1)
	binding := postgresRecoveryBinding()
	active := ActiveInvocation{
		Invocation: postgresRecoveryInvocation(start, binding),
		Command:    start,
		Binding:    binding,
	}
	stop := validCommand("cmd-pg-missing-stop", coordination.CommandStop, "binding-1", "lease-1", 1)
	result := StopResult{ResumeStateRef: "resume-state-1", CheckpointRef: "checkpoint-1", WorkspaceRevision: "main-rev-stop"}
	if err := store.RecordStopEvidence(context.Background(), active, stop, result); !kernel.IsCode(err, kernel.CodeStaleCommand) {
		t.Fatalf("missing active obligation = %v, want stale_command", err)
	}
}

func postgresRecoveryBinding() BindingSnapshot {
	return BindingSnapshot{
		ProjectID:         "project-a",
		ActorPrincipalID:  "agent-executor",
		TaskID:            "task-a",
		EndpointID:        "execute",
		Generation:        1,
		BindingRef:        "binding-1",
		LeaseRef:          "lease-1",
		WorkspaceRef:      "workspace-1",
		WorkspaceRevision: "main-rev-1",
		ContextSliceRef:   "ctx-1",
		Inputs:            PhaseInputSet{InputRevision: "inputs-1"},
	}
}

func postgresRecoveryInvocation(command PhaseCommand, binding BindingSnapshot) baseruntime.Invocation {
	now := time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC)
	return baseruntime.Invocation{
		ID:               deterministicInvocationID(command),
		ActorPrincipalID: binding.ActorPrincipalID,
		ProjectID:        binding.ProjectID,
		TaskID:           command.Endpoint.TaskID,
		EndpointID:       command.Endpoint.EndpointID,
		Generation:       uint64(command.Generation),
		Role:             auth.RoleExecutor,
		Status:           baseruntime.InvocationPrepared,
		BindingRef:       command.BindingRef,
		LeaseID:          command.LeaseRef,
		WorkspaceRef:     binding.WorkspaceRef,
		PromptHashes:     map[string]string{"shared": "prompt-hash"},
		SkillHashes:      map[string]string{"phase-runtime": "skill-hash"},
		EffectiveTools:   []auth.Tool{auth.ToolRuntimeAwaitInputs},
		CreatedAt:        now,
		ExpiresAt:        now.Add(time.Hour),
	}
}

type fakeRecoveryDB struct {
	rows map[string]fakeRecoveryRecord
}

type fakeRecoveryRecord struct {
	active        bool
	stopCommandID string
	stopResult    []byte
}

func newFakeRecoveryDB() *fakeRecoveryDB {
	return &fakeRecoveryDB{rows: map[string]fakeRecoveryRecord{}}
}

func (db *fakeRecoveryDB) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	switch {
	case strings.Contains(query, "INSERT INTO phase_recovery_obligations"):
		runCommandID := args[0].(string)
		row := db.rows[runCommandID]
		if row.stopCommandID == "" {
			row.active = true
		}
		db.rows[runCommandID] = row
		return fakePhaseResult(1), nil
	case strings.Contains(query, "SET stop_command_id"):
		runCommandID := args[0].(string)
		row, ok := db.rows[runCommandID]
		if !ok || !row.active || row.stopCommandID != "" || row.stopResult != nil {
			return fakePhaseResult(0), nil
		}
		row.stopCommandID = args[1].(string)
		row.stopResult = []byte(args[2].(string))
		db.rows[runCommandID] = row
		return fakePhaseResult(1), nil
	case strings.Contains(query, "SET active = FALSE"):
		runCommandID := args[0].(string)
		row := db.rows[runCommandID]
		row.active = false
		db.rows[runCommandID] = row
		return fakePhaseResult(1), nil
	default:
		return fakePhaseResult(0), nil
	}
}

func (db *fakeRecoveryDB) QueryContext(_ context.Context, query string, _ ...any) (baseruntime.RowsScanner, error) {
	var rows [][]any
	switch {
	case strings.Contains(query, "WHERE active = TRUE"):
		for runCommandID, row := range db.rows {
			if row.active {
				rows = append(rows, []any{runCommandID})
			}
		}
	case strings.Contains(query, "WHERE stop_result IS NOT NULL"):
		for _, row := range db.rows {
			if row.stopResult != nil {
				rows = append(rows, []any{row.stopResult})
			}
		}
	default:
		for runCommandID := range db.rows {
			rows = append(rows, []any{runCommandID})
		}
	}
	return &fakePhaseRows{rows: rows}, nil
}

func (db *fakeRecoveryDB) QueryRowContext(_ context.Context, query string, args ...any) baseruntime.RowScanner {
	runCommandID := args[0].(string)
	row, ok := db.rows[runCommandID]
	if !ok {
		return fakePhaseRow{err: sql.ErrNoRows}
	}
	if strings.Contains(query, "COALESCE(stop_command_id") {
		if row.stopCommandID == "" || row.stopResult == nil {
			return fakePhaseRow{err: sql.ErrNoRows}
		}
		return fakePhaseRow{values: []any{row.stopCommandID, row.stopResult}}
	}
	if strings.Contains(query, "WHERE run_command_id = $1 AND stop_command_id = $2") {
		commandID := args[1].(string)
		if row.stopCommandID != commandID || row.stopResult == nil {
			return fakePhaseRow{err: sql.ErrNoRows}
		}
		return fakePhaseRow{values: []any{row.stopResult}}
	}
	return fakePhaseRow{err: sql.ErrNoRows}
}

type fakePhaseResult int64

func (r fakePhaseResult) LastInsertId() (int64, error) { return 0, nil }
func (r fakePhaseResult) RowsAffected() (int64, error) { return int64(r), nil }

type fakePhaseRow struct {
	values []any
	err    error
}

func (r fakePhaseRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	return scanPhaseValues(r.values, dest)
}

type fakePhaseRows struct {
	rows   [][]any
	cursor int
}

func (r *fakePhaseRows) Next() bool {
	if r.cursor >= len(r.rows) {
		return false
	}
	r.cursor++
	return true
}

func (r *fakePhaseRows) Scan(dest ...any) error {
	return scanPhaseValues(r.rows[r.cursor-1], dest)
}

func (r *fakePhaseRows) Close() error { return nil }
func (r *fakePhaseRows) Err() error   { return nil }

func scanPhaseValues(values []any, dest []any) error {
	for i := range values {
		switch target := dest[i].(type) {
		case *string:
			*target = values[i].(string)
		case *[]byte:
			switch value := values[i].(type) {
			case []byte:
				*target = append((*target)[:0], value...)
			case StopResult:
				payload, err := json.Marshal(value)
				if err != nil {
					return err
				}
				*target = payload
			}
		default:
			return kernel.Error{Code: kernel.CodeInternalError, Message: "unsupported fake scan target", Recoverable: false}
		}
	}
	return nil
}
