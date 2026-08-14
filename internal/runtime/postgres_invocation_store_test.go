package runtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

func TestPostgresInvocationStoreCreateGetLeaseAndTransition(t *testing.T) {
	db := newFakeRuntimeDB()
	store := NewPostgresInvocationStore(db)
	invocation := validInvocation()
	invocation.ConsumerInvocationID = ""

	if err := store.Create(context.Background(), invocation); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := store.Create(context.Background(), invocation); err != nil {
		t.Fatalf("idempotent create: %v", err)
	}
	changed := invocation
	changed.BindingRef = "binding-other"
	if err := store.Create(context.Background(), changed); !kernel.IsCode(err, kernel.CodeIdempotencyConflict) {
		t.Fatalf("conflicting create = %v, want idempotency_conflict", err)
	}
	got, ok, err := store.Get(context.Background(), invocation.ID)
	if err != nil || !ok {
		t.Fatalf("get = %#v %v %v, want invocation", got, ok, err)
	}
	if got.ID != invocation.ID || got.BindingRef != invocation.BindingRef {
		t.Fatalf("get returned %#v, want original invocation", got)
	}
	if err := store.Transition(context.Background(), invocation.ID, InvocationPrepared, InvocationRunning); err != nil {
		t.Fatalf("transition running: %v", err)
	}
	byLease, ok, err := store.GetByLease(context.Background(), invocation.LeaseID)
	if err != nil || !ok {
		t.Fatalf("get by lease = %#v %v %v, want invocation", byLease, ok, err)
	}
	if byLease.Status != InvocationRunning {
		t.Fatalf("lease status = %s, want running", byLease.Status)
	}
	if err := store.Create(context.Background(), invocation); err != nil {
		t.Fatalf("create replay after status transition: %v", err)
	}
	if err := store.Transition(context.Background(), invocation.ID, InvocationPrepared, InvocationCompleted); !kernel.IsCode(err, kernel.CodeInvalidRequest) {
		t.Fatalf("invalid transition = %v, want invalid_request", err)
	}
	if err := store.Transition(context.Background(), invocation.ID, InvocationPrepared, InvocationStopped); !kernel.IsCode(err, kernel.CodeRevisionConflict) {
		t.Fatalf("stale transition = %v, want revision_conflict", err)
	}
}

func TestPostgresInvocationStorePersistsConsumerScope(t *testing.T) {
	db := newFakeRuntimeDB()
	store := NewPostgresInvocationStore(db)
	invocation := validContextInvocation("retrieve")
	invocation.ConsumerInvocationID = "inv-phase"
	invocation.ConsumerTaskID = "task-a"
	invocation.ConsumerRole = auth.RoleExecutor

	if err := store.Create(context.Background(), invocation); err != nil {
		t.Fatalf("create context retrieve: %v", err)
	}
	got, ok, err := store.Get(context.Background(), invocation.ID)
	if err != nil || !ok {
		t.Fatalf("get = %#v %v %v, want invocation", got, ok, err)
	}
	if got.ConsumerInvocationID != "inv-phase" || got.ConsumerTaskID != "task-a" || got.ConsumerRole != auth.RoleExecutor {
		t.Fatalf("consumer scope lost: %#v", got)
	}
}

type fakeRuntimeDB struct {
	rows map[kernel.InvocationID]fakeInvocationRow
}

type fakeInvocationRow struct {
	invocation  Invocation
	fingerprint string
}

func newFakeRuntimeDB() *fakeRuntimeDB {
	return &fakeRuntimeDB{rows: map[kernel.InvocationID]fakeInvocationRow{}}
}

func (db *fakeRuntimeDB) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	switch {
	case strings.Contains(query, "INSERT INTO runtime_invocations"):
		invocation, fingerprint, err := invocationFromInsertArgs(args)
		if err != nil {
			return fakeResult(0), err
		}
		if _, ok := db.rows[invocation.ID]; ok {
			return fakeResult(0), nil
		}
		db.rows[invocation.ID] = fakeInvocationRow{invocation: cloneInvocation(invocation), fingerprint: fingerprint}
		return fakeResult(1), nil
	case strings.Contains(query, "UPDATE runtime_invocations"):
		id := args[0].(kernel.InvocationID)
		from := args[1].(InvocationStatus)
		to := args[2].(InvocationStatus)
		row, ok := db.rows[id]
		if !ok || row.invocation.Status != from {
			return fakeResult(0), nil
		}
		row.invocation.Status = to
		db.rows[id] = row
		return fakeResult(1), nil
	default:
		return fakeResult(0), nil
	}
}

func (db *fakeRuntimeDB) QueryContext(_ context.Context, query string, args ...any) (RowsScanner, error) {
	if strings.Contains(query, "WHERE lease_id = $1") {
		lease := args[0].(kernel.LeaseID)
		var rows [][]any
		for _, row := range db.rows {
			if row.invocation.LeaseID == lease {
				rows = append(rows, invocationScanValues(row.invocation))
			}
		}
		return &fakeRows{rows: rows}, nil
	}
	return &fakeRows{}, nil
}

func (db *fakeRuntimeDB) QueryRowContext(_ context.Context, query string, args ...any) RowScanner {
	if strings.Contains(query, "SELECT COALESCE(invocation_fingerprint") {
		id := args[0].(kernel.InvocationID)
		row, ok := db.rows[id]
		if !ok {
			return fakeRow{err: sql.ErrNoRows}
		}
		return fakeRow{values: []any{row.fingerprint}}
	}
	if strings.Contains(query, "WHERE invocation_id = $1") {
		id := args[0].(kernel.InvocationID)
		row, ok := db.rows[id]
		if !ok {
			return fakeRow{err: sql.ErrNoRows}
		}
		return fakeRow{values: invocationScanValues(row.invocation)}
	}
	return fakeRow{err: sql.ErrNoRows}
}

func invocationFromInsertArgs(args []any) (Invocation, string, error) {
	invocation := Invocation{
		ID:                   args[0].(kernel.InvocationID),
		ActorPrincipalID:     args[1].(kernel.ActorPrincipalID),
		ProjectID:            args[2].(kernel.ProjectID),
		TaskID:               args[3].(kernel.TaskID),
		EndpointID:           args[4].(kernel.EndpointID),
		Generation:           uint64(args[5].(int64)),
		Role:                 args[6].(auth.Role),
		Operation:            args[7].(string),
		Status:               args[8].(InvocationStatus),
		BindingRef:           args[9].(kernel.BindingRef),
		LeaseID:              args[10].(kernel.LeaseID),
		WorkspaceRef:         args[11].(string),
		ContextSliceRef:      args[12].(string),
		TaskMemoryBufferRef:  args[13].(string),
		ConsumerInvocationID: args[14].(kernel.InvocationID),
		ConsumerTaskID:       args[15].(kernel.TaskID),
		ConsumerRole:         args[16].(auth.Role),
		CreatedAt:            args[21].(time.Time),
		ExpiresAt:            args[22].(time.Time),
	}
	if err := json.Unmarshal([]byte(args[17].(string)), &invocation.PromptHashes); err != nil {
		return Invocation{}, "", err
	}
	if err := json.Unmarshal([]byte(args[18].(string)), &invocation.SkillHashes); err != nil {
		return Invocation{}, "", err
	}
	if err := json.Unmarshal([]byte(args[19].(string)), &invocation.EffectiveTools); err != nil {
		return Invocation{}, "", err
	}
	return invocation, args[20].(string), nil
}

func invocationScanValues(invocation Invocation) []any {
	promptHashes, _ := json.Marshal(invocation.PromptHashes)
	skillHashes, _ := json.Marshal(invocation.SkillHashes)
	effectiveTools, _ := json.Marshal(invocation.EffectiveTools)
	return []any{
		invocation.ID,
		invocation.ActorPrincipalID,
		invocation.ProjectID,
		invocation.TaskID,
		invocation.EndpointID,
		int64(invocation.Generation),
		invocation.Role,
		invocation.Operation,
		invocation.Status,
		invocation.BindingRef,
		invocation.LeaseID,
		invocation.WorkspaceRef,
		invocation.ContextSliceRef,
		invocation.TaskMemoryBufferRef,
		invocation.ConsumerInvocationID,
		invocation.ConsumerTaskID,
		invocation.ConsumerRole,
		promptHashes,
		skillHashes,
		effectiveTools,
		invocation.CreatedAt,
		invocation.ExpiresAt,
	}
}

type fakeResult int64

func (r fakeResult) LastInsertId() (int64, error) { return 0, nil }
func (r fakeResult) RowsAffected() (int64, error) { return int64(r), nil }

type fakeRow struct {
	values []any
	err    error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	return scanFakeValues(r.values, dest)
}

type fakeRows struct {
	rows   [][]any
	cursor int
	err    error
}

func (r *fakeRows) Next() bool {
	if r.cursor >= len(r.rows) {
		return false
	}
	r.cursor++
	return true
}

func (r *fakeRows) Scan(dest ...any) error {
	return scanFakeValues(r.rows[r.cursor-1], dest)
}

func (r *fakeRows) Close() error { return nil }
func (r *fakeRows) Err() error   { return r.err }

func scanFakeValues(values []any, dest []any) error {
	for i := range values {
		switch target := dest[i].(type) {
		case *string:
			*target = values[i].(string)
		case *kernel.InvocationID:
			*target = values[i].(kernel.InvocationID)
		case *kernel.ActorPrincipalID:
			*target = values[i].(kernel.ActorPrincipalID)
		case *kernel.ProjectID:
			*target = values[i].(kernel.ProjectID)
		case *kernel.TaskID:
			*target = values[i].(kernel.TaskID)
		case *kernel.EndpointID:
			*target = values[i].(kernel.EndpointID)
		case *kernel.BindingRef:
			*target = values[i].(kernel.BindingRef)
		case *kernel.LeaseID:
			*target = values[i].(kernel.LeaseID)
		case *auth.Role:
			*target = values[i].(auth.Role)
		case *InvocationStatus:
			*target = values[i].(InvocationStatus)
		case *int64:
			*target = values[i].(int64)
		case *[]byte:
			*target = append((*target)[:0], values[i].([]byte)...)
		case *time.Time:
			*target = values[i].(time.Time)
		default:
			return kernel.Error{Code: kernel.CodeInternalError, Message: "unsupported fake scan target", Recoverable: false}
		}
	}
	return nil
}
