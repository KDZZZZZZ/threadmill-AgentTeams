package agentteams_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	adapter "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/adapters/agentteams"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

func TestPostgresExecutionStoreReserveCreatesReleaseWaitAttempt(t *testing.T) {
	ref := "execution://wait/1"
	db := openScriptDB(t, []sqlStep{
		beginStep(false),
		queryStep("FOR UPDATE", []driver.Value{ref}, [][]driver.Value{{
			ref, int64(1), "inv-wait", "threadmill-first", "worker-a", "fp-a", "terminated", "release_wait",
		}}),
		execStep("INSERT INTO agentteams_execution_refs", []driver.Value{
			ref, int64(2), "inv-wait", "threadmill-72a745b76bcb0c8b3efeba1000e056f9-attempt-2", "worker-b", "fp-a",
		}, 1),
		commitStep(),
	})
	store := adapter.NewPostgresExecutionStore(db)

	record, created, err := store.Reserve(context.Background(), ref, "fp-a", adapter.AgentTeamsExecutionRef{
		InvocationID: "inv-wait",
		HostRef:      " worker-b ",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !created || record.Attempt != 2 || record.Execution.AgentTeamsTaskID != "threadmill-72a745b76bcb0c8b3efeba1000e056f9-attempt-2" {
		t.Fatalf("Reserve() = %#v created %v, want second attempt", record, created)
	}
	assertScriptDone(t, db)
}

func TestPostgresExecutionStoreReserveDetectsFingerprintConflict(t *testing.T) {
	ref := "execution://conflict/1"
	db := openScriptDB(t, []sqlStep{
		beginStep(false),
		queryStep("FOR UPDATE", []driver.Value{ref}, [][]driver.Value{{
			ref, int64(1), "inv-a", "threadmill-first", "worker-a", "fp-old", "reserved", "",
		}}),
		rollbackStep(),
	})
	store := adapter.NewPostgresExecutionStore(db)

	_, _, err := store.Reserve(context.Background(), ref, "fp-new", adapter.AgentTeamsExecutionRef{
		InvocationID: "inv-a",
		HostRef:      "worker-a",
	})
	if !kernel.IsCode(err, kernel.CodeIdempotencyConflict) {
		t.Fatalf("Reserve() error = %v, want idempotency_conflict", err)
	}
	assertScriptDone(t, db)
}

func TestPostgresExecutionStoreMarkDispatchedRejectsTerminatedAttempt(t *testing.T) {
	db := openScriptDB(t, []sqlStep{
		beginStep(false),
		queryStep("FOR UPDATE", []driver.Value{"threadmill-old"}, [][]driver.Value{{
			"execution://old/1", int64(1), "inv-old", "threadmill-old", "worker-a", "fp-a", "terminated", "release_wait",
		}}),
		rollbackStep(),
	})
	store := adapter.NewPostgresExecutionStore(db)

	err := store.MarkDispatched(context.Background(), "threadmill-old")
	if !kernel.IsCode(err, kernel.CodeStaleCommand) {
		t.Fatalf("MarkDispatched() error = %v, want stale_command", err)
	}
	assertScriptDone(t, db)
}

func TestPostgresExecutionStoreGetByTaskIDReadsHistoricalAttempt(t *testing.T) {
	db := openScriptDB(t, []sqlStep{
		beginStep(true),
		queryStep("WHERE agentteams_task_id", []driver.Value{"threadmill-attempt-1"}, [][]driver.Value{{
			"execution://history/1", int64(1), "inv-history", "threadmill-attempt-1", "worker-a", "fp-a", "terminated", "release_wait",
		}}),
		commitStep(),
	})
	store := adapter.NewPostgresExecutionStore(db)

	record, ok, err := store.GetByTaskID(context.Background(), "threadmill-attempt-1")
	if err != nil || !ok {
		t.Fatalf("GetByTaskID() = %#v %v, %v; want historical record", record, ok, err)
	}
	if record.Attempt != 1 || record.TerminationMode != "release_wait" {
		t.Fatalf("historical record = %#v, want attempt 1 release_wait", record)
	}
	assertScriptDone(t, db)
}

type sqlStep struct {
	op       string
	contains string
	args     []driver.Value
	rows     [][]driver.Value
	affected int64
	readOnly bool
}

func beginStep(readOnly bool) sqlStep {
	return sqlStep{op: "begin", readOnly: readOnly}
}

func queryStep(contains string, args []driver.Value, rows [][]driver.Value) sqlStep {
	return sqlStep{op: "query", contains: contains, args: args, rows: rows}
}

func execStep(contains string, args []driver.Value, affected int64) sqlStep {
	return sqlStep{op: "exec", contains: contains, args: args, affected: affected}
}

func commitStep() sqlStep {
	return sqlStep{op: "commit"}
}

func rollbackStep() sqlStep {
	return sqlStep{op: "rollback"}
}

var scriptDriverRegistry = struct {
	sync.Mutex
	once    sync.Once
	next    int
	scripts map[string]*scriptState
	dbs     map[*sql.DB]*scriptState
}{scripts: map[string]*scriptState{}, dbs: map[*sql.DB]*scriptState{}}

func openScriptDB(t *testing.T, steps []sqlStep) *sql.DB {
	t.Helper()
	scriptDriverRegistry.once.Do(func() {
		sql.Register("agentteams_script", scriptDriver{})
	})
	scriptDriverRegistry.Lock()
	dsn := fmt.Sprintf("script-%d", scriptDriverRegistry.next)
	scriptDriverRegistry.next++
	scriptDriverRegistry.scripts[dsn] = &scriptState{steps: steps}
	scriptDriverRegistry.Unlock()
	db, err := sql.Open("agentteams_script", dsn)
	if err != nil {
		t.Fatal(err)
	}
	scriptDriverRegistry.Lock()
	scriptDriverRegistry.dbs[db] = scriptDriverRegistry.scripts[dsn]
	scriptDriverRegistry.Unlock()
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func assertScriptDone(t *testing.T, db *sql.DB) {
	t.Helper()
	scriptDriverRegistry.Lock()
	state := scriptDriverRegistry.dbs[db]
	scriptDriverRegistry.Unlock()
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.index != len(state.steps) {
		t.Fatalf("script consumed %d/%d steps", state.index, len(state.steps))
	}
}

type scriptDriver struct{}

func (scriptDriver) Open(name string) (driver.Conn, error) {
	scriptDriverRegistry.Lock()
	state := scriptDriverRegistry.scripts[name]
	scriptDriverRegistry.Unlock()
	if state == nil {
		return nil, fmt.Errorf("unknown script %s", name)
	}
	return &scriptConn{state: state}, nil
}

type scriptState struct {
	mu    sync.Mutex
	steps []sqlStep
	index int
}

func (s *scriptState) next(op string) (sqlStep, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.index >= len(s.steps) {
		return sqlStep{}, fmt.Errorf("unexpected %s after script end", op)
	}
	step := s.steps[s.index]
	s.index++
	if step.op != op {
		return sqlStep{}, fmt.Errorf("step %d = %s, want %s", s.index-1, step.op, op)
	}
	return step, nil
}

type scriptConn struct {
	state *scriptState
}

func (c *scriptConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare is not supported")
}

func (c *scriptConn) Close() error {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	if c.state.index != len(c.state.steps) {
		return fmt.Errorf("script consumed %d/%d steps", c.state.index, len(c.state.steps))
	}
	return nil
}

func (c *scriptConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *scriptConn) BeginTx(_ context.Context, opts driver.TxOptions) (driver.Tx, error) {
	step, err := c.state.next("begin")
	if err != nil {
		return nil, err
	}
	if step.readOnly != opts.ReadOnly {
		return nil, fmt.Errorf("begin readOnly = %v, want %v", opts.ReadOnly, step.readOnly)
	}
	return &scriptTx{state: c.state}, nil
}

func (c *scriptConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	step, err := c.state.next("query")
	if err != nil {
		return nil, err
	}
	if !strings.Contains(query, step.contains) {
		return nil, fmt.Errorf("query %q does not contain %q", query, step.contains)
	}
	if err := compareArgs(step.args, args); err != nil {
		return nil, err
	}
	return &scriptRows{rows: step.rows}, nil
}

func (c *scriptConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	step, err := c.state.next("exec")
	if err != nil {
		return nil, err
	}
	if !strings.Contains(query, step.contains) {
		return nil, fmt.Errorf("exec %q does not contain %q", query, step.contains)
	}
	if err := compareArgs(step.args, args); err != nil {
		return nil, err
	}
	return driver.RowsAffected(step.affected), nil
}

type scriptTx struct {
	state *scriptState
}

func (tx *scriptTx) Commit() error {
	_, err := tx.state.next("commit")
	return err
}

func (tx *scriptTx) Rollback() error {
	_, err := tx.state.next("rollback")
	return err
}

type scriptRows struct {
	rows  [][]driver.Value
	index int
}

func (r *scriptRows) Columns() []string {
	return []string{
		"invocation_ref", "attempt", "invocation_id", "agentteams_task_id",
		"host_ref", "dispatch_fingerprint", "state", "termination_mode",
	}
}

func (r *scriptRows) Close() error {
	return nil
}

func (r *scriptRows) Next(dest []driver.Value) error {
	if r.index >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.index])
	r.index++
	return nil
}

func compareArgs(want []driver.Value, got []driver.NamedValue) error {
	if len(want) != len(got) {
		return fmt.Errorf("args = %v, want %v", got, want)
	}
	for i := range want {
		if got[i].Value != want[i] {
			return fmt.Errorf("arg %d = %v, want %v", i, got[i].Value, want[i])
		}
	}
	return nil
}
