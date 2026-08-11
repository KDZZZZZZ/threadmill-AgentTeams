package coordination

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/postgres"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/migrations"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestPostgresWriteGraphTablesUsesAuthoritativeSnapshotShape(t *testing.T) {
	rec := &recordingDBTX{}
	snapshot := GraphSnapshot{
		Revision: 2,
		Tasks: []Task{{
			ID:          "task-a",
			ContractRef: "contract://task-a",
			Outcome:     TaskActive,
		}},
		Endpoints: []PhaseEndpoint{
			endpoint("task-a", EndpointPlan),
			endpoint("task-a", EndpointExecute),
			endpoint("task-a", EndpointVerify),
		},
		Edges: []Edge{{
			From:          ref("task-a", EndpointPlan),
			To:            ref("task-a", EndpointExecute),
			Signal:        SignalPhaseSatisfied,
			RequiredBy:    RequiredByStart,
			ArtifactKinds: []string{`quote"kind`, `path\kind`},
			OnFalse:       OnFalseBlock,
		}},
		Blockers: []Blocker{{
			ID:         "blocker-a",
			Target:     ref("task-a", EndpointExecute),
			RequiredBy: RequiredByStart,
			OnFalse:    OnFalseBlock,
			State:      BlockerActive,
		}},
		Results: []PhaseResult{{
			ID:         "result-a",
			Endpoint:   ref("task-a", EndpointPlan),
			BindingRef: "binding://task-a/plan/1",
			OutputRef:  "artifact://task-a/plan",
			Verdict:    VerdictSubmitted,
		}},
	}

	if err := writeGraphTables(context.Background(), rec, "project-a", snapshot); err != nil {
		t.Fatal(err)
	}
	if len(rec.execs) != 3+len(snapshot.Tasks)+len(snapshot.Endpoints)+len(snapshot.Edges)+len(snapshot.Blockers)+len(snapshot.Results) {
		t.Fatalf("exec count = %d, want deletes plus all snapshot rows; execs=%#v", len(rec.execs), rec.execs)
	}
	if !strings.Contains(rec.execs[0].query, "DELETE FROM coordination_edges") ||
		!strings.Contains(rec.execs[2].query, "DELETE FROM coordination_phase_results") {
		t.Fatalf("delete order = %#v, want relation tables cleared without deleting task or endpoint rows", rec.execs[:3])
	}
	edgeExec := rec.execs[3+len(snapshot.Tasks)+len(snapshot.Endpoints)]
	if got := edgeExec.args[7]; got != `{"quote\"kind","path\\kind"}` {
		t.Fatalf("artifact array literal = %v, want escaped text[] literal", got)
	}
}

func TestPostgresDecodeSnapshotForcesStoredRevision(t *testing.T) {
	raw, err := json.Marshal(GraphSnapshot{
		Revision: 99,
		Tasks: []Task{{
			ID:          "task-a",
			ContractRef: "contract://task-a",
			Outcome:     TaskActive,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := decodeSnapshot(7, raw)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Revision != 7 {
		t.Fatalf("revision = %d, want database revision 7", snapshot.Revision)
	}
}

func TestPostgresErrorMapping(t *testing.T) {
	if err := mapPostgresError(&pgconn.PgError{Code: "23505", ConstraintName: "coordination_one_active_phase_lease"}); !kernel.IsCode(err, kernel.CodeLeaseConflict) {
		t.Fatalf("lease unique err = %v, want lease_conflict", err)
	}
	if err := mapPostgresError(&pgconn.PgError{Code: "23505", ConstraintName: "coordination_graph_revisions_pkey"}); !kernel.IsCode(err, kernel.CodeRevisionConflict) {
		t.Fatalf("revision unique err = %v, want revision_conflict", err)
	}
	if err := mapPostgresError(&pgconn.PgError{Code: "23503"}); !kernel.IsCode(err, kernel.CodeInvalidGraph) {
		t.Fatalf("foreign key err = %v, want invalid_graph", err)
	}
	if err := mapPostgresError(&pgconn.PgError{Code: "40001"}); !kernel.IsCode(err, kernel.CodeRevisionConflict) {
		t.Fatalf("serialization err = %v, want revision_conflict", err)
	}
}

func TestPostgresStoreRealMigrationCASConcurrencyAndRestart(t *testing.T) {
	dsn := os.Getenv("THREADMILL_PG_TEST_DSN")
	if dsn == "" {
		t.Skip("THREADMILL_PG_TEST_DSN is not set")
	}
	ctx := context.Background()
	schema := fmt.Sprintf("coordination_it_%d", time.Now().UnixNano())
	baseDB, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer baseDB.Close()
	if _, err := baseDB.ExecContext(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	defer baseDB.ExecContext(context.Background(), `DROP SCHEMA `+schema+` CASCADE`)

	db, err := sql.Open("pgx", dsnWithSearchPath(t, dsn, schema))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	loaded, err := postgres.LoadMigrations(migrations.FS, migrations.Dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := postgres.NewMigrator(db).Apply(ctx, loaded); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	store := NewPostgresStore(db)
	graph := NewTaskManagerGraph(taskManagerPrincipal(), store, store, kernel.NewMemoryIdempotencyStore())
	revision := createPostgresTask(t, graph, store, "pg-task-a")
	if revision != 2 {
		t.Fatalf("initial revision = %d, want 2", revision)
	}
	if _, err := graph.ReplacePending(ctx, basicSubgraph("pg-task-b", "stale-decision", 1)); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("unauthorized stale replace = %v, want forbidden before graph write", err)
	}
	if err := store.RegisterReplacePending(ctx, projectID, "stale-decision"); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.ReplacePending(ctx, basicSubgraph("pg-task-b", "stale-decision", 1)); !kernel.IsCode(err, kernel.CodeRevisionConflict) {
		t.Fatalf("stale replace = %v, want revision_conflict", err)
	}

	controller := &recordingController{}
	runtime := newGraphRuntime(projectID, store, controller)
	const workers = 16
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- runtime.reconcile(ctx)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent reconcile failed: %v", err)
		}
	}
	commands := store.runtimeCommands(ctx, projectID)
	if len(commands) != 1 || commands[0].Action != CommandStart {
		t.Fatalf("commands after concurrent reconcile = %#v, want one start", commands)
	}
	if err := store.RecordPhaseInvocationStarted(ctx, projectID, commands[0]); err != nil {
		t.Fatalf("record started observation: %v", err)
	}
	if err := store.RecordPhaseInvocationStarted(ctx, projectID, commands[0]); err != nil {
		t.Fatalf("idempotent started observation: %v", err)
	}
	errs = make(chan error, workers)
	wg = sync.WaitGroup{}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- store.RecordPhaseInvocationStarted(ctx, projectID, commands[0])
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent idempotent started observation: %v", err)
		}
	}
	if err := runtime.reconcile(ctx); err != nil {
		t.Fatalf("fold started observation: %v", err)
	}
	if err := store.RecordPhaseInvocationStarted(ctx, projectID, commands[0]); err != nil {
		t.Fatalf("folded started replay should remain idempotent: %v", err)
	}
	if active := countPostgresActiveLeases(ctx, store, projectID); active != 1 {
		t.Fatalf("active leases = %d, want 1", active)
	}

	if err := store.RegisterTransition(ctx, projectID, "pg-hold-after-restart", GraphTransition{
		TargetKind: TargetPhaseEndpoint,
		Endpoint:   ref("pg-task-a", EndpointPlan),
		Action:     "held",
		Generation: 1,
	}); err != nil {
		t.Fatal(err)
	}
	revision = mustTransition(t, graph, revision, "pg-hold-after-restart")
	restartedController := &recordingController{}
	restarted := newGraphRuntime(projectID, NewPostgresStore(db), restartedController)
	if err := restarted.reconcile(ctx); err != nil {
		t.Fatalf("restarted reconcile: %v", err)
	}
	if got := restartedController.lastAction(); got != CommandStop {
		t.Fatalf("restart action = %s, want stop", got)
	}
	stop := restartedController.lastCommand()
	if stop.LeaseRef == "" || stop.Endpoint != ref("pg-task-a", EndpointPlan) {
		t.Fatalf("stop command = %#v, want persisted active lease stop", stop)
	}
	if err := store.RecordPhaseInvocationStopped(ctx, projectID, stop, "checkpoint://pg-task-a/plan/1", false); err != nil {
		t.Fatalf("record stopped observation: %v", err)
	}
	if err := store.RecordPhaseInvocationStopped(ctx, projectID, stop, "checkpoint://pg-task-a/plan/1", false); err != nil {
		t.Fatalf("idempotent stopped observation: %v", err)
	}
	errs = make(chan error, workers)
	wg = sync.WaitGroup{}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- store.RecordPhaseInvocationStopped(ctx, projectID, stop, "checkpoint://pg-task-a/plan/1", false)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent idempotent stopped observation: %v", err)
		}
	}
	if err := store.RecordPhaseInvocationStopped(ctx, projectID, stop, "checkpoint://pg-task-a/plan/other", false); !kernel.IsCode(err, kernel.CodeIdempotencyConflict) {
		t.Fatalf("conflicting stopped observation = %v, want idempotency_conflict", err)
	}
}

func TestPostgresStoreRejectsInvalidReplacePendingWithoutPartialWrites(t *testing.T) {
	dsn := os.Getenv("THREADMILL_PG_TEST_DSN")
	if dsn == "" {
		t.Skip("THREADMILL_PG_TEST_DSN is not set")
	}
	ctx := context.Background()
	schema := fmt.Sprintf("coordination_atomic_%d", time.Now().UnixNano())
	baseDB, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer baseDB.Close()
	if _, err := baseDB.ExecContext(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	defer baseDB.ExecContext(context.Background(), `DROP SCHEMA `+schema+` CASCADE`)

	db, err := sql.Open("pgx", dsnWithSearchPath(t, dsn, schema))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	loaded, err := postgres.LoadMigrations(migrations.FS, migrations.Dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := postgres.NewMigrator(db).Apply(ctx, loaded); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	store := NewPostgresStore(db)
	graph := NewTaskManagerGraph(taskManagerPrincipal(), store, store, kernel.NewMemoryIdempotencyStore())
	revision := createPostgresTask(t, graph, store, "pg-task-a")
	before := mustSnapshot(t, graph, revision)
	beforeCounts := postgresGraphTableCounts(t, ctx, db, projectID)

	runPolicyDecision := kernel.IdempotencyKey("decision-invalid-run-policy")
	if err := store.RegisterReplacePending(ctx, projectID, runPolicyDecision); err != nil {
		t.Fatal(err)
	}
	invalidRunPolicy := PendingSubgraph{
		RequestID:    runPolicyDecision,
		BaseRevision: revision,
		Endpoints:    []PhaseEndpoint{mustEndpoint(t, before, ref("pg-task-a", EndpointPlan))},
	}
	invalidRunPolicy.Endpoints[0].RunPolicy = RunHeld
	if _, err := graph.ReplacePending(ctx, invalidRunPolicy); !kernel.IsCode(err, kernel.CodeInvalidRequest) {
		t.Fatalf("invalid run_policy replace err = %v, want invalid_request", err)
	}
	assertPostgresGraphUnchanged(t, ctx, db, graph, projectID, before, beforeCounts)

	fixedEdgeDecision := kernel.IdempotencyKey("decision-invalid-fixed-edge")
	if err := store.RegisterReplacePending(ctx, projectID, fixedEdgeDecision); err != nil {
		t.Fatal(err)
	}
	invalidFixedEdge := basicSubgraph("pg-task-b", fixedEdgeDecision, revision)
	invalidFixedEdge.Edges = []Edge{edge(ref("pg-task-b", EndpointPlan), ref("pg-task-b", EndpointExecute))}
	if _, err := graph.ReplacePending(ctx, invalidFixedEdge); !kernel.IsCode(err, kernel.CodeInvalidGraph) {
		t.Fatalf("invalid fixed-edge replace err = %v, want invalid_graph", err)
	}
	assertPostgresGraphUnchanged(t, ctx, db, graph, projectID, before, beforeCounts)
}

type recordingDBTX struct {
	execs []recordedExec
}

type recordedExec struct {
	query string
	args  []any
}

func (r *recordingDBTX) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	r.execs = append(r.execs, recordedExec{query: query, args: append([]any(nil), args...)})
	return recordingResult(1), nil
}

func (r *recordingDBTX) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	return nil, nil
}

func (r *recordingDBTX) QueryRowContext(context.Context, string, ...any) *sql.Row {
	return nil
}

type recordingResult int64

func (r recordingResult) LastInsertId() (int64, error) { return 0, nil }
func (r recordingResult) RowsAffected() (int64, error) { return int64(r), nil }

func dsnWithSearchPath(t *testing.T, dsn, schema string) string {
	t.Helper()
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	query.Set("options", "-c search_path="+schema)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func createPostgresTask(t *testing.T, graph *Service, decisions *PostgresStore, taskID kernel.TaskID) kernel.Revision {
	t.Helper()
	requestID := kernel.IdempotencyKey("decision-create-" + string(taskID))
	base := latestRevision(t, graph)
	if err := decisions.RegisterReplacePending(context.Background(), projectID, requestID); err != nil {
		t.Fatal(err)
	}
	revision, err := graph.ReplacePending(context.Background(), basicSubgraph(taskID, requestID, base))
	if err != nil {
		t.Fatal(err)
	}
	return revision
}

func countPostgresActiveLeases(ctx context.Context, store *PostgresStore, projectID kernel.ProjectID) int {
	active := 0
	for _, lease := range store.runtimeLeases(ctx, projectID) {
		if lease.State == "active" {
			active++
		}
	}
	return active
}

type postgresGraphCounts struct {
	revisions int
	tasks     int
	endpoints int
	edges     int
	blockers  int
	results   int
}

func postgresGraphTableCounts(t *testing.T, ctx context.Context, db *sql.DB, projectID kernel.ProjectID) postgresGraphCounts {
	t.Helper()
	return postgresGraphCounts{
		revisions: countPostgresRows(t, ctx, db, `SELECT count(*) FROM coordination_graph_revisions WHERE project_id = $1`, projectID),
		tasks:     countPostgresRows(t, ctx, db, `SELECT count(*) FROM coordination_tasks WHERE project_id = $1`, projectID),
		endpoints: countPostgresRows(t, ctx, db, `SELECT count(*) FROM coordination_endpoints WHERE project_id = $1`, projectID),
		edges:     countPostgresRows(t, ctx, db, `SELECT count(*) FROM coordination_edges WHERE project_id = $1`, projectID),
		blockers:  countPostgresRows(t, ctx, db, `SELECT count(*) FROM coordination_blockers WHERE project_id = $1`, projectID),
		results:   countPostgresRows(t, ctx, db, `SELECT count(*) FROM coordination_phase_results WHERE project_id = $1`, projectID),
	}
}

func countPostgresRows(t *testing.T, ctx context.Context, db *sql.DB, query string, args ...any) int {
	t.Helper()
	var count int
	if err := db.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func assertPostgresGraphUnchanged(t *testing.T, ctx context.Context, db *sql.DB, graph *Service, projectID kernel.ProjectID, before GraphSnapshot, beforeCounts postgresGraphCounts) {
	t.Helper()
	after, err := graph.Snapshot(ctx, kernel.LatestRevision)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("latest graph changed after rejected replace:\nafter=%#v\nbefore=%#v", after, before)
	}
	afterCounts := postgresGraphTableCounts(t, ctx, db, projectID)
	if afterCounts != beforeCounts {
		t.Fatalf("graph table counts changed after rejected replace: after=%#v before=%#v", afterCounts, beforeCounts)
	}
}
