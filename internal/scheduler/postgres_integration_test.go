package scheduler

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	platformpostgres "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/postgres"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/migrations"
)

const schedulerIntegrationDatabaseEnv = "THREADMILL_TEST_DATABASE_URL"

func TestPostgresSchedulerLedgersAgainstRealDatabase(t *testing.T) {
	databaseURL := os.Getenv(schedulerIntegrationDatabaseEnv)
	if databaseURL == "" {
		t.Skip(schedulerIntegrationDatabaseEnv + " is required for the real PostgreSQL integration test")
	}
	ctx := context.Background()
	admin, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatalf("open PostgreSQL admin connection: %v", err)
	}
	defer admin.Close()
	if err := admin.PingContext(ctx); err != nil {
		t.Fatalf("ping PostgreSQL: %v", err)
	}

	schema := fmt.Sprintf("threadmill_scheduler_it_%d_%d", os.Getpid(), time.Now().UnixNano())
	if _, err := admin.ExecContext(ctx, `CREATE SCHEMA `+schedulerQuoteIdentifier(schema)); err != nil {
		t.Fatalf("create isolated schema: %v", err)
	}
	databaseURL, err = schedulerDatabaseURLWithSearchPath(databaseURL, schema)
	if err != nil {
		t.Fatalf("set schema in PostgreSQL URL: %v", err)
	}
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatalf("open isolated PostgreSQL connection: %v", err)
	}
	db.SetMaxOpenConns(24)
	db.SetMaxIdleConns(24)
	defer func() {
		_ = db.Close()
		_, _ = admin.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS `+schedulerQuoteIdentifier(schema)+` CASCADE`)
	}()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping isolated PostgreSQL connection: %v", err)
	}
	var currentSchema string
	if err := db.QueryRowContext(ctx, `SELECT current_schema()`).Scan(&currentSchema); err != nil {
		t.Fatalf("read current schema: %v", err)
	}
	if currentSchema != schema {
		t.Fatalf("current schema = %q, want %q", currentSchema, schema)
	}

	loaded, err := platformpostgres.LoadMigrations(migrations.FS, migrations.Dir)
	if err != nil {
		t.Fatalf("load full migrations: %v", err)
	}
	if err := platformpostgres.NewMigrator(db).Apply(ctx, loaded); err != nil {
		t.Fatalf("apply full migrations: %v", err)
	}
	assertCapacityMigrationFixed(t, ctx, db)

	projectID := kernel.ProjectID("project-real-scheduler")
	capacityLedger := NewPostgresCapacityLedger(db, projectID)
	capacity, err := capacityLedger.Ensure(ctx, 2, 5)
	if err != nil {
		t.Fatalf("ensure capacity: %v", err)
	}
	if capacity != (Capacity{Desired: 5, Healthy: 2, Active: 0, Revision: 1}) {
		t.Fatalf("initial capacity = %#v", capacity)
	}
	capacity, err = capacityLedger.Observe(ctx, 2, 0)
	if err != nil {
		t.Fatalf("repeat initial capacity observation: %v", err)
	}
	if capacity.Revision != 1 {
		t.Fatalf("no-op capacity observation revision = %d, want 1", capacity.Revision)
	}

	capacity, err = capacityLedger.Observe(ctx, 2, 2)
	if err != nil {
		t.Fatalf("observe active capacity: %v", err)
	}
	capacity, err = capacityLedger.SetDesired(ctx, capacity.Revision, 1)
	if err != nil {
		t.Fatalf("decrease desired capacity: %v", err)
	}
	if capacity.Desired != 1 || capacity.Active != 2 || capacity.Revision != 3 {
		t.Fatalf("draining capacity = %#v, want desired=1 active=2 revision=3", capacity)
	}
	stale, err := capacityLedger.SetDesired(ctx, 2, 7)
	if !kernel.IsCode(err, kernel.CodeRevisionConflict) {
		t.Fatalf("stale capacity CAS = %#v, %v; want revision_conflict", stale, err)
	}
	if stale != capacity {
		t.Fatalf("stale capacity CAS returned %#v, want current %#v", stale, capacity)
	}

	capacity = exerciseConcurrentCapacityCAS(t, ctx, capacityLedger, capacity)
	capacity = exerciseConcurrentCapacityObservation(t, ctx, capacityLedger, capacity)
	beforeInvalid := capacity
	if _, err := capacityLedger.Observe(ctx, 1, 2); !kernel.IsCode(err, kernel.CodeInvalidRequest) {
		t.Fatalf("invalid capacity observation error = %v, want invalid_request", err)
	}
	afterInvalid, err := capacityLedger.Snapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot after invalid observation: %v", err)
	}
	if afterInvalid != beforeInvalid {
		t.Fatalf("invalid observation changed capacity: before=%#v after=%#v", beforeInvalid, afterInvalid)
	}
	if _, err := db.ExecContext(ctx, `
UPDATE scheduler_capacity_ledger
SET active_invocations = healthy_capacity + 1
WHERE project_id = $1`, projectID); err == nil {
		t.Fatal("database accepted active_invocations > healthy_capacity")
	}

	restartedCapacityLedger := NewPostgresCapacityLedger(db, projectID)
	restartedCapacity, err := restartedCapacityLedger.Snapshot(ctx)
	if err != nil {
		t.Fatalf("read capacity after ledger rebuild: %v", err)
	}
	if restartedCapacity != capacity {
		t.Fatalf("capacity after rebuild = %#v, want %#v", restartedCapacity, capacity)
	}

	policy := BudgetPolicy{
		MaxTokens:           10_000,
		MaxCostUSD:          5.25,
		MaxWallTimeMS:       600_000,
		MaxAgentInvocations: 20,
		MaxRetries:          3,
		VerifyLevel:         VerifyStrict,
		ExplorationLevel:    ExplorationTargeted,
	}
	budgetLedger := NewPostgresBudgetLedger(db, projectID)
	budget, err := budgetLedger.Ensure(ctx, policy, BudgetStatus{TokensUsed: 100, CostUSD: 0.5})
	if err != nil {
		t.Fatalf("ensure budget: %v", err)
	}
	if budget.LedgerID != RuntimeBudgetLedgerID || budget.Policy != policy || budget.Revision != 1 {
		t.Fatalf("initial budget = %#v", budget)
	}
	updatedBudgetStatus := BudgetStatus{
		TokensUsed:           2_500,
		CostUSD:              1.75,
		WallTimeMS:           45_000,
		AgentInvocationsUsed: 4,
		RetriesUsed:          1,
	}
	budget, err = budgetLedger.Observe(ctx, updatedBudgetStatus)
	if err != nil {
		t.Fatalf("observe budget: %v", err)
	}
	if budget.Status != updatedBudgetStatus || budget.Revision != 2 {
		t.Fatalf("updated budget = %#v", budget)
	}

	restartedBudget, err := NewPostgresBudgetLedger(db, projectID).Snapshot(ctx)
	if err != nil {
		t.Fatalf("read budget after ledger rebuild: %v", err)
	}
	if restartedBudget != budget {
		t.Fatalf("budget after rebuild = %#v, want %#v", restartedBudget, budget)
	}
	provider := NewPostgresSchedulingStateProvider(db, projectID)
	runtimeState, err := provider.RuntimeSchedulingState(ctx)
	if err != nil {
		t.Fatalf("read runtime scheduling state: %v", err)
	}
	if runtimeState.Capacity != capacity || runtimeState.Budget != updatedBudgetStatus {
		t.Fatalf("runtime scheduling state = %#v, want capacity=%#v budget=%#v", runtimeState, capacity, updatedBudgetStatus)
	}
}

func exerciseConcurrentCapacityCAS(t *testing.T, ctx context.Context, ledger *PostgresCapacityLedger, before Capacity) Capacity {
	t.Helper()
	const writers = 12
	type outcome struct {
		capacity Capacity
		err      error
	}
	start := make(chan struct{})
	outcomes := make(chan outcome, writers)
	var ready sync.WaitGroup
	ready.Add(writers)
	for writer := 0; writer < writers; writer++ {
		desired := 10 + writer
		go func() {
			ready.Done()
			<-start
			capacity, err := ledger.SetDesired(ctx, before.Revision, desired)
			outcomes <- outcome{capacity: capacity, err: err}
		}()
	}
	ready.Wait()
	close(start)
	successes := 0
	for writer := 0; writer < writers; writer++ {
		outcome := <-outcomes
		if outcome.err == nil {
			successes++
			continue
		}
		if !kernel.IsCode(outcome.err, kernel.CodeRevisionConflict) {
			t.Fatalf("concurrent capacity CAS error = %v, want revision_conflict", outcome.err)
		}
	}
	if successes != 1 {
		t.Fatalf("concurrent capacity CAS successes = %d, want 1", successes)
	}
	after, err := ledger.Snapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot after concurrent CAS: %v", err)
	}
	if after.Revision != before.Revision+1 || after.Active != before.Active || after.Desired < 10 || after.Desired >= 10+writers {
		t.Fatalf("capacity after concurrent CAS = %#v, before %#v", after, before)
	}
	return after
}

func exerciseConcurrentCapacityObservation(t *testing.T, ctx context.Context, ledger *PostgresCapacityLedger, before Capacity) Capacity {
	t.Helper()
	const writers = 18
	type observation struct {
		healthy int
		active  int
	}
	observations := make(map[observation]struct{}, writers)
	start := make(chan struct{})
	errorsByWriter := make(chan error, writers)
	var ready sync.WaitGroup
	ready.Add(writers)
	for writer := 0; writer < writers; writer++ {
		observed := observation{healthy: 2 + writer%4, active: writer % 3}
		observations[observed] = struct{}{}
		go func() {
			ready.Done()
			<-start
			_, err := ledger.Observe(ctx, observed.healthy, observed.active)
			errorsByWriter <- err
		}()
	}
	ready.Wait()
	close(start)
	for writer := 0; writer < writers; writer++ {
		if err := <-errorsByWriter; err != nil {
			t.Fatalf("concurrent capacity observation: %v", err)
		}
	}
	after, err := ledger.Snapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot after concurrent observations: %v", err)
	}
	if after.Revision <= before.Revision || after.Revision > before.Revision+writers || after.Desired != before.Desired {
		t.Fatalf("capacity after concurrent observations = %#v, before %#v", after, before)
	}
	if _, ok := observations[observation{healthy: after.Healthy, active: after.Active}]; !ok {
		t.Fatalf("final capacity observation = healthy=%d active=%d, want one complete concurrent observation", after.Healthy, after.Active)
	}
	return after
}

func assertCapacityMigrationFixed(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	var legacyConstraints int
	if err := db.QueryRowContext(ctx, `
SELECT count(*)
FROM pg_constraint
WHERE conrelid = 'scheduler_capacity_ledger'::regclass
  AND contype = 'c'
  AND pg_get_constraintdef(oid) LIKE '%desired_concurrency <= healthy_capacity%'`).Scan(&legacyConstraints); err != nil {
		t.Fatalf("inspect capacity constraints: %v", err)
	}
	if legacyConstraints != 0 {
		t.Fatalf("legacy desired<=healthy constraints = %d, want 0", legacyConstraints)
	}
	var correctConstraints int
	if err := db.QueryRowContext(ctx, `
SELECT count(*)
FROM pg_constraint
WHERE conrelid = 'scheduler_capacity_ledger'::regclass
  AND conname = 'scheduler_capacity_active_within_healthy'`).Scan(&correctConstraints); err != nil {
		t.Fatalf("inspect active capacity constraint: %v", err)
	}
	if correctConstraints != 1 {
		t.Fatalf("active<=healthy constraints = %d, want 1", correctConstraints)
	}
}

func schedulerDatabaseURLWithSearchPath(databaseURL, schema string) (string, error) {
	parsed, err := url.Parse(databaseURL)
	if err != nil {
		return "", err
	}
	if parsed.Scheme != "postgres" && parsed.Scheme != "postgresql" {
		return "", fmt.Errorf("PostgreSQL integration URL must use postgres or postgresql scheme")
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func schedulerQuoteIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}
