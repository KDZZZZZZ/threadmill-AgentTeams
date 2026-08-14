package scheduler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

// RuntimeBudgetLedgerID is the single project-level budget consumed by the
// Graph Runtime scheduler. The database key keeps ledger_id so future scoped
// ledgers can be added without changing this production scheduling contract.
const RuntimeBudgetLedgerID = "runtime"

type PostgresCapacityLedger struct {
	db        *sql.DB
	projectID kernel.ProjectID
}

type BudgetState struct {
	LedgerID string
	Policy   BudgetPolicy
	Status   BudgetStatus
	Revision int
}

type PostgresBudgetLedger struct {
	db        *sql.DB
	projectID kernel.ProjectID
}

type PostgresSchedulingStateProvider struct {
	db        *sql.DB
	projectID kernel.ProjectID
}

type schedulerQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func NewPostgresCapacityLedger(db *sql.DB, projectID kernel.ProjectID) *PostgresCapacityLedger {
	return &PostgresCapacityLedger{db: db, projectID: projectID}
}

// Ensure creates the project's capacity row once. A concurrent or restarted
// caller reads the existing row instead of overwriting live scheduler facts.
func (l *PostgresCapacityLedger) Ensure(ctx context.Context, healthy, desired int) (Capacity, error) {
	if err := validatePostgresLedger(l.db, l.projectID); err != nil {
		return Capacity{}, err
	}
	if healthy < 0 || desired < 0 {
		return Capacity{}, kernel.InvalidArgument("initial capacity requires healthy and desired to be zero or greater")
	}
	if _, err := l.db.ExecContext(ctx, `
INSERT INTO scheduler_capacity_ledger (
    project_id, desired_concurrency, healthy_capacity, active_invocations, revision
) VALUES ($1, $2, $3, 0, 1)
ON CONFLICT (project_id) DO NOTHING`, l.projectID, desired, healthy); err != nil {
		return Capacity{}, fmt.Errorf("ensure scheduler capacity: %w", err)
	}
	return snapshotPostgresCapacity(ctx, l.db, l.projectID)
}

func (l *PostgresCapacityLedger) Snapshot(ctx context.Context) (Capacity, error) {
	if err := validatePostgresLedger(l.db, l.projectID); err != nil {
		return Capacity{}, err
	}
	return snapshotPostgresCapacity(ctx, l.db, l.projectID)
}

// SetDesired applies a compare-and-swap update. Desired capacity may exceed
// current healthy capacity; availability remains min(desired, healthy)-active.
func (l *PostgresCapacityLedger) SetDesired(ctx context.Context, expectedRevision, desired int) (Capacity, error) {
	if err := validatePostgresLedger(l.db, l.projectID); err != nil {
		return Capacity{}, err
	}
	if expectedRevision <= 0 {
		return Capacity{}, kernel.InvalidArgument("expected capacity revision must be positive")
	}
	if desired < 0 {
		return Capacity{}, kernel.InvalidArgument("desired concurrency must be zero or greater")
	}
	var next Capacity
	err := l.db.QueryRowContext(ctx, `
UPDATE scheduler_capacity_ledger
SET desired_concurrency = $3,
    revision = revision + 1,
    updated_at = now()
WHERE project_id = $1 AND revision = $2
RETURNING desired_concurrency, healthy_capacity, active_invocations, revision`,
		l.projectID, expectedRevision, desired,
	).Scan(&next.Desired, &next.Healthy, &next.Active, &next.Revision)
	if err == nil {
		return next, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Capacity{}, fmt.Errorf("set desired scheduler capacity: %w", err)
	}
	current, snapshotErr := snapshotPostgresCapacity(ctx, l.db, l.projectID)
	if snapshotErr != nil {
		return Capacity{}, snapshotErr
	}
	return current, kernel.RevisionConflict(kernel.Revision(expectedRevision), kernel.Revision(current.Revision))
}

// Observe atomically records scheduler-owned worker health and active
// invocation facts. It does not read or change desired capacity, so a desired
// decrease never cancels already active work.
func (l *PostgresCapacityLedger) Observe(ctx context.Context, healthy, active int) (Capacity, error) {
	if err := validatePostgresLedger(l.db, l.projectID); err != nil {
		return Capacity{}, err
	}
	if healthy < 0 || active < 0 || active > healthy {
		return Capacity{}, kernel.InvalidArgument("observed capacity requires 0 <= active <= healthy")
	}
	var next Capacity
	err := l.db.QueryRowContext(ctx, `
UPDATE scheduler_capacity_ledger
SET healthy_capacity = $2,
    active_invocations = $3,
    revision = revision + 1,
    updated_at = now()
WHERE project_id = $1
  AND (healthy_capacity, active_invocations) IS DISTINCT FROM ($2, $3)
RETURNING desired_concurrency, healthy_capacity, active_invocations, revision`,
		l.projectID, healthy, active,
	).Scan(&next.Desired, &next.Healthy, &next.Active, &next.Revision)
	if errors.Is(err, sql.ErrNoRows) {
		return snapshotPostgresCapacity(ctx, l.db, l.projectID)
	}
	if err != nil {
		return Capacity{}, fmt.Errorf("observe scheduler capacity: %w", err)
	}
	return next, nil
}

func NewPostgresBudgetLedger(db *sql.DB, projectID kernel.ProjectID) *PostgresBudgetLedger {
	return &PostgresBudgetLedger{db: db, projectID: projectID}
}

// Ensure creates the canonical project runtime budget once. Zero limits are
// stored as NULL and mean unlimited, matching BudgetPolicy semantics.
func (l *PostgresBudgetLedger) Ensure(ctx context.Context, policy BudgetPolicy, status BudgetStatus) (BudgetState, error) {
	if err := validatePostgresLedger(l.db, l.projectID); err != nil {
		return BudgetState{}, err
	}
	if err := validateBudgetPolicy(policy); err != nil {
		return BudgetState{}, err
	}
	if err := validateBudgetStatus(status); err != nil {
		return BudgetState{}, err
	}
	if _, err := l.db.ExecContext(ctx, `
INSERT INTO scheduler_budget_ledger (
    project_id, ledger_id,
    max_tokens, max_cost_usd, max_wall_time_ms, max_agent_invocations, max_retries,
    verify_level, exploration_level,
    tokens_used, cost_usd, wall_time_ms, agent_invocations_used, retries_used,
    revision
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, 1)
ON CONFLICT (project_id, ledger_id) DO NOTHING`,
		l.projectID, RuntimeBudgetLedgerID,
		nullablePositiveInt(policy.MaxTokens), nullablePositiveFloat(policy.MaxCostUSD),
		nullablePositiveInt(policy.MaxWallTimeMS), nullablePositiveInt(policy.MaxAgentInvocations),
		nullablePositiveInt(policy.MaxRetries), policy.VerifyLevel, policy.ExplorationLevel,
		status.TokensUsed, status.CostUSD, status.WallTimeMS,
		status.AgentInvocationsUsed, status.RetriesUsed,
	); err != nil {
		return BudgetState{}, fmt.Errorf("ensure scheduler budget: %w", err)
	}
	return snapshotPostgresBudget(ctx, l.db, l.projectID)
}

func (l *PostgresBudgetLedger) Snapshot(ctx context.Context) (BudgetState, error) {
	if err := validatePostgresLedger(l.db, l.projectID); err != nil {
		return BudgetState{}, err
	}
	return snapshotPostgresBudget(ctx, l.db, l.projectID)
}

func (l *PostgresBudgetLedger) Observe(ctx context.Context, status BudgetStatus) (BudgetState, error) {
	if err := validatePostgresLedger(l.db, l.projectID); err != nil {
		return BudgetState{}, err
	}
	if err := validateBudgetStatus(status); err != nil {
		return BudgetState{}, err
	}
	state, err := scanPostgresBudget(l.db.QueryRowContext(ctx, `
UPDATE scheduler_budget_ledger
SET tokens_used = $3,
    cost_usd = $4,
    wall_time_ms = $5,
    agent_invocations_used = $6,
    retries_used = $7,
    revision = revision + 1,
    updated_at = now()
WHERE project_id = $1 AND ledger_id = $2
RETURNING ledger_id,
          max_tokens, max_cost_usd, max_wall_time_ms, max_agent_invocations, max_retries,
          verify_level, exploration_level,
          tokens_used, cost_usd, wall_time_ms, agent_invocations_used, retries_used,
          revision`,
		l.projectID, RuntimeBudgetLedgerID,
		status.TokensUsed, status.CostUSD, status.WallTimeMS,
		status.AgentInvocationsUsed, status.RetriesUsed,
	))
	if errors.Is(err, sql.ErrNoRows) {
		return BudgetState{}, schedulerLedgerNotFound("budget", l.projectID)
	}
	if err != nil {
		return BudgetState{}, fmt.Errorf("observe scheduler budget: %w", err)
	}
	return state, nil
}

func NewPostgresSchedulingStateProvider(db *sql.DB, projectID kernel.ProjectID) *PostgresSchedulingStateProvider {
	return &PostgresSchedulingStateProvider{db: db, projectID: projectID}
}

// RuntimeSchedulingState reads capacity and budget from one repeatable-read
// transaction so a reconcile iteration never combines snapshots from two
// different database moments.
func (p *PostgresSchedulingStateProvider) RuntimeSchedulingState(ctx context.Context) (coordination.RuntimeSchedulingState, error) {
	if p == nil {
		return coordination.RuntimeSchedulingState{}, errors.New("postgres scheduling state provider is required")
	}
	if err := validatePostgresLedger(p.db, p.projectID); err != nil {
		return coordination.RuntimeSchedulingState{}, err
	}
	tx, err := p.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return coordination.RuntimeSchedulingState{}, fmt.Errorf("begin scheduler state snapshot: %w", err)
	}
	defer tx.Rollback()
	capacity, err := snapshotPostgresCapacity(ctx, tx, p.projectID)
	if err != nil {
		return coordination.RuntimeSchedulingState{}, err
	}
	budget, err := snapshotPostgresBudget(ctx, tx, p.projectID)
	if err != nil {
		return coordination.RuntimeSchedulingState{}, err
	}
	if err := tx.Commit(); err != nil {
		return coordination.RuntimeSchedulingState{}, fmt.Errorf("commit scheduler state snapshot: %w", err)
	}
	return coordination.RuntimeSchedulingState{Capacity: capacity, Budget: budget.Status}, nil
}

func snapshotPostgresCapacity(ctx context.Context, db schedulerQueryer, projectID kernel.ProjectID) (Capacity, error) {
	var capacity Capacity
	err := db.QueryRowContext(ctx, `
SELECT desired_concurrency, healthy_capacity, active_invocations, revision
FROM scheduler_capacity_ledger
WHERE project_id = $1`, projectID).Scan(
		&capacity.Desired,
		&capacity.Healthy,
		&capacity.Active,
		&capacity.Revision,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return Capacity{}, schedulerLedgerNotFound("capacity", projectID)
	}
	if err != nil {
		return Capacity{}, fmt.Errorf("read scheduler capacity: %w", err)
	}
	return capacity, nil
}

func snapshotPostgresBudget(ctx context.Context, db schedulerQueryer, projectID kernel.ProjectID) (BudgetState, error) {
	state, err := scanPostgresBudget(db.QueryRowContext(ctx, `
SELECT ledger_id,
       max_tokens, max_cost_usd, max_wall_time_ms, max_agent_invocations, max_retries,
       verify_level, exploration_level,
       tokens_used, cost_usd, wall_time_ms, agent_invocations_used, retries_used,
       revision
FROM scheduler_budget_ledger
WHERE project_id = $1 AND ledger_id = $2`, projectID, RuntimeBudgetLedgerID))
	if errors.Is(err, sql.ErrNoRows) {
		return BudgetState{}, schedulerLedgerNotFound("budget", projectID)
	}
	if err != nil {
		return BudgetState{}, fmt.Errorf("read scheduler budget: %w", err)
	}
	return state, nil
}

func scanPostgresBudget(row *sql.Row) (BudgetState, error) {
	var state BudgetState
	var maxTokens, maxWallTimeMS, maxAgentInvocations, maxRetries sql.NullInt64
	var maxCostUSD sql.NullFloat64
	if err := row.Scan(
		&state.LedgerID,
		&maxTokens,
		&maxCostUSD,
		&maxWallTimeMS,
		&maxAgentInvocations,
		&maxRetries,
		&state.Policy.VerifyLevel,
		&state.Policy.ExplorationLevel,
		&state.Status.TokensUsed,
		&state.Status.CostUSD,
		&state.Status.WallTimeMS,
		&state.Status.AgentInvocationsUsed,
		&state.Status.RetriesUsed,
		&state.Revision,
	); err != nil {
		return BudgetState{}, err
	}
	state.Policy.MaxTokens = int(maxTokens.Int64)
	state.Policy.MaxCostUSD = maxCostUSD.Float64
	state.Policy.MaxWallTimeMS = int(maxWallTimeMS.Int64)
	state.Policy.MaxAgentInvocations = int(maxAgentInvocations.Int64)
	state.Policy.MaxRetries = int(maxRetries.Int64)
	return state, nil
}

func validatePostgresLedger(db *sql.DB, projectID kernel.ProjectID) error {
	if db == nil {
		return errors.New("scheduler postgres database is required")
	}
	return kernel.RequireID("project_id", projectID)
}

func validateBudgetPolicy(policy BudgetPolicy) error {
	if policy.MaxTokens < 0 || policy.MaxCostUSD < 0 || policy.MaxWallTimeMS < 0 || policy.MaxAgentInvocations < 0 || policy.MaxRetries < 0 {
		return kernel.InvalidArgument("budget limits cannot be negative")
	}
	if math.IsNaN(policy.MaxCostUSD) || math.IsInf(policy.MaxCostUSD, 0) {
		return kernel.InvalidArgument("budget cost limit must be finite")
	}
	switch policy.VerifyLevel {
	case VerifyBasic, VerifyStandard, VerifyStrict:
	default:
		return kernel.InvalidArgument("verify_level must be basic, standard, or strict")
	}
	switch policy.ExplorationLevel {
	case ExplorationNone, ExplorationTargeted, ExplorationBroad:
	default:
		return kernel.InvalidArgument("exploration_level must be none, targeted, or broad")
	}
	return nil
}

func validateBudgetStatus(status BudgetStatus) error {
	if status.TokensUsed < 0 || status.CostUSD < 0 || status.WallTimeMS < 0 || status.AgentInvocationsUsed < 0 || status.RetriesUsed < 0 {
		return kernel.InvalidArgument("budget usage cannot be negative")
	}
	if math.IsNaN(status.CostUSD) || math.IsInf(status.CostUSD, 0) {
		return kernel.InvalidArgument("budget cost usage must be finite")
	}
	return nil
}

func nullablePositiveInt(value int) any {
	if value == 0 {
		return nil
	}
	return value
}

func nullablePositiveFloat(value float64) any {
	if value == 0 {
		return nil
	}
	return value
}

func schedulerLedgerNotFound(kind string, projectID kernel.ProjectID) error {
	return kernel.Error{
		Code:        kernel.CodeNotFound,
		Message:     fmt.Sprintf("scheduler %s ledger not found", kind),
		Recoverable: true,
		Details:     map[string]string{"project_id": string(projectID)},
	}
}

var _ coordination.RuntimeSchedulingStateProvider = (*PostgresSchedulingStateProvider)(nil)
