package agentteams

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

type postgresExecutionDB interface {
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
}

type PostgresExecutionStore struct {
	db postgresExecutionDB
}

func NewPostgresExecutionStore(db postgresExecutionDB) *PostgresExecutionStore {
	return &PostgresExecutionStore{db: db}
}

func (s *PostgresExecutionStore) GetByInvocationRef(ctx context.Context, ref string) (executionRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return executionRecord{}, false, err
	}
	tx, err := s.beginRead(ctx)
	if err != nil {
		return executionRecord{}, false, err
	}
	defer tx.Rollback()
	record, ok, err := scanExecutionRecord(tx.QueryRowContext(ctx, `
SELECT invocation_ref, attempt, invocation_id, agentteams_task_id, host_ref, dispatch_fingerprint, state, COALESCE(termination_mode, '')
FROM agentteams_execution_refs
WHERE invocation_ref = $1
ORDER BY attempt DESC
LIMIT 1`, ref))
	if err != nil {
		return executionRecord{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return executionRecord{}, false, err
	}
	return record, ok, nil
}

func (s *PostgresExecutionStore) GetByTaskID(ctx context.Context, taskID string) (executionRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return executionRecord{}, false, err
	}
	tx, err := s.beginRead(ctx)
	if err != nil {
		return executionRecord{}, false, err
	}
	defer tx.Rollback()
	record, ok, err := scanExecutionRecord(tx.QueryRowContext(ctx, `
SELECT invocation_ref, attempt, invocation_id, agentteams_task_id, host_ref, dispatch_fingerprint, state, COALESCE(termination_mode, '')
FROM agentteams_execution_refs
WHERE agentteams_task_id = $1`, taskID))
	if err != nil {
		return executionRecord{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return executionRecord{}, false, err
	}
	return record, ok, nil
}

func (s *PostgresExecutionStore) Reserve(
	ctx context.Context,
	invocationRef string,
	fingerprint string,
	execution AgentTeamsExecutionRef,
) (executionRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return executionRecord{}, false, err
	}
	if strings.TrimSpace(invocationRef) == "" || strings.TrimSpace(fingerprint) == "" {
		return executionRecord{}, false, kernel.InvalidArgument("invocation_ref and dispatch fingerprint are required")
	}
	execution.HostRef = strings.TrimSpace(execution.HostRef)
	if execution.InvocationID == "" || execution.HostRef == "" {
		return executionRecord{}, false, kernel.InvalidArgument("AgentTeams invocation_id and host_ref are required")
	}
	tx, err := s.beginReserve(ctx)
	if err != nil {
		return executionRecord{}, false, err
	}
	defer tx.Rollback()
	// A row lock cannot serialize the first reservation because the row does
	// not exist yet. Lock the stable InvocationRef key for this transaction so
	// concurrent processes also converge on one attempt before reading state.
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, invocationRef); err != nil {
		return executionRecord{}, false, err
	}

	existing, found, err := scanExecutionRecord(tx.QueryRowContext(ctx, `
SELECT invocation_ref, attempt, invocation_id, agentteams_task_id, host_ref, dispatch_fingerprint, state, COALESCE(termination_mode, '')
FROM agentteams_execution_refs
WHERE invocation_ref = $1
ORDER BY attempt DESC
LIMIT 1
FOR UPDATE`, invocationRef))
	if err != nil {
		return executionRecord{}, false, err
	}
	if found {
		if existing.Fingerprint != fingerprint || existing.Execution.InvocationID != execution.InvocationID {
			return executionRecord{}, false, kernel.IdempotencyConflict()
		}
		if existing.State != executionTerminated || existing.TerminationMode != TerminateReleaseWait {
			if err := tx.Commit(); err != nil {
				return executionRecord{}, false, err
			}
			return existing, false, nil
		}
		execution.AgentTeamsTaskID = attemptedTaskID(invocationRef, existing.Attempt+1)
		record, err := insertExecutionRecord(ctx, tx, invocationRef, existing.Attempt+1, fingerprint, execution)
		if err != nil {
			return executionRecord{}, false, err
		}
		if err := tx.Commit(); err != nil {
			return executionRecord{}, false, err
		}
		return record, true, nil
	}

	execution.AgentTeamsTaskID = attemptedTaskID(invocationRef, 1)
	record, err := insertExecutionRecord(ctx, tx, invocationRef, 1, fingerprint, execution)
	if err != nil {
		return executionRecord{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return executionRecord{}, false, err
	}
	return record, true, nil
}

func (s *PostgresExecutionStore) MarkDispatched(ctx context.Context, taskID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	record, ok, err := scanExecutionRecord(tx.QueryRowContext(ctx, `
SELECT invocation_ref, attempt, invocation_id, agentteams_task_id, host_ref, dispatch_fingerprint, state, COALESCE(termination_mode, '')
FROM agentteams_execution_refs
WHERE agentteams_task_id = $1
FOR UPDATE`, taskID))
	if err != nil {
		return err
	}
	if !ok {
		return kernel.Error{Code: kernel.CodeNotFound, Message: "AgentTeams execution reservation not found"}
	}
	if record.State == executionTerminated {
		return kernel.Error{Code: kernel.CodeStaleCommand, Message: "terminated AgentTeams execution cannot be dispatched", Recoverable: true}
	}
	if record.State == executionDispatched {
		return tx.Commit()
	}
	result, err := tx.ExecContext(ctx, `
UPDATE agentteams_execution_refs
SET state = 'dispatched', updated_at = now()
WHERE agentteams_task_id = $1 AND state = 'reserved'`, taskID)
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err == nil && affected != 1 {
		return kernel.Error{Code: kernel.CodeRevisionConflict, Message: "AgentTeams execution dispatch state changed", Recoverable: true}
	}
	return tx.Commit()
}

func (s *PostgresExecutionStore) MarkTerminated(ctx context.Context, taskID string, mode TerminateMode) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !validTerminateMode(mode) {
		return kernel.InvalidArgument("termination mode must be release_wait, recoverable_stop, or cancel")
	}
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	record, ok, err := scanExecutionRecord(tx.QueryRowContext(ctx, `
SELECT invocation_ref, attempt, invocation_id, agentteams_task_id, host_ref, dispatch_fingerprint, state, COALESCE(termination_mode, '')
FROM agentteams_execution_refs
WHERE agentteams_task_id = $1
FOR UPDATE`, taskID))
	if err != nil {
		return err
	}
	if !ok {
		return kernel.Error{Code: kernel.CodeNotFound, Message: "AgentTeams execution not found"}
	}
	if record.State == executionTerminated {
		if record.TerminationMode == mode {
			if _, err := tx.ExecContext(ctx, `
UPDATE agentteams_execution_refs
SET mcp_revoked_at = COALESCE(mcp_revoked_at, now()),
    host_slot_released_at = COALESCE(host_slot_released_at, now()),
    updated_at = now()
WHERE agentteams_task_id = $1`, taskID); err != nil {
				return err
			}
			return tx.Commit()
		}
		return kernel.IdempotencyConflict()
	}
	result, err := tx.ExecContext(ctx, `
UPDATE agentteams_execution_refs
SET state = 'terminated',
    termination_mode = $2,
    mcp_revoked_at = COALESCE(mcp_revoked_at, now()),
    host_slot_released_at = COALESCE(host_slot_released_at, now()),
    updated_at = now()
WHERE agentteams_task_id = $1 AND state <> 'terminated'`, taskID, mode)
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err == nil && affected != 1 {
		return kernel.Error{Code: kernel.CodeRevisionConflict, Message: "AgentTeams execution termination state changed", Recoverable: true}
	}
	return tx.Commit()
}

func (s *PostgresExecutionStore) beginRead(ctx context.Context) (*sql.Tx, error) {
	if s == nil || s.db == nil {
		return nil, kernel.Error{Code: kernel.CodeInternalError, Message: "Postgres execution store database is required", Recoverable: false}
	}
	return s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
}

func (s *PostgresExecutionStore) beginWrite(ctx context.Context) (*sql.Tx, error) {
	if s == nil || s.db == nil {
		return nil, kernel.Error{Code: kernel.CodeInternalError, Message: "Postgres execution store database is required", Recoverable: false}
	}
	return s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
}

func (s *PostgresExecutionStore) beginReserve(ctx context.Context) (*sql.Tx, error) {
	if s == nil || s.db == nil {
		return nil, kernel.Error{Code: kernel.CodeInternalError, Message: "Postgres execution store database is required", Recoverable: false}
	}
	// Read committed is intentional: a waiter must see the reservation that
	// committed while it waited for the advisory lock. Serializable would keep
	// the pre-wait snapshot and re-open the missing-row race.
	return s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
}

func insertExecutionRecord(
	ctx context.Context,
	tx *sql.Tx,
	invocationRef string,
	attempt int,
	fingerprint string,
	execution AgentTeamsExecutionRef,
) (executionRecord, error) {
	record := executionRecord{
		InvocationRef: invocationRef,
		Attempt:       attempt,
		Execution:     execution,
		Fingerprint:   fingerprint,
		State:         executionReserved,
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO agentteams_execution_refs (
  invocation_ref, attempt, invocation_id, agentteams_task_id, host_ref, dispatch_fingerprint, state
) VALUES ($1, $2, $3, $4, $5, $6, 'reserved')`,
		record.InvocationRef,
		record.Attempt,
		record.Execution.InvocationID,
		record.Execution.AgentTeamsTaskID,
		record.Execution.HostRef,
		record.Fingerprint,
	); err != nil {
		return executionRecord{}, err
	}
	return record, nil
}

func scanExecutionRecord(row interface{ Scan(...any) error }) (executionRecord, bool, error) {
	var record executionRecord
	var mode string
	if err := row.Scan(
		&record.InvocationRef,
		&record.Attempt,
		&record.Execution.InvocationID,
		&record.Execution.AgentTeamsTaskID,
		&record.Execution.HostRef,
		&record.Fingerprint,
		&record.State,
		&mode,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return executionRecord{}, false, nil
		}
		return executionRecord{}, false, err
	}
	record.TerminationMode = TerminateMode(mode)
	return record, true, nil
}

var _ ExecutionStore = (*PostgresExecutionStore)(nil)
