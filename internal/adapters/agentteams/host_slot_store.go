package agentteams

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

type HostSlotStore struct {
	db postgresExecutionDB
}

type HostSlotClaim struct {
	InvocationID    kernel.InvocationID
	TaskID          string
	HostRef         string
	MCPClientKey    string
	TokenHash       []byte
	TokenIdentifier string
	ClaimedAt       time.Time
	ReleasedAt      time.Time
	RevokedAt       time.Time
}

type HostFence struct {
	HostRef     string
	State       string
	StartedAt   time.Time
	CompletedAt time.Time
	ClearedAt   time.Time
}

func NewHostSlotStore(db postgresExecutionDB) *HostSlotStore {
	return &HostSlotStore{db: db}
}

func (s *HostSlotStore) ActiveCounts(ctx context.Context) (map[string]int, error) {
	tx, err := s.begin(ctx, true)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `
SELECT host_ref, COUNT(*)
FROM agentteams_execution_refs
WHERE host_slot_claimed_at IS NOT NULL
  AND host_slot_released_at IS NULL
  AND state IN ('reserved', 'dispatched')
GROUP BY host_ref`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	counts := map[string]int{}
	for rows.Next() {
		var host string
		var count int
		if err := rows.Scan(&host, &count); err != nil {
			return nil, err
		}
		counts[host] = count
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return counts, tx.Commit()
}

func (s *HostSlotStore) Claim(ctx context.Context, hostRef string, invocationID kernel.InvocationID, mcpClientKey string, tokenHash []byte, tokenIdentifier string) error {
	if hostRef == "" || invocationID == "" || mcpClientKey == "" || len(tokenHash) == 0 || tokenIdentifier == "" {
		return kernel.InvalidArgument("host slot claim requires host, invocation, MCP key, and token hash metadata")
	}
	tx, err := s.begin(ctx, false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, "agentteams-host-slot:"+hostRef); err != nil {
		return err
	}
	fence, fenced, err := scanHostFence(tx.QueryRowContext(ctx, `
SELECT host_ref, state, started_at, completed_at, cleared_at
FROM agentteams_host_fences
WHERE host_ref = $1 AND cleared_at IS NULL
FOR UPDATE`, hostRef))
	if err != nil {
		return err
	}
	if fenced {
		return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "AgentTeams host is fenced and cannot accept new claims: " + fence.State, Recoverable: true}
	}
	existing, ok, err := scanHostSlotClaim(tx.QueryRowContext(ctx, `
SELECT invocation_id, agentteams_task_id, host_ref, COALESCE(mcp_client_key, ''), mcp_token_hash,
       COALESCE(mcp_token_identifier, ''), host_slot_claimed_at, host_slot_released_at, mcp_revoked_at
FROM agentteams_execution_refs
WHERE host_ref = $1
  AND host_slot_claimed_at IS NOT NULL
  AND host_slot_released_at IS NULL
  AND state IN ('reserved', 'dispatched')
FOR UPDATE`, hostRef))
	if err != nil {
		return err
	}
	if ok && existing.InvocationID != invocationID {
		return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "AgentTeams host slot is already claimed", Recoverable: true}
	}
	if ok {
		if !existing.RevokedAt.IsZero() {
			return kernel.Error{Code: kernel.CodeStaleCommand, Message: "AgentTeams host slot is fenced and must be released before reuse", Recoverable: true}
		}
		if existing.MCPClientKey != mcpClientKey || existing.TokenIdentifier != tokenIdentifier ||
			!bytes.Equal(existing.TokenHash, tokenHash) {
			return kernel.IdempotencyConflict()
		}
		return tx.Commit()
	}
	result, err := tx.ExecContext(ctx, `
UPDATE agentteams_execution_refs
SET host_slot_claimed_at = now(),
    host_slot_released_at = NULL,
    mcp_client_key = $3,
    mcp_token_hash = $4,
    mcp_token_identifier = $5,
    mcp_revoked_at = NULL,
    updated_at = now()
WHERE invocation_id = $1
  AND host_ref = $2
  AND state IN ('reserved', 'dispatched')
  AND host_slot_claimed_at IS NULL`, invocationID, hostRef, mcpClientKey, tokenHash, tokenIdentifier)
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err == nil && affected != 1 {
		return kernel.Error{Code: kernel.CodeNotFound, Message: "AgentTeams execution reservation for host slot not found"}
	}
	return tx.Commit()
}

func (s *HostSlotStore) Release(ctx context.Context, taskID string, hostRef string) error {
	if taskID == "" || hostRef == "" {
		return kernel.InvalidArgument("host slot release requires task_id and host_ref")
	}
	tx, err := s.begin(ctx, false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	claim, ok, err := scanHostSlotClaim(tx.QueryRowContext(ctx, `
SELECT invocation_id, agentteams_task_id, host_ref, COALESCE(mcp_client_key, ''), mcp_token_hash,
       COALESCE(mcp_token_identifier, ''), host_slot_claimed_at, host_slot_released_at, mcp_revoked_at
FROM agentteams_execution_refs
WHERE agentteams_task_id = $1 AND host_ref = $2
FOR UPDATE`, taskID, hostRef))
	if err != nil {
		return err
	}
	if !ok {
		return kernel.Error{Code: kernel.CodeNotFound, Message: "AgentTeams host slot not found"}
	}
	if claim.RevokedAt.IsZero() {
		return kernel.Forbidden("AgentTeams host slot cannot be released before invocation MCP revocation or host fencing")
	}
	result, err := tx.ExecContext(ctx, `
UPDATE agentteams_execution_refs
SET host_slot_released_at = COALESCE(host_slot_released_at, now()),
    updated_at = now()
WHERE agentteams_task_id = $1 AND host_ref = $2`, taskID, hostRef)
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err == nil && affected != 1 {
		return kernel.Error{Code: kernel.CodeNotFound, Message: "AgentTeams host slot not found"}
	}
	return tx.Commit()
}

func (s *HostSlotStore) MarkRevoked(ctx context.Context, taskID string, hostRef string) error {
	if taskID == "" || hostRef == "" {
		return kernel.InvalidArgument("MCP revocation requires task_id and host_ref")
	}
	tx, err := s.begin(ctx, false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `
UPDATE agentteams_execution_refs
SET mcp_revoked_at = COALESCE(mcp_revoked_at, now()),
    updated_at = now()
WHERE agentteams_task_id = $1
  AND host_ref = $2
  AND host_slot_released_at IS NULL
  AND state IN ('reserved', 'dispatched')`, taskID, hostRef)
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err == nil && affected != 1 {
		return kernel.Error{Code: kernel.CodeNotFound, Message: "AgentTeams invocation MCP slot not found"}
	}
	return tx.Commit()
}

func (s *HostSlotStore) BeginHostFence(ctx context.Context, hostRef string) ([]HostSlotClaim, error) {
	if hostRef == "" {
		return nil, kernel.InvalidArgument("host fence requires host_ref")
	}
	tx, err := s.begin(ctx, false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, "agentteams-host-slot:"+hostRef); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO agentteams_host_fences (host_ref, state, started_at, completed_at, cleared_at, updated_at)
VALUES ($1, 'fencing', now(), NULL, NULL, now())
ON CONFLICT (host_ref) DO UPDATE
SET state = CASE
      WHEN agentteams_host_fences.cleared_at IS NOT NULL THEN 'fencing'
      ELSE agentteams_host_fences.state
    END,
    started_at = CASE
      WHEN agentteams_host_fences.cleared_at IS NOT NULL THEN now()
      ELSE agentteams_host_fences.started_at
    END,
    completed_at = CASE
      WHEN agentteams_host_fences.cleared_at IS NOT NULL THEN NULL
      ELSE agentteams_host_fences.completed_at
    END,
    cleared_at = NULL,
    updated_at = now()`, hostRef); err != nil {
		return nil, err
	}
	claims, err := activeByHostTx(ctx, tx, hostRef, "FOR UPDATE")
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return claims, nil
}

func (s *HostSlotStore) CompleteHostFence(ctx context.Context, hostRef string, claims []HostSlotClaim) error {
	if hostRef == "" {
		return kernel.InvalidArgument("host fence completion requires host_ref")
	}
	tx, err := s.begin(ctx, false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, "agentteams-host-slot:"+hostRef); err != nil {
		return err
	}
	for _, claim := range claims {
		if claim.HostRef != hostRef || claim.TaskID == "" {
			return kernel.InvalidArgument("host fence claim snapshot is invalid")
		}
		if _, err := tx.ExecContext(ctx, `
UPDATE agentteams_execution_refs
SET mcp_revoked_at = COALESCE(mcp_revoked_at, now()),
    updated_at = now()
WHERE agentteams_task_id = $1
  AND host_ref = $2
  AND host_slot_claimed_at IS NOT NULL
  AND host_slot_released_at IS NULL
  AND state IN ('reserved', 'dispatched')`, claim.TaskID, hostRef); err != nil {
			return err
		}
	}
	result, err := tx.ExecContext(ctx, `
UPDATE agentteams_host_fences
SET state = 'complete',
    completed_at = COALESCE(completed_at, now()),
    updated_at = now()
WHERE host_ref = $1 AND cleared_at IS NULL`, hostRef)
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err == nil && affected != 1 {
		return kernel.Error{Code: kernel.CodeNotFound, Message: "AgentTeams host fence not found"}
	}
	return tx.Commit()
}

func (s *HostSlotStore) ClearHostFenceIfReusable(ctx context.Context, hostRef string) (bool, error) {
	if hostRef == "" {
		return false, kernel.InvalidArgument("host fence clear requires host_ref")
	}
	tx, err := s.begin(ctx, false)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, "agentteams-host-slot:"+hostRef); err != nil {
		return false, err
	}
	_, fenced, err := scanHostFence(tx.QueryRowContext(ctx, `
SELECT host_ref, state, started_at, completed_at, cleared_at
FROM agentteams_host_fences
WHERE host_ref = $1 AND cleared_at IS NULL
FOR UPDATE`, hostRef))
	if err != nil {
		return false, err
	}
	if !fenced {
		return false, tx.Commit()
	}
	var active int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM agentteams_execution_refs
WHERE host_ref = $1
  AND host_slot_claimed_at IS NOT NULL
  AND host_slot_released_at IS NULL
  AND state IN ('reserved', 'dispatched')`, hostRef).Scan(&active); err != nil {
		return false, err
	}
	if active > 0 {
		return false, tx.Commit()
	}
	result, err := tx.ExecContext(ctx, `
UPDATE agentteams_host_fences
SET state = 'cleared',
    cleared_at = now(),
    updated_at = now()
WHERE host_ref = $1 AND cleared_at IS NULL`, hostRef)
	if err != nil {
		return false, err
	}
	if affected, err := result.RowsAffected(); err == nil && affected != 1 {
		return false, kernel.Error{Code: kernel.CodeRevisionConflict, Message: "AgentTeams host fence changed", Recoverable: true}
	}
	return true, tx.Commit()
}

// MarkHostFenced records that stopping the entire host made every invocation
// MCP on that host unreachable. New production force-stop code uses
// BeginHostFence and CompleteHostFence so the durable fence exists before the
// external host stop runs.
func (s *HostSlotStore) MarkHostFenced(ctx context.Context, hostRef string) error {
	claims, err := s.BeginHostFence(ctx, hostRef)
	if err != nil {
		return err
	}
	return s.CompleteHostFence(ctx, hostRef, claims)
}

func (s *HostSlotStore) ByInvocation(ctx context.Context, hostRef string, invocationID kernel.InvocationID) (HostSlotClaim, bool, error) {
	return s.queryOne(ctx, `
SELECT invocation_id, agentteams_task_id, host_ref, COALESCE(mcp_client_key, ''), mcp_token_hash,
       COALESCE(mcp_token_identifier, ''), host_slot_claimed_at, host_slot_released_at, mcp_revoked_at
FROM agentteams_execution_refs
WHERE invocation_id = $1 AND host_ref = $2
ORDER BY attempt DESC
LIMIT 1`, invocationID, hostRef)
}

func (s *HostSlotStore) ByTaskID(ctx context.Context, taskID string) (HostSlotClaim, bool, error) {
	return s.queryOne(ctx, `
SELECT invocation_id, agentteams_task_id, host_ref, COALESCE(mcp_client_key, ''), mcp_token_hash,
       COALESCE(mcp_token_identifier, ''), host_slot_claimed_at, host_slot_released_at, mcp_revoked_at
FROM agentteams_execution_refs
WHERE agentteams_task_id = $1`, taskID)
}

func (s *HostSlotStore) ActiveByHost(ctx context.Context, hostRef string) ([]HostSlotClaim, error) {
	if hostRef == "" {
		return nil, kernel.InvalidArgument("host_ref is required")
	}
	tx, err := s.begin(ctx, true)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	claims, err := activeByHostTx(ctx, tx, hostRef, "")
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return claims, nil
}

func activeByHostTx(ctx context.Context, tx *sql.Tx, hostRef string, suffix string) ([]HostSlotClaim, error) {
	query := `
SELECT invocation_id, agentteams_task_id, host_ref, COALESCE(mcp_client_key, ''), mcp_token_hash,
       COALESCE(mcp_token_identifier, ''), host_slot_claimed_at, host_slot_released_at, mcp_revoked_at
FROM agentteams_execution_refs
WHERE host_ref = $1
  AND host_slot_claimed_at IS NOT NULL
  AND host_slot_released_at IS NULL
  AND state IN ('reserved', 'dispatched')
ORDER BY agentteams_task_id`
	if suffix != "" {
		query += "\n" + suffix
	}
	rows, err := tx.QueryContext(ctx, query, hostRef)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	claims := make([]HostSlotClaim, 0, 1)
	for rows.Next() {
		claim, ok, err := scanHostSlotClaim(rows)
		if err != nil {
			return nil, err
		}
		if ok {
			claims = append(claims, claim)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return claims, nil
}

func (s *HostSlotStore) queryOne(ctx context.Context, query string, args ...any) (HostSlotClaim, bool, error) {
	tx, err := s.begin(ctx, true)
	if err != nil {
		return HostSlotClaim{}, false, err
	}
	defer tx.Rollback()
	claim, ok, err := scanHostSlotClaim(tx.QueryRowContext(ctx, query, args...))
	if err != nil {
		return HostSlotClaim{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return HostSlotClaim{}, false, err
	}
	return claim, ok, nil
}

func (s *HostSlotStore) begin(ctx context.Context, readOnly bool) (*sql.Tx, error) {
	if s == nil || s.db == nil {
		return nil, kernel.Error{Code: kernel.CodeInternalError, Message: "AgentTeams host slot store database is required"}
	}
	if readOnly {
		return s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	}
	return s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
}

func scanHostSlotClaim(row interface{ Scan(...any) error }) (HostSlotClaim, bool, error) {
	var claim HostSlotClaim
	var tokenHash []byte
	var claimedAt, releasedAt, revokedAt sql.NullTime
	if err := row.Scan(
		&claim.InvocationID,
		&claim.TaskID,
		&claim.HostRef,
		&claim.MCPClientKey,
		&tokenHash,
		&claim.TokenIdentifier,
		&claimedAt,
		&releasedAt,
		&revokedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return HostSlotClaim{}, false, nil
		}
		return HostSlotClaim{}, false, err
	}
	claim.TokenHash = append([]byte(nil), tokenHash...)
	if claimedAt.Valid {
		claim.ClaimedAt = claimedAt.Time
	}
	if releasedAt.Valid {
		claim.ReleasedAt = releasedAt.Time
	}
	if revokedAt.Valid {
		claim.RevokedAt = revokedAt.Time
	}
	return claim, true, nil
}

func scanHostFence(row interface{ Scan(...any) error }) (HostFence, bool, error) {
	var fence HostFence
	var completedAt, clearedAt sql.NullTime
	if err := row.Scan(&fence.HostRef, &fence.State, &fence.StartedAt, &completedAt, &clearedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return HostFence{}, false, nil
		}
		return HostFence{}, false, err
	}
	if completedAt.Valid {
		fence.CompletedAt = completedAt.Time
	}
	if clearedAt.Valid {
		fence.ClearedAt = clearedAt.Time
	}
	return fence, true, nil
}
