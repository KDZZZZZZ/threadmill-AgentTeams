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

// MarkHostFenced records that stopping the entire host made every invocation
// MCP on that host unreachable. The host advisory lock serializes the fence
// with new claims, so capacity cannot be released while a new claim slips in.
func (s *HostSlotStore) MarkHostFenced(ctx context.Context, hostRef string) error {
	if hostRef == "" {
		return kernel.InvalidArgument("host fence requires host_ref")
	}
	tx, err := s.begin(ctx, false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, "agentteams-host-slot:"+hostRef); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE agentteams_execution_refs
SET mcp_revoked_at = COALESCE(mcp_revoked_at, now()),
    updated_at = now()
WHERE host_ref = $1
  AND host_slot_claimed_at IS NOT NULL
  AND host_slot_released_at IS NULL
  AND state IN ('reserved', 'dispatched')`, hostRef); err != nil {
		return err
	}
	return tx.Commit()
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
