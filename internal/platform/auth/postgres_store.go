package auth

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

// PostgresStore persists only hashes and authority-bound capability fields.
// Plaintext session, CSRF, and agent-token secrets never cross this boundary.
type PostgresStore struct {
	db *sql.DB
}

var _ Store = (*PostgresStore)(nil)

func NewPostgresStore(db *sql.DB) *PostgresStore {
	return &PostgresStore{db: db}
}

func (s *PostgresStore) PutSession(ctx context.Context, record SessionRecord) error {
	if err := s.ready(); err != nil {
		return err
	}
	projectIDs, err := encodeProjectIDs(record.ProjectIDs)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `
INSERT INTO operator_sessions (
	session_hash, actor_principal_id, project_ids, csrf_hash, expires_at, revoked_at
) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (session_hash) DO UPDATE SET
	actor_principal_id = EXCLUDED.actor_principal_id,
	project_ids = EXCLUDED.project_ids,
	csrf_hash = EXCLUDED.csrf_hash,
	expires_at = EXCLUDED.expires_at,
	revoked_at = EXCLUDED.revoked_at`,
		record.SessionHash,
		record.ActorPrincipalID,
		projectIDs,
		record.CSRFHash,
		record.ExpiresAt,
		record.RevokedAt,
	)
	if err != nil {
		return fmt.Errorf("persist operator session: %w", err)
	}
	return nil
}

func (s *PostgresStore) SessionByHash(ctx context.Context, hash []byte) (SessionRecord, bool, error) {
	if err := s.ready(); err != nil {
		return SessionRecord{}, false, err
	}
	var (
		record     SessionRecord
		projectIDs []byte
	)
	err := s.db.QueryRowContext(ctx, `
SELECT session_hash, actor_principal_id, project_ids, csrf_hash, expires_at, revoked_at
FROM operator_sessions
WHERE session_hash = $1`, hash).Scan(
		&record.SessionHash,
		&record.ActorPrincipalID,
		&projectIDs,
		&record.CSRFHash,
		&record.ExpiresAt,
		&record.RevokedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return SessionRecord{}, false, nil
	}
	if err != nil {
		return SessionRecord{}, false, fmt.Errorf("load operator session: %w", err)
	}
	record.ProjectIDs, err = decodeProjectIDs(projectIDs)
	if err != nil {
		return SessionRecord{}, false, fmt.Errorf("decode operator session projects: %w", err)
	}
	return record, true, nil
}

func (s *PostgresStore) RevokeSession(ctx context.Context, hash []byte, at time.Time) error {
	if err := s.ready(); err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx, `
UPDATE operator_sessions
SET revoked_at = COALESCE(revoked_at, $2)
WHERE session_hash = $1`, hash, at)
	if err != nil {
		return fmt.Errorf("revoke operator session: %w", err)
	}
	return requireRevokedRow(result, "session not found")
}

func (s *PostgresStore) PutToken(ctx context.Context, record TokenRecord) error {
	if err := s.ready(); err != nil {
		return err
	}
	tools, err := encodeTools(record.Capability.Tools)
	if err != nil {
		return err
	}
	capability := record.Capability
	_, err = s.db.ExecContext(ctx, `
INSERT INTO agent_invocation_tokens (
	token_hash,
	actor_principal_id,
	project_id,
	task_id,
	invocation_id,
	role,
	operation,
	consumer_invocation_id,
	consumer_task_id,
	consumer_role,
	tools,
	expires_at,
	revoked_at
) VALUES ($1, $2, $3, NULLIF($4, ''), $5, $6, NULLIF($7, ''), NULLIF($8, ''), NULLIF($9, ''), NULLIF($10, ''), $11, $12, $13)
ON CONFLICT (token_hash) DO UPDATE SET
	actor_principal_id = EXCLUDED.actor_principal_id,
	project_id = EXCLUDED.project_id,
	task_id = EXCLUDED.task_id,
	invocation_id = EXCLUDED.invocation_id,
	role = EXCLUDED.role,
	operation = EXCLUDED.operation,
	consumer_invocation_id = EXCLUDED.consumer_invocation_id,
	consumer_task_id = EXCLUDED.consumer_task_id,
	consumer_role = EXCLUDED.consumer_role,
	tools = EXCLUDED.tools,
	expires_at = EXCLUDED.expires_at,
	revoked_at = EXCLUDED.revoked_at`,
		record.TokenHash,
		record.ActorPrincipalID,
		capability.ProjectID,
		capability.TaskID,
		capability.InvocationID,
		capability.Role,
		capability.Operation,
		capability.ConsumerInvocationID,
		capability.ConsumerTaskID,
		capability.ConsumerRole,
		tools,
		record.ExpiresAt,
		record.RevokedAt,
	)
	if err != nil {
		return fmt.Errorf("persist agent invocation token: %w", err)
	}
	return nil
}

func (s *PostgresStore) TokenByHash(ctx context.Context, hash []byte) (TokenRecord, bool, error) {
	if err := s.ready(); err != nil {
		return TokenRecord{}, false, err
	}
	var (
		record               TokenRecord
		projectID            string
		taskID               sql.NullString
		invocationID         string
		role                 string
		operation            sql.NullString
		consumerInvocationID sql.NullString
		consumerTaskID       sql.NullString
		consumerRole         sql.NullString
		tools                []byte
	)
	err := s.db.QueryRowContext(ctx, `
SELECT
	token_hash,
	actor_principal_id,
	project_id,
	task_id,
	invocation_id,
	role,
	operation,
	consumer_invocation_id,
	consumer_task_id,
	consumer_role,
	tools,
	expires_at,
	revoked_at
FROM agent_invocation_tokens
WHERE token_hash = $1`, hash).Scan(
		&record.TokenHash,
		&record.ActorPrincipalID,
		&projectID,
		&taskID,
		&invocationID,
		&role,
		&operation,
		&consumerInvocationID,
		&consumerTaskID,
		&consumerRole,
		&tools,
		&record.ExpiresAt,
		&record.RevokedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return TokenRecord{}, false, nil
	}
	if err != nil {
		return TokenRecord{}, false, fmt.Errorf("load agent invocation token: %w", err)
	}
	decodedTools, err := decodeTools(tools)
	if err != nil {
		return TokenRecord{}, false, fmt.Errorf("decode agent invocation token tools: %w", err)
	}
	record.Capability = Capability{
		ProjectID:            kernel.ProjectID(projectID),
		TaskID:               kernel.TaskID(taskID.String),
		InvocationID:         kernel.InvocationID(invocationID),
		ConsumerInvocationID: kernel.InvocationID(consumerInvocationID.String),
		ConsumerTaskID:       kernel.TaskID(consumerTaskID.String),
		ConsumerRole:         Role(consumerRole.String),
		Role:                 Role(role),
		Operation:            operation.String,
		Tools:                decodedTools,
		ExpiresAt:            record.ExpiresAt,
	}
	return record, true, nil
}

func (s *PostgresStore) RevokeToken(ctx context.Context, hash []byte, at time.Time) error {
	if err := s.ready(); err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx, `
UPDATE agent_invocation_tokens
SET revoked_at = COALESCE(revoked_at, $2)
WHERE token_hash = $1`, hash, at)
	if err != nil {
		return fmt.Errorf("revoke agent invocation token: %w", err)
	}
	return requireRevokedRow(result, "token not found")
}

func (s *PostgresStore) ready() error {
	if s == nil || s.db == nil {
		return errors.New("postgres auth store requires a database")
	}
	return nil
}

func requireRevokedRow(result sql.Result, missingMessage string) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect revocation result: %w", err)
	}
	if affected == 0 {
		return kernel.Error{Code: kernel.CodeUnauthorized, Message: missingMessage}
	}
	return nil
}

func encodeProjectIDs(projectIDs map[kernel.ProjectID]struct{}) ([]byte, error) {
	items := make([]string, 0, len(projectIDs))
	for projectID := range projectIDs {
		items = append(items, string(projectID))
	}
	sort.Strings(items)
	encoded, err := json.Marshal(items)
	if err != nil {
		return nil, fmt.Errorf("encode project ids: %w", err)
	}
	return encoded, nil
}

func decodeProjectIDs(encoded []byte) (map[kernel.ProjectID]struct{}, error) {
	var items []string
	if err := json.Unmarshal(encoded, &items); err != nil {
		return nil, err
	}
	projectIDs := make(map[kernel.ProjectID]struct{}, len(items))
	for _, item := range items {
		projectIDs[kernel.ProjectID(item)] = struct{}{}
	}
	return projectIDs, nil
}

func encodeTools(tools map[Tool]struct{}) ([]byte, error) {
	items := make([]string, 0, len(tools))
	for tool := range tools {
		items = append(items, string(tool))
	}
	sort.Strings(items)
	encoded, err := json.Marshal(items)
	if err != nil {
		return nil, fmt.Errorf("encode tools: %w", err)
	}
	return encoded, nil
}

func decodeTools(encoded []byte) (map[Tool]struct{}, error) {
	var items []string
	if err := json.Unmarshal(encoded, &items); err != nil {
		return nil, err
	}
	tools := make(map[Tool]struct{}, len(items))
	for _, item := range items {
		tools[Tool(item)] = struct{}{}
	}
	return tools, nil
}
