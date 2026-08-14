package runtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

type RowScanner interface {
	Scan(...any) error
}

type RowsScanner interface {
	Next() bool
	Scan(...any) error
	Close() error
	Err() error
}

type DBTX interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (RowsScanner, error)
	QueryRowContext(context.Context, string, ...any) RowScanner
}

type SQLDBTX interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type sqlDBTX struct {
	db SQLDBTX
}

func WrapSQLDBTX(db SQLDBTX) DBTX {
	return sqlDBTX{db: db}
}

func (db sqlDBTX) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return db.db.ExecContext(ctx, query, args...)
}

func (db sqlDBTX) QueryContext(ctx context.Context, query string, args ...any) (RowsScanner, error) {
	return db.db.QueryContext(ctx, query, args...)
}

func (db sqlDBTX) QueryRowContext(ctx context.Context, query string, args ...any) RowScanner {
	return db.db.QueryRowContext(ctx, query, args...)
}

type PostgresInvocationStore struct {
	db DBTX
}

func NewPostgresInvocationStore(db DBTX) *PostgresInvocationStore {
	return &PostgresInvocationStore{db: db}
}

func NewPostgresInvocationStoreFromSQL(db SQLDBTX) *PostgresInvocationStore {
	return NewPostgresInvocationStore(WrapSQLDBTX(db))
}

func (s *PostgresInvocationStore) Create(ctx context.Context, invocation Invocation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := invocation.Validate(); err != nil {
		return err
	}
	fingerprint, err := invocationFingerprint(invocation)
	if err != nil {
		return kernel.Error{Code: kernel.CodeInternalError, Message: "invocation creation payload cannot be canonicalized", Recoverable: false}
	}
	promptHashes, err := json.Marshal(invocation.PromptHashes)
	if err != nil {
		return kernel.Error{Code: kernel.CodeInternalError, Message: "prompt hashes cannot be encoded", Recoverable: false}
	}
	skillHashes, err := json.Marshal(invocation.SkillHashes)
	if err != nil {
		return kernel.Error{Code: kernel.CodeInternalError, Message: "skill hashes cannot be encoded", Recoverable: false}
	}
	effectiveTools, err := json.Marshal(invocation.EffectiveTools)
	if err != nil {
		return kernel.Error{Code: kernel.CodeInternalError, Message: "effective tools cannot be encoded", Recoverable: false}
	}
	result, err := s.db.ExecContext(ctx, `
INSERT INTO runtime_invocations (
  invocation_id, actor_principal_id, project_id, task_id, endpoint_id, generation,
  role, operation, status, binding_ref, lease_id, workspace_ref, context_slice_ref,
  task_memory_buffer_ref, consumer_invocation_id, consumer_task_id, consumer_role,
  prompt_hashes, skill_hashes, effective_tools, invocation_fingerprint, created_at, expires_at
) VALUES (
  $1, $2, $3, NULLIF($4, ''), NULLIF($5, ''), NULLIF($6, 0),
  $7, NULLIF($8, ''), $9, NULLIF($10, ''), NULLIF($11, ''), NULLIF($12, ''),
  NULLIF($13, ''), NULLIF($14, ''), NULLIF($15, ''), NULLIF($16, ''), NULLIF($17, ''),
  $18::jsonb, $19::jsonb, $20::jsonb, $21, $22, $23
) ON CONFLICT (invocation_id) DO NOTHING`,
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
		string(promptHashes),
		string(skillHashes),
		string(effectiveTools),
		fingerprint,
		invocation.CreatedAt,
		invocation.ExpiresAt,
	)
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err == nil && affected == 1 {
		return nil
	}
	existing, ok, err := s.Get(ctx, invocation.ID)
	if err != nil {
		return err
	}
	if !ok {
		return kernel.Error{Code: kernel.CodeRevisionConflict, Message: "invocation insert conflicted but row is not visible", Recoverable: true}
	}
	existingFingerprint, ok, err := s.fingerprint(ctx, invocation.ID)
	if err != nil {
		return err
	}
	if ok && existingFingerprint == fingerprint {
		return nil
	}
	if !ok && invocationsSameCreatePayload(existing, invocation) {
		return nil
	}
	return kernel.Error{Code: kernel.CodeIdempotencyConflict, Message: "invocation id already has a different creation payload", Recoverable: false}
}

func (s *PostgresInvocationStore) Get(ctx context.Context, id kernel.InvocationID) (Invocation, bool, error) {
	if err := ctx.Err(); err != nil {
		return Invocation{}, false, err
	}
	return scanInvocation(s.db.QueryRowContext(ctx, `
SELECT invocation_id, actor_principal_id, project_id, COALESCE(task_id, ''), COALESCE(endpoint_id, ''),
       COALESCE(generation, 0), role, COALESCE(operation, ''), status, COALESCE(binding_ref, ''),
       COALESCE(lease_id, ''), COALESCE(workspace_ref, ''), COALESCE(context_slice_ref, ''),
       COALESCE(task_memory_buffer_ref, ''), COALESCE(consumer_invocation_id, ''),
       COALESCE(consumer_task_id, ''), COALESCE(consumer_role, ''), prompt_hashes, skill_hashes,
       effective_tools, created_at, expires_at
FROM runtime_invocations
WHERE invocation_id = $1`, id))
}

func (s *PostgresInvocationStore) GetByLease(ctx context.Context, lease kernel.LeaseID) (Invocation, bool, error) {
	if err := ctx.Err(); err != nil {
		return Invocation{}, false, err
	}
	if lease == "" {
		return Invocation{}, false, kernel.InvalidArgument("lease id is required")
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT invocation_id, actor_principal_id, project_id, COALESCE(task_id, ''), COALESCE(endpoint_id, ''),
       COALESCE(generation, 0), role, COALESCE(operation, ''), status, COALESCE(binding_ref, ''),
       COALESCE(lease_id, ''), COALESCE(workspace_ref, ''), COALESCE(context_slice_ref, ''),
       COALESCE(task_memory_buffer_ref, ''), COALESCE(consumer_invocation_id, ''),
       COALESCE(consumer_task_id, ''), COALESCE(consumer_role, ''), prompt_hashes, skill_hashes,
       effective_tools, created_at, expires_at
FROM runtime_invocations
WHERE lease_id = $1
ORDER BY created_at
LIMIT 2`, lease)
	if err != nil {
		return Invocation{}, false, err
	}
	defer rows.Close()
	var found Invocation
	count := 0
	for rows.Next() {
		invocation, err := scanInvocationRow(rows)
		if err != nil {
			return Invocation{}, false, err
		}
		found = invocation
		count++
	}
	if err := rows.Err(); err != nil {
		return Invocation{}, false, err
	}
	switch count {
	case 0:
		return Invocation{}, false, nil
	case 1:
		return found, true, nil
	default:
		return Invocation{}, false, kernel.Error{Code: kernel.CodeLeaseConflict, Message: "multiple invocations share a lease", Recoverable: true}
	}
}

func (s *PostgresInvocationStore) Transition(ctx context.Context, id kernel.InvocationID, from, to InvocationStatus) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !validInvocationTransition(from, to) {
		return kernel.InvalidArgument("invalid invocation status transition")
	}
	result, err := s.db.ExecContext(ctx, `
UPDATE runtime_invocations
SET status = $3
WHERE invocation_id = $1 AND status = $2`, id, from, to)
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err == nil && affected == 1 {
		return nil
	}
	current, ok, err := s.Get(ctx, id)
	if err != nil {
		return err
	}
	if !ok {
		return kernel.Error{Code: kernel.CodeNotFound, Message: "invocation not found", Recoverable: false}
	}
	if current.Status != from {
		return kernel.Error{Code: kernel.CodeRevisionConflict, Message: "invocation status changed", Recoverable: true}
	}
	return kernel.Error{Code: kernel.CodeRevisionConflict, Message: "invocation status was not updated", Recoverable: true}
}

func (s *PostgresInvocationStore) fingerprint(ctx context.Context, id kernel.InvocationID) (string, bool, error) {
	var fingerprint string
	err := s.db.QueryRowContext(ctx, `SELECT COALESCE(invocation_fingerprint, '') FROM runtime_invocations WHERE invocation_id = $1`, id).Scan(&fingerprint)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return fingerprint, fingerprint != "", nil
}

func scanInvocation(row RowScanner) (Invocation, bool, error) {
	invocation, err := scanInvocationRow(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Invocation{}, false, nil
	}
	if err != nil {
		return Invocation{}, false, err
	}
	return invocation, true, nil
}

func scanInvocationRow(row RowScanner) (Invocation, error) {
	var invocation Invocation
	var generation int64
	var promptHashes, skillHashes, effectiveTools []byte
	if err := row.Scan(
		&invocation.ID,
		&invocation.ActorPrincipalID,
		&invocation.ProjectID,
		&invocation.TaskID,
		&invocation.EndpointID,
		&generation,
		&invocation.Role,
		&invocation.Operation,
		&invocation.Status,
		&invocation.BindingRef,
		&invocation.LeaseID,
		&invocation.WorkspaceRef,
		&invocation.ContextSliceRef,
		&invocation.TaskMemoryBufferRef,
		&invocation.ConsumerInvocationID,
		&invocation.ConsumerTaskID,
		&invocation.ConsumerRole,
		&promptHashes,
		&skillHashes,
		&effectiveTools,
		&invocation.CreatedAt,
		&invocation.ExpiresAt,
	); err != nil {
		return Invocation{}, err
	}
	invocation.Generation = uint64(generation)
	if err := json.Unmarshal(promptHashes, &invocation.PromptHashes); err != nil {
		return Invocation{}, err
	}
	if err := json.Unmarshal(skillHashes, &invocation.SkillHashes); err != nil {
		return Invocation{}, err
	}
	if err := json.Unmarshal(effectiveTools, &invocation.EffectiveTools); err != nil {
		return Invocation{}, err
	}
	return cloneInvocation(invocation), nil
}

func invocationsSameCreatePayload(a, b Invocation) bool {
	a.Status = b.Status
	a.CreatedAt = a.CreatedAt.Round(0)
	a.ExpiresAt = a.ExpiresAt.Round(0)
	b.CreatedAt = b.CreatedAt.Round(0)
	b.ExpiresAt = b.ExpiresAt.Round(0)
	af, aerr := invocationFingerprint(a)
	bf, berr := invocationFingerprint(b)
	return aerr == nil && berr == nil && af == bf
}

var _ InvocationStore = (*PostgresInvocationStore)(nil)
