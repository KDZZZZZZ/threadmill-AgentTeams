package workspace

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/jackc/pgx/v5/pgconn"
)

type PostgresStore struct {
	db         *sql.DB
	lockMu     sync.Mutex
	keyedLocks map[string]*sync.Mutex
}

func NewPostgresStore(db *sql.DB) *PostgresStore {
	return &PostgresStore{db: db, keyedLocks: make(map[string]*sync.Mutex)}
}

func NewPostgresService(db *sql.DB) *Service {
	return NewServiceWithStore(NewPostgresStore(db), NewLocalGitBackend())
}

func (s *PostgresStore) WithLock(ctx context.Context, key string, fn func(context.Context) error) error {
	if err := s.ready(); err != nil {
		return err
	}
	if key == "" || fn == nil {
		return kernel.InvalidArgument("workspace lock key and callback are required")
	}
	// Avoid exhausting the database pool with same-process waiters that each
	// hold a connection while PostgreSQL serializes the same advisory key.
	s.lockMu.Lock()
	local := s.keyedLocks[key]
	if local == nil {
		local = &sync.Mutex{}
		s.keyedLocks[key] = local
	}
	s.lockMu.Unlock()
	local.Lock()
	defer local.Unlock()

	conn, err := s.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire workspace lock connection: %w", err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock(hashtextextended($1, 0))`, key); err != nil {
		return fmt.Errorf("acquire workspace advisory lock: %w", err)
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = conn.ExecContext(unlockCtx, `SELECT pg_advisory_unlock(hashtextextended($1, 0))`, key)
	}()
	return fn(ctx)
}

func (s *PostgresStore) Insert(ctx context.Context, binding Binding) error {
	if err := s.ready(); err != nil {
		return err
	}
	volumeRefs := stringArrayJSON(binding.VolumeRefs)
	allowedDirs := stringArrayJSON(binding.AllowedDirs)
	declared := mustJSON(binding.DeclaredWrites)
	observed := mustJSON(binding.ObservedWrites)
	leases := mustJSON(binding.PhaseLeases)
	_, err := s.db.ExecContext(ctx, `
INSERT INTO workspace_bindings (
  id, task_id, generation, kind, root, branch_name, container_id,
  volume_refs, base_revision, current_revision, allowed_dirs,
  declared_writes, observed_writes, phase_leases, active_phase,
  active_invocation, status, repository_path, creation_fingerprint,
  binding_revision
) VALUES (
  $1, $2, $3, $4, $5, NULLIF($6, ''), NULLIF($7, ''),
  ARRAY(SELECT jsonb_array_elements_text($8::jsonb)), $9, $10,
  ARRAY(SELECT jsonb_array_elements_text($11::jsonb)),
  $12::jsonb, $13::jsonb, $14::jsonb, NULLIF($15, ''),
  NULLIF($16, ''), $17, $18, $19, $20
)`,
		binding.ID,
		binding.TaskID,
		binding.Generation,
		binding.Kind,
		binding.Root,
		binding.BranchName,
		binding.ContainerID,
		volumeRefs,
		binding.BaseRevision,
		binding.CurrentRevision,
		allowedDirs,
		declared,
		observed,
		leases,
		binding.ActivePhase,
		binding.ActiveInvocation,
		binding.Status,
		binding.RepositoryPath,
		binding.CreationFingerprint,
		binding.Revision,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return kernel.IdempotencyConflict()
		}
		return fmt.Errorf("insert workspace binding: %w", err)
	}
	return nil
}

func (s *PostgresStore) Get(ctx context.Context, id kernel.BindingRef) (Binding, error) {
	if err := s.ready(); err != nil {
		return Binding{}, err
	}
	binding, err := scanBinding(s.db.QueryRowContext(ctx, `SELECT `+bindingSelectColumns+` FROM workspace_bindings WHERE id = $1`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return Binding{}, workspaceNotFound()
	}
	if err != nil {
		return Binding{}, fmt.Errorf("get workspace binding: %w", err)
	}
	return binding, nil
}

func (s *PostgresStore) GetByRound(ctx context.Context, taskID kernel.TaskID, generation int) (Binding, bool, error) {
	if err := s.ready(); err != nil {
		return Binding{}, false, err
	}
	binding, err := scanBinding(s.db.QueryRowContext(ctx, `SELECT `+bindingSelectColumns+` FROM workspace_bindings WHERE task_id = $1 AND generation = $2`, taskID, generation))
	if errors.Is(err, sql.ErrNoRows) {
		return Binding{}, false, nil
	}
	if err != nil {
		return Binding{}, false, fmt.Errorf("get workspace binding by round: %w", err)
	}
	return binding, true, nil
}

func (s *PostgresStore) GetLatestByTask(ctx context.Context, taskID kernel.TaskID) (Binding, bool, error) {
	if err := s.ready(); err != nil {
		return Binding{}, false, err
	}
	binding, err := scanBinding(s.db.QueryRowContext(ctx, `SELECT `+bindingSelectColumns+` FROM workspace_bindings WHERE task_id = $1 ORDER BY generation DESC LIMIT 1`, taskID))
	if errors.Is(err, sql.ErrNoRows) {
		return Binding{}, false, nil
	}
	if err != nil {
		return Binding{}, false, fmt.Errorf("get latest workspace binding by task: %w", err)
	}
	return binding, true, nil
}

func (s *PostgresStore) GetByInvocation(ctx context.Context, invocationID kernel.InvocationID) (Binding, bool, error) {
	if err := s.ready(); err != nil {
		return Binding{}, false, err
	}
	binding, err := scanBinding(s.db.QueryRowContext(ctx, `SELECT `+bindingSelectColumns+` FROM workspace_bindings WHERE active_invocation = $1`, invocationID))
	if errors.Is(err, sql.ErrNoRows) {
		return Binding{}, false, nil
	}
	if err != nil {
		return Binding{}, false, fmt.Errorf("get workspace binding by invocation: %w", err)
	}
	return binding, true, nil
}

func (s *PostgresStore) UpdateCAS(ctx context.Context, next Binding, expected kernel.Revision) (Binding, error) {
	if err := s.ready(); err != nil {
		return Binding{}, err
	}
	if expected == kernel.LatestRevision {
		return Binding{}, kernel.InvalidArgument("expected binding revision is required")
	}
	observed := mustJSON(next.ObservedWrites)
	leases := mustJSON(next.PhaseLeases)
	updated, err := scanBinding(s.db.QueryRowContext(ctx, `
UPDATE workspace_bindings
SET current_revision = $2,
    observed_writes = $3::jsonb,
    phase_leases = $4::jsonb,
    active_phase = NULLIF($5, ''),
    active_invocation = NULLIF($6, ''),
    status = $7,
	allowed_dirs = ARRAY(SELECT jsonb_array_elements_text($9::jsonb)),
	declared_writes = $10::jsonb,
    binding_revision = binding_revision + 1,
    updated_at = now()
WHERE id = $1 AND binding_revision = $8
RETURNING `+bindingSelectColumns,
		next.ID,
		next.CurrentRevision,
		observed,
		leases,
		next.ActivePhase,
		next.ActiveInvocation,
		next.Status,
		expected,
		stringArrayJSON(next.AllowedDirs),
		mustJSON(next.DeclaredWrites),
	))
	if err == nil {
		return updated, nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return Binding{}, kernel.LeaseConflict("invocation is already bound to another workspace")
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Binding{}, fmt.Errorf("update workspace binding: %w", err)
	}
	var actual uint64
	lookupErr := s.db.QueryRowContext(ctx, `SELECT binding_revision FROM workspace_bindings WHERE id = $1`, next.ID).Scan(&actual)
	if errors.Is(lookupErr, sql.ErrNoRows) {
		return Binding{}, workspaceNotFound()
	}
	if lookupErr != nil {
		return Binding{}, fmt.Errorf("read workspace binding revision: %w", lookupErr)
	}
	return Binding{}, kernel.RevisionConflict(expected, kernel.Revision(actual))
}

func (s *PostgresStore) ready() error {
	if s == nil || s.db == nil {
		return kernel.Error{Code: kernel.CodeInternalError, Message: "Postgres workspace store database is required", Recoverable: false}
	}
	return nil
}

const bindingSelectColumns = `
id, binding_revision, task_id, generation, kind, root,
COALESCE(branch_name, ''), COALESCE(container_id, ''),
array_to_json(volume_refs)::text, base_revision, current_revision,
array_to_json(allowed_dirs)::text, declared_writes::text,
observed_writes::text, phase_leases::text,
COALESCE(active_phase, ''), COALESCE(active_invocation, ''), status,
repository_path, creation_fingerprint`

type rowScanner interface {
	Scan(...any) error
}

func scanBinding(row rowScanner) (Binding, error) {
	var binding Binding
	var volumeRefsJSON, allowedDirsJSON string
	var declaredJSON, observedJSON, leasesJSON string
	var revision uint64
	if err := row.Scan(
		&binding.ID,
		&revision,
		&binding.TaskID,
		&binding.Generation,
		&binding.Kind,
		&binding.Root,
		&binding.BranchName,
		&binding.ContainerID,
		&volumeRefsJSON,
		&binding.BaseRevision,
		&binding.CurrentRevision,
		&allowedDirsJSON,
		&declaredJSON,
		&observedJSON,
		&leasesJSON,
		&binding.ActivePhase,
		&binding.ActiveInvocation,
		&binding.Status,
		&binding.RepositoryPath,
		&binding.CreationFingerprint,
	); err != nil {
		return Binding{}, err
	}
	binding.Revision = kernel.Revision(revision)
	if err := json.Unmarshal([]byte(volumeRefsJSON), &binding.VolumeRefs); err != nil {
		return Binding{}, fmt.Errorf("decode workspace volume refs: %w", err)
	}
	if err := json.Unmarshal([]byte(allowedDirsJSON), &binding.AllowedDirs); err != nil {
		return Binding{}, fmt.Errorf("decode workspace allowed dirs: %w", err)
	}
	if err := json.Unmarshal([]byte(declaredJSON), &binding.DeclaredWrites); err != nil {
		return Binding{}, fmt.Errorf("decode workspace declared writes: %w", err)
	}
	if err := json.Unmarshal([]byte(observedJSON), &binding.ObservedWrites); err != nil {
		return Binding{}, fmt.Errorf("decode workspace observed writes: %w", err)
	}
	if err := json.Unmarshal([]byte(leasesJSON), &binding.PhaseLeases); err != nil {
		return Binding{}, fmt.Errorf("decode workspace phase leases: %w", err)
	}
	if binding.PhaseLeases == nil {
		binding.PhaseLeases = make(map[Phase]kernel.InvocationID)
	}
	return binding, nil
}

func mustJSON(value any) []byte {
	payload, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return payload
}

func stringArrayJSON(values []string) []byte {
	if values == nil {
		values = []string{}
	}
	return mustJSON(values)
}

var _ BindingStore = (*PostgresStore)(nil)
