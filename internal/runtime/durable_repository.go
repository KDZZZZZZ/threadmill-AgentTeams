package runtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/artifacts"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/executionreceipt"
	_ "modernc.org/sqlite"
)

// RuntimeStateRepository is the backend-neutral durable authority seam. Its
// typed stores share one transaction-owning database; it deliberately does
// not expose SQL or persistence details to phaseagent.
type RuntimeStateRepository interface {
	Close() error
	SchemaVersion(context.Context) (int, error)
	WaitingStore() WaitingStore
	ContinuationStore() DurableContinuationStore
	InputStore() DurablePhaseInputStore
	PhysicalExecutionStore() PhysicalExecutionStore
	ReceiptStore() executionreceipt.Store
	PhaseOutputStore() PhaseOutputStore
	ArtifactStore() artifacts.DurableStore
	LifecycleMutations() LifecycleMutationStore
	EventOutbox() RuntimeEventOutbox
	Recovery() RecoveryStateStore
	ListRuntimeEvents(context.Context) ([]RuntimeEvent, error)
}

// DurableContinuationStore is the write-capable counterpart of
// ContinuationResolver. Continuation material is logical-only and secret-free.
type DurableContinuationStore interface {
	ContinuationResolver
	Put(context.Context, ContinuationRef, ContinuationMaterial) error
}

// DurablePhaseInputStore combines the M4 reconstruction authority with the
// immutable input/binding writes required to cold-load a continuation.
type DurablePhaseInputStore interface {
	RehydrationInputResolver
	InputContinuationRebinder
	ContinuationBindingResolver
	Put(context.Context, WaitingKey, StoredPhaseInputSet) error
}

type RuntimeEvent struct {
	EventSequence  int64
	EventID        string
	EventType      string
	OccurredAt     time.Time
	TaskID         string
	InvocationID   string
	Generation     int
	ExecutionEpoch *ExecutionEpoch
	AggregateKey   string
	ResultRevision int64
	PayloadVersion int
	Payload        json.RawMessage
}

const latestRuntimeSchemaVersion = 4

type recoveryClock interface{ Now() time.Time }
type systemRecoveryClock struct{}

func (systemRecoveryClock) Now() time.Time { return time.Now() }

type SQLiteRuntimeStateRepository struct {
	db    *sql.DB
	clock recoveryClock
}

func OpenSQLiteRuntimeStateRepository(path string) (*SQLiteRuntimeStateRepository, error) {
	if path == "" {
		return nil, errors.New("runtime database path is required")
	}
	if path != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, err
		}
	}
	dsn := "file:" + filepath.ToSlash(path) + "?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1) // M5-B is explicitly a single-writer deployment.
	r := &SQLiteRuntimeStateRepository{db: db, clock: systemRecoveryClock{}}
	if err := r.migrate(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	return r, nil
}

func (r *SQLiteRuntimeStateRepository) Close() error { return r.db.Close() }
func (r *SQLiteRuntimeStateRepository) SchemaVersion(ctx context.Context) (int, error) {
	var version int
	err := r.db.QueryRowContext(ctx, "SELECT version FROM runtime_schema_version LIMIT 1").Scan(&version)
	return version, err
}
func (r *SQLiteRuntimeStateRepository) WaitingStore() WaitingStore { return sqliteWaitingStore{r} }
func (r *SQLiteRuntimeStateRepository) ContinuationStore() DurableContinuationStore {
	return sqliteContinuationStore{r}
}
func (r *SQLiteRuntimeStateRepository) InputStore() DurablePhaseInputStore {
	return sqliteInputStore{r}
}
func (r *SQLiteRuntimeStateRepository) PhysicalExecutionStore() PhysicalExecutionStore {
	return sqlitePhysicalStore{r}
}
func (r *SQLiteRuntimeStateRepository) ReceiptStore() executionreceipt.Store {
	return sqliteReceiptStore{r}
}
func (r *SQLiteRuntimeStateRepository) PhaseOutputStore() PhaseOutputStore {
	return sqliteOutputStore{r}
}
func (r *SQLiteRuntimeStateRepository) ArtifactStore() artifacts.DurableStore {
	return sqliteArtifactStore{r}
}
func (r *SQLiteRuntimeStateRepository) LifecycleMutations() LifecycleMutationStore {
	return sqliteLifecycleMutations{r}
}
func (r *SQLiteRuntimeStateRepository) EventOutbox() RuntimeEventOutbox {
	return sqliteRuntimeEventOutbox{r}
}
func (r *SQLiteRuntimeStateRepository) Recovery() RecoveryStateStore {
	return sqliteRecoveryStateStore{r}
}

func (r *SQLiteRuntimeStateRepository) migrate(ctx context.Context) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS runtime_schema_version (version INTEGER NOT NULL)"); err != nil {
		return err
	}
	var version int
	err = tx.QueryRowContext(ctx, "SELECT version FROM runtime_schema_version LIMIT 1").Scan(&version)
	if errors.Is(err, sql.ErrNoRows) {
		if _, err = tx.ExecContext(ctx, "INSERT INTO runtime_schema_version(version) VALUES(0)"); err != nil {
			return err
		}
		version = 0
	} else if err != nil {
		return err
	}
	if version > latestRuntimeSchemaVersion {
		return fmt.Errorf("runtime database schema version %d is newer than supported %d", version, latestRuntimeSchemaVersion)
	}
	if version == 0 {
		statements := []string{
			"CREATE TABLE runtime_waiting (task_id TEXT NOT NULL, invocation_id TEXT NOT NULL, generation INTEGER NOT NULL, revision INTEGER NOT NULL, state TEXT NOT NULL, payload BLOB NOT NULL, PRIMARY KEY(task_id,invocation_id,generation))",
			"CREATE TABLE runtime_continuations (ref TEXT PRIMARY KEY, payload BLOB NOT NULL)",
			"CREATE TABLE runtime_inputs (task_id TEXT NOT NULL, invocation_id TEXT NOT NULL, generation INTEGER NOT NULL, input_revision TEXT NOT NULL, sequence INTEGER NOT NULL, payload BLOB NOT NULL, PRIMARY KEY(task_id,invocation_id,generation,input_revision), UNIQUE(task_id,invocation_id,generation,sequence))",
			"CREATE TABLE runtime_bindings (binding_ref TEXT PRIMARY KEY, invocation_id TEXT NOT NULL, generation INTEGER NOT NULL, execution_epoch INTEGER NOT NULL, payload BLOB NOT NULL)",
			"CREATE TABLE runtime_physical_executions (task_id TEXT NOT NULL, invocation_id TEXT NOT NULL, generation INTEGER NOT NULL, execution_epoch INTEGER NOT NULL, revision INTEGER NOT NULL, state TEXT NOT NULL, payload BLOB NOT NULL, PRIMARY KEY(task_id,invocation_id,generation,execution_epoch))",
			"CREATE TABLE runtime_receipts (task_id TEXT NOT NULL, invocation_id TEXT NOT NULL, generation INTEGER NOT NULL, execution_epoch INTEGER NOT NULL, payload BLOB NOT NULL, PRIMARY KEY(task_id,invocation_id,generation,execution_epoch))",
			"CREATE TABLE runtime_phase_outputs (task_id TEXT NOT NULL, invocation_id TEXT NOT NULL, generation INTEGER NOT NULL, revision INTEGER NOT NULL, payload BLOB NOT NULL, PRIMARY KEY(task_id,invocation_id,generation))",
			"CREATE TABLE runtime_events (event_id TEXT PRIMARY KEY, event_type TEXT NOT NULL, occurred_at TEXT NOT NULL, task_id TEXT NOT NULL, invocation_id TEXT NOT NULL, generation INTEGER NOT NULL, execution_epoch INTEGER, aggregate_key TEXT NOT NULL, result_revision INTEGER NOT NULL, payload_version INTEGER NOT NULL, payload BLOB NOT NULL)",
		}
		for _, statement := range statements {
			if _, err = tx.ExecContext(ctx, statement); err != nil {
				return err
			}
		}
		if _, err = tx.ExecContext(ctx, "UPDATE runtime_schema_version SET version=?", 1); err != nil {
			return err
		}
		version = 1
	}
	if version == 1 {
		for _, statement := range []string{
			"CREATE TABLE runtime_artifacts (artifact_ref TEXT PRIMARY KEY, content_hash TEXT NOT NULL UNIQUE, payload BLOB NOT NULL)",
			"CREATE TABLE runtime_artifact_access (artifact_ref TEXT NOT NULL, task_id TEXT NOT NULL, invocation_id TEXT NOT NULL, PRIMARY KEY(artifact_ref,task_id,invocation_id))",
		} {
			if _, err = tx.ExecContext(ctx, statement); err != nil {
				return err
			}
		}
		if _, err = tx.ExecContext(ctx, "UPDATE runtime_schema_version SET version=?", 2); err != nil {
			return err
		}
		version = 2
	}
	if version == 2 {
		for _, statement := range []string{
			"CREATE TABLE runtime_event_order (event_sequence INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE)",
			"CREATE TABLE runtime_event_consumers (consumer_id TEXT PRIMARY KEY, last_acked_sequence INTEGER NOT NULL, revision INTEGER NOT NULL, updated_at TEXT NOT NULL)",
			"CREATE TABLE runtime_event_claims (consumer_id TEXT PRIMARY KEY, owner_id TEXT NOT NULL, expires_at TEXT NOT NULL, revision INTEGER NOT NULL)",
			"INSERT INTO runtime_event_order(event_id) SELECT event_id FROM runtime_events ORDER BY event_id",
		} {
			if _, err = tx.ExecContext(ctx, statement); err != nil {
				return err
			}
		}
		if _, err = tx.ExecContext(ctx, "UPDATE runtime_schema_version SET version=?", 3); err != nil {
			return err
		}
		version = 3
	}
	if version == 3 {
		if _, err = tx.ExecContext(ctx, "CREATE TABLE runtime_recovery_claims (task_id TEXT NOT NULL, invocation_id TEXT NOT NULL, generation INTEGER NOT NULL, observed_execution_epoch INTEGER NOT NULL, owner_id TEXT NOT NULL, lease_expires_at TEXT NOT NULL, fence INTEGER NOT NULL, revision INTEGER NOT NULL, claimed_at TEXT NOT NULL, renewed_at TEXT NOT NULL, PRIMARY KEY(task_id,invocation_id,generation))"); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "UPDATE runtime_schema_version SET version=?", latestRuntimeSchemaVersion); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func runtimeJSON(v any) ([]byte, error) { return json.Marshal(v) }
func noSecrets(data []byte) error {
	lower := strings.ToLower(string(data))
	for _, banned := range []string{"execution_token", "credential_value", "private_header", "controller_auth", "provider_api_key", "hidden_reasoning", "provider_conversation"} {
		if strings.Contains(lower, banned) {
			return fmt.Errorf("durable runtime payload contains forbidden field %q", banned)
		}
	}
	return nil
}
