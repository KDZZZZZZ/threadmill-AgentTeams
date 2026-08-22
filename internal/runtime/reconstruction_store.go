package runtime

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/phaseagent"
)

// DurableWorkspace is portable logical workspace authority. A local mount
// root is intentionally derived at runtime and never stored here.
type DurableWorkspace struct {
	Key         WaitingKey
	Ref         string
	AllowedDirs []string
	Revision    int64
	CreatedAt   time.Time
}
type DurableWorkspaceLease struct {
	Ref             string
	Key             WaitingKey
	WorkspaceRef    string
	ExecutionEpoch  ExecutionEpoch
	Fence, Revision int64
	State           string
	AcquiredAt      time.Time
}
type DurableContextSlice struct {
	Ref                  string
	Key                  WaitingKey
	BaselineRef, Content string
	Revision             int64
	CreatedAt            time.Time
}
type DurableTaskMemory struct {
	Ref       string
	Key       WaitingKey
	View      phaseagent.TaskMemoryBufferView
	Revision  int64
	CreatedAt time.Time
}

// DurableExecutionDescriptor is the controlled logical material needed to
// build a host envelope. It deliberately stores refs and agent-visible task
// text only; it is not a serialized HostEnvelope or carrier state.
type DurableExecutionDescriptor struct {
	Key              WaitingKey
	TaskContract     string
	PhaseInstruction string
	TaskSpec         string
	WorkspaceRef     string
	ContextSliceRef  string
	TaskMemoryRef    string
	Revision         int64
	CreatedAt        time.Time
}

var (
	ErrReconstructionConflict = errors.New("durable reconstruction record conflicts with existing authority")
	ErrWorkspaceLeaseConflict = errors.New("durable workspace lease changed or is owned by another epoch")
)

type sqliteReconstructionStore struct{ r *SQLiteRuntimeStateRepository }

func validReconstruction(k WaitingKey, ref string) error {
	if k.TaskID == "" || k.InvocationID == "" || k.Generation <= 0 || ref == "" {
		return errors.New("reconstruction owner and ref are required")
	}
	return nil
}
func validateAllowedDirs(dirs []string) error {
	for _, dir := range dirs {
		if dir == "" || filepath.IsAbs(dir) || filepath.VolumeName(dir) != "" {
			return errors.New("workspace allowed directory must be a non-empty portable relative path")
		}
	}
	return nil
}
func reconstructionJSON(v any) ([]byte, error) {
	b, err := runtimeJSON(v)
	if err != nil {
		return nil, err
	}
	if err = noSecrets(b); err != nil {
		return nil, err
	}
	return b, nil
}

func (s sqliteReconstructionStore) PutWorkspace(ctx context.Context, v DurableWorkspace) (DurableWorkspace, error) {
	if err := validReconstruction(v.Key, v.Ref); err != nil {
		return DurableWorkspace{}, err
	}
	if err := validateAllowedDirs(v.AllowedDirs); err != nil {
		return DurableWorkspace{}, err
	}
	tx, err := s.r.db.BeginTx(ctx, nil)
	if err != nil {
		return DurableWorkspace{}, err
	}
	defer tx.Rollback()
	var payload []byte
	err = tx.QueryRowContext(ctx, "SELECT payload FROM runtime_reconstruction_workspaces WHERE task_id=? AND invocation_id=? AND generation=? AND workspace_ref=?", v.Key.TaskID, v.Key.InvocationID, v.Key.Generation, v.Ref).Scan(&payload)
	if err == nil {
		var existing DurableWorkspace
		if err = jsonUnmarshal(payload, &existing); err != nil {
			return DurableWorkspace{}, err
		}
		if !reflect.DeepEqual(existing.AllowedDirs, v.AllowedDirs) {
			return DurableWorkspace{}, ErrReconstructionConflict
		}
		return existing, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return DurableWorkspace{}, err
	}
	v.Revision, v.CreatedAt = 1, time.Now().UTC()
	payload, err = reconstructionJSON(v)
	if err != nil {
		return DurableWorkspace{}, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO runtime_reconstruction_workspaces VALUES(?,?,?,?,?,?)", v.Key.TaskID, v.Key.InvocationID, v.Key.Generation, v.Ref, v.Revision, payload); err != nil {
		return DurableWorkspace{}, err
	}
	return v, tx.Commit()
}
func (s sqliteReconstructionStore) GetWorkspace(ctx context.Context, ref string, key WaitingKey) (DurableWorkspace, bool, error) {
	return getReconstruction[DurableWorkspace](ctx, s.r.db, "runtime_reconstruction_workspaces", "workspace_ref", ref, key)
}

func (s sqliteReconstructionStore) AcquireWorkspaceLease(ctx context.Context, v DurableWorkspaceLease) (DurableWorkspaceLease, error) {
	if err := validReconstruction(v.Key, v.Ref); err != nil || v.ExecutionEpoch <= 0 || v.WorkspaceRef == "" {
		return DurableWorkspaceLease{}, errors.New("workspace lease identity is required")
	}
	if _, found, err := s.GetWorkspace(ctx, v.WorkspaceRef, v.Key); err != nil || !found {
		if err != nil {
			return DurableWorkspaceLease{}, err
		}
		return DurableWorkspaceLease{}, errors.New("workspace lease references unknown workspace")
	}
	tx, err := s.r.db.BeginTx(ctx, nil)
	if err != nil {
		return DurableWorkspaceLease{}, err
	}
	defer tx.Rollback()
	var payload []byte
	err = tx.QueryRowContext(ctx, "SELECT payload FROM runtime_reconstruction_workspace_leases WHERE task_id=? AND invocation_id=? AND generation=? AND execution_epoch=?", v.Key.TaskID, v.Key.InvocationID, v.Key.Generation, v.ExecutionEpoch).Scan(&payload)
	if err == nil {
		var existing DurableWorkspaceLease
		if err = jsonUnmarshal(payload, &existing); err != nil {
			return DurableWorkspaceLease{}, err
		}
		if existing.Ref != v.Ref || existing.WorkspaceRef != v.WorkspaceRef {
			return DurableWorkspaceLease{}, ErrWorkspaceLeaseConflict
		}
		return existing, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return DurableWorkspaceLease{}, err
	}
	v.Revision, v.Fence, v.State, v.AcquiredAt = 1, 1, "active", time.Now().UTC()
	payload, err = reconstructionJSON(v)
	if err != nil {
		return DurableWorkspaceLease{}, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO runtime_reconstruction_workspace_leases VALUES(?,?,?,?,?,?,?,?)", v.Ref, v.Key.TaskID, v.Key.InvocationID, v.Key.Generation, v.ExecutionEpoch, v.Revision, v.State, payload); err != nil {
		return DurableWorkspaceLease{}, err
	}
	return v, tx.Commit()
}
func (s sqliteReconstructionStore) ReadWorkspaceLease(ctx context.Context, ref string, key WaitingKey, epoch ExecutionEpoch) (DurableWorkspaceLease, bool, error) {
	var payload []byte
	err := s.r.db.QueryRowContext(ctx, "SELECT payload FROM runtime_reconstruction_workspace_leases WHERE lease_ref=? AND task_id=? AND invocation_id=? AND generation=? AND execution_epoch=?", ref, key.TaskID, key.InvocationID, key.Generation, epoch).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return DurableWorkspaceLease{}, false, nil
	}
	if err != nil {
		return DurableWorkspaceLease{}, false, err
	}
	var value DurableWorkspaceLease
	err = jsonUnmarshal(payload, &value)
	return value, err == nil, err
}
func (s sqliteReconstructionStore) ReleaseWorkspaceLease(ctx context.Context, v DurableWorkspaceLease, expected int64) (DurableWorkspaceLease, bool, error) {
	if err := validReconstruction(v.Key, v.Ref); err != nil || v.ExecutionEpoch <= 0 {
		return DurableWorkspaceLease{}, false, ErrWorkspaceLeaseConflict
	}
	tx, err := s.r.db.BeginTx(ctx, nil)
	if err != nil {
		return DurableWorkspaceLease{}, false, err
	}
	defer tx.Rollback()
	var payload []byte
	err = tx.QueryRowContext(ctx, "SELECT payload FROM runtime_reconstruction_workspace_leases WHERE lease_ref=? AND task_id=? AND invocation_id=? AND generation=? AND execution_epoch=?", v.Ref, v.Key.TaskID, v.Key.InvocationID, v.Key.Generation, v.ExecutionEpoch).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return DurableWorkspaceLease{}, false, nil
	}
	if err != nil {
		return DurableWorkspaceLease{}, false, err
	}
	var current DurableWorkspaceLease
	if err = jsonUnmarshal(payload, &current); err != nil {
		return DurableWorkspaceLease{}, false, err
	}
	if current.State == "released" && current.WorkspaceRef == v.WorkspaceRef {
		return current, true, tx.Commit()
	}
	if current.State != "active" || current.Revision != expected || current.WorkspaceRef != v.WorkspaceRef {
		return current, false, nil
	}
	current.State, current.Revision = "released", current.Revision+1
	payload, err = reconstructionJSON(current)
	if err != nil {
		return DurableWorkspaceLease{}, false, err
	}
	result, err := tx.ExecContext(ctx, "UPDATE runtime_reconstruction_workspace_leases SET revision=?,state=?,payload=? WHERE lease_ref=? AND revision=?", current.Revision, current.State, payload, current.Ref, expected)
	if err != nil {
		return DurableWorkspaceLease{}, false, err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return DurableWorkspaceLease{}, false, err
	}
	if n != 1 {
		return DurableWorkspaceLease{}, false, nil
	}
	return current, true, tx.Commit()
}

func (s sqliteReconstructionStore) PutContextSlice(ctx context.Context, v DurableContextSlice) (DurableContextSlice, error) {
	if err := validReconstruction(v.Key, v.Ref); err != nil {
		return DurableContextSlice{}, err
	}
	return putImmutableContext(ctx, s.r.db, v)
}
func (s sqliteReconstructionStore) GetContextSlice(ctx context.Context, ref string, key WaitingKey) (DurableContextSlice, bool, error) {
	return getReconstruction[DurableContextSlice](ctx, s.r.db, "runtime_reconstruction_context_slices", "context_ref", ref, key)
}
func (s sqliteReconstructionStore) PutTaskMemory(ctx context.Context, v DurableTaskMemory) (DurableTaskMemory, error) {
	if err := validReconstruction(v.Key, v.Ref); err != nil {
		return DurableTaskMemory{}, err
	}
	return putImmutableMemory(ctx, s.r.db, v)
}
func (s sqliteReconstructionStore) GetTaskMemory(ctx context.Context, ref string, key WaitingKey) (DurableTaskMemory, bool, error) {
	return getReconstruction[DurableTaskMemory](ctx, s.r.db, "runtime_reconstruction_task_memory", "memory_ref", ref, key)
}

func (s sqliteReconstructionStore) PutExecutionDescriptor(ctx context.Context, value DurableExecutionDescriptor) (DurableExecutionDescriptor, error) {
	if err := validReconstruction(value.Key, value.WorkspaceRef); err != nil || value.ContextSliceRef == "" || value.TaskMemoryRef == "" || value.TaskContract == "" || value.PhaseInstruction == "" {
		return DurableExecutionDescriptor{}, errors.New("execution descriptor logical identity and task material are required")
	}
	tx, err := s.r.db.BeginTx(ctx, nil)
	if err != nil {
		return DurableExecutionDescriptor{}, err
	}
	defer tx.Rollback()
	var payload []byte
	err = tx.QueryRowContext(ctx, "SELECT payload FROM runtime_reconstruction_execution_descriptors WHERE task_id=? AND invocation_id=? AND generation=?", value.Key.TaskID, value.Key.InvocationID, value.Key.Generation).Scan(&payload)
	if err == nil {
		var existing DurableExecutionDescriptor
		if err = jsonUnmarshal(payload, &existing); err != nil {
			return DurableExecutionDescriptor{}, err
		}
		if existing.TaskContract != value.TaskContract || existing.PhaseInstruction != value.PhaseInstruction || existing.TaskSpec != value.TaskSpec || existing.WorkspaceRef != value.WorkspaceRef || existing.ContextSliceRef != value.ContextSliceRef || existing.TaskMemoryRef != value.TaskMemoryRef {
			return DurableExecutionDescriptor{}, ErrReconstructionConflict
		}
		return existing, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return DurableExecutionDescriptor{}, err
	}
	value.Revision, value.CreatedAt = 1, time.Now().UTC()
	payload, err = reconstructionJSON(value)
	if err != nil {
		return DurableExecutionDescriptor{}, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO runtime_reconstruction_execution_descriptors VALUES(?,?,?,?,?)", value.Key.TaskID, value.Key.InvocationID, value.Key.Generation, value.Revision, payload); err != nil {
		return DurableExecutionDescriptor{}, err
	}
	return value, tx.Commit()
}
func (s sqliteReconstructionStore) GetExecutionDescriptor(ctx context.Context, key WaitingKey) (DurableExecutionDescriptor, bool, error) {
	var payload []byte
	err := s.r.db.QueryRowContext(ctx, "SELECT payload FROM runtime_reconstruction_execution_descriptors WHERE task_id=? AND invocation_id=? AND generation=?", key.TaskID, key.InvocationID, key.Generation).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return DurableExecutionDescriptor{}, false, nil
	}
	if err != nil {
		return DurableExecutionDescriptor{}, false, err
	}
	var value DurableExecutionDescriptor
	err = jsonUnmarshal(payload, &value)
	return value, err == nil, err
}

func putImmutableContext(ctx context.Context, db *sql.DB, value DurableContextSlice) (DurableContextSlice, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return DurableContextSlice{}, err
	}
	defer tx.Rollback()
	var payload []byte
	err = tx.QueryRowContext(ctx, "SELECT payload FROM runtime_reconstruction_context_slices WHERE context_ref=? AND task_id=? AND invocation_id=? AND generation=?", value.Ref, value.Key.TaskID, value.Key.InvocationID, value.Key.Generation).Scan(&payload)
	if err == nil {
		var existing DurableContextSlice
		if err = jsonUnmarshal(payload, &existing); err != nil {
			return DurableContextSlice{}, err
		}
		if existing.BaselineRef != value.BaselineRef || existing.Content != value.Content {
			return DurableContextSlice{}, ErrReconstructionConflict
		}
		return existing, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return DurableContextSlice{}, err
	}
	value.Revision, value.CreatedAt = 1, time.Now().UTC()
	payload, err = reconstructionJSON(value)
	if err != nil {
		return DurableContextSlice{}, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO runtime_reconstruction_context_slices VALUES(?,?,?,?,?,?)", value.Ref, value.Key.TaskID, value.Key.InvocationID, value.Key.Generation, value.Revision, payload); err != nil {
		return DurableContextSlice{}, err
	}
	return value, tx.Commit()
}
func putImmutableMemory(ctx context.Context, db *sql.DB, value DurableTaskMemory) (DurableTaskMemory, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return DurableTaskMemory{}, err
	}
	defer tx.Rollback()
	var payload []byte
	err = tx.QueryRowContext(ctx, "SELECT payload FROM runtime_reconstruction_task_memory WHERE memory_ref=? AND task_id=? AND invocation_id=? AND generation=?", value.Ref, value.Key.TaskID, value.Key.InvocationID, value.Key.Generation).Scan(&payload)
	if err == nil {
		var existing DurableTaskMemory
		if err = jsonUnmarshal(payload, &existing); err != nil {
			return DurableTaskMemory{}, err
		}
		if !reflect.DeepEqual(existing.View, value.View) {
			return DurableTaskMemory{}, ErrReconstructionConflict
		}
		return existing, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return DurableTaskMemory{}, err
	}
	value.Revision, value.CreatedAt = 1, time.Now().UTC()
	payload, err = reconstructionJSON(value)
	if err != nil {
		return DurableTaskMemory{}, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO runtime_reconstruction_task_memory VALUES(?,?,?,?,?,?)", value.Ref, value.Key.TaskID, value.Key.InvocationID, value.Key.Generation, value.Revision, payload); err != nil {
		return DurableTaskMemory{}, err
	}
	return value, tx.Commit()
}
func getReconstruction[T any](ctx context.Context, db *sql.DB, table, refColumn, ref string, key WaitingKey) (T, bool, error) {
	var zero T
	var payload []byte
	query := fmt.Sprintf("SELECT payload FROM %s WHERE %s=? AND task_id=? AND invocation_id=? AND generation=?", table, refColumn)
	err := db.QueryRowContext(ctx, query, ref, key.TaskID, key.InvocationID, key.Generation).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return zero, false, nil
	}
	if err != nil {
		return zero, false, err
	}
	var value T
	err = jsonUnmarshal(payload, &value)
	return value, err == nil, err
}
