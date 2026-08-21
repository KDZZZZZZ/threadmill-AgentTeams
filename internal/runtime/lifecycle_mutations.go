package runtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/executionreceipt"
)

// LifecycleMutationStore owns multi-record Runtime transactions. It is kept
// inside internal/runtime so phaseagent never observes persistence mechanics.
type LifecycleMutationStore interface {
	RecordInputRevision(context.Context, WaitingKey, StoredPhaseInputSet) error
	PersistAwaitAdmission(context.Context, WaitingRecord, ContinuationMaterial) (WaitingRecord, error)
	PrepareRehydration(context.Context, WaitingKey, int64, ContinuationBinding) (WaitingRecord, ContinuationBinding, bool, error)
	RecordPackageConsumption(context.Context, executionreceipt.Receipt, PhysicalExecutionKey, int64) (executionreceipt.Receipt, PhysicalExecution, bool, error)
	ActivatePhysicalExecution(context.Context, WaitingKey, int64, PhysicalExecutionKey, int64) (WaitingRecord, PhysicalExecution, bool, error)
	AcceptPhaseOutput(context.Context, PhaseOutputRecord, WaitingKey, int64) (PhaseOutputRecord, WaitingRecord, bool, error)
	AdvanceTeardown(context.Context, PhysicalExecutionKey, int64, TeardownStep) (PhysicalExecution, bool, error)
}

type sqliteLifecycleMutations struct{ r *SQLiteRuntimeStateRepository }

func nowUTC() time.Time                           { return time.Now().UTC() }
func isNoRows(err error) bool                     { return errors.Is(err, sql.ErrNoRows) }
func jsonUnmarshal(data []byte, target any) error { return json.Unmarshal(data, target) }

func (s sqliteLifecycleMutations) RecordInputRevision(ctx context.Context, key WaitingKey, value StoredPhaseInputSet) error {
	if key.TaskID == "" || key.InvocationID == "" || key.Generation <= 0 || value.Inputs.InputRevision == "" {
		return errors.New("input store key and input revision are required")
	}
	b, err := runtimeJSON(value)
	if err != nil {
		return err
	}
	if err = noSecrets(b); err != nil {
		return err
	}
	tx, err := s.r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var existing []byte
	err = tx.QueryRowContext(ctx, "SELECT payload FROM runtime_inputs WHERE task_id=? AND invocation_id=? AND generation=? AND input_revision=?", key.TaskID, key.InvocationID, key.Generation, value.Inputs.InputRevision).Scan(&existing)
	if err == nil {
		if string(existing) == string(b) {
			return tx.Commit()
		}
		return errors.New("conflicting immutable input revision")
	}
	if err != nil && !isNoRows(err) {
		return err
	}
	var sequence int
	if err = tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(sequence),0)+1 FROM runtime_inputs WHERE task_id=? AND invocation_id=? AND generation=?", key.TaskID, key.InvocationID, key.Generation).Scan(&sequence); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO runtime_inputs VALUES(?,?,?,?,?,?)", key.TaskID, key.InvocationID, key.Generation, value.Inputs.InputRevision, sequence, b); err != nil {
		return err
	}
	if err = appendEvent(ctx, tx, "InputRevisionObserved", key, 0, "inputs", int64(sequence), b); err != nil {
		return err
	}
	return tx.Commit()
}

func (s sqliteLifecycleMutations) PersistAwaitAdmission(ctx context.Context, record WaitingRecord, material ContinuationMaterial) (WaitingRecord, error) {
	if err := record.Validate(); err != nil {
		return WaitingRecord{}, err
	}
	if material.Endpoint != record.Endpoint {
		return WaitingRecord{}, errors.New("continuation material endpoint is incompatible")
	}
	record.Revision = 1
	record.CreatedAt = nowUTC()
	record.UpdatedAt = record.CreatedAt
	cb, err := runtimeJSON(material)
	if err != nil {
		return WaitingRecord{}, err
	}
	rb, err := runtimeJSON(record)
	if err != nil {
		return WaitingRecord{}, err
	}
	if err = noSecrets(cb); err != nil {
		return WaitingRecord{}, err
	}
	if err = noSecrets(rb); err != nil {
		return WaitingRecord{}, err
	}
	tx, err := s.r.db.BeginTx(ctx, nil)
	if err != nil {
		return WaitingRecord{}, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "INSERT INTO runtime_continuations VALUES(?,?)", record.ContinuationRef, cb); err != nil {
		return WaitingRecord{}, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO runtime_waiting VALUES(?,?,?,?,?,?)", record.Key.TaskID, record.Key.InvocationID, record.Key.Generation, record.Revision, record.State, rb); err != nil {
		return WaitingRecord{}, mapUnique(err, ErrWaitingRecordExists)
	}
	if err = appendEvent(ctx, tx, "AwaitAdmitted", record.Key, record.ExecutionEpoch, "waiting", record.Revision, rb); err != nil {
		return WaitingRecord{}, err
	}
	return record, tx.Commit()
}

func (s sqliteLifecycleMutations) PrepareRehydration(ctx context.Context, key WaitingKey, expected int64, binding ContinuationBinding) (WaitingRecord, ContinuationBinding, bool, error) {
	if err := ValidateContinuationRebind(binding); err != nil {
		return WaitingRecord{}, ContinuationBinding{}, false, err
	}
	b, err := runtimeJSON(binding)
	if err != nil {
		return WaitingRecord{}, ContinuationBinding{}, false, err
	}
	if err = noSecrets(b); err != nil {
		return WaitingRecord{}, ContinuationBinding{}, false, err
	}
	tx, err := s.r.db.BeginTx(ctx, nil)
	if err != nil {
		return WaitingRecord{}, ContinuationBinding{}, false, err
	}
	defer tx.Rollback()
	var payload []byte
	err = tx.QueryRowContext(ctx, "SELECT payload FROM runtime_waiting WHERE task_id=? AND invocation_id=? AND generation=? AND revision=?", key.TaskID, key.InvocationID, key.Generation, expected).Scan(&payload)
	if isNoRows(err) {
		return WaitingRecord{}, ContinuationBinding{}, false, nil
	}
	if err != nil {
		return WaitingRecord{}, ContinuationBinding{}, false, err
	}
	var waiting WaitingRecord
	if err = jsonUnmarshal(payload, &waiting); err != nil {
		return WaitingRecord{}, ContinuationBinding{}, false, err
	}
	if waiting.State != AwaitStateWaiting || waiting.ExecutionEpoch+1 != binding.ExecutionEpoch || waiting.PreviousBindingRef != binding.PreviousBindingRef || waiting.InputRevision != binding.PreviousRevision {
		return WaitingRecord{}, ContinuationBinding{}, false, nil
	}
	waiting.State = AwaitStateRehydrating
	waiting.Revision++
	waiting.UpdatedAt = nowUTC()
	wb, err := runtimeJSON(waiting)
	if err != nil {
		return WaitingRecord{}, ContinuationBinding{}, false, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO runtime_bindings VALUES(?,?,?,?,?)", binding.BindingRef, binding.InvocationID, binding.Generation, binding.ExecutionEpoch, b); err != nil {
		return WaitingRecord{}, ContinuationBinding{}, false, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE runtime_waiting SET revision=?,state=?,payload=? WHERE task_id=? AND invocation_id=? AND generation=? AND revision=?", waiting.Revision, waiting.State, wb, key.TaskID, key.InvocationID, key.Generation, expected); err != nil {
		return WaitingRecord{}, ContinuationBinding{}, false, err
	}
	if err = appendEvent(ctx, tx, "RehydrationPrepared", key, binding.ExecutionEpoch, "waiting", waiting.Revision, wb); err != nil {
		return WaitingRecord{}, ContinuationBinding{}, false, err
	}
	return waiting, binding, true, tx.Commit()
}

func (s sqliteLifecycleMutations) RecordPackageConsumption(ctx context.Context, receipt executionreceipt.Receipt, key PhysicalExecutionKey, expected int64) (executionreceipt.Receipt, PhysicalExecution, bool, error) {
	if receipt.Key() != (executionreceipt.Key{TaskID: key.TaskID, InvocationID: key.InvocationID, Generation: key.Generation, ExecutionEpoch: int64(key.ExecutionEpoch)}) || !receipt.Consumed {
		return executionreceipt.Receipt{}, PhysicalExecution{}, false, errors.New("receipt does not match physical execution")
	}
	tx, err := s.r.db.BeginTx(ctx, nil)
	if err != nil {
		return executionreceipt.Receipt{}, PhysicalExecution{}, false, err
	}
	defer tx.Rollback()
	var pb []byte
	err = tx.QueryRowContext(ctx, "SELECT payload FROM runtime_physical_executions WHERE task_id=? AND invocation_id=? AND generation=? AND execution_epoch=? AND revision=?", key.TaskID, key.InvocationID, key.Generation, key.ExecutionEpoch, expected).Scan(&pb)
	if isNoRows(err) {
		return executionreceipt.Receipt{}, PhysicalExecution{}, false, nil
	}
	if err != nil {
		return executionreceipt.Receipt{}, PhysicalExecution{}, false, err
	}
	var physical PhysicalExecution
	if err = jsonUnmarshal(pb, &physical); err != nil {
		return executionreceipt.Receipt{}, PhysicalExecution{}, false, err
	}
	if physical.Key() != key || physical.State != PhysicalExecutionAccepted || physical.BindingRef != receipt.BindingRef || physical.InputRevision != receipt.InputRevision || physical.AgentPackageDigest != receipt.PackageDigest || physical.AgentSessionRef != receipt.SessionIdentity {
		return executionreceipt.Receipt{}, PhysicalExecution{}, false, errors.New("receipt conflicts with physical execution")
	}
	var rb []byte
	err = tx.QueryRowContext(ctx, "SELECT payload FROM runtime_receipts WHERE task_id=? AND invocation_id=? AND generation=? AND execution_epoch=?", receipt.TaskID, receipt.InvocationID, receipt.Generation, receipt.ExecutionEpoch).Scan(&rb)
	if err == nil {
		var old executionreceipt.Receipt
		if err = jsonUnmarshal(rb, &old); err != nil {
			return executionreceipt.Receipt{}, PhysicalExecution{}, false, err
		}
		if old.BindingRef != receipt.BindingRef || old.InputRevision != receipt.InputRevision || old.PackageDigest != receipt.PackageDigest || old.SessionIdentity != receipt.SessionIdentity {
			return old, PhysicalExecution{}, false, errors.New("conflicting package consumption receipt")
		}
		return old, physical, false, tx.Commit()
	}
	if !isNoRows(err) {
		return executionreceipt.Receipt{}, PhysicalExecution{}, false, err
	}
	receipt.Revision = 1
	receipt.RecordedAt = nowUTC()
	rb, err = runtimeJSON(receipt)
	if err != nil {
		return executionreceipt.Receipt{}, PhysicalExecution{}, false, err
	}
	physical.PackageConsumed = true
	physical.Revision++
	physical.UpdatedAt = nowUTC()
	pb, err = runtimeJSON(physical)
	if err != nil {
		return executionreceipt.Receipt{}, PhysicalExecution{}, false, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO runtime_receipts VALUES(?,?,?,?,?)", receipt.TaskID, receipt.InvocationID, receipt.Generation, receipt.ExecutionEpoch, rb); err != nil {
		return executionreceipt.Receipt{}, PhysicalExecution{}, false, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE runtime_physical_executions SET revision=?,state=?,payload=? WHERE task_id=? AND invocation_id=? AND generation=? AND execution_epoch=? AND revision=?", physical.Revision, physical.State, pb, key.TaskID, key.InvocationID, key.Generation, key.ExecutionEpoch, expected); err != nil {
		return executionreceipt.Receipt{}, PhysicalExecution{}, false, err
	}
	wk := WaitingKey{TaskID: key.TaskID, InvocationID: key.InvocationID, Generation: key.Generation}
	if err = appendEvent(ctx, tx, "PackageConsumptionRecorded", wk, key.ExecutionEpoch, "receipt", receipt.Revision, rb); err != nil {
		return executionreceipt.Receipt{}, PhysicalExecution{}, false, err
	}
	return receipt, physical, true, tx.Commit()
}
func (s sqliteLifecycleMutations) ActivatePhysicalExecution(ctx context.Context, waitingKey WaitingKey, waitingRevision int64, physicalKey PhysicalExecutionKey, physicalRevision int64) (WaitingRecord, PhysicalExecution, bool, error) {
	if waitingKey.TaskID != physicalKey.TaskID || waitingKey.InvocationID != physicalKey.InvocationID || waitingKey.Generation != physicalKey.Generation {
		return WaitingRecord{}, PhysicalExecution{}, false, errors.New("waiting and physical execution identities do not match")
	}
	tx, err := s.r.db.BeginTx(ctx, nil)
	if err != nil {
		return WaitingRecord{}, PhysicalExecution{}, false, err
	}
	defer tx.Rollback()
	var wb, pb []byte
	err = tx.QueryRowContext(ctx, "SELECT payload FROM runtime_waiting WHERE task_id=? AND invocation_id=? AND generation=?", waitingKey.TaskID, waitingKey.InvocationID, waitingKey.Generation).Scan(&wb)
	if isNoRows(err) {
		return WaitingRecord{}, PhysicalExecution{}, false, nil
	}
	if err != nil {
		return WaitingRecord{}, PhysicalExecution{}, false, err
	}
	err = tx.QueryRowContext(ctx, "SELECT payload FROM runtime_physical_executions WHERE task_id=? AND invocation_id=? AND generation=? AND execution_epoch=?", physicalKey.TaskID, physicalKey.InvocationID, physicalKey.Generation, physicalKey.ExecutionEpoch).Scan(&pb)
	if isNoRows(err) {
		return WaitingRecord{}, PhysicalExecution{}, false, nil
	}
	if err != nil {
		return WaitingRecord{}, PhysicalExecution{}, false, err
	}
	var waiting WaitingRecord
	var physical PhysicalExecution
	if err = jsonUnmarshal(wb, &waiting); err != nil {
		return WaitingRecord{}, PhysicalExecution{}, false, err
	}
	if err = jsonUnmarshal(pb, &physical); err != nil {
		return WaitingRecord{}, PhysicalExecution{}, false, err
	}
	if physical.Key() != physicalKey || waiting.Key != waitingKey {
		return WaitingRecord{}, PhysicalExecution{}, false, errors.New("persisted execution identity mismatch")
	}
	if waiting.State == AwaitStateRunning && physical.State == PhysicalExecutionRunning && waiting.ExecutionEpoch == physicalKey.ExecutionEpoch && waiting.PreviousBindingRef == physical.BindingRef && waiting.InputRevision == physical.InputRevision {
		return waiting, physical, false, tx.Commit()
	}
	if waiting.Revision != waitingRevision || physical.Revision != physicalRevision {
		return WaitingRecord{}, PhysicalExecution{}, false, nil
	}
	if waiting.State != AwaitStateRehydrating || physical.State != PhysicalExecutionAccepted || !physical.PackageConsumed || waiting.ExecutionEpoch+1 != physicalKey.ExecutionEpoch {
		return WaitingRecord{}, PhysicalExecution{}, false, errors.New("execution is not eligible for activation")
	}
	if physical.BindingRef == "" || physical.InputRevision == "" {
		return WaitingRecord{}, PhysicalExecution{}, false, errors.New("activation physical binding is required")
	}
	waiting.State = AwaitStateRunning
	waiting.ExecutionEpoch = physicalKey.ExecutionEpoch
	waiting.PreviousBindingRef = physical.BindingRef
	waiting.InputRevision = physical.InputRevision
	waiting.Revision++
	waiting.UpdatedAt = nowUTC()
	physical.State = PhysicalExecutionRunning
	physical.Revision++
	physical.UpdatedAt = waiting.UpdatedAt
	wb, err = runtimeJSON(waiting)
	if err != nil {
		return WaitingRecord{}, PhysicalExecution{}, false, err
	}
	pb, err = runtimeJSON(physical)
	if err != nil {
		return WaitingRecord{}, PhysicalExecution{}, false, err
	}
	if err = noSecrets(wb); err != nil {
		return WaitingRecord{}, PhysicalExecution{}, false, err
	}
	if err = noSecrets(pb); err != nil {
		return WaitingRecord{}, PhysicalExecution{}, false, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE runtime_waiting SET revision=?,state=?,payload=? WHERE task_id=? AND invocation_id=? AND generation=? AND revision=?", waiting.Revision, waiting.State, wb, waitingKey.TaskID, waitingKey.InvocationID, waitingKey.Generation, waitingRevision); err != nil {
		return WaitingRecord{}, PhysicalExecution{}, false, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE runtime_physical_executions SET revision=?,state=?,payload=? WHERE task_id=? AND invocation_id=? AND generation=? AND execution_epoch=? AND revision=?", physical.Revision, physical.State, pb, physicalKey.TaskID, physicalKey.InvocationID, physicalKey.Generation, physicalKey.ExecutionEpoch, physicalRevision); err != nil {
		return WaitingRecord{}, PhysicalExecution{}, false, err
	}
	if err = appendEvent(ctx, tx, "PhysicalExecutionActivated", waitingKey, physicalKey.ExecutionEpoch, "physical", physical.Revision, pb); err != nil {
		return WaitingRecord{}, PhysicalExecution{}, false, err
	}
	return waiting, physical, true, tx.Commit()
}

func (s sqliteLifecycleMutations) AcceptPhaseOutput(ctx context.Context, output PhaseOutputRecord, waitingKey WaitingKey, waitingRevision int64) (PhaseOutputRecord, WaitingRecord, bool, error) {
	if output.Key.TaskID != waitingKey.TaskID || output.Key.InvocationID != waitingKey.InvocationID || output.Key.Generation != waitingKey.Generation || output.BindingRef == "" || output.InputRevision == "" || output.ExecutionEpoch <= 0 {
		return PhaseOutputRecord{}, WaitingRecord{}, false, errors.New("phase output identity does not match waiting invocation")
	}
	tx, err := s.r.db.BeginTx(ctx, nil)
	if err != nil {
		return PhaseOutputRecord{}, WaitingRecord{}, false, err
	}
	defer tx.Rollback()
	var ob []byte
	err = tx.QueryRowContext(ctx, "SELECT payload FROM runtime_phase_outputs WHERE task_id=? AND invocation_id=? AND generation=?", output.Key.TaskID, output.Key.InvocationID, output.Key.Generation).Scan(&ob)
	if err == nil {
		var old PhaseOutputRecord
		if err = jsonUnmarshal(ob, &old); err != nil {
			return PhaseOutputRecord{}, WaitingRecord{}, false, err
		}
		if old.BindingRef != output.BindingRef || old.InputRevision != output.InputRevision || old.ExecutionEpoch != output.ExecutionEpoch || !reflect.DeepEqual(old.Output, output.Output) {
			return old, WaitingRecord{}, false, ErrConflictingOutput
		}
		var wb []byte
		if err = tx.QueryRowContext(ctx, "SELECT payload FROM runtime_waiting WHERE task_id=? AND invocation_id=? AND generation=?", waitingKey.TaskID, waitingKey.InvocationID, waitingKey.Generation).Scan(&wb); err != nil {
			return old, WaitingRecord{}, false, err
		}
		var waiting WaitingRecord
		err = jsonUnmarshal(wb, &waiting)
		return old, waiting, false, err
	}
	if !isNoRows(err) {
		return PhaseOutputRecord{}, WaitingRecord{}, false, err
	}
	var wb []byte
	err = tx.QueryRowContext(ctx, "SELECT payload FROM runtime_waiting WHERE task_id=? AND invocation_id=? AND generation=? AND revision=?", waitingKey.TaskID, waitingKey.InvocationID, waitingKey.Generation, waitingRevision).Scan(&wb)
	if isNoRows(err) {
		return PhaseOutputRecord{}, WaitingRecord{}, false, ErrCompletionConflict
	}
	if err != nil {
		return PhaseOutputRecord{}, WaitingRecord{}, false, err
	}
	var waiting WaitingRecord
	if err = jsonUnmarshal(wb, &waiting); err != nil {
		return PhaseOutputRecord{}, WaitingRecord{}, false, err
	}
	if waiting.State != AwaitStateRunning || waiting.ExecutionEpoch != output.ExecutionEpoch || waiting.PreviousBindingRef != output.BindingRef || waiting.InputRevision != output.InputRevision {
		return PhaseOutputRecord{}, WaitingRecord{}, false, ErrStaleCompletion
	}
	output.Revision = 1
	output.AcceptedAt = nowUTC()
	output.EventRecorded = true
	ob, err = runtimeJSON(output)
	if err != nil {
		return PhaseOutputRecord{}, WaitingRecord{}, false, err
	}
	if err = noSecrets(ob); err != nil {
		return PhaseOutputRecord{}, WaitingRecord{}, false, err
	}
	waiting.State = AwaitStateTerminal
	waiting.Revision++
	waiting.UpdatedAt = output.AcceptedAt
	wb, err = runtimeJSON(waiting)
	if err != nil {
		return PhaseOutputRecord{}, WaitingRecord{}, false, err
	}
	if err = noSecrets(wb); err != nil {
		return PhaseOutputRecord{}, WaitingRecord{}, false, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO runtime_phase_outputs VALUES(?,?,?,?,?)", output.Key.TaskID, output.Key.InvocationID, output.Key.Generation, output.Revision, ob); err != nil {
		return PhaseOutputRecord{}, WaitingRecord{}, false, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE runtime_waiting SET revision=?,state=?,payload=? WHERE task_id=? AND invocation_id=? AND generation=? AND revision=?", waiting.Revision, waiting.State, wb, waitingKey.TaskID, waitingKey.InvocationID, waitingKey.Generation, waitingRevision); err != nil {
		return PhaseOutputRecord{}, WaitingRecord{}, false, err
	}
	if err = appendEvent(ctx, tx, "PhaseOutputSubmitted", waitingKey, output.ExecutionEpoch, "phase_output", output.Revision, ob); err != nil {
		return PhaseOutputRecord{}, WaitingRecord{}, false, err
	}
	return output, waiting, true, tx.Commit()
}

// AdvanceTeardown records exactly one intent, completed external cleanup step,
// or final termination. Callers must invoke the external side effect outside
// this transaction and only then advance its matching completion step.
func (s sqliteLifecycleMutations) AdvanceTeardown(ctx context.Context, key PhysicalExecutionKey, expected int64, step TeardownStep) (PhysicalExecution, bool, error) {
	if key.TaskID == "" || key.InvocationID == "" || key.Generation <= 0 || key.ExecutionEpoch <= 0 {
		return PhysicalExecution{}, false, errors.New("physical execution identity is required")
	}
	tx, err := s.r.db.BeginTx(ctx, nil)
	if err != nil {
		return PhysicalExecution{}, false, err
	}
	defer tx.Rollback()
	var payload []byte
	err = tx.QueryRowContext(ctx, "SELECT payload FROM runtime_physical_executions WHERE task_id=? AND invocation_id=? AND generation=? AND execution_epoch=? AND revision=?", key.TaskID, key.InvocationID, key.Generation, key.ExecutionEpoch, expected).Scan(&payload)
	if isNoRows(err) {
		return PhysicalExecution{}, false, ErrCompletionConflict
	}
	if err != nil {
		return PhysicalExecution{}, false, err
	}
	var execution PhysicalExecution
	if err = jsonUnmarshal(payload, &execution); err != nil {
		return PhysicalExecution{}, false, err
	}
	if execution.Key() != key {
		return PhysicalExecution{}, false, errors.New("physical execution key does not match record")
	}
	changed := false
	eventType := ""
	switch step {
	case TeardownStepBegin:
		if execution.State == PhysicalExecutionTearingDown || execution.State == PhysicalExecutionTerminated {
			return execution, false, tx.Commit()
		}
		if execution.State != PhysicalExecutionRunning {
			return PhysicalExecution{}, false, ErrStaleCompletion
		}
		execution.State = PhysicalExecutionTearingDown
		eventType = "PhysicalExecutionTeardownStarted"
		changed = true
	case TeardownStepTerminate:
		if execution.State == PhysicalExecutionTerminated {
			return execution, false, tx.Commit()
		}
		if execution.State != PhysicalExecutionTearingDown || !allTeardownStepsDone(execution.Teardown) {
			return PhysicalExecution{}, false, ErrStaleCompletion
		}
		execution.State = PhysicalExecutionTerminated
		eventType = "PhysicalExecutionTerminated"
		changed = true
	case TeardownStepTask, TeardownStepWorker, TeardownStepMCP, TeardownStepCredential, TeardownStepToken, TeardownStepLease:
		if execution.State == PhysicalExecutionTerminated {
			if teardownStepDone(execution.Teardown, step) {
				return execution, false, tx.Commit()
			}
			return PhysicalExecution{}, false, ErrStaleCompletion
		}
		if execution.State != PhysicalExecutionTearingDown {
			return PhysicalExecution{}, false, ErrStaleCompletion
		}
		if teardownStepDone(execution.Teardown, step) {
			return execution, false, tx.Commit()
		}
		if err = markTeardownStep(&execution.Teardown, step); err != nil {
			return PhysicalExecution{}, false, err
		}
		eventType = "PhysicalExecutionTeardownStepCompleted"
		changed = true
	default:
		return PhysicalExecution{}, false, errors.New("unknown teardown step")
	}
	if !changed {
		return execution, false, tx.Commit()
	}
	execution.Revision++
	execution.UpdatedAt = nowUTC()
	payload, err = runtimeJSON(execution)
	if err != nil {
		return PhysicalExecution{}, false, err
	}
	if err = noSecrets(payload); err != nil {
		return PhysicalExecution{}, false, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE runtime_physical_executions SET revision=?,state=?,payload=? WHERE task_id=? AND invocation_id=? AND generation=? AND execution_epoch=? AND revision=?", execution.Revision, execution.State, payload, key.TaskID, key.InvocationID, key.Generation, key.ExecutionEpoch, expected); err != nil {
		return PhysicalExecution{}, false, err
	}
	waitingKey := WaitingKey{TaskID: key.TaskID, InvocationID: key.InvocationID, Generation: key.Generation}
	if err = appendEvent(ctx, tx, eventType, waitingKey, key.ExecutionEpoch, "physical_teardown", execution.Revision, payload); err != nil {
		return PhysicalExecution{}, false, err
	}
	return execution, true, tx.Commit()
}
