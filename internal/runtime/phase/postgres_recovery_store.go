package phase

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	baseruntime "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime"
)

type PostgresRecoveryStore struct {
	db          baseruntime.DBTX
	invocations baseruntime.InvocationStore
}

func NewPostgresRecoveryStore(db baseruntime.DBTX, invocations baseruntime.InvocationStore) *PostgresRecoveryStore {
	return &PostgresRecoveryStore{db: db, invocations: invocations}
}

func NewPostgresRecoveryStoreFromSQL(db baseruntime.SQLDBTX, invocations baseruntime.InvocationStore) *PostgresRecoveryStore {
	return NewPostgresRecoveryStore(baseruntime.WrapSQLDBTX(db), invocations)
}

func (s *PostgresRecoveryStore) RecordActiveInvocation(ctx context.Context, active ActiveInvocation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if active.Command.ID == "" || active.Invocation.ID == "" {
		return kernel.InvalidArgument("active invocation requires command and invocation ids")
	}
	if deterministicInvocationID(active.Command) != active.Invocation.ID {
		return kernel.Error{Code: kernel.CodeCommandConflict, Message: "run command does not match invocation id", Recoverable: false}
	}
	result, err := s.db.ExecContext(ctx, `
INSERT INTO phase_recovery_obligations (run_command_id, active)
VALUES ($1, TRUE)
ON CONFLICT (run_command_id) DO UPDATE
SET active = CASE
    WHEN phase_recovery_obligations.stop_command_id IS NULL
      AND phase_recovery_obligations.output_command_id IS NULL THEN TRUE
    ELSE phase_recovery_obligations.active
  END`,
		active.Command.ID,
	)
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err == nil && affected == 0 {
		return kernel.Error{Code: kernel.CodeRevisionConflict, Message: "active recovery obligation was not recorded", Recoverable: true}
	}
	return nil
}

func (s *PostgresRecoveryStore) RecoverActiveInvocation(ctx context.Context, command PhaseCommand, binding BindingSnapshot) (ActiveInvocation, bool, error) {
	if err := ctx.Err(); err != nil {
		return ActiveInvocation{}, false, err
	}
	if s.invocations == nil {
		return ActiveInvocation{}, false, kernel.Error{Code: kernel.CodeInternalError, Message: "invocation store is required for phase recovery", Recoverable: false}
	}
	invocation, ok, err := s.invocations.GetByLease(ctx, command.LeaseRef)
	if err != nil || !ok {
		return ActiveInvocation{}, ok, err
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT run_command_id
FROM phase_recovery_obligations
WHERE active = TRUE
  AND output_command_id IS NULL
  AND output_receipt IS NULL
ORDER BY run_command_id`)
	if err != nil {
		return ActiveInvocation{}, false, err
	}
	defer rows.Close()
	for rows.Next() {
		var runCommandID string
		if err := rows.Scan(&runCommandID); err != nil {
			return ActiveInvocation{}, false, err
		}
		if deterministicInvocationID(PhaseCommand{ID: runCommandID}) != invocation.ID {
			continue
		}
		active := ActiveInvocation{
			Invocation: invocation,
			Command:    command,
			Binding:    cloneBindingSnapshot(binding),
			Inputs:     clonePhaseInputSet(binding.Inputs),
		}
		return active, true, nil
	}
	if err := rows.Err(); err != nil {
		return ActiveInvocation{}, false, err
	}
	return ActiveInvocation{}, false, nil
}

func (s *PostgresRecoveryStore) RecordStopEvidence(ctx context.Context, active ActiveInvocation, command PhaseCommand, result StopResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateStopResult(result); err != nil {
		return err
	}
	runCommandID, ok, err := s.runCommandIDForInvocation(ctx, active.Invocation.ID)
	if err != nil {
		return err
	}
	if !ok {
		return kernel.Error{Code: kernel.CodeStaleCommand, Message: "active recovery obligation not found", Recoverable: true}
	}
	payload, err := json.Marshal(result)
	if err != nil {
		return kernel.Error{Code: kernel.CodeInternalError, Message: "stop result cannot be encoded", Recoverable: false}
	}
	existing, found, err := s.stopEvidenceForRun(ctx, runCommandID)
	if err != nil {
		return err
	}
	if found {
		if existing.CommandID == command.ID && sameStopResultPayload(existing.Payload, result) {
			return nil
		}
		return kernel.Error{Code: kernel.CodeIdempotencyConflict, Message: "stop evidence already exists with different payload", Recoverable: false}
	}
	if _, found, err := s.outputReceiptForRun(ctx, runCommandID); err != nil {
		return err
	} else if found {
		return kernel.Error{Code: kernel.CodeStaleCommand, Message: "output receipt already won terminal recovery", Recoverable: true}
	}
	sqlResult, err := s.db.ExecContext(ctx, `
UPDATE phase_recovery_obligations
SET stop_command_id = $2, stop_result = $3::jsonb
WHERE run_command_id = $1
  AND active = TRUE
  AND stop_command_id IS NULL
  AND stop_result IS NULL
  AND output_command_id IS NULL
  AND output_receipt IS NULL`,
		runCommandID,
		command.ID,
		string(payload),
	)
	if err != nil {
		return err
	}
	if affected, err := sqlResult.RowsAffected(); err == nil && affected == 1 {
		return nil
	}
	existing, found, err = s.stopEvidenceForRun(ctx, runCommandID)
	if err != nil {
		return err
	}
	if found && existing.CommandID == command.ID && sameStopResultPayload(existing.Payload, result) {
		return nil
	}
	if _, found, err := s.outputReceiptForRun(ctx, runCommandID); err != nil {
		return err
	} else if found {
		return kernel.Error{Code: kernel.CodeStaleCommand, Message: "output receipt already won terminal recovery", Recoverable: true}
	}
	return kernel.Error{Code: kernel.CodeRevisionConflict, Message: "stop evidence was not recorded", Recoverable: true}
}

func (s *PostgresRecoveryStore) RecordOutputReceipt(ctx context.Context, active ActiveInvocation, receipt OutputReceipt) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if receipt.InvocationID != active.Invocation.ID || receipt.Endpoint != active.Command.Endpoint || receipt.Generation != active.Command.Generation || receipt.BindingRef != active.Command.BindingRef || receipt.LeaseRef != active.Command.LeaseRef {
		return kernel.Error{Code: kernel.CodeIdempotencyConflict, Message: "output receipt does not match active invocation", Recoverable: false}
	}
	runCommandID, ok, err := s.runCommandIDForInvocation(ctx, active.Invocation.ID)
	if err != nil {
		return err
	}
	if !ok {
		return kernel.Error{Code: kernel.CodeStaleCommand, Message: "active recovery obligation not found", Recoverable: true}
	}
	payload, err := json.Marshal(receipt)
	if err != nil {
		return kernel.Error{Code: kernel.CodeInternalError, Message: "output receipt cannot be encoded", Recoverable: false}
	}
	existing, found, err := s.outputReceiptForRun(ctx, runCommandID)
	if err != nil {
		return err
	}
	if found {
		if existing.CommandID == active.Command.ID && sameOutputReceiptPayload(existing.Payload, receipt) {
			return nil
		}
		return kernel.Error{Code: kernel.CodeIdempotencyConflict, Message: "output receipt already exists with different payload", Recoverable: false}
	}
	if _, found, err := s.stopEvidenceForRun(ctx, runCommandID); err != nil {
		return err
	} else if found {
		return kernel.Error{Code: kernel.CodeStaleCommand, Message: "stop evidence already won terminal recovery", Recoverable: true}
	}
	sqlResult, err := s.db.ExecContext(ctx, `
UPDATE phase_recovery_obligations
SET output_command_id = $2, output_receipt = $3::jsonb
WHERE run_command_id = $1
  AND active = TRUE
  AND output_command_id IS NULL
  AND output_receipt IS NULL
  AND stop_command_id IS NULL
  AND stop_result IS NULL`,
		runCommandID,
		active.Command.ID,
		string(payload),
	)
	if err != nil {
		return err
	}
	if affected, err := sqlResult.RowsAffected(); err == nil && affected == 1 {
		return nil
	}
	existing, found, err = s.outputReceiptForRun(ctx, runCommandID)
	if err != nil {
		return err
	}
	if found && existing.CommandID == active.Command.ID && sameOutputReceiptPayload(existing.Payload, receipt) {
		return nil
	}
	if _, found, err := s.stopEvidenceForRun(ctx, runCommandID); err != nil {
		return err
	} else if found {
		return kernel.Error{Code: kernel.CodeStaleCommand, Message: "stop evidence already won terminal recovery", Recoverable: true}
	}
	return kernel.Error{Code: kernel.CodeRevisionConflict, Message: "output receipt was not recorded", Recoverable: true}
}

func (s *PostgresRecoveryStore) GetOutputReceipt(ctx context.Context, invocationID kernel.InvocationID, commandID string) (OutputReceipt, bool, error) {
	if err := ctx.Err(); err != nil {
		return OutputReceipt{}, false, err
	}
	runCommandID, ok, err := s.runCommandIDForInvocation(ctx, invocationID)
	if err != nil || !ok {
		return OutputReceipt{}, ok, err
	}
	var payload []byte
	query := `
SELECT output_receipt
FROM phase_recovery_obligations
WHERE run_command_id = $1 AND output_receipt IS NOT NULL`
	args := []any{runCommandID}
	if commandID != "" {
		query += ` AND output_command_id = $2`
		args = append(args, commandID)
	}
	err = s.db.QueryRowContext(ctx, query, args...).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return OutputReceipt{}, false, nil
	}
	if err != nil {
		return OutputReceipt{}, false, err
	}
	var receipt OutputReceipt
	if err := json.Unmarshal(payload, &receipt); err != nil {
		return OutputReceipt{}, false, err
	}
	return receipt, true, nil
}

func (s *PostgresRecoveryStore) GetStopEvidence(ctx context.Context, invocationID kernel.InvocationID, commandID string) (StopResult, bool, error) {
	if err := ctx.Err(); err != nil {
		return StopResult{}, false, err
	}
	runCommandID, ok, err := s.runCommandIDForInvocation(ctx, invocationID)
	if err != nil || !ok {
		return StopResult{}, ok, err
	}
	var payload []byte
	err = s.db.QueryRowContext(ctx, `
SELECT stop_result
FROM phase_recovery_obligations
WHERE run_command_id = $1 AND stop_command_id = $2 AND stop_result IS NOT NULL`,
		runCommandID,
		commandID,
	).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return StopResult{}, false, nil
	}
	if err != nil {
		return StopResult{}, false, err
	}
	var result StopResult
	if err := json.Unmarshal(payload, &result); err != nil {
		return StopResult{}, false, err
	}
	return result, true, nil
}

func (s *PostgresRecoveryStore) ClearActiveInvocation(ctx context.Context, invocationID kernel.InvocationID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	runCommandID, ok, err := s.runCommandIDForInvocation(ctx, invocationID)
	if err != nil || !ok {
		return err
	}
	_, err = s.db.ExecContext(ctx, `
UPDATE phase_recovery_obligations
SET active = FALSE
WHERE run_command_id = $1`,
		runCommandID,
	)
	return err
}

func (s *PostgresRecoveryStore) ValidateResume(ctx context.Context, command PhaseCommand, binding BindingSnapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if binding.NonResumable || binding.CheckpointRef == "" {
		return kernel.Error{Code: kernel.CodeStaleCheckpoint, Message: "checkpoint is not resumable", Recoverable: true}
	}
	if s.invocations == nil {
		return kernel.Error{Code: kernel.CodeInternalError, Message: "invocation store is required for resume validation", Recoverable: false}
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT run_command_id, stop_result
FROM phase_recovery_obligations
WHERE stop_result IS NOT NULL`)
	if err != nil {
		return err
	}
	var matchingRunCommandIDs []string
	for rows.Next() {
		var runCommandID string
		var payload []byte
		if err := rows.Scan(&runCommandID, &payload); err != nil {
			_ = rows.Close()
			return err
		}
		var result StopResult
		if err := json.Unmarshal(payload, &result); err != nil {
			_ = rows.Close()
			return err
		}
		if !result.NonResumable && result.CheckpointRef == binding.CheckpointRef {
			matchingRunCommandIDs = append(matchingRunCommandIDs, runCommandID)
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	// Close the recovery scan before consulting InvocationStore. Production
	// deployments may intentionally use a single SQL connection, so holding
	// rows open here would otherwise deadlock the nested lookup.
	for _, runCommandID := range matchingRunCommandIDs {
		stoppedInvocation, ok, err := s.invocations.Get(ctx, deterministicInvocationID(PhaseCommand{ID: runCommandID}))
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		if resumeScopeMatches(stoppedInvocation, command, binding) {
			return nil
		}
	}
	return kernel.Error{Code: kernel.CodeStaleCheckpoint, Message: "resume checkpoint has no persisted stop evidence", Recoverable: true}
}

func resumeScopeMatches(stopped baseruntime.Invocation, command PhaseCommand, binding BindingSnapshot) bool {
	return stopped.ProjectID == binding.ProjectID &&
		stopped.TaskID == command.Endpoint.TaskID &&
		stopped.EndpointID == command.Endpoint.EndpointID &&
		command.Generation > 0 &&
		uint64(command.Generation) == stopped.Generation+1 &&
		binding.TaskID == command.Endpoint.TaskID &&
		binding.EndpointID == command.Endpoint.EndpointID &&
		binding.Generation == command.Generation
}

type persistedStopEvidence struct {
	CommandID string
	Payload   []byte
}

type persistedOutputReceipt struct {
	CommandID string
	Payload   []byte
}

func (s *PostgresRecoveryStore) outputReceiptForRun(ctx context.Context, runCommandID string) (persistedOutputReceipt, bool, error) {
	var receipt persistedOutputReceipt
	err := s.db.QueryRowContext(ctx, `
SELECT COALESCE(output_command_id, ''), COALESCE(output_receipt, '{}'::jsonb)
FROM phase_recovery_obligations
WHERE run_command_id = $1 AND output_command_id IS NOT NULL AND output_receipt IS NOT NULL`,
		runCommandID,
	).Scan(&receipt.CommandID, &receipt.Payload)
	if errors.Is(err, sql.ErrNoRows) {
		return persistedOutputReceipt{}, false, nil
	}
	if err != nil {
		return persistedOutputReceipt{}, false, err
	}
	return receipt, true, nil
}

func (s *PostgresRecoveryStore) stopEvidenceForRun(ctx context.Context, runCommandID string) (persistedStopEvidence, bool, error) {
	var evidence persistedStopEvidence
	err := s.db.QueryRowContext(ctx, `
SELECT COALESCE(stop_command_id, ''), COALESCE(stop_result, '{}'::jsonb)
FROM phase_recovery_obligations
WHERE run_command_id = $1 AND stop_command_id IS NOT NULL AND stop_result IS NOT NULL`,
		runCommandID,
	).Scan(&evidence.CommandID, &evidence.Payload)
	if errors.Is(err, sql.ErrNoRows) {
		return persistedStopEvidence{}, false, nil
	}
	if err != nil {
		return persistedStopEvidence{}, false, err
	}
	return evidence, true, nil
}

func (s *PostgresRecoveryStore) runCommandIDForInvocation(ctx context.Context, invocationID kernel.InvocationID) (string, bool, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT run_command_id
FROM phase_recovery_obligations
ORDER BY run_command_id`)
	if err != nil {
		return "", false, err
	}
	defer rows.Close()
	for rows.Next() {
		var runCommandID string
		if err := rows.Scan(&runCommandID); err != nil {
			return "", false, err
		}
		if deterministicInvocationID(PhaseCommand{ID: runCommandID}) == invocationID {
			return runCommandID, true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return "", false, err
	}
	return "", false, nil
}

func sameStopResultPayload(payload []byte, expected StopResult) bool {
	var actual StopResult
	return json.Unmarshal(payload, &actual) == nil && actual == expected
}

func sameOutputReceiptPayload(payload []byte, expected OutputReceipt) bool {
	var actual OutputReceipt
	return json.Unmarshal(payload, &actual) == nil && outputReceiptsEqual(actual, expected)
}

func outputReceiptsEqual(left, right OutputReceipt) bool {
	if left.InvocationID != right.InvocationID ||
		left.CommandID != right.CommandID ||
		left.CommandAction != right.CommandAction ||
		left.CauseRef != right.CauseRef ||
		left.Endpoint != right.Endpoint ||
		left.Generation != right.Generation ||
		left.BindingRef != right.BindingRef ||
		left.LeaseRef != right.LeaseRef ||
		left.InputRevision != right.InputRevision ||
		left.WorkspaceRef != right.WorkspaceRef ||
		left.WorkspaceHead != right.WorkspaceHead ||
		left.OutputFingerprint != right.OutputFingerprint ||
		!left.SubmittedAtUTC.Equal(right.SubmittedAtUTC) ||
		left.Output.Phase != right.Output.Phase ||
		left.Output.ReportRef != right.Output.ReportRef {
		return false
	}
	return stringSlicesEqual(left.Output.DeliveryRefs, right.Output.DeliveryRefs) &&
		stringSlicesEqual(left.Output.EvidenceRefs, right.Output.EvidenceRefs)
}

func stringSlicesEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

var _ RecoveryStore = (*PostgresRecoveryStore)(nil)
