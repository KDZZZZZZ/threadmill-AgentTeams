DROP INDEX IF EXISTS phase_recovery_obligations_output_command_idx;

ALTER TABLE phase_recovery_obligations
  DROP CONSTRAINT IF EXISTS phase_recovery_obligations_output_receipt_object_check,
  DROP CONSTRAINT IF EXISTS phase_recovery_obligations_terminal_mutex_check,
  DROP CONSTRAINT IF EXISTS phase_recovery_obligations_output_pair_check,
  DROP COLUMN IF EXISTS output_receipt,
  DROP COLUMN IF EXISTS output_command_id;
