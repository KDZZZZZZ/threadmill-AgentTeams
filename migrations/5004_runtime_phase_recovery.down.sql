DROP INDEX IF EXISTS phase_recovery_obligations_stop_command_idx;
DROP INDEX IF EXISTS phase_recovery_obligations_active_idx;
DROP TABLE IF EXISTS phase_recovery_obligations;

DROP INDEX IF EXISTS runtime_invocations_lease_unique_idx;

ALTER TABLE runtime_invocations
  DROP CONSTRAINT IF EXISTS runtime_invocations_consumer_scope_check,
  DROP CONSTRAINT IF EXISTS runtime_invocations_consumer_role_check,
  DROP COLUMN IF EXISTS invocation_fingerprint,
  DROP COLUMN IF EXISTS consumer_role,
  DROP COLUMN IF EXISTS consumer_task_id,
  DROP COLUMN IF EXISTS consumer_invocation_id;
