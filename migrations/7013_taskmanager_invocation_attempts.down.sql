DROP INDEX IF EXISTS production_manager_inputs_retry_idx;

ALTER TABLE production_manager_inputs
  DROP COLUMN IF EXISTS dispatch_attempt;
