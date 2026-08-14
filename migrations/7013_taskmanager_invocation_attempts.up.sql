ALTER TABLE production_manager_inputs
  ADD COLUMN dispatch_attempt INTEGER NOT NULL DEFAULT 1 CHECK (dispatch_attempt > 0);

CREATE INDEX production_manager_inputs_retry_idx
  ON production_manager_inputs(project_id, status, dispatch_attempt, updated_at, input_ref)
  WHERE status IN ('pending', 'failed');
