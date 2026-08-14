ALTER TABLE production_manager_inputs
  DROP CONSTRAINT IF EXISTS production_manager_inputs_status_check;

ALTER TABLE production_manager_inputs
  ADD CONSTRAINT production_manager_inputs_status_check CHECK (
    status IN ('pending', 'dispatched', 'completed', 'failed')
  );
