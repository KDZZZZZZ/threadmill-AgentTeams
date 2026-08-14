UPDATE production_manager_inputs
SET status = 'completed'
WHERE status = 'failed';

ALTER TABLE production_manager_inputs
  DROP CONSTRAINT IF EXISTS production_manager_inputs_status_check;

ALTER TABLE production_manager_inputs
  ADD CONSTRAINT production_manager_inputs_status_check CHECK (
    status IN ('pending', 'dispatched', 'completed')
  );
