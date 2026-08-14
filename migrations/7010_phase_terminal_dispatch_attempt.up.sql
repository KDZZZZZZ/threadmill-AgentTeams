ALTER TABLE production_phase_terminal_obligations
  ADD COLUMN IF NOT EXISTS dispatch_request_id TEXT;

UPDATE production_phase_terminal_obligations
SET dispatch_request_id = request_id
WHERE dispatch_request_id IS NULL OR dispatch_request_id = '';

ALTER TABLE production_phase_terminal_obligations
  ALTER COLUMN dispatch_request_id SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS production_phase_terminal_dispatch_request_idx
  ON production_phase_terminal_obligations(project_id, input_kind, dispatch_request_id);
