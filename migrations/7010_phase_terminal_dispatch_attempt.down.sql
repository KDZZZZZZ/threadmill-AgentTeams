DROP INDEX IF EXISTS production_phase_terminal_dispatch_request_idx;

ALTER TABLE production_phase_terminal_obligations
  DROP COLUMN IF EXISTS dispatch_request_id;
