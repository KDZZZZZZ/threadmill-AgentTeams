ALTER TABLE production_manager_inputs
  DROP CONSTRAINT IF EXISTS production_manager_inputs_input_kind_check;

ALTER TABLE production_manager_inputs
  ADD CONSTRAINT production_manager_inputs_input_kind_check CHECK (
    input_kind IN ('requirement', 'manager', 'human', 'phase_output', 'phase_stopped', 'phase_failed', 'phase_orchestration')
  );

ALTER TABLE production_conversation_entries
  DROP CONSTRAINT IF EXISTS production_conversation_entries_entry_kind_check;

ALTER TABLE production_conversation_entries
  ADD CONSTRAINT production_conversation_entries_entry_kind_check CHECK (
    entry_kind IN ('requirement', 'manager', 'human', 'decision', 'runtime', 'phase_output', 'phase_stopped', 'phase_failed', 'phase_orchestration')
  );
