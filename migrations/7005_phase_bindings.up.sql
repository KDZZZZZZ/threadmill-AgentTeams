ALTER TABLE production_manager_inputs
  DROP CONSTRAINT IF EXISTS production_manager_inputs_input_kind_check;

ALTER TABLE production_manager_inputs
  ADD CONSTRAINT production_manager_inputs_input_kind_check CHECK (
    input_kind IN ('requirement', 'manager', 'human', 'phase_output', 'phase_stopped', 'phase_orchestration')
  );

ALTER TABLE production_conversation_entries
  DROP CONSTRAINT IF EXISTS production_conversation_entries_entry_kind_check;

ALTER TABLE production_conversation_entries
  ADD CONSTRAINT production_conversation_entries_entry_kind_check CHECK (
    entry_kind IN ('requirement', 'manager', 'human', 'decision', 'runtime', 'phase_output', 'phase_stopped', 'phase_orchestration')
  );

CREATE TABLE IF NOT EXISTS production_phase_bindings (
  project_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  endpoint_id TEXT NOT NULL,
  generation INTEGER NOT NULL CHECK (generation > 0),
  graph_binding_ref TEXT NOT NULL,
  workspace_ref TEXT NOT NULL,
  actor_principal_id TEXT NOT NULL,
  contract_ref TEXT NOT NULL,
  spec_ref TEXT NOT NULL,
  checkpoint_ref TEXT NOT NULL DEFAULT '',
  non_resumable BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (project_id, task_id, endpoint_id, generation, graph_binding_ref),
  UNIQUE (project_id, task_id, endpoint_id, generation)
);

CREATE INDEX IF NOT EXISTS production_phase_bindings_workspace_idx
  ON production_phase_bindings(project_id, workspace_ref);

CREATE TABLE IF NOT EXISTS production_phase_outputs (
  output_ref TEXT PRIMARY KEY,
  project_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  endpoint_id TEXT NOT NULL,
  generation INTEGER NOT NULL CHECK (generation > 0),
  binding_ref TEXT NOT NULL,
  lease_ref TEXT NOT NULL,
  invocation_id TEXT NOT NULL REFERENCES runtime_invocations(invocation_id),
  input_revision TEXT NOT NULL,
  output JSONB NOT NULL CHECK (jsonb_typeof(output) = 'object'),
  artifact_refs JSONB NOT NULL CHECK (jsonb_typeof(artifact_refs) = 'array'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS production_phase_outputs_endpoint_idx
  ON production_phase_outputs(project_id, task_id, endpoint_id, generation);
