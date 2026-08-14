CREATE TABLE IF NOT EXISTS phase_agentteams_prepared_invocations (
  invocation_ref TEXT PRIMARY KEY,
  invocation_id TEXT NOT NULL,
  project_id TEXT NOT NULL,
  role TEXT NOT NULL,
  operation TEXT NOT NULL DEFAULT '',
  room_id TEXT NOT NULL,
  spec TEXT NOT NULL,
  runtime_config_ref TEXT NOT NULL,
  envelope_ref TEXT NOT NULL,
  required_capabilities JSONB NOT NULL DEFAULT '[]'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS phase_agentteams_prepared_invocations_invocation_idx
  ON phase_agentteams_prepared_invocations (invocation_id, created_at);

CREATE TABLE IF NOT EXISTS phase_agentteams_host_states (
  invocation_id TEXT PRIMARY KEY,
  invocation_ref TEXT NOT NULL,
  agentteams_task_id TEXT NOT NULL,
  host_ref TEXT NOT NULL,
  termination_mode TEXT CHECK (
    termination_mode IS NULL
    OR termination_mode IN ('release_wait', 'recoverable_stop', 'cancel')
  ),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS phase_agentteams_host_states_execution_idx
  ON phase_agentteams_host_states (agentteams_task_id, host_ref);
