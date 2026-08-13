CREATE TABLE IF NOT EXISTS agentteams_execution_refs (
  invocation_ref TEXT PRIMARY KEY,
  invocation_id TEXT NOT NULL,
  agentteams_task_id TEXT NOT NULL UNIQUE,
  host_ref TEXT NOT NULL,
  dispatch_fingerprint TEXT NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('reserved', 'dispatched', 'terminated')),
  termination_mode TEXT CHECK (
    termination_mode IS NULL
    OR termination_mode IN ('release_wait', 'recoverable_stop', 'cancel')
  ),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (
    (state = 'terminated' AND termination_mode IS NOT NULL)
    OR (state <> 'terminated' AND termination_mode IS NULL)
  )
);

CREATE INDEX IF NOT EXISTS agentteams_execution_refs_invocation_idx
  ON agentteams_execution_refs (invocation_id, created_at);

CREATE INDEX IF NOT EXISTS agentteams_execution_refs_host_state_idx
  ON agentteams_execution_refs (host_ref, state, created_at);
