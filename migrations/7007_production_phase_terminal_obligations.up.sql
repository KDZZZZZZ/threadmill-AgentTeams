CREATE TABLE IF NOT EXISTS production_phase_terminal_obligations (
  obligation_id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL,
  invocation_id TEXT NOT NULL,
  command_id TEXT NOT NULL,
  command_action TEXT NOT NULL,
  task_id TEXT NOT NULL,
  endpoint_id TEXT NOT NULL,
  generation INTEGER NOT NULL CHECK (generation > 0),
  input_kind TEXT NOT NULL,
  request_id TEXT NOT NULL,
  conversation_id TEXT NOT NULL,
  body TEXT NOT NULL,
  payload JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
  payload_hash TEXT NOT NULL,
  identity_hash TEXT NOT NULL,
  intent_payload_hash TEXT NOT NULL DEFAULT '',
  intent_identity_hash TEXT NOT NULL DEFAULT '',
  seen_revision BIGINT NOT NULL CHECK (seen_revision > 0),
  target_kind TEXT NOT NULL,
  target_ref TEXT NOT NULL,
  manager_input_ref TEXT,
  status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('intent', 'pending', 'delivered', 'abandoned')),
  attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  last_error TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (project_id, input_kind, request_id)
);

CREATE INDEX IF NOT EXISTS production_phase_terminal_obligations_pending_idx
  ON production_phase_terminal_obligations(project_id, status, updated_at);
