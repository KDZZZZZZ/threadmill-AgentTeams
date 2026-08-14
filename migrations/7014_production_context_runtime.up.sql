CREATE TABLE production_context_invocations (
  project_id TEXT NOT NULL,
  invocation_id TEXT PRIMARY KEY REFERENCES runtime_invocations(invocation_id) ON DELETE RESTRICT,
  operation TEXT NOT NULL CHECK (operation IN ('retrieve', 'review')),
  request_key TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  task_id TEXT,
  consumer_invocation_id TEXT,
  room_id TEXT NOT NULL,
  spec TEXT NOT NULL,
  runtime_config_ref TEXT NOT NULL,
  envelope_ref TEXT NOT NULL,
  required_capabilities JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(required_capabilities) = 'array'),
  agentteams_task_id TEXT,
  host_ref TEXT,
  state TEXT NOT NULL CHECK (state IN ('prepared', 'dispatched', 'result_captured', 'completed', 'failed')),
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  UNIQUE (project_id, operation, request_key)
);

CREATE INDEX production_context_invocations_reconcile_idx
  ON production_context_invocations(project_id, state, updated_at, invocation_id)
  WHERE state IN ('prepared', 'dispatched', 'result_captured');

CREATE TABLE production_context_retrieve_results (
  project_id TEXT NOT NULL,
  invocation_id TEXT PRIMARY KEY REFERENCES production_context_invocations(invocation_id) ON DELETE CASCADE,
  search_request JSONB NOT NULL,
  result JSONB NOT NULL,
  result_hash TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);
