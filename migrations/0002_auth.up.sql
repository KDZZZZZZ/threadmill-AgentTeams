CREATE TABLE IF NOT EXISTS operator_sessions (
  session_hash BYTEA PRIMARY KEY,
  actor_principal_id TEXT NOT NULL,
  project_ids JSONB NOT NULL,
  csrf_hash BYTEA NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  revoked_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (jsonb_typeof(project_ids) = 'array')
);

CREATE INDEX IF NOT EXISTS operator_sessions_actor_idx
  ON operator_sessions (actor_principal_id);

CREATE TABLE IF NOT EXISTS agent_invocation_tokens (
  token_hash BYTEA PRIMARY KEY,
  actor_principal_id TEXT NOT NULL,
  project_id TEXT NOT NULL,
  task_id TEXT,
  invocation_id TEXT NOT NULL,
  role TEXT NOT NULL,
  tools JSONB NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  revoked_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (jsonb_typeof(tools) = 'array'),
  CHECK (role IN ('task_manager', 'context_agent', 'planner', 'executor', 'verifier')),
  CHECK (role NOT IN ('planner', 'executor', 'verifier') OR task_id IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS agent_invocation_tokens_invocation_idx
  ON agent_invocation_tokens (invocation_id);

CREATE INDEX IF NOT EXISTS agent_invocation_tokens_project_task_idx
  ON agent_invocation_tokens (project_id, task_id);
