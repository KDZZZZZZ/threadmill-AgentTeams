CREATE TABLE IF NOT EXISTS runtime_invocations (
  invocation_id TEXT PRIMARY KEY,
  actor_principal_id TEXT NOT NULL,
  project_id TEXT NOT NULL,
  task_id TEXT,
  endpoint_id TEXT,
  generation BIGINT,
  role TEXT NOT NULL,
  operation TEXT,
  status TEXT NOT NULL,
  binding_ref TEXT,
  lease_id TEXT,
  workspace_ref TEXT,
  context_slice_ref TEXT,
  task_memory_buffer_ref TEXT,
  prompt_hashes JSONB NOT NULL,
  skill_hashes JSONB NOT NULL,
  effective_tools JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  CHECK (role IN ('task_manager', 'context_agent', 'planner', 'executor', 'verifier')),
  CHECK (status IN ('prepared', 'running', 'waiting', 'stopped', 'completed', 'failed')),
  CHECK (jsonb_typeof(prompt_hashes) = 'object'),
  CHECK (jsonb_typeof(skill_hashes) = 'object'),
  CHECK (jsonb_typeof(effective_tools) = 'array'),
	CHECK (prompt_hashes <> '{}'::jsonb),
	CHECK (skill_hashes <> '{}'::jsonb),
	CHECK (jsonb_array_length(effective_tools) > 0),
	CHECK (expires_at > created_at),
  CHECK (role <> 'context_agent' OR (
    operation IS NOT NULL AND operation IN ('retrieve', 'curate', 'review')
  )),
  CHECK (role = 'context_agent' OR operation IS NULL),
  CHECK (role NOT IN ('planner', 'executor', 'verifier') OR (
    task_id IS NOT NULL AND endpoint_id IS NOT NULL AND generation > 0
    AND binding_ref IS NOT NULL AND lease_id IS NOT NULL
  ))
);

CREATE INDEX IF NOT EXISTS runtime_invocations_project_status_idx
  ON runtime_invocations (project_id, status, created_at);

CREATE INDEX IF NOT EXISTS runtime_invocations_task_endpoint_idx
  ON runtime_invocations (task_id, endpoint_id, generation);
