ALTER TABLE agent_invocation_tokens
  ADD COLUMN IF NOT EXISTS operation TEXT,
  ADD COLUMN IF NOT EXISTS consumer_invocation_id TEXT,
  ADD COLUMN IF NOT EXISTS consumer_task_id TEXT,
  ADD COLUMN IF NOT EXISTS consumer_role TEXT;

ALTER TABLE agent_invocation_tokens
  DROP CONSTRAINT IF EXISTS agent_invocation_tokens_operation_check,
  ADD CONSTRAINT agent_invocation_tokens_operation_check CHECK (
    (role = 'context_agent' AND operation IN ('retrieve', 'curate', 'review'))
    OR (role <> 'context_agent' AND operation IS NULL)
  ),
  DROP CONSTRAINT IF EXISTS agent_invocation_tokens_consumer_scope_check,
  ADD CONSTRAINT agent_invocation_tokens_consumer_scope_check CHECK (
    (consumer_invocation_id IS NULL AND consumer_task_id IS NULL AND consumer_role IS NULL)
    OR (role = 'context_agent' AND operation = 'retrieve' AND consumer_invocation_id IS NOT NULL)
  ),
  DROP CONSTRAINT IF EXISTS agent_invocation_tokens_consumer_role_check,
  ADD CONSTRAINT agent_invocation_tokens_consumer_role_check CHECK (
    consumer_role IS NULL
    OR consumer_role IN ('task_manager', 'context_agent', 'planner', 'executor', 'verifier')
  );
