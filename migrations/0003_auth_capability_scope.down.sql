ALTER TABLE agent_invocation_tokens
  DROP CONSTRAINT IF EXISTS agent_invocation_tokens_consumer_role_check,
  DROP CONSTRAINT IF EXISTS agent_invocation_tokens_consumer_scope_check,
  DROP CONSTRAINT IF EXISTS agent_invocation_tokens_operation_check,
  DROP COLUMN IF EXISTS consumer_role,
  DROP COLUMN IF EXISTS consumer_task_id,
  DROP COLUMN IF EXISTS consumer_invocation_id,
  DROP COLUMN IF EXISTS operation;
