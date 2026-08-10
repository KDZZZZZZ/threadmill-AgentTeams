DROP INDEX IF EXISTS agentteams_execution_refs_active_slot_counts_idx;
DROP INDEX IF EXISTS agentteams_execution_refs_active_host_slot_idx;

ALTER TABLE agentteams_execution_refs
  DROP COLUMN IF EXISTS mcp_revoked_at,
  DROP COLUMN IF EXISTS mcp_token_identifier,
  DROP COLUMN IF EXISTS mcp_token_hash,
  DROP COLUMN IF EXISTS mcp_client_key,
  DROP COLUMN IF EXISTS host_slot_released_at,
  DROP COLUMN IF EXISTS host_slot_claimed_at;
