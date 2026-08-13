ALTER TABLE agentteams_execution_refs
  ADD COLUMN IF NOT EXISTS host_slot_claimed_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS host_slot_released_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS mcp_client_key TEXT,
  ADD COLUMN IF NOT EXISTS mcp_token_hash BYTEA,
  ADD COLUMN IF NOT EXISTS mcp_token_identifier TEXT,
  ADD COLUMN IF NOT EXISTS mcp_revoked_at TIMESTAMPTZ;

CREATE UNIQUE INDEX IF NOT EXISTS agentteams_execution_refs_active_host_slot_idx
  ON agentteams_execution_refs (host_ref)
  WHERE host_slot_claimed_at IS NOT NULL
    AND host_slot_released_at IS NULL
    AND state IN ('reserved', 'dispatched');

CREATE INDEX IF NOT EXISTS agentteams_execution_refs_active_slot_counts_idx
  ON agentteams_execution_refs (host_ref, state, host_slot_claimed_at)
  WHERE host_slot_claimed_at IS NOT NULL
    AND host_slot_released_at IS NULL
    AND state IN ('reserved', 'dispatched');
