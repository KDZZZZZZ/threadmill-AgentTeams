CREATE TABLE IF NOT EXISTS agentteams_host_fences (
  host_ref TEXT PRIMARY KEY,
  state TEXT NOT NULL CHECK (state IN ('fencing', 'complete', 'cleared')),
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  completed_at TIMESTAMPTZ,
  cleared_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (
    (state = 'cleared' AND cleared_at IS NOT NULL)
    OR (state <> 'cleared' AND cleared_at IS NULL)
  )
);

CREATE INDEX IF NOT EXISTS agentteams_host_fences_active_idx
  ON agentteams_host_fences (host_ref, state)
  WHERE cleared_at IS NULL;
