DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM agentteams_execution_refs
    WHERE attempt <> 1
  ) THEN
    RAISE EXCEPTION 'cannot downgrade agentteams_execution_refs with non-baseline attempts';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM agentteams_execution_refs
    GROUP BY invocation_ref
    HAVING count(*) > 1
  ) THEN
    RAISE EXCEPTION 'cannot downgrade agentteams_execution_refs with multiple attempts per invocation_ref';
  END IF;
END $$;

DROP INDEX IF EXISTS agentteams_execution_refs_active_invocation_idx;

DROP INDEX IF EXISTS agentteams_execution_refs_invocation_idx;

ALTER TABLE agentteams_execution_refs
  DROP CONSTRAINT IF EXISTS agentteams_execution_refs_pkey;

ALTER TABLE agentteams_execution_refs
  DROP COLUMN IF EXISTS attempt;

ALTER TABLE agentteams_execution_refs
  ADD CONSTRAINT agentteams_execution_refs_pkey PRIMARY KEY (invocation_ref);

CREATE INDEX agentteams_execution_refs_invocation_idx
  ON agentteams_execution_refs (invocation_id, created_at);
