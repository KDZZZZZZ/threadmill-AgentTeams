ALTER TABLE agentteams_execution_refs
  ADD COLUMN IF NOT EXISTS attempt INTEGER;

UPDATE agentteams_execution_refs
SET attempt = 1
WHERE attempt IS NULL;

ALTER TABLE agentteams_execution_refs
  ALTER COLUMN attempt SET NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'agentteams_execution_refs'::regclass
      AND conname = 'agentteams_execution_refs_attempt_check'
  ) THEN
    ALTER TABLE agentteams_execution_refs
      ADD CONSTRAINT agentteams_execution_refs_attempt_check CHECK (attempt > 0);
  END IF;
END $$;

DO $$
DECLARE
  current_pk_columns TEXT[];
BEGIN
  SELECT array_agg(att.attname ORDER BY keys.ordinality)
  INTO current_pk_columns
  FROM pg_constraint con
  CROSS JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS keys(attnum, ordinality)
  JOIN pg_attribute att
    ON att.attrelid = con.conrelid
   AND att.attnum = keys.attnum
  WHERE con.conrelid = 'agentteams_execution_refs'::regclass
    AND con.contype = 'p'
  GROUP BY con.oid;

  IF current_pk_columns = ARRAY['invocation_ref'] THEN
    ALTER TABLE agentteams_execution_refs
      DROP CONSTRAINT agentteams_execution_refs_pkey;
  ELSIF current_pk_columns = ARRAY['invocation_ref', 'attempt'] THEN
    RETURN;
  ELSIF current_pk_columns IS NOT NULL THEN
    RAISE EXCEPTION 'agentteams_execution_refs has unexpected primary key columns: %', current_pk_columns;
  END IF;

  ALTER TABLE agentteams_execution_refs
    ADD CONSTRAINT agentteams_execution_refs_pkey PRIMARY KEY (invocation_ref, attempt);
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS agentteams_execution_refs_active_invocation_idx
  ON agentteams_execution_refs (invocation_ref)
  WHERE state IN ('reserved', 'dispatched');

DROP INDEX IF EXISTS agentteams_execution_refs_invocation_idx;

CREATE INDEX agentteams_execution_refs_invocation_idx
  ON agentteams_execution_refs (invocation_id, attempt, created_at);
