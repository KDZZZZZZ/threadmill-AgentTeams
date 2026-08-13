ALTER TABLE runtime_invocations
  ADD COLUMN IF NOT EXISTS consumer_invocation_id TEXT,
  ADD COLUMN IF NOT EXISTS consumer_task_id TEXT,
  ADD COLUMN IF NOT EXISTS consumer_role TEXT,
  ADD COLUMN IF NOT EXISTS invocation_fingerprint TEXT;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'runtime_invocations'::regclass
      AND conname = 'runtime_invocations_consumer_role_check'
  ) THEN
    ALTER TABLE runtime_invocations
      ADD CONSTRAINT runtime_invocations_consumer_role_check CHECK (
        consumer_role IS NULL
        OR consumer_role IN ('task_manager', 'context_agent', 'planner', 'executor', 'verifier')
      );
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'runtime_invocations'::regclass
      AND conname = 'runtime_invocations_consumer_scope_check'
  ) THEN
    ALTER TABLE runtime_invocations
      ADD CONSTRAINT runtime_invocations_consumer_scope_check CHECK (
        (
          consumer_invocation_id IS NULL
          AND consumer_task_id IS NULL
          AND consumer_role IS NULL
        )
        OR (
          role = 'context_agent'
          AND operation = 'retrieve'
        )
      );
  END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS runtime_invocations_lease_unique_idx
  ON runtime_invocations (lease_id)
  WHERE lease_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS phase_recovery_obligations (
  run_command_id TEXT PRIMARY KEY,
  active BOOLEAN NOT NULL,
  stop_command_id TEXT,
  stop_result JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (
    (stop_command_id IS NULL AND stop_result IS NULL)
    OR (stop_command_id IS NOT NULL AND stop_result IS NOT NULL)
  ),
  CHECK (stop_result IS NULL OR jsonb_typeof(stop_result) = 'object')
);

CREATE INDEX IF NOT EXISTS phase_recovery_obligations_active_idx
  ON phase_recovery_obligations (active, run_command_id)
  WHERE active = TRUE;

CREATE UNIQUE INDEX IF NOT EXISTS phase_recovery_obligations_stop_command_idx
  ON phase_recovery_obligations (stop_command_id)
  WHERE stop_command_id IS NOT NULL;
