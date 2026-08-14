ALTER TABLE phase_recovery_obligations
  ADD COLUMN IF NOT EXISTS output_command_id TEXT,
  ADD COLUMN IF NOT EXISTS output_receipt JSONB;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'phase_recovery_obligations'::regclass
      AND conname = 'phase_recovery_obligations_output_pair_check'
  ) THEN
    ALTER TABLE phase_recovery_obligations
      ADD CONSTRAINT phase_recovery_obligations_output_pair_check CHECK (
        (output_command_id IS NULL AND output_receipt IS NULL)
        OR (output_command_id IS NOT NULL AND output_receipt IS NOT NULL)
      );
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'phase_recovery_obligations'::regclass
      AND conname = 'phase_recovery_obligations_output_receipt_object_check'
  ) THEN
    ALTER TABLE phase_recovery_obligations
      ADD CONSTRAINT phase_recovery_obligations_output_receipt_object_check CHECK (
        output_receipt IS NULL OR jsonb_typeof(output_receipt) = 'object'
      );
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'phase_recovery_obligations'::regclass
      AND conname = 'phase_recovery_obligations_terminal_mutex_check'
  ) THEN
    ALTER TABLE phase_recovery_obligations
      ADD CONSTRAINT phase_recovery_obligations_terminal_mutex_check CHECK (
        NOT (
          stop_command_id IS NOT NULL
          AND stop_result IS NOT NULL
          AND output_command_id IS NOT NULL
          AND output_receipt IS NOT NULL
        )
      );
  END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS phase_recovery_obligations_output_command_idx
  ON phase_recovery_obligations (output_command_id)
  WHERE output_command_id IS NOT NULL;
