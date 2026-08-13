DO $$
DECLARE
    constraint_to_drop TEXT;
BEGIN
    SELECT constraint_name.conname
    INTO constraint_to_drop
    FROM pg_constraint AS constraint_name
    WHERE constraint_name.conrelid = 'scheduler_capacity_ledger'::regclass
      AND constraint_name.contype = 'c'
      AND pg_get_constraintdef(constraint_name.oid) LIKE '%desired_concurrency <= healthy_capacity%'
    LIMIT 1;

    IF constraint_to_drop IS NOT NULL THEN
        EXECUTE format(
            'ALTER TABLE scheduler_capacity_ledger DROP CONSTRAINT %I',
            constraint_to_drop
        );
    END IF;
END $$;

-- Repair any legacy inconsistent observation before enforcing the actual
-- capacity invariant. Desired capacity intentionally remains independent.
UPDATE scheduler_capacity_ledger
SET healthy_capacity = active_invocations,
    updated_at = now()
WHERE active_invocations > healthy_capacity;

ALTER TABLE scheduler_capacity_ledger
    ADD CONSTRAINT scheduler_capacity_active_within_healthy
    CHECK (active_invocations <= healthy_capacity);
