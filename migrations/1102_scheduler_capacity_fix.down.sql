ALTER TABLE scheduler_capacity_ledger
    DROP CONSTRAINT IF EXISTS scheduler_capacity_active_within_healthy;

-- Restore the legacy 1101 invariant without making rollback fail for rows
-- that legitimately used desired > healthy while 1102 was active.
UPDATE scheduler_capacity_ledger
SET healthy_capacity = desired_concurrency,
    updated_at = now()
WHERE desired_concurrency > healthy_capacity;

ALTER TABLE scheduler_capacity_ledger
    ADD CONSTRAINT scheduler_capacity_desired_within_healthy
    CHECK (desired_concurrency <= healthy_capacity);
