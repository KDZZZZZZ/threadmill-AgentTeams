UPDATE coordination_phase_leases
SET expires_at = COALESCE(released_at, created_at, now())
WHERE expires_at IS NULL;

ALTER TABLE coordination_phase_leases
    ALTER COLUMN expires_at SET NOT NULL;
