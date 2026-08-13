DROP INDEX IF EXISTS coordination_one_run_command_per_generation;
DROP INDEX IF EXISTS coordination_one_active_phase_lease;
DROP TABLE IF EXISTS coordination_binding_runtime;
DROP TABLE IF EXISTS coordination_runtime_observations;

ALTER TABLE coordination_phase_leases DROP COLUMN IF EXISTS expires_at;

ALTER TABLE coordination_phase_commands DROP COLUMN IF EXISTS not_executable;
ALTER TABLE coordination_phase_commands DROP COLUMN IF EXISTS quarantined;
ALTER TABLE coordination_phase_commands DROP COLUMN IF EXISTS retry_after;
ALTER TABLE coordination_phase_commands DROP COLUMN IF EXISTS accepted_at;

CREATE UNIQUE INDEX IF NOT EXISTS coordination_one_active_phase_lease
    ON coordination_phase_leases(project_id, task_id, endpoint_id, generation)
    WHERE state = 'active';

CREATE UNIQUE INDEX IF NOT EXISTS coordination_one_run_command_per_generation
    ON coordination_phase_commands(project_id, task_id, endpoint_id, generation)
    WHERE action IN ('start', 'resume');
