CREATE TABLE IF NOT EXISTS coordination_phase_commands (
    project_id TEXT NOT NULL,
    command_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    endpoint_id TEXT NOT NULL CHECK (endpoint_id IN ('plan', 'execute', 'verify')),
    generation INTEGER NOT NULL CHECK (generation > 0),
    binding_ref TEXT NOT NULL,
    lease_ref TEXT NOT NULL,
    action TEXT NOT NULL CHECK (action IN ('start', 'stop', 'resume')),
    cause_ref TEXT NOT NULL,
    accepted_at TIMESTAMPTZ,
    observed_event_ref TEXT,
    completed_event_ref TEXT,
    retry_after TIMESTAMPTZ,
    quarantined BOOLEAN NOT NULL DEFAULT false,
    not_executable BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, command_id),
    FOREIGN KEY (project_id, task_id, endpoint_id)
        REFERENCES coordination_endpoints(project_id, task_id, endpoint_id)
        ON DELETE CASCADE
);

ALTER TABLE coordination_phase_commands ADD COLUMN IF NOT EXISTS accepted_at TIMESTAMPTZ;
ALTER TABLE coordination_phase_commands ADD COLUMN IF NOT EXISTS retry_after TIMESTAMPTZ;
ALTER TABLE coordination_phase_commands ADD COLUMN IF NOT EXISTS quarantined BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE coordination_phase_commands ADD COLUMN IF NOT EXISTS not_executable BOOLEAN NOT NULL DEFAULT false;

CREATE TABLE IF NOT EXISTS coordination_phase_leases (
    project_id TEXT NOT NULL,
    lease_ref TEXT NOT NULL,
    task_id TEXT NOT NULL,
    endpoint_id TEXT NOT NULL CHECK (endpoint_id IN ('plan', 'execute', 'verify')),
    generation INTEGER NOT NULL CHECK (generation > 0),
    binding_ref TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('active', 'released')),
    expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    released_at TIMESTAMPTZ,
    PRIMARY KEY (project_id, lease_ref),
    FOREIGN KEY (project_id, task_id, endpoint_id)
        REFERENCES coordination_endpoints(project_id, task_id, endpoint_id)
        ON DELETE CASCADE
);

ALTER TABLE coordination_phase_leases ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS coordination_runtime_observations (
    project_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    command_id TEXT,
    lease_ref TEXT,
    task_id TEXT NOT NULL,
    endpoint_id TEXT NOT NULL CHECK (endpoint_id IN ('plan', 'execute', 'verify')),
    generation INTEGER NOT NULL CHECK (generation > 0),
	binding_ref TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('PhaseInvocationStarted', 'PhaseOutputSubmitted', 'PhaseInvocationFailed', 'PhaseInvocationStopped', 'DispatchRejected')),
    checkpoint_ref TEXT,
    non_resumable BOOLEAN NOT NULL DEFAULT false,
    folded BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, event_id),
	CHECK (
		(kind = 'DispatchRejected') OR
		(command_id IS NOT NULL AND lease_ref IS NOT NULL)
	),
	FOREIGN KEY (project_id, command_id)
		REFERENCES coordination_phase_commands(project_id, command_id),
	FOREIGN KEY (project_id, lease_ref)
		REFERENCES coordination_phase_leases(project_id, lease_ref)
);

CREATE TABLE IF NOT EXISTS coordination_binding_runtime (
    project_id TEXT NOT NULL,
    binding_ref TEXT NOT NULL,
    checkpoint_ref TEXT,
    non_resumable BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, binding_ref)
);

DROP INDEX IF EXISTS coordination_one_active_phase_lease;
CREATE UNIQUE INDEX coordination_one_active_phase_lease
    ON coordination_phase_leases(project_id, task_id, endpoint_id, generation)
    WHERE state = 'active';

DROP INDEX IF EXISTS coordination_one_run_command_per_generation;
CREATE UNIQUE INDEX coordination_one_run_command_per_generation
    ON coordination_phase_commands(project_id, task_id, endpoint_id, generation)
    WHERE action IN ('start', 'resume');
