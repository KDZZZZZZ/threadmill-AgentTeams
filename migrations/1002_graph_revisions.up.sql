CREATE TABLE IF NOT EXISTS coordination_graph_revisions (
    project_id TEXT NOT NULL,
    revision BIGINT NOT NULL CHECK (revision > 0),
    snapshot JSONB NOT NULL,
    decision_ref TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, revision)
);

CREATE TABLE IF NOT EXISTS coordination_decisions (
    project_id TEXT NOT NULL,
    decision_ref TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('replace_pending', 'transition')),
    payload_hash TEXT NOT NULL,
    transition_payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, decision_ref)
);

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
    observed_event_ref TEXT,
    completed_event_ref TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, command_id),
    FOREIGN KEY (project_id, task_id, endpoint_id)
        REFERENCES coordination_endpoints(project_id, task_id, endpoint_id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS coordination_phase_leases (
    project_id TEXT NOT NULL,
    lease_ref TEXT NOT NULL,
    task_id TEXT NOT NULL,
    endpoint_id TEXT NOT NULL CHECK (endpoint_id IN ('plan', 'execute', 'verify')),
    generation INTEGER NOT NULL CHECK (generation > 0),
    binding_ref TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('active', 'released')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    released_at TIMESTAMPTZ,
    PRIMARY KEY (project_id, lease_ref),
    FOREIGN KEY (project_id, task_id, endpoint_id)
        REFERENCES coordination_endpoints(project_id, task_id, endpoint_id)
        ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS coordination_one_active_phase_lease
    ON coordination_phase_leases(project_id, task_id, endpoint_id, generation)
    WHERE state = 'active';

CREATE UNIQUE INDEX IF NOT EXISTS coordination_one_run_command_per_generation
    ON coordination_phase_commands(project_id, task_id, endpoint_id, generation)
    WHERE action IN ('start', 'resume');
