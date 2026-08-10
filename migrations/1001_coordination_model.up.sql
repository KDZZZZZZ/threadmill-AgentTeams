CREATE TABLE IF NOT EXISTS coordination_tasks (
    project_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    contract_ref TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome IN ('active', 'done', 'canceled', 'failed')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, task_id)
);

CREATE TABLE IF NOT EXISTS coordination_endpoints (
    project_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    endpoint_id TEXT NOT NULL CHECK (endpoint_id IN ('plan', 'execute', 'verify')),
    spec_ref TEXT NOT NULL,
    binding_ref TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation > 0),
    state TEXT NOT NULL CHECK (state IN ('pending', 'submitted', 'satisfied', 'rejected')),
    run_policy TEXT NOT NULL CHECK (run_policy IN ('enabled', 'held')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, task_id, endpoint_id),
    FOREIGN KEY (project_id, task_id)
        REFERENCES coordination_tasks(project_id, task_id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS coordination_edges (
    project_id TEXT NOT NULL,
    from_task_id TEXT NOT NULL,
    from_endpoint_id TEXT NOT NULL CHECK (from_endpoint_id IN ('plan', 'execute', 'verify')),
    to_task_id TEXT NOT NULL,
    to_endpoint_id TEXT NOT NULL CHECK (to_endpoint_id IN ('plan', 'execute', 'verify')),
    signal TEXT NOT NULL CHECK (signal IN ('phase_satisfied', 'task_done')),
    required_by TEXT NOT NULL CHECK (required_by IN ('start', 'completion')),
    artifact_kinds TEXT[] NOT NULL DEFAULT '{}',
    on_false TEXT NOT NULL CHECK (on_false IN ('block', 'replan', 'cancel')),
    PRIMARY KEY (project_id, from_task_id, from_endpoint_id, to_task_id, to_endpoint_id, signal, required_by),
    FOREIGN KEY (project_id, from_task_id, from_endpoint_id)
        REFERENCES coordination_endpoints(project_id, task_id, endpoint_id)
        ON DELETE CASCADE,
    FOREIGN KEY (project_id, to_task_id, to_endpoint_id)
        REFERENCES coordination_endpoints(project_id, task_id, endpoint_id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS coordination_blockers (
    project_id TEXT NOT NULL,
    blocker_id TEXT NOT NULL,
    target_task_id TEXT NOT NULL,
    target_endpoint_id TEXT NOT NULL CHECK (target_endpoint_id IN ('plan', 'execute', 'verify')),
    required_by TEXT NOT NULL CHECK (required_by IN ('start', 'completion')),
    on_false TEXT NOT NULL CHECK (on_false IN ('block', 'replan', 'cancel')),
    state TEXT NOT NULL CHECK (state IN ('active', 'resolved', 'denied', 'obsolete')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, blocker_id),
    FOREIGN KEY (project_id, target_task_id, target_endpoint_id)
        REFERENCES coordination_endpoints(project_id, task_id, endpoint_id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS coordination_phase_results (
    project_id TEXT NOT NULL,
    result_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    endpoint_id TEXT NOT NULL CHECK (endpoint_id IN ('plan', 'execute', 'verify')),
    binding_ref TEXT NOT NULL,
    output_ref TEXT NOT NULL,
    verdict TEXT NOT NULL CHECK (verdict IN ('submitted', 'satisfied', 'rejected', 'invalidated')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, result_id),
    FOREIGN KEY (project_id, task_id, endpoint_id)
        REFERENCES coordination_endpoints(project_id, task_id, endpoint_id)
        ON DELETE CASCADE
);
