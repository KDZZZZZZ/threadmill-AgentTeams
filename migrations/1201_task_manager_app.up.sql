CREATE TABLE IF NOT EXISTS taskmanager_requirement_inputs (
    project_id TEXT NOT NULL,
    input_ref TEXT NOT NULL,
    task_id TEXT NOT NULL,
    contract_ref TEXT NOT NULL,
    requirement JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, input_ref)
);

CREATE TABLE IF NOT EXISTS taskmanager_contracts (
    project_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    input_ref TEXT NOT NULL,
    contract_ref TEXT NOT NULL,
    delivery_policy TEXT NOT NULL CHECK (delivery_policy IN ('non_code_artifact', 'code_merge', 'human_acceptance', 'external_delivery')),
    phase_specs JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, task_id),
    UNIQUE (project_id, contract_ref),
    FOREIGN KEY (project_id, input_ref)
        REFERENCES taskmanager_requirement_inputs(project_id, input_ref)
);

CREATE TABLE IF NOT EXISTS taskmanager_decisions (
    project_id TEXT NOT NULL,
    decision_ref TEXT NOT NULL,
    input_ref TEXT NOT NULL,
    expected_revision BIGINT NOT NULL CHECK (expected_revision > 0),
    kind TEXT NOT NULL CHECK (kind IN ('replace_pending', 'transition', 'terminal')),
    decision JSONB NOT NULL,
    transition_payload JSONB,
    payload_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, decision_ref)
);

CREATE INDEX IF NOT EXISTS taskmanager_decisions_input_ref
    ON taskmanager_decisions(project_id, input_ref, created_at);

CREATE TABLE IF NOT EXISTS taskmanager_replies (
    project_id TEXT NOT NULL,
    input_ref TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('accepted', 'rejected', 'conflict', 'deferred')),
    decision_ref TEXT NOT NULL,
    graph_revision BIGINT NOT NULL CHECK (graph_revision >= 0),
    reason TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, input_ref)
);
