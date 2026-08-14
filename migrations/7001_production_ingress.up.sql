CREATE TABLE IF NOT EXISTS production_manager_inputs (
    project_id TEXT NOT NULL,
    input_ref TEXT NOT NULL,
    request_id TEXT NOT NULL,
    input_kind TEXT NOT NULL CHECK (input_kind IN ('requirement', 'manager', 'human')),
    conversation_id TEXT NOT NULL,
    payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    observed_graph_revision BIGINT NOT NULL CHECK (observed_graph_revision > 0),
    selected_task_id TEXT,
    selected_endpoint_id TEXT,
    target_kind TEXT,
    target_ref TEXT,
    invocation_id TEXT NOT NULL UNIQUE REFERENCES runtime_invocations(invocation_id),
    status TEXT NOT NULL CHECK (status IN ('pending', 'dispatched', 'completed')),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (project_id, input_ref),
    UNIQUE (project_id, input_kind, request_id)
);

CREATE TABLE IF NOT EXISTS production_taskmanager_bindings (
    project_id TEXT NOT NULL,
    invocation_id TEXT NOT NULL PRIMARY KEY REFERENCES runtime_invocations(invocation_id),
    input_ref TEXT NOT NULL,
    room_id TEXT NOT NULL,
    spec TEXT NOT NULL,
    runtime_config_ref TEXT NOT NULL,
    envelope_ref TEXT NOT NULL,
    required_capabilities JSONB NOT NULL CHECK (jsonb_typeof(required_capabilities) = 'array'),
    decision_ref TEXT,
    decision_kind TEXT CHECK (decision_kind IS NULL OR decision_kind IN ('replace_pending', 'transition', 'terminal')),
    decision_action TEXT,
    mutation_applied BOOLEAN NOT NULL DEFAULT FALSE,
    applied_graph_revision BIGINT CHECK (applied_graph_revision IS NULL OR applied_graph_revision > 0),
    dispatched_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    FOREIGN KEY (project_id, input_ref)
        REFERENCES production_manager_inputs(project_id, input_ref),
    FOREIGN KEY (project_id, decision_ref)
        REFERENCES taskmanager_decisions(project_id, decision_ref)
);

CREATE TABLE IF NOT EXISTS production_conversation_entries (
    sequence BIGSERIAL PRIMARY KEY,
    project_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    entry_id TEXT NOT NULL,
    entry_kind TEXT NOT NULL CHECK (entry_kind IN ('requirement', 'manager', 'human', 'decision', 'runtime')),
    manager_input_ref TEXT,
    decision_ref TEXT,
    graph_revision BIGINT CHECK (graph_revision IS NULL OR graph_revision > 0),
    body TEXT,
    disposition TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    UNIQUE (project_id, conversation_id, entry_id)
);

CREATE INDEX IF NOT EXISTS production_manager_inputs_pending_idx
    ON production_manager_inputs(project_id, status, created_at);

CREATE INDEX IF NOT EXISTS production_conversation_cursor_idx
    ON production_conversation_entries(project_id, conversation_id, sequence);
