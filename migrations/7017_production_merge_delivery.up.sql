CREATE TABLE IF NOT EXISTS production_merge_deliveries (
    project_id TEXT NOT NULL,
    candidate_id TEXT NOT NULL PRIMARY KEY
        REFERENCES merge_candidates(id) ON DELETE CASCADE,
    task_id TEXT NOT NULL,
    verify_result_ref TEXT NOT NULL
        REFERENCES evidence_artifacts(id) ON DELETE RESTRICT,
    completion_payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('queued', 'delivered', 'failed')),
    manager_input_ref TEXT,
    dispatch_attempt INTEGER NOT NULL DEFAULT 0 CHECK (dispatch_attempt >= 0),
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    FOREIGN KEY (project_id, manager_input_ref)
        REFERENCES production_manager_inputs(project_id, input_ref)
);

CREATE INDEX IF NOT EXISTS production_merge_deliveries_ready_idx
    ON production_merge_deliveries(project_id, status, updated_at, candidate_id);

CREATE INDEX IF NOT EXISTS production_merge_deliveries_task_idx
    ON production_merge_deliveries(project_id, task_id, verify_result_ref);
