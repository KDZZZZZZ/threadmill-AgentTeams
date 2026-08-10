CREATE TABLE merge_operations (
	id text PRIMARY KEY,
	operation_token text NOT NULL UNIQUE,
	candidate_id text NOT NULL REFERENCES merge_candidates(id) ON DELETE CASCADE,
	target_repository text NOT NULL,
	target_branch text NOT NULL,
	expected_main_revision text NOT NULL,
	expected_merged_revision text NOT NULL,
	evidence_refs text[] NOT NULL DEFAULT '{}',
	audit_stable_key text NOT NULL,
	audit_type text NOT NULL,
	audit_project_id text NOT NULL,
	audit_task_id text NOT NULL,
	audit_workspace_ref text NOT NULL,
	audit_payload jsonb NOT NULL DEFAULT '{}',
	audit_artifact_refs text[] NOT NULL DEFAULT '{}',
	status text NOT NULL CHECK (status IN ('pending', 'finalized', 'recovery_required', 'aborted')),
	recovery_reason text,
	created_at timestamptz NOT NULL DEFAULT now(),
	updated_at timestamptz NOT NULL DEFAULT now(),
	finalized_at timestamptz,
	CHECK ((status = 'recovery_required') = (recovery_reason IS NOT NULL))
);

CREATE UNIQUE INDEX merge_operations_active_repository_idx
	ON merge_operations(target_repository)
	WHERE status IN ('pending', 'recovery_required');

CREATE UNIQUE INDEX merge_operations_active_candidate_idx
	ON merge_operations(candidate_id)
	WHERE status IN ('pending', 'recovery_required');
