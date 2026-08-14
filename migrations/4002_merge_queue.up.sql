CREATE TABLE merge_candidates (
	id text PRIMARY KEY,
	project_id text NOT NULL,
	task_id text NOT NULL,
	workspace_ref text NOT NULL REFERENCES workspace_bindings(id),
	verify_result_ref text NOT NULL REFERENCES evidence_artifacts(id) ON DELETE RESTRICT,
	diff_artifact_ref text NOT NULL REFERENCES evidence_artifacts(id) ON DELETE RESTRICT,
	target_repository text NOT NULL,
	target_branch text NOT NULL DEFAULT 'main',
	base_revision text NOT NULL,
	main_revision text NOT NULL,
	candidate_revision text NOT NULL,
	status text NOT NULL CHECK (status IN ('queued', 'merge_check', 'targeted_verify', 'merged', 'failed')),
	failure_reason text CHECK (failure_reason IN ('conflict', 'permission', 'main_drift', 'verify_failed')),
	failure_evidence_ref text REFERENCES evidence_artifacts(id) ON DELETE RESTRICT,
	merged_revision text,
	created_at timestamptz NOT NULL DEFAULT now(),
	updated_at timestamptz NOT NULL DEFAULT now(),
	CHECK ((status = 'failed') = (failure_reason IS NOT NULL AND failure_evidence_ref IS NOT NULL)),
	CHECK ((status = 'merged') = (merged_revision IS NOT NULL))
);

CREATE TABLE merge_candidate_evidence_refs (
	candidate_id text NOT NULL REFERENCES merge_candidates(id) ON DELETE CASCADE,
	artifact_id text NOT NULL REFERENCES evidence_artifacts(id) ON DELETE RESTRICT,
	created_at timestamptz NOT NULL DEFAULT now(),
	PRIMARY KEY (candidate_id, artifact_id)
);

CREATE INDEX merge_candidates_repository_queue_idx
	ON merge_candidates(target_repository, status, created_at, id);

-- A repository claim is held only while a reconciler performs mechanical
-- check, targeted verify, and the final serial main write.
CREATE TABLE merge_repository_claims (
	target_repository text PRIMARY KEY,
	candidate_id text NOT NULL UNIQUE REFERENCES merge_candidates(id) ON DELETE CASCADE,
	claimed_at timestamptz NOT NULL DEFAULT now()
);
