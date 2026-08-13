CREATE TABLE evidence_events (
	id text PRIMARY KEY,
	sequence bigserial UNIQUE NOT NULL,
	stable_key text UNIQUE NOT NULL,
	request_hash text NOT NULL,
	type text NOT NULL,
	project_id text,
	task_id text,
	workspace_ref text,
	phase_endpoint text,
	agent_invocation_id text,
	payload jsonb,
	artifact_refs text[] NOT NULL DEFAULT '{}',
	graph_revision bigint,
	created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE evidence_artifacts (
	id text PRIMARY KEY,
	type text NOT NULL,
	path_or_blob_ref text NOT NULL,
	content_hash text NOT NULL UNIQUE,
	size_bytes bigint NOT NULL,
	project_id text,
	task_id text,
	agent_invocation_id text,
	created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE evidence_artifact_grants (
	artifact_id text NOT NULL REFERENCES evidence_artifacts(id) ON DELETE CASCADE,
	project_id text NOT NULL,
	task_id text NOT NULL,
	created_at timestamptz NOT NULL DEFAULT now(),
	PRIMARY KEY(artifact_id, project_id, task_id)
);

CREATE INDEX evidence_events_task_sequence_idx ON evidence_events(task_id, sequence);
CREATE INDEX evidence_artifact_grants_scope_idx ON evidence_artifact_grants(project_id, task_id);
