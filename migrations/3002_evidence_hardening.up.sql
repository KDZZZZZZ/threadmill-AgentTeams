ALTER TABLE evidence_artifacts
	ADD COLUMN IF NOT EXISTS content_type text NOT NULL DEFAULT 'application/octet-stream';

ALTER TABLE evidence_events
	ADD CONSTRAINT evidence_events_request_hash_not_empty CHECK (request_hash <> '') NOT VALID,
	ADD CONSTRAINT evidence_events_type_not_empty CHECK (type <> '') NOT VALID;

ALTER TABLE evidence_artifacts
	ADD CONSTRAINT evidence_artifacts_content_hash_not_empty CHECK (content_hash <> '') NOT VALID,
	ADD CONSTRAINT evidence_artifacts_size_nonnegative CHECK (size_bytes >= 0) NOT VALID;

ALTER TABLE evidence_artifacts
	DROP CONSTRAINT IF EXISTS evidence_artifacts_content_hash_key;

ALTER TABLE evidence_artifacts
	ADD CONSTRAINT evidence_artifacts_type_content_hash_key UNIQUE (type, content_hash);

CREATE INDEX IF NOT EXISTS evidence_events_project_sequence_idx ON evidence_events(project_id, sequence);
CREATE INDEX IF NOT EXISTS evidence_events_project_task_sequence_idx ON evidence_events(project_id, task_id, sequence);
CREATE INDEX IF NOT EXISTS evidence_artifacts_project_task_idx ON evidence_artifacts(project_id, task_id);
