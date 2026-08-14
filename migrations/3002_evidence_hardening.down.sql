DROP INDEX IF EXISTS evidence_artifacts_project_task_idx;
DROP INDEX IF EXISTS evidence_events_project_task_sequence_idx;
DROP INDEX IF EXISTS evidence_events_project_sequence_idx;

DO $$
BEGIN
	IF EXISTS (
		SELECT 1
		FROM evidence_artifacts
		GROUP BY content_hash
		HAVING count(*) > 1
	) THEN
		RAISE EXCEPTION USING
			ERRCODE = '23505',
			MESSAGE = 'evidence hardening rollback blocked: duplicate content_hash values exist across artifact types',
			HINT = 'archive or delete the duplicate typed artifacts before retrying migration 3002 rollback';
	END IF;
END
$$;

ALTER TABLE evidence_artifacts
	DROP CONSTRAINT IF EXISTS evidence_artifacts_type_content_hash_key;

ALTER TABLE evidence_artifacts
	ADD CONSTRAINT evidence_artifacts_content_hash_key UNIQUE (content_hash);

ALTER TABLE evidence_artifacts
	DROP CONSTRAINT IF EXISTS evidence_artifacts_size_nonnegative,
	DROP CONSTRAINT IF EXISTS evidence_artifacts_content_hash_not_empty;

ALTER TABLE evidence_events
	DROP CONSTRAINT IF EXISTS evidence_events_type_not_empty,
	DROP CONSTRAINT IF EXISTS evidence_events_request_hash_not_empty;

ALTER TABLE evidence_artifacts
	DROP COLUMN IF EXISTS content_type;
