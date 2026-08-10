ALTER TABLE merge_repository_claims
	ADD COLUMN IF NOT EXISTS lease_owner text NOT NULL DEFAULT 'legacy',
	ADD COLUMN IF NOT EXISTS lease_expires_at timestamptz NOT NULL DEFAULT now();

CREATE INDEX IF NOT EXISTS merge_repository_claims_expiry_idx
	ON merge_repository_claims(lease_expires_at);

CREATE TABLE merge_audits (
	stable_key text PRIMARY KEY,
	request_hash text NOT NULL,
	type text NOT NULL,
	project_id text NOT NULL,
	task_id text NOT NULL,
	workspace_ref text NOT NULL,
	payload jsonb NOT NULL DEFAULT '{}',
	artifact_refs text[] NOT NULL DEFAULT '{}',
	delivered boolean NOT NULL DEFAULT false,
	delivered_at timestamptz,
	created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX merge_audits_pending_idx
	ON merge_audits(delivered, created_at, stable_key)
	WHERE delivered = false;
