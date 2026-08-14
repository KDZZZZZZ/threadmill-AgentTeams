DROP TABLE IF EXISTS merge_audits;
DROP INDEX IF EXISTS merge_repository_claims_expiry_idx;
ALTER TABLE merge_repository_claims
	DROP COLUMN IF EXISTS lease_expires_at,
	DROP COLUMN IF EXISTS claim_token,
	DROP COLUMN IF EXISTS lease_owner;
