DROP INDEX IF EXISTS workspace_bindings_active_invocation_uidx;

ALTER TABLE workspace_bindings
    DROP CONSTRAINT IF EXISTS workspace_bindings_active_pair,
    DROP CONSTRAINT IF EXISTS workspace_bindings_active_phase_supported,
    DROP CONSTRAINT IF EXISTS workspace_bindings_status_supported,
    DROP CONSTRAINT IF EXISTS workspace_bindings_kind_supported,
    DROP CONSTRAINT IF EXISTS workspace_bindings_generation_positive,
    DROP CONSTRAINT IF EXISTS workspace_bindings_revision_positive,
    DROP COLUMN IF EXISTS creation_fingerprint,
    DROP COLUMN IF EXISTS repository_path,
    DROP COLUMN IF EXISTS binding_revision;
