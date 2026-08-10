ALTER TABLE workspace_bindings
    ADD COLUMN binding_revision bigint NOT NULL DEFAULT 1,
    ADD COLUMN repository_path text NOT NULL DEFAULT '',
    ADD COLUMN creation_fingerprint text NOT NULL DEFAULT '';

ALTER TABLE workspace_bindings
    ADD CONSTRAINT workspace_bindings_revision_positive CHECK (binding_revision > 0),
    ADD CONSTRAINT workspace_bindings_generation_positive CHECK (generation > 0),
    ADD CONSTRAINT workspace_bindings_kind_supported CHECK (kind IN ('git_worktree', 'clone', 'container', 'remote')),
    ADD CONSTRAINT workspace_bindings_status_supported CHECK (status IN ('prepared', 'sealed')),
    ADD CONSTRAINT workspace_bindings_active_phase_supported CHECK (active_phase IS NULL OR active_phase IN ('plan', 'execute', 'verify')),
    ADD CONSTRAINT workspace_bindings_active_pair CHECK ((active_phase IS NULL) = (active_invocation IS NULL));

CREATE UNIQUE INDEX workspace_bindings_active_invocation_uidx
    ON workspace_bindings(active_invocation)
    WHERE active_invocation IS NOT NULL;
