CREATE TABLE workspace_bindings (
	id text PRIMARY KEY,
	task_id text NOT NULL,
	generation integer NOT NULL,
	kind text NOT NULL,
	root text NOT NULL,
	branch_name text,
	container_id text,
	volume_refs text[] NOT NULL DEFAULT '{}',
	base_revision text NOT NULL,
	current_revision text NOT NULL,
	allowed_dirs text[] NOT NULL DEFAULT '{}',
	declared_writes jsonb NOT NULL DEFAULT '{}',
	observed_writes jsonb NOT NULL DEFAULT '{}',
	phase_leases jsonb NOT NULL DEFAULT '{}',
	active_phase text,
	active_invocation text,
	status text NOT NULL,
	created_at timestamptz NOT NULL DEFAULT now(),
	updated_at timestamptz NOT NULL DEFAULT now(),
	UNIQUE(task_id, generation)
);

CREATE INDEX workspace_bindings_task_idx ON workspace_bindings(task_id, generation);
