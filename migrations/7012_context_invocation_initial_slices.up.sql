CREATE TABLE context_invocation_initial_slices (
  project_id TEXT NOT NULL,
  consumer_invocation_id TEXT NOT NULL,
  context_slice JSONB NOT NULL CHECK (jsonb_typeof(context_slice) = 'object'),
  graph_revision BIGINT NOT NULL CHECK (graph_revision > 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (project_id, consumer_invocation_id),
  CHECK ((context_slice ->> 'graph_revision')::BIGINT = graph_revision),
  CHECK (jsonb_typeof(context_slice -> 'nodes') = 'array'),
  CHECK (jsonb_typeof(context_slice -> 'subscription_ids') = 'array')
);

CREATE INDEX context_invocation_initial_slices_created_idx
  ON context_invocation_initial_slices (project_id, created_at, consumer_invocation_id);
