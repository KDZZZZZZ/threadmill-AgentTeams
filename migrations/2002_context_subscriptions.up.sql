CREATE TABLE context_subscriptions (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL,
  consumer_invocation_id TEXT NOT NULL,
  subgraph_ids JSONB NOT NULL CHECK (jsonb_typeof(subgraph_ids) = 'array'),
  event_kinds JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(event_kinds) = 'array'),
  permission_snapshot TEXT NOT NULL,
  source TEXT NOT NULL CHECK (source IN ('initial_slice', 'search', 'explicit')),
  active BOOLEAN NOT NULL DEFAULT true,
  canceled_at TIMESTAMPTZ,
  expired_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX context_subscriptions_invocation_active_idx
  ON context_subscriptions (project_id, consumer_invocation_id, active, id);

CREATE UNIQUE INDEX context_subscriptions_project_id_idx
  ON context_subscriptions (project_id, id);

CREATE TABLE context_delta_deliveries (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL,
  subscription_id TEXT NOT NULL,
  consumer_invocation_id TEXT NOT NULL,
  event_id TEXT NOT NULL,
  event_kind TEXT NOT NULL,
  subgraph_ids JSONB NOT NULL CHECK (jsonb_typeof(subgraph_ids) = 'array'),
  graph_revision BIGINT NOT NULL CHECK (graph_revision > 0),
  acked_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (subscription_id, event_id),
  FOREIGN KEY (project_id, subscription_id) REFERENCES context_subscriptions(project_id, id) ON DELETE CASCADE
);

CREATE INDEX context_delta_deliveries_pending_idx
  ON context_delta_deliveries (project_id, consumer_invocation_id, acked_at, id);
