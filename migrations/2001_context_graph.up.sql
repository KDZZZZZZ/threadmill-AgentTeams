CREATE TABLE context_subgraphs (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL,
  task_id TEXT,
  name TEXT NOT NULL,
  summary TEXT NOT NULL DEFAULT '',
  revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
  kind TEXT NOT NULL CHECK (kind IN ('general', 'task')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK ((kind = 'task' AND task_id IS NOT NULL) OR (kind = 'general' AND task_id IS NULL))
);

CREATE INDEX context_subgraphs_project_kind_idx
  ON context_subgraphs (project_id, kind, id);

CREATE TABLE context_nodes (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('directive', 'fact', 'hypothesis')),
  statement TEXT NOT NULL CHECK (statement <> ''),
  status TEXT NOT NULL CHECK (status IN ('accepted', 'disputed', 'superseded', 'outdated')),
  source_refs JSONB NOT NULL CHECK (jsonb_typeof(source_refs) = 'array' AND jsonb_array_length(source_refs) > 0),
  creator_agent_id TEXT NOT NULL,
  revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
  created_sequence BIGSERIAL NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX context_nodes_project_creator_seq_idx
  ON context_nodes (project_id, creator_agent_id, created_sequence DESC);

CREATE TABLE context_node_subgraph_memberships (
  node_id TEXT NOT NULL REFERENCES context_nodes(id) ON DELETE CASCADE,
  subgraph_id TEXT NOT NULL REFERENCES context_subgraphs(id) ON DELETE CASCADE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (node_id, subgraph_id)
);

CREATE INDEX context_node_subgraph_memberships_subgraph_idx
  ON context_node_subgraph_memberships (subgraph_id, node_id);

CREATE TABLE context_edges (
  from_ref TEXT NOT NULL,
  to_node_id TEXT NOT NULL REFERENCES context_nodes(id) ON DELETE CASCADE,
  kind TEXT NOT NULL CHECK (kind IN ('logical_adjacent', 'derives_from_subgraph')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (from_ref, to_node_id, kind)
);

CREATE INDEX context_edges_to_node_idx
  ON context_edges (to_node_id);

CREATE TABLE context_graph_revisions (
  project_id TEXT PRIMARY KEY,
  revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE context_audit_events (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL,
  actor_principal_id TEXT NOT NULL,
  action TEXT NOT NULL,
  subject_id TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX context_audit_events_project_created_idx
  ON context_audit_events (project_id, created_at, id);

CREATE TABLE context_outbox_events (
  id TEXT PRIMARY KEY,
  topic TEXT NOT NULL,
  key TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX context_outbox_events_topic_created_idx
  ON context_outbox_events (topic, created_at, id);
