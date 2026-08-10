CREATE TABLE context_task_subgraph_bindings (
  project_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  subgraph_id TEXT NOT NULL UNIQUE REFERENCES context_subgraphs(id) ON DELETE RESTRICT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (project_id, task_id)
);

CREATE TABLE context_task_projections (
  project_id TEXT NOT NULL,
  projection_id TEXT NOT NULL,
  node_id TEXT NOT NULL UNIQUE REFERENCES context_nodes(id) ON DELETE CASCADE,
  source_revision TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (project_id, projection_id)
);

CREATE TABLE context_task_recipients (
  node_id TEXT NOT NULL REFERENCES context_nodes(id) ON DELETE CASCADE,
  project_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  endpoint_refs JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(endpoint_refs) = 'array'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (node_id, project_id, task_id),
  FOREIGN KEY (project_id, task_id) REFERENCES context_task_subgraph_bindings(project_id, task_id) ON DELETE RESTRICT
);

CREATE TABLE context_task_memory_candidates (
  project_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  candidate_id TEXT NOT NULL,
  candidate JSONB NOT NULL,
  created_by_invocation_id TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (project_id, task_id, candidate_id),
  FOREIGN KEY (project_id, task_id) REFERENCES context_task_subgraph_bindings(project_id, task_id) ON DELETE RESTRICT
);

CREATE INDEX context_task_memory_candidates_task_idx
  ON context_task_memory_candidates (project_id, task_id, created_at, candidate_id);

CREATE TABLE context_task_memory_reviews (
  project_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('open', 'frozen-unreviewed', 'reviewed')),
  receipt JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (project_id, task_id),
  FOREIGN KEY (project_id, task_id) REFERENCES context_task_subgraph_bindings(project_id, task_id) ON DELETE RESTRICT
);
