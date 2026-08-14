CREATE TABLE IF NOT EXISTS operator_ui_task_grants (
  actor_principal_id TEXT NOT NULL,
  project_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  visible BOOLEAN NOT NULL DEFAULT FALSE,
  context_bodies BOOLEAN NOT NULL DEFAULT FALSE,
  candidate_bodies BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (actor_principal_id, project_id, task_id),
  CHECK (actor_principal_id <> ''),
  CHECK (project_id <> ''),
  CHECK (task_id <> ''),
  CHECK (NOT context_bodies OR visible),
  CHECK (NOT candidate_bodies OR visible)
);

CREATE INDEX IF NOT EXISTS operator_ui_task_grants_project_task_idx
  ON operator_ui_task_grants (project_id, task_id, actor_principal_id);
