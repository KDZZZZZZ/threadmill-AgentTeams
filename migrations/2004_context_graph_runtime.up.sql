ALTER TABLE context_subscriptions
  ADD COLUMN IF NOT EXISTS task_id TEXT,
  ADD COLUMN IF NOT EXISTS role TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS context_subscriptions_project_task_active_idx
  ON context_subscriptions (project_id, task_id, active, id);

ALTER TABLE context_outbox_events
  ADD COLUMN IF NOT EXISTS project_id TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS context_outbox_events_project_topic_created_idx
  ON context_outbox_events (project_id, topic, created_at, id);

ALTER TABLE context_task_memory_candidates
  ADD COLUMN IF NOT EXISTS creation_context JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE context_task_memory_candidates
  ADD COLUMN IF NOT EXISTS creation_context JSONB NOT NULL DEFAULT '{}'::jsonb;
