DROP INDEX IF EXISTS context_subscriptions_project_task_active_idx;

ALTER TABLE context_subscriptions
  DROP COLUMN IF EXISTS role,
  DROP COLUMN IF EXISTS task_id;

ALTER TABLE context_task_memory_candidates
  DROP COLUMN IF EXISTS creation_context;

DROP INDEX IF EXISTS context_outbox_events_project_topic_created_idx;

ALTER TABLE context_outbox_events
  DROP COLUMN IF EXISTS project_id;
