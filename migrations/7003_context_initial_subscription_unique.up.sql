CREATE UNIQUE INDEX IF NOT EXISTS context_subscriptions_active_initial_once_idx
  ON context_subscriptions (project_id, consumer_invocation_id)
  WHERE source = 'initial_slice' AND active;
