WITH keepers AS (
  SELECT DISTINCT ON (project_id, consumer_invocation_id)
    project_id,
    consumer_invocation_id,
    id AS keep_id
  FROM context_subscriptions
  WHERE source = 'initial_slice'
    AND active
  ORDER BY project_id, consumer_invocation_id, created_at, id
),
duplicate_groups AS (
  SELECT project_id, consumer_invocation_id
  FROM context_subscriptions
  WHERE source = 'initial_slice'
    AND active
  GROUP BY project_id, consumer_invocation_id
  HAVING count(*) > 1
),
merged_payloads AS (
  SELECT
    k.project_id,
    k.consumer_invocation_id,
    k.keep_id,
    COALESCE((
      SELECT jsonb_agg(value ORDER BY value)
      FROM (
        SELECT DISTINCT elem.value
        FROM context_subscriptions s
        CROSS JOIN LATERAL jsonb_array_elements_text(s.subgraph_ids) AS elem(value)
        WHERE s.project_id = k.project_id
          AND s.consumer_invocation_id = k.consumer_invocation_id
          AND s.source = 'initial_slice'
          AND s.active
      ) deduped_subgraphs
    ), '[]'::jsonb) AS merged_subgraph_ids,
    COALESCE((
      SELECT jsonb_agg(value ORDER BY value)
      FROM (
        SELECT DISTINCT elem.value
        FROM context_subscriptions s
        CROSS JOIN LATERAL jsonb_array_elements_text(s.event_kinds) AS elem(value)
        WHERE s.project_id = k.project_id
          AND s.consumer_invocation_id = k.consumer_invocation_id
          AND s.source = 'initial_slice'
          AND s.active
      ) deduped_events
    ), '[]'::jsonb) AS merged_event_kinds
  FROM keepers k
  JOIN duplicate_groups d
    ON d.project_id = k.project_id
   AND d.consumer_invocation_id = k.consumer_invocation_id
)
UPDATE context_subscriptions keep
SET subgraph_ids = merged_payloads.merged_subgraph_ids,
    event_kinds = merged_payloads.merged_event_kinds
FROM merged_payloads
WHERE keep.project_id = merged_payloads.project_id
  AND keep.consumer_invocation_id = merged_payloads.consumer_invocation_id
  AND keep.id = merged_payloads.keep_id;

WITH keepers AS (
  SELECT DISTINCT ON (project_id, consumer_invocation_id)
    project_id,
    consumer_invocation_id,
    id AS keep_id
  FROM context_subscriptions
  WHERE source = 'initial_slice'
    AND active
  ORDER BY project_id, consumer_invocation_id, created_at, id
),
duplicate_groups AS (
  SELECT project_id, consumer_invocation_id
  FROM context_subscriptions
  WHERE source = 'initial_slice'
    AND active
  GROUP BY project_id, consumer_invocation_id
  HAVING count(*) > 1
)
UPDATE context_subscriptions duplicate
SET active = false,
    expired_at = COALESCE(expired_at, now())
FROM keepers k
JOIN duplicate_groups d
  ON d.project_id = k.project_id
 AND d.consumer_invocation_id = k.consumer_invocation_id
WHERE duplicate.project_id = k.project_id
  AND duplicate.consumer_invocation_id = k.consumer_invocation_id
  AND duplicate.source = 'initial_slice'
  AND duplicate.active
  AND duplicate.id <> k.keep_id;

CREATE UNIQUE INDEX IF NOT EXISTS context_subscriptions_active_initial_once_idx
  ON context_subscriptions (project_id, consumer_invocation_id)
  WHERE source = 'initial_slice' AND active;
