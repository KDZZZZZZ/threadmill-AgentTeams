CREATE TABLE IF NOT EXISTS schema_migrations (
	version text PRIMARY KEY,
	name text NOT NULL,
	applied_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS platform_outbox_events (
	id text PRIMARY KEY,
	topic text NOT NULL,
	event_key text NOT NULL DEFAULT '',
	payload jsonb NOT NULL,
	available_at timestamptz NOT NULL DEFAULT now(),
	created_at timestamptz NOT NULL DEFAULT now(),
	attempts integer NOT NULL DEFAULT 0,
	claimed_by text,
	claimed_until timestamptz,
	delivered_at timestamptz
);

CREATE INDEX IF NOT EXISTS platform_outbox_claim_idx
	ON platform_outbox_events (available_at, created_at, id)
	WHERE delivered_at IS NULL;

CREATE TABLE IF NOT EXISTS platform_outbox_consumer_cursors (
	consumer text PRIMARY KEY,
	last_event_id text NOT NULL DEFAULT '',
	updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS platform_outbox_consumer_events (
	consumer text NOT NULL,
	event_id text NOT NULL REFERENCES platform_outbox_events(id) ON DELETE CASCADE,
	consumed_at timestamptz NOT NULL DEFAULT now(),
	PRIMARY KEY (consumer, event_id)
);
