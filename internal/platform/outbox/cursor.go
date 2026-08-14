package outbox

import (
	"context"
	"database/sql"
	"fmt"
)

type PostgresCursorStore struct {
	db *sql.DB
}

func NewPostgresCursorStore(db *sql.DB) *PostgresCursorStore {
	return &PostgresCursorStore{db: db}
}

func (s *PostgresCursorStore) Consumed(ctx context.Context, consumer, eventID string) (bool, error) {
	var consumed bool
	err := s.db.QueryRowContext(ctx, `
SELECT EXISTS (
	SELECT 1 FROM platform_outbox_consumer_events
	WHERE consumer = $1 AND event_id = $2
)`, consumer, eventID).Scan(&consumed)
	if err != nil {
		return false, fmt.Errorf("check consumer cursor: %w", err)
	}
	return consumed, nil
}

func (s *PostgresCursorStore) MarkConsumed(ctx context.Context, consumer, eventID string) error {
	_, err := s.db.ExecContext(ctx, `
INSERT INTO platform_outbox_consumer_events (consumer, event_id)
VALUES ($1, $2)
ON CONFLICT (consumer, event_id) DO NOTHING`, consumer, eventID)
	if err != nil {
		return fmt.Errorf("mark consumer event consumed: %w", err)
	}
	_, err = s.db.ExecContext(ctx, `
INSERT INTO platform_outbox_consumer_cursors (consumer, last_event_id)
VALUES ($1, $2)
ON CONFLICT (consumer) DO UPDATE SET last_event_id = EXCLUDED.last_event_id, updated_at = now()
WHERE platform_outbox_consumer_cursors.last_event_id < EXCLUDED.last_event_id`, consumer, eventID)
	if err != nil {
		return fmt.Errorf("advance consumer cursor: %w", err)
	}
	return nil
}

func (s *PostgresCursorStore) ConsumeTx(ctx context.Context, consumer string, event Event, apply func(context.Context, *sql.Tx, Event) error) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin consumer transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	var insertedEventID string
	err = tx.QueryRowContext(ctx, `
INSERT INTO platform_outbox_consumer_events (consumer, event_id)
VALUES ($1, $2)
ON CONFLICT (consumer, event_id) DO NOTHING
RETURNING event_id`, consumer, event.ID).Scan(&insertedEventID)
	if err == sql.ErrNoRows {
		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("commit skipped consumer transaction: %w", err)
		}
		committed = true
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("claim consumer event: %w", err)
	}

	if err := apply(ctx, tx, event); err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO platform_outbox_consumer_cursors (consumer, last_event_id)
VALUES ($1, $2)
ON CONFLICT (consumer) DO UPDATE SET last_event_id = EXCLUDED.last_event_id, updated_at = now()
WHERE platform_outbox_consumer_cursors.last_event_id < EXCLUDED.last_event_id`, consumer, event.ID); err != nil {
		return false, fmt.Errorf("advance consumer cursor: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit consumer transaction: %w", err)
	}
	committed = true
	return true, nil
}
