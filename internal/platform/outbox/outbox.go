package outbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"
)

type Event struct {
	ID          string
	Topic       string
	Key         string
	Payload     []byte
	AvailableAt time.Time
	Attempts    int
	CreatedAt   time.Time
}

type Store struct {
	db *sql.DB
}

func NewStore(db *sql.DB) *Store {
	return &Store{db: db}
}

func EnqueueTx(ctx context.Context, tx interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, event Event) error {
	if event.ID == "" {
		return errors.New("event id is required")
	}
	if event.Topic == "" {
		return errors.New("event topic is required")
	}
	if event.AvailableAt.IsZero() {
		event.AvailableAt = time.Now().UTC()
	}
	_, err := tx.ExecContext(ctx, `
INSERT INTO platform_outbox_events (id, topic, event_key, payload, available_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (id) DO NOTHING`,
		event.ID, event.Topic, event.Key, event.Payload, event.AvailableAt)
	if err != nil {
		return fmt.Errorf("enqueue outbox event: %w", err)
	}
	return nil
}

func (s *Store) Claim(ctx context.Context, consumer string, limit int, leaseFor time.Duration) ([]Event, error) {
	if limit <= 0 {
		return nil, errors.New("limit must be positive")
	}
	rows, err := s.db.QueryContext(ctx, `
WITH candidates AS (
	SELECT id
	FROM platform_outbox_events
	WHERE delivered_at IS NULL
	  AND available_at <= now()
	  AND (claimed_until IS NULL OR claimed_until < now())
	ORDER BY available_at, created_at, id
	LIMIT $1
	FOR UPDATE SKIP LOCKED
)
UPDATE platform_outbox_events e
SET claimed_by = $2,
    claimed_until = now() + ($3 * interval '1 second'),
    attempts = attempts + 1
FROM candidates
WHERE e.id = candidates.id
RETURNING e.id, e.topic, e.event_key, e.payload, e.available_at, e.attempts, e.created_at`,
		limit, consumer, int64(leaseFor.Seconds()))
	if err != nil {
		return nil, fmt.Errorf("claim outbox events: %w", err)
	}
	defer rows.Close()

	var events []Event
	for rows.Next() {
		var event Event
		if err := rows.Scan(&event.ID, &event.Topic, &event.Key, &event.Payload, &event.AvailableAt, &event.Attempts, &event.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan outbox event: %w", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate outbox events: %w", err)
	}
	return events, nil
}

func (s *Store) Ack(ctx context.Context, consumer, eventID string) error {
	result, err := s.db.ExecContext(ctx, `
UPDATE platform_outbox_events
SET delivered_at = now(), claimed_by = NULL, claimed_until = NULL
WHERE id = $1 AND claimed_by = $2 AND delivered_at IS NULL`,
		eventID, consumer)
	if err != nil {
		return fmt.Errorf("ack outbox event: %w", err)
	}
	affected, err := result.RowsAffected()
	if err == nil && affected == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func (s *Store) Replay(ctx context.Context, consumer string, afterID string, limit int) ([]Event, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, topic, event_key, payload, available_at, attempts, created_at
FROM platform_outbox_events
WHERE delivered_at IS NOT NULL
  AND ($1 = '' OR id > $1)
ORDER BY id
LIMIT $2`, afterID, limit)
	if err != nil {
		return nil, fmt.Errorf("replay outbox events for %s: %w", consumer, err)
	}
	defer rows.Close()

	var events []Event
	for rows.Next() {
		var event Event
		if err := rows.Scan(&event.ID, &event.Topic, &event.Key, &event.Payload, &event.AvailableAt, &event.Attempts, &event.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan replay event: %w", err)
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

type CursorStore interface {
	Consumed(context.Context, string, string) (bool, error)
	MarkConsumed(context.Context, string, string) error
}

func ApplyWithCursor(ctx context.Context, cursors CursorStore, consumer string, event Event, apply func(context.Context, Event) error) error {
	consumed, err := cursors.Consumed(ctx, consumer, event.ID)
	if err != nil {
		return err
	}
	if consumed {
		return nil
	}
	if err := apply(ctx, event); err != nil {
		return err
	}
	return cursors.MarkConsumed(ctx, consumer, event.ID)
}

type MemoryCursorStore struct {
	mu       sync.Mutex
	consumed map[string]map[string]struct{}
}

func NewMemoryCursorStore() *MemoryCursorStore {
	return &MemoryCursorStore{consumed: make(map[string]map[string]struct{})}
}

func (s *MemoryCursorStore) ApplyOnce(ctx context.Context, consumer string, event Event, apply func(context.Context, Event) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	events := s.consumed[consumer]
	if _, ok := events[event.ID]; ok {
		return nil
	}
	if err := apply(ctx, event); err != nil {
		return err
	}
	if events == nil {
		events = make(map[string]struct{})
		s.consumed[consumer] = events
	}
	events[event.ID] = struct{}{}
	return nil
}

func (s *MemoryCursorStore) Consumed(ctx context.Context, consumer, eventID string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	events := s.consumed[consumer]
	_, ok := events[eventID]
	return ok, nil
}

func (s *MemoryCursorStore) MarkConsumed(ctx context.Context, consumer, eventID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	events := s.consumed[consumer]
	if events == nil {
		events = make(map[string]struct{})
		s.consumed[consumer] = events
	}
	events[eventID] = struct{}{}
	return nil
}
