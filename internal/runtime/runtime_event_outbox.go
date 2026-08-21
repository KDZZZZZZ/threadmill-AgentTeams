package runtime

import (
	"context"
	"errors"
	"time"
)

var (
	ErrInvalidEventCursor = errors.New("invalid runtime event cursor")
	ErrEventPayload       = errors.New("runtime event payload is malformed or unsupported")
	ErrConsumerClaimed    = errors.New("runtime event consumer is claimed by another dispatcher")
	ErrConsumerClaimLost  = errors.New("runtime event consumer claim is not held")
	ErrEventAckOrder      = errors.New("runtime event acknowledgement is not monotonic")
)

// RuntimeEventBatch is ordered by the database-assigned EventSequence.
// NextCursor is not an acknowledgement; it becomes durable only through Ack.
type RuntimeEventBatch struct {
	Events     []RuntimeEvent
	NextCursor int64
}

type RuntimeEventReader interface {
	ReadAfter(context.Context, int64, int) (RuntimeEventBatch, error)
}

type RuntimeEventConsumerCursor struct {
	ConsumerID        string
	LastAckedSequence int64
	Revision          int64
	UpdatedAt         time.Time
}

// RuntimeEventOutbox owns ordered reads, durable checkpoints, and one
// serialized dispatcher lease per logical consumer. Expired leases may be
// reclaimed, so sink implementations dedupe EventID for at-least-once delivery.
type RuntimeEventOutbox interface {
	RuntimeEventReader
	ConsumerCursor(context.Context, string) (RuntimeEventConsumerCursor, error)
	ClaimConsumer(context.Context, string, string, time.Duration) (RuntimeEventConsumerCursor, error)
	AckEvent(context.Context, string, string, int64) (RuntimeEventConsumerCursor, error)
}

type RuntimeEventSink interface {
	Dispatch(context.Context, RuntimeEvent) error
}

// RuntimeEventDispatcher never encloses an external sink in a SQLite
// transaction. Successful dispatch followed by a pre-ack crash is deliberately
// redelivered with the same EventID after recovery.
type RuntimeEventDispatcher struct {
	Outbox     RuntimeEventOutbox
	ConsumerID string
	OwnerID    string
	ClaimTTL   time.Duration
	BatchSize  int
	Sink       RuntimeEventSink
}

func (d RuntimeEventDispatcher) DispatchOnce(ctx context.Context) (int, error) {
	if d.Outbox == nil || d.Sink == nil || d.ConsumerID == "" || d.OwnerID == "" {
		return 0, errors.New("runtime event dispatcher dependencies are required")
	}
	if d.ClaimTTL <= 0 || d.BatchSize <= 0 {
		return 0, errors.New("runtime event dispatcher claim ttl and batch size are required")
	}
	cursor, err := d.Outbox.ClaimConsumer(ctx, d.ConsumerID, d.OwnerID, d.ClaimTTL)
	if err != nil {
		return 0, err
	}
	batch, err := d.Outbox.ReadAfter(ctx, cursor.LastAckedSequence, d.BatchSize)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, event := range batch.Events {
		if err = d.Sink.Dispatch(ctx, event); err != nil {
			return count, err
		}
		if cursor, err = d.Outbox.AckEvent(ctx, d.ConsumerID, d.OwnerID, event.EventSequence); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}
