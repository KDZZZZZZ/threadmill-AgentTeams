package runtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

type sqliteRuntimeEventOutbox struct{ r *SQLiteRuntimeStateRepository }

func (s sqliteRuntimeEventOutbox) ReadAfter(ctx context.Context, cursor int64, limit int) (RuntimeEventBatch, error) {
	if cursor < 0 || limit <= 0 || limit > 1000 {
		return RuntimeEventBatch{}, ErrInvalidEventCursor
	}
	rows, err := s.r.db.QueryContext(ctx, "SELECT o.event_sequence,e.event_id,e.event_type,e.occurred_at,e.task_id,e.invocation_id,e.generation,e.execution_epoch,e.aggregate_key,e.result_revision,e.payload_version,e.payload FROM runtime_event_order o JOIN runtime_events e ON e.event_id=o.event_id WHERE o.event_sequence>? ORDER BY o.event_sequence LIMIT ?", cursor, limit)
	if err != nil {
		return RuntimeEventBatch{}, err
	}
	defer rows.Close()
	batch := RuntimeEventBatch{NextCursor: cursor}
	for rows.Next() {
		event, err := scanRuntimeEvent(rows)
		if err != nil {
			return RuntimeEventBatch{}, err
		}
		batch.Events = append(batch.Events, event)
		batch.NextCursor = event.EventSequence
	}
	return batch, rows.Err()
}

func (s sqliteRuntimeEventOutbox) ConsumerCursor(ctx context.Context, consumerID string) (RuntimeEventConsumerCursor, error) {
	if consumerID == "" {
		return RuntimeEventConsumerCursor{}, ErrInvalidEventCursor
	}
	value := RuntimeEventConsumerCursor{ConsumerID: consumerID}
	var at string
	err := s.r.db.QueryRowContext(ctx, "SELECT last_acked_sequence,revision,updated_at FROM runtime_event_consumers WHERE consumer_id=?", consumerID).Scan(&value.LastAckedSequence, &value.Revision, &at)
	if errors.Is(err, sql.ErrNoRows) {
		return value, nil
	}
	if err != nil {
		return RuntimeEventConsumerCursor{}, err
	}
	value.UpdatedAt, err = time.Parse(time.RFC3339Nano, at)
	return value, err
}

func (s sqliteRuntimeEventOutbox) ClaimConsumer(ctx context.Context, consumerID, ownerID string, ttl time.Duration) (RuntimeEventConsumerCursor, error) {
	if consumerID == "" || ownerID == "" || ttl <= 0 {
		return RuntimeEventConsumerCursor{}, ErrInvalidEventCursor
	}
	tx, err := s.r.db.BeginTx(ctx, nil)
	if err != nil {
		return RuntimeEventConsumerCursor{}, err
	}
	defer tx.Rollback()
	now := nowUTC()
	if _, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO runtime_event_consumers VALUES(?,?,?,?)", consumerID, 0, 0, now.Format(time.RFC3339Nano)); err != nil {
		return RuntimeEventConsumerCursor{}, err
	}
	var currentOwner, expiresAt string
	var revision int64
	err = tx.QueryRowContext(ctx, "SELECT owner_id,expires_at,revision FROM runtime_event_claims WHERE consumer_id=?", consumerID).Scan(&currentOwner, &expiresAt, &revision)
	if errors.Is(err, sql.ErrNoRows) {
		if _, err = tx.ExecContext(ctx, "INSERT INTO runtime_event_claims VALUES(?,?,?,?)", consumerID, ownerID, now.Add(ttl).Format(time.RFC3339Nano), 1); err != nil {
			return RuntimeEventConsumerCursor{}, err
		}
	} else if err != nil {
		return RuntimeEventConsumerCursor{}, err
	} else {
		expires, parseErr := time.Parse(time.RFC3339Nano, expiresAt)
		if parseErr != nil {
			return RuntimeEventConsumerCursor{}, parseErr
		}
		if currentOwner != ownerID && expires.After(now) {
			return RuntimeEventConsumerCursor{}, ErrConsumerClaimed
		}
		result, updateErr := tx.ExecContext(ctx, "UPDATE runtime_event_claims SET owner_id=?,expires_at=?,revision=? WHERE consumer_id=? AND revision=?", ownerID, now.Add(ttl).Format(time.RFC3339Nano), revision+1, consumerID, revision)
		if updateErr != nil {
			return RuntimeEventConsumerCursor{}, updateErr
		}
		changed, updateErr := result.RowsAffected()
		if updateErr != nil {
			return RuntimeEventConsumerCursor{}, updateErr
		}
		if changed != 1 {
			return RuntimeEventConsumerCursor{}, ErrConsumerClaimed
		}
	}
	if err = tx.Commit(); err != nil {
		return RuntimeEventConsumerCursor{}, err
	}
	return s.ConsumerCursor(ctx, consumerID)
}

func (s sqliteRuntimeEventOutbox) AckEvent(ctx context.Context, consumerID, ownerID string, sequence int64) (RuntimeEventConsumerCursor, error) {
	if consumerID == "" || ownerID == "" || sequence <= 0 {
		return RuntimeEventConsumerCursor{}, ErrInvalidEventCursor
	}
	tx, err := s.r.db.BeginTx(ctx, nil)
	if err != nil {
		return RuntimeEventConsumerCursor{}, err
	}
	defer tx.Rollback()
	var claimOwner, expiresAt string
	if err = tx.QueryRowContext(ctx, "SELECT owner_id,expires_at FROM runtime_event_claims WHERE consumer_id=?", consumerID).Scan(&claimOwner, &expiresAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return RuntimeEventConsumerCursor{}, ErrConsumerClaimLost
		}
		return RuntimeEventConsumerCursor{}, err
	}
	expires, err := time.Parse(time.RFC3339Nano, expiresAt)
	if err != nil {
		return RuntimeEventConsumerCursor{}, err
	}
	if claimOwner != ownerID || !expires.After(nowUTC()) {
		return RuntimeEventConsumerCursor{}, ErrConsumerClaimLost
	}
	value := RuntimeEventConsumerCursor{ConsumerID: consumerID}
	var updatedAt string
	if err = tx.QueryRowContext(ctx, "SELECT last_acked_sequence,revision,updated_at FROM runtime_event_consumers WHERE consumer_id=?", consumerID).Scan(&value.LastAckedSequence, &value.Revision, &updatedAt); err != nil {
		return RuntimeEventConsumerCursor{}, err
	}
	if sequence != value.LastAckedSequence+1 {
		return RuntimeEventConsumerCursor{}, ErrEventAckOrder
	}
	var exists int
	if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM runtime_event_order WHERE event_sequence=?", sequence).Scan(&exists); err != nil {
		return RuntimeEventConsumerCursor{}, err
	}
	if exists != 1 {
		return RuntimeEventConsumerCursor{}, ErrEventAckOrder
	}
	value.LastAckedSequence, value.Revision, value.UpdatedAt = sequence, value.Revision+1, nowUTC()
	result, err := tx.ExecContext(ctx, "UPDATE runtime_event_consumers SET last_acked_sequence=?,revision=?,updated_at=? WHERE consumer_id=? AND revision=?", value.LastAckedSequence, value.Revision, value.UpdatedAt.Format(time.RFC3339Nano), consumerID, value.Revision-1)
	if err != nil {
		return RuntimeEventConsumerCursor{}, err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return RuntimeEventConsumerCursor{}, err
	}
	if changed != 1 {
		return RuntimeEventConsumerCursor{}, ErrConsumerClaimLost
	}
	if err = tx.Commit(); err != nil {
		return RuntimeEventConsumerCursor{}, err
	}
	return value, nil
}

type runtimeEventScanner interface{ Scan(...any) error }

func scanRuntimeEvent(scanner runtimeEventScanner) (RuntimeEvent, error) {
	var value RuntimeEvent
	var occurredAt string
	var epoch sql.NullInt64
	var payload []byte
	if err := scanner.Scan(&value.EventSequence, &value.EventID, &value.EventType, &occurredAt, &value.TaskID, &value.InvocationID, &value.Generation, &epoch, &value.AggregateKey, &value.ResultRevision, &value.PayloadVersion, &payload); err != nil {
		return RuntimeEvent{}, err
	}
	value.Payload = json.RawMessage(payload)
	var err error
	value.OccurredAt, err = time.Parse(time.RFC3339Nano, occurredAt)
	if err != nil || value.EventSequence <= 0 || value.EventID == "" || value.PayloadVersion != 1 || !json.Valid(value.Payload) {
		return RuntimeEvent{}, ErrEventPayload
	}
	if epoch.Valid {
		valueEpoch := ExecutionEpoch(epoch.Int64)
		value.ExecutionEpoch = &valueEpoch
	}
	return value, nil
}
