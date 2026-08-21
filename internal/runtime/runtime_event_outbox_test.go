package runtime

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func appendOutboxTestEvent(t *testing.T, r *SQLiteRuntimeStateRepository, typ string) {
	t.Helper()
	tx, err := r.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err = appendEvent(context.Background(), tx, typ, WaitingKey{TaskID: "task", InvocationID: "invocation", Generation: 2}, 1, "test", 1, []byte(`{"kind":"test"}`)); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

type recordingEventSink struct {
	mu       sync.Mutex
	ids      []string
	failOnce map[string]bool
}

func (s *recordingEventSink) Dispatch(_ context.Context, event RuntimeEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ids = append(s.ids, event.EventID)
	if s.failOnce[event.EventID] {
		delete(s.failOnce, event.EventID)
		return errors.New("sink unavailable")
	}
	return nil
}

func TestRuntimeEventOutboxOrdersReadsAndRejectsMalformedPayload(t *testing.T) {
	ctx := context.Background()
	r, err := OpenSQLiteRuntimeStateRepository(filepath.Join(t.TempDir(), "runtime.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for _, typ := range []string{"InputRevisionObserved", "ArtifactRegistered", "PhaseOutputSubmitted"} {
		appendOutboxTestEvent(t, r, typ)
	}
	outbox := r.EventOutbox()
	batch, err := outbox.ReadAfter(ctx, 0, 2)
	if err != nil || len(batch.Events) != 2 || batch.Events[0].EventSequence >= batch.Events[1].EventSequence || batch.NextCursor != batch.Events[1].EventSequence {
		t.Fatalf("batch=%#v err=%v", batch, err)
	}
	empty, err := outbox.ReadAfter(ctx, batch.NextCursor, 2)
	if err != nil || len(empty.Events) != 1 || empty.Events[0].EventType != "PhaseOutputSubmitted" {
		t.Fatalf("remaining=%#v err=%v", empty, err)
	}
	if _, err = outbox.ReadAfter(ctx, -1, 1); !errors.Is(err, ErrInvalidEventCursor) {
		t.Fatalf("negative cursor err=%v", err)
	}
	if _, err = r.db.Exec("INSERT INTO runtime_events VALUES('malformed','Malformed','2026-01-01T00:00:00Z','task','invocation',2,NULL,'test',1,99,'not-json')"); err != nil {
		t.Fatal(err)
	}
	if _, err = r.db.Exec("INSERT INTO runtime_event_order(event_id) VALUES('malformed')"); err != nil {
		t.Fatal(err)
	}
	if _, err = outbox.ReadAfter(ctx, empty.NextCursor, 2); !errors.Is(err, ErrEventPayload) {
		t.Fatalf("malformed event err=%v", err)
	}
}

func TestRuntimeEventDispatcherIsAtLeastOnceAndCursorSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "runtime.db")
	r, err := OpenSQLiteRuntimeStateRepository(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, typ := range []string{"ArtifactRegistered", "PhaseOutputSubmitted", "PhysicalExecutionTeardownStepCompleted"} {
		appendOutboxTestEvent(t, r, typ)
	}
	first, err := r.EventOutbox().ReadAfter(ctx, 0, 3)
	if err != nil {
		t.Fatal(err)
	}
	sink := &recordingEventSink{failOnce: map[string]bool{first.Events[1].EventID: true}}
	dispatcher := RuntimeEventDispatcher{Outbox: r.EventOutbox(), ConsumerID: "audit-projection", OwnerID: "process-a", ClaimTTL: time.Minute, BatchSize: 3, Sink: sink}
	if count, err := dispatcher.DispatchOnce(ctx); err == nil || count != 1 {
		t.Fatalf("failure dispatch count=%d err=%v", count, err)
	}
	cursor, err := r.EventOutbox().ConsumerCursor(ctx, "audit-projection")
	if err != nil || cursor.LastAckedSequence != first.Events[0].EventSequence {
		t.Fatalf("cursor=%#v err=%v", cursor, err)
	}
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	r, err = OpenSQLiteRuntimeStateRepository(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	// A fresh owner reclaims only after the original lease expires. This models
	// crash recovery; the previously dispatched-but-unacked second EventID is
	// deliberately delivered again.
	if _, err = r.db.Exec("UPDATE runtime_event_claims SET expires_at='2000-01-01T00:00:00Z' WHERE consumer_id='audit-projection'"); err != nil {
		t.Fatal(err)
	}
	dispatcher = RuntimeEventDispatcher{Outbox: r.EventOutbox(), ConsumerID: "audit-projection", OwnerID: "process-b", ClaimTTL: time.Minute, BatchSize: 3, Sink: sink}
	if count, err := dispatcher.DispatchOnce(ctx); err != nil || count != 2 {
		t.Fatalf("recovery dispatch count=%d err=%v", count, err)
	}
	cursor, err = r.EventOutbox().ConsumerCursor(ctx, "audit-projection")
	if err != nil || cursor.LastAckedSequence != first.Events[2].EventSequence {
		t.Fatalf("reopened cursor=%#v err=%v", cursor, err)
	}
	if len(sink.ids) != 4 || sink.ids[1] != sink.ids[2] {
		t.Fatalf("expected exactly one redelivery after pre-ack failure, ids=%v", sink.ids)
	}
}

func TestRuntimeEventOutboxSerializesClaimsAndAcknowledgements(t *testing.T) {
	ctx := context.Background()
	r, err := OpenSQLiteRuntimeStateRepository(filepath.Join(t.TempDir(), "runtime.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	appendOutboxTestEvent(t, r, "ArtifactRegistered")
	appendOutboxTestEvent(t, r, "PhaseOutputSubmitted")
	outbox := r.EventOutbox()
	if _, err = outbox.ClaimConsumer(ctx, "consumer", "owner-a", time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err = outbox.ClaimConsumer(ctx, "consumer", "owner-b", time.Minute); !errors.Is(err, ErrConsumerClaimed) {
		t.Fatalf("concurrent claim err=%v", err)
	}
	batch, err := outbox.ReadAfter(ctx, 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = outbox.AckEvent(ctx, "consumer", "owner-a", batch.Events[1].EventSequence); !errors.Is(err, ErrEventAckOrder) {
		t.Fatalf("out-of-order ack err=%v", err)
	}
	if _, err = outbox.AckEvent(ctx, "consumer", "owner-a", batch.Events[0].EventSequence); err != nil {
		t.Fatal(err)
	}
	if _, err = outbox.AckEvent(ctx, "consumer", "owner-a", batch.Events[0].EventSequence); !errors.Is(err, ErrEventAckOrder) {
		t.Fatalf("cursor regression err=%v", err)
	}
	if _, err = r.db.Exec("UPDATE runtime_event_claims SET expires_at='2000-01-01T00:00:00Z' WHERE consumer_id='consumer'"); err != nil {
		t.Fatal(err)
	}
	if _, err = outbox.ClaimConsumer(ctx, "consumer", "owner-b", time.Minute); err != nil {
		t.Fatalf("stale claim recovery err=%v", err)
	}
}

func TestRuntimeEventOutboxConcurrentClaimHasSingleOwner(t *testing.T) {
	ctx := context.Background()
	r, err := OpenSQLiteRuntimeStateRepository(filepath.Join(t.TempDir(), "runtime.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	outbox := r.EventOutbox()
	start := make(chan struct{})
	errs := make(chan error, 2)
	for _, owner := range []string{"owner-a", "owner-b"} {
		go func(owner string) {
			<-start
			_, claimErr := outbox.ClaimConsumer(ctx, "consumer", owner, time.Minute)
			errs <- claimErr
		}(owner)
	}
	close(start)
	first, second := <-errs, <-errs
	if (first == nil) == (second == nil) {
		t.Fatalf("claims must have exactly one winner: %v, %v", first, second)
	}
	if first != nil && !errors.Is(first, ErrConsumerClaimed) {
		t.Fatalf("unexpected losing claim error: %v", first)
	}
	if second != nil && !errors.Is(second, ErrConsumerClaimed) {
		t.Fatalf("unexpected losing claim error: %v", second)
	}
}

func TestRuntimeEventOutboxMigratesV2EventsToOrderedFeed(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "runtime.db")
	r, err := OpenSQLiteRuntimeStateRepository(path)
	if err != nil {
		t.Fatal(err)
	}
	appendOutboxTestEvent(t, r, "ArtifactRegistered")
	appendOutboxTestEvent(t, r, "PhaseOutputSubmitted")
	for _, statement := range []string{
		"DROP TABLE runtime_event_claims",
		"DROP TABLE runtime_event_consumers",
		"DROP TABLE runtime_event_order",
		"UPDATE runtime_schema_version SET version=2",
	} {
		if _, err = r.db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	r, err = OpenSQLiteRuntimeStateRepository(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if version, err := r.SchemaVersion(ctx); err != nil || version != latestRuntimeSchemaVersion {
		t.Fatalf("version=%d err=%v", version, err)
	}
	batch, err := r.EventOutbox().ReadAfter(ctx, 0, 10)
	if err != nil || len(batch.Events) != 2 || batch.Events[0].EventSequence == 0 || batch.Events[1].EventSequence <= batch.Events[0].EventSequence {
		t.Fatalf("migrated batch=%#v err=%v", batch, err)
	}
}
