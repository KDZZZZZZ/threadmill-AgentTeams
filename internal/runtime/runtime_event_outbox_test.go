package runtime

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/artifacts"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/phaseagent"
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

func eventSinkIDs(s *recordingEventSink) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.ids...)
}

type recordingEventSink struct {
	mu       sync.Mutex
	ids      []string
	failOnce map[string]bool
}

// idempotentEventSink models a projection that keys its durable side effect by
// Runtime EventID. Delivery can repeat, but applying the projection cannot.
type idempotentEventSink struct {
	mu         sync.Mutex
	deliveries []string
	applied    map[string]int
}

func (s *idempotentEventSink) Dispatch(_ context.Context, event RuntimeEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deliveries = append(s.deliveries, event.EventID)
	if s.applied[event.EventID] == 0 {
		s.applied[event.EventID]++
	}
	return nil
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

func TestRuntimeEventOutboxColdReopenDispatchesLifecycleEventsWithoutChangingSnapshots(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "runtime.db")
	r, err := OpenSQLiteRuntimeStateRepository(path)
	if err != nil {
		t.Fatal(err)
	}
	key := WaitingKey{TaskID: "task", InvocationID: "inv", Generation: 2}
	waiting, err := r.WaitingStore().Create(ctx, WaitingRecord{
		Key: key, ExecutionEpoch: 2,
		Endpoint:           phaseagent.PhaseEndpointRef{TaskID: "task", EndpointID: string(phaseagent.PhaseExecute)},
		PreviousBindingRef: "binding-b2", InputRevision: "input-r5", ContinuationRef: "continuation",
		State: AwaitStateRunning,
	})
	if err != nil {
		t.Fatal(err)
	}
	execution, err := r.PhysicalExecutionStore().Create(ctx, PhysicalExecution{TaskID: key.TaskID, InvocationID: key.InvocationID, Generation: key.Generation, ExecutionEpoch: 2, BindingRef: "binding-b2", InputRevision: "input-r5", State: PhysicalExecutionRunning})
	if err != nil {
		t.Fatal(err)
	}
	owner := artifacts.TrustedOwner{TaskID: key.TaskID, InvocationID: key.InvocationID, Generation: key.Generation}
	metadata := durableArtifactMetadata(owner, "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", "s3://test/artifact")
	ref, created, err := r.ArtifactStore().RegisterArtifact(ctx, metadata, owner)
	if err != nil || !created {
		t.Fatalf("artifact registration ref=%q created=%t err=%v", ref, created, err)
	}
	outputCandidate := PhaseOutputRecord{Key: PhaseOutputKey{TaskID: key.TaskID, InvocationID: key.InvocationID, Generation: key.Generation}, BindingRef: "binding-b2", InputRevision: "input-r5", ExecutionEpoch: 2, Output: phaseagent.PhaseOutput{Phase: phaseagent.PhaseExecute, ReportRef: string(ref)}}
	if _, _, created, err = r.LifecycleMutations().AcceptPhaseOutput(ctx, outputCandidate, key, waiting.Revision); err != nil || !created {
		t.Fatalf("output acceptance created=%t err=%v", created, err)
	}
	execution, changed, err := r.LifecycleMutations().AdvanceTeardown(ctx, execution.Key(), execution.Revision, TeardownStepBegin)
	if err != nil || !changed {
		t.Fatalf("teardown begin changed=%t err=%v", changed, err)
	}
	if _, changed, err = r.LifecycleMutations().AdvanceTeardown(ctx, execution.Key(), execution.Revision, TeardownStepTask); err != nil || !changed {
		t.Fatalf("teardown task changed=%t err=%v", changed, err)
	}

	outbox := r.EventOutbox()
	batch, err := outbox.ReadAfter(ctx, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	var outputIndex = -1
	for i, event := range batch.Events {
		if event.EventType == "PhaseOutputSubmitted" {
			outputIndex = i
			break
		}
	}
	if outputIndex < 1 {
		t.Fatalf("missing ordered PhaseOutputSubmitted event: %#v", batch.Events)
	}
	sink := &idempotentEventSink{applied: map[string]int{}}
	if _, err = outbox.ClaimConsumer(ctx, "audit-projection", "process-a", time.Minute); err != nil {
		t.Fatal(err)
	}
	for _, event := range batch.Events[:outputIndex] {
		if err = sink.Dispatch(ctx, event); err != nil {
			t.Fatal(err)
		}
		if _, err = outbox.AckEvent(ctx, "audit-projection", "process-a", event.EventSequence); err != nil {
			t.Fatal(err)
		}
	}
	// Model a process crash after a successful downstream dispatch but before
	// its checkpoint transaction. The output itself is already authoritative.
	outputEvent := batch.Events[outputIndex]
	if err = sink.Dispatch(ctx, outputEvent); err != nil {
		t.Fatal(err)
	}
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}

	r, err = OpenSQLiteRuntimeStateRepository(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err = r.db.Exec("UPDATE runtime_event_claims SET expires_at='2000-01-01T00:00:00Z' WHERE consumer_id='audit-projection'"); err != nil {
		t.Fatal(err)
	}
	// Pending event delivery neither alters artifact ownership nor replays the
	// accepted logical output or teardown side effect.
	if err = r.ArtifactStore().ValidateArtifactAccess(ctx, owner, []artifacts.ArtifactRef{ref}); err != nil {
		t.Fatal(err)
	}
	if _, found, err := r.PhaseOutputStore().Get(ctx, outputCandidate.Key); err != nil || !found {
		t.Fatalf("output after reopen found=%t err=%v", found, err)
	}
	storedWaiting, found, err := r.WaitingStore().Get(ctx, key)
	if err != nil || !found || storedWaiting.State != AwaitStateTerminal {
		t.Fatalf("waiting after reopen=%#v found=%t err=%v", storedWaiting, found, err)
	}
	storedExecution, found, err := r.PhysicalExecutionStore().Get(ctx, execution.Key())
	if err != nil || !found || !storedExecution.Teardown.TeamHarnessTaskCancelled {
		t.Fatalf("teardown snapshot after reopen=%#v found=%t err=%v", storedExecution, found, err)
	}
	if _, duplicate, err := r.ArtifactStore().RegisterArtifact(ctx, metadata, owner); err != nil || duplicate {
		t.Fatalf("artifact duplicate created=%t err=%v", duplicate, err)
	}
	if _, _, duplicate, err := r.LifecycleMutations().AcceptPhaseOutput(ctx, outputCandidate, key, waiting.Revision); err != nil || duplicate {
		t.Fatalf("output duplicate created=%t err=%v", duplicate, err)
	}
	dispatcher := RuntimeEventDispatcher{Outbox: r.EventOutbox(), ConsumerID: "audit-projection", OwnerID: "process-b", ClaimTTL: time.Minute, BatchSize: 100, Sink: sink}
	if count, err := dispatcher.DispatchOnce(ctx); err != nil || count != len(batch.Events)-outputIndex {
		t.Fatalf("recovery dispatch count=%d err=%v", count, err)
	}
	sink.mu.Lock()
	ids := append([]string(nil), sink.deliveries...)
	outputApplied := sink.applied[outputEvent.EventID]
	sink.mu.Unlock()
	duplicates := 0
	for _, id := range ids {
		if id == outputEvent.EventID {
			duplicates++
		}
	}
	if duplicates != 2 {
		t.Fatalf("expected output EventID redelivery after pre-ack crash, ids=%v", ids)
	}
	if outputApplied != 1 {
		t.Fatalf("EventID idempotency applied output projection %d times", outputApplied)
	}
	cursor, err := r.EventOutbox().ConsumerCursor(ctx, "audit-projection")
	if err != nil || cursor.LastAckedSequence != batch.Events[len(batch.Events)-1].EventSequence {
		t.Fatalf("final cursor=%#v err=%v", cursor, err)
	}
	if post, err := r.EventOutbox().ReadAfter(ctx, cursor.LastAckedSequence, 10); err != nil || len(post.Events) != 0 {
		t.Fatalf("acked events were redelivered: %#v err=%v", post, err)
	}
}
