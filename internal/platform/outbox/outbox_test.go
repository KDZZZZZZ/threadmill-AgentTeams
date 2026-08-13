package outbox

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
)

func TestMemoryCursorApplyOnceSkipsAlreadyConsumedEvent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cursor := NewMemoryCursorStore()
	var sideEffects int
	event := Event{ID: "evt-1", Topic: "context.delta", Payload: []byte(`{"ok":true}`)}

	for i := 0; i < 2; i++ {
		if err := cursor.ApplyOnce(ctx, "consumer-a", event, func(context.Context, Event) error {
			sideEffects++
			return nil
		}); err != nil {
			t.Fatalf("ApplyOnce iteration %d returned error: %v", i, err)
		}
	}

	if sideEffects != 1 {
		t.Fatalf("side effects = %d, want 1", sideEffects)
	}
}

func TestMemoryCursorApplyOnceConcurrentReplayProducesOneSideEffect(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cursor := NewMemoryCursorStore()
	event := Event{ID: "evt-1", Topic: "context.delta", Payload: []byte(`{"ok":true}`)}
	const workers = 32
	var sideEffects atomic.Int64
	var wg sync.WaitGroup
	errs := make(chan error, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- cursor.ApplyOnce(ctx, "consumer-a", event, func(context.Context, Event) error {
				sideEffects.Add(1)
				return nil
			})
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("ApplyOnce returned error: %v", err)
		}
	}
	if got := sideEffects.Load(); got != 1 {
		t.Fatalf("side effects = %d, want 1", got)
	}
}
