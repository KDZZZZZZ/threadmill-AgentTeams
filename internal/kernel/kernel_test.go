package kernel

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func TestCheckExpectedRevision(t *testing.T) {
	if err := CheckExpectedRevision(Revision(7), Revision(7)); err != nil {
		t.Fatalf("expected matching revision to pass: %v", err)
	}

	for name, err := range map[string]error{
		"mismatch": CheckExpectedRevision(Revision(6), Revision(7)),
		"latest":   CheckExpectedRevision(LatestRevision, Revision(7)),
	} {
		if !IsCode(err, CodeRevisionConflict) {
			t.Fatalf("%s: expected revision_conflict, got %v", name, err)
		}
	}
}

func TestErrorCodeOf(t *testing.T) {
	if got := ErrorCodeOf(Forbidden("no")); got != CodeForbidden {
		t.Fatalf("expected forbidden, got %s", got)
	}

	pointerErr := &Error{Code: CodeStaleCommand, Message: "stale"}
	if got := ErrorCodeOf(pointerErr); got != CodeStaleCommand {
		t.Fatalf("expected pointer stale_command, got %s", got)
	}

	wrappedValue := fmt.Errorf("apply failed: %w", Error{Code: CodeLeaseConflict, Message: "lease"})
	if got := ErrorCodeOf(wrappedValue); got != CodeLeaseConflict {
		t.Fatalf("expected wrapped lease_conflict, got %s", got)
	}

	wrappedPointer := fmt.Errorf("dispatch failed: %w", &Error{Code: CodeExecutorUnavailable, Message: "host"})
	if got := ErrorCodeOf(wrappedPointer); got != CodeExecutorUnavailable {
		t.Fatalf("expected wrapped executor_unavailable, got %s", got)
	}

	if got := ErrorCodeOf(InvalidArgument("bad")); got != CodeInvalidRequest {
		t.Fatalf("expected invalid_request, got %s", got)
	}

	if got := ErrorCodeOf(errors.New("plain")); got != CodeInternalError {
		t.Fatalf("expected internal_error fallback, got %s", got)
	}
}

func TestMemoryIdempotencyStoreReplaysSamePayload(t *testing.T) {
	store := NewMemoryIdempotencyStore()
	var calls atomic.Int32

	first, err := store.Execute(context.Background(), "project:p1/op:create", "k1", []byte("payload"), func(context.Context) (IdempotencyResponse, error) {
		calls.Add(1)
		return IdempotencyResponse{StatusCode: 202, Body: []byte("accepted")}, nil
	})
	if err != nil {
		t.Fatalf("first execute failed: %v", err)
	}

	second, err := store.Execute(context.Background(), "project:p1/op:create", "k1", []byte("payload"), func(context.Context) (IdempotencyResponse, error) {
		calls.Add(1)
		return IdempotencyResponse{StatusCode: 500, Body: []byte("should-not-run")}, nil
	})
	if err != nil {
		t.Fatalf("second execute failed: %v", err)
	}

	if calls.Load() != 1 {
		t.Fatalf("expected executor to run once, ran %d times", calls.Load())
	}
	if first.StatusCode != second.StatusCode || string(second.Body) != "accepted" {
		t.Fatalf("expected replayed response, first=%+v second=%+v", first, second)
	}
}

func TestMemoryIdempotencyStoreRejectsDifferentPayload(t *testing.T) {
	store := NewMemoryIdempotencyStore()

	_, err := store.Execute(context.Background(), "project:p1/op:create", "k1", []byte("payload-a"), func(context.Context) (IdempotencyResponse, error) {
		return IdempotencyResponse{StatusCode: 202, Body: []byte("accepted")}, nil
	})
	if err != nil {
		t.Fatalf("first execute failed: %v", err)
	}

	_, err = store.Execute(context.Background(), "project:p1/op:create", "k1", []byte("payload-b"), func(context.Context) (IdempotencyResponse, error) {
		t.Fatal("executor must not run for conflicting payload")
		return IdempotencyResponse{}, nil
	})
	if !IsCode(err, CodeIdempotencyConflict) {
		t.Fatalf("expected idempotency_conflict, got %v", err)
	}
}

func TestMemoryIdempotencyStoreConcurrentSameKeyRunsOnce(t *testing.T) {
	store := NewMemoryIdempotencyStore()
	var calls atomic.Int32
	const workers = 64

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(workers)
	errs := make(chan error, workers)
	responses := make(chan IdempotencyResponse, workers)

	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			<-start
			response, err := store.Execute(context.Background(), "project:p1/op:create", "k1", []byte("payload"), func(context.Context) (IdempotencyResponse, error) {
				calls.Add(1)
				return IdempotencyResponse{StatusCode: 200, Body: []byte("ok")}, nil
			})
			if err != nil {
				errs <- err
				return
			}
			responses <- response
		}()
	}

	close(start)
	wg.Wait()
	close(errs)
	close(responses)

	for err := range errs {
		t.Fatalf("unexpected concurrent execute error: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("expected one executor call, got %d", calls.Load())
	}
	if len(responses) != workers {
		t.Fatalf("expected %d responses, got %d", workers, len(responses))
	}
	for response := range responses {
		if response.StatusCode != 200 || string(response.Body) != "ok" {
			t.Fatalf("unexpected response: %+v", response)
		}
	}
}

func TestMemoryIdempotencyStoreAllowsRetryAfterExecutorError(t *testing.T) {
	store := NewMemoryIdempotencyStore()
	expectedErr := errors.New("transient")

	_, err := store.Execute(context.Background(), "project:p1/op:create", "k1", []byte("payload"), func(context.Context) (IdempotencyResponse, error) {
		return IdempotencyResponse{}, expectedErr
	})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected transient error, got %v", err)
	}

	response, err := store.Execute(context.Background(), "project:p1/op:create", "k1", []byte("payload"), func(context.Context) (IdempotencyResponse, error) {
		return IdempotencyResponse{StatusCode: 201, Body: []byte("created")}, nil
	})
	if err != nil {
		t.Fatalf("retry execute failed: %v", err)
	}
	if response.StatusCode != 201 || string(response.Body) != "created" {
		t.Fatalf("unexpected retry response: %+v", response)
	}
}
