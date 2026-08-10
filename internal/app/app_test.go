package app

import (
	"context"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/config"
)

type closeRecorder struct {
	closed chan struct{}
}

func (r closeRecorder) Close(context.Context) error {
	close(r.closed)
	return nil
}

func TestRunClosesComponentsOnContextCancellation(t *testing.T) {
	closed := make(chan struct{})
	a := New(config.Config{HTTPAddr: ":0"}, WithComponent(closeRecorder{closed: closed}))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- a.Run(ctx)
	}()

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() error = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not return after context cancellation")
	}

	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("component was not closed")
	}
}
