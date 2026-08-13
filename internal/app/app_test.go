package app

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/config"
)

type closeRecorder struct {
	closed chan struct{}
}

func TestMigrateRunsConfiguredMigrationRunner(t *testing.T) {
	t.Parallel()

	called := false
	cfg := config.Config{DatabaseURL: "postgres://threadmill:threadmill@localhost:5432/threadmill"}
	a := New(cfg, WithMigrationRunner(func(ctx context.Context, got config.Config) error {
		called = true
		if ctx == nil {
			t.Fatal("migration context is nil")
		}
		if got.DatabaseURL != cfg.DatabaseURL {
			t.Fatalf("migration config = %#v, want %#v", got, cfg)
		}
		return nil
	}))

	if err := a.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate() error = %v, want nil", err)
	}
	if !called {
		t.Fatal("migration runner was not called")
	}
}

func TestMigrateReturnsConfiguredMigrationRunnerError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("migration failed")
	a := New(config.Config{DatabaseURL: "postgres://threadmill:threadmill@localhost:5432/threadmill"}, WithMigrationRunner(func(context.Context, config.Config) error {
		return wantErr
	}))

	if err := a.Migrate(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("Migrate() error = %v, want %v", err, wantErr)
	}
}

func TestServeHTTPShutsDownAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := serveHTTP(ctx, "127.0.0.1:0", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	if err != nil {
		t.Fatalf("serveHTTP() error = %v, want nil", err)
	}
}

func TestServeHTTPRequiresAddress(t *testing.T) {
	err := serveHTTP(context.Background(), "", http.NotFoundHandler())
	if err == nil {
		t.Fatal("serveHTTP() error = nil, want address validation error")
	}
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
