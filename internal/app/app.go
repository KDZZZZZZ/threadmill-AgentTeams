package app

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/app/fakehost"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/config"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/postgres"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/migrations"
)

type Component interface {
	Close(context.Context) error
}

type App struct {
	config        config.Config
	components    []Component
	migrationFunc func(context.Context, config.Config) error
}

type Option func(*App)

func New(cfg config.Config, opts ...Option) *App {
	a := &App{config: cfg}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

func WithComponent(component Component) Option {
	return func(a *App) {
		a.components = append(a.components, component)
	}
}

func WithMigrationRunner(runner func(context.Context, config.Config) error) Option {
	return func(a *App) {
		a.migrationFunc = runner
	}
}

func (a *App) Run(ctx context.Context) error {
	<-ctx.Done()
	return a.Shutdown(context.Background())
}

func (a *App) Shutdown(ctx context.Context) error {
	var err error
	for i := len(a.components) - 1; i >= 0; i-- {
		err = errors.Join(err, a.components[i].Close(ctx))
	}
	return err
}

func (a *App) Migrate(ctx context.Context) error {
	if a.migrationFunc != nil {
		return a.migrationFunc(ctx, a.config)
	}
	return runMigrations(ctx, a.config)
}

func (a *App) HTTPAddr() string {
	return a.config.HTTPAddr
}

func Serve(ctx context.Context, cfg config.Config) error {
	return New(cfg).Run(ctx)
}

// ServeFake runs the local acceptance host through the same HTTP/SSE and Web
// adapters used by the operator console. Fake mode replaces external
// infrastructure only; it does not introduce another graph or context model.
func ServeFake(ctx context.Context, httpAddr, webDistDir string) error {
	host, err := fakehost.New(ctx, fakehost.Options{WebDistDir: webDistDir})
	if err != nil {
		return fmt.Errorf("create fake host: %w", err)
	}
	return errors.Join(serveHTTP(ctx, httpAddr, host.Handler()), host.Close())
}

func serveHTTP(ctx context.Context, addr string, handler http.Handler) error {
	if addr == "" {
		return fmt.Errorf("http address is required")
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}
	server := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	serveErr := make(chan error, 1)
	go func() {
		err := server.Serve(listener)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		serveErr <- err
	}()

	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown http server: %w", err)
		}
		return <-serveErr
	}
}

func Migrate(ctx context.Context, cfg config.Config) error {
	if cfg.DatabaseURL == "" {
		return fmt.Errorf("database url is required")
	}
	return New(cfg).Migrate(ctx)
}

func runMigrations(ctx context.Context, cfg config.Config) error {
	db, err := postgres.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer db.Close(context.Background())

	loaded, err := postgres.LoadMigrations(migrations.FS, migrations.Dir)
	if err != nil {
		return fmt.Errorf("load migrations: %w", err)
	}
	if err := postgres.NewMigrator(db.SQL()).Apply(ctx, loaded); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	return nil
}
