package app

import (
	"context"
	"errors"
	"fmt"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/config"
)

type Component interface {
	Close(context.Context) error
}

type App struct {
	config     config.Config
	components []Component
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

func (a *App) Migrate(context.Context) error {
	return nil
}

func (a *App) HTTPAddr() string {
	return a.config.HTTPAddr
}

func Serve(ctx context.Context, cfg config.Config) error {
	return New(cfg).Run(ctx)
}

func Migrate(ctx context.Context, cfg config.Config) error {
	if cfg.DatabaseURL == "" {
		return fmt.Errorf("database url is required")
	}
	return New(cfg).Migrate(ctx)
}
