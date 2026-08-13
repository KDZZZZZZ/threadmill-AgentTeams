package app

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/config"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/postgres"
)

const maximumOperatorBootstrapTTL = 24 * time.Hour

// OperatorBootstrap is deliberately returned only by the explicit CLI path.
// Production HTTP startup never calls this function and never installs the
// returned values as a browser cookie.
type OperatorBootstrap struct {
	ActorPrincipalID kernel.ActorPrincipalID `json:"actor_principal_id"`
	ProjectID        kernel.ProjectID        `json:"project_id"`
	SessionToken     string                  `json:"session_token"`
	CSRFToken        string                  `json:"csrf_token"`
	ExpiresAt        time.Time               `json:"expires_at"`
}

func BootstrapOperator(ctx context.Context, cfg config.Config, actor kernel.ActorPrincipalID, ttl time.Duration) (OperatorBootstrap, error) {
	if err := runMigrations(ctx, cfg); err != nil {
		return OperatorBootstrap{}, err
	}
	db, err := postgres.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return OperatorBootstrap{}, err
	}
	defer db.Close(context.Background())
	return bootstrapOperatorWithStore(ctx, auth.NewPostgresStore(db.SQL()), time.Now, kernel.ProjectID(cfg.ProjectID), actor, ttl)
}

func bootstrapOperatorWithStore(ctx context.Context, store auth.Store, now func() time.Time, projectID kernel.ProjectID, actor kernel.ActorPrincipalID, ttl time.Duration) (OperatorBootstrap, error) {
	if store == nil || now == nil {
		return OperatorBootstrap{}, fmt.Errorf("operator bootstrap store and clock are required")
	}
	if strings.TrimSpace(string(actor)) == "" || kernel.IsZeroID(projectID) {
		return OperatorBootstrap{}, kernel.InvalidArgument("operator actor and project are required")
	}
	if ttl <= 0 || ttl > maximumOperatorBootstrapTTL {
		return OperatorBootstrap{}, kernel.InvalidArgument("operator bootstrap ttl must be greater than zero and no more than 24h")
	}
	issuedAt := now().UTC()
	authenticator := auth.NewAuthenticator(store, func() time.Time { return issuedAt })
	session, csrf, err := authenticator.IssueOperatorSession(ctx, actor, []kernel.ProjectID{projectID}, ttl)
	if err != nil {
		return OperatorBootstrap{}, err
	}
	return OperatorBootstrap{ActorPrincipalID: actor, ProjectID: projectID, SessionToken: session, CSRFToken: csrf, ExpiresAt: issuedAt.Add(ttl)}, nil
}
