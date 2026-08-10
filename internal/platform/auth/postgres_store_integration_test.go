package auth

import (
	"context"
	"crypto/sha256"
	"os"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/postgres"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/migrations"
)

const authIntegrationDatabaseEnv = "THREADMILL_TEST_DATABASE_URL"

func TestPostgresStoreRealDatabaseRoundTripAndRevocation(t *testing.T) {
	databaseURL := os.Getenv(authIntegrationDatabaseEnv)
	if databaseURL == "" {
		t.Skip(authIntegrationDatabaseEnv + " is required for the real PostgreSQL integration test")
	}
	ctx := context.Background()
	db, err := postgres.Open(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open PostgreSQL: %v", err)
	}
	t.Cleanup(func() { _ = db.Close(context.Background()) })
	loaded, err := postgres.LoadMigrations(migrations.FS, migrations.Dir)
	if err != nil {
		t.Fatalf("load migrations: %v", err)
	}
	if err := postgres.NewMigrator(db.SQL()).Apply(ctx, loaded); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	store := NewPostgresStore(db.SQL())
	suffix := time.Now().UTC().Format("20060102T150405.000000000")
	sessionHash := sha256.Sum256([]byte("session-" + suffix))
	tokenHash := sha256.Sum256([]byte("token-" + suffix))
	t.Cleanup(func() {
		_, _ = db.SQL().ExecContext(context.Background(), "DELETE FROM operator_sessions WHERE session_hash = $1", sessionHash[:])
		_, _ = db.SQL().ExecContext(context.Background(), "DELETE FROM agent_invocation_tokens WHERE token_hash = $1", tokenHash[:])
	})

	now := time.Now().UTC().Truncate(time.Microsecond)
	session := SessionRecord{
		SessionHash:      sessionHash[:],
		ActorPrincipalID: "operator-real-pg",
		ProjectIDs: map[kernel.ProjectID]struct{}{
			"project-a": {},
			"project-b": {},
		},
		CSRFHash:  []byte("csrf-hash-real-pg"),
		ExpiresAt: now.Add(time.Hour),
	}
	if err := store.PutSession(ctx, session); err != nil {
		t.Fatalf("PutSession: %v", err)
	}
	loadedSession, ok, err := store.SessionByHash(ctx, sessionHash[:])
	if err != nil || !ok {
		t.Fatalf("SessionByHash ok=%v err=%v", ok, err)
	}
	if loadedSession.ActorPrincipalID != session.ActorPrincipalID || len(loadedSession.ProjectIDs) != 2 {
		t.Fatalf("loaded session = %#v", loadedSession)
	}
	revokedAt := now.Add(time.Minute)
	if err := store.RevokeSession(ctx, sessionHash[:], revokedAt); err != nil {
		t.Fatalf("RevokeSession: %v", err)
	}
	if err := store.RevokeSession(ctx, sessionHash[:], revokedAt.Add(time.Minute)); err != nil {
		t.Fatalf("idempotent RevokeSession: %v", err)
	}
	loadedSession, ok, err = store.SessionByHash(ctx, sessionHash[:])
	if err != nil || !ok || loadedSession.RevokedAt == nil || !loadedSession.RevokedAt.Equal(revokedAt) {
		t.Fatalf("revoked session ok=%v err=%v record=%#v", ok, err, loadedSession)
	}

	token := TokenRecord{
		TokenHash:        tokenHash[:],
		ActorPrincipalID: "context-real-pg",
		Capability: Capability{
			ProjectID:            "project-a",
			InvocationID:         "invocation-context-real-pg",
			ConsumerInvocationID: "invocation-executor-real-pg",
			ConsumerTaskID:       "task-real-pg",
			ConsumerRole:         RoleExecutor,
			Role:                 RoleContext,
			Operation:            "retrieve",
			Tools:                ToolSet(ToolContextSearch, ToolContextGetNode),
			ExpiresAt:            now.Add(time.Hour),
		},
		ExpiresAt: now.Add(time.Hour),
	}
	if err := store.PutToken(ctx, token); err != nil {
		t.Fatalf("PutToken: %v", err)
	}
	loadedToken, ok, err := store.TokenByHash(ctx, tokenHash[:])
	if err != nil || !ok {
		t.Fatalf("TokenByHash ok=%v err=%v", ok, err)
	}
	if loadedToken.Capability.ConsumerInvocationID != token.Capability.ConsumerInvocationID ||
		loadedToken.Capability.ConsumerTaskID != token.Capability.ConsumerTaskID ||
		loadedToken.Capability.ConsumerRole != token.Capability.ConsumerRole ||
		loadedToken.Capability.Operation != token.Capability.Operation ||
		len(loadedToken.Capability.Tools) != len(token.Capability.Tools) {
		t.Fatalf("loaded token capability = %#v", loadedToken.Capability)
	}
	if err := store.RevokeToken(ctx, tokenHash[:], revokedAt); err != nil {
		t.Fatalf("RevokeToken: %v", err)
	}
	loadedToken, ok, err = store.TokenByHash(ctx, tokenHash[:])
	if err != nil || !ok || loadedToken.RevokedAt == nil || !loadedToken.RevokedAt.Equal(revokedAt) {
		t.Fatalf("revoked token ok=%v err=%v record=%#v", ok, err, loadedToken)
	}
}
