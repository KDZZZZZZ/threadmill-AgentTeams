package app

import (
	"context"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

func TestBootstrapOperatorIssuesExplicitSessionAndCSRFOnce(t *testing.T) {
	now := time.Date(2026, time.August, 11, 12, 0, 0, 0, time.UTC)
	store := auth.NewMemoryStore()
	result, err := bootstrapOperatorWithStore(context.Background(), store, func() time.Time { return now }, "project-a", "operator://alice", 8*time.Hour)
	if err != nil {
		t.Fatalf("bootstrapOperatorWithStore() error = %v", err)
	}
	if result.SessionToken == "" || result.CSRFToken == "" || result.SessionToken == result.CSRFToken {
		t.Fatalf("bootstrap result omitted distinct one-time secrets: %#v", result)
	}
	principal, record, err := auth.NewAuthenticator(store, func() time.Time { return now }).AuthenticateOperatorSession(context.Background(), result.SessionToken, "project-a")
	if err != nil {
		t.Fatalf("AuthenticateOperatorSession() error = %v", err)
	}
	if principal.ActorPrincipalID != "operator://alice" || !record.ExpiresAt.Equal(now.Add(8*time.Hour)) {
		t.Fatalf("persisted bootstrap session = principal %#v record %#v", principal, record)
	}
}

func TestBootstrapOperatorRejectsLongLivedCredential(t *testing.T) {
	_, err := bootstrapOperatorWithStore(context.Background(), auth.NewMemoryStore(), time.Now, "project-a", "operator://alice", 25*time.Hour)
	if err == nil {
		t.Fatal("bootstrapOperatorWithStore() error = nil, want ttl rejection")
	}
}
