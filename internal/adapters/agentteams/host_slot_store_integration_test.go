package agentteams

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/postgres"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/migrations"
)

func TestHostSlotStoreRealPostgresPreventsOversellAndHashesTokens(t *testing.T) {
	databaseURL := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("THREADMILL_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	admin, err := postgres.Open(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	defer admin.Close(context.Background())

	schema := fmt.Sprintf("tm_agentteams_slots_%d", time.Now().UnixNano())
	if _, err := admin.SQL().ExecContext(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.SQL().ExecContext(context.Background(), `DROP SCHEMA IF EXISTS `+schema+` CASCADE`)
	})
	scopedURL, err := slotTestDatabaseURLWithSearchPath(databaseURL, schema)
	if err != nil {
		t.Fatal(err)
	}
	db, err := postgres.Open(ctx, scopedURL)
	if err != nil {
		t.Fatalf("open scoped postgres: %v", err)
	}
	defer db.Close(context.Background())
	loaded, err := postgres.LoadMigrations(migrations.FS, migrations.Dir)
	if err != nil {
		t.Fatalf("load migrations: %v", err)
	}
	if err := postgres.NewMigrator(db.SQL()).Apply(ctx, loaded); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	executions := NewPostgresExecutionStore(db.SQL())
	slots := NewHostSlotStore(db.SQL())
	first, _, err := executions.Reserve(ctx, "execution://slot/a", "fp-a", AgentTeamsExecutionRef{InvocationID: "inv-slot-a", HostRef: "worker-a"})
	if err != nil {
		t.Fatalf("reserve first: %v", err)
	}
	second, _, err := executions.Reserve(ctx, "execution://slot/b", "fp-b", AgentTeamsExecutionRef{InvocationID: "inv-slot-b", HostRef: "worker-a"})
	if err != nil {
		t.Fatalf("reserve second: %v", err)
	}

	start := make(chan struct{})
	type claimResult struct {
		name string
		err  error
	}
	results := make(chan claimResult, 2)
	var ready sync.WaitGroup
	ready.Add(2)
	go func() {
		ready.Done()
		<-start
		results <- claimResult{name: "first", err: slots.Claim(ctx, "worker-a", "inv-slot-a", "mcp-a", auth.HashOpaqueSecret("plain-token-a"), "token-a")}
	}()
	go func() {
		ready.Done()
		<-start
		results <- claimResult{name: "second", err: slots.Claim(ctx, "worker-a", "inv-slot-b", "mcp-b", auth.HashOpaqueSecret("plain-token-b"), "token-b")}
	}()
	ready.Wait()
	close(start)

	successes := map[string]bool{}
	failures := 0
	for i := 0; i < 2; i++ {
		result := <-results
		if result.err == nil {
			successes[result.name] = true
			continue
		}
		failures++
		if !strings.Contains(result.err.Error(), "already claimed") && !strings.Contains(result.err.Error(), "already") {
			t.Fatalf("claim %s error = %v, want slot conflict", result.name, result.err)
		}
	}
	if len(successes) != 1 || failures != 1 {
		t.Fatalf("claim successes=%v failures=%d, want exactly one winner", successes, failures)
	}
	counts, err := slots.ActiveCounts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if counts["worker-a"] != 1 {
		t.Fatalf("active counts = %v, want worker-a=1", counts)
	}

	winnerTask := first.Execution.AgentTeamsTaskID
	winnerInvocation := first.Execution.InvocationID
	winnerKey := "mcp-a"
	if successes["second"] {
		winnerTask = second.Execution.AgentTeamsTaskID
		winnerInvocation = second.Execution.InvocationID
		winnerKey = "mcp-b"
	}
	var storedKey, storedIdentifier string
	var storedHash []byte
	if err := db.SQL().QueryRowContext(ctx, `
SELECT COALESCE(mcp_client_key, ''), mcp_token_hash, COALESCE(mcp_token_identifier, '')
FROM agentteams_execution_refs
WHERE agentteams_task_id = $1`, winnerTask).Scan(&storedKey, &storedHash, &storedIdentifier); err != nil {
		t.Fatalf("read stored slot metadata: %v", err)
	}
	if storedKey != winnerKey || storedIdentifier == "plain-token-a" || storedIdentifier == "plain-token-b" || len(storedHash) != 32 {
		t.Fatalf("stored slot metadata key=%q identifier=%q hash_len=%d", storedKey, storedIdentifier, len(storedHash))
	}
	if err := slots.MarkRevoked(ctx, "worker-a", winnerInvocation); err != nil {
		t.Fatalf("MarkRevoked() error = %v", err)
	}
	var revoked bool
	if err := db.SQL().QueryRowContext(ctx, `SELECT mcp_revoked_at IS NOT NULL FROM agentteams_execution_refs WHERE agentteams_task_id = $1`, winnerTask).Scan(&revoked); err != nil {
		t.Fatalf("read revoke marker: %v", err)
	}
	if !revoked {
		t.Fatal("mcp_revoked_at was not recorded")
	}
	if err := slots.Release(ctx, winnerTask, "worker-a"); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	if err := slots.Claim(ctx, "worker-a", "inv-slot-b", "mcp-b", auth.HashOpaqueSecret("plain-token-b"), "token-b"); err != nil {
		t.Fatalf("Claim after release error = %v", err)
	}
}

func slotTestDatabaseURLWithSearchPath(raw, schema string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}
