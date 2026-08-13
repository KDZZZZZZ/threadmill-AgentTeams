package agentteams

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
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
	if err := slots.Release(ctx, winnerTask, "worker-a"); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("release before revoke error = %v, want forbidden", err)
	}
	if err := slots.MarkRevoked(ctx, winnerTask, "worker-a"); err != nil {
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
	loserInvocation, loserKey, loserToken := first.Execution.InvocationID, "mcp-a", "token-a"
	if successes["first"] {
		loserInvocation, loserKey, loserToken = second.Execution.InvocationID, "mcp-b", "token-b"
	}
	if err := slots.Claim(ctx, "worker-a", winnerInvocation, winnerKey, auth.HashOpaqueSecret("plain-token-winner"), "token-winner"); !kernel.IsCode(err, kernel.CodeNotFound) {
		t.Fatalf("released attempt reclaim error = %v, want not_found", err)
	}
	if err := slots.Claim(ctx, "worker-a", loserInvocation, loserKey, auth.HashOpaqueSecret("plain-"+loserToken), loserToken); err != nil {
		t.Fatalf("Claim after release error = %v", err)
	}
	activeClaims, err := slots.ActiveByHost(ctx, "worker-a")
	if err != nil || len(activeClaims) != 1 || activeClaims[0].InvocationID != loserInvocation {
		t.Fatalf("ActiveByHost() = %#v, %v", activeClaims, err)
	}
	if err := slots.MarkHostFenced(ctx, "worker-a"); err != nil {
		t.Fatalf("MarkHostFenced() error = %v", err)
	}
	loserClaim, ok, err := slots.ByInvocation(ctx, "worker-a", loserInvocation)
	if err != nil || !ok || loserClaim.RevokedAt.IsZero() {
		t.Fatalf("fenced loser claim = %#v, %v, found=%v", loserClaim, err, ok)
	}
	if err := slots.Claim(ctx, "worker-a", loserInvocation, loserKey, auth.HashOpaqueSecret("plain-"+loserToken), loserToken); !kernel.IsCode(err, kernel.CodeExecutorUnavailable) {
		t.Fatalf("fenced active claim reuse error = %v, want executor_unavailable", err)
	}
}

func TestProductionForceStopFenceSerializesClaimRaceRealPostgres(t *testing.T) {
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

	schema := fmt.Sprintf("tm_agentteams_fence_%d", time.Now().UnixNano())
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
	first, _, err := executions.Reserve(ctx, "execution://fence/a", "fp-a", AgentTeamsExecutionRef{InvocationID: "inv-fence-a", HostRef: "worker-a"})
	if err != nil {
		t.Fatalf("reserve first: %v", err)
	}
	second, _, err := executions.Reserve(ctx, "execution://fence/b", "fp-b", AgentTeamsExecutionRef{InvocationID: "inv-fence-b", HostRef: "worker-a"})
	if err != nil {
		t.Fatalf("reserve second: %v", err)
	}
	if err := slots.Claim(ctx, "worker-a", first.Execution.InvocationID, "mcp-a", auth.HashOpaqueSecret("plain-a"), "token-a"); err != nil {
		t.Fatalf("claim first: %v", err)
	}

	fenceStarted := make(chan struct{})
	allowListHosts := make(chan struct{})
	wrappedSlots := &signalingFenceSlots{HostSlotStore: slots, started: fenceStarted}
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/workers":
			<-allowListHosts
			_, _ = w.Write([]byte(`{"workers":[{"name":"worker-a","phase":"Running","runtime":"qwenpaw","lastHeartbeat":"2026-08-11T10:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/managers":
			_, _ = w.Write([]byte(`{"managers":[]}`))
		case r.Method == http.MethodPut && r.URL.Path == "/api/v1/workers/worker-a":
			_, _ = w.Write([]byte(`{}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(controller.Close)
	controllerClient, err := NewAgentTeamsControllerClient(controller.URL, "controller-token", controller.Client())
	if err != nil {
		t.Fatal(err)
	}
	resolver := &staticMCPResolver{}
	client, err := NewProductionClient(ProductionClientOptions{
		Controller: controllerClient, Slots: wrappedSlots, MCPResolver: resolver,
		QwenPaw: staticQwenPawProvider{}, Taskflow: recordingTaskflow{},
		Containers: StaticContainerResolver{"worker-a": "qwenpaw-worker-a"},
	})
	if err != nil {
		t.Fatal(err)
	}

	forceDone := make(chan error, 1)
	go func() {
		forceDone <- client.ForceStopHost(ctx, "worker-a")
	}()
	<-fenceStarted

	lateErr := slots.Claim(ctx, "worker-a", second.Execution.InvocationID, "mcp-b", auth.HashOpaqueSecret("plain-b"), "token-b")
	if !kernel.IsCode(lateErr, kernel.CodeExecutorUnavailable) {
		t.Fatalf("late Claim while fence active = %v, want executor_unavailable", lateErr)
	}
	close(allowListHosts)
	if err := <-forceDone; err != nil {
		t.Fatalf("ForceStopHost() error = %v", err)
	}
	if len(resolver.revoked) != 1 || resolver.revoked[0] != first.Execution.InvocationID {
		t.Fatalf("server token revokes = %#v, want first invocation", resolver.revoked)
	}
	firstClaim, ok, err := slots.ByTaskID(ctx, first.Execution.AgentTeamsTaskID)
	if err != nil || !ok || firstClaim.RevokedAt.IsZero() {
		t.Fatalf("first claim revoked = %#v ok=%v err=%v", firstClaim, ok, err)
	}
	secondClaim, ok, err := slots.ByTaskID(ctx, second.Execution.AgentTeamsTaskID)
	if err != nil || !ok {
		t.Fatalf("second reservation lookup = %#v ok=%v err=%v", secondClaim, ok, err)
	}
	if !secondClaim.ClaimedAt.IsZero() || !secondClaim.RevokedAt.IsZero() {
		t.Fatalf("late failed claim mutated reservation: %#v", secondClaim)
	}
	if err := slots.Release(ctx, first.Execution.AgentTeamsTaskID, "worker-a"); err != nil {
		t.Fatalf("release fenced first claim: %v", err)
	}
	cleared, err := slots.ClearHostFenceIfReusable(ctx, "worker-a")
	if err != nil || !cleared {
		t.Fatalf("clear reusable fence = %v err=%v, want true nil", cleared, err)
	}
	if err := slots.Claim(ctx, "worker-a", second.Execution.InvocationID, "mcp-b", auth.HashOpaqueSecret("plain-b"), "token-b"); err != nil {
		t.Fatalf("claim after ready/release/clear: %v", err)
	}
}

type signalingFenceSlots struct {
	*HostSlotStore
	started chan<- struct{}
	once    sync.Once
}

func (s *signalingFenceSlots) BeginHostFence(ctx context.Context, hostRef string) ([]HostSlotClaim, error) {
	claims, err := s.HostSlotStore.BeginHostFence(ctx, hostRef)
	s.once.Do(func() { close(s.started) })
	return claims, err
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
