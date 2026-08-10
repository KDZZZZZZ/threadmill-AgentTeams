package mcpapi

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/contextgraph"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/postgres"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/migrations"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestHTTPHandlerRealPostgresTokenContextToolAndImmediateRevocation(t *testing.T) {
	ctx := context.Background()
	db := openMCPPostgresTestDB(t, ctx)
	defer db.Close()

	now := time.Now().UTC().Truncate(time.Microsecond)
	projectID := kernel.ProjectID("mcp-real-project")
	contextStore := contextgraph.NewPostgresStore(db, func() time.Time { return now })
	curator := auth.Principal{
		ActorPrincipalID: "mcp-real-context-agent",
		Kind:             auth.PrincipalAgent,
		ProjectID:        projectID,
		Role:             auth.RoleContext,
		Operation:        "curate",
		InvocationID:     "mcp-real-curator-invocation",
		Tools:            auth.ToolSet(auth.ToolContextCreateSubgraph),
	}
	created, err := contextStore.CreateSubgraph(ctx, curator, contextgraph.CreateGeneralSubgraphRequest{
		Name:    "Real PostgreSQL MCP context",
		Summary: "proves the HTTP gateway reads an authoritative context store",
	})
	if err != nil {
		t.Fatalf("seed context subgraph: %v", err)
	}

	authenticator := auth.NewAuthenticator(auth.NewPostgresStore(db), func() time.Time { return now })
	token, err := authenticator.IssueAgentToken(ctx, "mcp-real-executor", auth.Capability{
		ProjectID:    projectID,
		TaskID:       "mcp-real-task",
		InvocationID: "mcp-real-executor-invocation",
		Role:         auth.RoleExecutor,
		Tools:        auth.ToolSet(auth.ToolContextListSubgraphs),
		ExpiresAt:    now.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("issue real PostgreSQL invocation token: %v", err)
	}
	registry, err := NewRegistry(ContextReaderToolSpecs(contextStore)...)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewHTTPHandler(authenticator, registry, HTTPOptions{ServerVersion: "postgres-integration"})
	if err != nil {
		t.Fatal(err)
	}

	listed := serveMCP(t, handler, token, "", `{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}`, nil)
	if listed.Code != http.StatusOK {
		t.Fatalf("tools/list status=%d body=%s", listed.Code, listed.Body.String())
	}
	var listResponse struct {
		Result struct {
			Tools []toolDefinition `json:"tools"`
		} `json:"result"`
	}
	decodeRecorderJSON(t, listed, &listResponse)
	if len(listResponse.Result.Tools) != 1 || listResponse.Result.Tools[0].Name != string(auth.ToolContextListSubgraphs) {
		t.Fatalf("real token visible tools = %#v", listResponse.Result.Tools)
	}

	called := serveMCP(t, handler, token, "", `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"context.listSubgraphs","arguments":{}}}`, nil)
	if called.Code != http.StatusOK {
		t.Fatalf("tools/call status=%d body=%s", called.Code, called.Body.String())
	}
	var callResponse struct {
		Result toolCallResult `json:"result"`
	}
	decodeRecorderJSON(t, called, &callResponse)
	if callResponse.Result.IsError || len(callResponse.Result.Content) != 1 {
		t.Fatalf("real context tool result = %#v", callResponse.Result)
	}
	result, ok := callResponse.Result.StructuredContent["result"].([]any)
	if !ok || len(result) != 1 {
		t.Fatalf("real context result payload = %#v", callResponse.Result.StructuredContent)
	}
	item, ok := result[0].(map[string]any)
	if !ok || item["id"] != created.ID {
		t.Fatalf("real context subgraph = %#v, want id %s", result, created.ID)
	}

	if err := authenticator.RevokeAgentToken(ctx, token); err != nil {
		t.Fatalf("revoke real PostgreSQL invocation token: %v", err)
	}
	revoked := serveMCP(t, handler, token, "", `{"jsonrpc":"2.0","id":3,"method":"tools/list","params":{}}`, nil)
	if revoked.Code != http.StatusUnauthorized {
		t.Fatalf("revoked token status=%d body=%s", revoked.Code, revoked.Body.String())
	}
}

func openMCPPostgresTestDB(t *testing.T, ctx context.Context) *sql.DB {
	t.Helper()
	dsn := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://threadmill_test@127.0.0.1:5432/threadmill_test?sslmode=disable"
	}
	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := admin.PingContext(ctx); err != nil {
		_ = admin.Close()
		t.Fatalf("ping real PostgreSQL: %v", err)
	}
	schema := fmt.Sprintf("mcp_http_test_%d", time.Now().UnixNano())
	if _, err := admin.ExecContext(ctx, `CREATE SCHEMA `+schema); err != nil {
		_ = admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.ExecContext(context.Background(), `DROP SCHEMA `+schema+` CASCADE`)
		_ = admin.Close()
	})

	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	query.Set("options", "-c search_path="+schema)
	parsed.RawQuery = query.Encode()
	db, err := sql.Open("pgx", parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	loaded, err := postgres.LoadMigrations(migrations.FS, migrations.Dir)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := postgres.NewMigrator(db).Apply(ctx, loaded); err != nil {
		_ = db.Close()
		t.Fatalf("apply migrations in isolated MCP schema: %v", err)
	}
	return db
}
