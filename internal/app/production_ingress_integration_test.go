package app

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/adapters/agentteams"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/postgres"
	runtimepkg "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime/promptcatalog"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/taskmanager"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/transport/httpapi"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/migrations"
)

func TestProductionIngressPersistsInvocationConversationAndIdempotencyAgainstPostgres(t *testing.T) {
	databaseURL := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("THREADMILL_TEST_DATABASE_URL is required for the real PostgreSQL integration test")
	}
	ctx := context.Background()
	admin, err := postgres.Open(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	defer admin.Close(context.Background())
	schema := fmt.Sprintf("tm_production_ingress_%d", time.Now().UnixNano())
	if _, err := admin.SQL().ExecContext(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.SQL().ExecContext(context.Background(), `DROP SCHEMA IF EXISTS `+schema+` CASCADE`)
	})
	scopedURL, err := productionTestDatabaseURLWithSearchPath(databaseURL, schema)
	if err != nil {
		t.Fatal(err)
	}
	db, err := postgres.Open(ctx, scopedURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(context.Background())
	loaded, err := postgres.LoadMigrations(migrations.FS, migrations.Dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := postgres.NewMigrator(db.SQL()).Apply(ctx, loaded); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	available := make(map[auth.Tool]struct{})
	for _, tool := range auth.CanonicalTools() {
		available[tool] = struct{}{}
	}
	catalog, err := promptcatalog.Load(filepath.Join("..", ".."), available)
	if err != nil {
		t.Fatal(err)
	}
	assembler, err := runtimepkg.NewAssembler(catalog)
	if err != nil {
		t.Fatal(err)
	}
	graph := coordination.NewPostgresStore(db.SQL())
	now := time.Date(2026, time.August, 11, 15, 0, 0, 0, time.UTC)
	managerRuntime, err := newProductionTaskManagerRuntime(db.SQL(), "project-real", graph, func() time.Time { return now.Add(time.Minute) })
	if err != nil {
		t.Fatal(err)
	}
	ingress, err := newProductionIngress(db.SQL(), "project-real", "room-real", assembler, graph, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	decision := taskmanager.TaskManagerDecision{Action: "no_change", Reason: "the graph already reflects the request"}
	dispatcher := &recordingProductionDispatcher{duringDispatch: func(invocationRef string) error {
		principal := productionTestTaskManagerPrincipal(kernel.InvocationID(invocationRef))
		scope := auth.BoundScope{ProjectID: "project-real", InvocationID: principal.InvocationID}
		_, err := managerRuntime.SubmitTaskManagerDecision(ctx, principal, scope, decision)
		return err
	}}
	if err := ingress.setDispatcher(dispatcher); err != nil {
		t.Fatal(err)
	}
	principal := auth.Principal{ActorPrincipalID: "operator-real", Kind: auth.PrincipalOperator, ProjectID: "project-real", Role: auth.RoleOperator}
	req := httpapi.ManagerMessageRequest{RequestID: "request-real", ProjectID: "project-real", ConversationID: "conversation-real", Body: "hold the selected endpoint", SelectedEndpoint: &coordination.PhaseEndpointRef{TaskID: "task-a", EndpointID: coordination.EndpointExecute}}
	first, err := ingress.SubmitManagerMessage(ctx, principal, req)
	if err != nil {
		t.Fatalf("SubmitManagerMessage() error = %v", err)
	}
	second, err := ingress.SubmitManagerMessage(ctx, principal, req)
	if err != nil {
		t.Fatalf("idempotent SubmitManagerMessage() error = %v", err)
	}
	if first != second || dispatcher.calls != 1 {
		t.Fatalf("idempotent response first=%#v second=%#v dispatches=%d", first, second, dispatcher.calls)
	}
	conversation, err := ingress.Conversation(ctx, principal, "conversation-real", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(conversation.Messages) != 2 || conversation.Messages[0].ManagerInputRef != first.ManagerInputRef || conversation.Messages[1].DecisionRef == "" {
		t.Fatalf("persisted conversation = %#v", conversation)
	}
	var role, status string
	if err := db.SQL().QueryRowContext(ctx, `SELECT role, status FROM runtime_invocations WHERE invocation_id=$1`, first.InvocationRef).Scan(&role, &status); err != nil {
		t.Fatal(err)
	}
	if role != string(auth.RoleTaskManager) || status != string(runtimepkg.InvocationCompleted) {
		t.Fatalf("persisted invocation role=%q status=%q", role, status)
	}
	managerPrincipal := productionTestTaskManagerPrincipal(first.InvocationRef)
	managerScope := auth.BoundScope{ProjectID: "project-real", InvocationID: first.InvocationRef}
	decisionRef, err := managerRuntime.SubmitTaskManagerDecision(ctx, managerPrincipal, managerScope, decision)
	if err != nil {
		t.Fatalf("SubmitTaskManagerDecision() error = %v", err)
	}
	if replayed, err := managerRuntime.SubmitTaskManagerDecision(ctx, managerPrincipal, managerScope, decision); err != nil || replayed != decisionRef {
		t.Fatalf("idempotent decision replay ref=%q error=%v", replayed, err)
	}
	changedDecision := decision
	changedDecision.Reason = "different reason"
	if _, err := managerRuntime.SubmitTaskManagerDecision(ctx, managerPrincipal, managerScope, changedDecision); !kernel.IsCode(err, kernel.CodeIdempotencyConflict) {
		t.Fatalf("changed decision replay error = %v, want idempotency_conflict", err)
	}
	var appliedRevision kernel.Revision
	if err := db.SQL().QueryRowContext(ctx, `SELECT applied_graph_revision FROM production_taskmanager_bindings WHERE invocation_id=$1`, first.InvocationRef).Scan(&appliedRevision); err != nil {
		t.Fatal(err)
	}
	if appliedRevision != 1 {
		t.Fatalf("terminal decision applied revision = %d, want 1", appliedRevision)
	}
	conflict := req
	conflict.Body = "different payload"
	if _, err := ingress.SubmitManagerMessage(ctx, principal, conflict); !kernel.IsCode(err, kernel.CodeIdempotencyConflict) {
		t.Fatalf("conflicting replay error = %v, want idempotency_conflict", err)
	}
}

type recordingProductionDispatcher struct {
	calls          int
	duringDispatch func(string) error
}

func (r *recordingProductionDispatcher) Dispatch(_ context.Context, invocationRef string) (agentteams.AgentTeamsExecutionRef, error) {
	r.calls++
	if r.duringDispatch != nil {
		if err := r.duringDispatch(invocationRef); err != nil {
			return agentteams.AgentTeamsExecutionRef{}, err
		}
	}
	return agentteams.AgentTeamsExecutionRef{InvocationID: kernel.InvocationID(invocationRef), AgentTeamsTaskID: "task-real", HostRef: "manager-real"}, nil
}

func productionTestTaskManagerPrincipal(invocationID kernel.InvocationID) auth.Principal {
	return auth.Principal{
		ActorPrincipalID: "task-manager-real", Kind: auth.PrincipalAgent, ProjectID: "project-real", Role: auth.RoleTaskManager, InvocationID: invocationID,
		Tools: auth.ToolSet(auth.ToolCoordinationSnapshot, auth.ToolTaskManagerSubmitDecision, auth.ToolCoordinationReplacePending, auth.ToolCoordinationTransition),
	}
}

func productionTestDatabaseURLWithSearchPath(databaseURL, schema string) (string, error) {
	parsed, err := url.Parse(databaseURL)
	if err != nil {
		return "", err
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}
