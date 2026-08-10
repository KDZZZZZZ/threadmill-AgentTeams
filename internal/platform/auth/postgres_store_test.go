package auth

import (
	"testing"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

func TestPostgresAuthJSONRoundTripsCanonicalSets(t *testing.T) {
	projectsJSON, err := encodeProjectIDs(map[kernel.ProjectID]struct{}{
		"project-b": {},
		"project-a": {},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(projectsJSON), `["project-a","project-b"]`; got != want {
		t.Fatalf("project JSON = %s, want %s", got, want)
	}
	projects, err := decodeProjectIDs(projectsJSON)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := projects["project-a"]; !ok || len(projects) != 2 {
		t.Fatalf("decoded projects = %#v", projects)
	}

	toolsJSON, err := encodeTools(ToolSet(ToolWorkspaceRead, ToolContextExplore))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(toolsJSON), `["context.explore","workspace.read"]`; got != want {
		t.Fatalf("tool JSON = %s, want %s", got, want)
	}
	tools, err := decodeTools(toolsJSON)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := tools[ToolContextExplore]; !ok || len(tools) != 2 {
		t.Fatalf("decoded tools = %#v", tools)
	}
}

func TestPostgresAuthRejectsInvalidPersistedSetJSON(t *testing.T) {
	if _, err := decodeProjectIDs([]byte(`{"project":"a"}`)); err == nil {
		t.Fatal("decodeProjectIDs() error = nil, want malformed set rejection")
	}
	if _, err := decodeTools([]byte(`{"tool":"context.explore"}`)); err == nil {
		t.Fatal("decodeTools() error = nil, want malformed set rejection")
	}
}

func TestNilPostgresAuthStoreFailsClosed(t *testing.T) {
	var store *PostgresStore
	if err := store.ready(); err == nil {
		t.Fatal("nil postgres auth store was considered ready")
	}
	if err := NewPostgresStore(nil).ready(); err == nil {
		t.Fatal("postgres auth store without DB was considered ready")
	}
}
