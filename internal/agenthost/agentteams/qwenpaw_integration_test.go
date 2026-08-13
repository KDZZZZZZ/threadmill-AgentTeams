//go:build integration

package agentteams

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"testing"
	"time"

	phasemcp "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/mcp/phase"
)

// TestQwenPawMCPInjectorRealAPI runs only against an externally started,
// official QwenPaw app. It deliberately has no HTTP fake fallback.
func TestQwenPawMCPInjectorRealAPI(t *testing.T) {
	baseURL := os.Getenv("THREADMILL_IT_QWENPAW_URL")
	if baseURL == "" {
		t.Skip("requires official QwenPaw app at THREADMILL_IT_QWENPAW_URL")
	}
	injector := QwenPawMCPInjector{BaseURL: baseURL, PhaseMCPURL: "http://127.0.0.1:18080/mcp"}
	binding := phasemcp.ExecutionBinding{Token: "opaque-integration-token", ToolNames: []string{"agent.submitPhaseOutput", "artifact.register"}}
	executionID := "qwenpaw-it-" + time.Now().UTC().Format("20060102150405.000000000")
	ctx := context.Background()
	if err := injector.InjectPhaseMCP(ctx, executionID, binding); err != nil {
		t.Fatalf("create/policy through real QwenPaw API: %v", err)
	}
	key := "threadmill-" + taskflowSafeID(executionID)
	client := getJSON(t, baseURL+"/api/mcp/"+key)
	if client["url"] != injector.PhaseMCPURL || client["enabled"] != true || client["transport"] != "streamable_http" {
		t.Fatalf("unexpected real MCP client: %#v", client)
	}
	policy := getJSON(t, baseURL+"/api/mcp/policy/"+key)
	if policy["default_effect"] != "deny" {
		t.Fatalf("unexpected real policy: %#v", policy)
	}
	if err := injector.CleanupPhaseMCP(ctx, executionID, binding); err != nil {
		t.Fatalf("delete through real QwenPaw API: %v", err)
	}
	response, err := http.Get(baseURL + "/api/mcp/" + key)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("MCP client still exists: %s", response.Status)
	}
}
func getJSON(t *testing.T, url string) map[string]any {
	t.Helper()
	response, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode/100 != 2 {
		t.Fatalf("GET %s: %s", url, response.Status)
	}
	var out map[string]any
	if err := json.NewDecoder(response.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out
}
