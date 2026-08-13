package phasemcp

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/artifacts"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/phaseagent"
)

func TestHTTPMCPRegistersArtifactAndSubmitsFormalOutput(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "out"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "out", "report.md"), []byte("report"), 0644); err != nil {
		t.Fatal(err)
	}
	registry := NewBindingRegistry()
	runtime := &fakeRuntime{}
	binding := mustIssue(t, registry, runtime, &fakeAgent{}, "invocation-http", "task-http", time.Time{})
	services := registry.bindings[binding.Token]
	services.Binding.WorkspaceRoot = root
	services.Binding.AllowedDirs = []string{"out"}
	registry.bindings[binding.Token] = services
	recorder := &mcpRecorder{}
	handler, _ := NewHandler(registry, artifacts.NewInMemoryRegistry(recorder), recorder)
	server := httptest.NewServer(NewHTTPServer(handler))
	defer server.Close()
	refBody := callHTTPMCP(t, server.URL, binding.Token, "artifact.register", map[string]any{"controlled_path": "out/report.md", "kind": "generated_report", "task_id": "forged"})
	var ref struct {
		ArtifactRef string `json:"artifact_ref"`
	}
	decodeMCPText(t, refBody, &ref)
	if ref.ArtifactRef == "" {
		t.Fatal("artifact ref missing")
	}
	outputBody := callHTTPMCP(t, server.URL, binding.Token, "agent.submitPhaseOutput", phaseagent.PhaseOutput{ReportRef: ref.ArtifactRef})
	var receipt struct {
		Accepted bool `json:"accepted"`
	}
	decodeMCPText(t, outputBody, &receipt)
	if !receipt.Accepted || len(runtime.outputs) != 1 || !recorder.has(artifacts.EventPhaseOutputSubmitted) {
		t.Fatalf("formal output was not submitted: %#v %#v", runtime.outputs, recorder.events)
	}
	if _, err := http.Post(server.URL, "application/json", bytes.NewBufferString(`{}`)); err != nil {
		t.Fatal(err)
	}
}
func callHTTPMCP(t *testing.T, url, token, name string, arguments any) any {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": map[string]any{"name": name, "arguments": arguments}})
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewReader(body))
	req.Header.Set(ExecutionTokenHeader, token)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var response struct {
		Result any `json:"result"`
		Error  any `json:"error"`
	}
	if err := json.NewDecoder(res.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if response.Error != nil {
		t.Fatalf("mcp error %#v", response.Error)
	}
	return response.Result
}
func decodeMCPText(t *testing.T, result any, into any) {
	t.Helper()
	raw, _ := json.Marshal(result)
	var envelope struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil || len(envelope.Content) != 1 {
		t.Fatalf("mcp envelope %s %v", raw, err)
	}
	if err := json.Unmarshal([]byte(envelope.Content[0].Text), into); err != nil {
		t.Fatal(err)
	}
}
