//go:build integration

package agentteams

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestTeamHarnessStdioClientRealServer crosses Go -> Python stdio -> the
// unmodified AgentTeams TeamHarness server -> its actual shared task files.
// A local Matrix HTTP fixture supplies only the assignment notification that
// TeamHarness requires before transitioning prepared -> assigned.
func TestTeamHarnessStdioClientRealServer(t *testing.T) {
	python, err := exec.LookPath("python")
	if err != nil {
		t.Skip("python is unavailable")
	}
	endpoint, accessKey, secretKey := os.Getenv("THREADMILL_IT_MINIO_ENDPOINT"), os.Getenv("THREADMILL_IT_MINIO_ACCESS_KEY"), os.Getenv("THREADMILL_IT_MINIO_SECRET_KEY")
	if _, err := exec.LookPath("mc"); err != nil || endpoint == "" || accessKey == "" || secretKey == "" {
		t.Skip("requires fixture: mc on PATH and THREADMILL_IT_MINIO_ENDPOINT/ACCESS_KEY/SECRET_KEY")
	}
	root := filepath.Clean(filepath.Join("..", "..", ".."))
	serverPath := filepath.Join(root, "third_party", "agentteams", "plugins", "teamharness", "mcp", "server.py")
	if _, err := exec.Command(python, "-c", "import sys").Output(); err != nil {
		t.Skip("python cannot run")
	}
	fixture := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(request.URL.Path, "/members") {
			_ = json.NewEncoder(w).Encode(map[string]any{"chunk": []map[string]any{{"state_key": "@worker:test", "content": map[string]any{"membership": "join"}}}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"event_id": "$event"})
	}))
	defer fixture.Close()
	workspace := t.TempDir()
	client := TeamHarnessStdioClient{Python: python, ServerPath: serverPath, Workspace: workspace, Env: []string{"AGENTTEAMS_MATRIX_URL=" + fixture.URL, "AGENTTEAMS_WORKER_MATRIX_TOKEN=test-token", "AGENTTEAMS_FS_ENDPOINT=" + endpoint, "AGENTTEAMS_FS_ACCESS_KEY=" + accessKey, "AGENTTEAMS_FS_SECRET_KEY=" + secretKey, "AGENTTEAMS_FS_BUCKET=threadmill-it", "HTTP_PROXY=", "HTTPS_PROXY=", "NO_PROXY=127.0.0.1,localhost"}}
	request := TeamHarnessDelegateTaskRequest{ProjectID: "project-1", TaskID: "task-1", RoomID: "!room:test", Assignee: "@worker:test", Title: "test", Spec: "actual spec"}
	if err := client.DelegateTask(context.Background(), request); err != nil {
		t.Fatalf("delegate through real server: %v", err)
	}
	spec, err := osReadFile(filepath.Join(workspace, "shared", "tasks", "task-1", "spec.md"))
	if err != nil || strings.TrimSpace(string(spec)) != "actual spec" {
		t.Fatalf("real spec=%q err=%v", spec, err)
	}
	snapshot, err := client.CheckTask(context.Background(), "task-1")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Status != TeamHarnessTaskAssigned || snapshot.TaskID != "task-1" {
		t.Fatalf("real task state=%#v", snapshot)
	}
	if err := client.AcknowledgeTask(context.Background(), "task-1"); err != nil {
		t.Fatalf("worker ack through real server: %v", err)
	}
	snapshot, err = client.CheckTask(context.Background(), "task-1")
	if err != nil || snapshot.Status != TeamHarnessTaskInProgress {
		t.Fatalf("ack state=%#v err=%v", snapshot, err)
	}
	resultPath := filepath.Join(workspace, "shared", "tasks", "task-1", "result.md")
	if err := os.WriteFile(resultPath, []byte("deterministic worker result\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := client.SubmitTask(context.Background(), "task-1", "SUCCESS", "deterministic success", []string{"shared/tasks/task-1/result.md"}); err != nil {
		t.Fatalf("worker submit through real server: %v", err)
	}
	snapshot, err = client.CheckTask(context.Background(), "task-1")
	if err != nil || snapshot.Status != TeamHarnessTaskSubmitted || snapshot.ResultStatus != "SUCCESS" {
		t.Fatalf("submit state=%#v err=%v", snapshot, err)
	}
	// Use a second task/Matrix fixture to exercise cancel because official
	// TeamHarness rejects cancellation of terminal (submitted) tasks.
	fixture2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(request.URL.Path, "/members") {
			_ = json.NewEncoder(w).Encode(map[string]any{"chunk": []map[string]any{{"state_key": "@worker:test", "content": map[string]any{"membership": "join"}}}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"event_id": "$event-2"})
	}))
	defer fixture2.Close()
	cancelClient := client
	cancelClient.Env = append([]string(nil), client.Env...)
	cancelClient.Env[0] = "AGENTTEAMS_MATRIX_URL=" + fixture2.URL
	// Use a second task to exercise cancel because official TeamHarness rejects
	// cancellation of terminal (submitted) tasks.
	if err := cancelClient.DelegateTask(context.Background(), TeamHarnessDelegateTaskRequest{ProjectID: "project-1", TaskID: "task-2", RoomID: "!room:test", Assignee: "@worker:test", Title: "cancel", Spec: "cancel spec"}); err != nil {
		t.Fatal(err)
	}
	if err := client.CancelTask(context.Background(), "task-2", "test cancellation"); err != nil {
		t.Fatal(err)
	}
	snapshot, err = client.CheckTask(context.Background(), "task-2")
	if err != nil || snapshot.Status != TeamHarnessTaskCancelled {
		t.Fatalf("cancel state=%#v err=%v", snapshot, err)
	}
}

func osReadFile(name string) ([]byte, error) { return os.ReadFile(name) }
