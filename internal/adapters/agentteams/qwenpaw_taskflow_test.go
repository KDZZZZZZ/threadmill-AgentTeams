package agentteams

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

func TestTaskflowArgumentsExposeOnlyLeaderLifecycleActions(t *testing.T) {
	arguments, err := taskflowArguments(TaskflowCall{
		Action:     "delegate_task",
		ProjectID:  "project-a",
		TaskID:     "task-a",
		RoomID:     "!team:example.test",
		AssignedTo: "worker-a",
		Spec:       "perform the bounded invocation",
	})
	if err != nil {
		t.Fatal(err)
	}
	if arguments["role"] != "leader" || arguments["action"] != "delegate_task" {
		t.Fatalf("arguments = %#v", arguments)
	}
	if len(arguments) != 7 {
		t.Fatalf("delegate arguments contain unexpected fields: %#v", arguments)
	}

	for _, action := range []string{"ack_task", "submit_task", "projectflow"} {
		_, err := taskflowArguments(TaskflowCall{Action: action, TaskID: "task-a"})
		if err == nil {
			t.Fatalf("unsupported direct action %q accepted", action)
		}
	}
	if _, err := taskflowArguments(TaskflowCall{Action: "cancel_task", TaskID: "task-a"}); err == nil {
		t.Fatal("cancel_task without reason accepted")
	}
	if _, err := taskflowArguments(TaskflowCall{Action: "check_task", TaskID: "../task-a"}); err == nil {
		t.Fatal("unsafe provider task id accepted")
	}
}

func TestTaskCarrierProjectIDIsStableOpaqueAndPerThreadmillProject(t *testing.T) {
	first := taskCarrierProjectID("project-a", "task-a")
	if first != taskCarrierProjectID("project-a", "task-a") {
		t.Fatal("carrier id is not stable")
	}
	if first == taskCarrierProjectID("project-b", "task-a") {
		t.Fatal("carrier id aliases different Threadmill projects")
	}
	if !strings.HasPrefix(first, "threadmill-carrier-") || strings.Contains(first, "project-a") {
		t.Fatalf("carrier id is not opaque: %q", first)
	}
}

func TestMatrixUserIDForWorkerUsesSourceRoomServer(t *testing.T) {
	userID, err := matrixUserIDForWorker("!team:matrix-local.agentteams.io:18080", "threadmill-manager")
	if err != nil {
		t.Fatal(err)
	}
	if userID != "@threadmill-manager:matrix-local.agentteams.io:18080" {
		t.Fatalf("worker Matrix user ID = %q", userID)
	}
	for _, invalid := range []struct{ room, worker string }{
		{"team-room", "threadmill-manager"},
		{"!team:matrix.example.test/path", "threadmill-manager"},
		{"!team:matrix.example.test", "../manager"},
	} {
		if _, err := matrixUserIDForWorker(invalid.room, invalid.worker); !kernel.IsCode(err, kernel.CodeInvalidRequest) {
			t.Fatalf("matrixUserIDForWorker(%q, %q) error = %v", invalid.room, invalid.worker, err)
		}
	}
}

func TestParseCarrierPayloadKeepsProviderErrorPrivateToTransportLogic(t *testing.T) {
	provider := `{"ok":false,"tool":"projectflow","action":"resolve_project","error":"task not found"}`
	outer, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": 1,
		"result": map[string]any{"content": []map[string]string{{"type": "text", "text": provider}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := parseCarrierPayload(outer)
	if err != nil {
		t.Fatal(err)
	}
	if payload.OK || payload.Action != "resolve_project" || payload.ProviderError != "task not found" {
		t.Fatalf("payload = %#v", payload)
	}
}

func TestParseTaskflowCallResultPreservesUntrustedProviderFacts(t *testing.T) {
	payload := map[string]any{
		"ok":        true,
		"action":    "check_task",
		"effective": true,
		"task": map[string]any{
			"task_id":       "provider-task-a",
			"project_id":    "project-a",
			"assigned_to":   "worker-a",
			"status":        "submitted",
			"eventId":       "$matrix-event",
			"result_path":   "shared/tasks/provider-task-a/result.md",
			"result_status": "SUCCESS",
			"summary":       "provider says complete",
			"deliverables":  []string{"deliverables/report.md"},
		},
		"validationErrors": []string{},
	}
	text, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	outer, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"result": map[string]any{
			"content": []map[string]string{{"type": "text", "text": string(text)}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := parseTaskflowCallResult(outer)
	if err != nil {
		t.Fatal(err)
	}
	if !result.OK || result.Action != "check_task" || result.Task.TaskID != "provider-task-a" ||
		result.Task.HostRef != "worker-a" || result.ResultStatus != "SUCCESS" || !result.Effective {
		t.Fatalf("result = %#v", result)
	}
	if result.Summary != "provider says complete" || len(result.Deliverables) != 1 {
		t.Fatalf("result details = %#v", result)
	}
}

func TestParseTaskflowCallResultRejectsTransportErrorsWithoutEcho(t *testing.T) {
	secret := "secret-provider-body"
	_, err := parseTaskflowCallResult([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"` + secret + `"}}`))
	if !kernel.IsCode(err, kernel.CodeExecutorUnavailable) {
		t.Fatalf("error = %v, want executor_unavailable", err)
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("transport error leaked provider text: %v", err)
	}
}

func TestParseTaskflowCallResultRejectsProviderErrorWithoutEcho(t *testing.T) {
	secret := "provider-path-or-secret"
	provider, err := json.Marshal(map[string]any{
		"ok": false, "action": "check_task", "error": secret, "retryable": true,
		"task": map[string]string{"task_id": "task-a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	outer, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": 1,
		"result": map[string]any{"content": []map[string]string{{"type": "text", "text": string(provider)}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = parseTaskflowCallResult(outer)
	if !kernel.IsCode(err, kernel.CodeExecutorUnavailable) {
		t.Fatalf("error = %v, want executor_unavailable", err)
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("provider error leaked provider text: %v", err)
	}
}

func TestQwenPawDockerTaskflowRejectsUnsafeContainerBeforeExecution(t *testing.T) {
	caller, err := NewQwenPawDockerTaskflow("definitely-not-a-real-docker", "")
	if err != nil {
		t.Fatal(err)
	}
	_, err = caller.Call(context.Background(), "worker;Remove-Item", TaskflowCall{
		Action: "check_task",
		TaskID: "task-a",
	})
	if err == nil {
		t.Fatal("unsafe container name accepted")
	}
}

func TestTeamHarnessBridgePassesOnlyExplicitEnvironmentAllowlist(t *testing.T) {
	for _, forbidden := range []string{
		"env = dict(os.environ)",
		"env.update(pid1_env)",
		"os.environ.setdefault",
		"OPENAI_API_KEY",
		"DEEPSEEK_API_KEY",
	} {
		if strings.Contains(qwenPawTeamHarnessBridge, forbidden) {
			t.Fatalf("TeamHarness bridge contains forbidden environment propagation %q", forbidden)
		}
	}
	for _, required := range []string{"base_env_keys", "plugin_env_keys", "env=env"} {
		if !strings.Contains(qwenPawTeamHarnessBridge, required) {
			t.Fatalf("TeamHarness bridge is missing explicit environment boundary %q", required)
		}
	}
}

func TestCappedOutputReportsTruncation(t *testing.T) {
	buffer := newCappedOutput(4)
	if n, err := buffer.Write([]byte("abcdef")); err != nil || n != 6 {
		t.Fatalf("Write = %d, %v", n, err)
	}
	if !buffer.truncated || buffer.String() != "abcd" {
		t.Fatalf("buffer = %q truncated=%t", buffer.String(), buffer.truncated)
	}
}
