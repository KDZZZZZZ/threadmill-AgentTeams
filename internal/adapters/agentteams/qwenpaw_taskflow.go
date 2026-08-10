package agentteams

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os/exec"
	"strings"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

const qwenPawTeamHarnessBridge = `
import json
import os
import subprocess
import sys
from pathlib import Path
from urllib.request import urlopen

pid1_env = {}
for item in Path("/proc/1/environ").read_bytes().split(b"\0"):
    if item and b"=" in item:
        key, value = item.split(b"=", 1)
        pid1_env[key.decode()] = value.decode()

with urlopen("http://127.0.0.1:8088/api/mcp/teamharness", timeout=10) as response:
    client = json.load(response)

cwd = Path(client["cwd"])
working_dir = cwd.parents[2]
workspace = working_dir / "workspaces" / "default"
derived_env = {
    "QWENPAW_WORKING_DIR": str(working_dir),
    "TEAMHARNESS_RUNTIME_CONFIG": str(working_dir.parent / "runtime" / "runtime.yaml"),
    "TEAMHARNESS_SHARED_DIR": str((workspace / "shared").resolve()),
    "AGENTTEAMS_STORAGE_PREFIX": f"agentteams/{os.environ.get('AGENTTEAMS_FS_BUCKET', 'agentteams-storage')}",
}
base_env_keys = {
    "HOME",
    "LANG",
    "LC_ALL",
    "PATH",
    "PYTHONHOME",
    "PYTHONPATH",
    "REQUESTS_CA_BUNDLE",
    "SSL_CERT_DIR",
    "SSL_CERT_FILE",
    "TMPDIR",
    "TZ",
}
plugin_env_keys = {
    "TEAMHARNESS_RUNTIME_CONFIG",
    "TEAMHARNESS_SHARED_DIR",
    "AGENTTEAMS_MATRIX_URL",
    "AGENTTEAMS_WORKER_MATRIX_TOKEN",
    "AGENTTEAMS_MATRIX_USER_ID",
    "AGENTTEAMS_WORKER_ROLE",
    "AGENTTEAMS_AGENT_ROLE",
    "AGENTTEAMS_WORKER_NAME",
    "AGENTTEAMS_STORAGE_PREFIX",
    "AGENTTEAMS_SHARED_STORAGE_PREFIX",
    "AGENTTEAMS_FS_BUCKET",
    "AGENTTEAMS_FS_ENDPOINT",
    "AGENTTEAMS_FS_ACCESS_KEY",
    "AGENTTEAMS_FS_SECRET_KEY",
    "QWENPAW_WORKING_DIR",
}
source_env = dict(pid1_env)
source_env.update(os.environ)
env = {
    key: source_env[key]
    for key in base_env_keys
    if source_env.get(key)
}
for key, value in (client.get("env") or {}).items():
    key = str(key)
    value = str(value)
    if key not in plugin_env_keys:
        continue
    if key in source_env:
        env[key] = source_env[key]
    elif key in derived_env:
        env[key] = derived_env[key]
    elif "*" not in value:
        env[key] = value

request = sys.stdin.read()
proc = subprocess.run(
    [client["command"], *client.get("args", [])],
    input=request,
    text=True,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    cwd=client.get("cwd") or None,
    env=env,
    timeout=60,
)
if proc.returncode != 0:
    raise SystemExit(proc.returncode)
lines = [line for line in proc.stdout.splitlines() if line.strip()]
if not lines:
    raise SystemExit(2)
response = json.loads(lines[-1])
print(json.dumps(response, ensure_ascii=False))
`

type TaskflowCall struct {
	Action     string
	ProjectID  string
	TaskID     string
	RoomID     string
	AssignedTo string
	Spec       string
	Reason     string
}

type TaskflowCallResult struct {
	OK               bool
	Action           string
	Task             TaskSnapshot
	ResultStatus     string
	Summary          string
	Deliverables     []string
	Effective        bool
	ValidationErrors []string
	Retryable        bool
}

// QwenPawDockerTaskflow calls the built-in TeamHarness MCP inside an actual
// QwenPaw container. It uses docker exec without a shell and never imports or
// modifies third_party code.
type QwenPawDockerTaskflow struct {
	dockerBinary string
	pythonBinary string
	outputLimit  int
}

func NewQwenPawDockerTaskflow(dockerBinary, pythonBinary string) (*QwenPawDockerTaskflow, error) {
	dockerBinary = strings.TrimSpace(dockerBinary)
	pythonBinary = strings.TrimSpace(pythonBinary)
	if dockerBinary == "" {
		dockerBinary = "docker"
	}
	if pythonBinary == "" {
		pythonBinary = "/opt/venv/qwenpaw/bin/python"
	}
	return &QwenPawDockerTaskflow{
		dockerBinary: dockerBinary,
		pythonBinary: pythonBinary,
		outputLimit:  1 << 20,
	}, nil
}

func (c *QwenPawDockerTaskflow) Call(ctx context.Context, container string, call TaskflowCall) (TaskflowCallResult, error) {
	container = strings.TrimSpace(container)
	if !safeContainerName(container) {
		return TaskflowCallResult{}, kernel.InvalidArgument("QwenPaw container name is invalid")
	}
	arguments, err := taskflowArguments(call)
	if err != nil {
		return TaskflowCallResult{}, err
	}
	if call.Action == "delegate_task" {
		carrierID, err := c.ensureCarrier(ctx, container, call)
		if err != nil {
			return TaskflowCallResult{}, err
		}
		arguments["projectId"] = carrierID
	}
	raw, err := c.executeMCP(ctx, container, "taskflow", arguments)
	if err != nil {
		return TaskflowCallResult{}, err
	}
	result, err := parseTaskflowCallResult(raw)
	if err != nil {
		return TaskflowCallResult{}, err
	}
	if result.Action != call.Action || result.Task.TaskID != call.TaskID {
		return TaskflowCallResult{}, kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "QwenPaw TeamHarness returned a mismatched task identity", Recoverable: true}
	}
	return result, nil
}

func (c *QwenPawDockerTaskflow) executeMCP(
	ctx context.Context,
	container string,
	tool string,
	arguments map[string]any,
) ([]byte, error) {
	request, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "tools/call",
		"params": map[string]any{
			"name":      tool,
			"arguments": arguments,
		},
	})
	if err != nil {
		return nil, kernel.InvalidArgument("TeamHarness request cannot be encoded")
	}
	if len(request) > 2<<20 {
		return nil, kernel.InvalidArgument("TeamHarness request exceeds the size limit")
	}
	request = append(request, '\n')

	cmd := exec.CommandContext(
		ctx,
		c.dockerBinary,
		"exec", "-i", container,
		c.pythonBinary, "-c", qwenPawTeamHarnessBridge,
	)
	cmd.WaitDelay = 5 * time.Second
	cmd.Stdin = bytes.NewReader(request)
	stdout := newCappedOutput(c.outputLimit)
	stderr := newCappedOutput(64 << 10)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "QwenPaw TeamHarness process failed", Recoverable: true}
	}
	if stdout.truncated {
		return nil, kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "QwenPaw TeamHarness response exceeded the limit", Recoverable: true}
	}
	return append([]byte(nil), stdout.Bytes()...), nil
}

func (c *QwenPawDockerTaskflow) ensureCarrier(ctx context.Context, container string, call TaskflowCall) (string, error) {
	carrierID := taskCarrierProjectID(call.ProjectID, call.TaskID)
	resolved, err := c.executeMCP(ctx, container, "projectflow", map[string]any{
		"role":   "leader",
		"action": "resolve_project",
		"taskId": call.TaskID,
	})
	if err != nil {
		return "", err
	}
	payload, err := parseCarrierPayload(resolved)
	if err != nil {
		return "", err
	}
	if payload.OK {
		if payload.Action != "resolve_project" || payload.Task.TaskID != call.TaskID || payload.Task.ProjectID != carrierID {
			return "", kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "TeamHarness carrier readback mismatch", Recoverable: true}
		}
		return carrierID, nil
	}
	if payload.ProviderError != "task not found" {
		return "", kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "TeamHarness carrier lookup failed", Recoverable: true}
	}

	created, err := c.executeMCP(ctx, container, "projectflow", map[string]any{
		"role":      "leader",
		"action":    "create_project",
		"projectId": carrierID,
		"title":     "Threadmill execution carrier",
		"source":    "threadmill-runtime",
	})
	if err != nil {
		return "", err
	}
	createdPayload, err := parseCarrierPayload(created)
	if err != nil {
		return "", err
	}
	// Another dispatcher can win the same idempotent carrier race. A precise
	// "already exists" response is safe to continue; all other provider text
	// remains untrusted and is not surfaced.
	if !createdPayload.OK && createdPayload.ProviderError != "project already exists: "+carrierID {
		return "", kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "TeamHarness carrier creation failed", Recoverable: true}
	}

	planned, err := c.executeMCP(ctx, container, "projectflow", map[string]any{
		"role":      "leader",
		"action":    "plan_dag",
		"projectId": carrierID,
		"tasks": []map[string]any{{
			"taskId":     call.TaskID,
			"title":      "Threadmill bounded execution",
			"assignedTo": call.AssignedTo,
			"dependsOn":  []string{},
		}},
	})
	if err != nil {
		return "", err
	}
	plannedPayload, err := parseCarrierPayload(planned)
	if err != nil {
		return "", err
	}
	if !plannedPayload.OK || plannedPayload.Action != "plan_dag" {
		return "", kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "TeamHarness carrier planning failed", Recoverable: true}
	}
	return carrierID, nil
}

func taskCarrierProjectID(projectID, taskID string) string {
	sum := sha256.Sum256([]byte(projectID + "\x00" + taskID))
	return "threadmill-carrier-" + hex.EncodeToString(sum[:12])
}

func taskflowArguments(call TaskflowCall) (map[string]any, error) {
	call.Action = strings.TrimSpace(call.Action)
	if call.Action != "delegate_task" && call.Action != "check_task" && call.Action != "cancel_task" {
		return nil, kernel.InvalidArgument("taskflow action is invalid")
	}
	if !safeProviderID(call.TaskID) {
		return nil, kernel.InvalidArgument("taskflow task_id must be a safe provider id")
	}
	arguments := map[string]any{
		"role":   "leader",
		"action": call.Action,
		"taskId": call.TaskID,
	}
	switch call.Action {
	case "delegate_task":
		if strings.TrimSpace(call.ProjectID) == "" || strings.TrimSpace(call.RoomID) == "" ||
			strings.TrimSpace(call.AssignedTo) == "" || strings.TrimSpace(call.Spec) == "" {
			return nil, kernel.InvalidArgument("delegate_task project, room, assignee, and spec are required")
		}
		if len(call.ProjectID) > 512 || len(call.RoomID) > 512 || len(call.AssignedTo) > 512 || len(call.Spec) > 1<<20 {
			return nil, kernel.InvalidArgument("delegate_task field exceeds the size limit")
		}
		arguments["projectId"] = call.ProjectID
		arguments["roomId"] = call.RoomID
		arguments["assignedTo"] = call.AssignedTo
		arguments["spec"] = call.Spec
	case "cancel_task":
		if strings.TrimSpace(call.Reason) == "" {
			return nil, kernel.InvalidArgument("cancel_task reason is required")
		}
		if len(call.Reason) > 4096 {
			return nil, kernel.InvalidArgument("cancel_task reason exceeds the size limit")
		}
		arguments["reason"] = call.Reason
	}
	return arguments, nil
}

func parseTaskflowCallResult(raw []byte) (TaskflowCallResult, error) {
	text, err := mcpTextPayload(raw)
	if err != nil {
		return TaskflowCallResult{}, err
	}
	var payload struct {
		OK        bool   `json:"ok"`
		Action    string `json:"action"`
		Retryable bool   `json:"retryable"`
		Effective bool   `json:"effective"`
		Task      struct {
			TaskID       string   `json:"task_id"`
			ProjectID    string   `json:"project_id"`
			AssignedTo   string   `json:"assigned_to"`
			Status       string   `json:"status"`
			EventID      string   `json:"eventId"`
			ResultPath   string   `json:"result_path"`
			ResultStatus string   `json:"result_status"`
			Summary      string   `json:"summary"`
			Deliverables []string `json:"deliverables"`
		} `json:"task"`
		Result struct {
			Status       string   `json:"status"`
			Summary      string   `json:"summary"`
			Deliverables []string `json:"deliverables"`
		} `json:"result"`
		ValidationErrors []string `json:"validationErrors"`
	}
	if err := json.Unmarshal([]byte(text), &payload); err != nil {
		return TaskflowCallResult{}, kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "QwenPaw TeamHarness returned an invalid taskflow payload", Recoverable: true}
	}
	resultStatus := payload.Result.Status
	if resultStatus == "" {
		resultStatus = payload.Task.ResultStatus
	}
	summary := payload.Result.Summary
	if summary == "" {
		summary = payload.Task.Summary
	}
	deliverables := payload.Result.Deliverables
	if len(deliverables) == 0 {
		deliverables = payload.Task.Deliverables
	}
	result := TaskflowCallResult{
		OK:               payload.OK,
		Action:           payload.Action,
		Task:             TaskSnapshot{TaskID: payload.Task.TaskID, ProjectID: kernel.ProjectID(payload.Task.ProjectID), HostRef: payload.Task.AssignedTo, Status: payload.Task.Status, EventID: payload.Task.EventID, ResultPath: payload.Task.ResultPath},
		ResultStatus:     resultStatus,
		Summary:          summary,
		Deliverables:     append([]string(nil), deliverables...),
		Effective:        payload.Effective,
		ValidationErrors: append([]string(nil), payload.ValidationErrors...),
		Retryable:        payload.Retryable,
	}
	if !payload.OK {
		return TaskflowCallResult{}, kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "QwenPaw TeamHarness rejected the taskflow action", Recoverable: payload.Retryable}
	}
	return result, nil
}

func mcpTextPayload(raw []byte) (string, error) {
	var response struct {
		Error *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
		Result struct {
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		} `json:"result"`
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return "", kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "QwenPaw TeamHarness returned invalid JSON-RPC", Recoverable: true}
	}
	if response.Error != nil {
		return "", kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "QwenPaw TeamHarness returned a JSON-RPC error", Recoverable: true}
	}
	var text string
	for _, content := range response.Result.Content {
		if content.Type == "text" && strings.TrimSpace(content.Text) != "" {
			text = content.Text
			break
		}
	}
	if text == "" {
		return "", kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "QwenPaw TeamHarness returned no tool payload", Recoverable: true}
	}
	return text, nil
}

type carrierPayload struct {
	OK            bool   `json:"ok"`
	Action        string `json:"action"`
	ProviderError string `json:"error"`
	Task          struct {
		TaskID    string `json:"task_id"`
		ProjectID string `json:"project_id"`
	} `json:"task"`
}

func parseCarrierPayload(raw []byte) (carrierPayload, error) {
	text, err := mcpTextPayload(raw)
	if err != nil {
		return carrierPayload{}, err
	}
	var payload carrierPayload
	if err := json.Unmarshal([]byte(text), &payload); err != nil {
		return carrierPayload{}, kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "QwenPaw TeamHarness returned an invalid carrier payload", Recoverable: true}
	}
	return payload, nil
}

func safeContainerName(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || char == '-' || char == '_' || char == '.' {
			continue
		}
		return false
	}
	return true
}

func safeProviderID(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	for index, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') {
			continue
		}
		if index > 0 && (char == '-' || char == '_' || char == '.') {
			continue
		}
		return false
	}
	return true
}

type cappedOutput struct {
	bytes.Buffer
	limit     int
	truncated bool
}

func newCappedOutput(limit int) *cappedOutput {
	return &cappedOutput{limit: limit}
}

func (w *cappedOutput) Write(p []byte) (int, error) {
	original := len(p)
	remaining := w.limit - w.Len()
	if remaining <= 0 {
		w.truncated = true
		return original, nil
	}
	if len(p) > remaining {
		p = p[:remaining]
		w.truncated = true
	}
	_, _ = w.Buffer.Write(p)
	return original, nil
}
