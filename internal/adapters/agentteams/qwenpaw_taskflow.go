package agentteams

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os/exec"
	"strconv"
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

management_port = int(sys.argv[1])
with urlopen(f"http://127.0.0.1:{management_port}/api/mcp/teamharness", timeout=10) as response:
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

// qwenPawWorkspaceManifestBridge runs inside the already-fenced worker and
// scans only Threadmill's fixed task workspace. It preserves Runtime-owned
// manifest payload, replaces the file inventory, rejects links/special files,
// and never receives a caller-controlled filesystem path.
const qwenPawWorkspaceManifestBridge = `
import hashlib
import json
import os
import stat
import sys
from pathlib import Path
from urllib.request import urlopen

management_port = int(sys.argv[1])
task_id = sys.argv[2]
with urlopen(f"http://127.0.0.1:{management_port}/api/mcp/teamharness", timeout=10) as response:
    client = json.load(response)

cwd = Path(client["cwd"])
working_dir = cwd.parents[2]
task_root = working_dir / "workspaces" / "default" / "shared" / "tasks" / task_id
workspace = task_root / "workspace"
manifest_path = task_root / "threadmill" / "workspace.json"
manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
if manifest.get("version") != "threadmill.agentteams.workspace.v1" or not manifest.get("payload"):
    raise SystemExit(2)

protected = {".git", ".env", "credentials", "sessions", "logs", "tool results", "tool-results", "auth.json", "id_rsa", "id_ed25519"}
files = []
total = 0
for root, dirs, names in os.walk(workspace, topdown=True, followlinks=False):
    root_path = Path(root)
    kept_dirs = []
    for name in sorted(dirs):
        child = root_path / name
        relative = child.relative_to(workspace).as_posix()
        if name.strip().lower() in protected:
            continue
        if child.is_symlink():
            raise SystemExit(3)
        kept_dirs.append(name)
    dirs[:] = kept_dirs
    for name in sorted(names):
        child = root_path / name
        relative = child.relative_to(workspace).as_posix()
        if any(part.strip().lower() in protected for part in Path(relative).parts):
            continue
        info = child.lstat()
        if not stat.S_ISREG(info.st_mode):
            raise SystemExit(3)
        if info.st_size > 4 * 1024 * 1024:
            raise SystemExit(4)
        total += info.st_size
        if total > 512 * 1024 * 1024:
            raise SystemExit(4)
        digest = hashlib.sha256()
        with child.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        files.append({"path": relative, "mode": stat.S_IMODE(info.st_mode), "sha256": digest.hexdigest(), "size": info.st_size})

manifest["files"] = sorted(files, key=lambda item: item["path"])
temporary = manifest_path.with_name("workspace.json.tmp")
temporary.write_text(json.dumps(manifest, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
temporary.replace(manifest_path)
print(json.dumps({"ok": True, "files": len(files)}, separators=(",", ":")))
`

type TaskflowCall struct {
	Action       string
	Role         string
	ProjectID    string
	TaskID       string
	RoomID       string
	AssignedTo   string
	Spec         string
	Reason       string
	Status       string
	Summary      string
	Deliverables []string
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
	if call.Action == "delegate_task" {
		assignedTo, err := matrixUserIDForWorker(call.RoomID, call.AssignedTo)
		if err != nil {
			return TaskflowCallResult{}, err
		}
		call.AssignedTo = assignedTo
	}
	arguments, err := taskflowArguments(call)
	if err != nil {
		return TaskflowCallResult{}, err
	}
	if call.Action == "delegate_task" {
		carrierID, err := c.ensureCarrier(ctx, container, call)
		if err != nil {
			return TaskflowCallResult{}, fmt.Errorf("prepare AgentTeams task carrier: %w", err)
		}
		arguments["projectId"] = carrierID
		taskRoomID, err := c.ensureTaskRoom(ctx, container, call, carrierID)
		if err != nil {
			return TaskflowCallResult{}, fmt.Errorf("prepare AgentTeams task room: %w", err)
		}
		arguments["roomId"] = taskRoomID
	}
	raw, err := c.executeMCP(ctx, container, "taskflow", arguments)
	if err != nil {
		return TaskflowCallResult{}, err
	}
	result, err := parseTaskflowCallResult(raw)
	if err != nil {
		return TaskflowCallResult{}, fmt.Errorf("execute AgentTeams taskflow %s: %w", call.Action, err)
	}
	if result.Action != call.Action || result.Task.TaskID != call.TaskID {
		return TaskflowCallResult{}, kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "QwenPaw TeamHarness returned a mismatched task identity", Recoverable: true}
	}
	return result, nil
}

// PushSharedPath uses the installed TeamHarness filesync MCP inside the
// assigned worker. The path is fixed by Threadmill's execution identity; no
// Agent-provided filesystem or object-store path reaches this method.
func (c *QwenPawDockerTaskflow) PushSharedPath(ctx context.Context, container, sharedPath string) error {
	container = strings.TrimSpace(container)
	if !safeContainerName(container) {
		return kernel.InvalidArgument("QwenPaw container name is invalid")
	}
	sharedPath = strings.TrimSpace(sharedPath)
	if !strings.HasPrefix(sharedPath, "shared/tasks/threadmill-") || !strings.HasSuffix(sharedPath, "/") || strings.Contains(sharedPath, "\\") || strings.Contains(sharedPath, "..") {
		return kernel.InvalidArgument("AgentTeams shared task path is invalid")
	}
	raw, err := c.executeMCP(ctx, container, "filesync", map[string]any{
		"action": "push",
		"path":   sharedPath,
	})
	if err != nil {
		return err
	}
	text, err := mcpTextPayload(raw)
	if err != nil {
		return err
	}
	var result struct {
		OK bool `json:"ok"`
	}
	if err := json.Unmarshal([]byte(text), &result); err != nil || !result.OK {
		return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "TeamHarness filesync push failed", Recoverable: true}
	}
	return nil
}

// SnapshotSharedWorkspace refreshes the complete file inventory immediately
// before TeamHarness mirrors the task directory back to object storage.
func (c *QwenPawDockerTaskflow) SnapshotSharedWorkspace(ctx context.Context, container, taskID string) error {
	container = strings.TrimSpace(container)
	if !safeContainerName(container) || !safeProviderID(taskID) || !strings.HasPrefix(taskID, "threadmill-") {
		return kernel.InvalidArgument("AgentTeams workspace snapshot identity is invalid")
	}
	cmd := exec.CommandContext(
		ctx,
		c.dockerBinary,
		"exec", "-i", container,
		c.pythonBinary, "-c", qwenPawWorkspaceManifestBridge, strconv.Itoa(qwenPawManagementPort(container)), taskID,
	)
	cmd.WaitDelay = 5 * time.Second
	stdout := newCappedOutput(64 << 10)
	stderr := newCappedOutput(64 << 10)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "QwenPaw workspace snapshot failed", Recoverable: true}
	}
	var result struct {
		OK bool `json:"ok"`
	}
	if stdout.truncated || json.Unmarshal(stdout.Bytes(), &result) != nil || !result.OK {
		return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "QwenPaw workspace snapshot response is invalid", Recoverable: true}
	}
	return nil
}

func (c *QwenPawDockerTaskflow) ensureTaskRoom(ctx context.Context, container string, call TaskflowCall, carrierID string) (string, error) {
	invitee := strings.TrimSpace(call.AssignedTo)
	if !validMatrixUserID(invitee) {
		return "", kernel.InvalidArgument("assigned worker Matrix user ID is invalid")
	}
	raw, err := c.executeMCP(ctx, container, "roomflow", map[string]any{
		"action":       "create_task_room",
		"projectId":    carrierID,
		"name":         "Threadmill bounded execution",
		"source":       "threadmill-runtime",
		"sourceRoomId": call.RoomID,
		"invite":       []string{invitee},
	})
	if err != nil {
		return "", err
	}
	text, err := mcpTextPayload(raw)
	if err != nil {
		return "", err
	}
	var payload struct {
		OK     bool   `json:"ok"`
		Action string `json:"action"`
		RoomID string `json:"roomId"`
	}
	if err := json.Unmarshal([]byte(text), &payload); err != nil {
		return "", kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "TeamHarness returned an invalid task room payload", Recoverable: true}
	}
	payload.RoomID = strings.TrimSpace(payload.RoomID)
	if !payload.OK || payload.Action != "create_task_room" || !validMatrixRoomID(payload.RoomID) || payload.RoomID == strings.TrimSpace(call.RoomID) {
		return "", kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "TeamHarness task room preparation failed", Recoverable: true}
	}
	return payload.RoomID, nil
}

func matrixUserIDForWorker(roomID, worker string) (string, error) {
	roomID = strings.TrimSpace(roomID)
	worker = strings.TrimSpace(worker)
	if !validMatrixRoomID(roomID) || !safeProviderID(worker) {
		return "", kernel.InvalidArgument("task room source and assigned worker must be valid Matrix/provider identities")
	}
	separator := strings.IndexByte(roomID, ':')
	serverName := roomID[separator+1:]
	if strings.IndexFunc(serverName, func(char rune) bool { return char <= 0x20 || char == 0x7f || char == '/' || char == '\\' }) >= 0 {
		return "", kernel.InvalidArgument("task room source server name is invalid")
	}
	return "@" + worker + ":" + serverName, nil
}

func validMatrixRoomID(roomID string) bool {
	if len(roomID) < 4 || len(roomID) > 512 || roomID[0] != '!' {
		return false
	}
	separator := strings.IndexByte(roomID, ':')
	return separator > 1 && separator < len(roomID)-1
}

func validMatrixUserID(userID string) bool {
	if len(userID) < 4 || len(userID) > 512 || userID[0] != '@' {
		return false
	}
	separator := strings.IndexByte(userID, ':')
	return separator > 1 && separator < len(userID)-1
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
		c.pythonBinary, "-c", qwenPawTeamHarnessBridge, strconv.Itoa(qwenPawManagementPort(container)),
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
	if call.Action != "delegate_task" && call.Action != "check_task" && call.Action != "cancel_task" && call.Action != "submit_task" {
		return nil, kernel.InvalidArgument("taskflow action is invalid")
	}
	if !safeProviderID(call.TaskID) {
		return nil, kernel.InvalidArgument("taskflow task_id must be a safe provider id")
	}
	role := strings.TrimSpace(call.Role)
	if role == "" {
		role = "leader"
	}
	arguments := map[string]any{
		"role":   role,
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
	case "submit_task":
		if role != "worker" && role != "remote-member" {
			return nil, kernel.InvalidArgument("submit_task requires a worker role")
		}
		status := strings.TrimSpace(call.Status)
		if status != "SUCCESS" && status != "BLOCKED" && status != "FAILED" {
			return nil, kernel.InvalidArgument("submit_task status must be SUCCESS, BLOCKED, or FAILED")
		}
		if strings.TrimSpace(call.Summary) == "" || len(call.Summary) > 1<<16 {
			return nil, kernel.InvalidArgument("submit_task summary is required and must fit the size limit")
		}
		arguments["status"] = status
		arguments["summary"] = call.Summary
		arguments["deliverables"] = append([]string(nil), call.Deliverables...)
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
		Error     string `json:"error"`
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
		if payload.Error == "task not found" {
			return TaskflowCallResult{}, kernel.Error{Code: kernel.CodeNotFound, Message: "AgentTeams execution does not exist", Recoverable: false}
		}
		if payload.Action == "cancel_task" && strings.HasPrefix(payload.Error, "cannot cancel terminal task: ") {
			return TaskflowCallResult{}, kernel.Error{Code: kernel.CodeStaleCommand, Message: "AgentTeams task is already terminal", Recoverable: false}
		}
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
