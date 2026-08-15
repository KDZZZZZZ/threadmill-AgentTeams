package agentteams

// This file implements the two public AgentTeams integration entry points
// confirmed in AgentTeams main: TeamHarness' MCP stdio server and QwenPaw's
// localhost management HTTP API. It contains no copied TeamHarness state
// machine and does not modify third_party code.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	phasemcp "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/mcp/phase"
)

// TeamHarnessStdioClient invokes the official TeamHarness MCP stdio entry
// point. Each call starts a bounded child process, so Go context cancellation
// terminates the bridge on both Windows and Unix.
type TeamHarnessStdioClient struct {
	Python     string
	ServerPath string
	Workspace  string
	Env        []string
}

func (c TeamHarnessStdioClient) DelegateTask(ctx context.Context, request TeamHarnessDelegateTaskRequest) error {
	_, err := c.call(ctx, "delegate_task", map[string]any{"projectId": request.ProjectID, "taskId": request.TaskID, "roomId": request.RoomID, "assignedTo": request.Assignee, "title": request.Title, "spec": request.Spec})
	return err
}
func (c TeamHarnessStdioClient) CheckTask(ctx context.Context, taskID string) (TeamHarnessTaskSnapshot, error) {
	result, err := c.call(ctx, "check_task", map[string]any{"taskId": taskID})
	if err != nil {
		return TeamHarnessTaskSnapshot{}, err
	}
	task, _ := result["task"].(map[string]any)
	state := TeamHarnessTaskSnapshot{TaskID: stringValue(task["task_id"]), Status: TeamHarnessTaskStatus(stringValue(task["status"])), Acknowledged: stringValue(task["acknowledged_by_role"]) != ""}
	if output, ok := result["result"].(map[string]any); ok {
		state.ResultStatus, state.Summary = stringValue(output["status"]), stringValue(output["summary"])
		state.Deliverables = stringsValue(output["deliverables"])
		state.ResultPath = stringValue(output["result_path"])
	}
	return state, nil
}
func (c TeamHarnessStdioClient) CancelTask(ctx context.Context, taskID, reason string) error {
	_, err := c.call(ctx, "cancel_task", map[string]any{"taskId": taskID, "reason": reason})
	return err
}

// AcknowledgeTask and SubmitTask are integration-driver methods only. They
// retain TeamHarness's worker role; Threadmill's leader host never uses them.
func (c TeamHarnessStdioClient) AcknowledgeTask(ctx context.Context, taskID string) error {
	_, err := c.callAs(ctx, "worker", "ack_task", map[string]any{"taskId": taskID})
	return err
}
func (c TeamHarnessStdioClient) SubmitTask(ctx context.Context, taskID, status, summary string, deliverables []string) error {
	_, err := c.callAs(ctx, "worker", "submit_task", map[string]any{"taskId": taskID, "status": status, "summary": summary, "deliverables": deliverables})
	return err
}

func (c TeamHarnessStdioClient) call(ctx context.Context, action string, payload map[string]any) (map[string]any, error) {
	return c.callAs(ctx, "leader", action, payload)
}
func (c TeamHarnessStdioClient) callAs(ctx context.Context, role, action string, payload map[string]any) (map[string]any, error) {
	if c.Python == "" || c.ServerPath == "" || c.Workspace == "" {
		return nil, fmt.Errorf("python, server path, and workspace are required")
	}
	serverPath, err := filepath.Abs(c.ServerPath)
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, c.Python, serverPath)
	cmd.Dir = filepath.Dir(serverPath)
	cmd.Env = mergedEnvironment(c.Env)
	in, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	request := map[string]any{"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": map[string]any{"name": "taskflow", "arguments": map[string]any{"role": role, "workspaceDir": c.Workspace, "action": action, "payload": payload}}}
	if err := json.NewEncoder(in).Encode(request); err != nil {
		_ = cmd.Process.Kill()
		return nil, err
	}
	_ = in.Close()
	line, err := bufio.NewReader(out).ReadBytes('\n')
	if err != nil {
		_ = cmd.Wait()
		return nil, fmt.Errorf("teamharness response: %w: %s", err, stderr.String())
	}
	if err := cmd.Wait(); err != nil {
		return nil, fmt.Errorf("teamharness process: %w: %s", err, stderr.String())
	}
	var rpc struct {
		Result struct {
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
		} `json:"result"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(line, &rpc); err != nil {
		return nil, err
	}
	if rpc.Error != nil {
		return nil, fmt.Errorf("teamharness rpc: %s", rpc.Error.Message)
	}
	if len(rpc.Result.Content) == 0 {
		return nil, fmt.Errorf("teamharness returned no content")
	}
	var result map[string]any
	if err := json.Unmarshal([]byte(rpc.Result.Content[0].Text), &result); err != nil {
		return nil, err
	}
	if ok, _ := result["ok"].(bool); !ok {
		encoded, _ := json.Marshal(result)
		return nil, fmt.Errorf("teamharness %s: %s", action, encoded)
	}
	return result, nil
}

func mergedEnvironment(overrides []string) []string {
	values := make(map[string]string)
	for _, entry := range os.Environ() {
		if name, value, ok := strings.Cut(entry, "="); ok {
			values[strings.ToUpper(name)] = name + "=" + value
		}
	}
	for _, entry := range overrides {
		if name, value, ok := strings.Cut(entry, "="); ok {
			values[strings.ToUpper(name)] = name + "=" + value
		}
	}
	out := make([]string, 0, len(values))
	for _, entry := range values {
		out = append(out, entry)
	}
	return out
}
func stringValue(value any) string { text, _ := value.(string); return text }
func stringsValue(value any) []string {
	values, _ := value.([]any)
	out := make([]string, 0, len(values))
	for _, v := range values {
		out = append(out, stringValue(v))
	}
	return out
}

// QwenPawMCPInjector provisions one temporary streamable-HTTP MCP client per
// execution using QwenPaw's documented /api/mcp and /api/mcp/policy APIs.
// QwenPaw exposes no invocation-scoped agent binding API; unique client keys
// are therefore an MVP management boundary, not a stronger worker isolation.
type QwenPawMCPInjector struct {
	BaseURL, PhaseMCPURL string
	Client               *http.Client
}

func (i QwenPawMCPInjector) InjectPhaseMCP(ctx context.Context, executionID string, binding phasemcp.ExecutionBinding) error {
	key := "threadmill-" + taskflowSafeID(executionID)
	client := i.httpClient()
	tools := binding.ToolNames
	payload := map[string]any{"client_key": key, "client": map[string]any{"name": key, "description": "Threadmill Phase MCP", "enabled": true, "transport": "streamable_http", "url": i.PhaseMCPURL, "headers": map[string]string{"X-Threadmill-Execution-Token": binding.Token}, "tools": tools}}
	if err := i.request(ctx, client, http.MethodPost, "/api/mcp", payload, nil); err != nil {
		return err
	}
	return i.request(ctx, client, http.MethodPut, "/api/mcp/policy/"+key, map[string]any{"default_effect": "deny", "tool_overrides": toolOverrides(tools)}, nil)
}
func (i QwenPawMCPInjector) CleanupPhaseMCP(ctx context.Context, executionID string, binding phasemcp.ExecutionBinding) error {
	return i.request(ctx, i.httpClient(), http.MethodDelete, "/api/mcp/"+"threadmill-"+taskflowSafeID(executionID), nil, nil)
}
func (i QwenPawMCPInjector) httpClient() *http.Client {
	if i.Client != nil {
		return i.Client
	}
	return &http.Client{Timeout: 10 * time.Second}
}
func (i QwenPawMCPInjector) request(ctx context.Context, client *http.Client, method, path string, payload any, into any) error {
	var body io.Reader
	if payload != nil {
		b, e := json.Marshal(payload)
		if e != nil {
			return e
		}
		body = bytes.NewReader(b)
	}
	req, e := http.NewRequestWithContext(ctx, method, strings.TrimRight(i.BaseURL, "/")+path, body)
	if e != nil {
		return e
	}
	req.Header.Set("Content-Type", "application/json")
	res, e := client.Do(req)
	if e != nil {
		return e
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return fmt.Errorf("qwenpaw %s %s: %s", method, path, res.Status)
	}
	if into != nil {
		return json.NewDecoder(res.Body).Decode(into)
	}
	return nil
}
func toolOverrides(tools []string) []map[string]any {
	out := make([]map[string]any, 0, len(tools))
	for _, tool := range tools {
		out = append(out, map[string]any{"tool_name": tool, "effect": "allow"})
	}
	return out
}
