package agentteams

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

const (
	qwenPawAPIVersion    = "2.0.1"
	qwenPawResponseLimit = 1 << 20
)

// InvocationMCP is the short-lived Threadmill MCP client installed into one
// QwenPaw host. BearerToken is deliberately write-only: no method returns it.
type InvocationMCP struct {
	Key           string
	URL           string
	BearerToken   string
	ExpectedTools []string
}

// QwenPawAPI owns only QwenPaw's localhost management surface. It does not
// dispatch AgentTeams tasks or make Threadmill coordination decisions.
type QwenPawAPI struct {
	baseURL      string
	httpClient   *http.Client
	pollInterval time.Duration
	stableWindow time.Duration
}

func NewQwenPawAPI(baseURL string, httpClient *http.Client) (*QwenPawAPI, error) {
	parsed, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User != nil {
		return nil, kernel.InvalidArgument("QwenPaw API base URL must be an http(s) URL without userinfo")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, kernel.InvalidArgument("QwenPaw API base URL must use http or https")
	}
	if parsed.Path != "" && parsed.Path != "/" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, kernel.InvalidArgument("QwenPaw API base URL cannot contain a path, query, or fragment")
	}
	host := parsed.Hostname()
	if !strings.EqualFold(host, "localhost") {
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			return nil, kernel.InvalidArgument("QwenPaw management API must use a loopback host")
		}
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	client := *httpClient
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &QwenPawAPI{
		baseURL:      strings.TrimRight(parsed.String(), "/"),
		httpClient:   &client,
		pollInterval: 250 * time.Millisecond,
		stableWindow: 3 * time.Second,
	}, nil
}

func InvocationMCPKey(agentTeamsTaskID string) (string, error) {
	key := strings.TrimSpace(agentTeamsTaskID)
	if !safeProviderID(key) || !strings.HasPrefix(key, "threadmill-") {
		return "", kernel.InvalidArgument("AgentTeams task id cannot be used as an invocation MCP key")
	}
	return key, nil
}

func (a *QwenPawAPI) Ready(ctx context.Context) error {
	var response struct {
		Version string `json:"version"`
	}
	if err := a.doJSON(ctx, http.MethodGet, "/api/version", nil, &response); err != nil {
		return err
	}
	if response.Version != qwenPawAPIVersion {
		return kernel.Error{
			Code:        kernel.CodeExecutorUnavailable,
			Message:     fmt.Sprintf("QwenPaw API version %q is unsupported", response.Version),
			Recoverable: true,
		}
	}
	return nil
}

// WaitStartupReady waits until QwenPaw's management API is fully usable, not
// merely until /api/version answers. During worker wrapper startup the version
// endpoint can become reachable before the wrapper has finished applying its
// desired built-in/plugin configuration; Threadmill must not install an
// invocation MCP or delegate a task into that partially initialized window.
func (a *QwenPawAPI) WaitStartupReady(ctx context.Context) error {
	waitCtx := ctx
	cancel := func() {}
	if _, ok := waitCtx.Deadline(); !ok {
		waitCtx, cancel = context.WithTimeout(ctx, 90*time.Second)
	}
	defer cancel()

	var lastErr error
	for {
		if err := a.startupReady(waitCtx); err == nil {
			return nil
		} else {
			lastErr = err
		}

		timer := time.NewTimer(a.pollInterval)
		select {
		case <-waitCtx.Done():
			timer.Stop()
			if lastErr != nil {
				return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "QwenPaw worker did not complete startup", Recoverable: true}
			}
			return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "QwenPaw worker startup wait timed out", Recoverable: true}
		case <-timer.C:
		}
	}
}

func (a *QwenPawAPI) startupReady(ctx context.Context) error {
	if err := a.Ready(ctx); err != nil {
		return err
	}
	if _, err := a.listBuiltinTools(ctx); err != nil {
		return err
	}
	if _, err := a.AgentActivity(ctx); err != nil {
		return err
	}
	return nil
}

func (a *QwenPawAPI) AgentActivity(ctx context.Context) (HostActivity, error) {
	var response struct {
		Status           string     `json:"status"`
		RunningTaskCount int        `json:"running_task_count"`
		LastRunAt        *time.Time `json:"last_run_at"`
		LastFinishAt     *time.Time `json:"last_finish_at"`
	}
	if err := a.doJSON(ctx, http.MethodGet, "/api/agents/default/agent-status", nil, &response); err != nil {
		return HostActivity{}, err
	}
	response.Status = strings.TrimSpace(response.Status)
	if response.Status == "" || response.RunningTaskCount < 0 {
		return HostActivity{}, kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "QwenPaw agent activity response is invalid", Recoverable: true}
	}
	activity := HostActivity{Status: response.Status, RunningTaskCount: response.RunningTaskCount}
	if response.LastRunAt != nil {
		activity.LastRunAt = response.LastRunAt.UTC()
	}
	if response.LastFinishAt != nil {
		activity.LastFinishAt = response.LastFinishAt.UTC()
	}
	return activity, nil
}

// WaitInvocationReady closes the gap between QwenPaw's configuration
// readback and its actual workspace lifecycle. Enabling a built-in tool or
// changing one MCP client causes an asynchronous workspace hot reload. During
// that reload the version, tool-list, and MCP endpoints can all answer 200
// while the Matrix consumer that receives AgentTeams assignments is still
// being replaced. Require one uninterrupted idle window with both native and
// invocation tools readable before Runtime is allowed to delegate the task.
func (a *QwenPawAPI) WaitInvocationReady(ctx context.Context, key string, expected []string) error {
	key = strings.TrimSpace(key)
	if !safeQwenPawKey(key) {
		return kernel.InvalidArgument("QwenPaw MCP client key is invalid")
	}
	expected = normalizeToolNames(expected)
	waitCtx := ctx
	cancel := func() {}
	if _, ok := waitCtx.Deadline(); !ok {
		waitCtx, cancel = context.WithTimeout(ctx, 30*time.Second)
	}
	defer cancel()

	var stableSince time.Time
	for {
		ready := a.Ready(waitCtx) == nil && a.nativeProjectToolsEnabled(waitCtx) == nil && a.invocationToolsEnabled(waitCtx, key, expected) == nil
		if ready {
			activity, err := a.AgentActivity(waitCtx)
			ready = err == nil && strings.EqualFold(activity.Status, "idle") && activity.RunningTaskCount == 0
		}
		if ready {
			if stableSince.IsZero() {
				stableSince = time.Now()
			} else if time.Since(stableSince) >= a.stableWindow {
				return nil
			}
		} else {
			stableSince = time.Time{}
		}

		timer := time.NewTimer(a.pollInterval)
		select {
		case <-waitCtx.Done():
			timer.Stop()
			return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "QwenPaw workspace did not become stably idle", Recoverable: true}
		case <-timer.C:
		}
	}
}

var qwenPawNativeProjectTools = map[string]struct{}{
	"read_file": {}, "write_file": {}, "edit_file": {}, "append_file": {},
	"grep_search": {}, "glob_search": {}, "ast_search": {}, "execute_shell_command": {},
	"browser_use": {}, "web_search": {}, "web_fetch": {}, "view_image": {}, "view_video": {},
}

// EnsureNativeProjectTools keeps QwenPaw's normal project and research tools
// available. Threadmill does not replace an agent's editor, search, shell, or
// browser; it only owns the authoritative Coordination and Context Graph MCP
// mutations. Collaboration/history/sub-agent tools are deliberately left at
// the operator-configured state because they can create invisible scheduling
// or memory channels outside Graph Runtime.
func (a *QwenPawAPI) EnsureNativeProjectTools(ctx context.Context) error {
	if err := a.Ready(ctx); err != nil {
		return err
	}
	tools, err := a.listBuiltinTools(ctx)
	if err != nil {
		return err
	}
	for _, tool := range tools {
		if _, required := qwenPawNativeProjectTools[tool.Name]; !required || tool.Enabled {
			continue
		}
		path := "/api/agents/default/tools/" + url.PathEscape(tool.Name) + "/toggle"
		if err := a.doJSON(ctx, http.MethodPatch, path, nil, nil); err != nil {
			return err
		}
	}
	return a.nativeProjectToolsEnabled(ctx)
}

func (a *QwenPawAPI) nativeProjectToolsEnabled(ctx context.Context) error {
	tools, err := a.listBuiltinTools(ctx)
	if err != nil {
		return err
	}
	enabled := make(map[string]bool, len(tools))
	for _, tool := range tools {
		enabled[tool.Name] = tool.Enabled
	}
	for name := range qwenPawNativeProjectTools {
		if actual, known := enabled[name]; known && !actual {
			return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "QwenPaw native project tool readback mismatch", Recoverable: true}
		}
	}
	return nil
}

type qwenPawBuiltinTool struct {
	Name    string `json:"name"`
	Enabled bool   `json:"enabled"`
}

func (a *QwenPawAPI) listBuiltinTools(ctx context.Context) ([]qwenPawBuiltinTool, error) {
	var tools []qwenPawBuiltinTool
	if err := a.doJSON(ctx, http.MethodGet, "/api/agents/default/tools", nil, &tools); err != nil {
		return nil, err
	}
	for _, tool := range tools {
		if !safeQwenPawKey(tool.Name) {
			return nil, kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "QwenPaw returned an invalid built-in tool name", Recoverable: true}
		}
	}
	return tools, nil
}

func (a *QwenPawAPI) InstallInvocationMCP(ctx context.Context, desired InvocationMCP) error {
	desired.Key = strings.TrimSpace(desired.Key)
	desired.URL = strings.TrimSpace(desired.URL)
	if err := validateInvocationMCP(desired); err != nil {
		return err
	}
	desired.ExpectedTools = normalizeToolNames(desired.ExpectedTools)
	if err := a.Ready(ctx); err != nil {
		return err
	}

	client := map[string]any{
		"name":      desired.Key,
		"enabled":   true,
		"transport": "streamable_http",
		"url":       desired.URL,
		"headers": map[string]string{
			"Authorization": "Bearer " + desired.BearerToken,
		},
		"tools": desired.ExpectedTools,
	}
	existing, err := a.listMCP(ctx)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = a.RevokeInvocationMCP(cleanupCtx, desired.Key)
	}()
	if _, ok := existing[desired.Key]; ok {
		if err := a.doJSON(ctx, http.MethodPut, "/api/mcp/"+url.PathEscape(desired.Key), client, nil); err != nil {
			return err
		}
	} else {
		if err := a.doJSON(ctx, http.MethodPost, "/api/mcp", map[string]any{
			"client_key": desired.Key,
			"client":     client,
		}, nil); err != nil {
			return err
		}
	}

	var actual struct {
		Enabled   bool     `json:"enabled"`
		Transport string   `json:"transport"`
		URL       string   `json:"url"`
		Tools     []string `json:"tools"`
	}
	if err := a.doJSON(ctx, http.MethodGet, "/api/mcp/"+url.PathEscape(desired.Key), nil, &actual); err != nil {
		return err
	}
	if !actual.Enabled || actual.Transport != "streamable_http" || actual.URL != desired.URL ||
		!equalStrings(normalizeToolNames(actual.Tools), desired.ExpectedTools) {
		return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "QwenPaw MCP readback mismatch", Recoverable: true}
	}

	toolDefaults := make([]map[string]string, 0, len(desired.ExpectedTools))
	for _, tool := range desired.ExpectedTools {
		toolDefaults = append(toolDefaults, map[string]string{"tool_name": tool, "effect": "allow"})
	}
	policy := map[string]any{
		"default_effect":   "deny",
		"client_overrides": []any{},
		"tool_defaults":    toolDefaults,
		"tool_overrides":   []any{},
	}
	if err := a.doJSON(ctx, http.MethodPut, "/api/mcp/policy/"+url.PathEscape(desired.Key), policy, nil); err != nil {
		return err
	}
	var actualPolicy struct {
		DefaultEffect   string            `json:"default_effect"`
		ClientOverrides []json.RawMessage `json:"client_overrides"`
		ToolDefaults    []struct {
			ToolName string `json:"tool_name"`
			Effect   string `json:"effect"`
		} `json:"tool_defaults"`
		ToolOverrides []json.RawMessage `json:"tool_overrides"`
	}
	if err := a.doJSON(ctx, http.MethodGet, "/api/mcp/policy/"+url.PathEscape(desired.Key), nil, &actualPolicy); err != nil {
		return err
	}
	actualAllowed := make([]string, 0, len(actualPolicy.ToolDefaults))
	for _, item := range actualPolicy.ToolDefaults {
		if item.Effect != "allow" {
			return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "QwenPaw MCP policy readback mismatch", Recoverable: true}
		}
		actualAllowed = append(actualAllowed, item.ToolName)
	}
	if actualPolicy.DefaultEffect != "deny" || len(actualPolicy.ClientOverrides) != 0 || len(actualPolicy.ToolOverrides) != 0 ||
		!equalStrings(normalizeToolNames(actualAllowed), desired.ExpectedTools) {
		return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "QwenPaw MCP policy readback mismatch", Recoverable: true}
	}
	if err := a.waitForTools(ctx, desired.Key, desired.ExpectedTools); err != nil {
		return err
	}
	committed = true
	return nil
}

func (a *QwenPawAPI) RevokeInvocationMCP(ctx context.Context, key string) error {
	key = strings.TrimSpace(key)
	if key == "" {
		return kernel.InvalidArgument("QwenPaw MCP client key is required")
	}
	existing, err := a.listMCP(ctx)
	if err != nil {
		return err
	}
	if _, ok := existing[key]; !ok {
		return nil
	}
	if err := a.doJSON(ctx, http.MethodDelete, "/api/mcp/"+url.PathEscape(key), nil, nil); err != nil {
		return err
	}
	existing, err = a.listMCP(ctx)
	if err != nil {
		return err
	}
	if _, ok := existing[key]; ok {
		return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "QwenPaw MCP revocation readback mismatch", Recoverable: true}
	}
	return nil
}

// DeleteInvocationMCPIfPresent is the cold-start recovery path for durable
// Threadmill invocation clients. It deliberately does not call GET /api/mcp:
// QwenPaw may block that collection endpoint while restoring a historical MCP
// client whose bearer has already expired. Only canonical generated
// Threadmill keys are accepted, so operator-owned MCP clients cannot be
// removed through this path.
func (a *QwenPawAPI) DeleteInvocationMCPIfPresent(ctx context.Context, key string) error {
	key = strings.TrimSpace(key)
	if !isInvocationMCPKey(key) {
		return kernel.InvalidArgument("QwenPaw invocation MCP key is invalid")
	}
	if a == nil || a.httpClient == nil || a.baseURL == "" {
		return kernel.Error{Code: kernel.CodeInternalError, Message: "QwenPaw API client is not configured"}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, a.baseURL+"/api/mcp/"+url.PathEscape(key), nil)
	if err != nil {
		return kernel.InvalidArgument("QwenPaw API request is invalid")
	}
	req.Header.Set("Accept", "application/json")
	resp, err := a.httpClient.Do(req)
	if err != nil {
		return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "QwenPaw API is unavailable", Recoverable: true}
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: fmt.Sprintf("QwenPaw API returned HTTP %d", resp.StatusCode), Recoverable: true}
	}
	if _, err := io.Copy(io.Discard, io.LimitReader(resp.Body, qwenPawResponseLimit+1)); err != nil {
		return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "QwenPaw API response could not be read", Recoverable: true}
	}
	return nil
}

// PruneInvocationMCPExcept cleans provider-local Threadmill clients that are
// not represented in the current database (for example after switching to a
// fresh PostgreSQL schema while reusing a durable QwenPaw volume). The caller
// must already hold the host slot exclusively and invoke this before task
// delegation. Operator-owned and package MCP clients are never touched.
func (a *QwenPawAPI) PruneInvocationMCPExcept(ctx context.Context, currentKey string) error {
	currentKey = strings.TrimSpace(currentKey)
	if _, err := InvocationMCPKey(currentKey); err != nil {
		return kernel.InvalidArgument("current QwenPaw invocation MCP key is invalid")
	}
	existing, err := a.listMCP(ctx)
	if err != nil {
		return err
	}
	keys := make([]string, 0, len(existing))
	for key := range existing {
		if key != currentKey && isInvocationMCPKey(key) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := a.DeleteInvocationMCPIfPresent(ctx, key); err != nil {
			return err
		}
	}
	return nil
}

func isInvocationMCPKey(key string) bool {
	if !strings.HasPrefix(key, "threadmill-") || !safeQwenPawKey(key) {
		return false
	}
	suffix := strings.TrimPrefix(key, "threadmill-")
	base, attempt, hasAttempt := strings.Cut(suffix, "-attempt-")
	if hasAttempt {
		if attempt == "" || strings.Contains(attempt, "-attempt-") {
			return false
		}
		for _, char := range attempt {
			if char < '0' || char > '9' {
				return false
			}
		}
		suffix = base
	}
	if len(suffix) != 24 && len(suffix) != 32 {
		return false
	}
	for _, char := range suffix {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}

func validateInvocationMCP(desired InvocationMCP) error {
	if desired.Key == "" || desired.BearerToken == "" {
		return kernel.InvalidArgument("QwenPaw MCP key and bearer token are required")
	}
	if !safeQwenPawKey(desired.Key) {
		return kernel.InvalidArgument("QwenPaw MCP key contains unsupported characters")
	}
	parsed, err := url.Parse(strings.TrimSpace(desired.URL))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User != nil {
		return kernel.InvalidArgument("Threadmill MCP URL must be an http(s) URL without userinfo")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return kernel.InvalidArgument("Threadmill MCP URL must use http or https")
	}
	if parsed.Fragment != "" {
		return kernel.InvalidArgument("Threadmill MCP URL cannot contain a fragment")
	}
	if strings.IndexFunc(desired.BearerToken, func(char rune) bool {
		return char <= 0x20 || char == 0x7f
	}) >= 0 {
		return kernel.InvalidArgument("QwenPaw MCP bearer token contains whitespace or control characters")
	}
	for _, tool := range desired.ExpectedTools {
		if strings.TrimSpace(tool) == "" {
			return kernel.InvalidArgument("expected MCP tool names cannot be empty")
		}
	}
	return nil
}

func safeQwenPawKey(value string) bool {
	if len(value) > 128 {
		return false
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || char == '-' || char == '_' || char == '.' {
			continue
		}
		return false
	}
	return value != ""
}

func (a *QwenPawAPI) waitForTools(ctx context.Context, key string, expected []string) error {
	if len(expected) == 0 {
		return nil
	}
	want := append([]string(nil), expected...)
	sort.Strings(want)

	waitCtx := ctx
	cancel := func() {}
	if _, ok := ctx.Deadline(); !ok {
		waitCtx, cancel = context.WithTimeout(ctx, 30*time.Second)
	}
	defer cancel()

	for {
		if err := a.invocationToolsEnabled(waitCtx, key, want); err == nil {
			return nil
		}

		timer := time.NewTimer(a.pollInterval)
		select {
		case <-waitCtx.Done():
			timer.Stop()
			return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "QwenPaw MCP tools did not become callable", Recoverable: true}
		case <-timer.C:
		}
	}
}

func (a *QwenPawAPI) invocationToolsEnabled(ctx context.Context, key string, expected []string) error {
	var tools []struct {
		Name    string `json:"name"`
		Enabled bool   `json:"enabled"`
	}
	if err := a.doJSON(ctx, http.MethodGet, "/api/mcp/tools/"+url.PathEscape(key), nil, &tools); err != nil {
		return err
	}
	got := make([]string, 0, len(tools))
	for _, tool := range tools {
		if tool.Enabled {
			got = append(got, tool.Name)
		}
	}
	if !equalStrings(normalizeToolNames(got), normalizeToolNames(expected)) {
		return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "QwenPaw MCP tool readback mismatch", Recoverable: true}
	}
	return nil
}

func normalizeToolNames(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func (a *QwenPawAPI) listMCP(ctx context.Context) (map[string]json.RawMessage, error) {
	var clients []struct {
		Key string `json:"key"`
	}
	if err := a.doJSON(ctx, http.MethodGet, "/api/mcp", nil, &clients); err != nil {
		return nil, err
	}
	result := make(map[string]json.RawMessage, len(clients))
	for _, client := range clients {
		if client.Key != "" {
			result[client.Key] = nil
		}
	}
	return result, nil
}

func (a *QwenPawAPI) doJSON(ctx context.Context, method, path string, requestBody any, responseBody any) error {
	if a == nil || a.httpClient == nil || a.baseURL == "" {
		return kernel.Error{Code: kernel.CodeInternalError, Message: "QwenPaw API client is not configured"}
	}
	var body io.Reader
	if requestBody != nil {
		encoded, err := json.Marshal(requestBody)
		if err != nil {
			return kernel.InvalidArgument("QwenPaw API request cannot be encoded")
		}
		body = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, a.baseURL+path, body)
	if err != nil {
		return kernel.InvalidArgument("QwenPaw API request is invalid")
	}
	req.Header.Set("Accept", "application/json")
	if requestBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := a.httpClient.Do(req)
	if err != nil {
		return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "QwenPaw API is unavailable", Recoverable: true}
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: fmt.Sprintf("QwenPaw API returned HTTP %d", resp.StatusCode), Recoverable: true}
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, qwenPawResponseLimit+1))
	if err != nil {
		return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "QwenPaw API response could not be read", Recoverable: true}
	}
	if len(raw) > qwenPawResponseLimit {
		return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "QwenPaw API response exceeded the limit", Recoverable: true}
	}
	if responseBody == nil || resp.StatusCode == http.StatusNoContent {
		return nil
	}
	if err := json.Unmarshal(raw, responseBody); err != nil {
		return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "QwenPaw API returned invalid JSON", Recoverable: true}
	}
	return nil
}
