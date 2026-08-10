package agentteams

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	}, nil
}

func InvocationMCPKey(invocationID kernel.InvocationID) (string, error) {
	if err := kernel.RequireID("invocation_id", invocationID); err != nil {
		return "", err
	}
	sum := sha256.Sum256([]byte(invocationID))
	return "threadmill-" + hex.EncodeToString(sum[:12]), nil
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
		var tools []struct {
			Name    string `json:"name"`
			Enabled bool   `json:"enabled"`
		}
		err := a.doJSON(waitCtx, http.MethodGet, "/api/mcp/tools/"+url.PathEscape(key), nil, &tools)
		if err == nil {
			got := make([]string, 0, len(tools))
			for _, tool := range tools {
				if tool.Enabled {
					got = append(got, tool.Name)
				}
			}
			sort.Strings(got)
			if equalStrings(got, want) {
				return nil
			}
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
