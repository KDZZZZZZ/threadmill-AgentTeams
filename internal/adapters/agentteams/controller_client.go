package agentteams

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

const agentTeamsControllerResponseLimit = 1 << 20

type AgentTeamsControllerClient struct {
	baseURL    string
	bearer     string
	httpClient *http.Client
}

func NewAgentTeamsControllerClient(baseURL, bearer string, httpClient *http.Client) (*AgentTeamsControllerClient, error) {
	parsed, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User != nil {
		return nil, kernel.InvalidArgument("AgentTeams controller URL must be an http(s) URL without userinfo")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, kernel.InvalidArgument("AgentTeams controller URL must use http or https")
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, kernel.InvalidArgument("AgentTeams controller URL cannot contain a query or fragment")
	}
	if parsed.Hostname() == "" {
		return nil, kernel.InvalidArgument("AgentTeams controller URL must include a host")
	}
	bearer = strings.TrimSpace(bearer)
	if bearer == "" {
		return nil, kernel.InvalidArgument("AgentTeams controller bearer token is required")
	}
	if strings.IndexFunc(bearer, func(char rune) bool { return char <= 0x20 || char == 0x7f }) >= 0 {
		return nil, kernel.InvalidArgument("AgentTeams controller bearer token contains whitespace or control characters")
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	client := *httpClient
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &AgentTeamsControllerClient{
		baseURL:    strings.TrimRight(parsed.String(), "/"),
		bearer:     bearer,
		httpClient: &client,
	}, nil
}

func (c *AgentTeamsControllerClient) Check(ctx context.Context) error {
	var response struct {
		Version string `json:"version"`
	}
	return c.doJSON(ctx, http.MethodGet, "/api/v1/version", nil, &response)
}

func (c *AgentTeamsControllerClient) ListHosts(ctx context.Context) ([]HostStatus, error) {
	var workers struct {
		Workers []controllerWorker `json:"workers"`
	}
	if err := c.doJSON(ctx, http.MethodGet, "/api/v1/workers", nil, &workers); err != nil {
		return nil, err
	}
	var managers struct {
		Managers []controllerManager `json:"managers"`
	}
	if err := c.doJSON(ctx, http.MethodGet, "/api/v1/managers", nil, &managers); err != nil {
		return nil, err
	}
	hosts := make([]HostStatus, 0, len(workers.Workers)+len(managers.Managers))
	for _, worker := range workers.Workers {
		hosts = append(hosts, controllerWorkerHost(worker))
	}
	for _, manager := range managers.Managers {
		hosts = append(hosts, controllerManagerHost(manager))
	}
	sort.Slice(hosts, func(i, j int) bool { return hosts[i].Ref < hosts[j].Ref })
	return hosts, nil
}

func (c *AgentTeamsControllerClient) EnsureWorkerReady(ctx context.Context, name string) error {
	if !safeProviderID(name) {
		return kernel.InvalidArgument("AgentTeams worker name is invalid")
	}
	var response struct {
		Name  string `json:"name"`
		Phase string `json:"phase"`
	}
	return c.doJSON(ctx, http.MethodPost, "/api/v1/workers/"+url.PathEscape(name)+"/ensure-ready", nil, &response)
}

func (c *AgentTeamsControllerClient) WakeWorker(ctx context.Context, name string) error {
	if !safeProviderID(name) {
		return kernel.InvalidArgument("AgentTeams worker name is invalid")
	}
	return c.doJSON(ctx, http.MethodPost, "/api/v1/workers/"+url.PathEscape(name)+"/wake", nil, nil)
}

func (c *AgentTeamsControllerClient) SleepWorker(ctx context.Context, name string) error {
	if !safeProviderID(name) {
		return kernel.InvalidArgument("AgentTeams worker name is invalid")
	}
	return c.doJSON(ctx, http.MethodPost, "/api/v1/workers/"+url.PathEscape(name)+"/sleep", nil, nil)
}

func (c *AgentTeamsControllerClient) StopWorker(ctx context.Context, name string) error {
	if !safeProviderID(name) {
		return kernel.InvalidArgument("AgentTeams worker name is invalid")
	}
	return c.doJSON(ctx, http.MethodPut, "/api/v1/workers/"+url.PathEscape(name), map[string]string{"state": "Stopped"}, nil)
}

func (c *AgentTeamsControllerClient) StopManager(ctx context.Context, name string) error {
	if !safeProviderID(name) {
		return kernel.InvalidArgument("AgentTeams manager name is invalid")
	}
	return c.doJSON(ctx, http.MethodPut, "/api/v1/managers/"+url.PathEscape(name), map[string]string{"state": "Stopped"}, nil)
}

func (c *AgentTeamsControllerClient) doJSON(ctx context.Context, method, path string, requestBody any, responseBody any) error {
	if c == nil || c.httpClient == nil || c.baseURL == "" {
		return kernel.Error{Code: kernel.CodeInternalError, Message: "AgentTeams controller client is not configured"}
	}
	var body io.Reader
	if requestBody != nil {
		encoded, err := json.Marshal(requestBody)
		if err != nil {
			return kernel.InvalidArgument("AgentTeams controller request cannot be encoded")
		}
		body = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return kernel.InvalidArgument("AgentTeams controller request is invalid")
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.bearer)
	if requestBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "AgentTeams controller is unavailable", Recoverable: true}
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: fmt.Sprintf("AgentTeams controller returned HTTP %d", resp.StatusCode), Recoverable: true}
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, agentTeamsControllerResponseLimit+1))
	if err != nil {
		return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "AgentTeams controller response could not be read", Recoverable: true}
	}
	if len(raw) > agentTeamsControllerResponseLimit {
		return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "AgentTeams controller response exceeded the limit", Recoverable: true}
	}
	if responseBody == nil || resp.StatusCode == http.StatusNoContent {
		return nil
	}
	if err := json.Unmarshal(raw, responseBody); err != nil {
		return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "AgentTeams controller returned invalid JSON", Recoverable: true}
	}
	return nil
}

type controllerWorker struct {
	Name           string   `json:"name"`
	Phase          string   `json:"phase"`
	State          string   `json:"state"`
	Model          string   `json:"model"`
	Runtime        string   `json:"runtime"`
	Skills         []string `json:"skills"`
	ContainerState string   `json:"containerState"`
	LastHeartbeat  string   `json:"lastHeartbeat"`
	Role           string   `json:"role"`
}

type controllerManager struct {
	Name    string `json:"name"`
	Phase   string `json:"phase"`
	State   string `json:"state"`
	Model   string `json:"model"`
	Runtime string `json:"runtime"`
}

func controllerWorkerHost(worker controllerWorker) HostStatus {
	phase := worker.Phase
	if phase == "" && worker.State != "" {
		phase = worker.State
	}
	if phase == "Ready" {
		phase = "Running"
	}
	capabilities := append([]string{}, worker.Skills...)
	for _, value := range []string{worker.Runtime, worker.Model, worker.Role, worker.ContainerState} {
		if value != "" {
			capabilities = append(capabilities, value)
		}
	}
	return HostStatus{
		Ref:           worker.Name,
		Kind:          HostWorker,
		Phase:         phase,
		LastHeartbeat: parseControllerTime(worker.LastHeartbeat),
		Capacity:      1,
		Capabilities:  normalizeToolNames(capabilities),
	}
}

func controllerManagerHost(manager controllerManager) HostStatus {
	phase := manager.Phase
	if phase == "" && manager.State != "" {
		phase = manager.State
	}
	capabilities := normalizeToolNames([]string{manager.Runtime, manager.Model, "manager"})
	return HostStatus{
		Ref:           manager.Name,
		Kind:          HostManager,
		Phase:         phase,
		LastHeartbeat: time.Now().UTC(),
		Capacity:      1,
		Capabilities:  capabilities,
	}
}

func parseControllerTime(value string) time.Time {
	if value == "" {
		return time.Now().UTC()
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		parsed, err := time.Parse(layout, value)
		if err == nil {
			return parsed
		}
	}
	return time.Time{}
}
