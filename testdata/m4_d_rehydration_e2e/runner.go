//go:build ignore

// Local-only, auditable M4-D Docker runner. It is intentionally excluded from
// normal package builds; run.ps1 invokes it with `go run`.
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/agenthost/agentteams"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/artifacts"
	phasemcp "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/mcp/phase"
	runtime "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/phaseagent"
)

const (
	taskID       = "m4d-task"
	invocationID = "m4d-invocation"
	generation   = 3
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	controllerURL, controllerToken := mustEnv("M4D_CONTROLLER_URL"), mustEnv("M4D_CONTROLLER_TOKEN")
	resultPath := mustEnv("M4D_RESULT")
	workspace, err := os.MkdirTemp("", "threadmill-m4d-workspace-")
	must(err)
	defer os.RemoveAll(workspace)
	must(os.MkdirAll(filepath.Join(workspace, "out"), 0o755))
	must(os.WriteFile(filepath.Join(workspace, "out", "rehydration.txt"), []byte("M4-D fixture report\n"), 0o600))

	registry := phasemcp.NewBindingRegistry()
	recorder := &eventRecorder{}
	artifactRegistry := artifacts.NewInMemoryRegistry(recorder)
	handler, err := phasemcp.NewHandler(registry, artifactRegistry, recorder)
	must(err)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	must(err)
	trace := &mcpTrace{next: phasemcp.NewHTTPServer(handler), registry: registry}
	server := &http.Server{Handler: trace}
	go func() { _ = server.Serve(listener) }()
	defer server.Shutdown(context.Background())
	mcpURL := "http://host.docker.internal:" + fmt.Sprint(listener.Addr().(*net.TCPAddr).Port) + "/mcp"

	store, plan := rehydratingPlan()
	old := issueOldToken(registry, plan)
	registry.Revoke(old)
	if _, err := registry.Resolve(old); !errors.Is(err, phasemcp.ErrInvalidToken) {
		panic("old token remains resolvable")
	}
	// run.ps1 provisions the official embedded initializer with its local
	// openai-named provider. This is an opaque controller-side provider name;
	// no model completion is requested by this focused MCP discovery slice.
	controller := &agentteams.ControllerReprovisioner{BaseURL: controllerURL, BearerToken: controllerToken, Model: "qwen-plus", ModelProvider: "openai", Runtime: "qwenpaw", Image: "threadmill/qwenpaw-worker:m4d-current", PollInterval: time.Second}
	leases := localLeases{root: workspace}
	lease, err := leases.AcquireWorkspaceLease(ctx, plan)
	must(err)
	issuer := runtime.BindingRegistryAuthorizationIssuer{Registry: registry, PermissionRef: "m4d-permission", TTL: 5 * time.Minute}
	authorization, err := issuer.IssueExecutionAuthorization(ctx, plan, lease)
	must(err)
	trace.expectedDigest = tokenDigest(authorization.Token)
	workerName := fmt.Sprintf("tm-%s-g%d-e%d", invocationID, generation, 2)
	credential, err := controller.CreateMCPCredential(ctx, runtime.MCPCredentialRequest{WorkerName: workerName, HeaderName: phasemcp.ExecutionTokenHeader, Token: authorization.Token})
	must(err)
	defer func() {
		_ = controller.DeleteWorker(context.Background(), runtime.ProvisionedWorker{Name: workerName})
		_ = controller.RevokeMCPCredential(context.Background(), credential)
		_ = issuer.RevokeExecutionAuthorization(context.Background(), authorization)
		_ = leases.ReleaseWorkspaceLease(context.Background(), lease)
	}()
	credentialView, err := redactedCredentialGet(ctx, controllerURL, controllerToken, credential.Ref)
	must(err)
	if credentialView.ID != credential.Ref || credentialView.WorkerName != workerName || credentialView.HeaderName != phasemcp.ExecutionTokenHeader || credentialView.State != "active" {
		panic("credential redacted readback mismatch")
	}
	worker, err := controller.ProvisionWorker(ctx, runtime.WorkerProvisionRequest{WorkerName: workerName, Plan: plan, CredentialRef: credential.Ref, MCPName: "threadmill", MCPURL: mcpURL, Transport: "streamable_http"})
	must(err)
	readback, err := controller.WaitForRuntimeReady(ctx, worker)
	must(err)
	if !readback.MCPApplied || readback.DesiredGeneration != readback.AppliedGeneration || !contains(readback.HeaderNames, phasemcp.ExecutionTokenHeader) {
		panic("controller readback did not report applied Threadmill MCP")
	}
	// Merely connecting an MCP client is not tool discovery. Trigger the real
	// QwenPaw console agent path, which asks its configured MCP client for tool
	// schemas before it invokes the fixture-only model provider.
	must(triggerQwenPawToolDiscovery(ctx, mustEnv("M4D_DOCKER"), worker.Name))
	if !trace.waitForExpected(ctx) {
		panic("QwenPaw did not reach Threadmill MCP tools/list with token-B; observed methods: " + strings.Join(trace.observedMethods(), ","))
	}
	// Deliberately stop before TeamHarness task creation and final CAS: this
	// focused slice leaves the logical WaitingRecord in rehydrating.
	record, found, err := store.Get(ctx, runtime.WaitingKey{TaskID: taskID, InvocationID: invocationID, Generation: generation})
	must(err)
	if !found || record.State != runtime.AwaitStateRehydrating {
		panic("focused slice changed WaitingRecord state")
	}
	output, _ := json.MarshalIndent(map[string]any{"taskID": taskID, "invocationID": invocationID, "generation": generation, "executionEpoch": 2, "worker": worker.Name, "credentialRef": credential.Ref, "mcpURL": mcpURL, "tokenSHA256": trace.expectedDigest, "oldTokenRevoked": true, "desiredGeneration": readback.DesiredGeneration, "appliedGeneration": readback.AppliedGeneration, "mcpApplied": readback.MCPApplied, "headerNames": readback.HeaderNames, "qwenPawToolsListReachedThreadmill": true, "waitingState": record.State, "events": recorder.events}, "", "  ")
	must(os.WriteFile(resultPath, output, 0o600))
}

func rehydratingPlan() (*runtime.InMemoryWaitingStore, runtime.RehydrationPlan) {
	endpoint := phaseagent.PhaseEndpointRef{TaskID: taskID, EndpointID: string(phaseagent.PhaseExecute)}
	role, err := phaseagent.RoleForEndpoint(endpoint)
	must(err)
	start := phaseagent.StartPhaseInput{InvocationID: invocationID, Endpoint: endpoint, Generation: generation, BindingRef: "binding-r5", Inputs: phaseagent.PhaseInputSet{InputRevision: "input-r5"}}
	invocation, err := phaseagent.NewInvocationContext(start)
	must(err)
	store := runtime.NewInMemoryWaitingStore()
	record, err := store.Create(context.Background(), runtime.WaitingRecord{Key: runtime.WaitingKey{TaskID: taskID, InvocationID: invocationID, Generation: generation}, ExecutionEpoch: 1, Endpoint: endpoint, PreviousBindingRef: "binding-r4", InputRevision: "input-r4", ContinuationRef: "continuation-r4", State: runtime.AwaitStateRehydrating, WorkspaceRef: "workspace-m4d", AllowedDirs: []string{"out"}, ContextSliceRef: "slice-r4", TaskMemoryBufferRef: "memory-r4"})
	must(err)
	return store, runtime.RehydrationPlan{TaskID: taskID, InvocationID: invocationID, Generation: generation, NextExecutionEpoch: 2, Endpoint: endpoint, NewBindingRef: "binding-r5", NewInputRevision: "input-r5", Inputs: start.Inputs, Execution: phaseagent.ExecutionContext{Invocation: invocation, Role: role, Runtime: runtimeStub{}, ContextReader: readerStub{}, ContextAgent: agentStub{}}, Workspace: runtime.WorkspaceBinding{Ref: "workspace-m4d", AllowedDirs: []string{"out"}}, Context: runtime.RehydratedContext{SliceRef: "slice-r4"}, TaskMemory: runtime.RehydratedTaskMemory{BufferRef: "memory-r4"}, ContinuationRef: "continuation-r4", ExpectedWaitingRevision: record.Revision}
}

func issueOldToken(registry *phasemcp.BindingRegistry, plan runtime.RehydrationPlan) string {
	binding, err := registry.IssueExecution(plan.Execution, plan.Workspace.AllowedDirs, "old-permission", time.Now().Add(time.Minute))
	must(err)
	return binding.Token
}

type localLeases struct{ root string }

func (l *localLeases) AcquireWorkspaceLease(_ context.Context, plan runtime.RehydrationPlan) (runtime.WorkspaceLease, error) {
	return runtime.WorkspaceLease{Ref: "lease-e2", WorkspaceRef: plan.Workspace.Ref, WorkspaceRoot: l.root, AllowedDirs: append([]string(nil), plan.Workspace.AllowedDirs...), Epoch: plan.NextExecutionEpoch}, nil
}
func (*localLeases) ReleaseWorkspaceLease(context.Context, runtime.WorkspaceLease) error { return nil }

type eventRecorder struct{ events []artifacts.Event }

func (r *eventRecorder) Record(_ context.Context, e artifacts.Event) error {
	r.events = append(r.events, e)
	return nil
}

// mcpTrace is fixture-only observability around the production MCP handler.
// It never stores token material: only the first 16 hex characters of SHA-256
// are retained after BindingRegistry.Resolve has accepted the request.
type mcpTrace struct {
	next           http.Handler
	registry       *phasemcp.BindingRegistry
	expectedDigest string
	mu             sync.Mutex
	calls          []mcpTraceCall
}

type mcpTraceCall struct {
	Digest string
	Method string
}

func (t *mcpTrace) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	r.Body = io.NopCloser(bytes.NewReader(body))
	var request struct {
		Method string `json:"method"`
	}
	_ = json.Unmarshal(body, &request)
	token := r.Header.Get(phasemcp.ExecutionTokenHeader)
	if token != "" {
		if _, err := t.registry.Resolve(token); err == nil {
			t.mu.Lock()
			t.calls = append(t.calls, mcpTraceCall{Digest: tokenDigest(token), Method: request.Method})
			t.mu.Unlock()
		}
	}
	t.next.ServeHTTP(w, r)
}

func (t *mcpTrace) waitForExpected(ctx context.Context) bool {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		t.mu.Lock()
		for _, call := range t.calls {
			if call.Digest == t.expectedDigest && call.Method == "tools/list" {
				t.mu.Unlock()
				return true
			}
		}
		t.mu.Unlock()
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
		}
	}
}

func (t *mcpTrace) observedMethods() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	methods := make([]string, 0, len(t.calls))
	for _, call := range t.calls {
		if call.Digest == t.expectedDigest {
			methods = append(methods, call.Method)
		}
	}
	return methods
}

func triggerQwenPawToolDiscovery(ctx context.Context, docker, workerName string) error {
	probeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	script := `import json, urllib.request
payload = json.dumps({"input":[{"role":"user","content":[{"type":"text","text":"List the currently available tools."}]}],"session_id":"m4d-tool-discovery","user_id":"fixture"}).encode()
request = urllib.request.Request("http://127.0.0.1:8088/api/console/chat", data=payload, headers={"Content-Type":"application/json","X-Agent-Id":"default"}, method="POST")
with urllib.request.urlopen(request, timeout=20) as response:
    print(response.status)
`
	command := exec.CommandContext(probeCtx, docker, "exec", "agentteams-worker-"+workerName, "/opt/venv/qwenpaw/bin/python", "-c", script)
	output, err := command.CombinedOutput()
	if err != nil {
		return fmt.Errorf("QwenPaw console tool discovery failed: %T", err)
	}
	if strings.TrimSpace(string(output)) != "200" {
		return errors.New("QwenPaw console tool discovery returned a non-200 status")
	}
	return nil
}

type credentialReadback struct {
	ID         string `json:"id"`
	WorkerName string `json:"workerName"`
	HeaderName string `json:"headerName"`
	State      string `json:"state"`
}

func redactedCredentialGet(ctx context.Context, baseURL, bearer, id string) (credentialReadback, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(baseURL, "/")+"/api/v1/mcp-credentials/"+id, nil)
	if err != nil {
		return credentialReadback{}, err
	}
	req.Header.Set("Authorization", "Bearer "+bearer)
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return credentialReadback{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return credentialReadback{}, fmt.Errorf("credential readback returned HTTP %d", response.StatusCode)
	}
	var value credentialReadback
	if err := json.NewDecoder(response.Body).Decode(&value); err != nil {
		return credentialReadback{}, err
	}
	return value, nil
}

func contains(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

type runtimeStub struct{}

func (runtimeStub) AwaitInputs(context.Context, phaseagent.AwaitInputsRequest) (phaseagent.InputWaitResult, error) {
	return phaseagent.InputWaitResult{}, nil
}
func (runtimeStub) SubmitPhaseOutput(context.Context, phaseagent.PhaseOutput) error { return nil }
func (runtimeStub) ProposeOrchestration(context.Context, phaseagent.OrchestrationProposal) error {
	return nil
}
func (runtimeStub) SubmitRequirement(context.Context, phaseagent.Requirement) error { return nil }
func (runtimeStub) ListTaskMemoryCandidates(context.Context) (phaseagent.TaskMemoryBufferView, error) {
	return phaseagent.TaskMemoryBufferView{}, nil
}
func (runtimeStub) SubmitMemoryCandidate(context.Context, phaseagent.MemoryCandidate) (phaseagent.CandidateBufferedReceipt, error) {
	return phaseagent.CandidateBufferedReceipt{}, nil
}

type readerStub struct{}

func (readerStub) ListSubgraphs(context.Context, phaseagent.ListSubgraphsRequest) ([]phaseagent.ContextSubgraph, error) {
	return nil, nil
}
func (readerStub) Explore(context.Context, phaseagent.ExploreRequest) (phaseagent.ContextSliceDelta, error) {
	return phaseagent.ContextSliceDelta{}, nil
}
func (readerStub) Subscribe(context.Context, phaseagent.SubscribeRequest) (phaseagent.ContextSubscription, error) {
	return phaseagent.ContextSubscription{}, nil
}
func (readerStub) Unsubscribe(context.Context, string) error { return nil }

type agentStub struct{}

func (agentStub) Retrieve(context.Context, phaseagent.ContextRetrieveRequest) (phaseagent.ContextRetrieveResult, error) {
	return phaseagent.ContextRetrieveResult{}, nil
}

// This placeholder is intentionally fail-closed until the official TeamHarness
// server invocation is wired to the Controller-created worker in the next
// focused edit. No successful fixture run can claim task creation before then.
type containerTeamHarness struct{ docker, controllerURL, controllerToken string }

func (p *containerTeamHarness) CreateTeamHarnessTask(ctx context.Context, request runtime.TeamHarnessTaskRequest) (runtime.TeamHarnessTask, error) {
	room, err := p.roomID(ctx, request.Worker.Name)
	if err != nil {
		return runtime.TeamHarnessTask{}, err
	}
	id := fmt.Sprintf("tm-m4d-%d", time.Now().UnixNano())
	payload := map[string]any{"projectId": "threadmill-m4d", "taskId": id, "roomId": room, "assignedTo": request.Worker.Name, "title": "Threadmill rehydrated execute", "spec": "M4-D fixture; submit_task is not PhaseOutput."}
	if _, err := p.call(ctx, request.Worker.Name, "leader", "delegate_task", payload); err != nil {
		return runtime.TeamHarnessTask{}, err
	}
	return runtime.TeamHarnessTask{ID: id}, nil
}
func (p *containerTeamHarness) CancelTeamHarnessTask(context.Context, runtime.TeamHarnessTask) error {
	return nil
}
func (p *containerTeamHarness) roomID(ctx context.Context, worker string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(p.controllerURL, "/")+"/api/v1/workers/"+worker+"/status", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+p.controllerToken)
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("worker status returned HTTP %d", response.StatusCode)
	}
	var status struct {
		RoomID string `json:"roomID"`
	}
	if err := json.NewDecoder(response.Body).Decode(&status); err != nil {
		return "", err
	}
	if status.RoomID == "" {
		return "", errors.New("worker status omitted roomID")
	}
	return status.RoomID, nil
}
func (p *containerTeamHarness) call(ctx context.Context, worker, role, action string, payload map[string]any) (map[string]any, error) {
	cmd := exec.CommandContext(ctx, p.docker, "exec", "-i", "agentteams-worker-"+worker, "/opt/venv/qwenpaw/bin/python", "/opt/agentteams/plugins/teamharness/mcp/server.py")
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
	rpcRequest := map[string]any{"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": map[string]any{"name": "taskflow", "arguments": map[string]any{"role": role, "workspaceDir": "/root/agentteams-fs/shared", "action": action, "payload": payload}}}
	if err := json.NewEncoder(in).Encode(rpcRequest); err != nil {
		return nil, err
	}
	_ = in.Close()
	line, err := bufio.NewReader(out).ReadBytes('\n')
	if err != nil {
		return nil, fmt.Errorf("TeamHarness response: %w: %s", err, stderr.String())
	}
	if err := cmd.Wait(); err != nil {
		return nil, fmt.Errorf("TeamHarness process: %w: %s", err, stderr.String())
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
		return nil, errors.New(rpc.Error.Message)
	}
	if len(rpc.Result.Content) == 0 {
		return nil, errors.New("TeamHarness returned no content")
	}
	var result map[string]any
	if err := json.Unmarshal([]byte(rpc.Result.Content[0].Text), &result); err != nil {
		return nil, err
	}
	if ok, _ := result["ok"].(bool); !ok {
		return nil, fmt.Errorf("TeamHarness %s rejected", action)
	}
	return result, nil
}

func mustEnv(name string) string {
	value := os.Getenv(name)
	if value == "" {
		panic(name + " is required")
	}
	return value
}
func must(err error) {
	if err != nil {
		panic(err)
	}
}
func tokenDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:8])
}
