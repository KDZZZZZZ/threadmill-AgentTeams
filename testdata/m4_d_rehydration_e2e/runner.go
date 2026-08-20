//go:build ignore

// Local-only, auditable M4-D Docker runner. It is intentionally excluded from
// normal package builds; run.ps1 invokes it with `go run`.
package main

import (
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
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/agenthost/agentteams"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/artifacts"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/executionreceipt"
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
	matrixURL := mustEnv("M4D_MATRIX_URL")
	matrixAdminToken, err := matrixLogin(ctx, matrixURL, mustEnv("M4D_MATRIX_ADMIN_USER"), mustEnv("M4D_MATRIX_ADMIN_PASSWORD"))
	must(err)
	must(os.Setenv("AGENTTEAMS_WORKER_MATRIX_TOKEN", matrixAdminToken))
	defer os.Unsetenv("AGENTTEAMS_WORKER_MATRIX_TOKEN")
	resultPath := mustEnv("M4D_RESULT")
	docker := mustEnv("M4D_DOCKER")
	workspace, err := os.MkdirTemp("", "threadmill-m4d-workspace-")
	must(err)
	defer os.RemoveAll(workspace)
	must(os.MkdirAll(filepath.Join(workspace, "out"), 0o755))
	must(os.WriteFile(filepath.Join(workspace, "out", "rehydration.txt"), []byte("M4-D fixture report\n"), 0o600))

	registry := phasemcp.NewBindingRegistry()
	physical := runtime.NewInMemoryPhysicalExecutionStore()
	receipts := executionreceipt.NewInMemoryStore()
	receiptAuthority := &runtime.PackageConsumptionCoordinator{Store: receipts, PhysicalExecutions: physical}
	recorder := &eventRecorder{}
	artifactRegistry := artifacts.NewInMemoryRegistry(recorder)
	handler, err := phasemcp.NewHandler(registry, artifactRegistry, recorder, receiptAuthority)
	must(err)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	must(err)
	trace := &mcpTrace{next: phasemcp.NewHTTPServer(handler), registry: registry}
	server := &http.Server{Handler: trace}
	go func() { _ = server.Serve(listener) }()
	defer server.Shutdown(context.Background())
	mcpURL := "http://host.docker.internal:" + fmt.Sprint(listener.Addr().(*net.TCPAddr).Port) + "/mcp"

	controller := &agentteams.ControllerReprovisioner{BaseURL: controllerURL, BearerToken: controllerToken, Model: "qwen-plus", ModelProvider: "openai-compat", Runtime: "qwenpaw", Image: "threadmill/qwenpaw-worker:m4d-current", PollInterval: time.Second}
	success, err := runScenario(ctx, scenarioConfig{TaskID: taskID, InvocationID: invocationID, Workspace: workspace, Docker: docker, MCPURL: mcpURL, MatrixURL: matrixURL, MatrixToken: matrixAdminToken, Controller: controller, Registry: registry, Trace: trace, Physical: physical, Receipts: receipts, ConflictAfterActivation: false})
	must(err)
	if !success.TokenARevoked || !success.TokenBResolvable || success.Record.State != runtime.PhysicalExecutionRunning || success.Waiting.State != runtime.AwaitStateRunning {
		panic("successful M4-D activation evidence was incomplete")
	}
	receipt, found, err := receipts.Get(ctx, executionreceipt.Key{TaskID: success.Record.TaskID, InvocationID: success.Record.InvocationID, Generation: success.Record.Generation, ExecutionEpoch: int64(success.Record.ExecutionEpoch)})
	must(err)
	if !found || !receipt.Consumed || receipt.PackageDigest != success.Record.AgentPackageDigest || receipt.SessionIdentity != success.Record.AgentSessionRef || !trace.observedTool(phasemcp.ToolConfirmPackageConsumption) {
		panic("agent-originated package consumption receipt evidence was incomplete")
	}
	conflict, err := runScenario(ctx, scenarioConfig{TaskID: "m4d-conflict-task", InvocationID: "m4d-conflict-invocation", Workspace: workspace, Docker: docker, MCPURL: mcpURL, MatrixURL: matrixURL, MatrixToken: matrixAdminToken, Controller: controller, Registry: registry, Trace: trace, Physical: physical, Receipts: receipts, ConflictAfterActivation: true})
	must(err)
	if !conflict.ConflictObserved || conflict.TokenBResolvable || conflict.Record.State != runtime.PhysicalExecutionFailed {
		panic("M4-D final-CAS conflict teardown evidence was incomplete")
	}
	output, _ := json.MarshalIndent(map[string]any{"taskID": success.Record.TaskID, "invocationID": success.Record.InvocationID, "generation": success.Record.Generation, "previousExecutionEpoch": 1, "executionEpoch": success.Record.ExecutionEpoch, "worker": success.Record.WorkerName, "taskB": success.Record.TeamHarnessTaskID, "activationStatus": success.Record.ObservedTaskStatus, "assignedWorker": success.Record.TeamHarnessAssignedTo, "credentialRef": success.Record.CredentialBindingRef, "tokenSHA256": success.TokenBDigest, "oldTokenRevoked": success.TokenARevoked, "tokenBResolvableBeforeTeardown": success.TokenBResolvable, "desiredGeneration": success.Record.DesiredRuntimeGeneration, "appliedGeneration": success.Record.AppliedRuntimeGeneration, "mcpApplied": true, "physicalExecutionRevision": success.Record.Revision, "physicalEpochs": success.PhysicalEpochs, "waitingState": success.Waiting.State, "waitingRevision": success.Waiting.Revision, "casConflictTeardown": conflict.ConflictObserved, "conflictTokenBResolvableAfterTeardown": conflict.TokenBResolvable, "events": recorder.events}, "", "  ")
	must(os.WriteFile(resultPath, output, 0o600))
}

func rehydratingPlan(task, invocation string) (*runtime.InMemoryWaitingStore, runtime.RehydrationPlan) {
	endpoint := phaseagent.PhaseEndpointRef{TaskID: task, EndpointID: string(phaseagent.PhaseExecute)}
	role, err := phaseagent.RoleForEndpoint(endpoint)
	must(err)
	start := phaseagent.StartPhaseInput{InvocationID: invocation, Endpoint: endpoint, Generation: generation, BindingRef: "binding-r5", Inputs: phaseagent.PhaseInputSet{InputRevision: "input-r5"}}
	invocationContext, err := phaseagent.NewInvocationContext(start)
	must(err)
	store := runtime.NewInMemoryWaitingStore()
	record, err := store.Create(context.Background(), runtime.WaitingRecord{Key: runtime.WaitingKey{TaskID: task, InvocationID: invocation, Generation: generation}, ExecutionEpoch: 1, Endpoint: endpoint, PreviousBindingRef: "binding-r4", InputRevision: "input-r4", ContinuationRef: "continuation-r4", State: runtime.AwaitStateRehydrating, WorkspaceRef: "workspace-m4d", AllowedDirs: []string{"out"}, ContextSliceRef: "slice-r4", TaskMemoryBufferRef: "memory-r4"})
	must(err)
	return store, runtime.RehydrationPlan{TaskID: task, InvocationID: invocation, Generation: generation, NextExecutionEpoch: 2, Endpoint: endpoint, NewBindingRef: "binding-r5", NewInputRevision: "input-r5", Inputs: start.Inputs, Execution: phaseagent.ExecutionContext{Invocation: invocationContext, Role: role, Runtime: runtimeStub{}, ContextReader: readerStub{}, ContextAgent: agentStub{}}, Workspace: runtime.WorkspaceBinding{Ref: "workspace-m4d", AllowedDirs: []string{"out"}}, Context: runtime.RehydratedContext{SliceRef: "slice-r4"}, TaskMemory: runtime.RehydratedTaskMemory{BufferRef: "memory-r4"}, ContinuationRef: "continuation-r4", ExpectedWaitingRevision: record.Revision}
}

func issueOldToken(registry *phasemcp.BindingRegistry, plan runtime.RehydrationPlan) string {
	binding, err := registry.IssueExecution(plan.Execution, plan.Workspace.AllowedDirs, "old-permission", time.Now().Add(time.Minute))
	must(err)
	return binding.Token
}

type scenarioConfig struct {
	TaskID, InvocationID                 string
	Workspace, Docker, MCPURL, MatrixURL string
	MatrixToken                          string
	Controller                           *agentteams.ControllerReprovisioner
	Registry                             *phasemcp.BindingRegistry
	Trace                                *mcpTrace
	Physical                             runtime.PhysicalExecutionStore
	Receipts                             executionreceipt.Store
	ConflictAfterActivation              bool
}

type scenarioResult struct {
	Record           runtime.PhysicalExecution
	Waiting          runtime.WaitingRecord
	PhysicalEpochs   int
	TokenARevoked    bool
	TokenBResolvable bool
	TokenBDigest     string
	ConflictObserved bool
}

func runScenario(ctx context.Context, config scenarioConfig) (scenarioResult, error) {
	store, plan := rehydratingPlan(config.TaskID, config.InvocationID)
	physical := config.Physical
	// Historical epoch-A contains only durable, redacted evidence. It has no
	// invented TeamHarness task ID, so epoch-B cannot overwrite it.
	if _, err := physical.Create(ctx, runtime.PhysicalExecution{TaskID: plan.TaskID, InvocationID: plan.InvocationID, Generation: plan.Generation, ExecutionEpoch: 1, State: runtime.PhysicalExecutionTerminated, EvidenceRefs: []string{"event:await-relinquished"}}); err != nil {
		return scenarioResult{}, err
	}
	old := issueOldToken(config.Registry, plan)
	config.Registry.Revoke(old)
	if _, err := config.Registry.Resolve(old); !errors.Is(err, phasemcp.ErrInvalidToken) {
		return scenarioResult{}, errors.New("old token remains resolvable")
	}

	leases := &localLeases{root: config.Workspace}
	issuer := &recordingIssuer{inner: runtime.BindingRegistryAuthorizationIssuer{Registry: config.Registry, PermissionRef: "m4d-permission", TTL: 5 * time.Minute}}
	workerName := fmt.Sprintf("tm-%s-g%d-e%d", config.InvocationID, generation, 2)
	baseTaskflow := agentteams.TeamHarnessStdioClient{
		Python: "/opt/venv/qwenpaw/bin/python",
		// The official worker image packages TeamHarness as a QwenPaw built-in
		// plugin rather than a source checkout. This is the image's inspected
		// runtime path, not a copied or fixture-local state machine.
		ServerPath:    "/opt/agentteams/qwenpaw-builtin/plugins/teamharness/teamharness/mcp/server.py",
		Workspace:     "/root/agentteams-fs",
		CommandPrefix: []string{config.Docker, "exec", "-i", "-e", "AGENTTEAMS_WORKER_MATRIX_TOKEN", "agentteams-worker-" + workerName},
	}
	tracedTaskflow := &matrixObservedTaskflow{client: baseTaskflow, docker: config.Docker, workerName: workerName, matrixURL: config.MatrixURL, matrixToken: config.MatrixToken}
	activator := &agentteams.RehydratedTeamHarnessTaskActivator{Controller: config.Controller, Taskflow: tracedTaskflow, ProjectID: "threadmill-m4d", PollInterval: time.Second}
	var tasks runtime.TeamHarnessTaskProvisioner = activator
	if config.ConflictAfterActivation {
		tasks = conflictAfterActivation{next: activator, store: store, key: runtime.WaitingKey{TaskID: plan.TaskID, InvocationID: plan.InvocationID, Generation: plan.Generation}}
	}
	provisioner := &runtime.PhysicalExecutionProvisioner{
		Store: store, PhysicalExecutions: physical, Leases: leases, Tokens: issuer, Credentials: config.Controller,
		Workers: config.Controller, MCP: config.Controller, Runtime: config.Controller,
		Discovery: qwenPawDiscovery{docker: config.Docker, trace: config.Trace}, Tasks: tasks, Packages: e2ePackageMaterializer{}, Receipts: config.Receipts,
		MCPName: "threadmill", MCPURL: config.MCPURL, Transport: "streamable_http",
	}
	execution, err := provisioner.Provision(ctx, plan)
	if config.ConflictAfterActivation {
		if err == nil {
			return scenarioResult{}, errors.New("final CAS conflict unexpectedly succeeded")
		}
		stored, found, getErr := physical.Get(ctx, runtime.PhysicalExecutionKey{TaskID: plan.TaskID, InvocationID: plan.InvocationID, Generation: plan.Generation, ExecutionEpoch: 2})
		if getErr != nil || !found {
			return scenarioResult{}, fmt.Errorf("conflict physical execution missing: %v", getErr)
		}
		_, resolveErr := config.Registry.Resolve(issuer.lastToken)
		waiting, _, getErr := store.Get(ctx, runtime.WaitingKey{TaskID: plan.TaskID, InvocationID: plan.InvocationID, Generation: plan.Generation})
		if getErr != nil {
			return scenarioResult{}, getErr
		}
		return scenarioResult{Record: stored, Waiting: waiting, TokenARevoked: true, TokenBResolvable: resolveErr == nil, TokenBDigest: tokenDigest(issuer.lastToken), ConflictObserved: true}, nil
	}
	if err != nil {
		return scenarioResult{}, err
	}
	if _, err := config.Registry.Resolve(issuer.lastToken); err != nil {
		return scenarioResult{}, fmt.Errorf("token-B did not resolve before teardown: %w", err)
	}
	credentialView, err := redactedCredentialGet(ctx, config.Controller.BaseURL, config.Controller.BearerToken, execution.CredentialBindingRef)
	if err != nil || credentialView.ID != execution.CredentialBindingRef || credentialView.WorkerName != execution.WorkerName || credentialView.HeaderName != phasemcp.ExecutionTokenHeader || credentialView.State != "active" {
		return scenarioResult{}, errors.New("credential redacted readback mismatch")
	}
	all, err := physical.ListByInvocation(ctx, plan.TaskID, plan.InvocationID, plan.Generation)
	if err != nil || len(all) != 2 {
		return scenarioResult{}, fmt.Errorf("physical epoch history mismatch: records=%d err=%v", len(all), err)
	}
	waiting, found, err := store.Get(ctx, runtime.WaitingKey{TaskID: plan.TaskID, InvocationID: plan.InvocationID, Generation: plan.Generation})
	if err != nil || !found {
		return scenarioResult{}, fmt.Errorf("waiting record missing: %v", err)
	}
	result := scenarioResult{Record: execution, Waiting: waiting, PhysicalEpochs: len(all), TokenARevoked: true, TokenBResolvable: true, TokenBDigest: tokenDigest(issuer.lastToken)}
	// Fixture cleanup happens only after evidence is captured; it does not alter
	// the returned logical running record.
	_ = activator.CancelTeamHarnessTask(context.Background(), runtime.TeamHarnessTask{ID: execution.TeamHarnessTaskID})
	_ = config.Controller.DeleteWorker(context.Background(), runtime.ProvisionedWorker{ID: execution.WorkerID, Name: execution.WorkerName})
	_ = config.Controller.RevokeMCPCredential(context.Background(), runtime.MCPCredentialBinding{Ref: execution.CredentialBindingRef, WorkerName: execution.WorkerName})
	_ = issuer.RevokeExecutionAuthorization(context.Background(), runtime.IssuedExecutionAuthorization{Token: issuer.lastToken, Ref: execution.ExecutionAuthorizationRef})
	_ = leases.ReleaseWorkspaceLease(context.Background(), runtime.WorkspaceLease{Ref: execution.WorkspaceLeaseRef})
	return result, nil
}

type e2ePackageMaterializer struct{}

func (e2ePackageMaterializer) MaterializeRehydratedExecution(_ context.Context, plan runtime.RehydrationPlan) (runtime.RehydratedExecutionPackage, error) {
	return runtime.RehydratedExecutionPackage{
		TaskID: plan.TaskID, InvocationID: plan.InvocationID, Generation: plan.Generation, ExecutionEpoch: plan.NextExecutionEpoch,
		Endpoint: plan.Endpoint, BindingRef: plan.NewBindingRef, InputRevision: plan.NewInputRevision, Inputs: plan.Inputs, NewlyDelivered: plan.NewlyDelivered,
		TaskContract: "M4-D carrier validation", PhaseInstruction: "activate the rehydrated physical execution",
		Context: runtime.AgentVisibleContext{SliceRef: plan.Context.SliceRef, BaselineRef: plan.Context.BaselineRef}, TaskMemory: plan.TaskMemory.View,
		Workspace:    runtime.AgentVisibleWorkspace{Ref: plan.Workspace.Ref, Revision: plan.Workspace.Revision, AllowedDirs: append([]string(nil), plan.Workspace.AllowedDirs...)},
		ArtifactRefs: append([]artifacts.ArtifactRef(nil), plan.ArtifactRefs...), EventRefs: append([]string(nil), plan.EventRefs...), EvidenceRefs: append([]string(nil), plan.EvidenceRefs...),
	}, nil
}

type recordingIssuer struct {
	inner     runtime.BindingRegistryAuthorizationIssuer
	lastToken string
}

func (i *recordingIssuer) IssueExecutionAuthorization(ctx context.Context, plan runtime.RehydrationPlan, lease runtime.WorkspaceLease) (runtime.IssuedExecutionAuthorization, error) {
	authorization, err := i.inner.IssueExecutionAuthorization(ctx, plan, lease)
	if err == nil {
		i.lastToken = authorization.Token
	}
	return authorization, err
}
func (i *recordingIssuer) RevokeExecutionAuthorization(ctx context.Context, authorization runtime.IssuedExecutionAuthorization) error {
	return i.inner.RevokeExecutionAuthorization(ctx, authorization)
}

type qwenPawDiscovery struct {
	docker string
	trace  *mcpTrace
}

// matrixObservedTaskflow keeps the focused fixture on the production
// delegate_task path while proving the Worker channel crossed its initial
// callback-suppressed sync boundary before delegation. The delegator is the
// embedded test admin, distinct from the Worker-B assignee.
type matrixObservedTaskflow struct {
	client      agentteams.TeamHarnessStdioClient
	docker      string
	workerName  string
	matrixURL   string
	matrixToken string
}

func (c *matrixObservedTaskflow) DelegateTask(ctx context.Context, request agentteams.TeamHarnessDelegateTaskRequest) error {
	if err := waitForWorkerMatrixChannel(ctx, c.docker, c.workerName); err != nil {
		return err
	}
	if err := c.client.DelegateTask(ctx, request); err != nil {
		return err
	}
	if err := waitForAssignmentEvent(ctx, c.matrixURL, c.matrixToken, request); err != nil {
		return err
	}
	return nil
}

func (c *matrixObservedTaskflow) CheckTask(ctx context.Context, taskID string) (agentteams.TeamHarnessTaskSnapshot, error) {
	snapshot, err := c.client.CheckTask(ctx, taskID)
	if err != nil {
		fmt.Printf("[taskflow] check task=%s error=%v\n", taskID, err)
		return snapshot, err
	}
	fmt.Printf("[taskflow] check task=%s status=%s acknowledged=%t assignee=%s\n", taskID, snapshot.Status, snapshot.Acknowledged, snapshot.AssignedTo)
	return snapshot, nil
}

func (c *matrixObservedTaskflow) CancelTask(ctx context.Context, taskID, reason string) error {
	return c.client.CancelTask(ctx, taskID, reason)
}

func waitForWorkerMatrixChannel(ctx context.Context, docker, workerName string) error {
	return waitForWorkerLog(ctx, docker, workerName, func(logs string) bool {
		return strings.Contains(logs, "MatrixChannel: sync loop started") &&
			(strings.Contains(logs, "MatrixChannel: catch-up sync done") || strings.Contains(logs, "MatrixChannel: restored token, performing full-state sync"))
	}, "QwenPaw Matrix channel did not reach incremental-sync readiness")
}

func waitForWorkerLog(ctx context.Context, docker, workerName string, accepted func(string) bool, failure string) error {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	container := "agentteams-worker-" + workerName
	for {
		command := exec.CommandContext(ctx, docker, "logs", container)
		output, err := command.CombinedOutput()
		if err == nil && accepted(string(output)) {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s: %w", failure, ctx.Err())
		case <-ticker.C:
		}
	}
}

func matrixLogin(ctx context.Context, baseURL, user, password string) (string, error) {
	payload, _ := json.Marshal(map[string]any{"type": "m.login.password", "identifier": map[string]string{"type": "m.id.user", "user": user}, "password": password})
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(baseURL, "/")+"/_matrix/client/v3/login", bytes.NewReader(payload))
	if err != nil {
		return "", err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", fmt.Errorf("Matrix test delegator login failed with HTTP %d", response.StatusCode)
	}
	var result struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return "", err
	}
	if result.AccessToken == "" {
		return "", errors.New("Matrix test delegator login returned no access token")
	}
	return result.AccessToken, nil
}

func waitForAssignmentEvent(ctx context.Context, baseURL, token string, request agentteams.TeamHarnessDelegateTaskRequest) error {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		endpoint := strings.TrimRight(baseURL, "/") + "/_matrix/client/v3/rooms/" + url.PathEscape(request.RoomID) + "/messages?dir=b&limit=50"
		httpRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return err
		}
		httpRequest.Header.Set("Authorization", "Bearer "+token)
		response, err := http.DefaultClient.Do(httpRequest)
		if err == nil {
			var result struct {
				Chunk []struct {
					Sender  string `json:"sender"`
					EventID string `json:"event_id"`
					Content struct {
						Body string `json:"body"`
					} `json:"content"`
				} `json:"chunk"`
			}
			decodeErr := json.NewDecoder(response.Body).Decode(&result)
			response.Body.Close()
			if decodeErr == nil {
				for _, event := range result.Chunk {
					if event.EventID != "" && strings.Contains(event.Content.Body, request.TaskID) {
						if event.Sender == request.Assignee {
							return errors.New("TASK_ASSIGNED was self-authored by Worker-B")
						}
						return nil
					}
				}
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("Matrix TASK_ASSIGNED event was not observable in Worker-B room: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func (d qwenPawDiscovery) DiscoverMCPTools(ctx context.Context, worker runtime.ProvisionedWorker, authorization runtime.IssuedExecutionAuthorization) ([]string, error) {
	d.trace.expectedDigest = tokenDigest(authorization.Token)
	if err := triggerQwenPawToolDiscovery(ctx, d.docker, worker.Name); err != nil {
		return nil, err
	}
	if !d.trace.waitForExpected(ctx) {
		return nil, errors.New("QwenPaw did not reach Threadmill MCP tools/list with token-B")
	}
	return []string{"artifact.register", "agent.submitPhaseOutput"}, nil
}

// acknowledgingTaskflow is a deterministic worker driver for this integration
// fixture only. It invokes the official ack_task action; task state remains in
// the official TeamHarness store and is subsequently read via check_task.
type acknowledgingTaskflow struct {
	client agentteams.TeamHarnessStdioClient
	acked  bool
}

func (c *acknowledgingTaskflow) DelegateTask(ctx context.Context, request agentteams.TeamHarnessDelegateTaskRequest) error {
	return c.client.DelegateTask(ctx, request)
}
func (c *acknowledgingTaskflow) CheckTask(ctx context.Context, taskID string) (agentteams.TeamHarnessTaskSnapshot, error) {
	snapshot, err := c.client.CheckTask(ctx, taskID)
	if err != nil || c.acked || snapshot.Status != agentteams.TeamHarnessTaskAssigned {
		return snapshot, err
	}
	if err := c.client.AcknowledgeTask(ctx, taskID); err != nil {
		return agentteams.TeamHarnessTaskSnapshot{}, err
	}
	c.acked = true
	return c.client.CheckTask(ctx, taskID)
}
func (c *acknowledgingTaskflow) CancelTask(ctx context.Context, taskID, reason string) error {
	return c.client.CancelTask(ctx, taskID, reason)
}

type conflictAfterActivation struct {
	next  runtime.TeamHarnessTaskProvisioner
	store *runtime.InMemoryWaitingStore
	key   runtime.WaitingKey
}

func (c conflictAfterActivation) CreateTeamHarnessTask(ctx context.Context, request runtime.TeamHarnessTaskRequest) (runtime.TeamHarnessTask, error) {
	task, err := c.next.CreateTeamHarnessTask(ctx, request)
	if err != nil {
		return runtime.TeamHarnessTask{}, err
	}
	record, found, err := c.store.Get(ctx, c.key)
	if err != nil || !found {
		return runtime.TeamHarnessTask{}, errors.New("fixture could not create final-CAS conflict")
	}
	if _, swapped, err := c.store.CompareAndSwap(ctx, c.key, record.Revision, record); err != nil || !swapped {
		return runtime.TeamHarnessTask{}, errors.New("fixture could not advance waiting revision")
	}
	return task, nil
}
func (c conflictAfterActivation) CancelTeamHarnessTask(ctx context.Context, task runtime.TeamHarnessTask) error {
	return c.next.CancelTeamHarnessTask(ctx, task)
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
	Tool   string
}

func (t *mcpTrace) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	r.Body = io.NopCloser(bytes.NewReader(body))
	var request struct {
		Method string `json:"method"`
		Params struct {
			Name string `json:"name"`
		} `json:"params"`
	}
	_ = json.Unmarshal(body, &request)
	token := r.Header.Get(phasemcp.ExecutionTokenHeader)
	if token != "" {
		if _, err := t.registry.Resolve(token); err == nil {
			t.mu.Lock()
			t.calls = append(t.calls, mcpTraceCall{Digest: tokenDigest(token), Method: request.Method, Tool: request.Params.Name})
			t.mu.Unlock()
		}
	}
	if request.Method == "tools/call" && request.Params.Name == phasemcp.ToolConfirmPackageConsumption {
		fmt.Println("[mcp] confirmPackageConsumption entered with authenticated token")
		defer fmt.Println("[mcp] confirmPackageConsumption returned")
	}
	t.next.ServeHTTP(w, r)
}

func (t *mcpTrace) observedTool(name string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, call := range t.calls {
		if call.Tool == name && call.Method == "tools/call" {
			return true
		}
	}
	return false
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
policy = json.dumps({"default_effect":"allow","client_overrides":[],"tool_defaults":[],"tool_overrides":[]}).encode()
policy_request = urllib.request.Request("http://127.0.0.1:8088/api/mcp/policy/threadmill", data=policy, headers={"Content-Type":"application/json"}, method="PUT")
with urllib.request.urlopen(policy_request, timeout=20) as response:
    if response.status != 200:
        raise RuntimeError("Threadmill MCP policy write failed")
with urllib.request.urlopen("http://127.0.0.1:8088/api/mcp/policy/threadmill", timeout=20) as response:
    actual = json.load(response)
    if actual.get("default_effect") != "allow":
        raise RuntimeError("Threadmill MCP policy readback mismatch")
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
