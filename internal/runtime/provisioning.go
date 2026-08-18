package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	phasemcp "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/mcp/phase"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/phaseagent"
)

// WorkspaceLease is a physical write lease acquired for one execution epoch.
// It is intentionally separate from the logical WorkspaceBinding in a plan.
type WorkspaceLease struct {
	Ref           string
	WorkspaceRef  string
	WorkspaceRoot string
	AllowedDirs   []string
	Epoch         ExecutionEpoch
}

type WorkspaceLeaseAcquirer interface {
	AcquireWorkspaceLease(context.Context, RehydrationPlan) (WorkspaceLease, error)
	ReleaseWorkspaceLease(context.Context, WorkspaceLease) error
}

// IssuedExecutionAuthorization is ephemeral provisioning material. Token is
// deliberately unexported from PhysicalExecution and must not be persisted in
// status/events/logs. A concrete issuer may wrap BindingRegistry.Issue.
type IssuedExecutionAuthorization struct {
	Token string `json:"-"`
	Ref   string
}

type ExecutionAuthorizationIssuer interface {
	IssueExecutionAuthorization(context.Context, RehydrationPlan, WorkspaceLease) (IssuedExecutionAuthorization, error)
	RevokeExecutionAuthorization(context.Context, IssuedExecutionAuthorization) error
}

// BindingRegistryAuthorizationIssuer is the production-facing adapter for
// Threadmill's existing trusted invocation registry. Token material only
// crosses this private provisioning seam; callers receive a redacted ref.
type BindingRegistryAuthorizationIssuer struct {
	Registry      *phasemcp.BindingRegistry
	PermissionRef string
	TTL           time.Duration
}

func (i BindingRegistryAuthorizationIssuer) IssueExecutionAuthorization(_ context.Context, plan RehydrationPlan, lease WorkspaceLease) (IssuedExecutionAuthorization, error) {
	if i.Registry == nil {
		return IssuedExecutionAuthorization{}, errors.New("binding registry is required")
	}
	expires := time.Time{}
	if i.TTL > 0 {
		expires = time.Now().Add(i.TTL)
	}
	binding, err := i.Registry.IssueExecutionWithWorkspace(plan.Execution, lease.WorkspaceRoot, lease.AllowedDirs, i.PermissionRef, expires)
	if err != nil {
		return IssuedExecutionAuthorization{}, err
	}
	return IssuedExecutionAuthorization{Token: binding.Token, Ref: fmt.Sprintf("execution-auth:%s:g%d:e%d", plan.InvocationID, plan.Generation, plan.NextExecutionEpoch)}, nil
}

func (i BindingRegistryAuthorizationIssuer) RevokeExecutionAuthorization(_ context.Context, authorization IssuedExecutionAuthorization) error {
	if i.Registry == nil {
		return errors.New("binding registry is required")
	}
	i.Registry.Revoke(authorization.Token)
	return nil
}

type MCPCredentialRequest struct {
	WorkerName string
	HeaderName string
	Token      string `json:"-"`
}

type MCPCredentialBinding struct {
	Ref        string
	WorkerName string
}

type MCPCredentialProvisioner interface {
	CreateMCPCredential(context.Context, MCPCredentialRequest) (MCPCredentialBinding, error)
	RevokeMCPCredential(context.Context, MCPCredentialBinding) error
}

// WorkerProvisionRequest is desired state for the existing AgentTeams
// controller. The controller, not Threadmill, projects its runtime.yaml and
// private credential binding.
type WorkerProvisionRequest struct {
	WorkerName    string
	Plan          RehydrationPlan
	CredentialRef string
	MCPName       string
	MCPURL        string
	Transport     string
}

type ProvisionedWorker struct {
	ID                string
	Name              string
	RuntimeGeneration int64
	MCPClientID       string
}

type WorkerProvisioner interface {
	ProvisionWorker(context.Context, WorkerProvisionRequest) (ProvisionedWorker, error)
	DeleteWorker(context.Context, ProvisionedWorker) error
}

// MCPClientCleaner removes controller-projected QwenPaw MCP policy/client
// independently of worker deletion, which may be asynchronous in a backend.
type MCPClientCleaner interface {
	CleanupWorkerMCP(context.Context, ProvisionedWorker) error
}

// RuntimeReadback is redacted controller/runtime status. HeaderNames, rather
// than header values, make applied state auditable without secret disclosure.
type RuntimeReadback struct {
	BackendRunning    bool
	APIVersionReady   bool
	DesiredGeneration int64
	AppliedGeneration int64
	MCPClientID       string
	MCPApplied        bool
	HeaderNames       []string
}

type WorkerRuntimeGate interface {
	WaitForRuntimeReady(context.Context, ProvisionedWorker) (RuntimeReadback, error)
}

type MCPToolDiscoverer interface {
	DiscoverMCPTools(context.Context, ProvisionedWorker, IssuedExecutionAuthorization) ([]string, error)
}

// HTTPMCPToolDiscoverer verifies the freshly issued binding against the
// Threadmill MCP endpoint using tools/list. It is deliberately a discovery
// probe only: it cannot submit an output or advance the logical invocation.
// The opaque token is sent solely in the trusted header and is never included
// in the JSON-RPC request or returned error text.
type HTTPMCPToolDiscoverer struct {
	URL    string
	Client *http.Client
}

func (d HTTPMCPToolDiscoverer) DiscoverMCPTools(ctx context.Context, _ ProvisionedWorker, authorization IssuedExecutionAuthorization) ([]string, error) {
	if d.URL == "" || authorization.Token == "" {
		return nil, errors.New("MCP discovery endpoint and authorization are required")
	}
	payload := []byte(`{"jsonrpc":"2.0","id":"rehydration-discovery","method":"tools/list","params":{}}`)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, d.URL, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(phasemcp.ExecutionTokenHeader, authorization.Token)
	client := d.Client
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("MCP discovery failed with HTTP %d", response.StatusCode)
	}
	var rpc struct {
		Result struct {
			Tools []struct {
				Name string `json:"name"`
			} `json:"tools"`
		} `json:"result"`
		Error *struct {
			Code int `json:"code"`
		} `json:"error"`
	}
	if err := json.NewDecoder(response.Body).Decode(&rpc); err != nil {
		return nil, err
	}
	if rpc.Error != nil {
		return nil, fmt.Errorf("MCP discovery returned JSON-RPC error %d", rpc.Error.Code)
	}
	tools := make([]string, 0, len(rpc.Result.Tools))
	for _, tool := range rpc.Result.Tools {
		if tool.Name != "" {
			tools = append(tools, tool.Name)
		}
	}
	return tools, nil
}

type TeamHarnessTaskRequest struct {
	Plan      RehydrationPlan
	Worker    ProvisionedWorker
	Execution phaseagent.ExecutionContext
}

type TeamHarnessTask struct {
	ID string
}

type TeamHarnessTaskProvisioner interface {
	CreateTeamHarnessTask(context.Context, TeamHarnessTaskRequest) (TeamHarnessTask, error)
	CancelTeamHarnessTask(context.Context, TeamHarnessTask) error
}

// PhysicalExecution is the redacted carrier record for a logical Invocation.
// It intentionally has no raw token or credential value.
type PhysicalExecution struct {
	TaskID                    string
	InvocationID              string
	Generation                int
	ExecutionEpoch            ExecutionEpoch
	WorkerID                  string
	WorkerName                string
	TeamHarnessTaskID         string
	MCPClientID               string
	CredentialBindingRef      string
	DesiredRuntimeGeneration  int64
	AppliedRuntimeGeneration  int64
	WorkspaceLeaseRef         string
	ExecutionAuthorizationRef string
	// EvidenceRefs are opaque Artifact/Event evidence references. They never
	// contain private transport credentials or model/session material.
	EvidenceRefs []string
	// Revision and State belong to the physical-carrier lifecycle, not the
	// logical WaitingRecord lifecycle.
	Revision int64
	State    PhysicalExecutionState
	// ObservedTaskStatus and TaskAcknowledged are evidence returned by
	// TeamHarness; Threadmill does not synthesize taskflow states.
	ObservedTaskStatus string
	TaskAcknowledged   bool
	CreatedAt          time.Time
	UpdatedAt          time.Time
	Teardown           PhysicalExecutionTeardown
}

var (
	ErrProvisionInProgress = errors.New("physical execution provisioning is already in progress")
	ErrProvisionConflict   = errors.New("physical execution cannot be committed")
	ErrRuntimeNotReady     = errors.New("worker runtime is not ready")
	ErrMCPDiscoveryFailed  = errors.New("threadmill MCP tools are not discoverable")
)

// PhysicalExecutionProvisioner turns an M4-C plan into a fresh carrier. It
// owns no AgentTeams domain state: ports map to existing controller, QwenPaw,
// BindingRegistry, and TeamHarness seams. All partial creation is unwound in
// reverse order before the logical record is returned to waiting.
type PhysicalExecutionProvisioner struct {
	Store       WaitingStore
	Leases      WorkspaceLeaseAcquirer
	Tokens      ExecutionAuthorizationIssuer
	Credentials MCPCredentialProvisioner
	Workers     WorkerProvisioner
	MCP         MCPClientCleaner
	Runtime     WorkerRuntimeGate
	Discovery   MCPToolDiscoverer
	Tasks       TeamHarnessTaskProvisioner
	MCPName     string
	MCPURL      string
	Transport   string

	mu       sync.Mutex
	inflight map[physicalExecutionKey]struct{}
}

type physicalExecutionKey struct {
	TaskID, InvocationID string
	Generation           int
	Epoch                ExecutionEpoch
}

func (p *PhysicalExecutionProvisioner) Provision(ctx context.Context, plan RehydrationPlan) (PhysicalExecution, error) {
	if err := p.validate(plan); err != nil {
		return PhysicalExecution{}, err
	}
	key := physicalExecutionKey{TaskID: plan.TaskID, InvocationID: plan.InvocationID, Generation: plan.Generation, Epoch: plan.NextExecutionEpoch}
	p.mu.Lock()
	if p.inflight == nil {
		p.inflight = make(map[physicalExecutionKey]struct{})
	}
	if _, exists := p.inflight[key]; exists {
		p.mu.Unlock()
		return PhysicalExecution{}, ErrProvisionInProgress
	}
	p.inflight[key] = struct{}{}
	p.mu.Unlock()
	defer func() { p.mu.Lock(); delete(p.inflight, key); p.mu.Unlock() }()

	lease, err := p.Leases.AcquireWorkspaceLease(ctx, plan)
	if err != nil {
		return PhysicalExecution{}, p.rollback(ctx, plan, WorkspaceLease{}, IssuedExecutionAuthorization{}, MCPCredentialBinding{}, ProvisionedWorker{}, TeamHarnessTask{}, err)
	}
	if lease.WorkspaceRef != plan.Workspace.Ref || lease.Epoch != plan.NextExecutionEpoch || !allowedDirsWithin(lease.AllowedDirs, plan.Workspace.AllowedDirs) {
		return PhysicalExecution{}, p.rollback(ctx, plan, lease, IssuedExecutionAuthorization{}, MCPCredentialBinding{}, ProvisionedWorker{}, TeamHarnessTask{}, errors.New("acquired workspace lease is incompatible with rehydration plan"))
	}
	authorization, err := p.Tokens.IssueExecutionAuthorization(ctx, plan, lease)
	if err != nil {
		return PhysicalExecution{}, p.rollback(ctx, plan, lease, IssuedExecutionAuthorization{}, MCPCredentialBinding{}, ProvisionedWorker{}, TeamHarnessTask{}, err)
	}
	workerName := physicalWorkerName(plan)
	credential, err := p.Credentials.CreateMCPCredential(ctx, MCPCredentialRequest{WorkerName: workerName, HeaderName: "X-Threadmill-Execution-Token", Token: authorization.Token})
	if err != nil {
		return PhysicalExecution{}, p.rollback(ctx, plan, lease, authorization, MCPCredentialBinding{}, ProvisionedWorker{}, TeamHarnessTask{}, err)
	}
	if credential.Ref == "" || credential.WorkerName != workerName {
		return PhysicalExecution{}, p.rollback(ctx, plan, lease, authorization, credential, ProvisionedWorker{}, TeamHarnessTask{}, errors.New("credential binding does not belong to provisioned worker"))
	}
	worker, err := p.Workers.ProvisionWorker(ctx, WorkerProvisionRequest{WorkerName: workerName, Plan: plan, CredentialRef: credential.Ref, MCPName: p.MCPName, MCPURL: p.MCPURL, Transport: p.Transport})
	if err != nil {
		return PhysicalExecution{}, p.rollback(ctx, plan, lease, authorization, credential, ProvisionedWorker{}, TeamHarnessTask{}, err)
	}
	readback, err := p.Runtime.WaitForRuntimeReady(ctx, worker)
	if err != nil {
		return PhysicalExecution{}, p.rollback(ctx, plan, lease, authorization, credential, worker, TeamHarnessTask{}, err)
	}
	if !runtimeReady(worker, readback) {
		return PhysicalExecution{}, p.rollback(ctx, plan, lease, authorization, credential, worker, TeamHarnessTask{}, ErrRuntimeNotReady)
	}
	tools, err := p.Discovery.DiscoverMCPTools(ctx, worker, authorization)
	if err != nil {
		return PhysicalExecution{}, p.rollback(ctx, plan, lease, authorization, credential, worker, TeamHarnessTask{}, err)
	}
	if len(tools) == 0 {
		return PhysicalExecution{}, p.rollback(ctx, plan, lease, authorization, credential, worker, TeamHarnessTask{}, ErrMCPDiscoveryFailed)
	}
	task, err := p.Tasks.CreateTeamHarnessTask(ctx, TeamHarnessTaskRequest{Plan: plan, Worker: worker, Execution: plan.Execution})
	if err != nil {
		return PhysicalExecution{}, p.rollback(ctx, plan, lease, authorization, credential, worker, TeamHarnessTask{}, err)
	}
	if task.ID == "" {
		return PhysicalExecution{}, p.rollback(ctx, plan, lease, authorization, credential, worker, task, errors.New("new TeamHarness task ID is required"))
	}
	execution := PhysicalExecution{TaskID: plan.TaskID, InvocationID: plan.InvocationID, Generation: plan.Generation, ExecutionEpoch: plan.NextExecutionEpoch, WorkerID: worker.ID, WorkerName: worker.Name, TeamHarnessTaskID: task.ID, MCPClientID: readback.MCPClientID, CredentialBindingRef: credential.Ref, DesiredRuntimeGeneration: readback.DesiredGeneration, AppliedRuntimeGeneration: readback.AppliedGeneration, WorkspaceLeaseRef: lease.Ref, ExecutionAuthorizationRef: authorization.Ref, State: PhysicalExecutionDelegated}
	if err := p.commit(ctx, plan); err != nil {
		return PhysicalExecution{}, p.rollback(ctx, plan, lease, authorization, credential, worker, task, err)
	}
	return execution, nil
}

func (p *PhysicalExecutionProvisioner) commit(ctx context.Context, plan RehydrationPlan) error {
	record, found, err := p.Store.Get(ctx, WaitingKey{TaskID: plan.TaskID, InvocationID: plan.InvocationID, Generation: plan.Generation})
	if err != nil {
		return err
	}
	if !found || record.State != AwaitStateRehydrating || record.Revision != plan.ExpectedWaitingRevision || record.ExecutionEpoch+1 != plan.NextExecutionEpoch {
		return ErrProvisionConflict
	}
	running := record
	running.State, running.ExecutionEpoch, running.PreviousBindingRef, running.InputRevision = AwaitStateRunning, plan.NextExecutionEpoch, plan.NewBindingRef, plan.NewInputRevision
	_, swapped, err := p.Store.CompareAndSwap(ctx, record.Key, record.Revision, running)
	if err != nil {
		return err
	}
	if !swapped {
		return ErrProvisionConflict
	}
	return nil
}

func (p *PhysicalExecutionProvisioner) rollback(ctx context.Context, plan RehydrationPlan, lease WorkspaceLease, authorization IssuedExecutionAuthorization, credential MCPCredentialBinding, worker ProvisionedWorker, task TeamHarnessTask, cause error) error {
	if task.ID != "" {
		_ = p.Tasks.CancelTeamHarnessTask(ctx, task)
	}
	if worker.ID != "" || worker.Name != "" {
		_ = p.Workers.DeleteWorker(ctx, worker)
	}
	if worker.ID != "" || worker.Name != "" {
		_ = p.MCP.CleanupWorkerMCP(ctx, worker)
	}
	if credential.Ref != "" {
		_ = p.Credentials.RevokeMCPCredential(ctx, credential)
	}
	if authorization.Token != "" || authorization.Ref != "" {
		_ = p.Tokens.RevokeExecutionAuthorization(ctx, authorization)
	}
	if lease.Ref != "" {
		_ = p.Leases.ReleaseWorkspaceLease(ctx, lease)
	}
	if rollbackErr := p.rollbackWaiting(ctx, plan); rollbackErr != nil {
		return fmt.Errorf("provision: %w; rollback waiting: %v", cause, rollbackErr)
	}
	return cause
}

func (p *PhysicalExecutionProvisioner) rollbackWaiting(ctx context.Context, plan RehydrationPlan) error {
	record, found, err := p.Store.Get(ctx, WaitingKey{TaskID: plan.TaskID, InvocationID: plan.InvocationID, Generation: plan.Generation})
	if err != nil || !found {
		return ErrProvisionConflict
	}
	if record.State != AwaitStateRehydrating || record.Revision != plan.ExpectedWaitingRevision {
		return ErrProvisionConflict
	}
	waiting := record
	waiting.State = AwaitStateWaiting
	_, swapped, err := p.Store.CompareAndSwap(ctx, record.Key, record.Revision, waiting)
	if err != nil || !swapped {
		return ErrProvisionConflict
	}
	return nil
}

func (p *PhysicalExecutionProvisioner) validate(plan RehydrationPlan) error {
	if p == nil || p.Store == nil || p.Leases == nil || p.Tokens == nil || p.Credentials == nil || p.Workers == nil || p.MCP == nil || p.Runtime == nil || p.Discovery == nil || p.Tasks == nil {
		return errors.New("physical execution provisioner dependencies are required")
	}
	if plan.TaskID == "" || plan.InvocationID == "" || plan.Generation <= 0 || plan.NextExecutionEpoch <= 0 || plan.NewBindingRef == "" || plan.NewInputRevision == "" || plan.ExpectedWaitingRevision <= 0 {
		return errors.New("rehydration plan identity and binding are required")
	}
	if p.MCPName == "" || p.MCPURL == "" || p.Transport == "" {
		return errors.New("MCP desired configuration is required")
	}
	return nil
}

func runtimeReady(worker ProvisionedWorker, readback RuntimeReadback) bool {
	return worker.ID != "" && worker.Name != "" && readback.BackendRunning && readback.APIVersionReady && readback.MCPApplied && readback.MCPClientID != "" && readback.MCPClientID == worker.MCPClientID && readback.DesiredGeneration == worker.RuntimeGeneration && readback.AppliedGeneration == worker.RuntimeGeneration
}

func physicalWorkerName(plan RehydrationPlan) string {
	return fmt.Sprintf("tm-%s-g%d-e%d", plan.InvocationID, plan.Generation, plan.NextExecutionEpoch)
}
