package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	phasemcp "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/mcp/phase"
)

type registryAuthorizationIssuer struct {
	registry *phasemcp.BindingRegistry
	fail     bool
	active   map[string]bool
}

func (i *registryAuthorizationIssuer) IssueExecutionAuthorization(_ context.Context, plan RehydrationPlan, lease WorkspaceLease) (IssuedExecutionAuthorization, error) {
	if i.fail {
		return IssuedExecutionAuthorization{}, errors.New("token issue failed")
	}
	binding, err := i.registry.IssueExecutionWithWorkspace(plan.Execution, "", lease.AllowedDirs, "permission-e2", time.Now().Add(time.Hour))
	if err != nil {
		return IssuedExecutionAuthorization{}, err
	}
	i.active[binding.Token] = true
	return IssuedExecutionAuthorization{Token: binding.Token, Ref: "authorization-e2"}, nil
}
func (i *registryAuthorizationIssuer) RevokeExecutionAuthorization(_ context.Context, authorization IssuedExecutionAuthorization) error {
	i.registry.Revoke(authorization.Token)
	delete(i.active, authorization.Token)
	return nil
}

type provisionPorts struct {
	fail             string
	store            *InMemoryWaitingStore
	leases           map[string]bool
	credentials      map[string]bool
	workers          map[string]bool
	mcp              map[string]bool
	tasks            map[string]bool
	lastCredential   MCPCredentialRequest
	discoveryStarted chan struct{}
	discoveryRelease chan struct{}
}

func newProvisionPorts(store *InMemoryWaitingStore) *provisionPorts {
	return &provisionPorts{store: store, leases: map[string]bool{}, credentials: map[string]bool{}, workers: map[string]bool{}, mcp: map[string]bool{}, tasks: map[string]bool{}}
}
func (p *provisionPorts) AcquireWorkspaceLease(_ context.Context, plan RehydrationPlan) (WorkspaceLease, error) {
	if p.fail == "lease" {
		return WorkspaceLease{}, errors.New("lease failed")
	}
	p.leases["lease-e2"] = true
	return WorkspaceLease{Ref: "lease-e2", WorkspaceRef: plan.Workspace.Ref, AllowedDirs: append([]string(nil), plan.Workspace.AllowedDirs...), Epoch: plan.NextExecutionEpoch}, nil
}
func (p *provisionPorts) ReleaseWorkspaceLease(_ context.Context, lease WorkspaceLease) error {
	delete(p.leases, lease.Ref)
	return nil
}
func (p *provisionPorts) CreateMCPCredential(_ context.Context, request MCPCredentialRequest) (MCPCredentialBinding, error) {
	p.lastCredential = request
	if p.fail == "credential" {
		return MCPCredentialBinding{}, errors.New("credential failed")
	}
	p.credentials["credential-e2"] = true
	return MCPCredentialBinding{Ref: "credential-e2", WorkerName: request.WorkerName}, nil
}
func (p *provisionPorts) RevokeMCPCredential(_ context.Context, credential MCPCredentialBinding) error {
	delete(p.credentials, credential.Ref)
	return nil
}
func (p *provisionPorts) ProvisionWorker(_ context.Context, request WorkerProvisionRequest) (ProvisionedWorker, error) {
	if p.fail == "worker" {
		return ProvisionedWorker{}, errors.New("worker failed")
	}
	p.workers[request.WorkerName] = true
	p.mcp["threadmill-e2"] = true
	return ProvisionedWorker{ID: "worker-e2", Name: request.WorkerName, RuntimeGeneration: 9, MCPClientID: "threadmill-e2"}, nil
}
func (p *provisionPorts) DeleteWorker(_ context.Context, worker ProvisionedWorker) error {
	delete(p.workers, worker.Name)
	return nil
}
func (p *provisionPorts) CleanupWorkerMCP(_ context.Context, worker ProvisionedWorker) error {
	delete(p.mcp, worker.MCPClientID)
	return nil
}
func (p *provisionPorts) WaitForRuntimeReady(_ context.Context, worker ProvisionedWorker) (RuntimeReadback, error) {
	if p.fail == "runtime" {
		return RuntimeReadback{}, errors.New("runtime apply failed")
	}
	return RuntimeReadback{BackendRunning: true, APIVersionReady: true, DesiredGeneration: worker.RuntimeGeneration, AppliedGeneration: worker.RuntimeGeneration, MCPClientID: worker.MCPClientID, MCPApplied: true, HeaderNames: []string{"X-Threadmill-Execution-Token"}}, nil
}
func (p *provisionPorts) DiscoverMCPTools(_ context.Context, _ ProvisionedWorker, _ IssuedExecutionAuthorization) ([]string, error) {
	if p.fail == "discovery" {
		return nil, errors.New("discovery failed")
	}
	if p.discoveryStarted != nil {
		close(p.discoveryStarted)
		<-p.discoveryRelease
	}
	return []string{"artifact.register", "agent.submitPhaseOutput"}, nil
}
func (p *provisionPorts) CreateTeamHarnessTask(_ context.Context, request TeamHarnessTaskRequest) (TeamHarnessTask, error) {
	if p.fail == "task" {
		return TeamHarnessTask{}, errors.New("task failed")
	}
	if p.fail == "final-cas" {
		record, _, _ := p.store.Get(context.Background(), WaitingKey{TaskID: request.Plan.TaskID, InvocationID: request.Plan.InvocationID, Generation: request.Plan.Generation})
		changed := record
		changed.State = AwaitStateWaiting
		_, _, _ = p.store.CompareAndSwap(context.Background(), record.Key, record.Revision, changed)
	}
	p.tasks["team-task-e2"] = true
	return TeamHarnessTask{ID: "team-task-e2"}, nil
}
func (p *provisionPorts) CancelTeamHarnessTask(_ context.Context, task TeamHarnessTask) error {
	delete(p.tasks, task.ID)
	return nil
}

func provisionFixture(t *testing.T) (*PhysicalExecutionProvisioner, RehydrationPlan, *InMemoryWaitingStore, *registryAuthorizationIssuer, *provisionPorts) {
	t.Helper()
	coordinator, record, store, _ := rehydrationFixture(t)
	plan, err := coordinator.Prepare(context.Background(), RehydrationRequest{Key: record.Key, ExpectedWaitingRevision: record.Revision})
	if err != nil {
		t.Fatal(err)
	}
	issuer := &registryAuthorizationIssuer{registry: phasemcp.NewBindingRegistry(), active: map[string]bool{}}
	ports := newProvisionPorts(store)
	provisioner := &PhysicalExecutionProvisioner{Store: store, Leases: ports, Tokens: issuer, Credentials: ports, Workers: ports, MCP: ports, Runtime: ports, Discovery: ports, Tasks: ports, MCPName: "threadmill", MCPURL: "http://threadmill.test/mcp", Transport: "streamable_http"}
	return provisioner, plan, store, issuer, ports
}

func TestProvisionCreatesFreshCarrierAndCommitsRunning(t *testing.T) {
	provisioner, plan, store, issuer, ports := provisionFixture(t)
	old, err := issuer.registry.IssueExecution(plan.Execution, []string{"src"}, "permission-e1", time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	issuer.registry.Revoke(old.Token)
	execution, err := provisioner.Provision(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if execution.TaskID != plan.TaskID || execution.InvocationID != plan.InvocationID || execution.Generation != plan.Generation || execution.ExecutionEpoch != plan.NextExecutionEpoch || execution.WorkerName != "tm-invocation-a-g3-e2" || execution.TeamHarnessTaskID == "team-task-a" {
		t.Fatalf("physical execution mismatch: %#v", execution)
	}
	if len(issuer.active) != 1 || ports.lastCredential.WorkerName != execution.WorkerName || ports.lastCredential.HeaderName != "X-Threadmill-Execution-Token" || ports.lastCredential.Token == old.Token {
		t.Fatalf("fresh token/credential mismatch: active=%v credential=%#v", issuer.active, ports.lastCredential)
	}
	if _, err := issuer.registry.Resolve(old.Token); !errors.Is(err, phasemcp.ErrInvalidToken) {
		t.Fatalf("old token resolved: %v", err)
	}
	for token := range issuer.active {
		if _, err := issuer.registry.Resolve(token); err != nil {
			t.Fatalf("new token did not resolve: %v", err)
		}
	}
	record, found, err := store.Get(context.Background(), WaitingKey{TaskID: plan.TaskID, InvocationID: plan.InvocationID, Generation: plan.Generation})
	if err != nil || !found || record.State != AwaitStateRunning || record.ExecutionEpoch != 2 || record.PreviousBindingRef != "binding-r5" || record.InputRevision != "input-r5" {
		t.Fatalf("final CAS state mismatch: %#v found=%t err=%v", record, found, err)
	}
	encoded, err := json.Marshal(execution)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) == "" || string(encoded) == ports.lastCredential.Token || string(encoded) == old.Token {
		t.Fatalf("execution leaked a token: %s", encoded)
	}
}

func TestProvisionRollsBackEveryPartialCarrier(t *testing.T) {
	for _, stage := range []string{"lease", "token", "credential", "worker", "runtime", "discovery", "task", "final-cas"} {
		t.Run(stage, func(t *testing.T) {
			provisioner, plan, store, issuer, ports := provisionFixture(t)
			ports.fail, issuer.fail = stage, stage == "token"
			if _, err := provisioner.Provision(context.Background(), plan); err == nil {
				t.Fatal("provision unexpectedly succeeded")
			}
			if len(ports.leases) != 0 || len(ports.credentials) != 0 || len(ports.workers) != 0 || len(ports.mcp) != 0 || len(ports.tasks) != 0 || len(issuer.active) != 0 {
				t.Fatalf("partial carrier leaked: leases=%v credentials=%v workers=%v mcp=%v tasks=%v tokens=%v", ports.leases, ports.credentials, ports.workers, ports.mcp, ports.tasks, issuer.active)
			}
			record, _, _ := store.Get(context.Background(), WaitingKey{TaskID: plan.TaskID, InvocationID: plan.InvocationID, Generation: plan.Generation})
			if record.State != AwaitStateWaiting {
				t.Fatalf("rollback left state %s", record.State)
			}
		})
	}
}

func TestProvisionRejectsDuplicateConcurrentCarrier(t *testing.T) {
	provisioner, plan, _, _, _ := provisionFixture(t)
	// The first successful call commits running. Reusing the same plan must not
	// create a second epoch-2 carrier.
	if _, err := provisioner.Provision(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	if _, err := provisioner.Provision(context.Background(), plan); !errors.Is(err, ErrProvisionConflict) {
		t.Fatalf("duplicate plan error = %v", err)
	}
}

func TestProvisionRejectsWhileSameEpochIsInFlight(t *testing.T) {
	provisioner, plan, _, _, ports := provisionFixture(t)
	ports.discoveryStarted = make(chan struct{})
	ports.discoveryRelease = make(chan struct{})
	first := make(chan error, 1)
	go func() { _, err := provisioner.Provision(context.Background(), plan); first <- err }()
	<-ports.discoveryStarted
	if _, err := provisioner.Provision(context.Background(), plan); !errors.Is(err, ErrProvisionInProgress) {
		t.Fatalf("in-flight duplicate error = %v", err)
	}
	close(ports.discoveryRelease)
	if err := <-first; err != nil {
		t.Fatalf("first provision failed: %v", err)
	}
}

func TestBindingRegistryAuthorizationIssuerIssuesAndRevokesRedactedExecutionToken(t *testing.T) {
	_, plan, _, _, _ := provisionFixture(t)
	issuer := BindingRegistryAuthorizationIssuer{Registry: phasemcp.NewBindingRegistry(), PermissionRef: "permission-e2", TTL: time.Hour}
	authorization, err := issuer.IssueExecutionAuthorization(context.Background(), plan, WorkspaceLease{Ref: "lease-e2", WorkspaceRef: plan.Workspace.Ref, AllowedDirs: plan.Workspace.AllowedDirs, Epoch: plan.NextExecutionEpoch})
	if err != nil {
		t.Fatal(err)
	}
	if authorization.Token == "" || authorization.Ref == "" || authorization.Ref == authorization.Token {
		t.Fatalf("authorization is not redacted: %#v", authorization)
	}
	if _, err := issuer.Registry.Resolve(authorization.Token); err != nil {
		t.Fatalf("fresh token did not resolve: %v", err)
	}
	if err := issuer.RevokeExecutionAuthorization(context.Background(), authorization); err != nil {
		t.Fatal(err)
	}
	if _, err := issuer.Registry.Resolve(authorization.Token); !errors.Is(err, phasemcp.ErrInvalidToken) {
		t.Fatalf("revoked token resolved: %v", err)
	}
	encoded, err := json.Marshal(MCPCredentialRequest{WorkerName: "worker-e2", HeaderName: "X-Threadmill-Execution-Token", Token: "test-token"})
	if err != nil || strings.Contains(string(encoded), "test-token") {
		t.Fatalf("credential request token was serializable: %s err=%v", encoded, err)
	}
}
