package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	phasemcp "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/mcp/phase"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/phaseagent"
)

type awaitCleanupPorts struct {
	lease, task, token, mcp, credential, carrier int
}

func (p *awaitCleanupPorts) ReleaseWorkspaceLease(context.Context, WaitingRecord) error {
	p.lease++
	return nil
}
func (p *awaitCleanupPorts) CancelTeamHarnessTask(context.Context, string, string) error {
	p.task++
	return nil
}
func (p *awaitCleanupPorts) RevokeExecutionToken(context.Context, string) error {
	p.token++
	return nil
}
func (p *awaitCleanupPorts) CleanupExecutionMCP(context.Context, WaitingRecord) error {
	p.mcp++
	return nil
}
func (p *awaitCleanupPorts) RevokeWorkerCredential(context.Context, WaitingRecord) error {
	p.credential++
	return nil
}
func (p *awaitCleanupPorts) ReleaseExecutionCarrier(context.Context, WaitingRecord) error {
	p.carrier++
	return nil
}

func TestAwaitContinuationCoordinatorRelinquishesAuthenticatedEpoch(t *testing.T) {
	ctx := context.Background()
	waiting := NewInMemoryWaitingStore()
	physical := NewInMemoryPhysicalExecutionStore()
	endpoint := phaseagent.PhaseEndpointRef{TaskID: "task-a", EndpointID: "execute"}
	created, err := physical.Create(ctx, PhysicalExecution{TaskID: "task-a", InvocationID: "invocation-a", Generation: 3, ExecutionEpoch: 1, WorkerID: "worker-a", WorkerName: "worker-a", TeamHarnessTaskID: "task-a-physical", TeamHarnessAssignedTo: "worker-a", BindingRef: "B1", InputRevision: "r4", AgentSessionRef: "matrix:!old:test", AgentPackageDigest: "initial-digest", PackageConsumed: true, MCPClientID: "threadmill", CredentialBindingRef: "credential-a", DesiredRuntimeGeneration: 1, AppliedRuntimeGeneration: 1, WorkspaceLeaseRef: "lease-a", ExecutionAuthorizationRef: "auth-a", State: PhysicalExecutionRunning, ObservedTaskStatus: "in_progress", TaskAcknowledged: true})
	if err != nil {
		t.Fatal(err)
	}
	continuations := NewInMemoryContinuationStore()
	ports := &awaitCleanupPorts{}
	coordinator := &AwaitContinuationCoordinator{
		Binding:  phasemcp.InvocationBinding{TaskID: "task-a", InvocationID: "invocation-a", Generation: 3, ExecutionEpoch: 1, Endpoint: endpoint, BindingRef: "B1", InputRevision: "r4", AllowedDirs: []string{"out"}},
		Delegate: noopRuntime{}, Inputs: phaseagent.PhaseInputSet{InputRevision: "r4", Pending: []phaseagent.PendingInput{{InputID: "review", FromEndpoint: phaseagent.PhaseEndpointRef{TaskID: "task-a", EndpointID: "verify"}, RequiredBy: "completion"}}},
		ContinuationRef: "continuation-r4", Continuation: ContinuationMaterial{Endpoint: endpoint, WorkspaceRef: "workspace-a", ContextSliceRef: "slice-r4", TaskMemoryBufferRef: "memory-r4", EventRefs: []string{"event-a"}}, Continuations: continuations, ExecutionToken: "test-token-a",
		Relinquisher: ExecutionRelinquisher{Store: waiting, PhysicalExecutions: physical, Leases: ports, Tasks: ports, Tokens: ports, MCP: ports, Credentials: ports, Carriers: ports},
	}
	result, err := coordinator.AwaitInputs(ctx, phaseagent.AwaitInputsRequest{InputIDs: []string{"review"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.InputRevision != "r4" || len(result.Pending) != 1 {
		t.Fatalf("unexpected wait result: %#v", result)
	}
	record, found, err := waiting.Get(ctx, WaitingKey{TaskID: "task-a", InvocationID: "invocation-a", Generation: 3})
	if err != nil || !found || record.State != AwaitStateWaiting || record.ExecutionEpoch != 1 || record.PreviousBindingRef != "B1" {
		t.Fatalf("unexpected waiting record: %#v found=%t err=%v", record, found, err)
	}
	ended, found, err := physical.Get(ctx, created.Key())
	if err != nil || !found || ended.State != PhysicalExecutionTerminated {
		t.Fatalf("unexpected epoch-A record: %#v found=%t err=%v", ended, found, err)
	}
	if !ended.Teardown.TeamHarnessTaskCancelled || !ended.Teardown.WorkerDeleted || !ended.Teardown.MCPCleaned || !ended.Teardown.CredentialRevoked || !ended.Teardown.TokenRevoked || !ended.Teardown.LeaseReleased {
		t.Fatalf("incomplete teardown evidence: %#v", ended.Teardown)
	}
	if ports.lease != 1 || ports.task != 1 || ports.token != 1 || ports.mcp != 1 || ports.credential != 1 || ports.carrier != 1 {
		t.Fatalf("cleanup calls were not singular: %#v", ports)
	}
	encoded, err := json.Marshal(struct {
		Waiting  WaitingRecord
		Physical PhysicalExecution
	}{record, ended})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "test-token-a") {
		t.Fatal("raw execution token leaked into durable evidence")
	}
}

func TestAwaitContinuationCoordinatorRejectsUndeclaredInputBeforeMutation(t *testing.T) {
	endpoint := phaseagent.PhaseEndpointRef{TaskID: "task-a", EndpointID: "execute"}
	store := NewInMemoryWaitingStore()
	coordinator := &AwaitContinuationCoordinator{Binding: phasemcp.InvocationBinding{TaskID: "task-a", InvocationID: "invocation-a", Generation: 3, ExecutionEpoch: 1, Endpoint: endpoint, BindingRef: "B1", InputRevision: "r4"}, Inputs: phaseagent.PhaseInputSet{InputRevision: "r4", Pending: []phaseagent.PendingInput{{InputID: "review"}}}, ContinuationRef: "continuation-r4", Continuation: ContinuationMaterial{Endpoint: endpoint}, Continuations: NewInMemoryContinuationStore(), Relinquisher: ExecutionRelinquisher{Store: store}, ExecutionToken: "test-token"}
	_, err := coordinator.AwaitInputs(context.Background(), phaseagent.AwaitInputsRequest{InputIDs: []string{"invented"}})
	if err == nil || !strings.Contains(err.Error(), "undeclared") {
		t.Fatalf("expected undeclared input rejection, got %v", err)
	}
	_, found, getErr := store.Get(context.Background(), WaitingKey{TaskID: "task-a", InvocationID: "invocation-a", Generation: 3})
	if getErr != nil || found {
		t.Fatalf("rejected await mutated waiting state: found=%t err=%v", found, getErr)
	}
}

func TestAwaitContinuationCoordinatorRejectsStalePhysicalBinding(t *testing.T) {
	endpoint := phaseagent.PhaseEndpointRef{TaskID: "task-a", EndpointID: "execute"}
	physical := NewInMemoryPhysicalExecutionStore()
	_, err := physical.Create(context.Background(), PhysicalExecution{TaskID: "task-a", InvocationID: "invocation-a", Generation: 3, ExecutionEpoch: 1, WorkerID: "worker-a", WorkerName: "worker-a", TeamHarnessTaskID: "task-a", TeamHarnessAssignedTo: "worker-a", BindingRef: "stale", InputRevision: "r4", AgentSessionRef: "matrix:!old:test", AgentPackageDigest: "digest", PackageConsumed: true, MCPClientID: "threadmill", CredentialBindingRef: "credential", DesiredRuntimeGeneration: 1, AppliedRuntimeGeneration: 1, WorkspaceLeaseRef: "lease", ExecutionAuthorizationRef: "auth", State: PhysicalExecutionRunning, ObservedTaskStatus: "in_progress", TaskAcknowledged: true})
	if err != nil {
		t.Fatal(err)
	}
	coordinator := &AwaitContinuationCoordinator{Binding: phasemcp.InvocationBinding{TaskID: "task-a", InvocationID: "invocation-a", Generation: 3, ExecutionEpoch: 1, Endpoint: endpoint, BindingRef: "B1", InputRevision: "r4"}, Inputs: phaseagent.PhaseInputSet{InputRevision: "r4", Pending: []phaseagent.PendingInput{{InputID: "review"}}}, ContinuationRef: "continuation-r4", Continuation: ContinuationMaterial{Endpoint: endpoint}, Continuations: NewInMemoryContinuationStore(), Relinquisher: ExecutionRelinquisher{Store: NewInMemoryWaitingStore(), PhysicalExecutions: physical}, ExecutionToken: "test-token"}
	_, err = coordinator.AwaitInputs(context.Background(), phaseagent.AwaitInputsRequest{})
	if err == nil || !errors.Is(err, errors.New("physical execution does not match await binding")) && !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("expected stale binding rejection, got %v", err)
	}
}
