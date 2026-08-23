package runtime

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/executionreceipt"
	phasemcp "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/mcp/phase"
)

type testWorkspaceRoots struct{}

func (testWorkspaceRoots) ResolveWorkspaceRoot(_ context.Context, lease DurableWorkspaceLease) (string, error) {
	return "C:/ephemeral/" + lease.Ref, nil
}

func replacementReprovisionFixture(t *testing.T, path string) (*SQLiteRuntimeStateRepository, WaitingKey, *PhysicalExecutionProvisioner, *registryAuthorizationIssuer, *provisionPorts) {
	t.Helper()
	ctx := context.Background()
	repo, key, _ := activeRecoveryObservationFixture(t, path)
	store := repo.Reconstruction()
	if _, err := store.PutWorkspace(ctx, DurableWorkspace{Key: key, Ref: "workspace", AllowedDirs: []string{"out"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutContextSlice(ctx, DurableContextSlice{Key: key, Ref: "context", BaselineRef: "baseline", Content: "context"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutTaskMemory(ctx, DurableTaskMemory{Key: key, Ref: "memory"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutExecutionDescriptor(ctx, DurableExecutionDescriptor{Key: key, TaskContract: "contract", PhaseInstruction: "instruction", TaskSpec: "spec", WorkspaceRef: "workspace", ContextSliceRef: "context", TaskMemoryRef: "memory"}); err != nil {
		t.Fatal(err)
	}
	if _, err := replacementCoordinator(repo, "allocator", PhysicalExecutionObservation{Worker: ObservedWorkerNotFound}).PrepareLostCarrierReplacement(ctx, key); err != nil {
		t.Fatal(err)
	}
	issuer := &registryAuthorizationIssuer{registry: phasemcp.NewBindingRegistry(), active: map[string]bool{}}
	ports := newProvisionPorts(nil)
	ports.receipts = repo.ReceiptStore()
	ports.fail = "receipt"
	provisioner := &PhysicalExecutionProvisioner{Store: repo.WaitingStore(), PhysicalExecutions: repo.PhysicalExecutionStore(), Leases: DurableWorkspaceLeaseAuthority{Store: store, Roots: testWorkspaceRoots{}}, Tokens: issuer, Credentials: ports, Workers: ports, MCP: ports, Runtime: ports, Discovery: ports, Tasks: ports, Packages: ports, Receipts: repo.ReceiptStore(), Mutations: repo.LifecycleMutations(), MCPName: "threadmill", MCPURL: "http://threadmill.test/mcp", Transport: "streamable_http", ReceiptPollInterval: time.Millisecond}
	return repo, key, provisioner, issuer, ports
}

func scheduleReplacementReceipt(t *testing.T, repo *SQLiteRuntimeStateRepository, issuer *registryAuthorizationIssuer, ports *provisionPorts) {
	t.Helper()
	ports.taskHook = func(request TeamHarnessTaskRequest) {
		go func() {
			key := PhysicalExecutionKey{TaskID: request.Plan.TaskID, InvocationID: request.Plan.InvocationID, Generation: request.Plan.Generation, ExecutionEpoch: request.Plan.NextExecutionEpoch}
			deadline := time.Now().Add(time.Second)
			for time.Now().Before(deadline) {
				execution, found, err := repo.PhysicalExecutionStore().Get(context.Background(), key)
				if err == nil && found && execution.State == PhysicalExecutionAccepted {
					var token string
					for candidate := range issuer.active {
						token = candidate
						break
					}
					services, err := issuer.registry.Resolve(token)
					if err != nil {
						return
					}
					_, _ = (&PackageConsumptionCoordinator{Store: repo.ReceiptStore(), PhysicalExecutions: repo.PhysicalExecutionStore(), Mutations: repo.LifecycleMutations()}).ConfirmPackageConsumption(context.Background(), services.Binding, executionreceipt.Submission{PackageDigest: execution.AgentPackageDigest, SessionIdentity: execution.AgentSessionRef, Consumed: true})
					return
				}
				time.Sleep(time.Millisecond)
			}
		}()
	}
}

func TestReplacementReprovisionConsumesReservedEpochAndActivatesFreshCarrier(t *testing.T) {
	ctx := context.Background()
	repo, key, provisioner, issuer, ports := replacementReprovisionFixture(t, filepath.Join(t.TempDir(), "runtime.db"))
	defer repo.Close()
	scheduleReplacementReceipt(t, repo, issuer, ports)
	coordinator := &ReplacementReprovisionCoordinator{Repository: repo, Provisioner: provisioner, Surfaces: ExecutionSurfaces{Runtime: noopRuntime{}, ContextReader: noopReader{}, ContextAgent: noopAgent{}}, OwnerID: "recoverer", ClaimTTL: time.Minute}
	execution, err := coordinator.Reprovision(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if execution.ExecutionEpoch != 3 || execution.ReplacesExecutionEpoch != 2 || execution.State != PhysicalExecutionRunning || !execution.PackageConsumed || execution.WorkspaceLeaseRef == "" || execution.CredentialBindingRef == "" || execution.AgentSessionRef == "matrix:!room:test" {
		t.Fatalf("replacement=%#v", execution)
	}
	waiting, found, err := repo.WaitingStore().Get(ctx, key)
	if err != nil || !found || waiting.State != AwaitStateRunning || waiting.ExecutionEpoch != 3 {
		t.Fatalf("waiting=%#v found=%v err=%v", waiting, found, err)
	}
	if _, err = issuer.registry.Resolve(ports.lastCredential.Token); err != nil {
		t.Fatalf("fresh token does not resolve: %v", err)
	}
	if old, err := issuer.registry.Resolve("old-token"); err == nil || old.Binding.ExecutionEpoch == 2 {
		t.Fatal("old token unexpectedly resolved")
	}
	if countRuntimeEvents(t, repo, "PhysicalExecutionActivated") != 1 || countRuntimeEvents(t, repo, "PackageConsumptionRecorded") != 1 {
		t.Fatal("replacement activation evidence missing or duplicated")
	}
}

func TestReplacementReprovisionRejectsStaleClaimAndConcurrentActor(t *testing.T) {
	ctx := context.Background()
	repo, key, provisioner, issuer, ports := replacementReprovisionFixture(t, filepath.Join(t.TempDir(), "runtime.db"))
	defer repo.Close()
	scheduleReplacementReceipt(t, repo, issuer, ports)
	first := &ReplacementReprovisionCoordinator{Repository: repo, Provisioner: provisioner, Surfaces: ExecutionSurfaces{Runtime: noopRuntime{}, ContextReader: noopReader{}, ContextAgent: noopAgent{}}, OwnerID: "a", ClaimTTL: time.Minute}
	second := &ReplacementReprovisionCoordinator{Repository: repo, Provisioner: provisioner, Surfaces: ExecutionSurfaces{Runtime: noopRuntime{}, ContextReader: noopReader{}, ContextAgent: noopAgent{}}, OwnerID: "b", ClaimTTL: time.Minute}
	ports.discoveryStarted, ports.discoveryRelease = make(chan struct{}), make(chan struct{})
	result := make(chan error, 1)
	go func() { _, err := first.Reprovision(ctx, key); result <- err }()
	<-ports.discoveryStarted
	if _, err := second.Reprovision(ctx, key); !errors.Is(err, ErrRecoveryClaimed) {
		t.Fatalf("second err=%v", err)
	}
	close(ports.discoveryRelease)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestReplacementReprovisionFailureRestoresReservationWithoutNewEpoch(t *testing.T) {
	ctx := context.Background()
	repo, key, provisioner, issuer, ports := replacementReprovisionFixture(t, filepath.Join(t.TempDir(), "runtime.db"))
	defer repo.Close()
	_ = issuer
	ports.fail = "worker"
	coordinator := &ReplacementReprovisionCoordinator{Repository: repo, Provisioner: provisioner, Surfaces: ExecutionSurfaces{Runtime: noopRuntime{}, ContextReader: noopReader{}, ContextAgent: noopAgent{}}, OwnerID: "recoverer", ClaimTTL: time.Minute}
	if _, err := coordinator.Reprovision(ctx, key); err == nil {
		t.Fatal("worker failure unexpectedly succeeded")
	}
	reserved, found, err := repo.PhysicalExecutionStore().Get(ctx, PhysicalExecutionKey{TaskID: key.TaskID, InvocationID: key.InvocationID, Generation: key.Generation, ExecutionEpoch: 3})
	if err != nil || !found || reserved.State != PhysicalExecutionReserved || reserved.ReplacesExecutionEpoch != 2 {
		t.Fatalf("reserved=%#v found=%v err=%v", reserved, found, err)
	}
	if countRuntimeEvents(t, repo, "ReplacementEpochAllocated") != 1 {
		t.Fatal("failure allocated a second epoch")
	}
}

func TestReplacementReprovisionColdReopenKeepsReservation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime.db")
	repo, key, provisioner, issuer, ports := replacementReprovisionFixture(t, path)
	ports.fail = "runtime"
	coordinator := &ReplacementReprovisionCoordinator{Repository: repo, Provisioner: provisioner, Surfaces: ExecutionSurfaces{Runtime: noopRuntime{}, ContextReader: noopReader{}, ContextAgent: noopAgent{}}, OwnerID: "recoverer", ClaimTTL: time.Minute}
	if _, err := coordinator.Reprovision(context.Background(), key); err == nil {
		t.Fatal("runtime failure unexpectedly succeeded")
	}
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}
	repo, err := OpenSQLiteRuntimeStateRepository(path)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()
	reserved, found, err := repo.PhysicalExecutionStore().Get(context.Background(), PhysicalExecutionKey{TaskID: key.TaskID, InvocationID: key.InvocationID, Generation: key.Generation, ExecutionEpoch: 3})
	if err != nil || !found || reserved.State != PhysicalExecutionReserved {
		t.Fatalf("reopened reservation=%#v found=%v err=%v", reserved, found, err)
	}
	_ = issuer
}

var _ = sync.Mutex{}
