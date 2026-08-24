package runtime

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	phasemcp "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/mcp/phase"
)

func newRestartSupervisor(repo RuntimeStateRepository, coordinator *RecoveryCoordinator) RestartRecoverySupervisor {
	return RestartRecoverySupervisor{Repository: repo, Coordinator: coordinator, BatchSize: 32, MaxIterations: 4, MaxRetryPerKey: 2}
}

func TestRecoveryDiscoveryIncludesOnlyRecoverableDurableInvocations(t *testing.T) {
	ctx := context.Background()
	repo, activeKey, _ := activeRecoveryObservationFixture(t, filepath.Join(t.TempDir(), "runtime.db"))
	defer repo.Close()
	terminalKey := WaitingKey{TaskID: "terminal", InvocationID: "invocation", Generation: 1}
	if _, err := repo.PhysicalExecutionStore().Create(ctx, PhysicalExecution{TaskID: terminalKey.TaskID, InvocationID: terminalKey.InvocationID, Generation: terminalKey.Generation, ExecutionEpoch: 1, State: PhysicalExecutionTerminated}); err != nil {
		t.Fatal(err)
	}
	keys, err := repo.Recovery().ListRecoveryCandidates(ctx, 20)
	if err != nil {
		t.Fatal(err)
	}
	foundActive, foundTerminal := false, false
	for _, key := range keys {
		foundActive = foundActive || key == activeKey
		foundTerminal = foundTerminal || key == terminalKey
	}
	if !foundActive || foundTerminal {
		t.Fatalf("candidates=%#v", keys)
	}
}

func TestRestartRecoverySupervisorColdReopenKeepsHealthyCarrierAndNoDuplicateEffects(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "runtime.db")
	repo, key, old := activeRecoveryObservationFixture(t, path)
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}
	repo, err := OpenSQLiteRuntimeStateRepository(path)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()
	coordinator := replacementCoordinator(repo, "runtime-restarted", PhysicalExecutionObservation{Worker: ObservedWorkerReady, Runtime: ObservedRuntimeApplied, MCP: ObservedMCPApplied, Identity: ObservedCarrierIdentityVerified, Task: ObservedTaskInProgress})
	report, err := newRestartSupervisor(repo, coordinator).RecoverStartup(ctx)
	if err != nil || len(report.Items) != 1 || report.Items[0].Convergence != RecoveryStableActive {
		t.Fatalf("report=%#v err=%v", report, err)
	}
	if current := currentPhysical(t, repo, key, old.ExecutionEpoch); current.State != PhysicalExecutionRunning || countRuntimeEvents(t, repo, "ReplacementEpochAllocated") != 0 || countRuntimeEvents(t, repo, "PhysicalExecutionActivated") != 0 {
		t.Fatalf("healthy restart mutated physical=%#v", current)
	}
}

// TestRestartRecoveryProcessHelper is launched in a separate OS process by
// TestRestartRecoverySupervisorRecoversAcrossRuntimeProcess.  It creates the
// durable invocation and exits without running recovery, modelling a runtime
// process loss rather than merely closing a repository object.
func TestRestartRecoveryProcessHelper(t *testing.T) {
	if os.Getenv("THREADMILL_C4_6_HELPER") != "1" {
		return
	}
	path := os.Getenv("THREADMILL_C4_6_DB")
	if path == "" {
		t.Fatal("helper database path is required")
	}
	repo, _, _ := activeRecoveryObservationFixture(t, path)
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRestartRecoverySupervisorRecoversAcrossRuntimeProcess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime.db")
	command := exec.Command(os.Args[0], "-test.run=TestRestartRecoveryProcessHelper", "-test.v")
	command.Env = append(os.Environ(), "THREADMILL_C4_6_HELPER=1", "THREADMILL_C4_6_DB="+path)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("seed runtime process failed: %v\n%s", err, output)
	}
	// This is a new runtime instance and a freshly opened SQLite connection.
	// No in-memory store, token or session from the child can participate.
	repo, err := OpenSQLiteRuntimeStateRepository(path)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()
	keys, err := repo.Recovery().ListRecoveryCandidates(context.Background(), 10)
	if err != nil || len(keys) != 1 {
		t.Fatalf("discovery after process exit keys=%#v err=%v", keys, err)
	}
	coordinator := replacementCoordinator(repo, "runtime-parent", PhysicalExecutionObservation{Worker: ObservedWorkerReady, Runtime: ObservedRuntimeApplied, MCP: ObservedMCPApplied, Identity: ObservedCarrierIdentityVerified, Task: ObservedTaskInProgress})
	report, err := newRestartSupervisor(repo, coordinator).RecoverStartup(context.Background())
	if err != nil || len(report.Items) != 1 || report.Items[0].Convergence != RecoveryStableActive {
		t.Fatalf("report=%#v err=%v", report, err)
	}
}

func TestRestartRecoverySupervisorConsumesReservedEpochAfterColdReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "runtime.db")
	repo, key, provisioner, _, ports := replacementReprovisionFixture(t, path)
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}
	repo, err := OpenSQLiteRuntimeStateRepository(path)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()
	// All process-local capabilities are freshly composed after reopen. The
	// durable reservation alone identifies the work; it stores no token/session.
	provisioner.Store = repo.WaitingStore()
	provisioner.PhysicalExecutions = repo.PhysicalExecutionStore()
	provisioner.Receipts = repo.ReceiptStore()
	provisioner.Mutations = repo.LifecycleMutations()
	provisioner.Leases = DurableWorkspaceLeaseAuthority{Store: repo.Reconstruction(), Roots: testWorkspaceRoots{}}
	issuer := &registryAuthorizationIssuer{registry: phasemcp.NewBindingRegistry(), active: map[string]bool{}}
	provisioner.Tokens = issuer
	scheduleReplacementReceipt(t, repo, issuer, ports)
	reprovision := &ReplacementReprovisionCoordinator{Repository: repo, Provisioner: provisioner, Surfaces: ExecutionSurfaces{Runtime: noopRuntime{}, ContextReader: noopReader{}, ContextAgent: noopAgent{}}, OwnerID: "runtime-restarted", ClaimTTL: time.Minute}
	coordinator := newRecoveryCoordinator(repo, "runtime-restarted", &recoveryCleanupPorts{}, nil)
	coordinator.Reprovision = reprovision
	report, err := newRestartSupervisor(repo, coordinator).RecoverStartup(ctx)
	if err != nil || len(report.Items) < 1 {
		t.Fatalf("report=%#v err=%v", report, err)
	}
	current := currentPhysical(t, repo, key, 3)
	if current.State != PhysicalExecutionRunning || !current.PackageConsumed || countRuntimeEvents(t, repo, "ReplacementEpochAllocated") != 1 || countRuntimeEvents(t, repo, "PhysicalExecutionActivated") != 1 {
		t.Fatalf("replacement=%#v events allocation=%d activation=%d", current, countRuntimeEvents(t, repo, "ReplacementEpochAllocated"), countRuntimeEvents(t, repo, "PhysicalExecutionActivated"))
	}
}

type blockingRecoveryObserver struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (o *blockingRecoveryObserver) Observe(ctx context.Context, _ PhysicalExecutionObservationRequest) (PhysicalExecutionObservation, error) {
	o.once.Do(func() { close(o.started) })
	select {
	case <-ctx.Done():
		return PhysicalExecutionObservation{}, ctx.Err()
	case <-o.release:
		return PhysicalExecutionObservation{Worker: ObservedWorkerReady, Runtime: ObservedRuntimeApplied, MCP: ObservedMCPApplied, Identity: ObservedCarrierIdentityVerified}, nil
	}
}

func TestRestartRecoverySupervisorsUsePerInvocationClaimNotGlobalLock(t *testing.T) {
	ctx := context.Background()
	repo, key, _ := activeRecoveryObservationFixture(t, filepath.Join(t.TempDir(), "runtime.db"))
	defer repo.Close()
	observer := &blockingRecoveryObserver{started: make(chan struct{}), release: make(chan struct{})}
	first := newRecoveryCoordinator(repo, "runtime-a", &recoveryCleanupPorts{}, nil)
	first.Observer = observer
	second := newRecoveryCoordinator(repo, "runtime-b", &recoveryCleanupPorts{}, nil)
	second.Observer = &observingPhysicalExecution{value: PhysicalExecutionObservation{Worker: ObservedWorkerReady, Runtime: ObservedRuntimeApplied, MCP: ObservedMCPApplied, Identity: ObservedCarrierIdentityVerified}}
	firstResult := make(chan RestartRecoveryReport, 1)
	firstErr := make(chan error, 1)
	go func() {
		report, err := newRestartSupervisor(repo, first).RecoverStartup(ctx)
		firstResult <- report
		firstErr <- err
	}()
	<-observer.started
	report, err := newRestartSupervisor(repo, second).RecoverStartup(ctx)
	if err != nil || len(report.Items) != 1 || report.Items[0].Convergence != RecoveryContention || !errors.Is(report.Items[0].Err, ErrRecoveryClaimed) {
		t.Fatalf("second report=%#v err=%v", report, err)
	}
	close(observer.release)
	if err = <-firstErr; err != nil {
		t.Fatal(err)
	}
	if report = <-firstResult; len(report.Items) != 1 || report.Items[0].Convergence != RecoveryStableActive {
		t.Fatalf("first report=%#v", report)
	}
	if _, found, err := repo.Recovery().GetRecoveryClaim(ctx, key); err != nil || !found {
		t.Fatalf("claim evidence found=%v err=%v", found, err)
	}
}
