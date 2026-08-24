package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/agenthost/agentteams"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/executionreceipt"
	phasemcp "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/mcp/phase"
	runtime "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime"
)

type c46NoCleanup struct{ calls int }

func (p *c46NoCleanup) CompleteTeamHarnessTask(context.Context, runtime.TeamHarnessTask) error {
	p.calls++
	return nil
}
func (p *c46NoCleanup) DeleteWorker(context.Context, runtime.ProvisionedWorker) error {
	p.calls++
	return nil
}
func (p *c46NoCleanup) CleanupWorkerMCP(context.Context, runtime.ProvisionedWorker) error {
	p.calls++
	return nil
}
func (p *c46NoCleanup) RevokeMCPCredential(context.Context, runtime.MCPCredentialBinding) error {
	p.calls++
	return nil
}
func (p *c46NoCleanup) ReleaseWorkspaceLease(context.Context, runtime.WorkspaceLease) error {
	p.calls++
	return nil
}
func (p *c46NoCleanup) CreateMCPCredential(context.Context, runtime.MCPCredentialRequest) (runtime.MCPCredentialBinding, error) {
	return runtime.MCPCredentialBinding{}, errors.New("not used")
}
func (p *c46NoCleanup) ProvisionWorker(context.Context, runtime.WorkerProvisionRequest) (runtime.ProvisionedWorker, error) {
	return runtime.ProvisionedWorker{}, errors.New("not used")
}
func (p *c46NoCleanup) AcquireWorkspaceLease(context.Context, runtime.RehydrationPlan) (runtime.WorkspaceLease, error) {
	return runtime.WorkspaceLease{}, errors.New("not used")
}

type c46ProductionMarker struct {
	Mode                       string `json:"mode"`
	PID                        int    `json:"pid"`
	Candidates                 int    `json:"candidates"`
	Convergence                string `json:"convergence"`
	Action                     string `json:"action"`
	RuntimeBStarted            bool   `json:"runtimeBStarted"`
	SameSQLiteReopened         bool   `json:"sameSQLiteReopened"`
	ProductionObserverUsed     bool   `json:"productionObserverUsed"`
	WorkerIdentityVerified     bool   `json:"workerIdentityVerified"`
	TeamHarnessTaskActive      bool   `json:"teamHarnessTaskActive"`
	RuntimeGenerationVerified  bool   `json:"runtimeGenerationVerified"`
	MCPAppliedVerified         bool   `json:"mcpAppliedVerified"`
	SameExecutionEpoch         bool   `json:"sameExecutionEpoch"`
	ReplacementCreated         bool   `json:"replacementCreated"`
	DuplicatePhysicalExecution bool   `json:"duplicatePhysicalExecution"`
	DuplicateReceipt           bool   `json:"duplicateReceipt"`
	PhaseOutputAbsent          bool   `json:"phaseOutputAbsent"`
	TeardownProgressAbsent     bool   `json:"teardownProgressAbsent"`
}

func TestC46ProductionObserverRuntimeBChild(t *testing.T) {
	if os.Getenv("THREADMILL_C46_PROCESS_MODE") != "runtime-b-production" {
		return
	}
	required := func(name string) string {
		value := os.Getenv(name)
		if value == "" {
			t.Fatalf("%s is required", name)
		}
		return value
	}
	databasePath := required("THREADMILL_C46_PROCESS_DB")
	repository, err := runtime.OpenSQLiteRuntimeStateRepository(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	ctx := context.Background()
	keys, err := repository.Recovery().ListRecoveryCandidates(ctx, 8)
	if err != nil || len(keys) != 1 {
		t.Fatalf("recovery candidates=%v err=%v", keys, err)
	}
	before, err := repository.Recovery().LoadRecoverySnapshot(ctx, keys[0])
	if err != nil || before.CurrentPhysical == nil {
		t.Fatalf("load pre-recovery snapshot: %v", err)
	}
	physicalBefore := *before.CurrentPhysical
	taskflow := &agentteams.TeamHarnessStdioClient{
		Python: required("THREADMILL_C46_TEAMHARNESS_PYTHON"), ServerPath: required("THREADMILL_C46_TEAMHARNESS_SERVER"),
		Workspace:     required("THREADMILL_C46_TEAMHARNESS_WORKSPACE"),
		CommandPrefix: []string{required("THREADMILL_C46_DOCKER"), "exec", "-i", "-e", "AGENTTEAMS_WORKER_MATRIX_TOKEN", required("THREADMILL_C46_WORKER_CONTAINER")},
	}
	observer := &agentteams.ControllerPhysicalExecutionObserver{
		Controller: &agentteams.ControllerReprovisioner{BaseURL: required("THREADMILL_C46_CONTROLLER_URL"), BearerToken: required("THREADMILL_C46_CONTROLLER_TOKEN")},
		Taskflow:   taskflow,
	}
	observationRequest := runtime.PhysicalExecutionObservationRequest{
		TaskID: physicalBefore.TaskID, InvocationID: physicalBefore.InvocationID, Generation: physicalBefore.Generation,
		ExecutionEpoch: physicalBefore.ExecutionEpoch, WorkerID: physicalBefore.WorkerID, WorkerName: physicalBefore.WorkerName,
		TeamHarnessTaskID: physicalBefore.TeamHarnessTaskID, DesiredGeneration: physicalBefore.DesiredRuntimeGeneration,
		AppliedGeneration: physicalBefore.AppliedRuntimeGeneration, MCPClientID: physicalBefore.MCPClientID, AgentSessionRef: physicalBefore.AgentSessionRef,
	}
	observed, err := observer.Observe(ctx, observationRequest)
	if err != nil {
		t.Fatalf("production observer preflight: %v", err)
	}
	cleanup := &c46NoCleanup{}
	coordinator := &runtime.RecoveryCoordinator{Repository: repository, Mutations: repository.LifecycleMutations(), Observer: observer, OwnerID: "runtime-b-production", ClaimTTL: time.Minute,
		Cleanup: runtime.TerminalRecoveryCleanupPorts{Tasks: cleanup, Workers: cleanup, MCP: cleanup, Credentials: cleanup, Leases: cleanup, Bindings: phasemcp.NewBindingRegistry()},
	}
	report, err := (runtime.RestartRecoverySupervisor{Repository: repository, Coordinator: coordinator, BatchSize: 8, MaxIterations: 2, MaxRetryPerKey: 1}).RecoverStartup(ctx)
	if err != nil || len(report.Items) != 1 {
		t.Fatalf("restart recovery report=%#v err=%v", report, err)
	}
	item := report.Items[0]
	if item.Err != nil || item.Convergence != runtime.RecoveryStableActive || item.Result.Action != runtime.RecoveryActionCarrierActive {
		t.Fatalf("recovery item=%#v err=%v preflight=%#v", item, item.Err, observed)
	}
	after, err := repository.Recovery().LoadRecoverySnapshot(ctx, keys[0])
	if err != nil || after.CurrentPhysical == nil {
		t.Fatalf("load post-recovery snapshot: %v", err)
	}
	history, err := repository.PhysicalExecutionStore().ListByInvocation(ctx, keys[0].TaskID, keys[0].InvocationID, keys[0].Generation)
	if err != nil {
		t.Fatal(err)
	}
	receiptKey := executionreceipt.Key{TaskID: keys[0].TaskID, InvocationID: keys[0].InvocationID, Generation: keys[0].Generation, ExecutionEpoch: int64(physicalBefore.ExecutionEpoch)}
	receipt, receiptFound, err := repository.ReceiptStore().Get(ctx, receiptKey)
	if err != nil {
		t.Fatal(err)
	}
	_, outputFound, err := repository.PhaseOutputStore().Get(ctx, runtime.PhaseOutputKey{TaskID: keys[0].TaskID, InvocationID: keys[0].InvocationID, Generation: keys[0].Generation})
	if err != nil {
		t.Fatal(err)
	}
	claim, claimFound, err := repository.Recovery().GetRecoveryClaim(ctx, keys[0])
	if err != nil || !claimFound || claim.OwnerID != "" {
		t.Fatalf("claim not released: %#v found=%t err=%v", claim, claimFound, err)
	}
	physicalAfter := *after.CurrentPhysical
	marker := c46ProductionMarker{Mode: "runtime-b-production", PID: os.Getpid(), Candidates: len(report.Items), Convergence: string(item.Convergence), Action: string(item.Result.Action), RuntimeBStarted: true, SameSQLiteReopened: true, ProductionObserverUsed: true,
		WorkerIdentityVerified:     observed.Identity == runtime.ObservedCarrierIdentityVerified && observed.Worker == runtime.ObservedWorkerReady,
		TeamHarnessTaskActive:      observed.Task == runtime.ObservedTaskInProgress && observed.TaskID == physicalBefore.TeamHarnessTaskID,
		RuntimeGenerationVerified:  observed.Runtime == runtime.ObservedRuntimeApplied && observed.DesiredGeneration == physicalBefore.DesiredRuntimeGeneration && observed.AppliedGeneration == physicalBefore.AppliedRuntimeGeneration,
		MCPAppliedVerified:         observed.MCP == runtime.ObservedMCPApplied,
		SameExecutionEpoch:         physicalAfter.ExecutionEpoch == physicalBefore.ExecutionEpoch,
		ReplacementCreated:         len(history) != 1 || physicalAfter.ExecutionEpoch != physicalBefore.ExecutionEpoch,
		DuplicatePhysicalExecution: len(history) != 1, DuplicateReceipt: !receiptFound || receipt.Revision != 1,
		PhaseOutputAbsent: !outputFound, TeardownProgressAbsent: cleanup.calls == 0 && physicalAfter.State == runtime.PhysicalExecutionRunning,
	}
	encoded, _ := json.Marshal(marker)
	_, _ = os.Stdout.Write(append([]byte("C46_PROCESS_MARKER="), append(encoded, '\n')...))
}
