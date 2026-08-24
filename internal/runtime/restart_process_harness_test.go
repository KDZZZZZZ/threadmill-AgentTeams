package runtime

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/executionreceipt"
	phasemcp "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/mcp/phase"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/phaseagent"
)

const c46ProcessHarnessMode = "THREADMILL_C46_PROCESS_MODE"
const c46ProcessHarnessDB = "THREADMILL_C46_PROCESS_DB"
const c46ProcessHarnessDescriptor = "THREADMILL_C46_WORKER_DESCRIPTOR"
const c46ActiveCrashPoint = "THREADMILL_C46_ACTIVE_CRASH_POINT"

type c46WorkerExecutionDescriptor struct {
	TaskID                   string         `json:"taskID"`
	InvocationID             string         `json:"invocationID"`
	Generation               int            `json:"generation"`
	ExecutionEpoch           ExecutionEpoch `json:"executionEpoch"`
	WorkerReady              bool           `json:"workerReady"`
	WorkerName               string         `json:"workerName"`
	WorkerID                 string         `json:"workerID"`
	WorkerPhase              string         `json:"workerPhase"`
	ContainerState           string         `json:"containerState"`
	RuntimeDesiredGeneration int64          `json:"runtimeDesiredGeneration"`
	RuntimeAppliedGeneration int64          `json:"runtimeAppliedGeneration"`
	MCPClientID              string         `json:"mcpClientID"`
	MCPApplied               bool           `json:"mcpApplied"`
	CredentialRef            string         `json:"credentialRef"`
	MatrixRoomRef            string         `json:"matrixRoomRef"`
	TeamHarnessTaskID        string         `json:"teamHarnessTaskID"`
}

type c46ProcessMarker struct {
	Mode                       string                    `json:"mode"`
	PID                        int                       `json:"pid"`
	Candidates                 int                       `json:"candidates"`
	Convergence                string                    `json:"convergence,omitempty"`
	Action                     string                    `json:"action,omitempty"`
	Physical                   *PhysicalExecution        `json:"physical,omitempty"`
	PhysicalCount              int                       `json:"physicalCount,omitempty"`
	Waiting                    *WaitingRecord            `json:"waiting,omitempty"`
	WaitingCount               int                       `json:"waitingCount,omitempty"`
	Receipt                    *executionreceipt.Receipt `json:"receipt,omitempty"`
	ReceiptCount               int                       `json:"receiptCount,omitempty"`
	ReceiptEvents              int                       `json:"receiptEvents,omitempty"`
	ActivationEvents           int                       `json:"activationEvents,omitempty"`
	ActiveCrashPointReady      bool                      `json:"activeCrashPointReady,omitempty"`
	RuntimeBStarted            bool                      `json:"runtimeBStarted,omitempty"`
	SameSQLiteReopened         bool                      `json:"sameSQLiteReopened,omitempty"`
	ProductionObserverUsed     bool                      `json:"productionObserverUsed,omitempty"`
	WorkerIdentityVerified     bool                      `json:"workerIdentityVerified,omitempty"`
	TeamHarnessTaskActive      bool                      `json:"teamHarnessTaskActive,omitempty"`
	RuntimeGenerationVerified  bool                      `json:"runtimeGenerationVerified,omitempty"`
	MCPAppliedVerified         bool                      `json:"mcpAppliedVerified,omitempty"`
	SameExecutionEpoch         bool                      `json:"sameExecutionEpoch,omitempty"`
	ReplacementCreated         bool                      `json:"replacementCreated,omitempty"`
	DuplicatePhysicalExecution bool                      `json:"duplicatePhysicalExecution,omitempty"`
	DuplicateReceipt           bool                      `json:"duplicateReceipt,omitempty"`
	PhaseOutputAbsent          bool                      `json:"phaseOutputAbsent,omitempty"`
	TeardownProgressAbsent     bool                      `json:"teardownProgressAbsent,omitempty"`
}

// TestC46DurableProcessHarnessChild is entered only by the parent test below.
// Each mode is a separately spawned OS process: no Go pointer, in-memory
// BindingRegistry, workspace root, token, credential, or session state can
// cross the boundary. The SQLite path is the sole hand-off.
func TestC46DurableProcessHarnessChild(t *testing.T) {
	mode, path := os.Getenv(c46ProcessHarnessMode), os.Getenv(c46ProcessHarnessDB)
	if mode == "" {
		return
	}
	if path == "" {
		t.Fatal("C4-6 process harness database path is required")
	}
	ctx := context.Background()
	marker := c46ProcessMarker{Mode: mode, PID: os.Getpid()}
	switch mode {
	case "runtime-a-worker-descriptor", "runtime-a-active-crash":
		descriptorPath := os.Getenv(c46ProcessHarnessDescriptor)
		encoded, err := os.ReadFile(descriptorPath)
		if err != nil {
			t.Fatalf("read real Worker descriptor: %v", err)
		}
		var descriptor c46WorkerExecutionDescriptor
		if err := json.Unmarshal(encoded, &descriptor); err != nil {
			t.Fatalf("decode real Worker descriptor: %v", err)
		}
		if !descriptor.WorkerReady || descriptor.TaskID == "" || descriptor.InvocationID == "" || descriptor.Generation <= 0 || descriptor.ExecutionEpoch <= 0 || descriptor.WorkerName != "tm-"+descriptor.InvocationID+"-g1-e1" || descriptor.WorkerID != descriptor.WorkerName || descriptor.TeamHarnessTaskID != "tm-phase-"+descriptor.InvocationID+"-g1-e1" || !strings.EqualFold(descriptor.WorkerPhase, "ready") || !strings.EqualFold(descriptor.ContainerState, "running") || descriptor.RuntimeDesiredGeneration <= 0 || descriptor.RuntimeAppliedGeneration != descriptor.RuntimeDesiredGeneration || descriptor.MCPClientID == "" || !descriptor.MCPApplied || descriptor.CredentialRef == "" {
			t.Fatalf("invalid real Worker execution descriptor: %#v", descriptor)
		}
		repository, err := OpenSQLiteRuntimeStateRepository(path)
		if err != nil {
			t.Fatal(err)
		}
		physicalSeed := physicalExecutionFromC46Descriptor(descriptor)
		waitingSeed := waitingRecordFromC46Physical(physicalSeed)
		if err := repository.ContinuationStore().Put(ctx, waitingSeed.ContinuationRef, ContinuationMaterial{Endpoint: waitingSeed.Endpoint, WorkspaceRef: "c46-worker-workspace:" + descriptor.WorkerID, ContextSliceRef: "c46-worker-context:" + descriptor.InvocationID, TaskMemoryBufferRef: "c46-worker-memory:" + descriptor.InvocationID}); err != nil {
			t.Fatal(err)
		}
		if err := repository.InputStore().Put(ctx, waitingSeed.Key, StoredPhaseInputSet{Inputs: phaseagent.PhaseInputSet{InputRevision: physicalSeed.InputRevision}, AwaitConditionSatisfied: true}); err != nil {
			t.Fatal(err)
		}
		physical, err := repository.PhysicalExecutionStore().Create(ctx, physicalSeed)
		if err != nil {
			t.Fatal(err)
		}
		waiting, err := repository.WaitingStore().Create(ctx, waitingSeed)
		if err != nil {
			t.Fatal(err)
		}
		assertC46WaitingPhysicalAlignment(t, waiting, physical)
		physical.AgentSessionRef = descriptor.MatrixRoomRef
		physical.AgentPackageDigest = c46PackageDigest(physical)
		physical.State = PhysicalExecutionAccepted
		physical, swapped, err := repository.PhysicalExecutionStore().CompareAndSwap(ctx, physical.Key(), physical.Revision, physical)
		if err != nil || !swapped {
			t.Fatalf("accept descriptor-backed execution swapped=%t err=%v", swapped, err)
		}
		binding := phasemcp.InvocationBinding{TaskID: physical.TaskID, InvocationID: physical.InvocationID, Generation: physical.Generation, ExecutionEpoch: int64(physical.ExecutionEpoch), BindingRef: physical.BindingRef, InputRevision: physical.InputRevision}
		submission := executionreceipt.Submission{PackageDigest: physical.AgentPackageDigest, SessionIdentity: physical.AgentSessionRef, Consumed: true}
		consumer := PackageConsumptionCoordinator{Store: repository.ReceiptStore(), PhysicalExecutions: repository.PhysicalExecutionStore(), Mutations: repository.LifecycleMutations()}
		receipt, err := consumer.ConfirmPackageConsumption(ctx, binding, submission)
		if err != nil {
			t.Fatal(err)
		}
		// The duplicate follows the exact production coordinator path. Its
		// receipt key is idempotent and RecordPackageConsumption must not append
		// another event or change the accepted physical revision.
		duplicateReceipt, err := consumer.ConfirmPackageConsumption(ctx, binding, submission)
		if err != nil || !reflect.DeepEqual(duplicateReceipt, receipt) {
			t.Fatalf("duplicate receipt=%#v original=%#v err=%v", duplicateReceipt, receipt, err)
		}
		physical, found, err := repository.PhysicalExecutionStore().Get(ctx, physical.Key())
		if err != nil || !found || !physical.PackageConsumed || physical.State != PhysicalExecutionAccepted {
			t.Fatalf("receipt did not durably accept physical=%#v found=%t err=%v", physical, found, err)
		}
		// A wrong binding must fail before any receipt or state mutation.
		wrongBinding := binding
		wrongBinding.BindingRef = "wrong-" + binding.BindingRef
		beforeConflictPhysical := physical
		beforeConflictWaiting := waiting
		if _, err := consumer.ConfirmPackageConsumption(ctx, wrongBinding, submission); err == nil {
			t.Fatal("wrong binding receipt was accepted")
		}
		afterConflictPhysical, found, err := repository.PhysicalExecutionStore().Get(ctx, physical.Key())
		if err != nil || !found || !reflect.DeepEqual(afterConflictPhysical, beforeConflictPhysical) {
			t.Fatalf("wrong binding advanced physical=%#v found=%t err=%v", afterConflictPhysical, found, err)
		}
		afterConflictWaiting, found, err := repository.WaitingStore().Get(ctx, waiting.Key)
		if err != nil || !found || !reflect.DeepEqual(afterConflictWaiting, beforeConflictWaiting) {
			t.Fatalf("wrong binding advanced waiting=%#v found=%t err=%v", afterConflictWaiting, found, err)
		}
		beforeActivationEvents := c46RuntimeEventCount(t, repository, "PhysicalExecutionActivated")
		waiting, physical, activated, err := repository.LifecycleMutations().ActivatePhysicalExecution(ctx, waiting.Key, waiting.Revision, physical.Key(), physical.Revision)
		if err != nil || !activated {
			t.Fatalf("activate accepted execution activated=%t err=%v", activated, err)
		}
		assertC46RunningWaitingPhysicalAlignment(t, waiting, physical)
		duplicateWaiting, duplicatePhysical, duplicateActivated, err := repository.LifecycleMutations().ActivatePhysicalExecution(ctx, waiting.Key, waiting.Revision-1, physical.Key(), physical.Revision-1)
		if err != nil || duplicateActivated || !reflect.DeepEqual(duplicateWaiting, waiting) || !reflect.DeepEqual(duplicatePhysical, physical) {
			t.Fatalf("duplicate activation waiting=%#v physical=%#v activated=%t err=%v", duplicateWaiting, duplicatePhysical, duplicateActivated, err)
		}
		if got := c46RuntimeEventCount(t, repository, "PhysicalExecutionActivated"); got != beforeActivationEvents+1 {
			t.Fatalf("activation outbox event count=%d before=%d", got, beforeActivationEvents)
		}
		if err := repository.Close(); err != nil {
			t.Fatal(err)
		}
		// This is a genuine cold reopen in Runtime A, after all handles from the
		// creating repository have been closed.
		repository, err = OpenSQLiteRuntimeStateRepository(path)
		if err != nil {
			t.Fatal(err)
		}
		loaded, found, err := repository.PhysicalExecutionStore().Get(ctx, physical.Key())
		if err != nil || !found || !reflect.DeepEqual(loaded, physical) {
			t.Fatalf("cold-load physical=%#v found=%t err=%v; created=%#v", loaded, found, err, physical)
		}
		history, err := repository.PhysicalExecutionStore().ListByInvocation(ctx, physical.TaskID, physical.InvocationID, physical.Generation)
		if err != nil || len(history) != 1 {
			t.Fatalf("physical execution history=%#v err=%v", history, err)
		}
		loadedWaiting, waitingFound, err := repository.WaitingStore().Get(ctx, waiting.Key)
		if err != nil || !waitingFound || !reflect.DeepEqual(loadedWaiting, waiting) {
			t.Fatalf("cold-load waiting=%#v found=%t err=%v; created=%#v", loadedWaiting, waitingFound, err, waiting)
		}
		assertC46RunningWaitingPhysicalAlignment(t, loadedWaiting, loaded)
		loadedReceipt, receiptFound, err := repository.ReceiptStore().Get(ctx, receipt.Key())
		if err != nil || !receiptFound || !reflect.DeepEqual(loadedReceipt, receipt) {
			t.Fatalf("cold-load receipt=%#v found=%t err=%v; created=%#v", loadedReceipt, receiptFound, err, receipt)
		}
		if _, found, err := repository.PhaseOutputStore().Get(ctx, PhaseOutputKey{TaskID: physical.TaskID, InvocationID: physical.InvocationID, Generation: physical.Generation}); err != nil || found {
			t.Fatalf("unexpected PhaseOutput found=%t err=%v", found, err)
		}
		receiptEvents := c46RuntimeEventCount(t, repository, "PackageConsumptionRecorded")
		activationEvents := c46RuntimeEventCount(t, repository, "PhysicalExecutionActivated")
		marker.Physical = &loaded
		marker.PhysicalCount = len(history)
		marker.Waiting = &loadedWaiting
		marker.WaitingCount = 1
		marker.Receipt = &loadedReceipt
		marker.ReceiptCount = 1
		marker.ReceiptEvents = receiptEvents
		marker.ActivationEvents = activationEvents
		if mode == "runtime-a-active-crash" {
			// This marker follows Runtime A's cold reopen of all running snapshots.
			// The process remains alive solely for the parent to kill it directly.
			marker.ActiveCrashPointReady = true
			writeC46ProcessMarker(t, marker)
			select {}
		}
		if err := repository.Close(); err != nil {
			t.Fatal(err)
		}
	case "runtime-a":
		repository, _, _ := activeRecoveryObservationFixture(t, path)
		if err := repository.Close(); err != nil {
			t.Fatal(err)
		}
	case "runtime-b":
		repository, err := OpenSQLiteRuntimeStateRepository(path)
		if err != nil {
			t.Fatal(err)
		}
		defer repository.Close()
		coordinator := newRecoveryCoordinator(repository, "runtime-b", &recoveryCleanupPorts{}, nil)
		coordinator.Observer = &observingPhysicalExecution{value: PhysicalExecutionObservation{
			Worker: ObservedWorkerReady, Runtime: ObservedRuntimeApplied, MCP: ObservedMCPApplied,
			Identity: ObservedCarrierIdentityVerified, Task: ObservedTaskInProgress,
		}}
		report, err := (RestartRecoverySupervisor{Repository: repository, Coordinator: coordinator, BatchSize: 8, MaxIterations: 2, MaxRetryPerKey: 1}).RecoverStartup(ctx)
		if err != nil || len(report.Items) != 1 {
			t.Fatalf("restart supervisor report=%#v err=%v", report, err)
		}
		item := report.Items[0]
		if item.Convergence != RecoveryStableActive || item.Result.Action != RecoveryActionCarrierActive || item.Err != nil {
			t.Fatalf("unexpected recovery item=%#v", item)
		}
		claim, found, err := repository.Recovery().GetRecoveryClaim(ctx, item.Key)
		if err != nil || !found || claim.OwnerID != "" {
			t.Fatalf("recovery claim was not safely released: claim=%#v found=%t err=%v", claim, found, err)
		}
		marker.Candidates = len(report.Items)
		marker.Convergence = string(item.Convergence)
		marker.Action = string(item.Result.Action)
	default:
		t.Fatalf("unknown C4-6 process harness mode %q", mode)
	}
	writeC46ProcessMarker(t, marker)
}

func writeC46ProcessMarker(t *testing.T, marker c46ProcessMarker) {
	t.Helper()
	encoded, err := json.Marshal(marker)
	if err != nil {
		t.Fatal(err)
	}
	// The parent parses this exact, secret-free marker from test stdout.
	_, _ = os.Stdout.Write(append([]byte("C46_PROCESS_MARKER="), append(encoded, '\n')...))
}

func c46PackageDigest(physical PhysicalExecution) string {
	// This is the canonical, secret-free Runtime A package identity used by the
	// receipt seam. The real Matrix room comes from the Controller descriptor;
	// the digest binds it to this exact logical/epoch/binding carrier instead
	// of inferring consumption merely from Worker Ready.
	value := struct {
		TaskID, InvocationID, BindingRef, InputRevision, MatrixRoomRef string
		Generation                                                     int
		ExecutionEpoch                                                 ExecutionEpoch
	}{physical.TaskID, physical.InvocationID, physical.BindingRef, physical.InputRevision, physical.AgentSessionRef, physical.Generation, physical.ExecutionEpoch}
	encoded, _ := json.Marshal(value)
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func physicalExecutionFromC46Descriptor(descriptor c46WorkerExecutionDescriptor) PhysicalExecution {
	return PhysicalExecution{
		TaskID: descriptor.TaskID, InvocationID: descriptor.InvocationID, Generation: descriptor.Generation,
		ExecutionEpoch: descriptor.ExecutionEpoch, State: PhysicalExecutionProvisioning,
		WorkerID: descriptor.WorkerID, WorkerName: descriptor.WorkerName,
		TeamHarnessTaskID: descriptor.TeamHarnessTaskID, TeamHarnessAssignedTo: descriptor.WorkerName,
		BindingRef:    "c46-worker-binding:" + descriptor.WorkerID,
		InputRevision: "c46-worker-input:" + descriptor.WorkerID,
		MCPClientID:   descriptor.MCPClientID, CredentialBindingRef: descriptor.CredentialRef,
		DesiredRuntimeGeneration: descriptor.RuntimeDesiredGeneration,
		AppliedRuntimeGeneration: descriptor.RuntimeAppliedGeneration,
	}
}

func waitingRecordFromC46Physical(physical PhysicalExecution) WaitingRecord {
	return WaitingRecord{
		Key:                WaitingKey{TaskID: physical.TaskID, InvocationID: physical.InvocationID, Generation: physical.Generation},
		ExecutionEpoch:     physical.ExecutionEpoch,
		Endpoint:           phaseagent.PhaseEndpointRef{TaskID: physical.TaskID, EndpointID: string(phaseagent.PhaseExecute)},
		PreviousBindingRef: physical.BindingRef, InputRevision: physical.InputRevision,
		ContinuationRef: ContinuationRef("c46-worker-continuation:" + physical.InvocationID),
		// A ready Controller Worker is carrier evidence only. Without a package
		// receipt it is not a logical continuation, so C2-4 reserves `running`
		// for the later receipt-backed activation transaction.
		State: AwaitStateRehydrating,
	}
}

func assertC46WaitingPhysicalAlignment(t *testing.T, waiting WaitingRecord, physical PhysicalExecution) {
	t.Helper()
	if waiting.Key.TaskID != physical.TaskID || waiting.Key.InvocationID != physical.InvocationID || waiting.Key.Generation != physical.Generation || waiting.ExecutionEpoch != physical.ExecutionEpoch || waiting.PreviousBindingRef != physical.BindingRef || waiting.InputRevision != physical.InputRevision {
		t.Fatalf("WaitingRecord and PhysicalExecution are not aligned: waiting=%#v physical=%#v", waiting, physical)
	}
	if waiting.State != AwaitStateRehydrating || physical.State != PhysicalExecutionProvisioning {
		t.Fatalf("unexpected pre-receipt lifecycle states: waiting=%s physical=%s", waiting.State, physical.State)
	}
}

func assertC46RunningWaitingPhysicalAlignment(t *testing.T, waiting WaitingRecord, physical PhysicalExecution) {
	t.Helper()
	if waiting.Key.TaskID != physical.TaskID || waiting.Key.InvocationID != physical.InvocationID || waiting.Key.Generation != physical.Generation || waiting.ExecutionEpoch != physical.ExecutionEpoch || waiting.PreviousBindingRef != physical.BindingRef || waiting.InputRevision != physical.InputRevision || waiting.State != AwaitStateRunning || physical.State != PhysicalExecutionRunning || !physical.PackageConsumed {
		t.Fatalf("running WaitingRecord and PhysicalExecution are not aligned: waiting=%#v physical=%#v", waiting, physical)
	}
}

func c46RuntimeEventCount(t *testing.T, repository RuntimeStateRepository, eventType string) int {
	t.Helper()
	events, err := repository.ListRuntimeEvents(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, event := range events {
		if event.EventType == eventType {
			count++
		}
	}
	return count
}

// TestC46RealWorkerDescriptorDurablePhysicalExecution is invoked by the real
// Worker smoke after it has obtained a Ready/running Controller descriptor.
// This parent only passes file paths and inspects the closed database bytes;
// Runtime A is the sole logical authority and repository writer.
func TestC46RealWorkerDescriptorDurablePhysicalExecution(t *testing.T) {
	descriptorPath := os.Getenv(c46ProcessHarnessDescriptor)
	if descriptorPath == "" {
		t.Skip("requires the real C4-6 Worker smoke descriptor")
	}
	encoded, err := os.ReadFile(descriptorPath)
	if err != nil {
		t.Fatal(err)
	}
	var descriptor c46WorkerExecutionDescriptor
	if err := json.Unmarshal(encoded, &descriptor); err != nil {
		t.Fatal(err)
	}
	databasePath := filepath.Join(t.TempDir(), "runtime.db")
	activeCrash := os.Getenv(c46ActiveCrashPoint) == "1"
	var marker c46ProcessMarker
	if activeCrash {
		marker = runC46ActiveCrashChild(t, databasePath, descriptorPath)
	} else {
		marker = runC46RuntimeChildWithDescriptor(t, databasePath, descriptorPath)
	}
	wantMode := "runtime-a-worker-descriptor"
	if activeCrash {
		wantMode = "runtime-a-active-crash"
	}
	if marker.PID == os.Getpid() || marker.Mode != wantMode || (activeCrash && !marker.ActiveCrashPointReady) {
		t.Fatalf("Runtime A was not isolated: parent=%d marker=%#v", os.Getpid(), marker)
	}
	if marker.PhysicalCount != 1 || marker.WaitingCount != 1 || marker.ReceiptCount != 1 || marker.Physical == nil || marker.Waiting == nil || marker.Receipt == nil || marker.ReceiptEvents != 1 || marker.ActivationEvents != 1 {
		t.Fatalf("expected one activated PhysicalExecution, WaitingRecord, receipt, and success events: %#v", marker)
	}
	want := physicalExecutionFromC46Descriptor(descriptor)
	got := *marker.Physical
	if got.WorkerID != want.WorkerID || got.WorkerName != want.WorkerName || got.DesiredRuntimeGeneration != want.DesiredRuntimeGeneration || got.AppliedRuntimeGeneration != want.AppliedRuntimeGeneration || got.MCPClientID != want.MCPClientID || got.CredentialBindingRef != want.CredentialBindingRef || got.ExecutionEpoch <= 0 || got.Revision != 4 || got.State != PhysicalExecutionRunning || !got.PackageConsumed {
		t.Fatalf("durable PhysicalExecution does not match real descriptor: got=%#v descriptor=%#v", got, descriptor)
	}
	assertC46RunningWaitingPhysicalAlignment(t, *marker.Waiting, got)
	if marker.Waiting.Revision != 2 {
		t.Fatalf("unexpected Waiting revision after atomic activation: %#v", marker.Waiting)
	}
	if marker.Receipt.Key() != (executionreceipt.Key{TaskID: got.TaskID, InvocationID: got.InvocationID, Generation: got.Generation, ExecutionEpoch: int64(got.ExecutionEpoch)}) || marker.Receipt.BindingRef != got.BindingRef || marker.Receipt.InputRevision != got.InputRevision || marker.Receipt.PackageDigest != got.AgentPackageDigest || marker.Receipt.SessionIdentity != descriptor.MatrixRoomRef || !marker.Receipt.Consumed || marker.Receipt.Revision != 1 {
		t.Fatalf("durable receipt does not match activated physical execution: receipt=%#v physical=%#v", marker.Receipt, got)
	}
	database, err := os.ReadFile(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"c4-6-test-only-token", "threadmill-it-admin", "test-only-gateway-key", "test-only-key", "X-Threadmill-Execution-Token", "controller_auth", "private_header", "credential_value", "provider_conversation", "provider_session", "session_state"} {
		if strings.Contains(string(database), forbidden) {
			t.Fatalf("closed Runtime database contains forbidden capability/provider state %q", forbidden)
		}
	}
	if activeCrash {
		runtimeB := runC46ProductionObserverRuntimeB(t, databasePath)
		if runtimeB.PID == os.Getpid() || runtimeB.PID == marker.PID || runtimeB.Mode != "runtime-b-production" {
			t.Fatalf("Runtime B was not isolated: parent=%d A=%d B=%#v", os.Getpid(), marker.PID, runtimeB)
		}
		if runtimeB.Candidates != 1 || runtimeB.Convergence != string(RecoveryStableActive) || runtimeB.Action != string(RecoveryActionCarrierActive) || !runtimeB.RuntimeBStarted || !runtimeB.SameSQLiteReopened || !runtimeB.ProductionObserverUsed || !runtimeB.WorkerIdentityVerified || !runtimeB.TeamHarnessTaskActive || !runtimeB.RuntimeGenerationVerified || !runtimeB.MCPAppliedVerified || !runtimeB.SameExecutionEpoch || runtimeB.ReplacementCreated || runtimeB.DuplicatePhysicalExecution || runtimeB.DuplicateReceipt || !runtimeB.PhaseOutputAbsent || !runtimeB.TeardownProgressAbsent {
			t.Fatalf("production Runtime B recovery marker=%#v", runtimeB)
		}
		encoded, _ := json.Marshal(struct {
			RuntimeACrashed            bool   `json:"runtimeACrashed"`
			PostKillTaskActive         bool   `json:"postKillTaskActive"`
			RuntimeBStarted            bool   `json:"runtimeBStarted"`
			SameSQLiteReopened         bool   `json:"sameSQLiteReopened"`
			CandidateCount             int    `json:"candidateCount"`
			ProductionObserverUsed     bool   `json:"productionObserverUsed"`
			WorkerIdentityVerified     bool   `json:"workerIdentityVerified"`
			TeamHarnessTaskActive      bool   `json:"teamHarnessTaskActive"`
			RuntimeGenerationVerified  bool   `json:"runtimeGenerationVerified"`
			MCPAppliedVerified         bool   `json:"mcpAppliedVerified"`
			CarrierStillActive         bool   `json:"carrierStillActive"`
			RecoveryDisposition        string `json:"recoveryDisposition"`
			SameExecutionEpoch         bool   `json:"sameExecutionEpoch"`
			ReplacementCreated         bool   `json:"replacementCreated"`
			DuplicatePhysicalExecution bool   `json:"duplicatePhysicalExecution"`
			DuplicateReceipt           bool   `json:"duplicateReceipt"`
			PhaseOutputAbsent          bool   `json:"phaseOutputAbsent"`
			TeardownProgressAbsent     bool   `json:"teardownProgressAbsent"`
		}{
			RuntimeACrashed: true, PostKillTaskActive: true, RuntimeBStarted: runtimeB.RuntimeBStarted,
			SameSQLiteReopened: runtimeB.SameSQLiteReopened, CandidateCount: runtimeB.Candidates,
			ProductionObserverUsed: runtimeB.ProductionObserverUsed, WorkerIdentityVerified: runtimeB.WorkerIdentityVerified,
			TeamHarnessTaskActive: runtimeB.TeamHarnessTaskActive, RuntimeGenerationVerified: runtimeB.RuntimeGenerationVerified,
			MCPAppliedVerified: runtimeB.MCPAppliedVerified, CarrierStillActive: runtimeB.Action == string(RecoveryActionCarrierActive),
			RecoveryDisposition: runtimeB.Convergence, SameExecutionEpoch: runtimeB.SameExecutionEpoch,
			ReplacementCreated: runtimeB.ReplacementCreated, DuplicatePhysicalExecution: runtimeB.DuplicatePhysicalExecution,
			DuplicateReceipt: runtimeB.DuplicateReceipt, PhaseOutputAbsent: runtimeB.PhaseOutputAbsent,
			TeardownProgressAbsent: runtimeB.TeardownProgressAbsent,
		})
		t.Logf("C46_HEALTHY_RESTART_RECOVERY=%s", encoded)
	}
}

func runC46ProductionObserverRuntimeB(t *testing.T, databasePath string) c46ProcessMarker {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run=^TestC46ProductionObserverRuntimeBChild$")
	command.Env = append(os.Environ(), c46ProcessHarnessMode+"=runtime-b-production", c46ProcessHarnessDB+"="+databasePath)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("production Runtime B child failed: %v\n%s", err, output)
	}
	marker, err := parseC46ProcessMarker(output)
	if err != nil {
		t.Fatalf("decode production Runtime B marker: %v; output=%s", err, output)
	}
	return marker
}

func runC46RuntimeChildWithDescriptor(t *testing.T, databasePath, descriptorPath string) c46ProcessMarker {
	t.Helper()
	if descriptorPath == "" {
		t.Fatal("real Worker descriptor path is required")
	}
	return runC46RuntimeChildEnv(t, "runtime-a-worker-descriptor", databasePath, c46ProcessHarnessDescriptor+"="+descriptorPath)
}

func runC46ActiveCrashChild(t *testing.T, databasePath, descriptorPath string) c46ProcessMarker {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run=^TestC46DurableProcessHarnessChild$")
	command.Env = append(os.Environ(), c46ProcessHarnessMode+"=runtime-a-active-crash", c46ProcessHarnessDB+"="+databasePath, c46ProcessHarnessDescriptor+"="+descriptorPath)
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(stdout)
	var marker c46ProcessMarker
	found := false
	for scanner.Scan() {
		if candidate, err := parseC46ProcessMarker(scanner.Bytes()); err == nil {
			marker, found = candidate, true
			break
		}
	}
	if err := scanner.Err(); err != nil || !found || !marker.ActiveCrashPointReady || marker.PID != command.Process.Pid {
		_ = command.Process.Kill()
		_ = command.Wait()
		t.Fatalf("active crash marker=%#v found=%t err=%v childPID=%d", marker, found, err, command.Process.Pid)
	}
	// This bypasses Runtime shutdown completely: no repository Close, external
	// cleanup, Worker/task deletion, or credential/token revocation is called.
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err == nil || command.ProcessState == nil || !command.ProcessState.Exited() {
		t.Fatalf("Runtime A did not die abnormally: err=%v state=%v", err, command.ProcessState)
	}
	if _, err := os.Stat(databasePath); err != nil {
		t.Fatalf("Runtime SQLite did not survive crash point: %v", err)
	}
	return marker
}

func TestC46DurableProcessHarnessUsesTwoRuntimeProcessesAndOneSQLiteDB(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "runtime.db")
	first := runC46RuntimeChild(t, "runtime-a", databasePath)
	second := runC46RuntimeChild(t, "runtime-b", databasePath)
	if first.PID == second.PID || first.PID == os.Getpid() || second.PID == os.Getpid() {
		t.Fatalf("runtime processes were not isolated: parent=%d A=%d B=%d", os.Getpid(), first.PID, second.PID)
	}
	if first.Mode != "runtime-a" || second.Mode != "runtime-b" || first.Candidates != 0 || second.Candidates != 1 || second.Convergence != string(RecoveryStableActive) || second.Action != string(RecoveryActionCarrierActive) {
		t.Fatalf("A=%#v B=%#v", first, second)
	}
	// The parent intentionally never opens the repository. B can discover A's
	// durable record only through the shared SQLite path supplied to both child
	// processes; no InMemoryPhysicalExecutionStore is constructed here.
}

func runC46RuntimeChild(t *testing.T, mode, databasePath string) c46ProcessMarker {
	t.Helper()
	return runC46RuntimeChildEnv(t, mode, databasePath)
}

func runC46RuntimeChildEnv(t *testing.T, mode, databasePath string, extraEnv ...string) c46ProcessMarker {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run=^TestC46DurableProcessHarnessChild$")
	command.Env = append(os.Environ(), append([]string{c46ProcessHarnessMode + "=" + mode, c46ProcessHarnessDB + "=" + databasePath}, extraEnv...)...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("%s child failed: %v\n%s", mode, err, output)
	}
	marker, markerErr := parseC46ProcessMarker(output)
	if markerErr != nil {
		t.Fatalf("decode %s marker: %v; output=%s", mode, markerErr, output)
	}
	return marker
}

func parseC46ProcessMarker(output []byte) (c46ProcessMarker, error) {
	prefix := []byte("C46_PROCESS_MARKER=")
	index := -1
	for offset := 0; offset+len(prefix) <= len(output); offset++ {
		if string(output[offset:offset+len(prefix)]) == string(prefix) {
			index = offset + len(prefix)
			break
		}
	}
	if index < 0 {
		return c46ProcessMarker{}, errors.New("child omitted C46 process marker")
	}
	end := index
	for end < len(output) && output[end] != '\n' && output[end] != '\r' {
		end++
	}
	var marker c46ProcessMarker
	if err := json.Unmarshal(output[index:end], &marker); err != nil {
		return c46ProcessMarker{}, err
	}
	return marker, nil
}
