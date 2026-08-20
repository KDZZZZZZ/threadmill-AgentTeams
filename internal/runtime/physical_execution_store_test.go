package runtime

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func physicalRecord(epoch ExecutionEpoch, task string) PhysicalExecution {
	return PhysicalExecution{TaskID: "task-a", InvocationID: "inv-a", Generation: 3, ExecutionEpoch: epoch, WorkerID: "worker-" + task, WorkerName: "worker-" + task, TeamHarnessTaskID: task, TeamHarnessAssignedTo: "worker-" + task, BindingRef: "B2", InputRevision: "r5", AgentSessionRef: "matrix:!" + task + ":test", AgentPackageDigest: "digest-" + task, PackageConsumed: true, MCPClientID: "threadmill", CredentialBindingRef: "credential-" + task, ExecutionAuthorizationRef: "auth-" + task, WorkspaceLeaseRef: "lease-" + task, DesiredRuntimeGeneration: 9, AppliedRuntimeGeneration: 9, State: PhysicalExecutionAccepted, ObservedTaskStatus: "in_progress"}
}

func TestPhysicalExecutionStorePreservesEpochHistory(t *testing.T) {
	store := NewInMemoryPhysicalExecutionStore()
	epochA := physicalRecord(1, "task-a")
	epochA.State = PhysicalExecutionTerminated
	a, err := store.Create(context.Background(), epochA)
	if err != nil {
		t.Fatal(err)
	}
	b, err := store.Create(context.Background(), physicalRecord(2, "task-b"))
	if err != nil {
		t.Fatal(err)
	}
	if a.TeamHarnessTaskID == b.TeamHarnessTaskID {
		t.Fatal("task-B reused task-A")
	}
	all, err := store.ListByInvocation(context.Background(), "task-a", "inv-a", 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 || all[0].TeamHarnessTaskID != "task-a" || all[1].TeamHarnessTaskID != "task-b" {
		t.Fatalf("history=%#v", all)
	}
	if _, found, _ := store.Get(context.Background(), a.Key()); !found {
		t.Fatal("epoch-A removed")
	}
	storedA, found, err := store.Get(context.Background(), a.Key())
	if err != nil || !found || storedA.State != PhysicalExecutionTerminated {
		t.Fatalf("terminal epoch-A was not retained: record=%#v found=%t err=%v", storedA, found, err)
	}
}
func TestPhysicalExecutionStoreCASAndRedaction(t *testing.T) {
	store := NewInMemoryPhysicalExecutionStore()
	created, err := store.Create(context.Background(), physicalRecord(2, "task-b"))
	if err != nil {
		t.Fatal(err)
	}
	stale := created
	stale.State = PhysicalExecutionTerminated
	if _, swapped, err := store.CompareAndSwap(context.Background(), created.Key(), created.Revision+1, stale); err != nil || swapped {
		t.Fatalf("stale CAS swapped=%t err=%v", swapped, err)
	}
	updated := created
	updated.State = PhysicalExecutionRunning
	updated, swapped, err := store.CompareAndSwap(context.Background(), created.Key(), created.Revision, updated)
	if err != nil || !swapped || updated.Revision != 2 {
		t.Fatalf("CAS=%#v swapped=%t err=%v", updated, swapped, err)
	}
	encoded, err := json.Marshal(updated)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "secret-token") || strings.Contains(string(encoded), "X-Threadmill-Execution-Token") {
		t.Fatalf("physical record leaked secret material: %s", encoded)
	}
}
func TestValidatePhysicalExecutionReady(t *testing.T) {
	ready := physicalRecord(2, "task-b")
	if !ValidatePhysicalExecutionReady(ready) {
		t.Fatal("complete physical execution not ready")
	}
	for name, mutate := range map[string]func(*PhysicalExecution){"worker": func(e *PhysicalExecution) { e.WorkerID = "" }, "task": func(e *PhysicalExecution) { e.TeamHarnessTaskID = "" }, "assigned-worker": func(e *PhysicalExecution) { e.TeamHarnessAssignedTo = "other-worker" }, "agent-session": func(e *PhysicalExecution) { e.AgentSessionRef = "" }, "package-digest": func(e *PhysicalExecution) { e.AgentPackageDigest = "" }, "receipt": func(e *PhysicalExecution) { e.PackageConsumed = false }, "binding": func(e *PhysicalExecution) { e.BindingRef = "" }, "revision": func(e *PhysicalExecution) { e.InputRevision = "" }, "generation": func(e *PhysicalExecution) { e.AppliedRuntimeGeneration-- }, "mcp": func(e *PhysicalExecution) { e.MCPClientID = "" }, "acceptance": func(e *PhysicalExecution) { e.ObservedTaskStatus = "assigned"; e.TaskAcknowledged = false }} {
		t.Run(name, func(t *testing.T) {
			value := ready
			mutate(&value)
			if ValidatePhysicalExecutionReady(value) {
				t.Fatal("invalid execution reported ready")
			}
		})
	}
}
