package runtime

import (
	"context"
	"testing"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/executionreceipt"
	phasemcp "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/mcp/phase"
)

func receiptExecution(t *testing.T) (*InMemoryPhysicalExecutionStore, PhysicalExecution) {
	t.Helper()
	store := NewInMemoryPhysicalExecutionStore()
	value := physicalRecord(2, "task-b")
	value.PackageConsumed = false
	value.State = PhysicalExecutionAccepted
	created, err := store.Create(context.Background(), value)
	if err != nil {
		t.Fatal(err)
	}
	return store, created
}

func receiptBinding(value PhysicalExecution) phasemcp.InvocationBinding {
	return phasemcp.InvocationBinding{TaskID: value.TaskID, InvocationID: value.InvocationID, Generation: value.Generation, ExecutionEpoch: int64(value.ExecutionEpoch), BindingRef: value.BindingRef, InputRevision: value.InputRevision}
}

func TestPackageConsumptionCoordinatorValidatesAuthorityAndIsIdempotent(t *testing.T) {
	physical, execution := receiptExecution(t)
	store := executionreceipt.NewInMemoryStore()
	coordinator := &PackageConsumptionCoordinator{Store: store, PhysicalExecutions: physical}
	submission := executionreceipt.Submission{PackageDigest: execution.AgentPackageDigest, SessionIdentity: execution.AgentSessionRef, Consumed: true}
	first, err := coordinator.ConfirmPackageConsumption(context.Background(), receiptBinding(execution), submission)
	if err != nil {
		t.Fatal(err)
	}
	second, err := coordinator.ConfirmPackageConsumption(context.Background(), receiptBinding(execution), submission)
	if err != nil || second != first {
		t.Fatalf("idempotent receipt=%#v err=%v", second, err)
	}
	for name, mutate := range map[string]func(*phasemcp.InvocationBinding, *executionreceipt.Submission){
		"wrong-digest": func(_ *phasemcp.InvocationBinding, s *executionreceipt.Submission) { s.PackageDigest = "wrong" },
		"old-epoch":    func(b *phasemcp.InvocationBinding, _ *executionreceipt.Submission) { b.ExecutionEpoch = 1 },
		"old-binding": func(b *phasemcp.InvocationBinding, _ *executionreceipt.Submission) {
			b.BindingRef, b.InputRevision = "B1", "r4"
		},
		"other-invocation": func(b *phasemcp.InvocationBinding, _ *executionreceipt.Submission) { b.InvocationID = "other" },
		"synthesized-session": func(_ *phasemcp.InvocationBinding, s *executionreceipt.Submission) {
			s.SessionIdentity = "qwenpaw://worker/task"
		},
	} {
		t.Run(name, func(t *testing.T) {
			binding, candidate := receiptBinding(execution), submission
			mutate(&binding, &candidate)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, err := coordinator.ConfirmPackageConsumption(ctx, binding, candidate); err == nil {
				t.Fatal("invalid receipt was accepted")
			}
		})
	}
}

func TestReceiptFromEpochOneCannotReadyEpochTwo(t *testing.T) {
	_, execution := receiptExecution(t)
	store := executionreceipt.NewInMemoryStore()
	_, _, err := store.PutIfAbsent(context.Background(), executionreceipt.Receipt{TaskID: execution.TaskID, InvocationID: execution.InvocationID, Generation: execution.Generation, ExecutionEpoch: 1, BindingRef: "B1", InputRevision: "r4", PackageDigest: "old", SessionIdentity: "matrix:!old:test", Consumed: true})
	if err != nil {
		t.Fatal(err)
	}
	provisioner := PhysicalExecutionProvisioner{Receipts: store}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := provisioner.waitForPackageConsumption(ctx, execution); err == nil {
		t.Fatal("epoch-1 receipt readied epoch 2")
	}
}
