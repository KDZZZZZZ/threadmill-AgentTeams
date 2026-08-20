package executionreceipt

import (
	"context"
	"testing"
)

func TestInMemoryStoreIsIdempotentAndRejectsConflicts(t *testing.T) {
	store := NewInMemoryStore()
	receipt := Receipt{TaskID: "task-a", InvocationID: "inv-a", Generation: 3, ExecutionEpoch: 2, BindingRef: "B2", InputRevision: "r5", PackageDigest: "digest-b", SessionIdentity: "matrix:!room:test", Consumed: true}
	created, inserted, err := store.PutIfAbsent(context.Background(), receipt)
	if err != nil || !inserted || created.Revision != 1 || created.RecordedAt.IsZero() {
		t.Fatalf("create=%#v inserted=%t err=%v", created, inserted, err)
	}
	duplicate, inserted, err := store.PutIfAbsent(context.Background(), receipt)
	if err != nil || inserted || duplicate != created {
		t.Fatalf("duplicate=%#v inserted=%t err=%v", duplicate, inserted, err)
	}
	conflict := receipt
	conflict.SessionIdentity = "matrix:!other:test"
	if _, _, err := store.PutIfAbsent(context.Background(), conflict); err == nil {
		t.Fatal("conflicting session receipt was accepted")
	}
	epochOne := receipt
	epochOne.ExecutionEpoch = 1
	epochOne.BindingRef = "B1"
	epochOne.InputRevision = "r4"
	epochOne.PackageDigest = "digest-a"
	if _, inserted, err := store.PutIfAbsent(context.Background(), epochOne); err != nil || !inserted {
		t.Fatalf("epoch history did not coexist: inserted=%t err=%v", inserted, err)
	}
}
