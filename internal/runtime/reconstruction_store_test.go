package runtime

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/phaseagent"
)

func reconstructionFixture(t *testing.T) (*SQLiteRuntimeStateRepository, WaitingKey, DurableReconstructionStore) {
	t.Helper()
	repo, err := OpenSQLiteRuntimeStateRepository(filepath.Join(t.TempDir(), "runtime.db"))
	if err != nil {
		t.Fatal(err)
	}
	key := WaitingKey{TaskID: "task", InvocationID: "invocation", Generation: 2}
	return repo, key, repo.Reconstruction()
}
func putReconstructionFixture(t *testing.T, store DurableReconstructionStore, key WaitingKey) {
	t.Helper()
	if _, err := store.PutWorkspace(context.Background(), DurableWorkspace{Key: key, Ref: "workspace-r7", AllowedDirs: []string{"src", "out"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutContextSlice(context.Background(), DurableContextSlice{Key: key, Ref: "context-r9", BaselineRef: "baseline-r1", Content: "approved context"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutTaskMemory(context.Background(), DurableTaskMemory{Key: key, Ref: "memory-r3", View: phaseagent.TaskMemoryBufferView{Candidates: []phaseagent.TaskMemoryCandidateView{{CandidateID: "candidate-1"}}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutExecutionDescriptor(context.Background(), DurableExecutionDescriptor{Key: key, TaskContract: "contract", PhaseInstruction: "instruction", TaskSpec: "spec", WorkspaceRef: "workspace-r7", ContextSliceRef: "context-r9", TaskMemoryRef: "memory-r3"}); err != nil {
		t.Fatal(err)
	}
}

func TestDurableReconstructionStoreColdReopenAndIdempotency(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime.db")
	repo, err := OpenSQLiteRuntimeStateRepository(path)
	if err != nil {
		t.Fatal(err)
	}
	key, store := WaitingKey{TaskID: "task", InvocationID: "invocation", Generation: 2}, repo.Reconstruction()
	putReconstructionFixture(t, store, key)
	first, err := store.AcquireWorkspaceLease(context.Background(), DurableWorkspaceLease{Ref: "lease-e2", Key: key, WorkspaceRef: "workspace-r7", ExecutionEpoch: 2})
	if err != nil {
		t.Fatal(err)
	}
	again, err := store.AcquireWorkspaceLease(context.Background(), DurableWorkspaceLease{Ref: "lease-e2", Key: key, WorkspaceRef: "workspace-r7", ExecutionEpoch: 2})
	if err != nil || again.Revision != first.Revision || again.Fence != first.Fence {
		t.Fatalf("idempotent lease=%+v err=%v", again, err)
	}
	if _, err := store.AcquireWorkspaceLease(context.Background(), DurableWorkspaceLease{Ref: "other", Key: key, WorkspaceRef: "workspace-r7", ExecutionEpoch: 2}); !errors.Is(err, ErrWorkspaceLeaseConflict) {
		t.Fatalf("err=%v", err)
	}
	if _, swapped, err := store.ReleaseWorkspaceLease(context.Background(), first, first.Revision-1); err != nil || swapped {
		t.Fatalf("stale release swapped=%v err=%v", swapped, err)
	}
	released, swapped, err := store.ReleaseWorkspaceLease(context.Background(), first, first.Revision)
	if err != nil || !swapped || released.State != "released" {
		t.Fatalf("released=%+v swapped=%v err=%v", released, swapped, err)
	}
	if _, swapped, err = store.ReleaseWorkspaceLease(context.Background(), released, released.Revision); err != nil || !swapped {
		t.Fatalf("idempotent release swapped=%v err=%v", swapped, err)
	}
	if err = repo.Close(); err != nil {
		t.Fatal(err)
	}
	repo, err = OpenSQLiteRuntimeStateRepository(path)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()
	store = repo.Reconstruction()
	if descriptor, found, err := store.GetExecutionDescriptor(context.Background(), key); err != nil || !found || descriptor.Revision != 1 {
		t.Fatalf("descriptor=%+v found=%v err=%v", descriptor, found, err)
	}
	if current, found, err := store.ReadWorkspaceLease(context.Background(), "lease-e2", key, 2); err != nil || !found || current.State != "released" {
		t.Fatalf("lease=%+v found=%v err=%v", current, found, err)
	}
	if _, err = store.PutContextSlice(context.Background(), DurableContextSlice{Key: key, Ref: "context-r9", BaselineRef: "baseline-r1", Content: "approved context"}); err != nil {
		t.Fatalf("reopened idempotent put: %v", err)
	}
}

func TestDurableReconstructionStoreRejectsSecretsPathsAndConflicts(t *testing.T) {
	repo, key, store := reconstructionFixture(t)
	defer repo.Close()
	if _, err := store.PutWorkspace(context.Background(), DurableWorkspace{Key: key, Ref: "workspace", AllowedDirs: []string{"C:\\private"}}); err == nil {
		t.Fatal("absolute path accepted")
	}
	if _, err := store.PutContextSlice(context.Background(), DurableContextSlice{Key: key, Ref: "context", Content: "execution_token"}); err == nil {
		t.Fatal("secret-bearing durable content accepted")
	}
	if _, err := store.PutWorkspace(context.Background(), DurableWorkspace{Key: key, Ref: "workspace", AllowedDirs: []string{"src"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutWorkspace(context.Background(), DurableWorkspace{Key: key, Ref: "workspace", AllowedDirs: []string{"other"}}); !errors.Is(err, ErrReconstructionConflict) {
		t.Fatalf("err=%v", err)
	}
}

func TestDurableReconstructionStoreConcurrentImmutableWriteHasOneAuthority(t *testing.T) {
	repo, key, store := reconstructionFixture(t)
	defer repo.Close()
	const writers = 8
	start := make(chan struct{})
	var wg sync.WaitGroup
	results := make(chan error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := store.PutContextSlice(context.Background(), DurableContextSlice{Key: key, Ref: "context", Content: "same"})
			results <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal(err)
		}
	}
	value, found, err := store.GetContextSlice(context.Background(), "context", key)
	if err != nil || !found || value.Revision != 1 || value.Content != "same" {
		t.Fatalf("value=%+v found=%v err=%v", value, found, err)
	}
}

func TestDurableReconstructionAuthorityUsesOnlyRepositoryRecords(t *testing.T) {
	repo, key, store := reconstructionFixture(t)
	defer repo.Close()
	putReconstructionFixture(t, store, key)
	waiting := testWaitingRecord()
	waiting.Key = key
	waiting.WorkspaceRef = "workspace-r7"
	waiting.ContextSliceRef = "context-r9"
	waiting.TaskMemoryBufferRef = "memory-r3"
	waiting.AllowedDirs = []string{"src", "out"}
	authority := DurableReconstructionAuthority{Store: store}
	workspace, err := authority.ReconstructWorkspace(context.Background(), waiting, ContinuationMaterial{WorkspaceRef: "workspace-r7"})
	if err != nil || workspace.WriteLeaseHeld {
		t.Fatalf("workspace=%+v err=%v", workspace, err)
	}
	contextValue, err := authority.ReconstructContext(context.Background(), waiting, ContinuationMaterial{ContextSliceRef: "context-r9", ContextBaselineRef: "baseline-r1"})
	if err != nil || contextValue.SliceRef != "context-r9" {
		t.Fatalf("context=%+v err=%v", contextValue, err)
	}
	memory, err := authority.ReconstructTaskMemory(context.Background(), waiting, ContinuationMaterial{TaskMemoryBufferRef: "memory-r3"})
	if err != nil || memory.BufferRef != "memory-r3" {
		t.Fatalf("memory=%+v err=%v", memory, err)
	}
	if _, err := authority.ReconstructWorkspace(context.Background(), waiting, ContinuationMaterial{WorkspaceRef: "foreign"}); !errors.Is(err, ErrReconstructionConflict) {
		t.Fatalf("foreign workspace err=%v", err)
	}
}
