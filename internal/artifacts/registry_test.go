package artifacts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type memoryRecorder struct{ events []Event }

func (r *memoryRecorder) Record(_ context.Context, event Event) error {
	r.events = append(r.events, event)
	return nil
}

func TestRegisterFencesPathsDeduplicatesAndPreservesInvocationAccess(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "out"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "out", "report.md"), []byte("report"), 0644); err != nil {
		t.Fatal(err)
	}
	ownerA := TrustedOwner{TaskID: "task-a", InvocationID: "inv-a", WorkspaceRoot: root, AllowedDirs: []string{"out"}}
	ownerB := TrustedOwner{TaskID: "task-b", InvocationID: "inv-b", WorkspaceRoot: root, AllowedDirs: []string{"out"}}
	recorder := &memoryRecorder{}
	registry := NewInMemoryRegistry(recorder)
	ref, err := registry.Register(context.Background(), RegisterRequest{Owner: ownerA, ControlledPath: "out/report.md", Kind: ArtifactTypeGeneratedReport})
	if err != nil {
		t.Fatal(err)
	}
	again, err := registry.Register(context.Background(), RegisterRequest{Owner: ownerA, ControlledPath: "out/report.md", Kind: ArtifactTypeGeneratedReport})
	if err != nil || again != ref {
		t.Fatalf("idempotency ref=%q again=%q err=%v", ref, again, err)
	}
	if err := registry.ValidateReferences(context.Background(), ownerA, []ArtifactRef{ref}); err != nil {
		t.Fatal(err)
	}
	if err := registry.ValidateReferences(context.Background(), ownerB, []ArtifactRef{ref}); err == nil {
		t.Fatal("other Task must not gain access by guessing ref")
	}
	if _, err := registry.Register(context.Background(), RegisterRequest{Owner: ownerA, ControlledPath: "../secret", Kind: ArtifactTypeToolOutput}); err == nil {
		t.Fatal("path escape accepted")
	}
	if len(recorder.events) != 2 || recorder.events[0].Type != EventArtifactRegistered {
		t.Fatalf("events=%#v", recorder.events)
	}
}

type recordingDurableStore struct {
	metadata DurableMetadata
	owner    TrustedOwner
	calls    int
	err      error
}

func (s *recordingDurableStore) RegisterArtifact(_ context.Context, metadata DurableMetadata, owner TrustedOwner) (ArtifactRef, bool, error) {
	s.calls++
	s.metadata, s.owner = metadata, owner
	if s.err != nil {
		return "", false, s.err
	}
	return metadata.Ref, true, nil
}
func (s *recordingDurableStore) GetArtifact(context.Context, ArtifactRef) (DurableMetadata, bool, error) {
	return DurableMetadata{}, false, nil
}
func (s *recordingDurableStore) ValidateArtifactAccess(context.Context, TrustedOwner, []ArtifactRef) error {
	return nil
}

type recordingPublisher struct {
	ref   string
	calls int
	err   error
}

func (p *recordingPublisher) Publish(_ context.Context, _ string, _ string) (string, error) {
	p.calls++
	return p.ref, p.err
}

func TestDurableRegistryPublishesBeforeMetadataAndNeverStoresWorkspacePath(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "out"), 0o755); err != nil {
		t.Fatal(err)
	}
	contents := []byte("report")
	if err := os.WriteFile(filepath.Join(root, "out", "report.md"), contents, 0o600); err != nil {
		t.Fatal(err)
	}
	owner := TrustedOwner{TaskID: "task", InvocationID: "invocation", Generation: 3, WorkspaceRoot: root, AllowedDirs: []string{"out"}}
	store := &recordingDurableStore{}
	publisher := &recordingPublisher{ref: "s3://threadmill/artifacts/sha256/content"}
	registry := NewDurableRegistry(store, publisher)
	ref, err := registry.Register(context.Background(), RegisterRequest{Owner: owner, ControlledPath: "out/report.md", Kind: ArtifactTypeGeneratedReport})
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(contents)
	if ref == "" || publisher.calls != 1 || store.calls != 1 || store.metadata.ContentHash != hex.EncodeToString(digest[:]) || store.metadata.BlobRef != publisher.ref || filepath.IsAbs(store.metadata.BlobRef) || store.metadata.BlobRef == root || !store.metadata.CreatedAt.After(time.Time{}) {
		t.Fatalf("ref=%q publisher=%d metadata=%#v", ref, publisher.calls, store.metadata)
	}
	if store.owner.Generation != 3 || store.owner.TaskID != owner.TaskID || store.owner.InvocationID != owner.InvocationID {
		t.Fatalf("trusted owner was not preserved: %#v", store.owner)
	}
}

func TestDurableRegistryDoesNotMutateMetadataWhenPublishFails(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "out"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "out", "report.md"), []byte("report"), 0o600); err != nil {
		t.Fatal(err)
	}
	store := &recordingDurableStore{}
	publisher := &recordingPublisher{err: errors.New("object store unavailable")}
	registry := NewDurableRegistry(store, publisher)
	_, err := registry.Register(context.Background(), RegisterRequest{Owner: TrustedOwner{TaskID: "task", InvocationID: "inv", Generation: 1, WorkspaceRoot: root, AllowedDirs: []string{"out"}}, ControlledPath: "out/report.md", Kind: ArtifactTypeGeneratedReport})
	if err == nil || publisher.calls != 1 || store.calls != 0 {
		t.Fatalf("err=%v publisher=%d metadata calls=%d", err, publisher.calls, store.calls)
	}
}
