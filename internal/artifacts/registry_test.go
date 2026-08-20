package artifacts

import (
	"context"
	"os"
	"path/filepath"
	"testing"
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
