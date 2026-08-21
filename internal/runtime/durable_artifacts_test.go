package runtime

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/artifacts"
)

func durableArtifactMetadata(owner artifacts.TrustedOwner, hash, blob string) artifacts.DurableMetadata {
	return artifacts.DurableMetadata{Ref: artifacts.ArtifactRef("artifact-" + hash[:24]), Type: artifacts.ArtifactTypeGeneratedReport, ContentHash: hash, BlobRef: blob, OriginTaskID: owner.TaskID, OriginInvocationID: owner.InvocationID, CreatedAt: time.Now().UTC()}
}

func TestDurableArtifactRegistrationIsAtomicAndReopens(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "runtime.db")
	r, err := OpenSQLiteRuntimeStateRepository(path)
	if err != nil {
		t.Fatal(err)
	}
	owner := artifacts.TrustedOwner{TaskID: "task", InvocationID: "inv", Generation: 2}
	metadata := durableArtifactMetadata(owner, "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", "objects/sha256/0123")
	ref, created, err := r.ArtifactStore().RegisterArtifact(ctx, metadata, owner)
	if err != nil || !created || ref != metadata.Ref {
		t.Fatalf("register ref=%q created=%t err=%v", ref, created, err)
	}
	if err = r.ArtifactStore().ValidateArtifactAccess(ctx, owner, []artifacts.ArtifactRef{ref}); err != nil {
		t.Fatal(err)
	}
	if _, created, err = r.ArtifactStore().RegisterArtifact(ctx, metadata, owner); err != nil || created {
		t.Fatalf("duplicate created=%t err=%v", created, err)
	}
	events, err := r.ListRuntimeEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, event := range events {
		if event.EventType == artifacts.EventArtifactRegistered {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("registered events=%d", count)
	}
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	r, err = OpenSQLiteRuntimeStateRepository(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got, found, err := r.ArtifactStore().GetArtifact(ctx, ref)
	if err != nil || !found || got.ContentHash != metadata.ContentHash || got.BlobRef != metadata.BlobRef {
		t.Fatalf("reopen artifact=%#v found=%t err=%v", got, found, err)
	}
	if err = r.ArtifactStore().ValidateArtifactAccess(ctx, owner, []artifacts.ArtifactRef{ref}); err != nil {
		t.Fatal(err)
	}
	if _, created, err = r.ArtifactStore().RegisterArtifact(ctx, metadata, owner); err != nil || created {
		t.Fatalf("reopen duplicate created=%t err=%v", created, err)
	}
}

func TestDurableArtifactAccessIsLogicalAcrossEpochAndFencedAcrossInvocation(t *testing.T) {
	ctx := context.Background()
	r, err := OpenSQLiteRuntimeStateRepository(filepath.Join(t.TempDir(), "runtime.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	ownerA := artifacts.TrustedOwner{TaskID: "task", InvocationID: "inv-a", Generation: 2}
	metadata := durableArtifactMetadata(ownerA, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "objects/sha256/a")
	ref, _, err := r.ArtifactStore().RegisterArtifact(ctx, metadata, ownerA)
	if err != nil {
		t.Fatal(err)
	}
	// A fresh Epoch does not change the TaskID+InvocationID access key.
	if err = r.ArtifactStore().ValidateArtifactAccess(ctx, artifacts.TrustedOwner{TaskID: "task", InvocationID: "inv-a", Generation: 2}, []artifacts.ArtifactRef{ref}); err != nil {
		t.Fatal(err)
	}
	if err = r.ArtifactStore().ValidateArtifactAccess(ctx, artifacts.TrustedOwner{TaskID: "task", InvocationID: "inv-b", Generation: 2}, []artifacts.ArtifactRef{ref}); err == nil {
		t.Fatal("cross invocation access granted")
	}
}

func TestDurableArtifactRegistrationOutboxFailureRollsBack(t *testing.T) {
	ctx := context.Background()
	r, err := OpenSQLiteRuntimeStateRepository(filepath.Join(t.TempDir(), "runtime.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err = r.db.Exec("CREATE TRIGGER reject_artifact_event BEFORE INSERT ON runtime_events WHEN NEW.event_type='ArtifactRegistered' BEGIN SELECT RAISE(ABORT, 'outbox unavailable'); END"); err != nil {
		t.Fatal(err)
	}
	owner := artifacts.TrustedOwner{TaskID: "task", InvocationID: "inv", Generation: 2}
	metadata := durableArtifactMetadata(owner, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "objects/sha256/b")
	if _, _, err = r.ArtifactStore().RegisterArtifact(ctx, metadata, owner); err == nil {
		t.Fatal("outbox failure committed artifact")
	}
	if _, found, err := r.ArtifactStore().GetArtifact(ctx, metadata.Ref); err != nil || found {
		t.Fatalf("metadata survived rollback found=%t err=%v", found, err)
	}
	if err = r.ArtifactStore().ValidateArtifactAccess(ctx, owner, []artifacts.ArtifactRef{metadata.Ref}); err == nil {
		t.Fatal("grant survived rollback")
	}
}

func TestDurableArtifactRegistrationConcurrentDuplicateHasOneEvent(t *testing.T) {
	ctx := context.Background()
	r, err := OpenSQLiteRuntimeStateRepository(filepath.Join(t.TempDir(), "runtime.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	owner := artifacts.TrustedOwner{TaskID: "task", InvocationID: "inv", Generation: 2}
	metadata := durableArtifactMetadata(owner, "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", "objects/sha256/c")
	start := make(chan struct{})
	errs := make(chan error, 2)
	var group sync.WaitGroup
	for range 2 {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			_, _, err := r.ArtifactStore().RegisterArtifact(ctx, metadata, owner)
			errs <- err
		}()
	}
	close(start)
	group.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	events, err := r.ListRuntimeEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, event := range events {
		if event.EventType == artifacts.EventArtifactRegistered {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("events=%d", count)
	}
}

func TestDurableArtifactRegistrationRejectsConflictingMetadataForSameHash(t *testing.T) {
	ctx := context.Background()
	r, err := OpenSQLiteRuntimeStateRepository(filepath.Join(t.TempDir(), "runtime.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	owner := artifacts.TrustedOwner{TaskID: "task", InvocationID: "inv", Generation: 2}
	metadata := durableArtifactMetadata(owner, "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd", "s3://bucket/sha256/d")
	if _, _, err = r.ArtifactStore().RegisterArtifact(ctx, metadata, owner); err != nil {
		t.Fatal(err)
	}
	conflicting := metadata
	conflicting.Type = artifacts.ArtifactTypeToolOutput
	if _, _, err = r.ArtifactStore().RegisterArtifact(ctx, conflicting, owner); err == nil {
		t.Fatal("same hash with conflicting artifact type accepted")
	}
	conflicting = metadata
	conflicting.BlobRef = "s3://bucket/other/d"
	if _, _, err = r.ArtifactStore().RegisterArtifact(ctx, conflicting, owner); err == nil {
		t.Fatal("same hash with conflicting blob ref accepted")
	}
	events, err := r.ListRuntimeEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].EventType != artifacts.EventArtifactRegistered {
		t.Fatalf("events=%#v", events)
	}
}

type durableArtifactPublisher struct {
	ref   string
	calls int
}

func (p *durableArtifactPublisher) Publish(context.Context, string, string) (string, error) {
	p.calls++
	return p.ref, nil
}

func TestDurableArtifactRegistryCompositionAllowsOrphanButNeverMetadataBeforePublish(t *testing.T) {
	ctx := context.Background()
	r, err := OpenSQLiteRuntimeStateRepository(filepath.Join(t.TempDir(), "runtime.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err = r.db.Exec("CREATE TRIGGER reject_artifact_event BEFORE INSERT ON runtime_events WHEN NEW.event_type='ArtifactRegistered' BEGIN SELECT RAISE(ABORT, 'outbox unavailable'); END"); err != nil {
		t.Fatal(err)
	}
	publisher := &durableArtifactPublisher{ref: "s3://threadmill/artifacts/sha256/known"}
	registry, err := NewDurableArtifactRegistry(r, publisher)
	if err != nil {
		t.Fatal(err)
	}
	workspace := t.TempDir()
	if err = os.Mkdir(filepath.Join(workspace, "out"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(workspace, "out", "report.md"), []byte("report"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = registry.Register(ctx, artifacts.RegisterRequest{Owner: artifacts.TrustedOwner{TaskID: "task", InvocationID: "invocation", Generation: 2, WorkspaceRoot: workspace, AllowedDirs: []string{"out"}}, ControlledPath: "out/report.md", Kind: artifacts.ArtifactTypeGeneratedReport})
	if err == nil || publisher.calls != 1 {
		t.Fatalf("err=%v publisher calls=%d", err, publisher.calls)
	}
	events, err := r.ListRuntimeEvents(ctx)
	if err != nil || len(events) != 0 {
		t.Fatalf("events=%#v err=%v", events, err)
	}
}
