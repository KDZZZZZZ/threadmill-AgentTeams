package agentteams

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path"
	"testing"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/objectstore"
)

func TestSharedObjectFileTransportAgainstRealMinIO(t *testing.T) {
	endpoint := os.Getenv("THREADMILL_AGENTTEAMS_MINIO_ENDPOINT")
	accessKey := os.Getenv("THREADMILL_AGENTTEAMS_MINIO_ACCESS_KEY")
	secretKey := os.Getenv("THREADMILL_AGENTTEAMS_MINIO_SECRET_KEY")
	bucket := os.Getenv("THREADMILL_AGENTTEAMS_MINIO_BUCKET")
	if endpoint == "" || accessKey == "" || secretKey == "" || bucket == "" {
		t.Skip("real AgentTeams MinIO credentials are required")
	}
	store, err := objectstore.NewMinIOStore(objectstore.MinIOConfig{
		Endpoint: endpoint, AccessKey: accessKey, SecretKey: secretKey, Bucket: bucket, Secure: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	prefix := "teams/threadmill-runtime"
	taskID := "threadmill-minio-integration"
	projector := &recordingExecutionProjector{owned: true, exported: executionProjectionFile("retry/policy.go", "package retry\n")}
	transport, err := NewSharedObjectFileTransport(store, bucket, prefix, projector)
	if err != nil {
		t.Fatal(err)
	}
	execution := AgentTeamsExecutionRef{InvocationID: "inv-minio-integration", AgentTeamsTaskID: taskID, HostRef: "worker-a"}
	defer func() {
		_ = store.Delete(context.Background(), objectstore.ObjectRef{Bucket: bucket, Key: path.Join(prefix, "shared", "tasks", taskID, "workspace", "retry", "policy.go")})
		_ = store.Delete(context.Background(), objectstore.ObjectRef{Bucket: bucket, Key: path.Join(prefix, "shared", "tasks", taskID, "threadmill", "workspace.json")})
	}()
	if err := transport.PrepareExecution(context.Background(), execution, PreparedInvocation{InvocationID: execution.InvocationID}); err != nil {
		t.Fatalf("PrepareExecution against real AgentTeams MinIO: %v", err)
	}
	manifest, err := store.Get(context.Background(), objectstore.ObjectRef{Bucket: bucket, Key: path.Join(prefix, "shared", "tasks", taskID, "threadmill", "workspace.json")})
	if err != nil {
		t.Fatal(err)
	}
	defer manifest.Body.Close()
	raw, err := io.ReadAll(manifest.Body)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parseExecutionWorkspaceManifest(raw); err != nil {
		t.Fatalf("parse real MinIO workspace manifest: %v", err)
	}
}

func TestSharedObjectFileTransportProjectsAndImportsFullExecutionIdentity(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	projector := &recordingExecutionProjector{owned: true, exported: executionProjectionFile("workspace/app.go", "before")}
	transport, err := NewSharedObjectFileTransport(store, "agentteams-storage", "teams/threadmill-runtime", projector)
	if err != nil {
		t.Fatal(err)
	}
	execution := AgentTeamsExecutionRef{InvocationID: "inv-a", AgentTeamsTaskID: "threadmill-task-a", HostRef: "worker-a"}
	prepared := PreparedInvocation{InvocationID: execution.InvocationID}
	if err := transport.PrepareExecution(ctx, execution, prepared); err != nil {
		t.Fatal(err)
	}
	if projector.exportExecution != execution {
		t.Fatalf("export execution = %#v, want %#v", projector.exportExecution, execution)
	}
	checkpoint, err := transport.PullExecution(ctx, execution)
	if err != nil {
		t.Fatal(err)
	}
	if projector.importExecution != execution || checkpoint.WorkspaceRevision != "workspace-head" {
		t.Fatalf("import execution/checkpoint = %#v/%#v", projector.importExecution, checkpoint)
	}
	if len(projector.imported.Files) != 1 || string(projector.imported.Files[0].Content) != "before" {
		t.Fatalf("imported projection = %#v", projector.imported)
	}
}

func TestSharedObjectFileTransportSkipsNonPhaseProjection(t *testing.T) {
	store := objectstore.NewMemoryStore()
	projector := &recordingExecutionProjector{owned: false}
	transport, err := NewSharedObjectFileTransport(store, "agentteams-storage", "teams/threadmill-runtime", projector)
	if err != nil {
		t.Fatal(err)
	}
	execution := AgentTeamsExecutionRef{InvocationID: "inv-manager", AgentTeamsTaskID: "threadmill-task-manager", HostRef: "manager"}
	if err := transport.PrepareExecution(context.Background(), execution, PreparedInvocation{InvocationID: execution.InvocationID}); err != nil {
		t.Fatal(err)
	}
	if _, err := transport.PullExecution(context.Background(), execution); err != nil {
		t.Fatal(err)
	}
	if projector.exports != 0 || projector.imports != 0 {
		t.Fatalf("non-phase projection calls export/import = %d/%d", projector.exports, projector.imports)
	}
}

type recordingExecutionProjector struct {
	owned           bool
	exported        ExecutionFileProjection
	exportExecution AgentTeamsExecutionRef
	importExecution AgentTeamsExecutionRef
	imported        ExecutionFileProjection
	exports         int
	imports         int
}

func (p *recordingExecutionProjector) OwnsExecution(context.Context, AgentTeamsExecutionRef) (bool, error) {
	return p.owned, nil
}

func (p *recordingExecutionProjector) ExportExecutionFiles(_ context.Context, execution AgentTeamsExecutionRef) (ExecutionFileProjection, error) {
	p.exports++
	p.exportExecution = execution
	return p.exported, nil
}

func (p *recordingExecutionProjector) ImportExecutionFiles(_ context.Context, execution AgentTeamsExecutionRef, projection ExecutionFileProjection) (ExecutionWorkspaceCheckpoint, error) {
	p.imports++
	p.importExecution = execution
	p.imported = projection
	return ExecutionWorkspaceCheckpoint{WorkspaceRevision: "workspace-head"}, nil
}

func executionProjectionFile(path, body string) ExecutionFileProjection {
	content := []byte(body)
	sum := sha256.Sum256(content)
	return ExecutionFileProjection{
		Manifest: []byte(`{"version":"test"}`),
		Files:    []ExecutionFile{{Path: path, Mode: 0o644, Content: content, SHA256: hex.EncodeToString(sum[:])}},
	}
}
