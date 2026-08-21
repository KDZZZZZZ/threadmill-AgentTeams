//go:build integration

package agentteams

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/artifacts"
	phasemcp "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/mcp/phase"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/phaseagent"
)

// TestDurableArtifactAuthorityMinIOVertical crosses the actual local MinIO
// fixture: a token-bound MCP registration publishes/HEAD-verifies bytes,
// commits durable metadata/outbox, then a fresh repository instance validates
// the returned ArtifactRef for a later physical epoch of the same invocation.
func TestDurableArtifactAuthorityMinIOVertical(t *testing.T) {
	endpoint := os.Getenv("THREADMILL_IT_MINIO_ENDPOINT")
	accessKey := os.Getenv("THREADMILL_IT_MINIO_ACCESS_KEY")
	secretKey := os.Getenv("THREADMILL_IT_MINIO_SECRET_KEY")
	if endpoint == "" || accessKey == "" || secretKey == "" {
		t.Skip("requires the existing local MinIO integration fixture")
	}
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "runtime.db")
	workspace := t.TempDir()
	if err := os.Mkdir(filepath.Join(workspace, "out"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "out", "report.md"), []byte("real MinIO durable MCP artifact"), 0o600); err != nil {
		t.Fatal(err)
	}
	publisher, err := artifacts.NewS3BlobPublisher(artifacts.S3BlobPublisherConfig{
		Endpoint: endpoint, Bucket: "threadmill-it", Prefix: "threadmill/c3-2b",
		AccessKey: accessKey, SecretKey: secretKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	repository, err := runtime.OpenSQLiteRuntimeStateRepository(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	initialRepository := repository
	defer initialRepository.Close()
	authority, err := NewDurableArtifactAuthority(repository, publisher)
	if err != nil {
		t.Fatal(err)
	}
	bindings := phasemcp.NewBindingRegistry()
	initial := durableArtifactBinding(t, bindings, &durableArtifactTestRuntime{}, "task", "invocation", 3, 1, workspace)
	handler, err := authority.NewPhaseHandler(bindings, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := handler.RegisterArtifact(ctx, initial.Token, "out/report.md", artifacts.ArtifactTypeGeneratedReport, "text/markdown")
	if err != nil {
		t.Fatal(err)
	}
	if err = repository.Close(); err != nil {
		t.Fatal(err)
	}
	repository, err = runtime.OpenSQLiteRuntimeStateRepository(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	authority, err = NewDurableArtifactAuthority(repository, publisher)
	if err != nil {
		t.Fatal(err)
	}
	continuedRuntime := &durableArtifactTestRuntime{}
	continuedBindings := phasemcp.NewBindingRegistry()
	continued := durableArtifactBinding(t, continuedBindings, continuedRuntime, "task", "invocation", 3, 2, workspace)
	continuedHandler, err := authority.NewPhaseHandler(continuedBindings, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = continuedHandler.SubmitPhaseOutput(ctx, continued.Token, phaseagent.PhaseOutput{ReportRef: string(ref)}); err != nil {
		t.Fatal(err)
	}
	if len(continuedRuntime.outputs) != 1 {
		t.Fatalf("outputs=%#v", continuedRuntime.outputs)
	}
	events, err := repository.ListRuntimeEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	registered := 0
	for _, event := range events {
		if event.EventType == artifacts.EventArtifactRegistered {
			registered++
		}
	}
	if registered != 1 {
		t.Fatalf("ArtifactRegistered events=%d", registered)
	}
}
