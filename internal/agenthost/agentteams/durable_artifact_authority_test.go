package agentteams

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/artifacts"
	phasemcp "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/mcp/phase"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/phaseagent"
)

type durableArtifactTestPublisher struct {
	prefix string
	calls  int
	err    error
}

func (p *durableArtifactTestPublisher) Publish(_ context.Context, _ string, hash string) (string, error) {
	p.calls++
	return p.prefix + hash, p.err
}

type durableArtifactTestRuntime struct{ outputs []phaseagent.PhaseOutput }

func (*durableArtifactTestRuntime) AwaitInputs(context.Context, phaseagent.AwaitInputsRequest) (phaseagent.InputWaitResult, error) {
	return phaseagent.InputWaitResult{}, nil
}
func (r *durableArtifactTestRuntime) SubmitPhaseOutput(_ context.Context, output phaseagent.PhaseOutput) error {
	r.outputs = append(r.outputs, output)
	return nil
}
func (*durableArtifactTestRuntime) ProposeOrchestration(context.Context, phaseagent.OrchestrationProposal) error {
	return nil
}
func (*durableArtifactTestRuntime) SubmitRequirement(context.Context, phaseagent.Requirement) error {
	return nil
}
func (*durableArtifactTestRuntime) ListTaskMemoryCandidates(context.Context) (phaseagent.TaskMemoryBufferView, error) {
	return phaseagent.TaskMemoryBufferView{}, nil
}
func (*durableArtifactTestRuntime) SubmitMemoryCandidate(context.Context, phaseagent.MemoryCandidate) (phaseagent.CandidateBufferedReceipt, error) {
	return phaseagent.CandidateBufferedReceipt{}, nil
}

type durableArtifactTestReader struct{}

func (durableArtifactTestReader) ListSubgraphs(context.Context, phaseagent.ListSubgraphsRequest) ([]phaseagent.ContextSubgraph, error) {
	return nil, nil
}
func (durableArtifactTestReader) Explore(context.Context, phaseagent.ExploreRequest) (phaseagent.ContextSliceDelta, error) {
	return phaseagent.ContextSliceDelta{}, nil
}
func (durableArtifactTestReader) Subscribe(context.Context, phaseagent.SubscribeRequest) (phaseagent.ContextSubscription, error) {
	return phaseagent.ContextSubscription{}, nil
}
func (durableArtifactTestReader) Unsubscribe(context.Context, string) error { return nil }

type durableArtifactTestAgent struct{}

func (durableArtifactTestAgent) Retrieve(context.Context, phaseagent.ContextRetrieveRequest) (phaseagent.ContextRetrieveResult, error) {
	return phaseagent.ContextRetrieveResult{}, nil
}

type durableArtifactObserver struct{ events []artifacts.Event }

func (o *durableArtifactObserver) Record(_ context.Context, event artifacts.Event) error {
	o.events = append(o.events, event)
	return nil
}

func durableArtifactBinding(t *testing.T, registry *phasemcp.BindingRegistry, runtimeValue phaseagent.Runtime, taskID, invocationID string, generation int, epoch int64, workspace string) phasemcp.ExecutionBinding {
	t.Helper()
	role, err := phaseagent.RoleForEndpoint(phaseagent.PhaseEndpointRef{TaskID: taskID, EndpointID: string(phaseagent.PhaseExecute)})
	if err != nil {
		t.Fatal(err)
	}
	binding, err := registry.Issue(phasemcp.BoundServices{Binding: phasemcp.InvocationBinding{
		TaskID: taskID, InvocationID: invocationID, Generation: generation, ExecutionEpoch: epoch,
		Endpoint: phaseagent.PhaseEndpointRef{TaskID: taskID, EndpointID: string(phaseagent.PhaseExecute)}, Role: role.Phase,
		BindingRef: "binding-" + invocationID, InputRevision: "r5", WorkspaceRoot: workspace, AllowedDirs: []string{"out"}, Capabilities: role.Capabilities,
	}, Runtime: runtimeValue, Reader: durableArtifactTestReader{}, Agent: durableArtifactTestAgent{}})
	if err != nil {
		t.Fatal(err)
	}
	return binding
}

func TestDurableArtifactAuthorityUsesOneRegistryForMCPEvidenceAndColdReopen(t *testing.T) {
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "runtime.db")
	repository, err := runtime.OpenSQLiteRuntimeStateRepository(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	workspace := t.TempDir()
	if err = os.Mkdir(filepath.Join(workspace, "out"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(workspace, "out", "report.md"), []byte("formal report"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(workspace, "out", "result.md"), []byte("execution evidence"), 0o600); err != nil {
		t.Fatal(err)
	}
	publisher := &durableArtifactTestPublisher{prefix: "s3://threadmill/artifacts/sha256/"}
	authority, err := NewDurableArtifactAuthority(repository, publisher)
	if err != nil {
		t.Fatal(err)
	}
	bindings := phasemcp.NewBindingRegistry()
	initialRuntime := &durableArtifactTestRuntime{}
	initial := durableArtifactBinding(t, bindings, initialRuntime, "task", "invocation", 3, 1, workspace)
	observer := &durableArtifactObserver{}
	handler, err := authority.NewPhaseHandler(bindings, observer, nil)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := handler.RegisterArtifact(ctx, initial.Token, "out/report.md", artifacts.ArtifactTypeGeneratedReport, "text/markdown")
	if err != nil {
		t.Fatal(err)
	}
	if publisher.calls != 1 {
		t.Fatalf("publisher calls=%d", publisher.calls)
	}
	for _, event := range observer.events {
		if event.Type == artifacts.EventArtifactRegistered {
			t.Fatal("legacy observer recorded authoritative ArtifactRegistered")
		}
	}
	metadata, found, err := repository.ArtifactStore().GetArtifact(ctx, ref)
	if err != nil || !found || metadata.OriginTaskID != "task" || metadata.OriginInvocationID != "invocation" || !strings.HasPrefix(metadata.BlobRef, publisher.prefix) || filepath.IsAbs(metadata.BlobRef) {
		t.Fatalf("metadata=%#v found=%t err=%v", metadata, found, err)
	}
	// Evidence ingestion receives the same durable registrar and derives
	// identity from the Runtime-built HostExecutionRequest, not taskflow data.
	ingestor, err := authority.EvidenceIngestor(observer)
	if err != nil {
		t.Fatal(err)
	}
	request := HostExecutionRequest{InvocationID: "invocation", Endpoint: phaseagent.PhaseEndpointRef{TaskID: "task", EndpointID: string(phaseagent.PhaseExecute)}, Generation: 3, Envelope: HostEnvelope{Workspace: WorkspaceMount{Root: workspace, AllowedDirs: []string{"out"}}}}
	evidence, err := ingestor.IngestExecutionEvidence(ctx, request, TeamHarnessTaskSnapshot{ResultPath: "out/result.md"})
	if err != nil || evidence.ResultRef == "" {
		t.Fatalf("evidence=%#v err=%v", evidence, err)
	}
	if err = repository.Close(); err != nil {
		t.Fatal(err)
	}

	// A new process/repository instance and a new physical epoch retain only
	// logical TaskID+InvocationID artifact access.
	reopened, err := runtime.OpenSQLiteRuntimeStateRepository(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reopenedAuthority, err := NewDurableArtifactAuthority(reopened, publisher)
	if err != nil {
		t.Fatal(err)
	}
	newBindings := phasemcp.NewBindingRegistry()
	continuedRuntime := &durableArtifactTestRuntime{}
	continued := durableArtifactBinding(t, newBindings, continuedRuntime, "task", "invocation", 3, 2, workspace)
	continuedHandler, err := reopenedAuthority.NewPhaseHandler(newBindings, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = continuedHandler.SubmitPhaseOutput(ctx, continued.Token, phaseagent.PhaseOutput{ReportRef: string(ref)}); err != nil {
		t.Fatalf("reopened same-invocation output rejected: %v", err)
	}
	if len(continuedRuntime.outputs) != 1 {
		t.Fatalf("outputs=%#v", continuedRuntime.outputs)
	}
	other := durableArtifactBinding(t, newBindings, &durableArtifactTestRuntime{}, "task", "other-invocation", 3, 2, workspace)
	if err = continuedHandler.SubmitPhaseOutput(ctx, other.Token, phaseagent.PhaseOutput{ReportRef: string(ref)}); err == nil {
		t.Fatal("cross invocation artifact access accepted")
	}
	events, err := reopened.ListRuntimeEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	registered := 0
	for _, event := range events {
		if event.EventType == artifacts.EventArtifactRegistered {
			registered++
		}
	}
	if registered != 2 {
		t.Fatalf("ArtifactRegistered outbox events=%d", registered)
	}
}

func TestDurableArtifactAuthorityFencesPathEscapeAndPublisherFailure(t *testing.T) {
	ctx := context.Background()
	repository, err := runtime.OpenSQLiteRuntimeStateRepository(filepath.Join(t.TempDir(), "runtime.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	workspace := t.TempDir()
	if err = os.Mkdir(filepath.Join(workspace, "out"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(workspace, "out", "report.md"), []byte("report"), 0o600); err != nil {
		t.Fatal(err)
	}
	publisher := &durableArtifactTestPublisher{prefix: "s3://bucket/object/", err: errors.New("publish failed")}
	authority, err := NewDurableArtifactAuthority(repository, publisher)
	if err != nil {
		t.Fatal(err)
	}
	bindings := phasemcp.NewBindingRegistry()
	binding := durableArtifactBinding(t, bindings, &durableArtifactTestRuntime{}, "task", "invocation", 4, 1, workspace)
	handler, err := authority.NewPhaseHandler(bindings, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = handler.RegisterArtifact(ctx, binding.Token, "../outside", artifacts.ArtifactTypeGeneratedReport, ""); err == nil || publisher.calls != 0 {
		t.Fatalf("path escape err=%v publisher=%d", err, publisher.calls)
	}
	outside := filepath.Join(t.TempDir(), "outside.md")
	if err = os.WriteFile(outside, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(workspace, "out", "outside-link.md")
	if err = os.Symlink(outside, link); err == nil {
		if _, err = handler.RegisterArtifact(ctx, binding.Token, "out/outside-link.md", artifacts.ArtifactTypeGeneratedReport, ""); err == nil || publisher.calls != 0 {
			t.Fatalf("symlink escape err=%v publisher=%d", err, publisher.calls)
		}
	} else {
		t.Logf("symlink escape assertion skipped: %v", err)
	}
	if _, err = handler.RegisterArtifact(ctx, binding.Token, "out/report.md", artifacts.ArtifactTypeGeneratedReport, ""); err == nil || publisher.calls != 1 {
		t.Fatalf("publish error=%v publisher=%d", err, publisher.calls)
	}
	events, err := repository.ListRuntimeEvents(ctx)
	if err != nil || len(events) != 0 {
		t.Fatalf("events=%#v err=%v", events, err)
	}
}
