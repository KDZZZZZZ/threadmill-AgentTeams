package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/artifacts"
	phasemcp "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/mcp/phase"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/phaseagent"
)

func testWaitingRecord() WaitingRecord {
	return WaitingRecord{
		Key:                 WaitingKey{TaskID: "task-a", InvocationID: "invocation-a", Generation: 3},
		ExecutionEpoch:      1,
		Endpoint:            phaseagent.PhaseEndpointRef{TaskID: "task-a", EndpointID: "execute"},
		PreviousBindingRef:  "binding-r4",
		InputRevision:       "input-r4",
		PendingInputIDs:     []string{"review"},
		ContinuationRef:     "continuation-opaque-1",
		State:               AwaitStatePreparingAwait,
		WorkspaceRef:        "workspace-r7",
		ContextSliceRef:     "context-r9",
		TaskMemoryBufferRef: "task-memory-r2",
		EventRefs:           []string{"event-a"},
		EvidenceRefs:        []string{"evidence-a"},
	}
}

func TestInMemoryWaitingStoreCASAndTerminalDelete(t *testing.T) {
	t.Parallel()
	store := NewInMemoryWaitingStore()
	initial := testWaitingRecord()
	initial.State = AwaitStateRunning
	record, err := store.Create(context.Background(), initial)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(context.Background(), initial); !errors.Is(err, ErrWaitingRecordExists) {
		t.Fatalf("duplicate create error = %v", err)
	}
	next := record
	next.State = AwaitStatePreparingAwait
	next, swapped, err := store.CompareAndSwap(context.Background(), record.Key, record.Revision, next)
	if err != nil || !swapped || next.Revision != 2 {
		t.Fatalf("prepare transition = %#v swapped=%t err=%v", next, swapped, err)
	}
	stale := next
	stale.State = AwaitStateRelinquishing
	if _, swapped, err := store.CompareAndSwap(context.Background(), record.Key, record.Revision, stale); err != nil || swapped {
		t.Fatalf("stale CAS swapped=%t err=%v", swapped, err)
	}
	relinquishing := next
	relinquishing.State = AwaitStateRelinquishing
	relinquishing, swapped, err = store.CompareAndSwap(context.Background(), record.Key, next.Revision, relinquishing)
	if err != nil || !swapped {
		t.Fatalf("relinquishing transition swapped=%t err=%v", swapped, err)
	}
	waiting := relinquishing
	waiting.State = AwaitStateWaiting
	waiting, swapped, err = store.CompareAndSwap(context.Background(), record.Key, relinquishing.Revision, waiting)
	if err != nil || !swapped {
		t.Fatalf("waiting transition swapped=%t err=%v", swapped, err)
	}
	rehydrating := waiting
	rehydrating.State = AwaitStateRehydrating
	rehydrating, swapped, err = store.CompareAndSwap(context.Background(), record.Key, waiting.Revision, rehydrating)
	if err != nil || !swapped {
		t.Fatalf("rehydrating transition swapped=%t err=%v", swapped, err)
	}
	running := rehydrating
	running.State = AwaitStateRunning
	running, swapped, err = store.CompareAndSwap(context.Background(), record.Key, rehydrating.Revision, running)
	if err != nil || !swapped {
		t.Fatalf("running transition swapped=%t err=%v", swapped, err)
	}
	terminal := running
	terminal.State = AwaitStateTerminal
	terminal, swapped, err = store.CompareAndSwap(context.Background(), record.Key, running.Revision, terminal)
	if err != nil || !swapped {
		t.Fatalf("terminal transition swapped=%t err=%v", swapped, err)
	}
	if deleted, err := store.Delete(context.Background(), record.Key, terminal.Revision-1); err != nil || deleted {
		t.Fatalf("stale delete deleted=%t err=%v", deleted, err)
	}
	if deleted, err := store.Delete(context.Background(), record.Key, terminal.Revision); err != nil || !deleted {
		t.Fatalf("delete deleted=%t err=%v", deleted, err)
	}
	if _, found, err := store.Get(context.Background(), record.Key); err != nil || found {
		t.Fatalf("record after delete found=%t err=%v", found, err)
	}
}

func TestContinuationRebindKeepsLogicalInvocationAndGeneration(t *testing.T) {
	t.Parallel()
	binding := ContinuationBinding{InvocationID: "invocation-a", Generation: 3, ExecutionEpoch: 2, PreviousBindingRef: "binding-r4", PreviousRevision: "input-r4", BindingRef: "binding-r5", InputRevision: "input-r5"}
	if err := ValidateContinuationRebind(binding); err != nil {
		t.Fatal(err)
	}
	if binding.InvocationID != "invocation-a" || binding.Generation != 3 || binding.ExecutionEpoch != 2 {
		t.Fatalf("logical identity changed: %#v", binding)
	}
	if err := ValidateContinuationRebind(ContinuationBinding{InvocationID: "invocation-a", Generation: 3, ExecutionEpoch: 2, PreviousBindingRef: "binding-r4", PreviousRevision: "input-r4", BindingRef: "binding-r4", InputRevision: "input-r5"}); err == nil {
		t.Fatal("immutable binding ref reuse accepted")
	}
}

type callLog struct{ calls []string }

func (l *callLog) RecordAwaiting(_ context.Context, record WaitingRecord) error {
	l.calls = append(l.calls, "event:"+string(record.ContinuationRef))
	return nil
}
func (l *callLog) ReleaseWorkspaceLease(_ context.Context, _ WaitingRecord) error {
	l.calls = append(l.calls, "lease")
	return nil
}
func (l *callLog) CancelTeamHarnessTask(_ context.Context, id string, _ string) error {
	l.calls = append(l.calls, "task:"+id)
	return nil
}
func (l *callLog) CleanupExecutionMCP(_ context.Context, _ WaitingRecord) error {
	l.calls = append(l.calls, "mcp")
	return nil
}
func (l *callLog) RevokeWorkerCredential(_ context.Context, _ WaitingRecord) error {
	l.calls = append(l.calls, "credential")
	return nil
}
func (l *callLog) ReleaseExecutionCarrier(_ context.Context, _ WaitingRecord) error {
	l.calls = append(l.calls, "carrier")
	return nil
}

type registryRevoker struct {
	registry *phasemcp.BindingRegistry
	log      *callLog
}

func (r *registryRevoker) RevokeExecutionToken(_ context.Context, token string) error {
	r.log.calls = append(r.log.calls, "token")
	r.registry.Revoke(token)
	return nil
}

func TestRelinquishRevokesTokenAndRetainsLogicalReferences(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	workspace := t.TempDir()
	if err := os.Mkdir(filepath.Join(workspace, "out"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "out", "report.md"), []byte("report"), 0644); err != nil {
		t.Fatal(err)
	}
	owner := artifacts.TrustedOwner{TaskID: "task-a", InvocationID: "invocation-a", WorkspaceRoot: workspace, AllowedDirs: []string{"out"}}
	artifactRegistry := artifacts.NewInMemoryRegistry(nil)
	ref, err := artifactRegistry.Register(ctx, artifacts.RegisterRequest{Owner: owner, ControlledPath: "out/report.md", Kind: artifacts.ArtifactTypeGeneratedReport})
	if err != nil {
		t.Fatal(err)
	}

	bindingRegistry := phasemcp.NewBindingRegistry()
	token, err := bindingRegistry.Issue(phasemcp.BoundServices{Binding: phasemcp.InvocationBinding{TaskID: "task-a", InvocationID: "invocation-a", Endpoint: phaseagent.PhaseEndpointRef{TaskID: "task-a", EndpointID: "execute"}, Generation: 3, Role: phaseagent.PhaseExecute, BindingRef: "binding-r4"}, Runtime: noopRuntime{}, Reader: noopReader{}, Agent: noopAgent{}, Expires: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bindingRegistry.Resolve(token.Token); err != nil {
		t.Fatalf("token should resolve before relinquish: %v", err)
	}

	record := testWaitingRecord()
	record.ArtifactRefs = []artifacts.ArtifactRef{ref}
	log := &callLog{}
	revoker := &registryRevoker{registry: bindingRegistry, log: log}
	relinquisher := ExecutionRelinquisher{Store: NewInMemoryWaitingStore(), Events: log, Leases: log, Tasks: log, Tokens: revoker, MCP: log, Credentials: log, Carriers: log}
	waiting, err := relinquisher.Relinquish(ctx, RelinquishRequest{Record: record, TeamHarnessID: "team-task-a", ExecutionToken: token.Token, Reason: "await inputs"})
	if err != nil {
		t.Fatal(err)
	}
	if waiting.State != AwaitStateWaiting || waiting.Key.InvocationID != record.Key.InvocationID || waiting.Key.Generation != record.Key.Generation || waiting.ExecutionEpoch != 1 {
		t.Fatalf("waiting record identity/state mismatch: %#v", waiting)
	}
	if _, err := bindingRegistry.Resolve(token.Token); !errors.Is(err, phasemcp.ErrInvalidToken) {
		t.Fatalf("old token after relinquish = %v", err)
	}
	if err := artifactRegistry.ValidateReferences(ctx, owner, waiting.ArtifactRefs); err != nil {
		t.Fatalf("await cleanup lost artifact reference: %v", err)
	}
	encoded, err := json.Marshal(waiting)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{token.Token, "credential-a", "X-Threadmill-Execution-Token"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("waiting record leaked %q: %s", forbidden, encoded)
		}
	}
	want := []string{"event:continuation-opaque-1", "lease", "task:team-task-a", "token", "mcp", "credential", "carrier"}
	if strings.Join(log.calls, ",") != strings.Join(want, ",") {
		t.Fatalf("relinquish order log=%v", log.calls)
	}
	if _, err := relinquisher.Relinquish(ctx, RelinquishRequest{Record: record, TeamHarnessID: "team-task-a", ExecutionToken: token.Token}); err != nil {
		t.Fatalf("completed relinquish should be idempotent: %v", err)
	}
}

type noopRuntime struct{}

func (noopRuntime) AwaitInputs(context.Context, phaseagent.AwaitInputsRequest) (phaseagent.InputWaitResult, error) {
	return phaseagent.InputWaitResult{}, nil
}
func (noopRuntime) SubmitPhaseOutput(context.Context, phaseagent.PhaseOutput) error { return nil }
func (noopRuntime) ProposeOrchestration(context.Context, phaseagent.OrchestrationProposal) error {
	return nil
}
func (noopRuntime) SubmitRequirement(context.Context, phaseagent.Requirement) error { return nil }
func (noopRuntime) ListTaskMemoryCandidates(context.Context) (phaseagent.TaskMemoryBufferView, error) {
	return phaseagent.TaskMemoryBufferView{}, nil
}
func (noopRuntime) SubmitMemoryCandidate(context.Context, phaseagent.MemoryCandidate) (phaseagent.CandidateBufferedReceipt, error) {
	return phaseagent.CandidateBufferedReceipt{}, nil
}

type noopReader struct{}

func (noopReader) ListSubgraphs(context.Context, phaseagent.ListSubgraphsRequest) ([]phaseagent.ContextSubgraph, error) {
	return nil, nil
}
func (noopReader) Explore(context.Context, phaseagent.ExploreRequest) (phaseagent.ContextSliceDelta, error) {
	return phaseagent.ContextSliceDelta{}, nil
}
func (noopReader) Subscribe(context.Context, phaseagent.SubscribeRequest) (phaseagent.ContextSubscription, error) {
	return phaseagent.ContextSubscription{}, nil
}
func (noopReader) Unsubscribe(context.Context, string) error { return nil }

type noopAgent struct{}

func (noopAgent) Retrieve(context.Context, phaseagent.ContextRetrieveRequest) (phaseagent.ContextRetrieveResult, error) {
	return phaseagent.ContextRetrieveResult{}, nil
}
