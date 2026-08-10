package coordination

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

func TestTaskManagerGraphReplacePendingCreatesFixedTaskAndHistory(t *testing.T) {
	graph, decisions, _ := newGraphHarness()
	ctx := context.Background()

	base := latestRevision(t, graph)
	requestID := kernel.IdempotencyKey("decision-create-task")
	if err := decisions.RegisterReplacePending(projectID, requestID); err != nil {
		t.Fatal(err)
	}

	revision, err := graph.ReplacePending(ctx, basicSubgraph("task-a", requestID, base))
	if err != nil {
		t.Fatalf("ReplacePending failed: %v", err)
	}
	if revision != base.Next() {
		t.Fatalf("revision = %d, want %d", revision, base.Next())
	}

	snapshot, err := graph.Snapshot(ctx, revision)
	if err != nil {
		t.Fatalf("snapshot failed: %v", err)
	}
	if len(snapshot.Tasks) != 1 {
		t.Fatalf("tasks = %d, want 1", len(snapshot.Tasks))
	}
	if got := endpointIDs(snapshot.Endpoints); got != "plan,execute,verify" {
		t.Fatalf("endpoint set = %s", got)
	}

	historical, err := graph.Snapshot(ctx, base)
	if err != nil {
		t.Fatalf("historical snapshot failed: %v", err)
	}
	if len(historical.Tasks) != 0 {
		t.Fatalf("historical revision was mutated: %#v", historical.Tasks)
	}
}

func TestTaskManagerGraphRequiresCanonicalTaskManagerCapability(t *testing.T) {
	store := NewMemoryStore()
	decisions := NewMemoryDecisionLog()
	principal := taskManagerPrincipal()
	principal.Role = auth.RoleExecutor
	principal.Tools = auth.ToolSet(auth.ToolCoordinationReplacePending)
	graph := NewTaskManagerGraph(principal, store, decisions, kernel.NewMemoryIdempotencyStore())

	_, err := graph.ReplacePending(context.Background(), basicSubgraph("task-a", "decision-a", 1))
	if !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("err = %v, want forbidden", err)
	}
}

func TestReplacePendingRequiresPersistedDecisionRef(t *testing.T) {
	graph, _, _ := newGraphHarness()

	_, err := graph.ReplacePending(context.Background(), basicSubgraph("task-a", "missing-decision", latestRevision(t, graph)))
	if !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("err = %v, want forbidden", err)
	}
}

func TestReplacePendingRejectsCyclesAndMissingFixedEndpoints(t *testing.T) {
	graph, decisions, _ := newGraphHarness()
	ctx := context.Background()

	base := latestRevision(t, graph)
	requestID := kernel.IdempotencyKey("decision-missing-verify")
	if err := decisions.RegisterReplacePending(projectID, requestID); err != nil {
		t.Fatal(err)
	}
	missing := basicSubgraph("task-a", requestID, base)
	missing.Endpoints = missing.Endpoints[:2]
	_, err := graph.ReplacePending(ctx, missing)
	if !kernel.IsCode(err, kernel.CodeInvalidRequest) {
		t.Fatalf("missing endpoint err = %v, want invalid_request", err)
	}

	base = latestRevision(t, graph)
	requestID = "decision-cycle"
	if err := decisions.RegisterReplacePending(projectID, requestID); err != nil {
		t.Fatal(err)
	}
	cyclic := basicSubgraph("task-a", requestID, base)
	cyclic.Tasks = append(cyclic.Tasks, Task{ID: "task-b", ContractRef: "contract://task-b", Outcome: TaskActive})
	cyclic.Endpoints = append(cyclic.Endpoints, endpointsFor("task-b")...)
	cyclic.Edges = []Edge{
		edge(ref("task-a", EndpointVerify), ref("task-b", EndpointPlan)),
		edge(ref("task-b", EndpointVerify), ref("task-a", EndpointPlan)),
	}
	_, err = graph.ReplacePending(ctx, cyclic)
	if !kernel.IsCode(err, kernel.CodeInvalidGraph) {
		t.Fatalf("cycle err = %v, want invalid_graph", err)
	}
}

func TestReplacePendingRejectsBindingMutationAndIdempotencyConflict(t *testing.T) {
	graph, decisions, _ := newGraphHarness()
	ctx := context.Background()

	base := latestRevision(t, graph)
	requestID := kernel.IdempotencyKey("decision-create")
	if err := decisions.RegisterReplacePending(projectID, requestID); err != nil {
		t.Fatal(err)
	}
	revision, err := graph.ReplacePending(ctx, basicSubgraph("task-a", requestID, base))
	if err != nil {
		t.Fatal(err)
	}

	replaceID := kernel.IdempotencyKey("decision-replace")
	if err := decisions.RegisterReplacePending(projectID, replaceID); err != nil {
		t.Fatal(err)
	}
	next := basicSubgraph("task-a", replaceID, revision)
	next.Tasks = nil
	next.Endpoints = []PhaseEndpoint{next.Endpoints[0]}
	next.Endpoints[0].BindingRef = "binding://task-a/plan/changed"
	_, err = graph.ReplacePending(ctx, next)
	if !kernel.IsCode(err, kernel.CodeStaleBinding) {
		t.Fatalf("binding mutation err = %v, want stale_binding", err)
	}

	conflict := basicSubgraph("task-a", requestID, base)
	conflict.Tasks[0].ContractRef = "contract://different"
	_, err = graph.ReplacePending(ctx, conflict)
	if !kernel.IsCode(err, kernel.CodeIdempotencyConflict) {
		t.Fatalf("idempotency err = %v, want idempotency_conflict", err)
	}
}

func TestReplacePendingSameRequestIDIsConcurrentIdempotent(t *testing.T) {
	graph, decisions, _ := newGraphHarness()
	ctx := context.Background()

	requestID := kernel.IdempotencyKey("decision-concurrent")
	if err := decisions.RegisterReplacePending(projectID, requestID); err != nil {
		t.Fatal(err)
	}
	subgraph := basicSubgraph("task-a", requestID, latestRevision(t, graph))
	const workers = 100
	var calls int32
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	revisions := make(chan kernel.Revision, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			atomic.AddInt32(&calls, 1)
			revision, err := graph.ReplacePending(ctx, subgraph)
			if err != nil {
				errs <- err
				return
			}
			revisions <- revision
		}()
	}
	wg.Wait()
	close(errs)
	close(revisions)

	for err := range errs {
		t.Fatalf("concurrent ReplacePending failed: %v", err)
	}
	var first kernel.Revision
	for revision := range revisions {
		if first == 0 {
			first = revision
		}
		if revision != first {
			t.Fatalf("revision = %d, want all %d", revision, first)
		}
	}
	snapshot, err := graph.Snapshot(ctx, kernel.LatestRevision)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Revision != first || len(snapshot.Tasks) != 1 {
		t.Fatalf("snapshot revision/tasks = %d/%d, want %d/1", snapshot.Revision, len(snapshot.Tasks), first)
	}
}

func TestTransitionUsesPersistedDecisionAndClosedStateSet(t *testing.T) {
	graph, decisions, _ := newGraphHarness()
	ctx := context.Background()
	revision := createTask(t, graph, decisions, "task-a")

	if err := decisions.RegisterTransition(projectID, "bad", GraphTransition{
		TargetKind: TargetPhaseEndpoint,
		Endpoint:   ref("task-a", EndpointPlan),
		Action:     "patch_state",
		Generation: 1,
	}); !kernel.IsCode(err, kernel.CodeInvalidRequest) {
		t.Fatalf("invalid transition err = %v, want invalid_request", err)
	}

	if _, err := graph.Transition(ctx, revision, "missing"); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("missing transition err = %v, want forbidden", err)
	}
	if err := decisions.RegisterTransition(projectID, "missing-generation", GraphTransition{
		TargetKind: TargetPhaseEndpoint,
		Endpoint:   ref("task-a", EndpointPlan),
		Action:     "held",
	}); !kernel.IsCode(err, kernel.CodeTransitionRejected) {
		t.Fatalf("missing generation err = %v, want transition_rejected", err)
	}

	registerTransition(t, decisions, "submit-plan", GraphTransition{
		TargetKind: TargetPhaseEndpoint,
		Endpoint:   ref("task-a", EndpointPlan),
		Action:     string(EndpointSubmitted),
	})
	revision, err := graph.Transition(ctx, revision, "submit-plan")
	if err != nil {
		t.Fatalf("submit transition failed: %v", err)
	}

	registerTransition(t, decisions, "satisfy-plan", GraphTransition{
		TargetKind: TargetPhaseEndpoint,
		Endpoint:   ref("task-a", EndpointPlan),
		Action:     string(EndpointSatisfied),
		Result: PhaseResult{
			Endpoint:   ref("task-a", EndpointPlan),
			BindingRef: "binding://task-a/plan/1",
			OutputRef:  "artifact://phase-output/plan",
		},
	})
	revision, err = graph.Transition(ctx, revision, "satisfy-plan")
	if err != nil {
		t.Fatalf("satisfy transition failed: %v", err)
	}

	snapshot, err := graph.Snapshot(ctx, revision)
	if err != nil {
		t.Fatal(err)
	}
	if endpointState(snapshot, ref("task-a", EndpointPlan)) != EndpointSatisfied {
		t.Fatalf("plan state = %s, want satisfied", endpointState(snapshot, ref("task-a", EndpointPlan)))
	}
	if len(snapshot.Results) != 1 || snapshot.Results[0].Verdict != VerdictSatisfied {
		t.Fatalf("results = %#v, want satisfied result", snapshot.Results)
	}
}

func TestTransitionRejectsRevisionConflictAndTaskDoneBeforeVerify(t *testing.T) {
	graph, decisions, _ := newGraphHarness()
	ctx := context.Background()
	revision := createTask(t, graph, decisions, "task-a")

	registerTransition(t, decisions, "done-too-early", GraphTransition{
		TargetKind: TargetTask,
		TaskID:     "task-a",
		Action:     string(TaskDone),
	})
	_, err := graph.Transition(ctx, revision, "done-too-early")
	if !kernel.IsCode(err, kernel.CodeTransitionRejected) {
		t.Fatalf("done-too-early err = %v, want transition_rejected", err)
	}

	registerTransition(t, decisions, "hold-plan", GraphTransition{
		TargetKind: TargetPhaseEndpoint,
		Endpoint:   ref("task-a", EndpointPlan),
		Action:     "held",
	})
	revision, err = graph.Transition(ctx, revision, "hold-plan")
	if err != nil {
		t.Fatal(err)
	}
	registerTransition(t, decisions, "release-plan", GraphTransition{
		TargetKind: TargetPhaseEndpoint,
		Endpoint:   ref("task-a", EndpointPlan),
		Action:     "released",
	})
	_, err = graph.Transition(ctx, revision-1, "release-plan")
	if !kernel.IsCode(err, kernel.CodeRevisionConflict) {
		t.Fatalf("revision conflict err = %v, want revision_conflict", err)
	}
}

func TestStoppedTransitionRequiresEvidenceAndRollsGenerationWithoutPersistentStoppedState(t *testing.T) {
	graph, decisions, _ := newGraphHarness()
	ctx := context.Background()
	revision := createTask(t, graph, decisions, "task-a")

	registerTransition(t, decisions, "submit-plan", GraphTransition{
		TargetKind: TargetPhaseEndpoint,
		Endpoint:   ref("task-a", EndpointPlan),
		Action:     string(EndpointSubmitted),
		Generation: 1,
	})
	revision = mustTransition(t, graph, revision, "submit-plan")

	registerTransition(t, decisions, "hold-plan", GraphTransition{
		TargetKind: TargetPhaseEndpoint,
		Endpoint:   ref("task-a", EndpointPlan),
		Action:     "held",
		Generation: 1,
	})
	revision = mustTransition(t, graph, revision, "hold-plan")

	registerTransition(t, decisions, "held-submit", GraphTransition{
		TargetKind: TargetPhaseEndpoint,
		Endpoint:   ref("task-a", EndpointPlan),
		Action:     string(EndpointSubmitted),
		Generation: 1,
	})
	_, err := graph.Transition(ctx, revision, "held-submit")
	if !kernel.IsCode(err, kernel.CodeTransitionRejected) {
		t.Fatalf("held submit err = %v, want transition_rejected", err)
	}

	registerTransition(t, decisions, "bad-stop", GraphTransition{
		TargetKind:    TargetPhaseEndpoint,
		Endpoint:      ref("task-a", EndpointPlan),
		Action:        "stopped",
		Generation:    1,
		NewBindingRef: "binding://task-a/plan/2",
	})
	_, err = graph.Transition(ctx, revision, "bad-stop")
	if !kernel.IsCode(err, kernel.CodeIncompleteStopEvidence) {
		t.Fatalf("bad stop err = %v, want incomplete_stop_evidence", err)
	}

	registerTransition(t, decisions, "stop-plan", GraphTransition{
		TargetKind:    TargetPhaseEndpoint,
		Endpoint:      ref("task-a", EndpointPlan),
		Action:        "stopped",
		Generation:    1,
		NewBindingRef: "binding://task-a/plan/2",
		CheckpointRef: "checkpoint://task-a/plan/1",
		EvidenceRefs:  []string{"event://stopped/1"},
	})
	revision = mustTransition(t, graph, revision, "stop-plan")

	snapshot := mustSnapshot(t, graph, revision)
	endpoint := mustEndpoint(t, snapshot, ref("task-a", EndpointPlan))
	if endpoint.State != EndpointPending || endpoint.RunPolicy != RunHeld || endpoint.Generation != 2 || endpoint.BindingRef != "binding://task-a/plan/2" {
		t.Fatalf("endpoint after stop = %#v, want pending held generation 2 with new binding", endpoint)
	}

	registerTransition(t, decisions, "old-generation-output", GraphTransition{
		TargetKind: TargetPhaseEndpoint,
		Endpoint:   ref("task-a", EndpointPlan),
		Action:     string(EndpointSatisfied),
		Generation: 1,
		Result: PhaseResult{
			Endpoint:   ref("task-a", EndpointPlan),
			BindingRef: "binding://task-a/plan/1",
			OutputRef:  "artifact://phase-output/old-plan",
		},
	})
	_, err = graph.Transition(ctx, revision, "old-generation-output")
	if !kernel.IsCode(err, kernel.CodeTransitionRejected) {
		t.Fatalf("old generation output err = %v, want transition_rejected", err)
	}
}

func TestReplacePendingRejectsEditableBuiltInAndDuplicateEdges(t *testing.T) {
	graph, decisions, _ := newGraphHarness()
	ctx := context.Background()

	requestID := kernel.IdempotencyKey("decision-fixed-edge")
	if err := decisions.RegisterReplacePending(projectID, requestID); err != nil {
		t.Fatal(err)
	}
	withFixedEdge := basicSubgraph("task-a", requestID, latestRevision(t, graph))
	withFixedEdge.Edges = []Edge{edge(ref("task-a", EndpointPlan), ref("task-a", EndpointExecute))}
	_, err := graph.ReplacePending(ctx, withFixedEdge)
	if !kernel.IsCode(err, kernel.CodeInvalidGraph) {
		t.Fatalf("fixed edge err = %v, want invalid_graph", err)
	}

	requestID = "decision-duplicate-edge"
	if err := decisions.RegisterReplacePending(projectID, requestID); err != nil {
		t.Fatal(err)
	}
	duplicate := basicSubgraph("task-a", requestID, latestRevision(t, graph))
	duplicate.Tasks = append(duplicate.Tasks, Task{ID: "task-b", ContractRef: "contract://task-b", Outcome: TaskActive})
	duplicate.Endpoints = append(duplicate.Endpoints, endpointsFor("task-b")...)
	cross := edge(ref("task-a", EndpointVerify), ref("task-b", EndpointPlan))
	duplicate.Edges = []Edge{cross, cross}
	_, err = graph.ReplacePending(ctx, duplicate)
	if !kernel.IsCode(err, kernel.CodeInvalidGraph) {
		t.Fatalf("duplicate edge err = %v, want invalid_graph", err)
	}
}

func TestSnapshotsDeepCopyEdgeArtifactKinds(t *testing.T) {
	graph, decisions, _ := newGraphHarness()
	ctx := context.Background()

	requestID := kernel.IdempotencyKey("decision-cross-edge")
	if err := decisions.RegisterReplacePending(projectID, requestID); err != nil {
		t.Fatal(err)
	}
	subgraph := basicSubgraph("task-a", requestID, latestRevision(t, graph))
	subgraph.Tasks = append(subgraph.Tasks, Task{ID: "task-b", ContractRef: "contract://task-b", Outcome: TaskActive})
	subgraph.Endpoints = append(subgraph.Endpoints, endpointsFor("task-b")...)
	subgraph.Edges = []Edge{edge(ref("task-a", EndpointVerify), ref("task-b", EndpointPlan))}
	revision, err := graph.ReplacePending(ctx, subgraph)
	if err != nil {
		t.Fatal(err)
	}

	first := mustSnapshot(t, graph, revision)
	first.Edges[0].ArtifactKinds[0] = "mutated"
	second := mustSnapshot(t, graph, revision)
	if second.Edges[0].ArtifactKinds[0] != "phase_output" {
		t.Fatalf("artifact kinds leaked mutation: %#v", second.Edges[0].ArtifactKinds)
	}
}

func TestTaskAndBlockerTerminalTransitionsAreClosed(t *testing.T) {
	graph, decisions, _ := newGraphHarness()
	ctx := context.Background()

	requestID := kernel.IdempotencyKey("decision-task-with-blocker")
	if err := decisions.RegisterReplacePending(projectID, requestID); err != nil {
		t.Fatal(err)
	}
	subgraph := basicSubgraph("task-a", requestID, latestRevision(t, graph))
	subgraph.Blockers = []Blocker{{
		ID:         "blocker-a",
		Target:     ref("task-a", EndpointPlan),
		RequiredBy: RequiredByStart,
		OnFalse:    OnFalseBlock,
		State:      BlockerActive,
	}}
	revision, err := graph.ReplacePending(ctx, subgraph)
	if err != nil {
		t.Fatal(err)
	}

	registerTransition(t, decisions, "resolve-blocker", GraphTransition{
		TargetKind: TargetBlocker,
		BlockerID:  "blocker-a",
		Action:     string(BlockerResolved),
	})
	revision = mustTransition(t, graph, revision, "resolve-blocker")
	registerTransition(t, decisions, "deny-resolved-blocker", GraphTransition{
		TargetKind: TargetBlocker,
		BlockerID:  "blocker-a",
		Action:     string(BlockerDenied),
	})
	_, err = graph.Transition(ctx, revision, "deny-resolved-blocker")
	if !kernel.IsCode(err, kernel.CodeTransitionRejected) {
		t.Fatalf("terminal blocker err = %v, want transition_rejected", err)
	}

	registerTransition(t, decisions, "cancel-task", GraphTransition{
		TargetKind: TargetTask,
		TaskID:     "task-a",
		Action:     string(TaskCanceled),
	})
	revision = mustTransition(t, graph, revision, "cancel-task")
	registerTransition(t, decisions, "fail-canceled-task", GraphTransition{
		TargetKind: TargetTask,
		TaskID:     "task-a",
		Action:     string(TaskFailed),
	})
	_, err = graph.Transition(ctx, revision, "fail-canceled-task")
	if !kernel.IsCode(err, kernel.CodeTransitionRejected) {
		t.Fatalf("terminal task err = %v, want transition_rejected", err)
	}
}

func TestDuplicateResultIDCannotOverwriteExistingResult(t *testing.T) {
	graph, decisions, _ := newGraphHarness()
	ctx := context.Background()
	revision := createTask(t, graph, decisions, "task-a")

	registerTransition(t, decisions, "submit-plan", GraphTransition{TargetKind: TargetPhaseEndpoint, Endpoint: ref("task-a", EndpointPlan), Action: string(EndpointSubmitted), Generation: 1})
	revision = mustTransition(t, graph, revision, "submit-plan")
	registerTransition(t, decisions, "satisfy-plan", GraphTransition{
		TargetKind: TargetPhaseEndpoint,
		Endpoint:   ref("task-a", EndpointPlan),
		Action:     string(EndpointSatisfied),
		Generation: 1,
		Result:     phaseResult("shared-result", "task-a", EndpointPlan),
	})
	revision = mustTransition(t, graph, revision, "satisfy-plan")

	registerTransition(t, decisions, "submit-execute", GraphTransition{TargetKind: TargetPhaseEndpoint, Endpoint: ref("task-a", EndpointExecute), Action: string(EndpointSubmitted), Generation: 1})
	revision = mustTransition(t, graph, revision, "submit-execute")
	registerTransition(t, decisions, "duplicate-result", GraphTransition{
		TargetKind: TargetPhaseEndpoint,
		Endpoint:   ref("task-a", EndpointExecute),
		Action:     string(EndpointSatisfied),
		Generation: 1,
		Result:     phaseResult("shared-result", "task-a", EndpointExecute),
	})
	_, err := graph.Transition(ctx, revision, "duplicate-result")
	if !kernel.IsCode(err, kernel.CodeTransitionRejected) {
		t.Fatalf("duplicate result err = %v, want transition_rejected", err)
	}
}

func TestDecisionRefsAreProjectOperationScopedAndPayloadBound(t *testing.T) {
	store := NewMemoryStore()
	decisions := NewMemoryDecisionLog()
	idempotency := kernel.NewMemoryIdempotencyStore()
	first := NewTaskManagerGraph(taskManagerPrincipal(), store, decisions, idempotency)
	secondPrincipal := taskManagerPrincipal()
	secondPrincipal.InvocationID = "invocation-task-manager-2"
	second := NewTaskManagerGraph(secondPrincipal, store, decisions, idempotency)
	ctx := context.Background()

	requestID := kernel.IdempotencyKey("decision-cross-invocation")
	if err := decisions.RegisterReplacePending(projectID, requestID); err != nil {
		t.Fatal(err)
	}
	base := latestRevision(t, first)
	subgraph := basicSubgraph("task-a", requestID, base)
	revision, err := first.ReplacePending(ctx, subgraph)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := second.ReplacePending(ctx, subgraph)
	if err != nil {
		t.Fatal(err)
	}
	if replayed != revision {
		t.Fatalf("cross invocation replay revision = %d, want %d", replayed, revision)
	}

	conflict := subgraph
	conflict.Tasks[0].ContractRef = "contract://changed"
	_, err = second.ReplacePending(ctx, conflict)
	if !kernel.IsCode(err, kernel.CodeIdempotencyConflict) {
		t.Fatalf("cross invocation conflict err = %v, want idempotency_conflict", err)
	}

	if err := decisions.RegisterTransition(projectID, "same-transition-ref", GraphTransition{
		TargetKind: TargetPhaseEndpoint,
		Endpoint:   ref("task-a", EndpointPlan),
		Action:     "held",
		Generation: 1,
	}); err != nil {
		t.Fatal(err)
	}
	err = decisions.RegisterTransition(projectID, "same-transition-ref", GraphTransition{
		TargetKind: TargetPhaseEndpoint,
		Endpoint:   ref("task-a", EndpointExecute),
		Action:     "held",
		Generation: 1,
	})
	if !kernel.IsCode(err, kernel.CodeIdempotencyConflict) {
		t.Fatalf("decision payload conflict err = %v, want idempotency_conflict", err)
	}
}

func TestPhaseControllerIsOnlyApplyCommandSurface(t *testing.T) {
	var controller PhaseController = phaseControllerFunc(func(context.Context, PhaseCommand) error {
		return nil
	})
	err := controller.Apply(context.Background(), PhaseCommand{
		ID:         "cmd-1",
		Endpoint:   ref("task-a", EndpointPlan),
		Generation: 1,
		BindingRef: "binding://task-a/plan/1",
		LeaseRef:   "lease://task-a/plan/1",
		Action:     CommandStart,
		CauseRef:   "revision://2",
	})
	if err != nil {
		t.Fatal(err)
	}
}

type phaseControllerFunc func(context.Context, PhaseCommand) error

func (f phaseControllerFunc) Apply(ctx context.Context, command PhaseCommand) error {
	return f(ctx, command)
}

const projectID kernel.ProjectID = "project-a"

func newGraphHarness() (*Service, *MemoryDecisionLog, *MemoryStore) {
	store := NewMemoryStore()
	decisions := NewMemoryDecisionLog()
	graph := NewTaskManagerGraph(taskManagerPrincipal(), store, decisions, kernel.NewMemoryIdempotencyStore())
	return graph, decisions, store
}

func taskManagerPrincipal() auth.Principal {
	return auth.Principal{
		ActorPrincipalID: "actor-task-manager",
		Kind:             auth.PrincipalAgent,
		ProjectID:        projectID,
		TaskID:           "task-manager",
		InvocationID:     "invocation-task-manager",
		Role:             auth.RoleTaskManager,
		Tools: auth.ToolSet(
			auth.ToolCoordinationSnapshot,
			auth.ToolCoordinationReplacePending,
			auth.ToolCoordinationTransition,
			auth.ToolTaskManagerSubmitDecision,
		),
	}
}

func latestRevision(t *testing.T, graph *Service) kernel.Revision {
	t.Helper()
	snapshot, err := graph.Snapshot(context.Background(), kernel.LatestRevision)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot.Revision
}

func createTask(t *testing.T, graph *Service, decisions *MemoryDecisionLog, taskID kernel.TaskID) kernel.Revision {
	t.Helper()
	requestID := kernel.IdempotencyKey("decision-create-" + string(taskID))
	base := latestRevision(t, graph)
	if err := decisions.RegisterReplacePending(projectID, requestID); err != nil {
		t.Fatal(err)
	}
	revision, err := graph.ReplacePending(context.Background(), basicSubgraph(taskID, requestID, base))
	if err != nil {
		t.Fatal(err)
	}
	return revision
}

func registerTransition(t *testing.T, decisions *MemoryDecisionLog, ref string, transition GraphTransition) {
	t.Helper()
	if transition.TargetKind == TargetPhaseEndpoint && transition.Generation == 0 {
		transition.Generation = 1
	}
	if err := decisions.RegisterTransition(projectID, ref, transition); err != nil {
		t.Fatal(err)
	}
}

func mustTransition(t *testing.T, graph *Service, revision kernel.Revision, transitionRef string) kernel.Revision {
	t.Helper()
	next, err := graph.Transition(context.Background(), revision, transitionRef)
	if err != nil {
		t.Fatal(err)
	}
	return next
}

func mustSnapshot(t *testing.T, graph *Service, revision kernel.Revision) GraphSnapshot {
	t.Helper()
	snapshot, err := graph.Snapshot(context.Background(), revision)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func mustEndpoint(t *testing.T, snapshot GraphSnapshot, ref PhaseEndpointRef) PhaseEndpoint {
	t.Helper()
	for _, endpoint := range snapshot.Endpoints {
		if endpoint.Ref == ref {
			return endpoint
		}
	}
	t.Fatalf("missing endpoint %#v", ref)
	return PhaseEndpoint{}
}

func phaseResult(id string, taskID kernel.TaskID, endpointID EndpointID) PhaseResult {
	return PhaseResult{
		ID:         id,
		Endpoint:   ref(taskID, endpointID),
		BindingRef: kernel.BindingRef("binding://" + string(taskID) + "/" + string(endpointID) + "/1"),
		OutputRef:  "artifact://phase-output/" + string(taskID) + "/" + string(endpointID),
	}
}

func basicSubgraph(taskID kernel.TaskID, requestID kernel.IdempotencyKey, base kernel.Revision) PendingSubgraph {
	return PendingSubgraph{
		RequestID:    requestID,
		BaseRevision: base,
		Tasks: []Task{{
			ID:          taskID,
			ContractRef: "contract://" + string(taskID),
			Outcome:     TaskActive,
		}},
		Endpoints: endpointsFor(taskID),
	}
}

func endpointsFor(taskID kernel.TaskID) []PhaseEndpoint {
	return []PhaseEndpoint{
		endpoint(taskID, EndpointPlan),
		endpoint(taskID, EndpointExecute),
		endpoint(taskID, EndpointVerify),
	}
}

func endpoint(taskID kernel.TaskID, endpointID EndpointID) PhaseEndpoint {
	return PhaseEndpoint{
		Ref:        ref(taskID, endpointID),
		SpecRef:    "spec://" + string(taskID) + "/" + string(endpointID),
		BindingRef: kernel.BindingRef("binding://" + string(taskID) + "/" + string(endpointID) + "/1"),
		Generation: 1,
		State:      EndpointPending,
		RunPolicy:  RunEnabled,
	}
}

func ref(taskID kernel.TaskID, endpointID EndpointID) PhaseEndpointRef {
	return PhaseEndpointRef{TaskID: taskID, EndpointID: endpointID}
}

func edge(from, to PhaseEndpointRef) Edge {
	return Edge{
		From:          from,
		To:            to,
		Signal:        SignalPhaseSatisfied,
		RequiredBy:    RequiredByStart,
		ArtifactKinds: []string{"phase_output"},
		OnFalse:       OnFalseBlock,
	}
}

func endpointIDs(endpoints []PhaseEndpoint) string {
	out := ""
	for i, endpoint := range endpoints {
		if i > 0 {
			out += ","
		}
		out += string(endpoint.Ref.EndpointID)
	}
	return out
}

func endpointState(snapshot GraphSnapshot, ref PhaseEndpointRef) EndpointState {
	for _, endpoint := range snapshot.Endpoints {
		if endpoint.Ref == ref {
			return endpoint.State
		}
	}
	return ""
}

func TestNoRuntimeOrCRUDPublicGraphMethods(t *testing.T) {
	var _ TaskManagerGraph = (*Service)(nil)
	typ := reflect.TypeOf((*TaskManagerGraph)(nil)).Elem()
	if typ.NumMethod() != 3 {
		t.Fatalf("TaskManagerGraph has %d methods, want exactly Snapshot/ReplacePending/Transition", typ.NumMethod())
	}
}
