package coordination

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/postgres"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/migrations"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// TestComplexGraphParallelJoinBlockerHotReplaceAgainstPostgres is the IT02
// graph acceptance path. The PhaseController is a recording boundary here;
// the production AgentTeams/DeepSeek boundary is exercised separately by the
// production E2E. Every graph mutation in this test still goes through the
// TaskManagerGraph and a registered decision reference.
func TestComplexGraphParallelJoinBlockerHotReplaceAgainstPostgres(t *testing.T) {
	dsn := os.Getenv("THREADMILL_PG_TEST_DSN")
	if dsn == "" {
		dsn = os.Getenv("THREADMILL_TEST_DATABASE_URL")
	}
	if dsn == "" {
		t.Skip("THREADMILL_PG_TEST_DSN or THREADMILL_TEST_DATABASE_URL is required")
	}
	ctx := context.Background()
	db := openComplexGraphTestDB(t, ctx, dsn)
	store := NewPostgresStore(db)
	graph := NewTaskManagerGraph(taskManagerPrincipal(), store, store, kernel.NewMemoryIdempotencyStore())

	const (
		taskA kernel.TaskID = "complex-a"
		taskB kernel.TaskID = "complex-b"
		taskC kernel.TaskID = "complex-c"
	)
	createRef := kernel.IdempotencyKey("tmdec-complex-create")
	if err := store.RegisterReplacePending(ctx, projectID, createRef); err != nil {
		t.Fatal(err)
	}
	revision, err := graph.ReplacePending(ctx, PendingSubgraph{
		RequestID:    createRef,
		BaseRevision: latestRevision(t, graph),
		Tasks: []Task{
			{ID: taskA, ContractRef: "contract://complex-a", Outcome: TaskActive},
			{ID: taskB, ContractRef: "contract://complex-b", Outcome: TaskActive},
			{ID: taskC, ContractRef: "contract://complex-c", Outcome: TaskActive},
		},
		Endpoints: append(append(endpointsFor(taskA), endpointsFor(taskB)...), endpointsFor(taskC)...),
		Edges: []Edge{
			{
				From: ref(taskA, EndpointVerify), To: ref(taskC, EndpointVerify),
				Signal: SignalTaskDone, RequiredBy: RequiredByCompletion,
				ArtifactKinds: []string{"verify_result"}, OnFalse: OnFalseBlock,
			},
			{
				From: ref(taskB, EndpointVerify), To: ref(taskC, EndpointVerify),
				Signal: SignalTaskDone, RequiredBy: RequiredByCompletion,
				ArtifactKinds: []string{"verify_result"}, OnFalse: OnFalseBlock,
			},
		},
		Blockers: []Blocker{{
			ID: "human-approve-b-execute", Target: ref(taskB, EndpointExecute),
			RequiredBy: RequiredByStart, OnFalse: OnFalseBlock, State: BlockerActive,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	assertComplexRevisionDecision(t, ctx, db, revision, string(createRef))

	controller := &recordingController{}
	scheduling := &mutableComplexScheduling{state: RuntimeSchedulingState{Capacity: RuntimeCapacity{Desired: 2, Healthy: 2, Revision: 1}}}
	runner, err := NewRuntime(RuntimeOptions{
		ProjectID: projectID, Store: store, PhaseController: controller, Scheduling: scheduling,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	initial := complexRunCommands(t, controller, 1, ref(taskA, EndpointPlan), ref(taskB, EndpointPlan))

	// Capacity is runtime state, not graph semantics. Shrinking it below the
	// current active count neither changes the graph nor creates a third lease.
	scheduling.SetCapacity(1, 1, 2)
	if err := runner.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if got := latestRevision(t, graph); got != revision {
		t.Fatalf("capacity change advanced graph revision = %d, want %d", got, revision)
	}
	if active := countPostgresActiveLeases(ctx, store, projectID); active != 2 {
		t.Fatalf("active leases after capacity shrink = %d, want existing 2", active)
	}
	scheduling.SetCapacity(2, 2, 3)

	for _, command := range initial {
		if err := store.RecordPhaseInvocationStarted(ctx, projectID, command); err != nil {
			t.Fatal(err)
		}
	}
	if err := runner.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}

	revision = complexTransition(t, ctx, db, store, graph, revision, "tmdec-hold-a-plan", GraphTransition{
		TargetKind: TargetPhaseEndpoint, Endpoint: ref(taskA, EndpointPlan), Action: "held", Generation: 1,
	})
	heldA := mustEndpoint(t, mustSnapshot(t, graph, revision), ref(taskA, EndpointPlan))
	earlyReplaceRef := kernel.IdempotencyKey("tmdec-replace-running-a-plan")
	if err := store.RegisterReplacePending(ctx, projectID, earlyReplaceRef); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.ReplacePending(ctx, PendingSubgraph{
		RequestID: earlyReplaceRef, BaseRevision: revision, Endpoints: []PhaseEndpoint{heldA},
	}); !kernel.IsCode(err, kernel.CodeEndpointInFlight) {
		t.Fatalf("replace running held endpoint = %v, want endpoint_in_flight", err)
	}
	if got := latestRevision(t, graph); got != revision {
		t.Fatalf("rejected hot replace advanced revision = %d, want %d", got, revision)
	}

	if err := runner.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	stopA := complexCommand(t, controller, ref(taskA, EndpointPlan), 1, CommandStop)
	checkpoint := "checkpoint://complex-a/plan/1"
	if err := store.RecordPhaseInvocationStopped(ctx, projectID, stopA, checkpoint, false); err != nil {
		t.Fatal(err)
	}
	if err := runner.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	// Releasing A's lease gives the second slot to C.plan while B.plan keeps
	// running, proving actual parallel scheduling rather than sequential state
	// mutation.
	_ = complexCommand(t, controller, ref(taskC, EndpointPlan), 1, CommandStart)

	stoppedEvidence := deterministicPhaseObservationID(phaseObservationStopped, stopA.ID)
	revision = complexTransition(t, ctx, db, store, graph, revision, "tmdec-stopped-a-plan", GraphTransition{
		TargetKind: TargetPhaseEndpoint, Endpoint: ref(taskA, EndpointPlan), Action: "stopped", Generation: 1,
		NewBindingRef: "binding://complex-a/plan/2", NewSpecRef: "spec://complex-a/plan/2",
		CheckpointRef: checkpoint, EvidenceRefs: []string{stoppedEvidence},
	})
	stoppedA := mustEndpoint(t, mustSnapshot(t, graph, revision), ref(taskA, EndpointPlan))
	hotReplaceRef := kernel.IdempotencyKey("tmdec-hot-replace-a-plan")
	if err := store.RegisterReplacePending(ctx, projectID, hotReplaceRef); err != nil {
		t.Fatal(err)
	}
	revision, err = graph.ReplacePending(ctx, PendingSubgraph{
		RequestID: hotReplaceRef, BaseRevision: revision,
		Endpoints: []PhaseEndpoint{stoppedA},
		Blockers: []Blocker{{
			ID: "hot-review-a-plan", Target: ref(taskA, EndpointPlan),
			RequiredBy: RequiredByStart, OnFalse: OnFalseBlock, State: BlockerResolved,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	assertComplexRevisionDecision(t, ctx, db, revision, string(hotReplaceRef))
	revision = complexTransition(t, ctx, db, store, graph, revision, "tmdec-release-a-plan", GraphTransition{
		TargetKind: TargetPhaseEndpoint, Endpoint: ref(taskA, EndpointPlan), Action: "released", Generation: 2,
	})
	if err := runner.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if complexHasCommand(controller, ref(taskA, EndpointPlan), 2, CommandResume) {
		t.Fatal("A.plan resumed before capacity became available")
	}

	// B.plan completion frees one slot and A resumes from the persisted
	// checkpoint. C.plan remains concurrently active.
	revision = completeComplexEndpoint(t, ctx, db, store, graph, runner, revision, initial[ref(taskB, EndpointPlan)], "b-plan")
	resumeA := complexCommand(t, controller, ref(taskA, EndpointPlan), 2, CommandResume)
	if err := store.RecordPhaseInvocationStarted(ctx, projectID, resumeA); err != nil {
		t.Fatal(err)
	}
	if err := runner.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}

	cPlan := complexCommand(t, controller, ref(taskC, EndpointPlan), 1, CommandStart)
	revision = completeComplexEndpoint(t, ctx, db, store, graph, runner, revision, cPlan, "c-plan")
	cExecute := complexCommand(t, controller, ref(taskC, EndpointExecute), 1, CommandStart)
	revision = completeComplexEndpoint(t, ctx, db, store, graph, runner, revision, resumeA, "a-plan-resume")
	aExecute := complexCommand(t, controller, ref(taskA, EndpointExecute), 1, CommandStart)

	// Resolving the manual blocker changes graph eligibility, but capacity=2
	// still prevents B.execute from starting until another running endpoint
	// finishes.
	revision = complexTransition(t, ctx, db, store, graph, revision, "tmdec-resolve-b-blocker", GraphTransition{
		TargetKind: TargetBlocker, BlockerID: "human-approve-b-execute", Action: string(BlockerResolved),
	})
	if err := runner.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if complexHasCommand(controller, ref(taskB, EndpointExecute), 1, CommandStart) {
		t.Fatal("B.execute started while both capacity slots were occupied")
	}

	revision = completeComplexEndpoint(t, ctx, db, store, graph, runner, revision, aExecute, "a-execute")
	bExecute := complexCommand(t, controller, ref(taskB, EndpointExecute), 1, CommandStart)
	revision = completeComplexEndpoint(t, ctx, db, store, graph, runner, revision, cExecute, "c-execute")
	aVerify := complexCommand(t, controller, ref(taskA, EndpointVerify), 1, CommandStart)
	revision = completeComplexEndpoint(t, ctx, db, store, graph, runner, revision, bExecute, "b-execute")
	cVerify := complexCommand(t, controller, ref(taskC, EndpointVerify), 1, CommandStart)

	// C.verify is allowed to run while declared completion inputs are pending,
	// but its PhaseOutput cannot be accepted until both source Tasks are done.
	earlySubmitRef := "tmdec-c-verify-submit-before-join"
	if err := store.RegisterTransition(ctx, projectID, earlySubmitRef, GraphTransition{
		TargetKind: TargetPhaseEndpoint, Endpoint: ref(taskC, EndpointVerify),
		Action: string(EndpointSubmitted), Generation: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.Transition(ctx, revision, earlySubmitRef); !kernel.IsCode(err, kernel.CodeTransitionRejected) {
		t.Fatalf("C.verify submitted before completion join = %v, want transition_rejected", err)
	}
	if got := latestRevision(t, graph); got != revision {
		t.Fatalf("rejected completion join advanced revision = %d, want %d", got, revision)
	}

	revision = completeComplexEndpoint(t, ctx, db, store, graph, runner, revision, aVerify, "a-verify")
	revision = complexTransition(t, ctx, db, store, graph, revision, "tmdec-done-a", GraphTransition{
		TargetKind: TargetTask, TaskID: taskA, Action: string(TaskDone),
	})
	bVerify := complexCommand(t, controller, ref(taskB, EndpointVerify), 1, CommandStart)
	revision = completeComplexEndpoint(t, ctx, db, store, graph, runner, revision, bVerify, "b-verify")
	revision = complexTransition(t, ctx, db, store, graph, revision, "tmdec-done-b", GraphTransition{
		TargetKind: TargetTask, TaskID: taskB, Action: string(TaskDone),
	})
	revision = completeComplexEndpoint(t, ctx, db, store, graph, runner, revision, cVerify, "c-verify-after-join")
	revision = complexTransition(t, ctx, db, store, graph, revision, "tmdec-done-c", GraphTransition{
		TargetKind: TargetTask, TaskID: taskC, Action: string(TaskDone),
	})

	final := mustSnapshot(t, graph, revision)
	for _, taskID := range []kernel.TaskID{taskA, taskB, taskC} {
		for _, task := range final.Tasks {
			if task.ID == taskID && task.Outcome != TaskDone {
				t.Fatalf("task %s outcome = %s, want done", taskID, task.Outcome)
			}
		}
	}
	if active := countPostgresActiveLeases(ctx, store, projectID); active != 0 {
		t.Fatalf("final active leases = %d, want 0", active)
	}
	if endpoint := mustEndpoint(t, final, ref(taskA, EndpointPlan)); endpoint.Generation != 2 || endpoint.BindingRef != "binding://complex-a/plan/2" {
		t.Fatalf("hot-replaced A.plan = %#v", endpoint)
	}
}

type mutableComplexScheduling struct {
	mu    sync.Mutex
	state RuntimeSchedulingState
}

func (s *mutableComplexScheduling) RuntimeSchedulingState(context.Context) (RuntimeSchedulingState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state, nil
}

func (s *mutableComplexScheduling) SetCapacity(desired, healthy, revision int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.Capacity.Desired = desired
	s.state.Capacity.Healthy = healthy
	s.state.Capacity.Revision = revision
}

func openComplexGraphTestDB(t *testing.T, ctx context.Context, dsn string) *sql.DB {
	t.Helper()
	base, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = base.Close() })
	schema := fmt.Sprintf("coordination_complex_%d", time.Now().UnixNano())
	if _, err := base.ExecContext(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = base.ExecContext(context.Background(), `DROP SCHEMA `+schema+` CASCADE`) })
	db, err := sql.Open("pgx", dsnWithSearchPath(t, dsn, schema))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	loaded, err := postgres.LoadMigrations(migrations.FS, migrations.Dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := postgres.NewMigrator(db).Apply(ctx, loaded); err != nil {
		t.Fatal(err)
	}
	return db
}

func complexRunCommands(t *testing.T, controller *recordingController, generation int, refs ...PhaseEndpointRef) map[PhaseEndpointRef]PhaseCommand {
	t.Helper()
	want := make(map[PhaseEndpointRef]struct{}, len(refs))
	for _, ref := range refs {
		want[ref] = struct{}{}
	}
	got := make(map[PhaseEndpointRef]PhaseCommand, len(refs))
	for _, command := range controller.commandsByID() {
		if command.Action != CommandStart || command.Generation != generation {
			continue
		}
		if _, ok := want[command.Endpoint]; ok {
			got[command.Endpoint] = command
		}
	}
	if len(got) != len(want) {
		t.Fatalf("initial parallel commands = %#v, want refs %#v", got, refs)
	}
	return got
}

func complexCommand(t *testing.T, controller *recordingController, ref PhaseEndpointRef, generation int, action CommandAction) PhaseCommand {
	t.Helper()
	matches := make([]PhaseCommand, 0, 1)
	for _, command := range controller.commandsByID() {
		if command.Endpoint == ref && command.Generation == generation && command.Action == action {
			matches = append(matches, command)
		}
	}
	if len(matches) != 1 {
		t.Fatalf("commands for %#v generation=%d action=%s = %#v, want exactly one", ref, generation, action, matches)
	}
	return matches[0]
}

func complexHasCommand(controller *recordingController, ref PhaseEndpointRef, generation int, action CommandAction) bool {
	for _, command := range controller.commandsByID() {
		if command.Endpoint == ref && command.Generation == generation && command.Action == action {
			return true
		}
	}
	return false
}

func completeComplexEndpoint(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	store *PostgresStore,
	graph *Service,
	runner RuntimeRunner,
	revision kernel.Revision,
	command PhaseCommand,
	prefix string,
) kernel.Revision {
	t.Helper()
	if err := store.RecordPhaseOutputSubmitted(ctx, projectID, command); err != nil {
		t.Fatalf("record %s PhaseOutput: %v", prefix, err)
	}
	if err := runner.Reconcile(ctx); err != nil {
		t.Fatalf("fold %s PhaseOutput: %v", prefix, err)
	}
	revision = complexTransition(t, ctx, db, store, graph, revision, "tmdec-"+prefix+"-submitted", GraphTransition{
		TargetKind: TargetPhaseEndpoint, Endpoint: command.Endpoint,
		Action: string(EndpointSubmitted), Generation: command.Generation,
	})
	revision = complexTransition(t, ctx, db, store, graph, revision, "tmdec-"+prefix+"-satisfied", GraphTransition{
		TargetKind: TargetPhaseEndpoint, Endpoint: command.Endpoint,
		Action: string(EndpointSatisfied), Generation: command.Generation,
		Result: PhaseResult{
			ID: "result-" + prefix, Endpoint: command.Endpoint, BindingRef: command.BindingRef,
			OutputRef: "artifact://" + prefix,
		},
	})
	if err := runner.Reconcile(ctx); err != nil {
		t.Fatalf("reconcile after %s satisfied: %v", prefix, err)
	}
	return revision
}

func complexTransition(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	store *PostgresStore,
	graph *Service,
	revision kernel.Revision,
	decisionRef string,
	transition GraphTransition,
) kernel.Revision {
	t.Helper()
	if err := store.RegisterTransition(ctx, projectID, decisionRef, transition); err != nil {
		t.Fatal(err)
	}
	next, err := graph.Transition(ctx, revision, decisionRef)
	if err != nil {
		t.Fatalf("transition %s: %v", decisionRef, err)
	}
	assertComplexRevisionDecision(t, ctx, db, next, decisionRef)
	return next
}

func assertComplexRevisionDecision(t *testing.T, ctx context.Context, db *sql.DB, revision kernel.Revision, decisionRef string) {
	t.Helper()
	var actual string
	if err := db.QueryRowContext(ctx, `
SELECT decision_ref
FROM coordination_graph_revisions
WHERE project_id=$1 AND revision=$2`, projectID, revision).Scan(&actual); err != nil {
		t.Fatal(err)
	}
	if actual != decisionRef {
		t.Fatalf("revision %d decision_ref = %q, want %q", revision, actual, decisionRef)
	}
}
