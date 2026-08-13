package uiprojection

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/contextgraph"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/scheduler"
)

func TestSnapshotRebuildsFromCanonicalObjectsAndFiltersTasks(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Date(2026, 8, 11, 2, 0, 0, 0, time.UTC)
	projectID := kernel.ProjectID("project-1")
	taskA := coordination.Task{ID: "task-a", ContractRef: "contract-a", Outcome: coordination.TaskActive}
	taskB := coordination.Task{ID: "task-b", ContractRef: "contract-b", Outcome: coordination.TaskDone}
	hidden := coordination.Task{ID: "task-hidden", ContractRef: "contract-hidden", Outcome: coordination.TaskActive}
	planA := endpoint(taskA.ID, coordination.EndpointPlan, 1)
	executeA := endpoint(taskA.ID, coordination.EndpointExecute, 1)
	planB := endpoint(taskB.ID, coordination.EndpointPlan, 1)
	hiddenPlan := endpoint(hidden.ID, coordination.EndpointPlan, 1)
	graph := coordination.GraphSnapshot{
		Revision: 7,
		Tasks:    []coordination.Task{taskA, taskB, hidden},
		Endpoints: []coordination.PhaseEndpoint{
			planA, executeA, planB, hiddenPlan,
		},
		Edges: []coordination.Edge{
			{From: planA.Ref, To: planB.Ref, Signal: coordination.SignalPhaseSatisfied, RequiredBy: coordination.RequiredByStart},
			{From: planA.Ref, To: hiddenPlan.Ref, Signal: coordination.SignalPhaseSatisfied, RequiredBy: coordination.RequiredByStart},
		},
	}
	invocations := []runtime.Invocation{
		invocation(projectID, taskA.ID, coordination.EndpointPlan, "inv-plan", 1, runtime.InvocationRunning, now),
		invocation(projectID, taskA.ID, coordination.EndpointExecute, "inv-execute", 1, runtime.InvocationWaiting, now.Add(time.Minute)),
		invocation(projectID, hidden.ID, coordination.EndpointPlan, "inv-hidden", 1, runtime.InvocationWaiting, now.Add(2*time.Minute)),
		invocation("another-project", taskA.ID, coordination.EndpointPlan, "inv-foreign", 1, runtime.InvocationWaiting, now),
		{ProjectID: projectID, TaskID: taskA.ID, ID: "manager", Role: auth.RoleTaskManager, Status: runtime.InvocationWaiting},
	}
	permissions := &fakePermissions{
		project: true,
		grants: map[kernel.TaskID]TaskReadGrant{
			taskA.ID:  {Visible: true, ContextBodies: true, CandidateBodies: true},
			taskB.ID:  {Visible: true, ContextBodies: true, CandidateBodies: true},
			hidden.ID: {Visible: false},
		},
	}
	service := NewService(
		fakeCapacityReader{record: CapacityRecord{Capacity: scheduler.Capacity{Desired: 4, Healthy: 5, Active: 2, Revision: 3}, UpdatedAt: now}},
		fakeGraphReader{latest: graph},
		fakeInvocationReader{items: invocations},
		fakeContextReader{},
		fakeCursorReader{cursor: "evt_42"},
		permissions,
	)

	got, err := service.Snapshot(ctx, operator(projectID), projectID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got.Revision != 7 || got.Cursor != "evt_42" {
		t.Fatalf("snapshot revision/cursor = %d/%q", got.Revision, got.Cursor)
	}
	if got.Capacity.DesiredConcurrency != 4 || got.Capacity.HealthyCapacity != 5 || got.Capacity.ActiveInvocations != 2 {
		t.Fatalf("capacity did not preserve scheduler snapshot: %+v", got.Capacity)
	}
	if got.Capacity.WaitingInvocations != 1 {
		t.Fatalf("waiting = %d, want one visible logical waiting Invocation", got.Capacity.WaitingInvocations)
	}
	if gotTasks := taskIDs(got.Tasks); !reflect.DeepEqual(gotTasks, []kernel.TaskID{"task-a", "task-b"}) {
		t.Fatalf("visible tasks = %v", gotTasks)
	}
	if len(got.Edges) != 1 || got.Edges[0].To.TaskID != taskB.ID {
		t.Fatalf("edges were not task-ACL filtered: %+v", got.Edges)
	}
	assertNode(t, got.Nodes, planA.Ref, "running", "inv-plan")
	assertNode(t, got.Nodes, executeA.Ref, "waiting", "inv-execute")
	assertNode(t, got.Nodes, planB.Ref, "pending", "")
	for _, node := range got.Nodes {
		if node.TaskID == hidden.ID {
			t.Fatalf("hidden task node leaked: %+v", node)
		}
	}
}

func TestInspectEndpointSeparatesSubscriptionsSliceAndCreatorCandidates(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Date(2026, 8, 11, 3, 0, 0, 0, time.UTC)
	projectID := kernel.ProjectID("project-1")
	ref := coordination.PhaseEndpointRef{TaskID: "task-a", EndpointID: coordination.EndpointExecute}
	graph := coordination.GraphSnapshot{Revision: 11, Tasks: []coordination.Task{{ID: ref.TaskID, ContractRef: "contract-a", Outcome: coordination.TaskActive}}, Endpoints: []coordination.PhaseEndpoint{{Ref: ref, SpecRef: "spec", BindingRef: "binding-2", Generation: 2, State: coordination.EndpointPending, RunPolicy: coordination.RunEnabled}}}
	old := invocation(projectID, ref.TaskID, ref.EndpointID, "inv-old", 1, runtime.InvocationStopped, now)
	current := invocation(projectID, ref.TaskID, ref.EndpointID, "inv-current", 2, runtime.InvocationRunning, now.Add(time.Minute))
	current.ContextSliceRef = "slice-current"
	current.TaskMemoryBufferRef = "buffer-task-a"
	current.WorkspaceRef = "workspace-a"
	inspection := ContextInspection{
		Subscriptions: []contextgraph.SubscriptionInspection{
			{ID: "sub-search", ConsumerInvocationID: string(current.ID), Source: "search", SubgraphIDs: []string{"general-b", "general-a"}, Active: true},
			{ID: "sub-initial", ConsumerInvocationID: string(current.ID), Source: "initial_slice", SubgraphIDs: []string{"task-a"}, Active: true},
			{ID: "sub-foreign", ConsumerInvocationID: string(old.ID), Source: "explicit", SubgraphIDs: []string{"secret"}, Active: true},
		},
		Slice: contextgraph.ContextSlice{
			GraphRevision: 19,
			Nodes:         []contextgraph.ContextNode{{ID: "node-1", Kind: "fact", Statement: "visible fact", Status: "accepted", SourceRefs: []string{"artifact-1"}, SubgraphIDs: []string{"general-a"}}},
		},
		Frontier: []string{"node:z", "node:a"},
		Candidates: []CandidateInspectionRecord{
			{ProjectID: projectID, TaskID: ref.TaskID, CreatedByInvocationID: current.ID, View: candidate("candidate-current", "from current")},
			{ProjectID: projectID, TaskID: ref.TaskID, CreatedByInvocationID: old.ID, View: candidate("candidate-old", "from old")},
			{ProjectID: projectID, TaskID: "another-task", CreatedByInvocationID: current.ID, View: candidate("candidate-cross-task", "cross task")},
			{ProjectID: "another-project", TaskID: ref.TaskID, CreatedByInvocationID: current.ID, View: candidate("candidate-cross-project", "cross project")},
		},
	}
	service := NewService(
		fakeCapacityReader{},
		fakeGraphReader{latest: graph},
		fakeInvocationReader{items: []runtime.Invocation{old, current}},
		fakeContextReader{inspection: inspection},
		fakeCursorReader{},
		&fakePermissions{project: true, grants: map[kernel.TaskID]TaskReadGrant{ref.TaskID: {Visible: true, ContextBodies: true, CandidateBodies: true}}},
	)

	got, err := service.InspectEndpoint(ctx, operator(projectID), projectID, ref, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got.Invocation == nil || got.Invocation.InvocationID != current.ID || got.Generation != 2 {
		t.Fatalf("selected invocation = %+v generation=%d", got.Invocation, got.Generation)
	}
	if len(got.Subscriptions) != 2 || got.Subscriptions[0].SubscriptionID != "sub-initial" || got.Subscriptions[1].SubscriptionID != "sub-search" {
		t.Fatalf("subscriptions = %+v", got.Subscriptions)
	}
	if !reflect.DeepEqual(got.Subscriptions[1].SubgraphIDs, []string{"general-a", "general-b"}) {
		t.Fatalf("subscription subgraphs were not canonicalized: %v", got.Subscriptions[1].SubgraphIDs)
	}
	if got.ContextSlice == nil || got.ContextSlice.ContextSliceRef != "slice-current" || got.ContextSlice.Revision != "19" || len(got.ContextSlice.Nodes) != 1 {
		t.Fatalf("context slice = %+v", got.ContextSlice)
	}
	if got.ContextSlice.Omitted == nil {
		t.Fatal("context slice omitted projection must be a stable empty array")
	}
	if !reflect.DeepEqual(got.ContextSlice.Frontier, []string{"node:a", "node:z"}) {
		t.Fatalf("frontier = %v", got.ContextSlice.Frontier)
	}
	if got.TaskMemoryBuffer == nil || len(got.TaskMemoryBuffer.Candidates) != 1 || got.TaskMemoryBuffer.Candidates[0].CandidateID != "candidate-current" {
		t.Fatalf("creator-filtered memory buffer = %+v", got.TaskMemoryBuffer)
	}
	if got.ContextSlice.Nodes[0].NodeID == got.TaskMemoryBuffer.Candidates[0].CandidateID {
		t.Fatal("ContextNode and TaskMemoryBuffer candidate were collapsed into one object")
	}
}

func TestInspectEndpointRedactsBodiesAndMarksHistoricalSubscriptionsInactive(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Date(2026, 8, 11, 4, 0, 0, 0, time.UTC)
	projectID := kernel.ProjectID("project-1")
	ref := coordination.PhaseEndpointRef{TaskID: "task-a", EndpointID: coordination.EndpointPlan}
	old := invocation(projectID, ref.TaskID, ref.EndpointID, "inv-old", 1, runtime.InvocationStopped, now)
	old.ContextSliceRef = "slice-old"
	old.TaskMemoryBufferRef = "buffer-a"
	graph := coordination.GraphSnapshot{Revision: 3, Tasks: []coordination.Task{{ID: ref.TaskID, ContractRef: "contract", Outcome: coordination.TaskActive}}, Endpoints: []coordination.PhaseEndpoint{{Ref: ref, SpecRef: "spec", BindingRef: "binding-2", Generation: 2, State: coordination.EndpointPending, RunPolicy: coordination.RunEnabled}}}
	inspection := ContextInspection{
		Subscriptions: []contextgraph.SubscriptionInspection{{ID: "sub-old", ConsumerInvocationID: string(old.ID), Source: "explicit", SubgraphIDs: []string{"general"}, Active: true}},
		Slice: contextgraph.ContextSlice{GraphRevision: 8, Nodes: []contextgraph.ContextNode{
			{ID: "node-1", Kind: "fact", Statement: "secret one"},
			{ID: "node-2", Kind: "fact", Statement: "secret two"},
		}},
		Candidates: []CandidateInspectionRecord{{ProjectID: projectID, TaskID: ref.TaskID, CreatedByInvocationID: old.ID, View: candidate("candidate-1", "secret candidate")}},
	}
	service := NewService(fakeCapacityReader{}, fakeGraphReader{latest: graph}, fakeInvocationReader{items: []runtime.Invocation{old}}, fakeContextReader{inspection: inspection}, fakeCursorReader{}, &fakePermissions{project: true, grants: map[kernel.TaskID]TaskReadGrant{ref.TaskID: {Visible: true}}})

	got, err := service.InspectEndpoint(ctx, operator(projectID), projectID, ref, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Subscriptions) != 1 || got.Subscriptions[0].Active {
		t.Fatalf("historical subscription shown active: %+v", got.Subscriptions)
	}
	if got.ContextSlice == nil || len(got.ContextSlice.Nodes) != 0 || !hasOmission(got.ContextSlice.Omitted, "forbidden", 2) {
		t.Fatalf("context bodies were not safely omitted: %+v", got.ContextSlice)
	}
	if got.TaskMemoryBuffer == nil || len(got.TaskMemoryBuffer.Candidates) != 0 || !hasOmission(got.TaskMemoryBuffer.Omitted, "forbidden", 1) {
		t.Fatalf("candidate bodies were not separately omitted: %+v", got.TaskMemoryBuffer)
	}
}

func TestInspectEndpointNeverFabricatesInvocationForUnrunEndpoint(t *testing.T) {
	t.Parallel()
	projectID := kernel.ProjectID("project-1")
	ref := coordination.PhaseEndpointRef{TaskID: "task-a", EndpointID: coordination.EndpointVerify}
	graph := coordination.GraphSnapshot{Revision: 2, Tasks: []coordination.Task{{ID: ref.TaskID, ContractRef: "contract", Outcome: coordination.TaskActive}}, Endpoints: []coordination.PhaseEndpoint{{Ref: ref, SpecRef: "spec", BindingRef: "binding", Generation: 1, State: coordination.EndpointPending, RunPolicy: coordination.RunEnabled}}}
	service := NewService(fakeCapacityReader{}, fakeGraphReader{latest: graph}, fakeInvocationReader{}, fakeContextReader{}, fakeCursorReader{}, &fakePermissions{project: true, grants: map[kernel.TaskID]TaskReadGrant{ref.TaskID: {Visible: true}}})

	got, err := service.InspectEndpoint(context.Background(), operator(projectID), projectID, ref, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got.Invocation != nil || got.ContextSlice != nil || got.TaskMemoryBuffer != nil {
		t.Fatalf("unrun endpoint received fabricated resources: %+v", got)
	}
}

func TestProjectionRejectsCrossProjectOperator(t *testing.T) {
	t.Parallel()
	service := NewService(fakeCapacityReader{}, fakeGraphReader{}, fakeInvocationReader{}, fakeContextReader{}, fakeCursorReader{}, &fakePermissions{project: true})
	_, err := service.Snapshot(context.Background(), operator("project-a"), "project-b", 0)
	if !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("error = %v, want forbidden", err)
	}
}

type fakeCapacityReader struct{ record CapacityRecord }

func (f fakeCapacityReader) ReadCapacity(context.Context, kernel.ProjectID) (CapacityRecord, error) {
	return f.record, nil
}

type fakeGraphReader struct{ latest coordination.GraphSnapshot }

func (f fakeGraphReader) Latest(context.Context, kernel.ProjectID) (coordination.GraphSnapshot, error) {
	return f.latest, nil
}
func (f fakeGraphReader) Snapshot(context.Context, kernel.ProjectID, kernel.Revision) (coordination.GraphSnapshot, error) {
	return f.latest, nil
}

type fakeInvocationReader struct{ items []runtime.Invocation }

func (f fakeInvocationReader) ListInvocations(context.Context, InvocationFilter) ([]runtime.Invocation, error) {
	return append([]runtime.Invocation(nil), f.items...), nil
}

type fakeContextReader struct{ inspection ContextInspection }

func (f fakeContextReader) InspectInvocation(context.Context, auth.Principal, runtime.Invocation) (ContextInspection, error) {
	return f.inspection, nil
}

type fakeCursorReader struct{ cursor string }

func (f fakeCursorReader) CurrentCursor(context.Context, kernel.ProjectID) (string, error) {
	return f.cursor, nil
}

type fakePermissions struct {
	project bool
	grants  map[kernel.TaskID]TaskReadGrant
}

func (f *fakePermissions) CanReadProject(context.Context, auth.Principal, kernel.ProjectID) (bool, error) {
	return f.project, nil
}
func (f *fakePermissions) TaskGrant(_ context.Context, _ auth.Principal, _ kernel.ProjectID, taskID kernel.TaskID) (TaskReadGrant, error) {
	return f.grants[taskID], nil
}

func endpoint(taskID kernel.TaskID, endpointID kernel.EndpointID, generation int) coordination.PhaseEndpoint {
	return coordination.PhaseEndpoint{Ref: coordination.PhaseEndpointRef{TaskID: taskID, EndpointID: endpointID}, SpecRef: "spec-" + string(endpointID), BindingRef: kernel.BindingRef("binding-" + string(taskID) + "-" + string(endpointID)), Generation: generation, State: coordination.EndpointPending, RunPolicy: coordination.RunEnabled}
}

func invocation(projectID kernel.ProjectID, taskID kernel.TaskID, endpointID kernel.EndpointID, id kernel.InvocationID, generation uint64, status runtime.InvocationStatus, createdAt time.Time) runtime.Invocation {
	role := auth.RolePlanner
	if endpointID == coordination.EndpointExecute {
		role = auth.RoleExecutor
	}
	if endpointID == coordination.EndpointVerify {
		role = auth.RoleVerifier
	}
	return runtime.Invocation{ID: id, ProjectID: projectID, TaskID: taskID, EndpointID: endpointID, Generation: generation, Role: role, Status: status, CreatedAt: createdAt}
}

func candidate(id, statement string) contextgraph.TaskMemoryCandidateView {
	return contextgraph.TaskMemoryCandidateView{CandidateID: id, Candidate: contextgraph.MemoryCandidate{Statement: statement, Kind: "fact", SourceRefs: []string{"artifact"}, SubgraphIDs: []string{"general"}}}
}

func operator(projectID kernel.ProjectID) auth.Principal {
	return auth.Principal{ActorPrincipalID: "operator-1", Kind: auth.PrincipalOperator, ProjectID: projectID, Role: auth.RoleOperator}
}

func taskIDs(tasks []TaskSummary) []kernel.TaskID {
	out := make([]kernel.TaskID, len(tasks))
	for i := range tasks {
		out[i] = tasks[i].TaskID
	}
	return out
}

func assertNode(t *testing.T, nodes []GraphNode, ref coordination.PhaseEndpointRef, state string, invocationID kernel.InvocationID) {
	t.Helper()
	for _, node := range nodes {
		if node.TaskID == ref.TaskID && node.EndpointID == ref.EndpointID {
			if node.State != state || node.LatestInvocationRef != invocationID {
				t.Fatalf("node %v = state %q invocation %q", ref, node.State, node.LatestInvocationRef)
			}
			return
		}
	}
	t.Fatalf("node %v not found", ref)
}

func hasOmission(omitted []OmittedContext, reason string, count int) bool {
	for _, item := range omitted {
		if item.Reason == reason && item.Count == count {
			return true
		}
	}
	return false
}
