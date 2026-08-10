package contextgraph

import (
	"context"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

func TestSubscriptionsAreInvocationIsolatedUnionedCancelableAndReplayDeltas(t *testing.T) {
	store := seededStore()
	now := time.Date(2026, 8, 11, 9, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	store.subgraphs["general-c"] = SubgraphRecord{
		Subgraph:  ContextSubgraph{ID: "general-c", Name: "General C", Summary: "more context", Revision: 1, Kind: string(SubgraphKindGeneral)},
		ProjectID: "project-a",
		CreatedAt: now,
		UpdatedAt: now,
	}
	phase := principal(auth.RoleExecutor, "phase-agent", "task-1", auth.ToolSet(auth.ToolContextSubscribe, auth.ToolContextUnsubscribe))
	other := phase
	other.ActorPrincipalID = "other"
	other.InvocationID = "inv-other"

	initial, err := store.CreateInitialSlice(context.Background(), phase, []string{"general-a"})
	if err != nil {
		t.Fatal(err)
	}
	explicit, err := store.Subscribe(context.Background(), phase, SubscribeRequest{SubgraphIDs: []string{"general-c"}})
	if err != nil {
		t.Fatal(err)
	}
	overlap, err := store.Subscribe(context.Background(), phase, SubscribeRequest{SubgraphIDs: []string{"general-a"}})
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := store.Subscribe(context.Background(), other, SubscribeRequest{SubgraphIDs: []string{"general-c"}})
	if err != nil {
		t.Fatal(err)
	}

	effective, err := store.EffectiveSubgraphs(context.Background(), phase)
	if err != nil {
		t.Fatal(err)
	}
	if got := joinStrings(effective); got != "general-a,general-c" {
		t.Fatalf("effective = %s, want general-a,general-c", got)
	}
	if err := store.Unsubscribe(context.Background(), phase, overlap.ID); err != nil {
		t.Fatal(err)
	}
	effective, _ = store.EffectiveSubgraphs(context.Background(), phase)
	if got := joinStrings(effective); got != "general-a,general-c" {
		t.Fatalf("overlap cancel effective = %s, want general-a,general-c", got)
	}
	if err := store.Unsubscribe(context.Background(), phase, initial.SubscriptionIDs[0]); err != nil {
		t.Fatal(err)
	}
	effective, _ = store.EffectiveSubgraphs(context.Background(), phase)
	if got := joinStrings(effective); got != "general-c" {
		t.Fatalf("exclusive cancel effective = %s, want general-c", got)
	}
	if err := store.Unsubscribe(context.Background(), phase, "missing"); !kernel.IsCode(err, kernel.CodeNotFound) {
		t.Fatalf("unknown unsubscribe err = %v, want not_found", err)
	}
	if err := store.Unsubscribe(context.Background(), phase, foreign.ID); !kernel.IsCode(err, kernel.CodeNotFound) {
		t.Fatalf("foreign unsubscribe err = %v, want not_found", err)
	}

	ctxAgent := contextPrincipal(auth.ToolContextCreateNode)
	if _, err := store.CreateNode(context.Background(), ctxAgent, CreateGeneralNodeRequest{
		Statement:   "delta mutation",
		Kind:        string(NodeKindFact),
		SourceRefs:  []string{"source:delta"},
		SubgraphIDs: []string{"general-c"},
	}); err != nil {
		t.Fatal(err)
	}
	first, err := store.PendingDeltas(context.Background(), phase)
	if err != nil {
		t.Fatal(err)
	}
	second, _ := store.PendingDeltas(context.Background(), phase)
	if len(first) != 1 || len(second) != 1 || first[0].ID != second[0].ID {
		t.Fatalf("pending replay first=%#v second=%#v", first, second)
	}
	if first[0].ProjectID != "project-a" {
		t.Fatalf("delta project = %q, want project-a", first[0].ProjectID)
	}
	if err := store.Unsubscribe(context.Background(), phase, explicit.ID); err != nil {
		t.Fatal(err)
	}
	afterCancel, _ := store.PendingDeltas(context.Background(), phase)
	if len(afterCancel) != 0 {
		t.Fatalf("canceled subscription delivered deltas: %#v", afterCancel)
	}
	if err := store.EndInvocation(context.Background(), phase, other.InvocationID); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("cross-invocation expiry err = %v, want forbidden", err)
	}
	if inspected, err := store.InspectSubscriptions(context.Background(), phase, other.InvocationID); !kernel.IsCode(err, kernel.CodeForbidden) || inspected != nil {
		t.Fatalf("cross-invocation inspection = %#v, %v; want forbidden", inspected, err)
	}
	operator := auth.Principal{
		ActorPrincipalID: "operator-1",
		Kind:             auth.PrincipalOperator,
		ProjectID:        "project-a",
		Role:             auth.RoleOperator,
	}
	inspectedByOperator, err := store.InspectSubscriptions(context.Background(), operator, other.InvocationID)
	if err != nil || len(inspectedByOperator) != 1 {
		t.Fatalf("operator inspection = %#v, %v; want one subscription", inspectedByOperator, err)
	}
	if err := store.EndInvocation(context.Background(), other, other.InvocationID); err != nil {
		t.Fatal(err)
	}
	inspected, _ := store.InspectSubscriptions(context.Background(), other, other.InvocationID)
	if len(inspected) != 1 || inspected[0].Active {
		t.Fatalf("expired invocation subscriptions = %#v", inspected)
	}
}

func TestTaskProjectionMemoryBufferAndReviewInvariants(t *testing.T) {
	store := seededStore()
	resolver := fakeTaskEndpointResolver{
		tasks: map[string]bool{"project-a/task-a": true, "project-b/task-a": true},
		done:  map[string]bool{},
		endpoints: map[string]bool{
			"project-a/task-a/plan": true,
			"project-b/task-a/plan": true,
		},
	}
	store.SetTaskEndpointResolver(resolver)
	tm := principal(auth.RoleTaskManager, "tm", "task-a", auth.ToolSet(
		auth.ToolContextRegisterTaskSubgraph,
		auth.ToolContextProjectTaskContext,
		auth.ToolContextFinalizeTaskMemory,
		auth.ToolContextSubscribe,
	))
	binding, err := store.RegisterTaskSubgraph(context.Background(), tm, "task-a")
	if err != nil {
		t.Fatal(err)
	}
	again, err := store.RegisterTaskSubgraph(context.Background(), tm, "task-a")
	if err != nil {
		t.Fatal(err)
	}
	if again != binding {
		t.Fatalf("binding changed: first=%#v second=%#v", binding, again)
	}
	if _, err := store.Subscribe(context.Background(), tm, SubscribeRequest{SubgraphIDs: []string{binding.SubgraphID}}); err != nil {
		t.Fatal(err)
	}
	projection := TaskContextProjection{
		ProjectionID:   "projection-1",
		SourceRevision: "1",
		Statement:      "task contract projection",
		Kind:           string(NodeKindDirective),
		SourceRefs:     []string{"contract:task-a"},
		SubgraphIDs:    []string{binding.SubgraphID},
		Recipients: []TaskContextRecipient{{
			TaskID:       "task-a",
			EndpointRefs: []PhaseEndpointRef{{TaskID: "task-a", EndpointID: "plan"}},
		}},
	}
	nodeID, err := store.ProjectTaskContext(context.Background(), tm, ProjectTaskContextRequest{Projection: projection})
	if err != nil {
		t.Fatal(err)
	}
	projectionDeltas, err := store.PendingDeltas(context.Background(), tm)
	if err != nil {
		t.Fatal(err)
	}
	if len(projectionDeltas) != 1 {
		t.Fatalf("projection pending deltas = %#v, want one", projectionDeltas)
	}
	same, err := store.ProjectTaskContext(context.Background(), tm, ProjectTaskContextRequest{Projection: projection})
	if err != nil {
		t.Fatal(err)
	}
	if same != nodeID {
		t.Fatalf("idempotent projection returned %s, want %s", same, nodeID)
	}
	tmB := tm
	tmB.ProjectID = "project-b"
	bindingB, err := store.RegisterTaskSubgraph(context.Background(), tmB, "task-a")
	if err != nil {
		t.Fatal(err)
	}
	projectionB := projection
	projectionB.SubgraphIDs = []string{bindingB.SubgraphID}
	projectBNodeID, err := store.ProjectTaskContext(context.Background(), tmB, ProjectTaskContextRequest{Projection: projectionB})
	if err != nil {
		t.Fatal(err)
	}
	if projectBNodeID == nodeID {
		t.Fatalf("cross-project projection id collided: %s", nodeID)
	}
	stale := projection
	stale.SourceRevision = "0"
	if _, err := store.ProjectTaskContext(context.Background(), tm, ProjectTaskContextRequest{Projection: stale}); !kernel.IsCode(err, kernel.CodeRevisionConflict) {
		t.Fatalf("stale projection err = %v, want revision_conflict", err)
	}
	cross := projection
	cross.ProjectionID = "projection-cross"
	cross.Recipients[0].TaskID = "task-b"
	if _, err := store.ProjectTaskContext(context.Background(), tm, ProjectTaskContextRequest{Projection: cross}); !kernel.IsCode(err, kernel.CodeNotFound) {
		t.Fatalf("cross task projection err = %v, want not_found", err)
	}

	phase := principal(auth.RoleExecutor, "phase-agent", "task-a", auth.ToolSet(auth.ToolAgentSubmitMemoryCandidate, auth.ToolAgentListTaskMemoryCandidates, auth.ToolContextSubscribe))
	if _, err := store.Subscribe(context.Background(), phase, SubscribeRequest{SubgraphIDs: []string{"general-a"}}); err != nil {
		t.Fatal(err)
	}
	beforeRevision := store.GraphRevision()
	beforeOutbox := len(store.OutboxEvents())
	beforeCandidateDeltas, err := store.PendingDeltas(context.Background(), phase)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := store.SubmitCandidate(context.Background(), phase, SubmitCandidateRequest{Candidate: MemoryCandidate{
		Statement:   "general reusable fact",
		Kind:        string(NodeKindFact),
		SourceRefs:  []string{"artifact:fact"},
		SubgraphIDs: []string{"general-a"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if store.GraphRevision() != beforeRevision || len(store.OutboxEvents()) != beforeOutbox {
		t.Fatal("candidate append changed graph revision or emitted delta/outbox")
	}
	afterCandidateDeltas, err := store.PendingDeltas(context.Background(), phase)
	if err != nil {
		t.Fatal(err)
	}
	if len(afterCandidateDeltas) != len(beforeCandidateDeltas) {
		t.Fatalf("candidate append emitted delta: before=%#v after=%#v", beforeCandidateDeltas, afterCandidateDeltas)
	}
	view, err := store.ListTaskCandidates(context.Background(), phase)
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Candidates) != 1 || view.Candidates[0].CandidateID != candidate.CandidateID {
		t.Fatalf("candidate view = %#v", view)
	}
	if _, err := store.FinalizeTaskMemory(context.Background(), tm, "task-a"); !kernel.IsCode(err, kernel.CodeTransitionRejected) {
		t.Fatalf("finalize before done err = %v, want transition_rejected", err)
	}
	if got := store.AuditEvents(); len(got) == 0 || got[len(got)-1].Action != "context.task_memory.finalize_rejected" {
		t.Fatalf("missing rejected finalize audit: %#v", got)
	}
	resolver.done["project-a/task-a"] = true
	batch, err := store.FinalizeTaskMemory(context.Background(), tm, "task-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Candidates) != 1 {
		t.Fatalf("frozen batch = %#v", batch)
	}
	if _, err := store.SubmitCandidate(context.Background(), phase, SubmitCandidateRequest{Candidate: MemoryCandidate{Statement: "late", Kind: string(NodeKindFact), SourceRefs: []string{"late"}, SubgraphIDs: []string{"general-a"}}}); !kernel.IsCode(err, kernel.CodeTransitionRejected) {
		t.Fatalf("late candidate err = %v, want transition_rejected", err)
	}
	reviewer := principal(auth.RoleContext, "ctx-reviewer", "task-a", auth.ToolSet(auth.ToolContextSubmitReview))
	reviewer.Operation = "review"
	beforeNodes := len(store.nodes)
	_, err = store.SubmitReview(context.Background(), reviewer, CandidateReviewSubmission{Decisions: []CandidateReviewDecision{{
		CandidateID: candidate.CandidateID,
		Action:      "create",
		SubgraphIDs: []string{"task-1-context"},
	}}})
	if !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("bad review err = %v, want forbidden", err)
	}
	if len(store.nodes) != beforeNodes {
		t.Fatal("failed review left partial nodes")
	}
	receipt, err := store.SubmitReview(context.Background(), reviewer, CandidateReviewSubmission{Decisions: []CandidateReviewDecision{{
		CandidateID: candidate.CandidateID,
		Action:      "create",
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(receipt.NodeIDs) != 1 || store.taskMemory[scopedTask("project-a", "task-a")] != TaskMemoryReviewed {
		t.Fatalf("review receipt/state = %#v / %s", receipt, store.taskMemory[scopedTask("project-a", "task-a")])
	}
	reviewDeltas, err := store.PendingDeltas(context.Background(), phase)
	if err != nil {
		t.Fatal(err)
	}
	if len(reviewDeltas) != len(beforeCandidateDeltas)+1 {
		t.Fatalf("review pending deltas = %#v, want one new delta", reviewDeltas)
	}
	if got := store.nodes[receipt.NodeIDs[0]].Node.SourceRefs; joinStrings(got) != "artifact:fact,candidate:"+candidate.CandidateID {
		t.Fatalf("review node source refs = %#v", got)
	}
	replayed, err := store.SubmitReview(context.Background(), reviewer, CandidateReviewSubmission{Decisions: []CandidateReviewDecision{{CandidateID: candidate.CandidateID, Action: "reject"}}})
	if err != nil {
		t.Fatal(err)
	}
	if joinStrings(replayed.NodeIDs) != joinStrings(receipt.NodeIDs) {
		t.Fatalf("review replay = %#v, want %#v", replayed, receipt)
	}
}

func TestSearchAutoSubscriptionRequiresTrustedConsumerAndBindsOriginalInvocation(t *testing.T) {
	store := seededStore()
	ctxAgent := contextPrincipal(auth.ToolContextSubmitReview, auth.ToolContextSearch)
	createNode(t, store, ctxAgent, "search-hit", "creator-1", "", nil)
	searcher := contextPrincipal(auth.ToolContextSearch)
	searcher.ConsumerInvocationID = ""
	if _, err := store.Search(context.Background(), searcher, SearchRequest{Keywords: []string{"search-hit"}}); !kernel.IsCode(err, kernel.CodeInvalidRequest) {
		t.Fatalf("search without trusted consumer err = %v, want invalid_request", err)
	}
	searcher.ConsumerInvocationID = "inv-original"
	searcher.ConsumerTaskID = "task-1"
	searcher.ConsumerRole = auth.RoleExecutor
	result, err := store.Search(context.Background(), searcher, SearchRequest{Keywords: []string{"search-hit"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.SubscriptionIDs) != 1 {
		t.Fatalf("subscription ids = %#v", result.SubscriptionIDs)
	}
	record := store.subscriptions[result.SubscriptionIDs[0]]
	if record.Subscription.ConsumerInvocationID != "inv-original" {
		t.Fatalf("consumer invocation = %q, want inv-original", record.Subscription.ConsumerInvocationID)
	}
	if record.TaskID != "task-1" || record.Role != string(auth.RoleExecutor) {
		t.Fatalf("subscription scope = task:%q role:%q", record.TaskID, record.Role)
	}
	original := principal(auth.RoleExecutor, "original", "task-1", auth.ToolSet(auth.ToolContextUnsubscribe))
	original.InvocationID = "inv-original"
	if err := store.Unsubscribe(context.Background(), original, result.SubscriptionIDs[0]); err != nil {
		t.Fatalf("original consumer could not cancel search subscription: %v", err)
	}
	if store.subscriptions[result.SubscriptionIDs[0]].Active {
		t.Fatal("search subscription remained active after original consumer canceled it")
	}
}

func TestTaskStateIsProjectScopedAndDeltasCannotBeForgedAcrossProject(t *testing.T) {
	store := seededStore()
	store.SetTaskEndpointResolver(fakeTaskEndpointResolver{
		tasks: map[string]bool{"project-a/task-same": true, "project-b/task-same": true},
	})
	tmA := principal(auth.RoleTaskManager, "tm-a", "task-same", auth.ToolSet(auth.ToolContextRegisterTaskSubgraph, auth.ToolContextFinalizeTaskMemory))
	tmB := tmA
	tmB.ProjectID = "project-b"
	bindingA, err := store.RegisterTaskSubgraph(context.Background(), tmA, "task-same")
	if err != nil {
		t.Fatal(err)
	}
	bindingB, err := store.RegisterTaskSubgraph(context.Background(), tmB, "task-same")
	if err != nil {
		t.Fatal(err)
	}
	if bindingA.SubgraphID == bindingB.SubgraphID {
		t.Fatalf("cross-project bindings collided: %#v", bindingA)
	}
	phaseA := principal(auth.RoleExecutor, "phase-a", "task-same", auth.ToolSet(auth.ToolAgentSubmitMemoryCandidate, auth.ToolAgentListTaskMemoryCandidates, auth.ToolContextSubscribe, auth.ToolContextUnsubscribe))
	phaseB := phaseA
	phaseB.ProjectID = "project-b"
	if _, err := store.SubmitCandidate(context.Background(), phaseA, SubmitCandidateRequest{Candidate: MemoryCandidate{Statement: "a", Kind: string(NodeKindFact), SourceRefs: []string{"source:a"}, SubgraphIDs: []string{"general-a"}}}); err != nil {
		t.Fatal(err)
	}
	viewB, err := store.ListTaskCandidates(context.Background(), phaseB)
	if err != nil {
		t.Fatal(err)
	}
	if len(viewB.Candidates) != 0 {
		t.Fatalf("project-b saw project-a candidates: %#v", viewB)
	}
	sub, err := store.Subscribe(context.Background(), phaseA, SubscribeRequest{SubgraphIDs: []string{"general-a"}})
	if err != nil {
		t.Fatal(err)
	}
	ctxAgent := contextPrincipal(auth.ToolContextCreateNode)
	if _, err := store.CreateNode(context.Background(), ctxAgent, CreateGeneralNodeRequest{Statement: "project delta", Kind: string(NodeKindFact), SourceRefs: []string{"source:project-delta"}, SubgraphIDs: []string{"general-a"}}); err != nil {
		t.Fatal(err)
	}
	pendingA, err := store.PendingDeltas(context.Background(), phaseA)
	if err != nil {
		t.Fatal(err)
	}
	if len(pendingA) == 0 {
		t.Fatal("project-a expected pending delta")
	}
	attacker := phaseA
	attacker.ActorPrincipalID = "phase-forger"
	attacker.InvocationID = "inv-forger"
	attacker.ConsumerInvocationID = phaseA.InvocationID
	forgedSub, err := store.Subscribe(context.Background(), attacker, SubscribeRequest{SubgraphIDs: []string{"general-a"}})
	if err != nil {
		t.Fatal(err)
	}
	if forgedSub.ConsumerInvocationID != string(attacker.InvocationID) {
		t.Fatalf("ordinary subscribe used forged consumer: %#v", forgedSub)
	}
	if err := store.Unsubscribe(context.Background(), attacker, sub.ID); !kernel.IsCode(err, kernel.CodeNotFound) {
		t.Fatalf("forged consumer unsubscribe err = %v, want not_found", err)
	}
	forgedPending, err := store.PendingDeltas(context.Background(), attacker)
	if err != nil {
		t.Fatal(err)
	}
	if len(forgedPending) != 0 {
		t.Fatalf("forged consumer saw victim pending deltas: %#v", forgedPending)
	}
	forged := pendingA[0].ID
	if err := store.AckDelta(context.Background(), attacker, forged); !kernel.IsCode(err, kernel.CodeNotFound) {
		t.Fatalf("forged consumer ack err = %v, want not_found", err)
	}
	if err := store.AckDelta(context.Background(), phaseB, forged); !kernel.IsCode(err, kernel.CodeNotFound) {
		t.Fatalf("cross-project ack err = %v, want not_found", err)
	}
	if err := store.Unsubscribe(context.Background(), phaseB, sub.ID); !kernel.IsCode(err, kernel.CodeNotFound) {
		t.Fatalf("cross-project unsubscribe err = %v, want not_found", err)
	}
}

func TestTaskProjectionDeltasUseSubscriptionConsumerScopeForMultipleRecipients(t *testing.T) {
	store := seededStore()
	store.SetTaskEndpointResolver(fakeTaskEndpointResolver{
		tasks: map[string]bool{"project-a/task-a": true, "project-a/task-b": true, "project-a/task-c": true},
		endpoints: map[string]bool{
			"project-a/task-a/plan": true,
			"project-a/task-b/plan": true,
			"project-a/task-c/plan": true,
		},
	})
	tm := principal(auth.RoleTaskManager, "tm", "task-manager", auth.ToolSet(auth.ToolContextRegisterTaskSubgraph, auth.ToolContextProjectTaskContext))
	bindingA, err := store.RegisterTaskSubgraph(context.Background(), tm, "task-a")
	if err != nil {
		t.Fatal(err)
	}
	bindingB, err := store.RegisterTaskSubgraph(context.Background(), tm, "task-b")
	if err != nil {
		t.Fatal(err)
	}
	bindingC, err := store.RegisterTaskSubgraph(context.Background(), tm, "task-c")
	if err != nil {
		t.Fatal(err)
	}
	phaseA := principal(auth.RoleExecutor, "phase-a", "task-a", auth.ToolSet(auth.ToolContextSubscribe))
	phaseB := principal(auth.RoleExecutor, "phase-b", "task-b", auth.ToolSet(auth.ToolContextSubscribe))
	phaseC := principal(auth.RoleExecutor, "phase-c", "task-c", auth.ToolSet(auth.ToolContextSubscribe))
	if _, err := store.Subscribe(context.Background(), phaseA, SubscribeRequest{SubgraphIDs: []string{bindingA.SubgraphID}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Subscribe(context.Background(), phaseB, SubscribeRequest{SubgraphIDs: []string{bindingB.SubgraphID}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Subscribe(context.Background(), phaseC, SubscribeRequest{SubgraphIDs: []string{bindingC.SubgraphID}}); err != nil {
		t.Fatal(err)
	}
	_, err = store.ProjectTaskContext(context.Background(), tm, ProjectTaskContextRequest{Projection: TaskContextProjection{
		ProjectionID:   "projection-multi",
		SourceRevision: "1",
		Statement:      "shared projection",
		Kind:           string(NodeKindDirective),
		SourceRefs:     []string{"contract:multi"},
		SubgraphIDs:    []string{bindingA.SubgraphID, bindingB.SubgraphID},
		Recipients: []TaskContextRecipient{
			{TaskID: "task-a", EndpointRefs: []PhaseEndpointRef{{TaskID: "task-a", EndpointID: "plan"}}},
			{TaskID: "task-b", EndpointRefs: []PhaseEndpointRef{{TaskID: "task-b", EndpointID: "plan"}}},
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	pendingA, err := store.PendingDeltas(context.Background(), phaseA)
	if err != nil {
		t.Fatal(err)
	}
	pendingB, err := store.PendingDeltas(context.Background(), phaseB)
	if err != nil {
		t.Fatal(err)
	}
	pendingC, err := store.PendingDeltas(context.Background(), phaseC)
	if err != nil {
		t.Fatal(err)
	}
	if len(pendingA) != 1 || joinStrings(pendingA[0].SubgraphIDs) != bindingA.SubgraphID {
		t.Fatalf("task-a deltas = %#v", pendingA)
	}
	if len(pendingB) != 1 || joinStrings(pendingB[0].SubgraphIDs) != bindingB.SubgraphID {
		t.Fatalf("task-b deltas = %#v", pendingB)
	}
	if len(pendingC) != 0 {
		t.Fatalf("unrelated task-c saw deltas: %#v", pendingC)
	}
}

type fakeTaskEndpointResolver struct {
	tasks     map[string]bool
	done      map[string]bool
	endpoints map[string]bool
}

func (r fakeTaskEndpointResolver) TaskExists(_ context.Context, projectID kernel.ProjectID, taskID kernel.TaskID) (bool, error) {
	return r.tasks[string(projectID)+"/"+string(taskID)], nil
}

func (r fakeTaskEndpointResolver) TaskDone(_ context.Context, projectID kernel.ProjectID, taskID kernel.TaskID) (bool, error) {
	return r.done[string(projectID)+"/"+string(taskID)], nil
}

func (r fakeTaskEndpointResolver) EndpointExists(_ context.Context, projectID kernel.ProjectID, endpoint PhaseEndpointRef) (bool, error) {
	return r.endpoints[string(projectID)+"/"+string(endpoint.TaskID)+"/"+string(endpoint.EndpointID)], nil
}

func joinStrings(values []string) string {
	out := ""
	for i, value := range values {
		if i > 0 {
			out += ","
		}
		out += value
	}
	return out
}
