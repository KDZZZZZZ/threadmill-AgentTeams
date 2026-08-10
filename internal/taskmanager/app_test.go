package taskmanager

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

const projectID = kernel.ProjectID("project-a")

func TestRequirementPersistsDecisionMetadataBeforeReplacePending(t *testing.T) {
	h := newHarness()

	result, err := h.manager.HandleRequirement(context.Background(), RequirementInput{
		InputRef:    "requirement-input-1",
		TaskID:      "task-a",
		ContractRef: "contract://task-a",
		Requirement: Requirement{Text: "ship task-a", Goal: "complete task-a"},
	})
	if err != nil {
		t.Fatalf("HandleRequirement failed: %v", err)
	}
	if result.DecisionRef == "" {
		t.Fatal("decision ref was not returned")
	}

	decision := h.decisions.must(t, result.DecisionRef)
	if decision.InputRef != "requirement-input-1" || decision.ExpectedRevision == 0 {
		t.Fatalf("decision metadata = %#v, want inputRef and concrete expected revision", decision)
	}
	if decision.Decision.Action != "replace_pending" || decision.Decision.Reason == "" {
		t.Fatalf("decision = %#v, want persisted TaskManagerDecision", decision.Decision)
	}
	h.decisions.assertBefore(t, "submit:replace_pending", "replace_pending")

	snapshot := h.snapshot(t)
	if len(snapshot.Tasks) != 1 || snapshot.Tasks[0].ID != "task-a" {
		t.Fatalf("tasks = %#v, want created task-a", snapshot.Tasks)
	}
	if got := endpointIDs(snapshot.Endpoints); got != "plan,execute,verify" {
		t.Fatalf("endpoints = %s, want plan,execute,verify", got)
	}
	for _, endpoint := range snapshot.Endpoints {
		want := "artifact://contract/task-a/" + string(endpoint.Ref.EndpointID) + "-spec"
		if endpoint.SpecRef != want {
			t.Fatalf("endpoint %s spec = %s, want contract spec %s", endpoint.Ref.EndpointID, endpoint.SpecRef, want)
		}
	}
}

func TestRequirementRevisionConflictReReadsAndSubmitsNewDecision(t *testing.T) {
	h := newHarness()
	h.graph.beforeReplace = func() {
		if h.graph.beforeReplace != nil {
			h.graph.beforeReplace = nil
			h.createTask(t, "other-task")
		}
	}

	result, err := h.manager.HandleRequirement(context.Background(), RequirementInput{
		InputRef:    "requirement-input-2",
		TaskID:      "task-a",
		ContractRef: "contract://task-a",
		Requirement: Requirement{Text: "ship task-a", Goal: "complete task-a"},
	})
	if !kernel.IsCode(err, kernel.CodeRevisionConflict) {
		t.Fatalf("err = %v, want revision conflict", err)
	}
	if result.Status != ReplyConflict || result.DecisionRef == "" {
		t.Fatalf("result = %#v, want conflict with retained decision ref", result)
	}
	if h.decisions.countKind(DecisionKindReplacePending) != 2 {
		t.Fatalf("decisions = %#v, want original + concurrent create only, no retry decision", h.decisions.records)
	}
	if h.decisions.appliedMoreThanOnce(result.DecisionRef) {
		t.Fatalf("conflicted decision ref was reused: %#v", h.decisions.calls)
	}
}

func TestTerminalDecisionsPersistWithoutGraphMutation(t *testing.T) {
	h := newHarness()
	h.createTask(t, "task-a")
	before := h.snapshot(t).Revision

	_, err := h.manager.HandleDelivery(context.Background(), DeliveryInput{
		InputRef: "delivery-missing-evidence",
		TaskID:   "task-a",
		Evidence: DeliveryEvidence{},
	})
	if err != nil {
		t.Fatalf("HandleDelivery failed: %v", err)
	}
	after := h.snapshot(t).Revision
	if after != before {
		t.Fatalf("revision changed from %d to %d for deferred delivery", before, after)
	}
	decision := h.decisions.last(t)
	if decision.Kind != DecisionKindTerminal || decision.Decision.Action != "defer" {
		t.Fatalf("decision = %#v, want terminal defer", decision)
	}
}

func TestMissingDeliveryPolicyIsAuditableDefer(t *testing.T) {
	h := newHarness()
	h.contracts.missingPolicy["task-a"] = true

	result, err := h.manager.HandleRequirement(context.Background(), RequirementInput{
		InputRef:    "requirement-missing-policy",
		TaskID:      "task-a",
		ContractRef: "contract://task-a",
		Requirement: Requirement{Text: "ship task-a"},
	})
	if err != nil {
		t.Fatalf("HandleRequirement failed: %v", err)
	}
	if result.Status != ReplyDeferred || result.DecisionRef == "" {
		t.Fatalf("result = %#v, want deferred decision ref", result)
	}
	if len(h.snapshot(t).Tasks) != 0 {
		t.Fatalf("graph mutated for missing policy: %#v", h.snapshot(t).Tasks)
	}
	decision := h.decisions.must(t, result.DecisionRef)
	if decision.Kind != DecisionKindTerminal || decision.Decision.Action != "defer" {
		t.Fatalf("decision = %#v, want terminal defer", decision)
	}
}

func TestInvalidTaskContractDefersBeforeGraphMutation(t *testing.T) {
	h := newHarness()
	h.contracts.missingSpec["task-a"] = coordination.EndpointVerify

	result, err := h.manager.HandleRequirement(context.Background(), RequirementInput{
		InputRef:    "requirement-missing-spec",
		TaskID:      "task-a",
		ContractRef: "contract://task-a",
		Requirement: Requirement{Text: "ship task-a"},
	})
	if err != nil {
		t.Fatalf("HandleRequirement failed: %v", err)
	}
	if result.Status != ReplyDeferred || result.DecisionRef == "" {
		t.Fatalf("result = %#v, want deferred decision ref", result)
	}
	if len(h.snapshot(t).Tasks) != 0 {
		t.Fatalf("graph mutated for invalid contract: %#v", h.snapshot(t).Tasks)
	}
	if h.decisions.countKind(DecisionKindReplacePending) != 0 {
		t.Fatalf("decisions = %#v, want no graph mutation decision", h.decisions.records)
	}
}

func TestManagerDecisionControlsOnlyItsExplicitEndpointAction(t *testing.T) {
	h := newHarness()
	h.createTask(t, "task-a")
	execute := ref("task-a", coordination.EndpointExecute)

	result, err := h.manager.HandleManagerDecision(context.Background(), ManagerDecisionInput{
		InputRef:     "manager-input-1",
		Endpoint:     execute,
		SeenRevision: h.snapshot(t).Revision,
	}, TaskManagerDecision{
		Action:    "held",
		TargetRef: targetRef(execute),
		Reason:    "Task Manager accepted the explicit hold intent",
	})
	if err != nil {
		t.Fatalf("HandleManagerDecision failed: %v", err)
	}
	if result.Status != ReplyAccepted {
		t.Fatalf("result = %#v, want accepted", result)
	}
	endpoint := h.endpoint(t, execute)
	if endpoint.RunPolicy != coordination.RunHeld || endpoint.Generation != 1 {
		t.Fatalf("endpoint = %#v, want held without stopped generation change", endpoint)
	}
	h.decisions.assertOrder(t, []string{"submit:transition:held", "transition:held"})
}

func TestManagerDecisionAllowsProjectLevelNoChangeWithoutGraphMutation(t *testing.T) {
	h := newHarness()
	h.createTask(t, "task-a")
	before := h.snapshot(t).Revision

	result, err := h.manager.HandleManagerDecision(context.Background(), ManagerDecisionInput{
		InputRef:     "manager-input-project",
		SeenRevision: before,
	}, TaskManagerDecision{
		Action: "no_change",
		Reason: "the request only asks for a status explanation",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != ReplyAccepted || result.DecisionRef == "" || result.GraphRevision != before {
		t.Fatalf("result = %#v, want no-change decision at revision %d", result, before)
	}
	if after := h.snapshot(t).Revision; after != before {
		t.Fatalf("graph revision = %d, want unchanged %d", after, before)
	}
	decision := h.decisions.must(t, result.DecisionRef)
	if decision.Kind != DecisionKindTerminal || decision.Decision.Action != "no_change" {
		t.Fatalf("decision = %#v, want terminal no_change", decision)
	}
}

func TestStoppedEventControlsStoppedReplaceRelease(t *testing.T) {
	h := newHarness()
	h.createTask(t, "task-a")
	execute := ref("task-a", coordination.EndpointExecute)
	if _, err := h.manager.HandleManagerDecision(context.Background(), ManagerDecisionInput{
		InputRef:     "manager-hold",
		Endpoint:     execute,
		SeenRevision: h.snapshot(t).Revision,
	}, TaskManagerDecision{
		Action:    "held",
		TargetRef: targetRef(execute),
		Reason:    "hold before changing pending scope",
	}); err != nil {
		t.Fatal(err)
	}
	held := h.endpoint(t, execute)

	result, err := h.manager.HandlePhaseStopped(context.Background(), PhaseStoppedInput{
		InputRef:      "stopped-event-1",
		Endpoint:      execute,
		CommandID:     "cmd-stop-1",
		LeaseRef:      "lease-1",
		Generation:    held.Generation,
		EvidenceRefs:  []string{"event://phase-stopped"},
		CheckpointRef: "checkpoint://task-a/execute/1",
		NewBindingRef: "binding://task-a/execute/2",
		Replacement: Replacement{
			Endpoints: []coordination.PhaseEndpoint{held},
		},
		ReleaseAfterReplace: true,
	})
	if err != nil {
		t.Fatalf("HandlePhaseStopped failed: %v", err)
	}
	if result.Status != ReplyAccepted {
		t.Fatalf("result = %#v, want accepted", result)
	}
	endpoint := h.endpoint(t, execute)
	if endpoint.Generation != 2 || endpoint.RunPolicy != coordination.RunEnabled {
		t.Fatalf("endpoint = %#v, want stopped generation 2 and released", endpoint)
	}
	h.decisions.assertOrder(t, []string{"submit:transition:stopped", "submit:replace_pending", "replace_pending", "submit:transition:released"})
}

func TestStoppedEventRejectsUnauthenticatedShape(t *testing.T) {
	h := newHarness()
	h.createTask(t, "task-a")
	execute := ref("task-a", coordination.EndpointExecute)

	_, err := h.manager.HandlePhaseStopped(context.Background(), PhaseStoppedInput{
		InputRef:   "bad-stopped-event",
		Endpoint:   execute,
		Generation: 1,
	})
	if !kernel.IsCode(err, kernel.CodeInvalidRequest) {
		t.Fatalf("err = %v, want invalid request", err)
	}
}

func TestStoppedEventSemanticRejectPersistsDecision(t *testing.T) {
	h := newHarness()
	h.createTask(t, "task-a")
	execute := ref("task-a", coordination.EndpointExecute)
	before := h.snapshot(t).Revision

	result, err := h.manager.HandlePhaseStopped(context.Background(), PhaseStoppedInput{
		InputRef:      "stopped-not-held",
		Endpoint:      execute,
		CommandID:     "cmd-stop-1",
		LeaseRef:      "lease-1",
		Generation:    1,
		EvidenceRefs:  []string{"event://phase-stopped"},
		CheckpointRef: "checkpoint://task-a/execute/1",
		NewBindingRef: "binding://task-a/execute/2",
	})
	if !kernel.IsCode(err, kernel.CodeTransitionRejected) {
		t.Fatalf("err = %v, want transition rejected", err)
	}
	if result.Status != ReplyRejected || result.DecisionRef == "" || result.GraphRevision != before {
		t.Fatalf("result = %#v, want rejected decision at current revision %d", result, before)
	}
	decision := h.decisions.must(t, result.DecisionRef)
	if decision.Kind != DecisionKindTerminal || decision.Decision.Action != "reject" {
		t.Fatalf("decision = %#v, want terminal reject", decision)
	}
}

func TestProjectionFailureKeepsGraphAcceptedAndRetriesStableProjection(t *testing.T) {
	h := newHarness()
	h.context.failProjection = true

	result, err := h.manager.HandleRequirement(context.Background(), RequirementInput{
		InputRef:    "requirement-projection-fails",
		TaskID:      "task-a",
		ContractRef: "contract://task-a",
		Requirement: Requirement{Text: "ship task-a"},
	})
	if !kernel.IsCode(err, kernel.CodeInternalError) {
		t.Fatalf("HandleRequirement error = %v, want recoverable projection error", err)
	}
	if result.Status != ReplyDeferred || len(h.snapshot(t).Tasks) != 1 {
		t.Fatalf("result=%#v tasks=%#v, want applied graph and deferred context projection", result, h.snapshot(t).Tasks)
	}
	if len(h.context.pending) != 1 {
		t.Fatalf("pending projections = %#v, want one", h.context.pending)
	}
	projection := h.context.pending[0]
	if projection.SourceRevision != result.GraphRevision || projection.ProjectionID == "" {
		t.Fatalf("projection = %#v, want stable id and source revision", projection)
	}
	h.context.failProjection = false
	if err := h.manager.RetryProjection(context.Background(), projection.ProjectionID); err != nil {
		t.Fatalf("RetryProjection failed: %v", err)
	}
	if len(h.context.projected) != 1 || h.context.projected[0].ProjectionID != projection.ProjectionID {
		t.Fatalf("projected = %#v, want retry of same projection", h.context.projected)
	}
	if h.decisions.countKind(DecisionKindReplacePending) != 1 {
		t.Fatalf("decisions = %#v, want no graph decision replay on projection retry", h.decisions.records)
	}
}

func TestTaskContextRegistrationFailureIsVisibleAndGraphRemainsAuthoritative(t *testing.T) {
	h := newHarness()
	h.context.failRegistration = true

	result, err := h.manager.HandleRequirement(context.Background(), RequirementInput{
		InputRef:    "requirement-registration-fails",
		TaskID:      "task-a",
		ContractRef: "contract://task-a",
		Requirement: Requirement{Text: "ship task-a"},
	})
	if !kernel.IsCode(err, kernel.CodeInternalError) {
		t.Fatalf("HandleRequirement error = %v, want registration error", err)
	}
	if result.Status != ReplyDeferred || result.DecisionRef == "" || len(h.snapshot(t).Tasks) != 1 {
		t.Fatalf("result=%#v tasks=%#v, want applied graph and visible deferred context registration", result, h.snapshot(t).Tasks)
	}
	if len(h.replies.replies) == 0 || h.replies.replies[len(h.replies.replies)-1].Status != ReplyDeferred {
		t.Fatalf("replies = %#v, want persisted deferred reply", h.replies.replies)
	}
}

func TestDeliveryPoliciesReadAuthoritativeContractStore(t *testing.T) {
	policies := []DeliveryPolicy{
		DeliveryPolicyNonCodeArtifact,
		DeliveryPolicyExternalDelivery,
		DeliveryPolicyHumanAcceptance,
		DeliveryPolicyCodeMerge,
	}
	for _, policy := range policies {
		t.Run(string(policy), func(t *testing.T) {
			h := newHarness()
			h.contracts.policy["task-a"] = policy
			h.createTask(t, "task-a")
			h.satisfyEndpoint(t, "task-a", coordination.EndpointPlan)
			h.satisfyEndpoint(t, "task-a", coordination.EndpointExecute)
			h.satisfyEndpoint(t, "task-a", coordination.EndpointVerify)
			h.finalizer.failures = 1

			_, err := h.manager.HandleDelivery(context.Background(), DeliveryInput{
				InputRef: "delivery-input-1",
				TaskID:   "task-a",
				Evidence: evidenceFor(policy),
			})
			if err != nil {
				t.Fatalf("HandleDelivery failed: %v", err)
			}
			if h.task(t, "task-a").Outcome != coordination.TaskDone {
				t.Fatalf("task outcome = %s, want done", h.task(t, "task-a").Outcome)
			}
			if h.finalizer.calls != 1 || h.finalizer.batches[0] == "" {
				t.Fatalf("finalizer calls = %d batches=%#v, want one failed frozen batch", h.finalizer.calls, h.finalizer.batches)
			}
			_, err = h.manager.RetryFinalize(context.Background(), "task-a")
			if err != nil {
				t.Fatalf("RetryFinalize failed: %v", err)
			}
			if h.finalizer.batches[0] != h.finalizer.batches[1] {
				t.Fatalf("finalizer batch changed across retry: %#v", h.finalizer.batches)
			}
		})
	}
}

func TestCodeMergeRequiresLatestMainVerifyAndMergeEvidence(t *testing.T) {
	h := newHarness()
	h.contracts.policy["task-a"] = DeliveryPolicyCodeMerge
	h.createTask(t, "task-a")
	h.satisfyEndpoint(t, "task-a", coordination.EndpointPlan)
	h.satisfyEndpoint(t, "task-a", coordination.EndpointExecute)
	h.satisfyEndpoint(t, "task-a", coordination.EndpointVerify)

	_, err := h.manager.HandleDelivery(context.Background(), DeliveryInput{
		InputRef: "delivery-input-code-merge",
		TaskID:   "task-a",
		Evidence: DeliveryEvidence{LatestMainVerified: true},
	})
	if err != nil {
		t.Fatalf("HandleDelivery returned unexpected error: %v", err)
	}
	if h.task(t, "task-a").Outcome == coordination.TaskDone {
		t.Fatal("code_merge task was marked done without merge success")
	}
	if h.decisions.last(t).Decision.Action != "defer" {
		t.Fatalf("last decision = %#v, want auditable defer", h.decisions.last(t))
	}
}

func TestTaskManagerDecisionShapeRemainsFourFields(t *testing.T) {
	typ := reflect.TypeOf(TaskManagerDecision{})
	if typ.NumField() != 4 {
		t.Fatalf("TaskManagerDecision has %d fields, want 4", typ.NumField())
	}
}

type harness struct {
	store     *coordination.MemoryStore
	graph     *graphSpy
	decisions *decisionSpy
	contracts *contractSpy
	context   *contextSpy
	replies   *replySpy
	finalizer *finalizerSpy
	manager   *Manager
}

func newHarness() *harness {
	store := coordination.NewMemoryStore()
	decisions := newDecisionSpy()
	graph := &graphSpy{
		inner: coordination.NewTaskManagerGraph(taskManagerPrincipal(), store, decisions, kernel.NewMemoryIdempotencyStore()),
	}
	h := &harness{
		store:     store,
		graph:     graph,
		decisions: decisions,
		contracts: newContractSpy(),
		context:   newContextSpy(),
		replies:   &replySpy{},
		finalizer: &finalizerSpy{},
	}
	h.manager = NewManager(Options{
		ProjectID:      projectID,
		Graph:          graph,
		Decisions:      decisions,
		Contracts:      h.contracts,
		TaskContext:    h.context,
		Replies:        h.replies,
		MemoryFinalize: h.finalizer,
	})
	return h
}

func (h *harness) createTask(t *testing.T, taskID kernel.TaskID) {
	t.Helper()
	_, err := h.manager.HandleRequirement(context.Background(), RequirementInput{
		InputRef:    "create-" + string(taskID),
		TaskID:      taskID,
		ContractRef: "contract://" + string(taskID),
		Requirement: Requirement{Text: "ship " + string(taskID), Goal: "complete " + string(taskID)},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func (h *harness) satisfyEndpoint(t *testing.T, taskID kernel.TaskID, endpointID coordination.EndpointID) {
	t.Helper()
	snapshot := h.snapshot(t)
	endpoint := h.endpoint(t, ref(taskID, endpointID))
	_, err := h.manager.transition(context.Background(), "test-"+string(endpointID), snapshot.Revision, TaskManagerDecision{
		Action:    string(coordination.EndpointSubmitted),
		TargetRef: targetRef(ref(taskID, endpointID)),
		Reason:    "test submitted transition",
	}, coordination.GraphTransition{
		TargetKind: coordination.TargetPhaseEndpoint,
		Endpoint:   ref(taskID, endpointID),
		Action:     string(coordination.EndpointSubmitted),
		Generation: endpoint.Generation,
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot = h.snapshot(t)
	endpoint = h.endpoint(t, ref(taskID, endpointID))
	_, err = h.manager.transition(context.Background(), "test-"+string(endpointID), snapshot.Revision, TaskManagerDecision{
		Action:    string(coordination.EndpointSatisfied),
		TargetRef: targetRef(ref(taskID, endpointID)),
		Reason:    "test satisfied transition",
	}, coordination.GraphTransition{
		TargetKind: coordination.TargetPhaseEndpoint,
		Endpoint:   ref(taskID, endpointID),
		Action:     string(coordination.EndpointSatisfied),
		Generation: endpoint.Generation,
		Result: coordination.PhaseResult{
			ID:         fmt.Sprintf("result-%s-%s", taskID, endpointID),
			Endpoint:   ref(taskID, endpointID),
			BindingRef: endpoint.BindingRef,
			OutputRef:  fmt.Sprintf("artifact://%s/%s", taskID, endpointID),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func (h *harness) snapshot(t *testing.T) coordination.GraphSnapshot {
	t.Helper()
	snapshot, err := h.graph.Snapshot(context.Background(), kernel.LatestRevision)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func (h *harness) endpoint(t *testing.T, ref coordination.PhaseEndpointRef) coordination.PhaseEndpoint {
	t.Helper()
	for _, endpoint := range h.snapshot(t).Endpoints {
		if endpoint.Ref == ref {
			return endpoint
		}
	}
	t.Fatalf("missing endpoint %#v", ref)
	return coordination.PhaseEndpoint{}
}

func (h *harness) task(t *testing.T, taskID kernel.TaskID) coordination.Task {
	t.Helper()
	for _, task := range h.snapshot(t).Tasks {
		if task.ID == taskID {
			return task
		}
	}
	t.Fatalf("missing task %s", taskID)
	return coordination.Task{}
}

type graphSpy struct {
	inner         coordination.TaskManagerGraph
	beforeReplace func()
}

func (g *graphSpy) Snapshot(ctx context.Context, revision kernel.Revision) (coordination.GraphSnapshot, error) {
	return g.inner.Snapshot(ctx, revision)
}

func (g *graphSpy) ReplacePending(ctx context.Context, next coordination.PendingSubgraph) (kernel.Revision, error) {
	if g.beforeReplace != nil {
		g.beforeReplace()
	}
	return g.inner.ReplacePending(ctx, next)
}

func (g *graphSpy) Transition(ctx context.Context, revision kernel.Revision, ref string) (kernel.Revision, error) {
	return g.inner.Transition(ctx, revision, ref)
}

type decisionRecord struct {
	Ref string
	DecisionSubmission
}

type decisionSpy struct {
	seq     int
	calls   []string
	records []decisionRecord
	byRef   map[string]decisionRecord
	inner   *coordination.MemoryDecisionLog
}

func newDecisionSpy() *decisionSpy {
	return &decisionSpy{byRef: map[string]decisionRecord{}, inner: coordination.NewMemoryDecisionLog()}
}

func (d *decisionSpy) SubmitDecision(_ context.Context, submission DecisionSubmission) (string, error) {
	d.seq++
	ref := fmt.Sprintf("decision-%d", d.seq)
	d.calls = append(d.calls, fmt.Sprintf("submit:%s:%s:%s", submission.Kind, submission.Decision.Action, ref))
	record := decisionRecord{Ref: ref, DecisionSubmission: submission}
	d.records = append(d.records, record)
	d.byRef[ref] = record
	switch submission.Kind {
	case DecisionKindReplacePending:
		return ref, d.inner.RegisterReplacePending(submission.ProjectID, kernel.IdempotencyKey(ref))
	case DecisionKindTransition:
		return ref, d.inner.RegisterTransition(submission.ProjectID, ref, submission.Transition)
	default:
		return ref, nil
	}
}

func (d *decisionSpy) AuthorizeReplacePending(ctx context.Context, projectID kernel.ProjectID, ref kernel.IdempotencyKey) error {
	d.calls = append(d.calls, "replace_pending:"+string(ref))
	return d.inner.AuthorizeReplacePending(ctx, projectID, ref)
}

func (d *decisionSpy) ResolveTransition(ctx context.Context, projectID kernel.ProjectID, ref string) (coordination.GraphTransition, error) {
	record, err := d.inner.ResolveTransition(ctx, projectID, ref)
	if err == nil {
		action := record.Action
		d.calls = append(d.calls, "transition:"+action+":"+ref)
	}
	return record, err
}

func (d *decisionSpy) must(t *testing.T, ref string) decisionRecord {
	t.Helper()
	record, ok := d.byRef[ref]
	if !ok {
		t.Fatalf("missing decision %s", ref)
	}
	return record
}

func (d *decisionSpy) last(t *testing.T) decisionRecord {
	t.Helper()
	if len(d.records) == 0 {
		t.Fatal("missing decision")
	}
	return d.records[len(d.records)-1]
}

func (d *decisionSpy) countKind(kind DecisionKind) int {
	count := 0
	for _, record := range d.records {
		if record.Kind == kind {
			count++
		}
	}
	return count
}

func (d *decisionSpy) assertBefore(t *testing.T, first, second string) {
	t.Helper()
	firstIndex, secondIndex := -1, -1
	for i, call := range d.calls {
		if hasPrefix(call, first) && firstIndex == -1 {
			firstIndex = i
		}
		if hasPrefix(call, second) && secondIndex == -1 {
			secondIndex = i
		}
	}
	if firstIndex == -1 || secondIndex == -1 || firstIndex > secondIndex {
		t.Fatalf("calls = %#v, want %s before %s", d.calls, first, second)
	}
}

func (d *decisionSpy) assertOrder(t *testing.T, want []string) {
	t.Helper()
	next := 0
	for _, call := range d.calls {
		if next < len(want) && hasPrefix(call, want[next]) {
			next++
		}
	}
	if next != len(want) {
		t.Fatalf("calls = %#v, want ordered prefixes %#v", d.calls, want)
	}
}

func (d *decisionSpy) appliedMoreThanOnce(ref string) bool {
	count := 0
	for _, call := range d.calls {
		if contains(call, ref) && !hasPrefix(call, "submit:") {
			count++
		}
	}
	return count > 1
}

type contractSpy struct {
	policy        map[kernel.TaskID]DeliveryPolicy
	missingPolicy map[kernel.TaskID]bool
	missingSpec   map[kernel.TaskID]coordination.EndpointID
	contracts     map[kernel.TaskID]TaskContract
}

func newContractSpy() *contractSpy {
	return &contractSpy{
		policy:        map[kernel.TaskID]DeliveryPolicy{},
		missingPolicy: map[kernel.TaskID]bool{},
		missingSpec:   map[kernel.TaskID]coordination.EndpointID{},
		contracts:     map[kernel.TaskID]TaskContract{},
	}
}

func (c *contractSpy) ResolveRequirementContract(_ context.Context, input RequirementInput) (TaskContract, error) {
	policy := c.policy[input.TaskID]
	if policy == "" && !c.missingPolicy[input.TaskID] {
		policy = DeliveryPolicyNonCodeArtifact
	}
	contract := TaskContract{
		TaskID:         input.TaskID,
		ContractRef:    input.ContractRef,
		DeliveryPolicy: policy,
		PhaseSpecs: map[coordination.EndpointID]string{
			coordination.EndpointPlan:    "artifact://contract/" + string(input.TaskID) + "/plan-spec",
			coordination.EndpointExecute: "artifact://contract/" + string(input.TaskID) + "/execute-spec",
			coordination.EndpointVerify:  "artifact://contract/" + string(input.TaskID) + "/verify-spec",
		},
	}
	if missing := c.missingSpec[input.TaskID]; missing != "" {
		contract.PhaseSpecs[missing] = ""
	}
	c.contracts[input.TaskID] = contract
	return contract, nil
}

func (c *contractSpy) TaskContract(_ context.Context, taskID kernel.TaskID) (TaskContract, error) {
	contract := c.contracts[taskID]
	if c.missingPolicy[taskID] {
		contract.DeliveryPolicy = ""
	}
	if override := c.policy[taskID]; override != "" {
		contract.DeliveryPolicy = override
	}
	return contract, nil
}

type contextSpy struct {
	failRegistration bool
	failProjection   bool
	registered       []kernel.TaskID
	pending          []ProjectionRequest
	projected        []ProjectionRequest
	byID             map[string]ProjectionRequest
}

func newContextSpy() *contextSpy {
	return &contextSpy{byID: map[string]ProjectionRequest{}}
}

func (c *contextSpy) RegisterTaskSubgraph(_ context.Context, taskID kernel.TaskID) error {
	c.registered = append(c.registered, taskID)
	if c.failRegistration {
		return kernel.Error{Code: kernel.CodeInternalError, Message: "task context registration failed", Recoverable: true}
	}
	return nil
}

func (c *contextSpy) EnqueueProjection(_ context.Context, request ProjectionRequest) error {
	c.byID[request.ProjectionID] = request
	if c.failProjection {
		c.pending = append(c.pending, request)
		return kernel.Error{Code: kernel.CodeInternalError, Message: "projection failed", Recoverable: true}
	}
	c.projected = append(c.projected, request)
	return nil
}

func (c *contextSpy) RetryProjection(_ context.Context, projectionID string) error {
	request, ok := c.byID[projectionID]
	if !ok {
		return kernel.Error{Code: kernel.CodeNotFound, Message: "projection not found"}
	}
	c.projected = append(c.projected, request)
	return nil
}

type replySpy struct {
	replies []ManagerReplyEvent
}

func (r *replySpy) AppendManagerReply(_ context.Context, reply ManagerReplyEvent) error {
	r.replies = append(r.replies, reply)
	return nil
}

type finalizerSpy struct {
	failures int
	calls    int
	batches  []string
}

func (f *finalizerSpy) FinalizeTaskMemory(_ context.Context, _ kernel.TaskID, frozenBatchID string) error {
	f.calls++
	f.batches = append(f.batches, frozenBatchID)
	if f.failures > 0 {
		f.failures--
		return kernel.Error{Code: kernel.CodeInternalError, Message: "temporary finalizer failure", Recoverable: true}
	}
	return nil
}

func taskManagerPrincipal() auth.Principal {
	return auth.Principal{
		ActorPrincipalID: "tm",
		Kind:             auth.PrincipalAgent,
		Role:             auth.RoleTaskManager,
		ProjectID:        projectID,
		Tools: auth.ToolSet(
			auth.ToolCoordinationSnapshot,
			auth.ToolCoordinationReplacePending,
			auth.ToolCoordinationTransition,
		),
	}
}

func evidenceFor(policy DeliveryPolicy) DeliveryEvidence {
	switch policy {
	case DeliveryPolicyNonCodeArtifact:
		return DeliveryEvidence{ArtifactRefs: []string{"artifact://delivery"}}
	case DeliveryPolicyExternalDelivery:
		return DeliveryEvidence{ExternalDelivered: true, EvidenceRefs: []string{"external://delivery"}}
	case DeliveryPolicyHumanAcceptance:
		return DeliveryEvidence{HumanAccepted: true}
	case DeliveryPolicyCodeMerge:
		return DeliveryEvidence{LatestMainVerified: true, MergeSucceeded: true, MergeCommitRef: "commit://abc"}
	default:
		return DeliveryEvidence{}
	}
}

func endpointIDs(endpoints []coordination.PhaseEndpoint) string {
	out := ""
	for i, endpoint := range endpoints {
		if i > 0 {
			out += ","
		}
		out += string(endpoint.Ref.EndpointID)
	}
	return out
}

func ref(taskID kernel.TaskID, endpointID coordination.EndpointID) coordination.PhaseEndpointRef {
	return coordination.PhaseEndpointRef{TaskID: taskID, EndpointID: endpointID}
}

func hasPrefix(value, prefix string) bool {
	return len(value) >= len(prefix) && value[:len(prefix)] == prefix
}

func contains(value, needle string) bool {
	for i := 0; i+len(needle) <= len(value); i++ {
		if value[i:i+len(needle)] == needle {
			return true
		}
	}
	return needle == ""
}
