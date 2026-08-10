package phase

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	baseruntime "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime/promptcatalog"
)

func TestApplyStartAssemblesAndDispatchesOnce(t *testing.T) {
	ctx := context.Background()
	harness := newHarness(t)
	controller := harness.controller()
	cmd := validCommand("cmd-start", coordination.CommandStart, "binding-1", "lease-1", 1)

	if err := controller.Apply(ctx, cmd); err != nil {
		t.Fatalf("apply start: %v", err)
	}
	if err := controller.Apply(ctx, cmd); err != nil {
		t.Fatalf("replay start: %v", err)
	}
	if got := len(harness.host.dispatches); got != 1 {
		t.Fatalf("dispatch count = %d, want 1", got)
	}
	dispatch := harness.host.dispatches[0]
	if dispatch.Start.InvocationID == "" || dispatch.Start.BindingRef != cmd.BindingRef || dispatch.Start.Generation != cmd.Generation {
		t.Fatalf("dispatch start input lost command authority: %#v", dispatch.Start)
	}
	if dispatch.Invocation.BindingRef != cmd.BindingRef || dispatch.Invocation.LeaseID != cmd.LeaseRef {
		t.Fatalf("invocation authority = %#v, want command binding/lease", dispatch.Invocation)
	}
	if dispatch.Capability.InvocationID != dispatch.Invocation.ID || !containsTool(dispatch.Capability.Tools, auth.ToolRuntimeAwaitInputs) {
		t.Fatalf("capability not bound to dispatched invocation/tools: %#v", dispatch.Capability)
	}
	if dispatch.Prompt.Text == "" || dispatch.Prompt.SHA256 == "" {
		t.Fatalf("prompt was not assembled: %#v", dispatch.Prompt)
	}

	conflict := cmd
	conflict.BindingRef = "other-binding"
	if err := controller.Apply(ctx, conflict); !kernel.IsCode(err, kernel.CodeCommandConflict) {
		t.Fatalf("conflicting replay = %v, want command_conflict", err)
	}
}

func TestApplyStartSingleFlightConcurrentReplaysUseOneInvocation(t *testing.T) {
	ctx := context.Background()
	harness := newHarness(t)
	controller := harness.controller()
	cmd := validCommand("cmd-start-concurrent", coordination.CommandStart, "binding-1", "lease-1", 1)
	harness.host.dispatchBlock = make(chan struct{})
	var ready atomic.Int32
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ready.Add(1)
			errs <- controller.Apply(ctx, cmd)
		}()
	}
	for ready.Load() < 2 {
		time.Sleep(time.Millisecond)
	}
	close(harness.host.dispatchBlock)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent apply: %v", err)
		}
	}
	if got := len(harness.host.dispatches); got != 1 {
		t.Fatalf("dispatch count = %d, want 1", got)
	}
	if harness.host.dispatches[0].Invocation.ID != deterministicInvocationID(cmd) {
		t.Fatalf("invocation id = %s, want deterministic id", harness.host.dispatches[0].Invocation.ID)
	}
}

func TestApplyStartRejectsStaleBindingLeaseAndDispatchFailureLeavesNoActiveSession(t *testing.T) {
	ctx := context.Background()
	harness := newHarness(t)
	controller := harness.controller()

	stale := validCommand("cmd-stale", coordination.CommandStart, "old-binding", "lease-1", 1)
	if err := controller.Apply(ctx, stale); !kernel.IsCode(err, kernel.CodeStaleBinding) {
		t.Fatalf("stale binding = %v, want stale_binding", err)
	}
	leaseConflict := validCommand("cmd-lease", coordination.CommandStart, "binding-1", "wrong-lease", 1)
	if err := controller.Apply(ctx, leaseConflict); !kernel.IsCode(err, kernel.CodeLeaseConflict) {
		t.Fatalf("lease conflict = %v, want lease_conflict", err)
	}

	harness.host.dispatchErr = errors.New("executor down")
	cmd := validCommand("cmd-fail", coordination.CommandStart, "binding-1", "lease-1", 1)
	if err := controller.Apply(ctx, cmd); err == nil {
		t.Fatal("dispatch failure returned nil")
	}
	if got := len(harness.host.dispatches); got != 0 {
		t.Fatalf("dispatches recorded on failed host start = %d, want 0", got)
	}
	if got := len(harness.host.activeSessions); got != 0 {
		t.Fatalf("active sessions after failed dispatch = %d, want 0", got)
	}
	harness.host.dispatchErr = nil
	if err := controller.Apply(ctx, cmd); err != nil {
		t.Fatalf("retry after dispatch failure: %v", err)
	}
	if got := len(harness.host.dispatches); got != 1 {
		t.Fatalf("dispatches after retry = %d, want 1", got)
	}
	if harness.host.dispatches[0].Invocation.ID != deterministicInvocationID(cmd) {
		t.Fatalf("retry did not reuse deterministic invocation id")
	}
}

func TestControllerFailClosesMismatchedResolverSnapshots(t *testing.T) {
	ctx := context.Background()
	harness := newHarness(t)
	harness.binding.bypassValidation = true
	harness.binding.current.TaskID = "other-task"
	controller := harness.controller()
	cmd := validCommand("cmd-mismatch", coordination.CommandStart, "binding-1", "lease-1", 1)
	if err := controller.Apply(ctx, cmd); !kernel.IsCode(err, kernel.CodeStaleBinding) {
		t.Fatalf("mismatched resolve snapshot = %v, want stale_binding", err)
	}

	harness = newHarness(t)
	harness.binding.bypassValidation = true
	controller = harness.controller()
	if err := controller.Apply(ctx, cmd); err != nil {
		t.Fatalf("apply start: %v", err)
	}
	invocationID := harness.host.dispatches[0].Invocation.ID
	if _, err := controller.AwaitInputs(ctx, invocationID, AwaitInputsRequest{}); err != nil {
		t.Fatalf("await inputs: %v", err)
	}
	harness.binding.current.ProjectID = "other-project"
	if _, err := controller.SubmitPhaseOutput(ctx, invocationID, validOutput()); !kernel.IsCode(err, kernel.CodeStaleBinding) {
		t.Fatalf("mismatched refresh snapshot = %v, want stale_binding", err)
	}
}

func TestAwaitInputsAndRuntimeUpdatesReassembleInvocation(t *testing.T) {
	ctx := context.Background()
	harness := newHarness(t)
	controller := harness.controller()
	cmd := validCommand("cmd-start", coordination.CommandStart, "binding-1", "lease-1", 1)
	if err := controller.Apply(ctx, cmd); err != nil {
		t.Fatalf("apply start: %v", err)
	}
	invocationID := harness.host.dispatches[0].Invocation.ID

	result, err := controller.AwaitInputs(ctx, invocationID, AwaitInputsRequest{})
	if err != nil {
		t.Fatalf("await inputs: %v", err)
	}
	if result.InputRevision != "inputs-2" || len(result.Delivered) != 1 || len(result.Pending) != 0 {
		t.Fatalf("await result = %#v, want new delivered input", result)
	}
	if got := harness.host.suspendCalls[invocationID]; got != 1 {
		t.Fatalf("await did not suspend/fence invocation before waiting")
	}
	if got := len(harness.host.rehydrates); got != 1 {
		t.Fatalf("rehydrates after await = %d, want 1", got)
	}
	if harness.host.rehydrates[0].Start.Inputs.InputRevision != "inputs-2" {
		t.Fatalf("rehydrate did not use latest inputs: %#v", harness.host.rehydrates[0].Start.Inputs)
	}

	if err := controller.OnContextDelta(ctx, invocationID, ContextDelta{SubscriptionID: "sub-1", SubgraphID: "sg-1", Revision: 3}); err != nil {
		t.Fatalf("context delta: %v", err)
	}
	if got := len(harness.host.rehydrates); got != 2 {
		t.Fatalf("rehydrates after context delta = %d, want 2", got)
	}
}

func TestAwaitInputsBlocksRehydrateWhenRunningTransitionFailsAndCanRetry(t *testing.T) {
	ctx := context.Background()
	harness := newHarness(t)
	harness.store.failTransitions = append(harness.store.failTransitions, transitionFailure{
		from: baseruntime.InvocationWaiting,
		to:   baseruntime.InvocationRunning,
		err:  kernel.Error{Code: kernel.CodeRevisionConflict, Message: "transient transition conflict", Recoverable: true},
	})
	controller := harness.controller()
	cmd := validCommand("cmd-await-transition-retry", coordination.CommandStart, "binding-1", "lease-1", 1)
	if err := controller.Apply(ctx, cmd); err != nil {
		t.Fatalf("apply start: %v", err)
	}
	invocationID := harness.host.dispatches[0].Invocation.ID
	if _, err := controller.AwaitInputs(ctx, invocationID, AwaitInputsRequest{}); !kernel.IsCode(err, kernel.CodeRevisionConflict) {
		t.Fatalf("await with failed waiting->running = %v, want revision_conflict", err)
	}
	if got := len(harness.host.rehydrates); got != 0 {
		t.Fatalf("rehydrates after failed transition = %d, want 0", got)
	}
	if _, err := controller.AwaitInputs(ctx, invocationID, AwaitInputsRequest{}); err != nil {
		t.Fatalf("retry await: %v", err)
	}
	if got := len(harness.host.rehydrates); got != 1 {
		t.Fatalf("rehydrates after retry = %d, want 1", got)
	}
}

func TestSubmitPhaseOutputRoutesArtifactsAndRejectsPendingOrStaleBinding(t *testing.T) {
	ctx := context.Background()
	harness := newHarness(t)
	controller := harness.controller()
	cmd := validCommand("cmd-start", coordination.CommandStart, "binding-1", "lease-1", 1)
	if err := controller.Apply(ctx, cmd); err != nil {
		t.Fatalf("apply start: %v", err)
	}
	invocationID := harness.host.dispatches[0].Invocation.ID

	_, err := controller.SubmitPhaseOutput(ctx, invocationID, PhaseOutput{
		Phase:        "execute",
		DeliveryRefs: []string{"workspace/result.txt"},
		ReportRef:    "workspace/report.md",
	})
	if !kernel.IsCode(err, kernel.CodeTransitionRejected) {
		t.Fatalf("submit with pending input = %v, want transition_rejected", err)
	}

	if _, err := controller.AwaitInputs(ctx, invocationID, AwaitInputsRequest{}); err != nil {
		t.Fatalf("await inputs: %v", err)
	}
	harness.binding.current.BindingRef = "new-binding"
	_, err = controller.SubmitPhaseOutput(ctx, invocationID, PhaseOutput{
		Phase:        "execute",
		DeliveryRefs: []string{"workspace/result.txt"},
		ReportRef:    "workspace/report.md",
	})
	if !kernel.IsCode(err, kernel.CodeStaleBinding) {
		t.Fatalf("submit after stale binding = %v, want stale_binding", err)
	}
	harness.binding.current.BindingRef = "binding-1"
	accepted, err := controller.SubmitPhaseOutput(ctx, invocationID, PhaseOutput{
		Phase:        "execute",
		DeliveryRefs: []string{"workspace/result.txt"},
		ReportRef:    "workspace/report.md",
		EvidenceRefs: []string{"evidence/test.log"},
	})
	if err != nil {
		t.Fatalf("submit output: %v", err)
	}
	if len(accepted.Output.DeliveryRefs) != 1 || accepted.Output.DeliveryRefs[0] != "artifact-1" || accepted.Output.ReportRef != "artifact-2" {
		t.Fatalf("artifact refs not routed: %#v", accepted.Output)
	}
	if accepted.Endpoint != cmd.Endpoint || accepted.Generation != cmd.Generation || accepted.BindingRef != cmd.BindingRef || accepted.LeaseRef != cmd.LeaseRef || accepted.WorkspaceHead != "main-rev-1" {
		t.Fatalf("receipt lost trusted runtime binding: %#v", accepted)
	}
	byCommand, ok, err := controller.OutputByCommand(ctx, cmd.ID)
	if err != nil || !ok {
		t.Fatalf("output by command = %#v %v, %v; want receipt", byCommand, ok, err)
	}
	if byCommand.InvocationID != accepted.InvocationID || byCommand.BindingRef != accepted.BindingRef {
		t.Fatalf("command receipt differs from invocation receipt: %#v vs %#v", byCommand, accepted)
	}

	second := newHarness(t)
	second.binding.current.BindingRef = "new-binding"
	secondController := second.controller()
	if err := secondController.Apply(ctx, cmd); !kernel.IsCode(err, kernel.CodeStaleBinding) {
		t.Fatalf("start against stale binding = %v, want stale_binding", err)
	}
}

func TestSubmitPhaseOutputRouteFailureDoesNotRevokeAndCanRetry(t *testing.T) {
	ctx := context.Background()
	harness := newHarness(t)
	harness.artifacts.errOnce = errors.New("artifact store unavailable")
	controller := harness.controller()
	cmd := validCommand("cmd-route-retry", coordination.CommandStart, "binding-1", "lease-1", 1)
	if err := controller.Apply(ctx, cmd); err != nil {
		t.Fatalf("apply start: %v", err)
	}
	invocationID := harness.host.dispatches[0].Invocation.ID
	if _, err := controller.AwaitInputs(ctx, invocationID, AwaitInputsRequest{}); err != nil {
		t.Fatalf("await inputs: %v", err)
	}
	if _, err := controller.SubmitPhaseOutput(ctx, invocationID, validOutput()); err == nil {
		t.Fatal("submit unexpectedly succeeded with route failure")
	}
	if harness.host.revoked[invocationID] {
		t.Fatal("route failure revoked invocation before pending output was persisted")
	}
	if _, ok, err := controller.Output(ctx, invocationID); err != nil || ok {
		t.Fatalf("receipt after route failure = ok %v err %v, want no receipt", ok, err)
	}
	if _, err := controller.SubmitPhaseOutput(ctx, invocationID, validOutput()); err != nil {
		t.Fatalf("retry submit after route failure: %v", err)
	}
}

func TestSubmitPhaseOutputCompletedTransitionFailureUsesPendingReceiptOnRetry(t *testing.T) {
	ctx := context.Background()
	harness := newHarness(t)
	harness.store.failTransitions = append(harness.store.failTransitions, transitionFailure{
		from: baseruntime.InvocationRunning,
		to:   baseruntime.InvocationCompleted,
		err:  kernel.Error{Code: kernel.CodeRevisionConflict, Message: "terminal transition conflict", Recoverable: true},
	})
	controller := harness.controller()
	cmd := validCommand("cmd-terminal-retry", coordination.CommandStart, "binding-1", "lease-1", 1)
	if err := controller.Apply(ctx, cmd); err != nil {
		t.Fatalf("apply start: %v", err)
	}
	invocationID := harness.host.dispatches[0].Invocation.ID
	if _, err := controller.AwaitInputs(ctx, invocationID, AwaitInputsRequest{}); err != nil {
		t.Fatalf("await inputs: %v", err)
	}
	if _, err := controller.SubmitPhaseOutput(ctx, invocationID, validOutput()); !kernel.IsCode(err, kernel.CodeRevisionConflict) {
		t.Fatalf("submit with terminal transition failure = %v, want revision_conflict", err)
	}
	if !harness.host.revoked[invocationID] {
		t.Fatal("terminal transition failure should happen after revoke/fence")
	}
	if _, ok, err := controller.Output(ctx, invocationID); err != nil || ok {
		t.Fatalf("final receipt after terminal failure = ok %v err %v, want no final receipt", ok, err)
	}
	routed := harness.artifacts.next
	if _, err := controller.SubmitPhaseOutput(ctx, invocationID, validOutput()); err != nil {
		t.Fatalf("retry submit after terminal failure: %v", err)
	}
	if harness.artifacts.next != routed {
		t.Fatalf("retry rerouted artifacts: got counter %d, want %d", harness.artifacts.next, routed)
	}
}

func TestSubmitPhaseOutputRejectsIncompleteCompletionDeliveryMetadata(t *testing.T) {
	ctx := context.Background()
	harness := newHarness(t)
	harness.inputs.next.Delivered[0].SourceRevision = ""
	controller := harness.controller()
	cmd := validCommand("cmd-bad-input", coordination.CommandStart, "binding-1", "lease-1", 1)
	if err := controller.Apply(ctx, cmd); err != nil {
		t.Fatalf("apply start: %v", err)
	}
	invocationID := harness.host.dispatches[0].Invocation.ID
	if _, err := controller.AwaitInputs(ctx, invocationID, AwaitInputsRequest{}); err != nil {
		t.Fatalf("await inputs: %v", err)
	}
	if _, err := controller.SubmitPhaseOutput(ctx, invocationID, validOutput()); !kernel.IsCode(err, kernel.CodeTransitionRejected) {
		t.Fatalf("submit with missing source revision = %v, want transition_rejected", err)
	}

	second := newHarness(t)
	second.inputs.next.Delivered[0].ArtifactRefs = []string{"wrong-artifact"}
	secondController := second.controller()
	if err := secondController.Apply(ctx, cmd); err != nil {
		t.Fatalf("second apply start: %v", err)
	}
	secondInvocation := second.host.dispatches[0].Invocation.ID
	if _, err := secondController.AwaitInputs(ctx, secondInvocation, AwaitInputsRequest{}); err != nil {
		t.Fatalf("second await inputs: %v", err)
	}
	if _, err := secondController.SubmitPhaseOutput(ctx, secondInvocation, validOutput()); !kernel.IsCode(err, kernel.CodeTransitionRejected) {
		t.Fatalf("submit with missing required artifact = %v, want transition_rejected", err)
	}
}

func TestSubmitPhaseOutputRevokeFailureKeepsInvocationRetryable(t *testing.T) {
	ctx := context.Background()
	harness := newHarness(t)
	controller := harness.controller()
	cmd := validCommand("cmd-submit-retry", coordination.CommandStart, "binding-1", "lease-1", 1)
	if err := controller.Apply(ctx, cmd); err != nil {
		t.Fatalf("apply start: %v", err)
	}
	invocationID := harness.host.dispatches[0].Invocation.ID
	if _, err := controller.AwaitInputs(ctx, invocationID, AwaitInputsRequest{}); err != nil {
		t.Fatalf("await inputs: %v", err)
	}
	harness.host.revokeErr = errors.New("fence unavailable")
	if _, err := controller.SubmitPhaseOutput(ctx, invocationID, validOutput()); err == nil {
		t.Fatal("submit unexpectedly succeeded with revoke failure")
	}
	if _, ok, err := controller.Output(ctx, invocationID); err != nil || ok {
		t.Fatalf("receipt after failed revoke = ok %v err %v, want no receipt", ok, err)
	}
	harness.host.revokeErr = nil
	if _, err := controller.SubmitPhaseOutput(ctx, invocationID, validOutput()); err != nil {
		t.Fatalf("retry submit after revoke failure: %v", err)
	}
}

func TestStopAndResumeInvalidateOldInvocationAndCreateFreshRuntime(t *testing.T) {
	ctx := context.Background()
	harness := newHarness(t)
	controller := harness.controller()
	start := validCommand("cmd-start", coordination.CommandStart, "binding-1", "lease-1", 1)
	if err := controller.Apply(ctx, start); err != nil {
		t.Fatalf("apply start: %v", err)
	}
	oldInvocation := harness.host.dispatches[0].Invocation.ID

	stop := validCommand("cmd-stop", coordination.CommandStop, "binding-1", "lease-1", 1)
	if err := controller.Apply(ctx, stop); err != nil {
		t.Fatalf("apply stop: %v", err)
	}
	if !harness.host.revoked[oldInvocation] {
		t.Fatalf("old invocation %s was not revoked", oldInvocation)
	}
	if got := len(harness.host.stops); got != 1 {
		t.Fatalf("stop calls = %d, want 1", got)
	}
	if _, err := controller.SubmitPhaseOutput(ctx, oldInvocation, PhaseOutput{Phase: "execute", ReportRef: "workspace/report.md"}); !kernel.IsCode(err, kernel.CodeStaleCommand) {
		t.Fatalf("old invocation submit = %v, want stale_command", err)
	}

	harness.binding.current = BindingSnapshot{
		ProjectID:           "project-a",
		ActorPrincipalID:    "agent-executor",
		TaskID:              "task-a",
		EndpointID:          "execute",
		Generation:          2,
		BindingRef:          "binding-2",
		LeaseRef:            "lease-2",
		WorkspaceRef:        "workspace-2",
		WorkspaceRevision:   "main-rev-2",
		ContextSliceRef:     "ctx-2",
		ContextSlice:        `{"nodes":[]}`,
		TaskMemoryBufferRef: "mem-2",
		TaskMemoryBuffer:    `{"candidates":[]}`,
		TaskContract:        `{"task":"a"}`,
		PhaseSpec:           `{"phase":"execute"}`,
		Inputs:              deliveredInputs("inputs-3"),
		CheckpointRef:       "checkpoint-1",
	}
	resume := validCommand("cmd-resume", coordination.CommandResume, "binding-2", "lease-2", 2)
	if err := controller.Apply(ctx, resume); err != nil {
		t.Fatalf("apply resume: %v", err)
	}
	if got := len(harness.host.dispatches); got != 2 {
		t.Fatalf("dispatches after resume = %d, want 2", got)
	}
	newDispatch := harness.host.dispatches[1]
	if newDispatch.Invocation.ID == oldInvocation || newDispatch.CheckpointRef != "checkpoint-1" || newDispatch.Invocation.Generation != 2 {
		t.Fatalf("resume did not create fresh checkpoint-bound invocation: %#v", newDispatch)
	}
}

func TestStopRejectsIncompleteResumableEvidence(t *testing.T) {
	ctx := context.Background()
	harness := newHarness(t)
	harness.host.stopResult = StopResult{ResumeStateRef: "resume-state-1", CheckpointRef: "checkpoint-1"}
	controller := harness.controller()
	if err := controller.Apply(ctx, validCommand("cmd-start", coordination.CommandStart, "binding-1", "lease-1", 1)); err != nil {
		t.Fatalf("apply start: %v", err)
	}
	if err := controller.Apply(ctx, validCommand("cmd-stop", coordination.CommandStop, "binding-1", "lease-1", 1)); !kernel.IsCode(err, kernel.CodeIncompleteStopEvidence) {
		t.Fatalf("incomplete resumable stop = %v, want incomplete_stop_evidence", err)
	}
	if got := len(harness.recovery.stops); got != 0 {
		t.Fatalf("recorded incomplete stop evidence count = %d, want 0", got)
	}
}

func TestHardStopNonResumableMakesResumeStaleCheckpoint(t *testing.T) {
	ctx := context.Background()
	harness := newHarness(t)
	harness.host.stopResult = StopResult{NonResumable: true, WorkspaceRevision: "main-rev-hard"}
	controller := harness.controller()
	if err := controller.Apply(ctx, validCommand("cmd-start", coordination.CommandStart, "binding-1", "lease-1", 1)); err != nil {
		t.Fatalf("apply start: %v", err)
	}
	if err := controller.Apply(ctx, validCommand("cmd-stop", coordination.CommandStop, "binding-1", "lease-1", 1)); err != nil {
		t.Fatalf("hard stop: %v", err)
	}
	harness.binding.current.Generation = 2
	harness.binding.current.BindingRef = "binding-2"
	harness.binding.current.LeaseRef = "lease-2"
	harness.binding.current.NonResumable = true
	if err := controller.Apply(ctx, validCommand("cmd-resume", coordination.CommandResume, "binding-2", "lease-2", 2)); !kernel.IsCode(err, kernel.CodeStaleCheckpoint) {
		t.Fatalf("resume after non-resumable = %v, want stale_checkpoint", err)
	}
}

type testHarness struct {
	store     *fakeInvocationStore
	assembler *fakeAssembler
	binding   *fakeBindingResolver
	inputs    *fakeInputRuntime
	artifacts *fakeArtifactRouter
	host      *fakeHost
	recovery  *fakeRecoveryStore
}

func newHarness(t *testing.T) *testHarness {
	t.Helper()
	return &testHarness{
		store:     newFakeInvocationStore(),
		assembler: &fakeAssembler{},
		binding: &fakeBindingResolver{current: BindingSnapshot{
			ProjectID:           "project-a",
			ActorPrincipalID:    "agent-executor",
			TaskID:              "task-a",
			EndpointID:          "execute",
			Generation:          1,
			BindingRef:          "binding-1",
			LeaseRef:            "lease-1",
			WorkspaceRef:        "workspace-1",
			WorkspaceRevision:   "main-rev-1",
			ContextSliceRef:     "ctx-1",
			ContextSlice:        `{"nodes":[]}`,
			TaskMemoryBufferRef: "mem-1",
			TaskMemoryBuffer:    `{"candidates":[]}`,
			TaskContract:        `{"task":"a"}`,
			PhaseSpec:           `{"phase":"execute"}`,
			Inputs: PhaseInputSet{
				InputRevision: "inputs-1",
				Required: []InputRequirement{{
					InputID:           "plan-output",
					FromEndpoint:      PhaseEndpointRef{TaskID: "task-a", EndpointID: "plan"},
					RequiredArtifacts: []string{"artifact-plan"},
					RequiredBy:        "completion",
				}},
				Pending: []PendingInput{{InputID: "plan-output", FromEndpoint: PhaseEndpointRef{TaskID: "task-a", EndpointID: "plan"}, RequiredBy: "completion"}},
			},
		}},
		inputs: &fakeInputRuntime{next: InputWaitResult{
			InputRevision: "inputs-2",
			Delivered: []InputDelivery{{
				InputID:        "plan-output",
				FromEndpoint:   PhaseEndpointRef{TaskID: "task-a", EndpointID: "plan"},
				PhaseOutputRef: "phase-output-1",
				ArtifactRefs:   []string{"artifact-plan"},
				SourceRevision: "source-rev-1",
			}},
		}},
		artifacts: &fakeArtifactRouter{},
		host:      newFakeHost(),
		recovery:  &fakeRecoveryStore{},
	}
}

func (h *testHarness) controller() *Controller {
	return NewController(Config{
		InvocationStore: h.store,
		Assembler:       h.assembler,
		BindingResolver: h.binding,
		InputRuntime:    h.inputs,
		ArtifactRouter:  h.artifacts,
		Host:            h.host,
		RecoveryStore:   h.recovery,
		Now:             func() time.Time { return time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC) },
	})
}

func validCommand(id string, action coordination.CommandAction, binding kernel.BindingRef, lease kernel.LeaseID, generation int) PhaseCommand {
	return PhaseCommand{
		ID:         id,
		Endpoint:   PhaseEndpointRef{TaskID: "task-a", EndpointID: "execute"},
		Generation: generation,
		BindingRef: binding,
		LeaseRef:   lease,
		Action:     action,
		CauseRef:   "graph-rev-1",
	}
}

func deliveredInputs(revision string) PhaseInputSet {
	return PhaseInputSet{
		InputRevision: revision,
		Required: []InputRequirement{{
			InputID:           "plan-output",
			FromEndpoint:      PhaseEndpointRef{TaskID: "task-a", EndpointID: "plan"},
			RequiredArtifacts: []string{"artifact-plan"},
			RequiredBy:        "completion",
		}},
		Delivered: []InputDelivery{{
			InputID:        "plan-output",
			FromEndpoint:   PhaseEndpointRef{TaskID: "task-a", EndpointID: "plan"},
			PhaseOutputRef: "phase-output-1",
			ArtifactRefs:   []string{"artifact-plan"},
			SourceRevision: "source-rev-1",
		}},
	}
}

func validOutput() PhaseOutput {
	return PhaseOutput{
		Phase:        "execute",
		DeliveryRefs: []string{"workspace/result.txt"},
		ReportRef:    "workspace/report.md",
		EvidenceRefs: []string{"evidence/test.log"},
	}
}

type fakeAssembler struct{}

func (a *fakeAssembler) Assemble(invocation baseruntime.Invocation, data promptcatalog.RenderData) (baseruntime.Assembly, error) {
	invocation.PromptHashes = map[string]string{"shared": "prompt-hash", string(invocation.Role): "role-hash"}
	invocation.SkillHashes = map[string]string{"phase-runtime": "skill-hash"}
	invocation.EffectiveTools = []auth.Tool{auth.ToolRuntimeAwaitInputs, auth.ToolAgentSubmitPhaseOutput}
	if err := invocation.Validate(); err != nil {
		return baseruntime.Assembly{}, err
	}
	return baseruntime.Assembly{
		Invocation: invocation,
		Prompt: promptcatalog.Rendered{
			Text:         data.RuntimeEnvelope + "\n" + data.StartOrResumeInput,
			PromptHashes: invocation.PromptHashes,
			SkillHashes:  invocation.SkillHashes,
			SHA256:       "rendered-hash",
		},
	}, nil
}

type transitionFailure struct {
	from baseruntime.InvocationStatus
	to   baseruntime.InvocationStatus
	err  error
}

type fakeInvocationStore struct {
	base            *baseruntime.MemoryInvocationStore
	failTransitions []transitionFailure
}

func newFakeInvocationStore() *fakeInvocationStore {
	return &fakeInvocationStore{base: baseruntime.NewMemoryInvocationStore()}
}

func (s *fakeInvocationStore) Create(ctx context.Context, invocation baseruntime.Invocation) error {
	return s.base.Create(ctx, invocation)
}

func (s *fakeInvocationStore) Get(ctx context.Context, id kernel.InvocationID) (baseruntime.Invocation, bool, error) {
	return s.base.Get(ctx, id)
}

func (s *fakeInvocationStore) Transition(ctx context.Context, id kernel.InvocationID, from, to baseruntime.InvocationStatus) error {
	for i, failure := range s.failTransitions {
		if failure.from == from && failure.to == to {
			s.failTransitions = append(s.failTransitions[:i], s.failTransitions[i+1:]...)
			return failure.err
		}
	}
	return s.base.Transition(ctx, id, from, to)
}

type fakeBindingResolver struct {
	current          BindingSnapshot
	bypassValidation bool
}

func (r *fakeBindingResolver) Resolve(_ context.Context, command PhaseCommand) (BindingSnapshot, error) {
	if r.bypassValidation {
		return r.current, nil
	}
	if r.current.BindingRef != command.BindingRef || r.current.Generation != command.Generation {
		return BindingSnapshot{}, kernel.StaleBinding("binding no longer matches command")
	}
	if r.current.LeaseRef != command.LeaseRef {
		return BindingSnapshot{}, kernel.LeaseConflict("lease no longer matches command")
	}
	if r.current.TaskID != command.Endpoint.TaskID || r.current.EndpointID != command.Endpoint.EndpointID {
		return BindingSnapshot{}, kernel.StaleBinding("endpoint no longer matches command")
	}
	return r.current, nil
}

func (r *fakeBindingResolver) Refresh(_ context.Context, invocation ActiveInvocation) (BindingSnapshot, error) {
	if r.bypassValidation {
		return r.current, nil
	}
	if r.current.BindingRef != invocation.Command.BindingRef || r.current.Generation != invocation.Command.Generation {
		return BindingSnapshot{}, kernel.StaleBinding("binding no longer matches invocation")
	}
	return r.current, nil
}

type fakeInputRuntime struct {
	next InputWaitResult
}

func (r *fakeInputRuntime) AwaitInputs(_ context.Context, _ ActiveInvocation, _ AwaitInputsRequest) (InputWaitResult, error) {
	return r.next, nil
}

type fakeArtifactRouter struct {
	next    int
	errOnce error
}

func (r *fakeArtifactRouter) Route(_ context.Context, _ ActiveInvocation, ref string) (string, error) {
	if r.errOnce != nil {
		err := r.errOnce
		r.errOnce = nil
		return "", err
	}
	r.next++
	return "artifact-" + string(rune('0'+r.next)), nil
}

type fakeHost struct {
	dispatchErr    error
	revokeErr      error
	dispatchBlock  chan struct{}
	dispatches     []DispatchRequest
	rehydrates     []DispatchRequest
	stops          []StopRequest
	stopResult     StopResult
	activeSessions map[kernel.InvocationID]bool
	suspended      map[kernel.InvocationID]bool
	suspendCalls   map[kernel.InvocationID]int
	revoked        map[kernel.InvocationID]bool
}

func newFakeHost() *fakeHost {
	return &fakeHost{
		stopResult:     StopResult{ResumeStateRef: "resume-state-1", CheckpointRef: "checkpoint-1", WorkspaceRevision: "main-rev-stop"},
		activeSessions: map[kernel.InvocationID]bool{},
		suspended:      map[kernel.InvocationID]bool{},
		suspendCalls:   map[kernel.InvocationID]int{},
		revoked:        map[kernel.InvocationID]bool{},
	}
}

func (h *fakeHost) Dispatch(_ context.Context, req DispatchRequest) error {
	if h.dispatchBlock != nil {
		<-h.dispatchBlock
	}
	if h.dispatchErr != nil {
		return h.dispatchErr
	}
	h.dispatches = append(h.dispatches, req)
	h.activeSessions[req.Invocation.ID] = true
	return nil
}

func (h *fakeHost) Rehydrate(_ context.Context, req DispatchRequest) error {
	h.rehydrates = append(h.rehydrates, req)
	h.suspended[req.Invocation.ID] = false
	return nil
}

func (h *fakeHost) Suspend(_ context.Context, invocationID kernel.InvocationID) error {
	h.suspended[invocationID] = true
	h.suspendCalls[invocationID]++
	delete(h.activeSessions, invocationID)
	return nil
}

func (h *fakeHost) Stop(_ context.Context, req StopRequest) (StopResult, error) {
	h.stops = append(h.stops, req)
	return h.stopResult, nil
}

func (h *fakeHost) Revoke(_ context.Context, invocationID kernel.InvocationID) error {
	if h.revokeErr != nil {
		return h.revokeErr
	}
	h.revoked[invocationID] = true
	delete(h.activeSessions, invocationID)
	return nil
}

type fakeRecoveryStore struct {
	stops []StopResult
}

func (s *fakeRecoveryStore) RecordStopEvidence(_ context.Context, _ ActiveInvocation, result StopResult) error {
	s.stops = append(s.stops, result)
	return nil
}

func (s *fakeRecoveryStore) ValidateResume(_ context.Context, _ PhaseCommand, binding BindingSnapshot) error {
	if binding.CheckpointRef == "" || binding.NonResumable {
		return kernel.Error{Code: kernel.CodeStaleCheckpoint, Message: "checkpoint is not resumable", Recoverable: true}
	}
	return nil
}

func containsTool(tools map[auth.Tool]struct{}, target auth.Tool) bool {
	_, ok := tools[target]
	return ok
}
