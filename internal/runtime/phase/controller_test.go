package phase

import (
	"context"
	"errors"
	"strings"
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
	harness.now = func() time.Time { return time.Date(2026, 8, 11, 1, 0, 0, 0, time.UTC) }
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

func TestApplyStartObservationFailureRetriesWithoutRedispatch(t *testing.T) {
	ctx := context.Background()
	harness := newHarness(t)
	harness.observations.failStartedOnce = kernel.Error{Code: kernel.CodeRevisionConflict, Message: "observation unavailable", Recoverable: true}
	controller := harness.controller()
	cmd := validCommand("cmd-start-observation-retry", coordination.CommandStart, "binding-1", "lease-1", 1)

	if err := controller.Apply(ctx, cmd); !kernel.IsCode(err, kernel.CodeRevisionConflict) {
		t.Fatalf("apply with observation failure = %v, want revision_conflict", err)
	}
	if got := len(harness.host.dispatches); got != 1 {
		t.Fatalf("dispatches after failed observation = %d, want 1", got)
	}
	if err := controller.Apply(ctx, cmd); err != nil {
		t.Fatalf("retry observation: %v", err)
	}
	if got := len(harness.host.dispatches); got != 1 {
		t.Fatalf("retry redispatched invocation = %d, want 1", got)
	}
	if got := len(harness.observations.started); got != 1 || harness.observations.started[0] != cmd {
		t.Fatalf("started observations = %#v, want one command", harness.observations.started)
	}
}

func TestStopAfterDispatchCrashBeforeRunningTransitionRecoversActiveInvocation(t *testing.T) {
	ctx := context.Background()
	harness := newHarness(t)
	harness.host.dispatchReturnBlock = make(chan struct{})
	first := harness.controller()
	start := validCommand("cmd-start-dispatch-crash", coordination.CommandStart, "binding-1", "lease-1", 1)
	startErr := make(chan error, 1)
	go func() {
		startErr <- first.Apply(ctx, start)
	}()
	for len(harness.host.dispatches) == 0 {
		time.Sleep(time.Millisecond)
	}
	invocationID := harness.host.dispatches[0].Invocation.ID

	rebuilt := harness.controller()
	stop := validCommand("cmd-stop-after-dispatch-crash", coordination.CommandStop, "binding-1", "lease-1", 1)
	if err := rebuilt.Apply(ctx, stop); err != nil {
		t.Fatalf("rebuilt stop after dispatch crash: %v", err)
	}
	if got := len(harness.host.stops); got != 1 {
		t.Fatalf("stop calls = %d, want 1", got)
	}
	stopped, ok, err := harness.store.Get(ctx, invocationID)
	if err != nil || !ok || stopped.Status != baseruntime.InvocationStopped {
		t.Fatalf("stopped invocation = %#v %v, %v; want stopped", stopped, ok, err)
	}

	close(harness.host.dispatchReturnBlock)
	if err := <-startErr; !kernel.IsCode(err, kernel.CodeRevisionConflict) {
		t.Fatalf("original start after recovered stop = %v, want revision_conflict", err)
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

func TestSubmitPhaseOutputEndsInvocationLifecycle(t *testing.T) {
	ctx := context.Background()
	harness := newHarness(t)
	controller := harness.controller()
	cmd := validCommand("cmd-output-lifecycle", coordination.CommandStart, "binding-1", "lease-1", 1)
	if err := controller.Apply(ctx, cmd); err != nil {
		t.Fatalf("apply start: %v", err)
	}
	invocationID := harness.host.dispatches[0].Invocation.ID
	if _, err := controller.AwaitInputs(ctx, invocationID, AwaitInputsRequest{}); err != nil {
		t.Fatalf("await inputs: %v", err)
	}
	if _, err := controller.SubmitPhaseOutput(ctx, invocationID, validOutput()); err != nil {
		t.Fatalf("submit output: %v", err)
	}
	if got := harness.lifecycle.completeCalls[invocationID]; got != 1 {
		t.Fatalf("lifecycle complete calls = %d, want 1", got)
	}
	if got := harness.lifecycle.endCalls[invocationID]; got != 0 {
		t.Fatalf("successful submit ended lifecycle as abort = %d, want 0", got)
	}
	if got := len(harness.observations.outputs); got != 1 || harness.observations.outputs[0] != cmd {
		t.Fatalf("output observations = %#v, want submitted command", harness.observations.outputs)
	}
	if _, ok := harness.recovery.active[invocationID]; ok {
		t.Fatal("completed invocation kept active recovery obligation")
	}
}

func TestSubmitPhaseOutputCompletedObservationFailureRestartsFromRecoveryReceipt(t *testing.T) {
	ctx := context.Background()
	harness := newHarness(t)
	harness.observations.failOutputOnce = kernel.Error{Code: kernel.CodeRevisionConflict, Message: "observation unavailable", Recoverable: true}
	controller := harness.controller()
	cmd := validCommand("cmd-output-observation-restart", coordination.CommandStart, "binding-1", "lease-1", 1)
	if err := controller.Apply(ctx, cmd); err != nil {
		t.Fatalf("apply start: %v", err)
	}
	invocationID := harness.host.dispatches[0].Invocation.ID
	if _, err := controller.AwaitInputs(ctx, invocationID, AwaitInputsRequest{}); err != nil {
		t.Fatalf("await inputs: %v", err)
	}
	if _, err := controller.SubmitPhaseOutput(ctx, invocationID, validOutput()); !kernel.IsCode(err, kernel.CodeRevisionConflict) {
		t.Fatalf("submit with observation failure = %v, want revision_conflict", err)
	}
	completed, ok, err := harness.store.Get(ctx, invocationID)
	if err != nil || !ok || completed.Status != baseruntime.InvocationCompleted {
		t.Fatalf("invocation after observation failure = %#v %v %v, want completed", completed, ok, err)
	}
	routed := harness.artifacts.next
	rebuilt := harness.controller()
	receipt, err := rebuilt.SubmitPhaseOutput(ctx, invocationID, validOutput())
	if err != nil {
		t.Fatalf("retry completed submit from recovery: %v", err)
	}
	if harness.artifacts.next != routed {
		t.Fatalf("completed retry rerouted artifacts: got %d want %d", harness.artifacts.next, routed)
	}
	if receipt.InvocationID != invocationID || receipt.CommandID != cmd.ID {
		t.Fatalf("receipt = %#v, want persisted command receipt", receipt)
	}
	if got := len(harness.observations.outputs); got != 1 || harness.observations.outputs[0] != cmd {
		t.Fatalf("output observations = %#v, want one replay", harness.observations.outputs)
	}
	if _, ok := harness.recovery.active[invocationID]; ok {
		t.Fatal("successful output observation replay kept active recovery obligation")
	}

	if _, err := rebuilt.SubmitPhaseOutput(ctx, invocationID, PhaseOutput{Phase: "execute", ReportRef: "different"}); !kernel.IsCode(err, kernel.CodeIdempotencyConflict) {
		t.Fatalf("completed retry with different payload = %v, want idempotency_conflict", err)
	}
}

func TestApplyStartCompletedOutputReplayCompletesResolvedResourcesBeforeClearing(t *testing.T) {
	ctx := context.Background()
	harness := newHarness(t)
	harness.observations.failOutputOnce = kernel.Error{Code: kernel.CodeRevisionConflict, Message: "observation unavailable", Recoverable: true}
	controller := harness.controller()
	cmd := validCommand("cmd-output-replay-complete-resources", coordination.CommandStart, "binding-1", "lease-1", 1)
	if err := controller.Apply(ctx, cmd); err != nil {
		t.Fatalf("apply start: %v", err)
	}
	invocationID := harness.host.dispatches[0].Invocation.ID
	if _, err := controller.AwaitInputs(ctx, invocationID, AwaitInputsRequest{}); err != nil {
		t.Fatalf("await inputs: %v", err)
	}
	if _, err := controller.SubmitPhaseOutput(ctx, invocationID, validOutput()); !kernel.IsCode(err, kernel.CodeRevisionConflict) {
		t.Fatalf("submit with observation failure = %v, want revision_conflict", err)
	}
	if got := harness.lifecycle.completeCalls[invocationID]; got != 1 {
		t.Fatalf("initial lifecycle complete calls = %d, want 1", got)
	}

	harness.lifecycle.errOnce = errors.New("release unavailable")
	if err := harness.controller().Apply(ctx, cmd); err == nil || err.Error() != "release unavailable" {
		t.Fatalf("completed replay with release failure = %v, want release unavailable", err)
	}
	if _, ok := harness.recovery.active[invocationID]; !ok {
		t.Fatal("release failure cleared active recovery obligation")
	}
	if got := harness.lifecycle.completeCalls[invocationID]; got != 2 {
		t.Fatalf("failed replay lifecycle complete calls = %d, want 2", got)
	}

	if err := harness.controller().Apply(ctx, cmd); err != nil {
		t.Fatalf("retry completed replay: %v", err)
	}
	if _, ok := harness.recovery.active[invocationID]; ok {
		t.Fatal("successful replay kept active recovery obligation")
	}
	if got := harness.lifecycle.completeCalls[invocationID]; got != 3 {
		t.Fatalf("successful replay lifecycle complete calls = %d, want 3", got)
	}
}

func TestSubmitPhaseOutputClearActiveFailureCanRetryCompletedCleanup(t *testing.T) {
	ctx := context.Background()
	harness := newHarness(t)
	harness.recovery.clearErrOnce = errors.New("recovery cleanup unavailable")
	controller := harness.controller()
	cmd := validCommand("cmd-output-clear-retry", coordination.CommandStart, "binding-1", "lease-1", 1)
	if err := controller.Apply(ctx, cmd); err != nil {
		t.Fatalf("apply start: %v", err)
	}
	invocationID := harness.host.dispatches[0].Invocation.ID
	if _, err := controller.AwaitInputs(ctx, invocationID, AwaitInputsRequest{}); err != nil {
		t.Fatalf("await inputs: %v", err)
	}
	if _, err := controller.SubmitPhaseOutput(ctx, invocationID, validOutput()); err == nil {
		t.Fatal("submit unexpectedly succeeded with clear failure")
	}
	completed, ok, err := harness.store.Get(ctx, invocationID)
	if err != nil || !ok || completed.Status != baseruntime.InvocationCompleted {
		t.Fatalf("invocation after clear failure = %#v %v, %v; want completed", completed, ok, err)
	}
	routed := harness.artifacts.next
	if _, err := controller.SubmitPhaseOutput(ctx, invocationID, validOutput()); err != nil {
		t.Fatalf("retry submit after clear failure: %v", err)
	}
	if harness.artifacts.next != routed {
		t.Fatalf("retry rerouted artifacts: got counter %d, want %d", harness.artifacts.next, routed)
	}
	if _, ok := harness.recovery.active[invocationID]; ok {
		t.Fatal("retry left completed active recovery obligation")
	}
}

func TestStopAndSubmitPhaseOutputConcurrentOnlyOneTerminalObservation(t *testing.T) {
	ctx := context.Background()
	harness := newHarness(t)
	controller := harness.controller()
	cmd := validCommand("cmd-terminal-race", coordination.CommandStart, "binding-1", "lease-1", 1)
	if err := controller.Apply(ctx, cmd); err != nil {
		t.Fatalf("apply start: %v", err)
	}
	invocationID := harness.host.dispatches[0].Invocation.ID
	if _, err := controller.AwaitInputs(ctx, invocationID, AwaitInputsRequest{}); err != nil {
		t.Fatalf("await inputs: %v", err)
	}
	stop := validCommand("cmd-terminal-race-stop", coordination.CommandStop, "binding-1", "lease-1", 1)

	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		_, err := controller.SubmitPhaseOutput(ctx, invocationID, validOutput())
		errs <- err
	}()
	go func() {
		defer wg.Done()
		<-start
		errs <- controller.Apply(ctx, stop)
	}()
	close(start)
	wg.Wait()
	close(errs)

	var success, stale int
	for err := range errs {
		switch {
		case err == nil:
			success++
		case kernel.IsCode(err, kernel.CodeStaleCommand), kernel.IsCode(err, kernel.CodeRevisionConflict):
			stale++
		default:
			t.Fatalf("terminal race error = %v, want nil or stale/transition error", err)
		}
	}
	if success != 1 || stale != 1 {
		t.Fatalf("terminal race results: success=%d stale=%d, want one winner and one loser", success, stale)
	}
	if got := len(harness.observations.outputs) + len(harness.observations.stopped); got != 1 {
		t.Fatalf("terminal observations output=%#v stopped=%#v, want exactly one", harness.observations.outputs, harness.observations.stopped)
	}
	if len(harness.observations.outputs) == 1 && len(harness.recovery.stops) != 0 {
		t.Fatalf("output winner also persisted stop evidence: %#v", harness.recovery.stops)
	}
	if len(harness.observations.stopped) == 1 && len(harness.recovery.outputs) != 0 {
		t.Fatalf("stop winner also persisted output receipt: %#v", harness.recovery.outputs)
	}
}

func TestStopWithStaleCompletedRecoveryObligationDoesNotCallHostStop(t *testing.T) {
	ctx := context.Background()
	harness := newHarness(t)
	controller := harness.controller()
	cmd := validCommand("cmd-output-clears-stale", coordination.CommandStart, "binding-1", "lease-1", 1)
	if err := controller.Apply(ctx, cmd); err != nil {
		t.Fatalf("apply start: %v", err)
	}
	invocationID := harness.host.dispatches[0].Invocation.ID
	staleActive := harness.recovery.active[invocationID]
	if _, err := controller.AwaitInputs(ctx, invocationID, AwaitInputsRequest{}); err != nil {
		t.Fatalf("await inputs: %v", err)
	}
	if _, err := controller.SubmitPhaseOutput(ctx, invocationID, validOutput()); err != nil {
		t.Fatalf("submit output: %v", err)
	}
	harness.recovery.active[invocationID] = staleActive

	rebuilt := harness.controller()
	stop := validCommand("cmd-stop-stale-completed", coordination.CommandStop, "binding-1", "lease-1", 1)
	if err := rebuilt.Apply(ctx, stop); !kernel.IsCode(err, kernel.CodeStaleCommand) {
		t.Fatalf("stop with completed stale obligation = %v, want stale_command", err)
	}
	if got := len(harness.host.stops); got != 0 {
		t.Fatalf("host stop calls = %d, want 0", got)
	}
	if _, ok := harness.recovery.active[invocationID]; ok {
		t.Fatal("completed stale obligation was not cleared")
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
	if got := harness.lifecycle.endCalls[oldInvocation]; got != 1 {
		t.Fatalf("old invocation lifecycle end calls = %d, want 1", got)
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
	if newDispatch.Invocation.LeaseID == start.LeaseRef || newDispatch.Invocation.Generation == 1 {
		t.Fatalf("resume reused old lease or generation: %#v", newDispatch.Invocation)
	}
}

func TestStopAfterControllerRebuildRecoversActiveInvocationAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	harness := newHarness(t)
	first := harness.controller()
	if err := first.Apply(ctx, validCommand("cmd-start-rebuild", coordination.CommandStart, "binding-1", "lease-1", 1)); err != nil {
		t.Fatalf("apply start: %v", err)
	}
	invocationID := harness.host.dispatches[0].Invocation.ID

	rebuilt := harness.controller()
	stop := validCommand("cmd-stop-rebuild", coordination.CommandStop, "binding-1", "lease-1", 1)
	if err := rebuilt.Apply(ctx, stop); err != nil {
		t.Fatalf("rebuilt stop: %v", err)
	}
	if got := len(harness.host.stops); got != 1 {
		t.Fatalf("stop calls after rebuilt stop = %d, want 1", got)
	}
	if got := harness.lifecycle.endCalls[invocationID]; got != 1 {
		t.Fatalf("lifecycle end calls after rebuilt stop = %d, want 1", got)
	}

	again := harness.controller()
	if err := again.Apply(ctx, stop); err != nil {
		t.Fatalf("duplicate stop after rebuild: %v", err)
	}
	if got := len(harness.host.stops); got != 1 {
		t.Fatalf("duplicate stop called host again: %d, want 1", got)
	}
	if got := harness.lifecycle.endCalls[invocationID]; got != 1 {
		t.Fatalf("duplicate stop ended lifecycle again: %d, want 1", got)
	}
}

func TestStopEvidenceFailureKeepsRecoveryObligation(t *testing.T) {
	ctx := context.Background()
	harness := newHarness(t)
	controller := harness.controller()
	if err := controller.Apply(ctx, validCommand("cmd-start-evidence-retry", coordination.CommandStart, "binding-1", "lease-1", 1)); err != nil {
		t.Fatalf("apply start: %v", err)
	}
	invocationID := harness.host.dispatches[0].Invocation.ID
	harness.recovery.recordErrOnce = errors.New("event log unavailable")
	stop := validCommand("cmd-stop-evidence-retry", coordination.CommandStop, "binding-1", "lease-1", 1)
	if err := controller.Apply(ctx, stop); err == nil {
		t.Fatal("stop unexpectedly succeeded with evidence failure")
	}
	if harness.host.revoked[invocationID] {
		t.Fatal("evidence failure revoked invocation before recovery obligation was persisted")
	}
	if got := harness.lifecycle.endCalls[invocationID]; got != 0 {
		t.Fatalf("evidence failure ended lifecycle = %d, want 0", got)
	}

	rebuilt := harness.controller()
	if err := rebuilt.Apply(ctx, stop); err != nil {
		t.Fatalf("retry stop after evidence failure: %v", err)
	}
	if got := harness.lifecycle.endCalls[invocationID]; got != 1 {
		t.Fatalf("lifecycle end calls after retry = %d, want 1", got)
	}
}

func TestStopTransitionFailureKeepsRecoveryObligation(t *testing.T) {
	ctx := context.Background()
	harness := newHarness(t)
	harness.store.failTransitions = append(harness.store.failTransitions, transitionFailure{
		from: baseruntime.InvocationRunning,
		to:   baseruntime.InvocationStopped,
		err:  kernel.Error{Code: kernel.CodeRevisionConflict, Message: "transient terminal transition conflict", Recoverable: true},
	})
	controller := harness.controller()
	if err := controller.Apply(ctx, validCommand("cmd-start-transition-retry", coordination.CommandStart, "binding-1", "lease-1", 1)); err != nil {
		t.Fatalf("apply start: %v", err)
	}
	invocationID := harness.host.dispatches[0].Invocation.ID
	stop := validCommand("cmd-stop-transition-retry", coordination.CommandStop, "binding-1", "lease-1", 1)
	if err := controller.Apply(ctx, stop); !kernel.IsCode(err, kernel.CodeRevisionConflict) {
		t.Fatalf("stop with transition failure = %v, want revision_conflict", err)
	}
	if got := len(harness.host.stops); got != 1 {
		t.Fatalf("first stop host calls = %d, want 1", got)
	}
	if got := harness.lifecycle.endCalls[invocationID]; got != 1 {
		t.Fatalf("first stop lifecycle end calls = %d, want 1", got)
	}
	if _, ok := harness.recovery.active[invocationID]; !ok {
		t.Fatal("transition failure removed persistent active recovery obligation")
	}
	rebuilt := harness.controller()
	if err := rebuilt.Apply(ctx, stop); err != nil {
		t.Fatalf("retry stop after transition failure: %v", err)
	}
	if got := len(harness.host.stops); got != 1 {
		t.Fatalf("retry called host stop again: %d, want 1", got)
	}
	if got := harness.lifecycle.endCalls[invocationID]; got != 2 {
		t.Fatalf("retry lifecycle end attempts = %d, want 2 idempotent attempts", got)
	}
	if !harness.host.revoked[invocationID] || !harness.lifecycle.ended[invocationID] {
		t.Fatal("retry did not leave invocation mechanically revoked and ended")
	}
	if _, ok := harness.recovery.active[invocationID]; ok {
		t.Fatal("successful retry kept persistent active recovery obligation")
	}
}

func TestStopObservationFailureRetriesWithoutCallingHostStopAgain(t *testing.T) {
	ctx := context.Background()
	harness := newHarness(t)
	harness.observations.failStoppedOnce = kernel.Error{Code: kernel.CodeRevisionConflict, Message: "observation unavailable", Recoverable: true}
	controller := harness.controller()
	if err := controller.Apply(ctx, validCommand("cmd-start-stop-observation", coordination.CommandStart, "binding-1", "lease-1", 1)); err != nil {
		t.Fatalf("apply start: %v", err)
	}
	invocationID := harness.host.dispatches[0].Invocation.ID
	stop := validCommand("cmd-stop-observation-retry", coordination.CommandStop, "binding-1", "lease-1", 1)
	if err := controller.Apply(ctx, stop); !kernel.IsCode(err, kernel.CodeRevisionConflict) {
		t.Fatalf("stop with observation failure = %v, want revision_conflict", err)
	}
	if got := len(harness.host.stops); got != 1 {
		t.Fatalf("host stop calls after observation failure = %d, want 1", got)
	}
	if _, ok := harness.recovery.active[invocationID]; !ok {
		t.Fatal("observation failure removed active recovery obligation")
	}

	rebuilt := harness.controller()
	if err := rebuilt.Apply(ctx, stop); err != nil {
		t.Fatalf("retry stop observation: %v", err)
	}
	if got := len(harness.host.stops); got != 1 {
		t.Fatalf("retry called host stop again: %d, want 1", got)
	}
	if got := len(harness.observations.stopped); got != 1 || harness.observations.stopped[0].command != stop {
		t.Fatalf("stopped observations = %#v, want one stop command", harness.observations.stopped)
	}
	if harness.observations.stopped[0].checkpointRef != "checkpoint-1" || harness.observations.stopped[0].nonResumable {
		t.Fatalf("stopped evidence = %#v, want checkpoint from stop result", harness.observations.stopped[0])
	}
	if _, ok := harness.recovery.active[invocationID]; ok {
		t.Fatal("retry kept active recovery obligation")
	}
}

func TestStopObservationFailureAfterRestartDoesNotClearActiveBeforeReplay(t *testing.T) {
	ctx := context.Background()
	harness := newHarness(t)
	harness.observations.failStoppedCount = 2
	harness.observations.failStoppedErr = kernel.Error{Code: kernel.CodeRevisionConflict, Message: "observation unavailable", Recoverable: true}
	first := harness.controller()
	if err := first.Apply(ctx, validCommand("cmd-start-stop-observation-twice", coordination.CommandStart, "binding-1", "lease-1", 1)); err != nil {
		t.Fatalf("apply start: %v", err)
	}
	invocationID := harness.host.dispatches[0].Invocation.ID
	stop := validCommand("cmd-stop-observation-twice", coordination.CommandStop, "binding-1", "lease-1", 1)
	if err := first.Apply(ctx, stop); !kernel.IsCode(err, kernel.CodeRevisionConflict) {
		t.Fatalf("first stop observation failure = %v, want revision_conflict", err)
	}
	second := harness.controller()
	if err := second.Apply(ctx, stop); !kernel.IsCode(err, kernel.CodeRevisionConflict) {
		t.Fatalf("second stop observation failure = %v, want revision_conflict", err)
	}
	if _, ok := harness.recovery.active[invocationID]; !ok {
		t.Fatal("active recovery obligation was cleared before stopped observation succeeded")
	}
	third := harness.controller()
	if err := third.Apply(ctx, stop); err != nil {
		t.Fatalf("third stop observation replay: %v", err)
	}
	if got := len(harness.host.stops); got != 1 {
		t.Fatalf("host stop calls = %d, want 1", got)
	}
	if got := len(harness.observations.stopped); got != 1 || harness.observations.stopped[0].command != stop {
		t.Fatalf("stopped observations = %#v, want one successful replay", harness.observations.stopped)
	}
	if _, ok := harness.recovery.active[invocationID]; ok {
		t.Fatal("active recovery obligation survived successful stopped observation")
	}
}

func TestStopRevokeFailureAfterEvidenceRetriesMechanicalEndAndCleanup(t *testing.T) {
	ctx := context.Background()
	harness := newHarness(t)
	harness.host.revokeErrOnce = errors.New("token revocation unavailable")
	controller := harness.controller()
	if err := controller.Apply(ctx, validCommand("cmd-start-revoke-retry", coordination.CommandStart, "binding-1", "lease-1", 1)); err != nil {
		t.Fatalf("apply start: %v", err)
	}
	invocationID := harness.host.dispatches[0].Invocation.ID
	stop := validCommand("cmd-stop-revoke-retry", coordination.CommandStop, "binding-1", "lease-1", 1)
	if err := controller.Apply(ctx, stop); err == nil {
		t.Fatal("stop unexpectedly succeeded with revoke failure")
	}
	if got := len(harness.host.stops); got != 1 {
		t.Fatalf("host stop calls after revoke failure = %d, want 1", got)
	}
	if harness.host.revoked[invocationID] || harness.lifecycle.ended[invocationID] {
		t.Fatal("failed revoke should not mark invocation revoked or ended")
	}
	if _, ok := harness.recovery.active[invocationID]; !ok {
		t.Fatal("revoke failure removed active recovery obligation")
	}

	rebuilt := harness.controller()
	if err := rebuilt.Apply(ctx, stop); err != nil {
		t.Fatalf("retry stop after revoke failure: %v", err)
	}
	stopped, ok, err := harness.store.Get(ctx, invocationID)
	if err != nil || !ok || stopped.Status != baseruntime.InvocationStopped {
		t.Fatalf("invocation after revoke retry = %#v %v, %v; want stopped", stopped, ok, err)
	}
	if got := len(harness.host.stops); got != 1 {
		t.Fatalf("retry called host stop again: %d, want 1", got)
	}
	if !harness.host.revoked[invocationID] || !harness.lifecycle.ended[invocationID] {
		t.Fatal("retry did not complete idempotent revoke/end")
	}
	if _, ok := harness.recovery.active[invocationID]; ok {
		t.Fatal("retry kept active recovery obligation")
	}
}

func TestStopLifecycleEndFailureAfterEvidenceRetriesMechanicalEndAndCleanup(t *testing.T) {
	ctx := context.Background()
	harness := newHarness(t)
	harness.lifecycle.errOnce = errors.New("session termination unavailable")
	controller := harness.controller()
	if err := controller.Apply(ctx, validCommand("cmd-start-end-retry", coordination.CommandStart, "binding-1", "lease-1", 1)); err != nil {
		t.Fatalf("apply start: %v", err)
	}
	invocationID := harness.host.dispatches[0].Invocation.ID
	stop := validCommand("cmd-stop-end-retry", coordination.CommandStop, "binding-1", "lease-1", 1)
	if err := controller.Apply(ctx, stop); err == nil {
		t.Fatal("stop unexpectedly succeeded with lifecycle end failure")
	}
	if got := len(harness.host.stops); got != 1 {
		t.Fatalf("host stop calls after end failure = %d, want 1", got)
	}
	if !harness.host.revoked[invocationID] || harness.lifecycle.ended[invocationID] {
		t.Fatal("end failure should leave revoked true and ended false")
	}
	if _, ok := harness.recovery.active[invocationID]; !ok {
		t.Fatal("end failure removed active recovery obligation")
	}

	rebuilt := harness.controller()
	if err := rebuilt.Apply(ctx, stop); err != nil {
		t.Fatalf("retry stop after lifecycle end failure: %v", err)
	}
	stopped, ok, err := harness.store.Get(ctx, invocationID)
	if err != nil || !ok || stopped.Status != baseruntime.InvocationStopped {
		t.Fatalf("invocation after end retry = %#v %v, %v; want stopped", stopped, ok, err)
	}
	if got := len(harness.host.stops); got != 1 {
		t.Fatalf("retry called host stop again: %d, want 1", got)
	}
	if !harness.host.revoked[invocationID] || !harness.lifecycle.ended[invocationID] {
		t.Fatal("retry did not complete idempotent revoke/end")
	}
	if _, ok := harness.recovery.active[invocationID]; ok {
		t.Fatal("retry kept active recovery obligation")
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
	store        *fakeInvocationStore
	assembler    *fakeAssembler
	binding      *fakeBindingResolver
	inputs       *fakeInputRuntime
	artifacts    *fakeArtifactRouter
	host         *fakeHost
	recovery     *fakeRecoveryStore
	lifecycle    *fakeLifecycle
	observations *fakeObservationWriter
	now          func() time.Time
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
		recovery:  newFakeRecoveryStore(),
		lifecycle: &fakeLifecycle{
			completeCalls: map[kernel.InvocationID]int{},
			completed:     map[kernel.InvocationID]bool{},
			endCalls:      map[kernel.InvocationID]int{},
			ended:         map[kernel.InvocationID]bool{},
		},
		observations: &fakeObservationWriter{},
		now:          func() time.Time { return time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC) },
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
		Lifecycle:       h.lifecycle,
		Observations:    h.observations,
		Now:             h.now,
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

func (s *fakeInvocationStore) GetByLease(ctx context.Context, lease kernel.LeaseID) (baseruntime.Invocation, bool, error) {
	return s.base.GetByLease(ctx, lease)
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
	dispatchErr         error
	revokeErr           error
	revokeErrOnce       error
	dispatchBlock       chan struct{}
	dispatchReturnBlock chan struct{}
	dispatches          []DispatchRequest
	rehydrates          []DispatchRequest
	stops               []StopRequest
	stopResult          StopResult
	activeSessions      map[kernel.InvocationID]bool
	suspended           map[kernel.InvocationID]bool
	suspendCalls        map[kernel.InvocationID]int
	revoked             map[kernel.InvocationID]bool
	revokeCalls         map[kernel.InvocationID]int
}

func newFakeHost() *fakeHost {
	return &fakeHost{
		stopResult:     StopResult{ResumeStateRef: "resume-state-1", CheckpointRef: "checkpoint-1", WorkspaceRevision: "main-rev-stop"},
		activeSessions: map[kernel.InvocationID]bool{},
		suspended:      map[kernel.InvocationID]bool{},
		suspendCalls:   map[kernel.InvocationID]int{},
		revoked:        map[kernel.InvocationID]bool{},
		revokeCalls:    map[kernel.InvocationID]int{},
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
	if h.dispatchReturnBlock != nil {
		<-h.dispatchReturnBlock
	}
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
	h.revokeCalls[invocationID]++
	if h.revokeErrOnce != nil {
		err := h.revokeErrOnce
		h.revokeErrOnce = nil
		return err
	}
	if h.revokeErr != nil {
		return h.revokeErr
	}
	h.revoked[invocationID] = true
	delete(h.activeSessions, invocationID)
	return nil
}

type fakeLifecycle struct {
	completeCalls map[kernel.InvocationID]int
	completed     map[kernel.InvocationID]bool
	endCalls      map[kernel.InvocationID]int
	ended         map[kernel.InvocationID]bool
	errOnce       error
}

func (l *fakeLifecycle) Complete(_ context.Context, invocation baseruntime.Invocation) error {
	l.completeCalls[invocation.ID]++
	if l.errOnce != nil {
		err := l.errOnce
		l.errOnce = nil
		return err
	}
	l.completed[invocation.ID] = true
	return nil
}

func (l *fakeLifecycle) End(_ context.Context, invocation baseruntime.Invocation) error {
	l.endCalls[invocation.ID]++
	if l.errOnce != nil {
		err := l.errOnce
		l.errOnce = nil
		return err
	}
	l.ended[invocation.ID] = true
	return nil
}

type fakeStopObservation struct {
	command       PhaseCommand
	checkpointRef string
	nonResumable  bool
}

type fakeObservationWriter struct {
	failStartedOnce  error
	failOutputOnce   error
	failFailedOnce   error
	failStoppedOnce  error
	failStoppedCount int
	failStoppedErr   error
	started          []PhaseCommand
	outputs          []PhaseCommand
	failed           []PhaseCommand
	stopped          []fakeStopObservation
}

func (w *fakeObservationWriter) RecordPhaseInvocationStarted(_ context.Context, _ kernel.ProjectID, command coordination.PhaseCommand) error {
	if w.failStartedOnce != nil {
		err := w.failStartedOnce
		w.failStartedOnce = nil
		return err
	}
	w.started = append(w.started, command)
	return nil
}

func (w *fakeObservationWriter) RecordPhaseOutputSubmitted(_ context.Context, _ kernel.ProjectID, command coordination.PhaseCommand) error {
	if w.failOutputOnce != nil {
		err := w.failOutputOnce
		w.failOutputOnce = nil
		return err
	}
	w.outputs = append(w.outputs, command)
	return nil
}

func (w *fakeObservationWriter) RecordPhaseInvocationFailed(_ context.Context, _ kernel.ProjectID, command coordination.PhaseCommand) error {
	if w.failFailedOnce != nil {
		err := w.failFailedOnce
		w.failFailedOnce = nil
		return err
	}
	w.failed = append(w.failed, command)
	return nil
}

func (w *fakeObservationWriter) RecordPhaseInvocationStopped(_ context.Context, _ kernel.ProjectID, command coordination.PhaseCommand, checkpointRef string, nonResumable bool) error {
	if w.failStoppedCount > 0 {
		w.failStoppedCount--
		if w.failStoppedErr != nil {
			return w.failStoppedErr
		}
		return kernel.Error{Code: kernel.CodeRevisionConflict, Message: "observation unavailable", Recoverable: true}
	}
	if w.failStoppedOnce != nil {
		err := w.failStoppedOnce
		w.failStoppedOnce = nil
		return err
	}
	w.stopped = append(w.stopped, fakeStopObservation{command: command, checkpointRef: checkpointRef, nonResumable: nonResumable})
	return nil
}

type fakeRecoveryStore struct {
	active        map[kernel.InvocationID]ActiveInvocation
	outputs       map[string]OutputReceipt
	stops         map[string]StopResult
	recordErrOnce error
	clearErrOnce  error
}

func newFakeRecoveryStore() *fakeRecoveryStore {
	return &fakeRecoveryStore{
		active:  map[kernel.InvocationID]ActiveInvocation{},
		outputs: map[string]OutputReceipt{},
		stops:   map[string]StopResult{},
	}
}

func (s *fakeRecoveryStore) RecordActiveInvocation(_ context.Context, active ActiveInvocation) error {
	s.active[active.Invocation.ID] = cloneActiveInvocation(active)
	return nil
}

func (s *fakeRecoveryStore) RecoverActiveInvocation(_ context.Context, command PhaseCommand, _ BindingSnapshot) (ActiveInvocation, bool, error) {
	for _, active := range s.active {
		if active.Command.LeaseRef == command.LeaseRef {
			return cloneActiveInvocation(active), true, nil
		}
	}
	return ActiveInvocation{}, false, nil
}

func (s *fakeRecoveryStore) RecordOutputReceipt(_ context.Context, active ActiveInvocation, receipt OutputReceipt) error {
	key := outputReceiptKey(active.Invocation.ID, active.Command.ID)
	if existing, ok := s.outputs[key]; ok {
		if outputReceiptsEqual(existing, receipt) {
			return nil
		}
		return kernel.Error{Code: kernel.CodeIdempotencyConflict, Message: "output receipt already exists with different payload", Recoverable: false}
	}
	s.outputs[key] = receipt
	return nil
}

func (s *fakeRecoveryStore) GetOutputReceipt(_ context.Context, invocationID kernel.InvocationID, commandID string) (OutputReceipt, bool, error) {
	if commandID != "" {
		receipt, ok := s.outputs[outputReceiptKey(invocationID, commandID)]
		return receipt, ok, nil
	}
	for key, receipt := range s.outputs {
		if strings.HasPrefix(key, string(invocationID)+"/") {
			return receipt, true, nil
		}
	}
	return OutputReceipt{}, false, nil
}

func (s *fakeRecoveryStore) RecordStopEvidence(_ context.Context, active ActiveInvocation, command PhaseCommand, result StopResult) error {
	if s.recordErrOnce != nil {
		err := s.recordErrOnce
		s.recordErrOnce = nil
		return err
	}
	key := stopEvidenceKey(active.Invocation.ID, command.ID)
	if _, ok := s.stops[key]; ok {
		return nil
	}
	s.stops[key] = result
	return nil
}

func (s *fakeRecoveryStore) GetStopEvidence(_ context.Context, invocationID kernel.InvocationID, commandID string) (StopResult, bool, error) {
	result, ok := s.stops[stopEvidenceKey(invocationID, commandID)]
	return result, ok, nil
}

func (s *fakeRecoveryStore) ClearActiveInvocation(_ context.Context, invocationID kernel.InvocationID) error {
	if s.clearErrOnce != nil {
		err := s.clearErrOnce
		s.clearErrOnce = nil
		return err
	}
	delete(s.active, invocationID)
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

func stopEvidenceKey(invocationID kernel.InvocationID, commandID string) string {
	return string(invocationID) + "/" + commandID
}

func outputReceiptKey(invocationID kernel.InvocationID, commandID string) string {
	return string(invocationID) + "/" + commandID
}
