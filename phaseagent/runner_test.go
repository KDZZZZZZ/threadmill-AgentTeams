package phaseagent

import (
	"context"
	"errors"
	"testing"
)

type fakeExecutor struct {
	calls     []ExecutionContext
	executeFn func(context.Context, ExecutionContext) error
}

func (e *fakeExecutor) Execute(ctx context.Context, execution ExecutionContext) error {
	e.calls = append(e.calls, execution)
	if e.executeFn != nil {
		return e.executeFn(ctx, execution)
	}
	return nil
}

var _ PhaseExecutor = (*fakeExecutor)(nil)

func TestRunnerRunStartDerivesRoleAndFinishes(t *testing.T) {
	t.Parallel()

	executor := &fakeExecutor{}
	runner := mustNewRunner(t, &mockRuntime{}, executor)
	result, err := runner.RunStart(context.Background(), runnerStartInput("inv-1", 1, "execute"))
	if err != nil {
		t.Fatalf("RunStart: %v", err)
	}
	if result.State != InvocationFinished || len(executor.calls) != 1 {
		t.Fatalf("unexpected result or executor calls: result=%#v calls=%d", result, len(executor.calls))
	}
	call := executor.calls[0]
	if call.Invocation.State != InvocationRunning || call.Role.Phase != PhaseExecute || !call.Role.Capabilities.AllowImplementationWrite {
		t.Fatalf("runner did not derive execute role correctly: %#v", call)
	}
}

func TestRunnerRejectsInvalidStartWithoutCallingExecutor(t *testing.T) {
	t.Parallel()

	executor := &fakeExecutor{}
	runner := mustNewRunner(t, &mockRuntime{}, executor)
	invalid := runnerStartInput("", 1, "plan")
	if _, err := runner.RunStart(context.Background(), invalid); err == nil {
		t.Fatal("invalid start should be rejected")
	}
	if len(executor.calls) != 0 {
		t.Fatalf("executor was called for invalid start: %#v", executor.calls)
	}
}

func TestRunnerMarksExecutorErrorAsFailed(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("executor failed")
	executor := &fakeExecutor{executeFn: func(_ context.Context, _ ExecutionContext) error { return wantErr }}
	runner := mustNewRunner(t, &mockRuntime{}, executor)
	result, err := runner.RunStart(context.Background(), runnerStartInput("inv-1", 1, "plan"))
	if !errors.Is(err, wantErr) || result.State != InvocationFailed {
		t.Fatalf("unexpected failed execution: result=%#v err=%v", result, err)
	}
}

func TestRunnerUsesSharedRoleContract(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		endpointID          string
		implementationWrite bool
		evidenceWrite       bool
	}{
		{"plan", false, false},
		{"execute", true, true},
		{"verify", false, true},
	} {
		role, err := RoleForEndpoint(PhaseEndpointRef{TaskID: "task-1", EndpointID: test.endpointID})
		if err != nil {
			t.Fatalf("RoleForEndpoint(%s): %v", test.endpointID, err)
		}
		if role.Capabilities.AllowImplementationWrite != test.implementationWrite || role.Capabilities.AllowEvidenceWrite != test.evidenceWrite {
			t.Fatalf("wrong capabilities for %s: %#v", test.endpointID, role.Capabilities)
		}
	}
}

func TestFakeExecutorUsesRuntimeOutboundPort(t *testing.T) {
	t.Parallel()

	runtime := &mockRuntime{candidateReceipt: CandidateBufferedReceipt{CandidateID: "candidate-1"}}
	executor := &fakeExecutor{executeFn: func(ctx context.Context, execution ExecutionContext) error {
		if err := execution.Runtime.SubmitRequirement(ctx, Requirement{Text: "add coverage"}); err != nil {
			return err
		}
		if _, err := execution.Runtime.SubmitMemoryCandidate(ctx, MemoryCandidate{Statement: "coverage gap", Kind: "fact", SourceRefs: []string{"test-log"}}); err != nil {
			return err
		}
		return execution.Runtime.ProposeOrchestration(ctx, OrchestrationProposal{
			ProposalID: "proposal-1", FromEndpoint: execution.Invocation.Start.Endpoint,
		})
	}}
	runner := mustNewRunner(t, runtime, executor)
	if _, err := runner.RunStart(context.Background(), runnerStartInput("inv-1", 1, "plan")); err != nil {
		t.Fatalf("RunStart: %v", err)
	}
	if len(runtime.requirements) != 1 || len(runtime.candidateSubmits) != 1 || len(runtime.proposals) != 1 {
		t.Fatalf("runtime outbound calls were not forwarded: %#v", runtime)
	}
}

func TestRunnerRunResumeUsesNewInvocationAndExplicitState(t *testing.T) {
	t.Parallel()

	executor := &fakeExecutor{}
	runner := mustNewRunner(t, &mockRuntime{}, executor)
	old := runnerStartInput("inv-old", 1, "execute")
	resumed := ResumePhaseInput{Start: runnerStartInput("inv-new", 2, "execute"), CheckpointRef: "checkpoint-1"}
	state := &ResumeState{CompletedWork: []string{"edited file"}, NextSafeStep: "run checks"}
	result, err := runner.RunResume(context.Background(), resumed, state)
	if err != nil {
		t.Fatalf("RunResume: %v", err)
	}
	call := executor.calls[0]
	if result.Start.InvocationID == old.InvocationID || result.Start.Generation == old.Generation || call.ResumeState != state {
		t.Fatalf("resume reused old invocation or lost restored state: result=%#v call=%#v", result, call)
	}
}

func TestRunnerAwaitPathDoesNotUseResume(t *testing.T) {
	t.Parallel()

	runtime := &mockRuntime{awaitResult: InputWaitResult{InputRevision: "rev-2"}}
	executor := &fakeExecutor{executeFn: func(ctx context.Context, execution ExecutionContext) error {
		update, err := execution.Runtime.AwaitInputs(ctx, AwaitInputsRequest{})
		if err != nil {
			return err
		}
		merged, err := MergeInputWaitResult(execution.Invocation.Inputs, update)
		if err != nil {
			return err
		}
		if merged.InputRevision != "rev-2" {
			t.Fatalf("await merge did not use latest revision: %#v", merged)
		}
		return nil
	}}
	runner := mustNewRunner(t, runtime, executor)
	start := runnerStartInput("inv-1", 1, "verify")
	result, err := runner.RunStart(context.Background(), start)
	if err != nil {
		t.Fatalf("RunStart: %v", err)
	}
	if result.Start.InvocationID != start.InvocationID || result.Start.Generation != start.Generation || executor.calls[0].ResumeState != nil {
		t.Fatalf("await path incorrectly became a resume: result=%#v call=%#v", result, executor.calls[0])
	}
}

func mustNewRunner(t *testing.T, runtime Runtime, executor PhaseExecutor) *Runner {
	t.Helper()
	runner, err := NewRunner(runtime, executor, &fakeContextReader{}, &fakeContextAgent{})
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	return runner
}

func mustNewRunnerWithContext(t *testing.T, runtime Runtime, executor PhaseExecutor, reader ContextGraphReader, agent ContextAgent) *Runner {
	t.Helper()
	runner, err := NewRunner(runtime, executor, reader, agent)
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	return runner
}

func runnerStartInput(invocationID string, generation int, endpointID string) StartPhaseInput {
	return StartPhaseInput{
		InvocationID: invocationID,
		Endpoint:     PhaseEndpointRef{TaskID: "task-1", EndpointID: endpointID},
		Generation:   generation,
		BindingRef:   "binding-" + invocationID,
	}
}
