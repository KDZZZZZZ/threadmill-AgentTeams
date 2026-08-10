package integration

import (
	"context"
	"strings"
	"testing"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/evidence"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/mergequeue"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime/phase"
)

func TestVerifierDispatchesLatestMainThroughPhaseRuntime(t *testing.T) {
	req := verifyRequest()
	bindings := &fakeBindings{binding: verifyBinding(req)}
	runtime := &fakeRuntime{}
	revisions := &fakeRevisions{values: []string{"main-2", "main-2"}}
	acceptor := &fakeAcceptor{result: mergequeue.TargetedVerifyResult{Passed: true, EvidenceRefs: []evidence.ArtifactID{"artifact-verify"}}}
	verifier := Verifier{Bindings: bindings, Runtime: runtime, Revisions: revisions, Results: acceptor}

	result, err := verifier.Verify(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Passed || len(result.EvidenceRefs) != 1 {
		t.Fatalf("result = %#v", result)
	}
	if runtime.command.Action != coordination.CommandStart || runtime.command.Endpoint.EndpointID != coordination.EndpointVerify {
		t.Fatalf("phase command = %#v", runtime.command)
	}
	if !strings.Contains(runtime.command.ID, string(req.Candidate.ID)) || runtime.receipt.InvocationID == "" {
		t.Fatalf("targeted verifier did not receive fresh invocation identity: %#v", runtime.receipt)
	}
	if bindings.request.WorkspaceRoot != req.WorkspaceRoot || acceptor.receipt.WorkspaceHead != req.LatestMainRevision {
		t.Fatalf("latest-main authority was not preserved: binding=%#v receipt=%#v", bindings.request, acceptor.receipt)
	}
}

func TestVerifierInvalidatesReceiptWhenMainMovesDuringInvocation(t *testing.T) {
	req := verifyRequest()
	verifier := Verifier{
		Bindings:  &fakeBindings{binding: verifyBinding(req)},
		Runtime:   &fakeRuntime{},
		Revisions: &fakeRevisions{values: []string{"main-2", "main-3"}},
		Results: ResultAcceptorFunc(func(context.Context, phase.OutputReceipt) (mergequeue.TargetedVerifyResult, error) {
			return mergequeue.TargetedVerifyResult{Passed: true, EvidenceRefs: []evidence.ArtifactID{"artifact-verify"}}, nil
		}),
	}

	_, err := verifier.Verify(context.Background(), req)
	if err == nil || !strings.Contains(err.Error(), "main advanced from main-2 to main-3") {
		t.Fatalf("main drift error = %v", err)
	}
}

func TestVerifierRejectsRegistrarBindingThatIsNotTemporaryLatestMain(t *testing.T) {
	req := verifyRequest()
	binding := verifyBinding(req)
	binding.WorkspaceRevision = "old-main"
	runtime := &fakeRuntime{}
	verifier := Verifier{
		Bindings:  &fakeBindings{binding: binding},
		Runtime:   runtime,
		Revisions: &fakeRevisions{values: []string{"main-2"}},
		Results: ResultAcceptorFunc(func(context.Context, phase.OutputReceipt) (mergequeue.TargetedVerifyResult, error) {
			return mergequeue.TargetedVerifyResult{}, nil
		}),
	}

	_, err := verifier.Verify(context.Background(), req)
	if !kernel.IsCode(err, kernel.CodeStaleBinding) {
		t.Fatalf("binding error = %v, want stale_binding", err)
	}
	if runtime.command.ID != "" {
		t.Fatalf("runtime was called with stale binding: %#v", runtime.command)
	}
}

type fakeBindings struct {
	binding phase.BindingSnapshot
	request mergequeue.TargetedVerifyRequest
}

func (f *fakeBindings) RegisterTargetedVerify(_ context.Context, req mergequeue.TargetedVerifyRequest) (phase.BindingSnapshot, error) {
	f.request = req
	return f.binding, nil
}

type fakeRuntime struct {
	command coordination.PhaseCommand
	receipt phase.OutputReceipt
}

func (f *fakeRuntime) Apply(_ context.Context, command coordination.PhaseCommand) error {
	f.command = command
	f.receipt = phase.OutputReceipt{
		Output:        phase.PhaseOutput{Phase: "verify", ReportRef: "artifact-report", EvidenceRefs: []string{"artifact-verify"}},
		InvocationID:  kernel.InvocationID("inv:" + command.ID),
		Endpoint:      command.Endpoint,
		Generation:    command.Generation,
		BindingRef:    command.BindingRef,
		LeaseRef:      command.LeaseRef,
		InputRevision: "inputs-main-2",
		WorkspaceRef:  "C:/tmp/merge-check",
		WorkspaceHead: "main-2",
	}
	return nil
}

func (f *fakeRuntime) OutputByCommand(_ context.Context, commandID string) (phase.OutputReceipt, bool, error) {
	return f.receipt, f.command.ID == commandID, nil
}

type fakeRevisions struct {
	values []string
	index  int
}

func (f *fakeRevisions) CurrentRevision(context.Context, string, string) (string, error) {
	if f.index >= len(f.values) {
		return f.values[len(f.values)-1], nil
	}
	value := f.values[f.index]
	f.index++
	return value, nil
}

type fakeAcceptor struct {
	result  mergequeue.TargetedVerifyResult
	receipt phase.OutputReceipt
}

func (f *fakeAcceptor) AcceptTargetedVerify(_ context.Context, receipt phase.OutputReceipt) (mergequeue.TargetedVerifyResult, error) {
	f.receipt = receipt
	return f.result, nil
}

func verifyRequest() mergequeue.TargetedVerifyRequest {
	return mergequeue.TargetedVerifyRequest{
		Candidate: mergequeue.Candidate{
			ID:               "candidate-a",
			ProjectID:        "project-a",
			TaskID:           "task-a",
			TargetRepository: "C:/repos/project.git",
			TargetBranch:     "main",
		},
		WorkspaceRoot:      "C:/tmp/merge-check",
		LatestMainRevision: "main-2",
	}
}

func verifyBinding(req mergequeue.TargetedVerifyRequest) phase.BindingSnapshot {
	return phase.BindingSnapshot{
		ProjectID:           req.Candidate.ProjectID,
		ActorPrincipalID:    "verifier-runtime",
		TaskID:              req.Candidate.TaskID,
		EndpointID:          coordination.EndpointVerify,
		Generation:          2,
		BindingRef:          "binding-targeted-main-2",
		LeaseRef:            "lease-targeted-main-2",
		WorkspaceRef:        req.WorkspaceRoot,
		WorkspaceRevision:   req.LatestMainRevision,
		ContextSliceRef:     "context-targeted-main-2",
		TaskMemoryBufferRef: "memory-task-a",
		TaskContract:        "contract-a",
		PhaseSpec:           "targeted verify latest main",
		Inputs:              phase.PhaseInputSet{InputRevision: "inputs-main-2"},
	}
}
