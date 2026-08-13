package phaseagent

import (
	"context"
	"encoding/json"
	"testing"
)

type mockHost struct {
	starts  []StartPhaseInput
	stops   []StopPhaseInput
	resumes []ResumePhaseInput
	ack     StopPhaseAck
}

func (m *mockHost) Start(_ context.Context, input StartPhaseInput) error {
	m.starts = append(m.starts, input)
	return nil
}

func (m *mockHost) Stop(_ context.Context, input StopPhaseInput) (StopPhaseAck, error) {
	m.stops = append(m.stops, input)
	return m.ack, nil
}

func (m *mockHost) Resume(_ context.Context, input ResumePhaseInput) error {
	m.resumes = append(m.resumes, input)
	return nil
}

var _ Host = (*mockHost)(nil)

func TestFreshStartEntersRunning(t *testing.T) {
	t.Parallel()

	start := validStartInput("inv-1", 1)
	session, err := NewInvocationContext(start)
	if err != nil {
		t.Fatalf("NewInvocationContext: %v", err)
	}
	if session.State != InvocationRunning || session.Start.InvocationID != "inv-1" {
		t.Fatalf("unexpected fresh session: %#v", session)
	}

	host := &mockHost{}
	if err := host.Start(context.Background(), start); err != nil || len(host.starts) != 1 {
		t.Fatalf("Start was not accepted: starts=%#v err=%v", host.starts, err)
	}
}

func TestInvocationStatesDoNotModelPhaseOutputSubmission(t *testing.T) {
	t.Parallel()

	states := []InvocationState{
		InvocationRunning,
		InvocationWaiting,
		InvocationStopping,
		InvocationStopped,
		InvocationFinished,
		InvocationFailed,
	}
	for _, state := range states {
		if state == "submitted" {
			t.Fatal("PhaseOutput submission must not be an InvocationState")
		}
	}
}

func TestMergeInputWaitResult(t *testing.T) {
	t.Parallel()

	current := PhaseInputSet{
		InputRevision: "rev-1",
		Delivered:     []InputDelivery{{InputID: "A", FromEndpoint: PhaseEndpointRef{TaskID: "task-a", EndpointID: "verify"}, PhaseOutputRef: "out-a"}},
		Pending:       []PendingInput{{InputID: "B"}, {InputID: "C"}},
	}
	update := InputWaitResult{
		InputRevision: "rev-2",
		Delivered: []InputDelivery{
			{InputID: "A", FromEndpoint: PhaseEndpointRef{TaskID: "task-a", EndpointID: "verify"}, PhaseOutputRef: "out-a"},
			{InputID: "B", FromEndpoint: PhaseEndpointRef{TaskID: "task-b", EndpointID: "verify"}, PhaseOutputRef: "out-b"},
		},
		Pending:        []PendingInput{{InputID: "C"}},
		TerminalReason: "source_failed",
	}

	merged, err := MergeInputWaitResult(current, update)
	if err != nil {
		t.Fatalf("MergeInputWaitResult: %v", err)
	}
	if merged.InputRevision != "rev-2" || len(merged.Delivered) != 2 || merged.Delivered[1].InputID != "B" {
		t.Fatalf("deliveries were not merged: %#v", merged)
	}
	if len(merged.Pending) != 1 || merged.Pending[0].InputID != "C" {
		t.Fatalf("pending was not replaced: %#v", merged.Pending)
	}
}

func TestMergeInputWaitResultRejectsConflictingDelivery(t *testing.T) {
	t.Parallel()

	current := PhaseInputSet{Delivered: []InputDelivery{{InputID: "A", FromEndpoint: PhaseEndpointRef{TaskID: "task-a", EndpointID: "verify"}}}}
	update := InputWaitResult{Delivered: []InputDelivery{{InputID: "A", FromEndpoint: PhaseEndpointRef{TaskID: "other-task", EndpointID: "verify"}}}}
	if _, err := MergeInputWaitResult(current, update); err == nil {
		t.Fatal("conflicting source delivery should be rejected")
	}
}

func TestWaitingAndCheckpointResumeRemainDistinct(t *testing.T) {
	t.Parallel()

	waiting := InvocationContext{Start: validStartInput("inv-1", 1), State: InvocationWaiting}
	resume := ResumePhaseInput{Start: validStartInput("inv-2", 2), CheckpointRef: "checkpoint-1"}
	if waiting.Start.InvocationID == resume.Start.InvocationID || waiting.Start.Generation == resume.Start.Generation {
		t.Fatal("checkpoint resume must use a new invocation and generation")
	}
	if waiting.State != InvocationWaiting || resume.CheckpointRef == "" {
		t.Fatal("waiting and checkpoint resume were not represented separately")
	}
}

func TestStopAckAndResumeStateJSON(t *testing.T) {
	t.Parallel()

	state := ResumeState{
		CompletedWork:    []string{"validated input A"},
		PendingWork:      []string{"summarize B"},
		ConsumedInputIDs: []string{"A"},
		NextSafeStep:     "summarize the delivered review",
	}
	encoded, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("Marshal ResumeState: %v", err)
	}
	var got ResumeState
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatalf("Unmarshal ResumeState: %v", err)
	}
	if got.NextSafeStep != state.NextSafeStep || len(got.ConsumedInputIDs) != 1 {
		t.Fatalf("resume state round trip lost data: %#v", got)
	}

	host := &mockHost{ack: StopPhaseAck{ResumeStateRef: "resume-state-artifact"}}
	ack, err := host.Stop(context.Background(), StopPhaseInput{InvocationID: "inv-1", CommandID: "stop-1", Reason: "held"})
	if err != nil || ack.ResumeStateRef != "resume-state-artifact" {
		t.Fatalf("unexpected stop acknowledgement: %#v, %v", ack, err)
	}
}

func TestPhaseCapabilities(t *testing.T) {
	t.Parallel()

	plan, err := CapabilitiesFor(PhasePlan)
	if err != nil {
		t.Fatalf("plan capabilities: %v", err)
	}
	execute, err := CapabilitiesFor(PhaseExecute)
	if err != nil {
		t.Fatalf("execute capabilities: %v", err)
	}
	verify, err := CapabilitiesFor(PhaseVerify)
	if err != nil {
		t.Fatalf("verify capabilities: %v", err)
	}
	if plan.AllowImplementationWrite || !plan.AllowStructuredArtifactWrite {
		t.Fatalf("unexpected plan policy: %#v", plan)
	}
	if !execute.AllowImplementationWrite {
		t.Fatalf("execute must allow implementation writes: %#v", execute)
	}
	if verify.AllowImplementationWrite || !verify.AllowEvidenceWrite {
		t.Fatalf("verify policy must preserve candidate implementation: %#v", verify)
	}
}

func validStartInput(invocationID string, generation int) StartPhaseInput {
	return StartPhaseInput{
		InvocationID: invocationID,
		Endpoint:     PhaseEndpointRef{TaskID: "task-1", EndpointID: "execute"},
		Generation:   generation,
		BindingRef:   "binding-" + invocationID,
	}
}
