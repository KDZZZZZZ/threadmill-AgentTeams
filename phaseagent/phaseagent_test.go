package phaseagent

import (
	"encoding/json"
	"testing"
)

func TestPhaseValidity(t *testing.T) {
	t.Parallel()

	for _, phase := range []Phase{PhasePlan, PhaseExecute, PhaseVerify} {
		if !phase.Valid() || phase.Validate() != nil {
			t.Errorf("%q should be valid", phase)
		}
	}
	if Phase("review").Valid() || Phase("review").Validate() == nil {
		t.Fatal("review should not be a valid Phase")
	}
}

func TestPhaseEndpointRefValidation(t *testing.T) {
	t.Parallel()

	valid := PhaseEndpointRef{TaskID: "task-1", EndpointID: "execute"}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid endpoint: %v", err)
	}
	invalid := PhaseEndpointRef{TaskID: "task-1", EndpointID: "review"}
	if err := invalid.Validate(); err == nil {
		t.Fatal("invalid endpoint ID should be rejected")
	}
}

func TestStartPhaseInputJSONRoundTrip(t *testing.T) {
	t.Parallel()

	want := StartPhaseInput{
		InvocationID: "inv-1",
		Endpoint:     PhaseEndpointRef{TaskID: "task-1", EndpointID: "execute"},
		Generation:   2,
		BindingRef:   "binding-2",
		Inputs: PhaseInputSet{
			InputRevision: "input-r2",
			Required: []InputRequirement{{
				InputID:           "schema",
				FromEndpoint:      PhaseEndpointRef{TaskID: "schema-task", EndpointID: "verify"},
				RequiredArtifacts: []string{"schema-ref"},
				RequiredBy:        "start",
			}},
			Delivered: []InputDelivery{{
				InputID:        "schema",
				FromEndpoint:   PhaseEndpointRef{TaskID: "schema-task", EndpointID: "verify"},
				PhaseOutputRef: "output-1",
				ArtifactRefs:   []string{"artifact-1"},
				SourceRevision: "r7",
			}},
			Pending: []PendingInput{{
				InputID: "review", FromEndpoint: PhaseEndpointRef{TaskID: "review-task", EndpointID: "verify"}, RequiredBy: "completion",
			}},
		},
	}

	encoded, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var got StartPhaseInput
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.InvocationID != want.InvocationID || got.Endpoint != want.Endpoint || got.Generation != 2 || got.BindingRef != "binding-2" {
		t.Fatalf("round trip lost start fields: %#v", got)
	}
	if got.Inputs.Required[0].FromEndpoint.EndpointID != "verify" || got.Inputs.Delivered[0].FromEndpoint.TaskID != "schema-task" || got.Inputs.Pending[0].FromEndpoint.TaskID != "review-task" {
		t.Fatalf("round trip lost input endpoints: %#v", got.Inputs)
	}
}

func TestStartPhaseInputValidation(t *testing.T) {
	t.Parallel()

	valid := StartPhaseInput{
		InvocationID: "inv-1",
		Endpoint:     PhaseEndpointRef{TaskID: "task-1", EndpointID: "plan"},
		Generation:   1,
		BindingRef:   "binding-1",
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid input: %v", err)
	}

	for _, input := range []StartPhaseInput{
		{Endpoint: valid.Endpoint, Generation: 1, BindingRef: "binding-1"},
		{InvocationID: "inv-1", Endpoint: valid.Endpoint, BindingRef: "binding-1"},
		{InvocationID: "inv-1", Endpoint: valid.Endpoint, Generation: 1},
		{InvocationID: "inv-1", Endpoint: PhaseEndpointRef{TaskID: "task-1", EndpointID: "review"}, Generation: 1, BindingRef: "binding-1"},
	} {
		if err := input.Validate(); err == nil {
			t.Fatalf("invalid StartPhaseInput was accepted: %#v", input)
		}
	}
}

func TestLifecycleDTOJSONRoundTrip(t *testing.T) {
	t.Parallel()

	stop := StopPhaseInput{InvocationID: "inv-1", CommandID: "command-1", Reason: "held"}
	ack := StopPhaseAck{ResumeStateRef: "resume-state-1"}
	resume := ResumePhaseInput{
		Start:         StartPhaseInput{InvocationID: "inv-2", Endpoint: PhaseEndpointRef{TaskID: "task-1", EndpointID: "execute"}, Generation: 2, BindingRef: "binding-2"},
		CheckpointRef: "checkpoint-1",
	}

	for _, value := range []any{stop, ack, resume} {
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatalf("Marshal(%T): %v", value, err)
		}
		if len(encoded) == 0 {
			t.Fatalf("Marshal(%T) returned no JSON", value)
		}
	}
	stopJSON, _ := json.Marshal(stop)
	var gotStop StopPhaseInput
	if err := json.Unmarshal(stopJSON, &gotStop); err != nil {
		t.Fatalf("Unmarshal stop: %v", err)
	}
	if gotStop.CommandID != "command-1" || gotStop.Reason != "held" {
		t.Fatalf("stop round trip lost fields: %#v", gotStop)
	}
	ackJSON, _ := json.Marshal(ack)
	var gotAck StopPhaseAck
	if err := json.Unmarshal(ackJSON, &gotAck); err != nil {
		t.Fatalf("Unmarshal stop ack: %v", err)
	}
	if gotAck.ResumeStateRef != "resume-state-1" {
		t.Fatalf("stop ack round trip lost fields: %#v", gotAck)
	}

	encoded, _ := json.Marshal(resume)
	var got ResumePhaseInput
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatalf("Unmarshal resume: %v", err)
	}
	if got.CheckpointRef != "checkpoint-1" || got.Start.InvocationID != "inv-2" || got.Start.Generation != 2 {
		t.Fatalf("resume round trip lost fields: %#v", got)
	}
}

func TestTaskMemoryDTOJSON(t *testing.T) {
	t.Parallel()

	buffer := TaskMemoryBufferView{Candidates: []TaskMemoryCandidateView{{
		CandidateID: "candidate-1",
		Candidate: MemoryCandidate{
			Statement:   "API v2 rejects empty names",
			Kind:        "fact",
			SourceRefs:  []string{"test-log"},
			SubgraphIDs: []string{"general-api"},
		},
	}}}
	receipt := CandidateBufferedReceipt{CandidateID: "candidate-1"}

	encoded, err := json.Marshal(buffer)
	if err != nil {
		t.Fatalf("Marshal buffer: %v", err)
	}
	var got TaskMemoryBufferView
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatalf("Unmarshal buffer: %v", err)
	}
	if len(got.Candidates) != 1 || got.Candidates[0].CandidateID != receipt.CandidateID || got.Candidates[0].Candidate.SubgraphIDs[0] != "general-api" {
		t.Fatalf("buffer round trip lost candidate data: %#v", got)
	}
	receiptJSON, err := json.Marshal(receipt)
	if err != nil {
		t.Fatalf("Marshal receipt: %v", err)
	}
	var gotReceipt CandidateBufferedReceipt
	if err := json.Unmarshal(receiptJSON, &gotReceipt); err != nil {
		t.Fatalf("Unmarshal receipt: %v", err)
	}
	if gotReceipt.CandidateID != receipt.CandidateID {
		t.Fatalf("receipt round trip lost candidate ID: %#v", gotReceipt)
	}
}

func TestDomainStructureConstruction(t *testing.T) {
	t.Parallel()

	output := PhaseOutput{Phase: PhaseVerify, DeliveryRefs: []string{"verify-result"}, ReportRef: "verify-report", EvidenceRefs: []string{"test-log"}}
	proposal := OrchestrationProposal{
		ProposalID: "proposal-1", ClientRef: "client-1", FromEndpoint: PhaseEndpointRef{TaskID: "task-1", EndpointID: "verify"}, FromInvocationID: "inv-1",
		BasedOnGraphRevision: 3, OrchestrationAdvice: "retry", Rationale: "verification evidence shows a regression", EvidenceRefs: []string{"test-log"},
	}
	requirement := Requirement{Text: "Add compatibility coverage", Constraints: []string{"keep API stable"}}

	if output.Phase != PhaseVerify || proposal.FromEndpoint.EndpointID != "verify" || requirement.Text == "" {
		t.Fatal("constructed domain values were not retained")
	}
}
