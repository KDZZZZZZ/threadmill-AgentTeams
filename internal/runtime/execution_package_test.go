package runtime

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/artifacts"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/phaseagent"
)

func TestValidateRehydratedExecutionPackagePreservesVisibleStateAndRejectsPrivateState(t *testing.T) {
	oldDelivery := phaseagent.InputDelivery{InputID: "design", PhaseOutputRef: "phase-output-design", SourceRevision: "r4"}
	newDelivery := phaseagent.InputDelivery{InputID: "review", PhaseOutputRef: "phase-output-review", ArtifactRefs: []string{"artifact-review"}, SourceRevision: "r5"}
	inputs := phaseagent.PhaseInputSet{InputRevision: "r5", Delivered: []phaseagent.InputDelivery{oldDelivery, newDelivery}}
	plan := RehydrationPlan{
		TaskID: "task-a", InvocationID: "invocation-a", Generation: 3, NextExecutionEpoch: 2,
		Endpoint: phaseagent.PhaseEndpointRef{TaskID: "task-a", EndpointID: "execute"}, NewBindingRef: "B2", NewInputRevision: "r5",
		Inputs: inputs, NewlyDelivered: []phaseagent.InputDelivery{newDelivery},
		Context:      RehydratedContext{SliceRef: "context-slice", BaselineRef: "context-baseline"},
		TaskMemory:   RehydratedTaskMemory{View: phaseagent.TaskMemoryBufferView{Candidates: []phaseagent.TaskMemoryCandidateView{{CandidateID: "memory-1"}}}},
		Workspace:    WorkspaceBinding{Ref: "workspace-a", Revision: "workspace-r7", AllowedDirs: []string{"src", "out"}},
		ArtifactRefs: []artifacts.ArtifactRef{"artifact-existing"}, EventRefs: []string{"event-existing"}, EvidenceRefs: []string{"evidence-existing"},
	}
	pkg := RehydratedExecutionPackage{
		TaskID: plan.TaskID, InvocationID: plan.InvocationID, Generation: plan.Generation, ExecutionEpoch: plan.NextExecutionEpoch,
		Endpoint: plan.Endpoint, BindingRef: plan.NewBindingRef, InputRevision: plan.NewInputRevision, Inputs: inputs,
		NewlyDelivered: []phaseagent.InputDelivery{newDelivery}, TaskContract: "implement the task", PhaseInstruction: "continue execute phase",
		Context:    AgentVisibleContext{SliceRef: "context-slice", BaselineRef: "context-baseline", Content: "authorized context"},
		TaskMemory: plan.TaskMemory.View, Workspace: AgentVisibleWorkspace{Ref: "workspace-a", Revision: "workspace-r7", AllowedDirs: []string{"src"}},
		ArtifactRefs: plan.ArtifactRefs, EventRefs: plan.EventRefs, EvidenceRefs: plan.EvidenceRefs,
	}
	if err := ValidateRehydratedExecutionPackage(plan, pkg); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(pkg)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"execution-token-secret", "credential-secret", "Authorization: Bearer", "controller-auth", "cas_revision", "worker-a", "session-a", "hidden reasoning"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("agent-visible package leaked %q: %s", forbidden, encoded)
		}
	}

	invalid := pkg
	invalid.NewlyDelivered = []phaseagent.InputDelivery{{InputID: "invented"}}
	if err := ValidateRehydratedExecutionPackage(plan, invalid); err == nil {
		t.Fatal("package accepted a newly delivered input outside the complete input set")
	}
	invalid = pkg
	invalid.Workspace.AllowedDirs = []string{"src", "secrets"}
	if err := ValidateRehydratedExecutionPackage(plan, invalid); err == nil {
		t.Fatal("package expanded workspace permissions")
	}
}
