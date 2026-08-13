package agentteams

import (
	"context"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/artifacts"
)

// ExecutionEvidence is audit/debugging material collected from an AgentTeams
// task. It is intentionally distinct from phaseagent.PhaseOutput.
type ExecutionEvidence struct {
	ResultRef       artifacts.ArtifactRef   `json:"result_ref,omitempty"`
	DeliverableRefs []artifacts.ArtifactRef `json:"deliverable_refs,omitempty"`
}

// ExecutionEvidenceIngestor accepts untrusted taskflow result paths only after
// validating them against the Runtime-provided workspace mount.
type ExecutionEvidenceIngestor interface {
	IngestExecutionEvidence(context.Context, HostExecutionRequest, TeamHarnessTaskSnapshot) (ExecutionEvidence, error)
}

type ArtifactEvidenceIngestor struct {
	Registrar artifacts.Registrar
	Recorder  artifacts.EventRecorder
}

func (i ArtifactEvidenceIngestor) RecordExecutionFailure(ctx context.Context, request HostExecutionRequest) error {
	if i.Recorder == nil {
		return nil
	}
	return i.Recorder.Record(ctx, artifacts.Event{Type: artifacts.EventAgentTeamsExecutionFailed, TaskID: request.Endpoint.TaskID, InvocationID: request.InvocationID})
}

func (i ArtifactEvidenceIngestor) IngestExecutionEvidence(ctx context.Context, request HostExecutionRequest, result TeamHarnessTaskSnapshot) (ExecutionEvidence, error) {
	owner := artifacts.TrustedOwner{TaskID: request.Endpoint.TaskID, InvocationID: request.InvocationID, WorkspaceRoot: request.Envelope.Workspace.Root, AllowedDirs: request.Envelope.Workspace.AllowedDirs}
	evidence := ExecutionEvidence{}
	if result.ResultPath != "" {
		ref, err := i.Registrar.Register(ctx, artifacts.RegisterRequest{Owner: owner, ControlledPath: result.ResultPath, Kind: artifacts.ArtifactTypeGeneratedReport})
		if err != nil {
			return ExecutionEvidence{}, err
		}
		evidence.ResultRef = ref
	}
	for _, path := range result.Deliverables {
		ref, err := i.Registrar.Register(ctx, artifacts.RegisterRequest{Owner: owner, ControlledPath: path, Kind: artifacts.ArtifactTypeToolOutput})
		if err != nil {
			return ExecutionEvidence{}, err
		}
		evidence.DeliverableRefs = append(evidence.DeliverableRefs, ref)
	}
	if i.Recorder != nil {
		refs := append([]artifacts.ArtifactRef(nil), evidence.DeliverableRefs...)
		if evidence.ResultRef != "" {
			refs = append(refs, evidence.ResultRef)
		}
		if err := i.Recorder.Record(ctx, artifacts.Event{Type: artifacts.EventAgentTeamsExecutionCompleted, TaskID: owner.TaskID, InvocationID: owner.InvocationID, ArtifactRefs: refs}); err != nil {
			return ExecutionEvidence{}, err
		}
	}
	return evidence, nil
}
