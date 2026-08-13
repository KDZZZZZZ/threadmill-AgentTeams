package mcpapi

import (
	"context"
	"encoding/json"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/evidence"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

type evidenceRegisterRequest struct {
	Type        evidence.ArtifactType `json:"type"`
	Path        string                `json:"path,omitempty"`
	ContentType string                `json:"content_type,omitempty"`
	Body        string                `json:"body"`
}

type EvidenceRegistrar interface {
	Register(context.Context, evidence.RegisterArtifact) (evidence.Artifact, error)
}

func EvidenceToolSpec(registrar EvidenceRegistrar) ToolSpec {
	return ToolSpec{ID: auth.ToolEvidenceRegister, Handler: HandlerFunc(func(ctx context.Context, _ auth.Principal, scope auth.BoundScope, payload json.RawMessage) (any, error) {
		var req evidenceRegisterRequest
		if err := decodePayload(payload, &req); err != nil {
			return nil, err
		}
		if req.Type == "" {
			return nil, kernel.InvalidArgument("type is required")
		}
		return registrar.Register(ctx, evidence.RegisterArtifact{
			Type:              req.Type,
			ProjectID:         scope.ProjectID,
			TaskID:            scope.TaskID,
			AgentInvocationID: scope.InvocationID,
			Path:              req.Path,
			ContentType:       req.ContentType,
			Body:              []byte(req.Body),
		})
	})}
}
