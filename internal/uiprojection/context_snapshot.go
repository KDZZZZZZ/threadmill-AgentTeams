package uiprojection

import (
	"context"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/contextgraph"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

func (s *Service) ContextSnapshot(ctx context.Context, principal auth.Principal, projectID kernel.ProjectID) (contextgraph.ContextGraphSnapshot, error) {
	if err := s.requireProject(ctx, principal, projectID); err != nil {
		return contextgraph.ContextGraphSnapshot{}, err
	}
	if s.contextGraph == nil {
		return contextgraph.ContextGraphSnapshot{}, kernel.Error{Code: kernel.CodeInternalError, Message: "Context graph snapshot reader is not configured", Recoverable: true}
	}
	return s.contextGraph.ProjectContextSnapshot(ctx, projectID)
}
