package uiprojection

import (
	"context"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

func (s *Service) requireProject(ctx context.Context, principal auth.Principal, projectID kernel.ProjectID) error {
	if err := kernel.RequireID("project_id", projectID); err != nil {
		return err
	}
	if principal.Kind != auth.PrincipalOperator || principal.Role != auth.RoleOperator || principal.ProjectID == "" {
		return kernel.Forbidden("UI projection requires an authenticated operator")
	}
	if principal.ProjectID != projectID {
		return kernel.Forbidden("operator is not allowed for project")
	}
	if s == nil || s.permissions == nil {
		return kernel.Forbidden("UI projection permission reader is not configured")
	}
	allowed, err := s.permissions.CanReadProject(ctx, principal, projectID)
	if err != nil {
		return err
	}
	if !allowed {
		return kernel.Forbidden("operator is not allowed for project")
	}
	return nil
}

func (s *Service) taskGrant(ctx context.Context, principal auth.Principal, projectID kernel.ProjectID, taskID kernel.TaskID) (TaskReadGrant, error) {
	if err := kernel.RequireID("task_id", taskID); err != nil {
		return TaskReadGrant{}, err
	}
	grant, err := s.permissions.TaskGrant(ctx, principal, projectID, taskID)
	if err != nil {
		return TaskReadGrant{}, err
	}
	return grant, nil
}
