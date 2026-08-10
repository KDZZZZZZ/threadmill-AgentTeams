package auth

import "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"

func RequireTool(principal Principal, tool Tool, requested Scope) (BoundScope, error) {
	if principal.ProjectID == "" {
		return BoundScope{}, kernel.Error{Code: kernel.CodeUnauthorized, Message: "principal has no project binding"}
	}
	if principal.Kind != PrincipalAgent {
		return BoundScope{}, kernel.Forbidden("MCP tool requires an agent invocation principal")
	}
	if requested.ProjectID != "" && requested.ProjectID != principal.ProjectID {
		return BoundScope{}, kernel.Forbidden("requested project does not match authenticated principal")
	}
	if requested.TaskID != "" && requested.TaskID != principal.TaskID {
		return BoundScope{}, kernel.Forbidden("requested task does not match authenticated invocation")
	}
	if requested.InvocationID != "" && requested.InvocationID != principal.InvocationID {
		return BoundScope{}, kernel.Forbidden("requested invocation does not match authenticated invocation")
	}
	if !roleAllowsTool(principal.Role, tool) {
		return BoundScope{}, kernel.Forbidden("tool is outside role capability")
	}
	if !principal.HasTool(tool) {
		return BoundScope{}, kernel.Forbidden("tool is outside invocation capability")
	}
	return BoundScope{
		ProjectID:    principal.ProjectID,
		TaskID:       principal.TaskID,
		InvocationID: principal.InvocationID,
	}, nil
}
