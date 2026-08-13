package auth

import (
	"context"
	"net/http"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

const SessionCookieName = "threadmill_session"

type Clock func() time.Time

type Authenticator struct {
	store Store
	now   Clock
}

func NewAuthenticator(store Store, now Clock) *Authenticator {
	if now == nil {
		now = time.Now
	}
	return &Authenticator{store: store, now: now}
}

func (a *Authenticator) IssueOperatorSession(ctx context.Context, actorPrincipalID kernel.ActorPrincipalID, projectIDs []kernel.ProjectID, ttl time.Duration) (string, string, error) {
	if err := validateOperatorSessionRequest(actorPrincipalID, projectIDs, ttl); err != nil {
		return "", "", err
	}
	sessionSecret, sessionHash, err := NewOpaqueSecret()
	if err != nil {
		return "", "", err
	}
	csrfSecret, csrfHash, err := NewOpaqueSecret()
	if err != nil {
		return "", "", err
	}
	projects := make(map[kernel.ProjectID]struct{}, len(projectIDs))
	for _, projectID := range projectIDs {
		projects[projectID] = struct{}{}
	}
	record := SessionRecord{
		SessionHash:      sessionHash,
		ActorPrincipalID: actorPrincipalID,
		ProjectIDs:       projects,
		CSRFHash:         csrfHash,
		ExpiresAt:        a.now().Add(ttl),
	}
	if err := a.store.PutSession(ctx, record); err != nil {
		return "", "", err
	}
	return sessionSecret, csrfSecret, nil
}

func (a *Authenticator) AuthenticateOperatorSession(ctx context.Context, sessionSecret string, projectID kernel.ProjectID) (Principal, SessionRecord, error) {
	record, ok, err := a.store.SessionByHash(ctx, HashOpaqueSecret(sessionSecret))
	if err != nil {
		return Principal{}, SessionRecord{}, err
	}
	if !ok {
		return Principal{}, SessionRecord{}, kernel.Error{Code: kernel.CodeUnauthorized, Message: "session is missing or invalid"}
	}
	if !record.ExpiresAt.After(a.now()) {
		return Principal{}, SessionRecord{}, kernel.Error{Code: kernel.CodeUnauthorized, Message: "session expired"}
	}
	if record.RevokedAt != nil {
		return Principal{}, SessionRecord{}, kernel.Error{Code: kernel.CodeUnauthorized, Message: "session revoked"}
	}
	if _, ok := record.ProjectIDs[projectID]; !ok {
		return Principal{}, SessionRecord{}, kernel.Forbidden("operator is not allowed for project")
	}
	return Principal{
		ActorPrincipalID: record.ActorPrincipalID,
		Kind:             PrincipalOperator,
		ProjectID:        projectID,
		Role:             RoleOperator,
		Tools:            ToolSet(),
		AuthenticatedAt:  a.now(),
	}, record, nil
}

func (a *Authenticator) RevokeOperatorSession(ctx context.Context, sessionSecret string) error {
	return a.store.RevokeSession(ctx, HashOpaqueSecret(sessionSecret), a.now())
}

func (a *Authenticator) IssueAgentToken(ctx context.Context, actorPrincipalID kernel.ActorPrincipalID, capability Capability) (string, error) {
	if err := kernel.RequireID("actor_principal_id", actorPrincipalID); err != nil {
		return "", err
	}
	if err := validateCapability(capability, a.now()); err != nil {
		return "", err
	}
	tokenSecret, tokenHash, err := NewOpaqueSecret()
	if err != nil {
		return "", err
	}
	record := TokenRecord{
		TokenHash:        tokenHash,
		ActorPrincipalID: actorPrincipalID,
		Capability:       capability,
		ExpiresAt:        capability.ExpiresAt,
	}
	if err := a.store.PutToken(ctx, record); err != nil {
		return "", err
	}
	return tokenSecret, nil
}

func (a *Authenticator) AuthenticateAgentToken(ctx context.Context, tokenSecret string) (Principal, error) {
	record, ok, err := a.store.TokenByHash(ctx, HashOpaqueSecret(tokenSecret))
	if err != nil {
		return Principal{}, err
	}
	if !ok {
		return Principal{}, kernel.Error{Code: kernel.CodeUnauthorized, Message: "agent token is missing or invalid"}
	}
	if !record.ExpiresAt.After(a.now()) {
		return Principal{}, kernel.Error{Code: kernel.CodeUnauthorized, Message: "agent token expired"}
	}
	if record.RevokedAt != nil {
		return Principal{}, kernel.Error{Code: kernel.CodeUnauthorized, Message: "agent token revoked"}
	}
	capability := record.Capability
	if err := kernel.RequireID("actor_principal_id", record.ActorPrincipalID); err != nil {
		return Principal{}, kernel.Error{Code: kernel.CodeUnauthorized, Message: "agent token has an invalid principal binding"}
	}
	if err := validateCapability(capability, a.now()); err != nil {
		return Principal{}, err
	}
	return Principal{
		ActorPrincipalID:     record.ActorPrincipalID,
		Kind:                 PrincipalAgent,
		ProjectID:            capability.ProjectID,
		Role:                 capability.Role,
		Operation:            capability.Operation,
		TaskID:               capability.TaskID,
		InvocationID:         capability.InvocationID,
		ConsumerInvocationID: capability.ConsumerInvocationID,
		ConsumerTaskID:       capability.ConsumerTaskID,
		ConsumerRole:         capability.ConsumerRole,
		Tools:                cloneTools(capability.Tools),
		AuthenticatedAt:      a.now(),
	}, nil
}

func (a *Authenticator) RevokeAgentToken(ctx context.Context, tokenSecret string) error {
	return a.store.RevokeToken(ctx, HashOpaqueSecret(tokenSecret), a.now())
}

func SessionCookie(sessionSecret string, expiresAt time.Time) *http.Cookie {
	return &http.Cookie{
		Name:     SessionCookieName,
		Value:    sessionSecret,
		Path:     "/",
		Expires:  expiresAt,
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteStrictMode,
	}
}

func validateCapability(capability Capability, now time.Time) error {
	if err := kernel.RequireID("project_id", capability.ProjectID); err != nil {
		return err
	}
	if err := kernel.RequireID("invocation_id", capability.InvocationID); err != nil {
		return err
	}
	if capability.Role == "" {
		return kernel.InvalidArgument("role is required")
	}
	if !isAgentRole(capability.Role) {
		return kernel.Forbidden("unsupported agent role")
	}
	if capability.Role == RoleContext {
		switch capability.Operation {
		case "retrieve", "curate", "review":
		default:
			return kernel.InvalidArgument("context capability operation must be retrieve, curate, or review")
		}
	} else if capability.Operation != "" {
		return kernel.InvalidArgument("operation is only valid for context capability")
	}
	if (capability.ConsumerInvocationID != "" || capability.ConsumerTaskID != "" || capability.ConsumerRole != "") && (capability.Role != RoleContext || capability.Operation != "retrieve") {
		return kernel.Forbidden("consumer invocation is only valid for context retrieve capability")
	}
	if capability.ConsumerRole != "" && !isAgentRole(capability.ConsumerRole) {
		return kernel.Forbidden("consumer role is not a supported agent role")
	}
	if len(capability.Tools) == 0 {
		return kernel.Forbidden("capability must include at least one tool")
	}
	for tool := range capability.Tools {
		if !roleAllowsTool(capability.Role, tool) {
			return kernel.Forbidden("tool is outside role capability")
		}
		if !operationAllowsTool(capability.Role, capability.Operation, tool) {
			return kernel.Forbidden("tool is outside invocation operation capability")
		}
	}
	if capability.Role.IsPhase() {
		if err := kernel.RequireID("task_id", capability.TaskID); err != nil {
			return kernel.StaleBinding("phase capability must bind a task")
		}
	}
	if !capability.ExpiresAt.After(now) {
		return kernel.Error{Code: kernel.CodeUnauthorized, Message: "capability already expired"}
	}
	return nil
}

func validateOperatorSessionRequest(actorPrincipalID kernel.ActorPrincipalID, projectIDs []kernel.ProjectID, ttl time.Duration) error {
	if err := kernel.RequireID("actor_principal_id", actorPrincipalID); err != nil {
		return err
	}
	if len(projectIDs) == 0 {
		return kernel.Forbidden("operator session must bind at least one project")
	}
	for _, projectID := range projectIDs {
		if err := kernel.RequireID("project_id", projectID); err != nil {
			return err
		}
	}
	if ttl <= 0 {
		return kernel.InvalidArgument("session ttl must be positive")
	}
	return nil
}

func isAgentRole(role Role) bool {
	switch role {
	case RoleTaskManager, RoleContext, RolePlanner, RoleExecutor, RoleVerifier:
		return true
	default:
		return false
	}
}

func roleAllowsTool(role Role, tool Tool) bool {
	switch role {
	case RoleTaskManager:
		switch tool {
		case ToolContextListSubgraphs,
			ToolContextExplore,
			ToolContextSubscribe,
			ToolContextUnsubscribe,
			ToolContextAgentRetrieve,
			ToolCoordinationSnapshot,
			ToolTaskManagerSubmitDecision,
			ToolCoordinationReplacePending,
			ToolCoordinationTransition,
			ToolContextRegisterTaskSubgraph,
			ToolContextProjectTaskContext,
			ToolContextFinalizeTaskMemory:
			return true
		}
	case RoleContext:
		switch tool {
		case ToolContextListSubgraphs,
			ToolContextExplore,
			ToolContextGetSubgraph,
			ToolContextGetNode,
			ToolContextSearch,
			ToolContextCreateNode,
			ToolContextUpdateNode,
			ToolContextDeleteNode,
			ToolContextCreateSubgraph,
			ToolContextUpdateSubgraph,
			ToolContextDeleteSubgraph,
			ToolContextSubmitReview:
			return true
		}
	case RolePlanner:
		switch tool {
		case ToolContextListSubgraphs,
			ToolContextExplore,
			ToolContextSubscribe,
			ToolContextUnsubscribe,
			ToolContextAgentRetrieve,
			ToolRuntimeAwaitInputs,
			ToolAgentProposeOrchestration,
			ToolAgentSubmitRequirement,
			ToolAgentListTaskMemoryCandidates,
			ToolAgentSubmitMemoryCandidate,
			ToolAgentSubmitPhaseOutput,
			ToolWorkspaceList,
			ToolWorkspaceRead,
			ToolWorkspaceWritePlan,
			ToolWorkspaceDiff,
			ToolEvidenceRegister:
			return true
		}
	case RoleExecutor:
		switch tool {
		case ToolContextListSubgraphs,
			ToolContextExplore,
			ToolContextSubscribe,
			ToolContextUnsubscribe,
			ToolContextAgentRetrieve,
			ToolRuntimeAwaitInputs,
			ToolAgentProposeOrchestration,
			ToolAgentSubmitRequirement,
			ToolAgentListTaskMemoryCandidates,
			ToolAgentSubmitMemoryCandidate,
			ToolAgentSubmitPhaseOutput,
			ToolWorkspaceList,
			ToolWorkspaceRead,
			ToolWorkspaceWrite,
			ToolWorkspaceRun,
			ToolWorkspaceDiff,
			ToolEvidenceRegister:
			return true
		}
	case RoleVerifier:
		switch tool {
		case ToolContextListSubgraphs,
			ToolContextExplore,
			ToolContextSubscribe,
			ToolContextUnsubscribe,
			ToolContextAgentRetrieve,
			ToolRuntimeAwaitInputs,
			ToolAgentProposeOrchestration,
			ToolAgentSubmitRequirement,
			ToolAgentListTaskMemoryCandidates,
			ToolAgentSubmitMemoryCandidate,
			ToolAgentSubmitPhaseOutput,
			ToolWorkspaceList,
			ToolWorkspaceRead,
			ToolWorkspaceRun,
			ToolWorkspaceDiff,
			ToolEvidenceRegister:
			return true
		}
	}
	return false
}

// operationAllowsTool narrows the role maximum to the exact invocation
// operation. Non-context roles have a single operation-less bundle.
func operationAllowsTool(role Role, operation string, tool Tool) bool {
	if role != RoleContext {
		return operation == "" && roleAllowsTool(role, tool)
	}
	switch operation {
	case "retrieve":
		switch tool {
		case ToolContextListSubgraphs, ToolContextExplore,
			ToolContextGetSubgraph, ToolContextGetNode, ToolContextSearch:
			return true
		}
	case "curate":
		switch tool {
		case ToolContextListSubgraphs, ToolContextExplore,
			ToolContextGetSubgraph, ToolContextGetNode,
			ToolContextCreateNode, ToolContextUpdateNode, ToolContextDeleteNode,
			ToolContextCreateSubgraph, ToolContextUpdateSubgraph, ToolContextDeleteSubgraph:
			return true
		}
	case "review":
		switch tool {
		case ToolContextListSubgraphs, ToolContextExplore,
			ToolContextGetSubgraph, ToolContextGetNode, ToolContextSearch,
			ToolContextSubmitReview:
			return true
		}
	}
	return false
}
