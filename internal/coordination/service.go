package coordination

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

type Service struct {
	principal   auth.Principal
	store       Store
	decisions   DecisionLog
	idempotency kernel.IdempotencyStore
}

func NewTaskManagerGraph(
	principal auth.Principal,
	store Store,
	decisions DecisionLog,
	idempotency kernel.IdempotencyStore,
) *Service {
	if idempotency == nil {
		idempotency = kernel.NewMemoryIdempotencyStore()
	}
	return &Service{
		principal:   principal,
		store:       store,
		decisions:   decisions,
		idempotency: idempotency,
	}
}

func (s *Service) Snapshot(ctx context.Context, revision kernel.Revision) (GraphSnapshot, error) {
	scope, err := s.require(auth.ToolCoordinationSnapshot)
	if err != nil {
		return GraphSnapshot{}, err
	}
	if s.store == nil {
		return GraphSnapshot{}, kernel.Error{Code: kernel.CodeInternalError, Message: "coordination store is not configured", Recoverable: true}
	}
	if revision.IsLatestRead() {
		return s.store.Latest(ctx, scope.ProjectID)
	}
	return s.store.Snapshot(ctx, scope.ProjectID, revision)
}

func (s *Service) ReplacePending(ctx context.Context, next PendingSubgraph) (kernel.Revision, error) {
	scope, err := s.require(auth.ToolCoordinationReplacePending)
	if err != nil {
		return 0, err
	}
	if s.store == nil || s.decisions == nil {
		return 0, kernel.Error{Code: kernel.CodeInternalError, Message: "coordination graph dependencies are not configured", Recoverable: true}
	}
	payload, err := json.Marshal(next)
	if err != nil {
		return 0, kernel.InvalidArgument("pending subgraph must be JSON serializable")
	}
	response, err := s.idempotency.Execute(
		ctx,
		kernel.IDScope(fmt.Sprintf("%s:coordination.replacePending", scope.ProjectID)),
		next.RequestID,
		payload,
		func(ctx context.Context) (kernel.IdempotencyResponse, error) {
			if err := s.decisions.AuthorizeReplacePending(ctx, scope.ProjectID, next.RequestID); err != nil {
				return kernel.IdempotencyResponse{}, err
			}
			updated, err := s.store.ReplacePending(ctx, scope.ProjectID, next)
			if err != nil {
				return kernel.IdempotencyResponse{}, err
			}
			body, err := json.Marshal(updated.Revision)
			if err != nil {
				return kernel.IdempotencyResponse{}, err
			}
			return kernel.IdempotencyResponse{StatusCode: 200, Body: body}, nil
		},
	)
	if err != nil {
		return 0, err
	}
	var revision kernel.Revision
	if err := json.Unmarshal(response.Body, &revision); err != nil {
		return 0, kernel.Error{Code: kernel.CodeInternalError, Message: "stored idempotency response is not a graph revision", Recoverable: true}
	}
	return revision, nil
}

func (s *Service) Transition(ctx context.Context, expectedRevision kernel.Revision, transitionRef string) (kernel.Revision, error) {
	scope, err := s.require(auth.ToolCoordinationTransition)
	if err != nil {
		return 0, err
	}
	if s.store == nil || s.decisions == nil {
		return 0, kernel.Error{Code: kernel.CodeInternalError, Message: "coordination graph dependencies are not configured", Recoverable: true}
	}
	if transitionRef == "" {
		return 0, kernel.InvalidArgument("transitionRef is required")
	}
	transition, err := s.decisions.ResolveTransition(ctx, scope.ProjectID, transitionRef)
	if err != nil {
		return 0, err
	}
	payload, err := json.Marshal(struct {
		ExpectedRevision kernel.Revision `json:"expected_revision"`
		TransitionRef    string          `json:"transition_ref"`
		Transition       GraphTransition `json:"transition"`
	}{ExpectedRevision: expectedRevision, TransitionRef: transitionRef, Transition: transition})
	if err != nil {
		return 0, kernel.InvalidArgument("transition payload must be JSON serializable")
	}
	response, err := s.idempotency.Execute(
		ctx,
		kernel.IDScope(fmt.Sprintf("%s:coordination.transition", scope.ProjectID)),
		kernel.IdempotencyKey(transitionRef),
		payload,
		func(ctx context.Context) (kernel.IdempotencyResponse, error) {
			updated, err := s.store.TransitionWithDecisionRef(ctx, scope.ProjectID, expectedRevision, transitionRef, transition)
			if err != nil {
				return kernel.IdempotencyResponse{}, err
			}
			body, err := json.Marshal(updated.Revision)
			if err != nil {
				return kernel.IdempotencyResponse{}, err
			}
			return kernel.IdempotencyResponse{StatusCode: 200, Body: body}, nil
		},
	)
	if err != nil {
		return 0, err
	}
	var revision kernel.Revision
	if err := json.Unmarshal(response.Body, &revision); err != nil {
		return 0, kernel.Error{Code: kernel.CodeInternalError, Message: "stored idempotency response is not a graph revision", Recoverable: true}
	}
	return revision, nil
}

func (s *Service) require(tool auth.Tool) (auth.BoundScope, error) {
	scope, err := auth.RequireTool(s.principal, tool, auth.Scope{ProjectID: s.principal.ProjectID, TaskID: s.principal.TaskID, InvocationID: s.principal.InvocationID})
	if err != nil {
		return auth.BoundScope{}, err
	}
	if s.principal.Role != auth.RoleTaskManager {
		return auth.BoundScope{}, kernel.Forbidden("coordination graph tools require the canonical Task Manager role")
	}
	return scope, nil
}
