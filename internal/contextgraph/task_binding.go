package contextgraph

import (
	"context"
	"fmt"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

func (s *MemoryStore) RegisterTaskSubgraph(ctx context.Context, principal auth.Principal, taskID kernel.TaskID) (TaskContextSubgraphBinding, error) {
	if err := ctx.Err(); err != nil {
		return TaskContextSubgraphBinding{}, err
	}
	if err := requireTool(principal, auth.ToolContextRegisterTaskSubgraph, principal.ProjectID); err != nil {
		return TaskContextSubgraphBinding{}, err
	}
	if taskID == "" {
		taskID = principal.TaskID
	}
	if taskID == "" {
		return TaskContextSubgraphBinding{}, kernel.InvalidArgument("task_id is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.taskResolver == nil {
		return TaskContextSubgraphBinding{}, kernel.InvalidArgument("task endpoint resolver is required")
	}
	exists, err := s.taskResolver.TaskExists(ctx, principal.ProjectID, taskID)
	if err != nil {
		return TaskContextSubgraphBinding{}, err
	}
	if !exists {
		return TaskContextSubgraphBinding{}, kernel.Error{Code: kernel.CodeNotFound, Message: "task not found"}
	}
	key := scopedTask(principal.ProjectID, taskID)
	if binding, ok := s.taskBindings[key]; ok {
		return binding, nil
	}
	subgraphID := fmt.Sprintf("%s-%s-context", principal.ProjectID, taskID)
	if _, exists := s.subgraphs[subgraphID]; exists {
		return TaskContextSubgraphBinding{}, kernel.InvalidGraph("task context subgraph id already exists")
	}
	now := s.now().UTC()
	binding := TaskContextSubgraphBinding{TaskID: string(taskID), SubgraphID: subgraphID}
	s.taskBindings[key] = binding
	s.subgraphs[subgraphID] = SubgraphRecord{
		Subgraph:  ContextSubgraph{ID: subgraphID, Name: string(taskID), Summary: "task context", Revision: 1, Kind: string(SubgraphKindTask)},
		ProjectID: principal.ProjectID,
		TaskID:    taskID,
		CreatedAt: now,
		UpdatedAt: now,
	}
	s.graphRevision++
	s.audit = append(s.audit, AuditEvent{
		ID:        fmt.Sprintf("audit-%d", len(s.audit)+1),
		ProjectID: principal.ProjectID,
		ActorID:   principal.ActorPrincipalID,
		Action:    "context.task_subgraph.register",
		SubjectID: subgraphID,
		CreatedAt: now,
	})
	return binding, nil
}
