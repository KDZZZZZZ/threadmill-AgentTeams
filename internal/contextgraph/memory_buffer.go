package contextgraph

import (
	"context"
	"fmt"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

type TaskMemoryBufferReader interface {
	ListTaskCandidates(context.Context, auth.Principal) (TaskMemoryBufferView, error)
}

type CandidateSubmitter interface {
	SubmitCandidate(context.Context, auth.Principal, SubmitCandidateRequest) (TaskMemoryCandidateView, error)
}

func (s *MemoryStore) ListTaskCandidates(ctx context.Context, principal auth.Principal) (TaskMemoryBufferView, error) {
	if err := ctx.Err(); err != nil {
		return TaskMemoryBufferView{}, err
	}
	if err := requireTool(principal, auth.ToolAgentListTaskMemoryCandidates, principal.ProjectID); err != nil {
		return TaskMemoryBufferView{}, err
	}
	if principal.TaskID == "" {
		return TaskMemoryBufferView{}, kernel.InvalidArgument("task_id is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return candidateView(s.candidates[scopedTask(principal.ProjectID, principal.TaskID)]), nil
}

func (s *MemoryStore) SubmitCandidate(ctx context.Context, principal auth.Principal, req SubmitCandidateRequest) (TaskMemoryCandidateView, error) {
	if err := ctx.Err(); err != nil {
		return TaskMemoryCandidateView{}, err
	}
	if err := requireTool(principal, auth.ToolAgentSubmitMemoryCandidate, principal.ProjectID); err != nil {
		return TaskMemoryCandidateView{}, err
	}
	if principal.TaskID == "" {
		return TaskMemoryCandidateView{}, kernel.InvalidArgument("task_id is required")
	}
	candidate := cloneMemoryCandidate(req.Candidate)
	if err := validateMemoryCandidate(candidate); err != nil {
		return TaskMemoryCandidateView{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := scopedTask(principal.ProjectID, principal.TaskID)
	if state := s.taskMemory[key]; state == TaskMemoryFrozenUnreviewed || state == TaskMemoryReviewed {
		return TaskMemoryCandidateView{}, kernel.TransitionRejected("task memory is frozen")
	}
	if err := rejectTaskMembership(s.subgraphs, candidate.SubgraphIDs); err != nil {
		return TaskMemoryCandidateView{}, err
	}
	id := fmt.Sprintf("candidate-%s-%s-%d", principal.ProjectID, principal.TaskID, len(s.candidates[key])+1)
	record := CandidateBufferRecord{
		CandidateID: id,
		TaskID:      string(principal.TaskID),
		Candidate:   candidate,
		CreationContext: NodeCreationContext{
			CreatorAgentID: string(principal.ActorPrincipalID),
		},
		CreatedByInvocationID: principal.InvocationID,
		CreatedAt:             s.now().UTC(),
	}
	s.candidates[key] = append(s.candidates[key], record)
	return TaskMemoryCandidateView{CandidateID: id, Candidate: cloneMemoryCandidate(candidate)}, nil
}

func validateMemoryCandidate(candidate MemoryCandidate) error {
	if candidate.Statement == "" {
		return kernel.InvalidArgument("candidate statement is required")
	}
	if err := validateNodeKind(candidate.Kind); err != nil {
		return err
	}
	if len(candidate.SourceRefs) == 0 {
		return kernel.InvalidArgument("candidate source_refs are required")
	}
	return nil
}

func candidateView(records []CandidateBufferRecord) TaskMemoryBufferView {
	view := TaskMemoryBufferView{Candidates: make([]TaskMemoryCandidateView, 0, len(records))}
	for _, record := range records {
		view.Candidates = append(view.Candidates, TaskMemoryCandidateView{
			CandidateID: record.CandidateID,
			Candidate:   cloneMemoryCandidate(record.Candidate),
		})
	}
	return view
}
