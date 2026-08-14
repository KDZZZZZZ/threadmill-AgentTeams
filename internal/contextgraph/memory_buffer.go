package contextgraph

import (
	"context"
	"fmt"
	"regexp"
	"strings"

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
	if err := s.validateCandidateNodeRefsLocked(principal, candidate.SourceRefs); err != nil {
		return TaskMemoryCandidateView{}, err
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

func (s *MemoryStore) validateCandidateNodeRefsLocked(principal auth.Principal, sourceRefs []string) error {
	nodeRefs := candidateNodeRefs(sourceRefs)
	if len(nodeRefs) == 0 {
		return nil
	}
	visible := make(map[string]struct{})
	for _, node := range s.materializeInvocationSliceLocked(principal, consumerInvocationID(principal)).Nodes {
		visible[node.ID] = struct{}{}
	}
	for _, nodeID := range uniqueStrings(nodeRefs) {
		record, ok := s.nodes[nodeID]
		if !ok {
			return kernel.Error{Code: kernel.CodeNotFound, Message: "candidate node_ref is not available in the current context"}
		}
		if _, ok := generalVisibleNode(principal.ProjectID, s.subgraphs, record.Node); !ok {
			return kernel.Error{Code: kernel.CodeNotFound, Message: "candidate node_ref is not available in the current context"}
		}
		if _, ok := visible[nodeID]; !ok {
			return kernel.Error{Code: kernel.CodeNotFound, Message: "candidate node_ref is not available in the current context"}
		}
	}
	return nil
}

func validateMemoryCandidate(candidate MemoryCandidate) error {
	statement := strings.TrimSpace(candidate.Statement)
	if statement == "" {
		return kernel.InvalidArgument("candidate statement is required")
	}
	if obviousRecoverableMemoryPattern.MatchString(statement) || obviousEphemeralMemoryPattern.MatchString(statement) {
		return kernel.InvalidArgument("candidate statement copies a runtime identifier, authoritative binding pointer, or ephemeral execution state")
	}
	if err := validateNodeKind(candidate.Kind); err != nil {
		return err
	}
	if len(candidate.SourceRefs) == 0 {
		return kernel.InvalidArgument("candidate source_refs are required")
	}
	for _, sourceRef := range candidate.SourceRefs {
		if strings.TrimSpace(sourceRef) == "node:" {
			return kernel.InvalidArgument("candidate node source ref requires a node id")
		}
	}
	return nil
}

func candidateNodeRefs(sourceRefs []string) []string {
	var out []string
	for _, sourceRef := range uniqueStrings(sourceRefs) {
		if strings.HasPrefix(sourceRef, "node:") {
			out = append(out, strings.TrimPrefix(sourceRef, "node:"))
		}
	}
	return out
}

// Semantic usefulness is decided by the Context Agent reviewer. This narrow
// ingress guard only catches objective anti-patterns that should never consume
// review capacity or become general knowledge.
var obviousRecoverableMemoryPattern = regexp.MustCompile(`(?i)(?:\binv_[a-z0-9]+\b|\b(?:tm|context)-invocation:[a-z0-9]+\b|\bsubscription(?:_id)?\s*[:=]\s*[a-z0-9._:-]+|\bsub-[a-z0-9._:-]+|\btask-contract:[a-z0-9._:-]+|\bphase-spec:[a-z0-9._:-]+|\bbinding(?:_ref)?\s*[:=]\s*[a-z0-9._:-]+|\bthreadmill-[a-z0-9._-]+-task-\d+\b)`)

var obviousEphemeralMemoryPattern = regexp.MustCompile(`(?i)(?:\bcurrent(?:ly)?\s+(?:queue|queued|waiting|running)\b|当前(?:队列|排队|等待|运行状态)|目前(?:排队|等待|运行))`)

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
