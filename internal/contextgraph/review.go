package contextgraph

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

type TaskMemoryFinalizer interface {
	FinalizeTaskMemory(context.Context, auth.Principal, kernel.TaskID) (FrozenCandidateBatch, error)
}

type ContextCandidateReviewer interface {
	SubmitReview(context.Context, auth.Principal, CandidateReviewSubmission) (TaskMemoryReviewReceipt, error)
}

func (s *MemoryStore) FinalizeTaskMemory(ctx context.Context, principal auth.Principal, taskID kernel.TaskID) (FrozenCandidateBatch, error) {
	if err := ctx.Err(); err != nil {
		return FrozenCandidateBatch{}, err
	}
	if err := requireTool(principal, auth.ToolContextFinalizeTaskMemory, principal.ProjectID); err != nil {
		return FrozenCandidateBatch{}, err
	}
	if taskID == "" {
		taskID = principal.TaskID
	}
	if taskID == "" {
		return FrozenCandidateBatch{}, kernel.InvalidArgument("task_id is required")
	}
	if s.taskResolver == nil {
		return FrozenCandidateBatch{}, kernel.InvalidArgument("task endpoint resolver is required")
	}
	done, err := s.taskResolver.TaskDone(ctx, principal.ProjectID, taskID)
	if err != nil {
		return FrozenCandidateBatch{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := scopedTask(principal.ProjectID, taskID)
	if !done {
		s.audit = append(s.audit, AuditEvent{
			ID:        fmt.Sprintf("audit-%d", len(s.audit)+1),
			ProjectID: principal.ProjectID,
			ActorID:   principal.ActorPrincipalID,
			Action:    "context.task_memory.finalize_rejected",
			SubjectID: string(taskID),
			CreatedAt: s.now().UTC(),
		})
		return FrozenCandidateBatch{}, kernel.TransitionRejected("task memory can only be finalized after done")
	}
	state := s.taskMemory[key]
	if state == TaskMemoryReviewed {
		receipt := s.reviewReceipts[key]
		return FrozenCandidateBatch{TaskID: receipt.TaskID, Candidates: candidateView(s.candidates[key]).Candidates}, nil
	}
	if state == "" || state == TaskMemoryOpen {
		s.taskMemory[key] = TaskMemoryFrozenUnreviewed
	}
	return FrozenCandidateBatch{TaskID: string(taskID), Candidates: candidateView(s.candidates[key]).Candidates}, nil
}

func (s *MemoryStore) SubmitReview(ctx context.Context, principal auth.Principal, submission CandidateReviewSubmission) (TaskMemoryReviewReceipt, error) {
	if err := ctx.Err(); err != nil {
		return TaskMemoryReviewReceipt{}, err
	}
	if err := requireContextAgent(principal, auth.ToolContextSubmitReview, principal.ProjectID); err != nil {
		return TaskMemoryReviewReceipt{}, err
	}
	if principal.TaskID == "" {
		return TaskMemoryReviewReceipt{}, kernel.InvalidArgument("task_id is required for candidate review")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := scopedTask(principal.ProjectID, principal.TaskID)
	if s.taskMemory[key] == TaskMemoryReviewed {
		receipt := s.reviewReceipts[key]
		receipt.ReviewedIDs = append([]string(nil), receipt.ReviewedIDs...)
		receipt.NodeIDs = append([]string(nil), receipt.NodeIDs...)
		receipt.RejectedIDs = append([]string(nil), receipt.RejectedIDs...)
		return receipt, nil
	}
	if s.taskMemory[key] != TaskMemoryFrozenUnreviewed {
		return TaskMemoryReviewReceipt{}, kernel.TransitionRejected("task memory batch is not frozen")
	}
	working := s.cloneLocked()
	receipt, err := reviewFrozenBatch(working, s.now().UTC(), principal, submission)
	if err != nil {
		return TaskMemoryReviewReceipt{}, err
	}
	working.taskMemory[key] = TaskMemoryReviewed
	working.reviewReceipts[key] = receipt
	s.replaceLocked(working)
	return receipt, nil
}

func reviewFrozenBatch(working *memoryData, now time.Time, principal auth.Principal, submission CandidateReviewSubmission) (TaskMemoryReviewReceipt, error) {
	key := scopedTask(principal.ProjectID, principal.TaskID)
	records := working.candidates[key]
	if len(submission.Decisions) != len(records) {
		return TaskMemoryReviewReceipt{}, kernel.InvalidArgument("review decisions must cover frozen batch exactly once")
	}
	candidates := make(map[string]CandidateBufferRecord, len(records))
	for _, record := range records {
		candidates[record.CandidateID] = record
	}
	seen := map[string]struct{}{}
	for i, decision := range submission.Decisions {
		if _, ok := seen[decision.CandidateID]; ok {
			return TaskMemoryReviewReceipt{}, kernel.InvalidArgument("duplicate candidate review decision")
		}
		seen[decision.CandidateID] = struct{}{}
		record, ok := candidates[decision.CandidateID]
		if !ok {
			return TaskMemoryReviewReceipt{}, kernel.InvalidArgument("review decision references candidate outside frozen batch")
		}
		decision = normalizeReviewDecision(record, decision)
		if err := validateReviewDecision(principal, working, decision); err != nil {
			return TaskMemoryReviewReceipt{}, err
		}
		submission.Decisions[i] = decision
	}
	var receipt TaskMemoryReviewReceipt
	receipt.TaskID = string(principal.TaskID)
	for _, decision := range submission.Decisions {
		receipt.ReviewedIDs = append(receipt.ReviewedIDs, decision.CandidateID)
		if decision.Action == "reject" {
			receipt.RejectedIDs = append(receipt.RejectedIDs, decision.CandidateID)
			if err := appendMutationEvents(working, now, principal, "context.candidate_review.reject", "context.candidate_review.reject", decision.CandidateID, decision); err != nil {
				return TaskMemoryReviewReceipt{}, err
			}
			continue
		}
		record := candidates[decision.CandidateID]
		nodeID := fmt.Sprintf("review-node-%d", working.nextSeq+1)
		working.nextSeq++
		node := normalizeNode(ContextNode{
			ID:             nodeID,
			Kind:           decision.Kind,
			Statement:      decision.Statement,
			Status:         reviewStatus(decision.Action),
			SubgraphIDs:    decision.SubgraphIDs,
			SourceRefs:     uniqueStrings(append(append([]string(nil), record.Candidate.SourceRefs...), "candidate:"+decision.CandidateID)),
			CreatorAgentID: string(principal.ActorPrincipalID),
		})
		working.nodes[nodeID] = NodeRecord{Node: node, Revision: 1, ProjectID: principal.ProjectID, CreatedAt: now, Sequence: working.nextSeq}
		for _, subgraphID := range node.SubgraphIDs {
			subgraph := working.subgraphs[subgraphID]
			subgraph.Subgraph.Revision++
			subgraph.UpdatedAt = now
			working.subgraphs[subgraphID] = subgraph
		}
		if decision.TargetNodeID != "" {
			target, ok := working.nodes[decision.TargetNodeID]
			if ok && target.ProjectID == principal.ProjectID {
				target.Node.Status = targetStatus(decision.Action)
				target.Revision++
				working.nodes[decision.TargetNodeID] = target
			}
		}
		if err := appendMutationEvents(working, now, principal, "context.candidate_review."+decision.Action, "context.candidate_review."+decision.Action, nodeID, node); err != nil {
			return TaskMemoryReviewReceipt{}, err
		}
		receipt.NodeIDs = append(receipt.NodeIDs, nodeID)
	}
	sort.Strings(receipt.ReviewedIDs)
	sort.Strings(receipt.RejectedIDs)
	working.graphRevision++
	working.appendContextDeltas(principal, lastOutboxID(working), "context.candidate_review.reviewed", reviewChangedSubgraphs(working, receipt.NodeIDs))
	return receipt, nil
}

func reviewChangedSubgraphs(working *memoryData, nodeIDs []string) []string {
	set := map[string]struct{}{}
	for _, nodeID := range nodeIDs {
		record, ok := working.nodes[nodeID]
		if !ok {
			continue
		}
		for _, subgraphID := range record.Node.SubgraphIDs {
			set[subgraphID] = struct{}{}
		}
	}
	return sortedStringSet(set)
}

func validateReviewDecision(principal auth.Principal, working *memoryData, decision CandidateReviewDecision) error {
	switch decision.Action {
	case "create", "revise", "supersede", "dispute", "reject":
	default:
		return kernel.InvalidArgument("review action is not allowed")
	}
	if decision.Action == "reject" {
		return nil
	}
	if err := validateNodeKind(decision.Kind); err != nil {
		return err
	}
	if err := rejectTaskMembership(working.subgraphs, decision.SubgraphIDs); err != nil {
		return err
	}
	for _, subgraphID := range decision.SubgraphIDs {
		subgraph, ok := working.subgraphs[subgraphID]
		if !ok || !canSeeSubgraph(principal, subgraph) || subgraph.Subgraph.Kind != string(SubgraphKindGeneral) {
			return kernel.Error{Code: kernel.CodeNotFound, Message: "general subgraph not found"}
		}
	}
	if decision.Action != "create" {
		if decision.TargetNodeID == "" {
			return kernel.InvalidArgument("target_node_id is required")
		}
		target, ok := working.nodes[decision.TargetNodeID]
		if !ok || target.ProjectID != principal.ProjectID || !canSeeNode(principal, working.subgraphs, target) {
			return kernel.Error{Code: kernel.CodeNotFound, Message: "target node not found"}
		}
		if err := rejectTaskMembership(working.subgraphs, target.Node.SubgraphIDs); err != nil {
			return err
		}
	}
	return nil
}

func normalizeReviewDecision(record CandidateBufferRecord, decision CandidateReviewDecision) CandidateReviewDecision {
	if decision.Statement == "" {
		decision.Statement = record.Candidate.Statement
	}
	if decision.Kind == "" {
		decision.Kind = record.Candidate.Kind
	}
	if len(decision.SubgraphIDs) == 0 {
		decision.SubgraphIDs = append([]string(nil), record.Candidate.SubgraphIDs...)
	}
	return decision
}

func reviewStatus(action string) string {
	if action == "dispute" {
		return string(NodeStatusDisputed)
	}
	return string(NodeStatusAccepted)
}

func targetStatus(action string) string {
	switch action {
	case "supersede":
		return string(NodeStatusSuperseded)
	case "dispute":
		return string(NodeStatusDisputed)
	default:
		return string(NodeStatusOutdated)
	}
}
