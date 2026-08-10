package mergequeue

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/evidence"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

type MemoryStore struct {
	mu         sync.Mutex
	candidates map[CandidateID]Candidate
	repoClaims map[string]Claim
	audits     map[kernel.IdempotencyKey]auditRecord
	auditOrder []kernel.IdempotencyKey
	now        func() time.Time
}

type auditRecord struct {
	StableKey    kernel.IdempotencyKey
	Type         string
	ProjectID    kernel.ProjectID
	TaskID       kernel.TaskID
	WorkspaceRef kernel.BindingRef
	Payload      map[string]string
	ArtifactRefs []evidence.ArtifactID
	Delivered    bool
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		candidates: make(map[CandidateID]Candidate),
		repoClaims: make(map[string]Claim),
		audits:     make(map[kernel.IdempotencyKey]auditRecord),
		now:        time.Now,
	}
}

func (s *MemoryStore) enqueue(ctx context.Context, req EnqueueRequest, audit auditRecord) (Candidate, error) {
	if err := ctx.Err(); err != nil {
		return Candidate{}, err
	}
	normalizedTarget, err := normalizeTargetRepository(req.TargetRepository)
	if err != nil {
		return Candidate{}, err
	}
	req.TargetRepository = normalizedTarget
	if req.TargetBranch == "" {
		req.TargetBranch = "main"
	}
	if err := validateEnqueue(req); err != nil {
		return Candidate{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.candidates[req.ID]; ok {
		if sameEnqueue(existing, req) {
			if err := s.appendAuditLocked(audit); err != nil {
				return Candidate{}, err
			}
			return cloneCandidate(existing), nil
		}
		return Candidate{}, kernel.IdempotencyConflict()
	}
	now := s.now().UTC()
	candidate := Candidate{
		ID:                req.ID,
		ProjectID:         req.ProjectID,
		TaskID:            req.TaskID,
		WorkspaceRef:      req.WorkspaceRef,
		VerifyResultRef:   req.VerifyResultRef,
		DiffArtifactRef:   req.DiffArtifactRef,
		TargetRepository:  req.TargetRepository,
		TargetBranch:      req.TargetBranch,
		BaseRevision:      req.BaseRevision,
		MainRevision:      req.MainRevision,
		CandidateRevision: req.CandidateRevision,
		Status:            StatusQueued,
		EvidenceRefs:      dedupeEvidence(append([]evidence.ArtifactID{req.VerifyResultRef, req.DiffArtifactRef}, req.EvidenceRefs...)),
		CreatedAt:         now,
		UpdatedAt:         now,
	}
	if err := s.appendAuditLocked(audit); err != nil {
		return Candidate{}, err
	}
	s.candidates[candidate.ID] = candidate
	return cloneCandidate(candidate), nil
}

func (s *MemoryStore) Get(ctx context.Context, id CandidateID) (Candidate, error) {
	if err := ctx.Err(); err != nil {
		return Candidate{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	candidate, ok := s.candidates[id]
	if !ok {
		return Candidate{}, kernel.Error{Code: kernel.CodeNotFound, Message: "merge candidate not found"}
	}
	return cloneCandidate(candidate), nil
}

func (s *MemoryStore) ClaimNext(ctx context.Context, targetRepository string) (Claim, bool, error) {
	if err := ctx.Err(); err != nil {
		return Claim{}, false, err
	}
	var err error
	targetRepository, err = normalizeTargetRepository(targetRepository)
	if err != nil {
		return Claim{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, claimed := s.repoClaims[targetRepository]; claimed {
		return Claim{}, false, nil
	}
	queued := make([]Candidate, 0)
	for _, candidate := range s.candidates {
		if candidate.TargetRepository == targetRepository && (candidate.Status == StatusQueued || candidate.Status == StatusMergeCheck || candidate.Status == StatusTargetedVerify) {
			queued = append(queued, candidate)
		}
	}
	if len(queued) == 0 {
		return Claim{}, false, nil
	}
	sort.Slice(queued, func(i, j int) bool {
		if queued[i].CreatedAt.Equal(queued[j].CreatedAt) {
			return queued[i].ID < queued[j].ID
		}
		return queued[i].CreatedAt.Before(queued[j].CreatedAt)
	})
	candidate := queued[0]
	if candidate.Status == StatusQueued {
		candidate.Status = StatusMergeCheck
		candidate.UpdatedAt = s.now().UTC()
		s.candidates[candidate.ID] = candidate
	}
	claim := Claim{
		Candidate: cloneCandidate(candidate),
		OwnerID:   "memory",
		Token:     "memory:" + string(candidate.ID),
		ExpiresAt: s.now().UTC().Add(time.Hour),
	}
	s.repoClaims[targetRepository] = claim
	return claim, true, nil
}

func (s *MemoryStore) RenewClaim(ctx context.Context, claim Claim) (Claim, error) {
	if err := ctx.Err(); err != nil {
		return Claim{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.repoClaims[claim.Candidate.TargetRepository]
	if !ok || current.Candidate.ID != claim.Candidate.ID || current.Token != claim.Token {
		return Claim{}, kernel.LeaseConflict("merge claim is no longer current")
	}
	candidate, ok := s.candidates[claim.Candidate.ID]
	if !ok {
		return Claim{}, kernel.Error{Code: kernel.CodeNotFound, Message: "merge candidate not found"}
	}
	current.Candidate = cloneCandidate(candidate)
	current.ExpiresAt = s.now().UTC().Add(time.Hour)
	s.repoClaims[claim.Candidate.TargetRepository] = current
	return current, nil
}

func (s *MemoryStore) advance(ctx context.Context, claim Claim, from, to Status, evidenceRefs []evidence.ArtifactID, mergedRevision string) (Candidate, error) {
	if err := ctx.Err(); err != nil {
		return Candidate{}, err
	}
	if !validAdvance(from, to) {
		return Candidate{}, kernel.InvalidArgument("invalid merge candidate transition")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.requireClaimLocked(claim); err != nil {
		return Candidate{}, err
	}
	candidate, ok := s.candidates[claim.Candidate.ID]
	if !ok {
		return Candidate{}, kernel.Error{Code: kernel.CodeNotFound, Message: "merge candidate not found"}
	}
	if candidate.Status != from {
		return Candidate{}, kernel.Error{Code: kernel.CodeTransitionRejected, Message: "merge candidate status changed", Recoverable: true}
	}
	if to == StatusMerged && strings.TrimSpace(mergedRevision) == "" {
		return Candidate{}, kernel.InvalidArgument("merged_revision is required")
	}
	candidate.Status = to
	candidate.EvidenceRefs = dedupeEvidence(append(candidate.EvidenceRefs, evidenceRefs...))
	candidate.MergedRevision = mergedRevision
	candidate.UpdatedAt = s.now().UTC()
	s.candidates[claim.Candidate.ID] = candidate
	return cloneCandidate(candidate), nil
}

func (s *MemoryStore) commitMerged(ctx context.Context, claim Claim, evidenceRefs []evidence.ArtifactID, mergedRevision string, audit auditRecord) (Candidate, error) {
	if err := ctx.Err(); err != nil {
		return Candidate{}, err
	}
	if strings.TrimSpace(mergedRevision) == "" {
		return Candidate{}, kernel.InvalidArgument("merged_revision is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.requireClaimLocked(claim); err != nil {
		return Candidate{}, err
	}
	candidate, ok := s.candidates[claim.Candidate.ID]
	if !ok {
		return Candidate{}, kernel.Error{Code: kernel.CodeNotFound, Message: "merge candidate not found"}
	}
	if candidate.Status == StatusMerged && candidate.MergedRevision == mergedRevision {
		if err := s.appendAuditLocked(audit); err != nil {
			return Candidate{}, err
		}
		return cloneCandidate(candidate), nil
	}
	if candidate.Status != StatusTargetedVerify {
		return Candidate{}, kernel.Error{Code: kernel.CodeTransitionRejected, Message: "merge candidate status changed", Recoverable: true}
	}
	if err := s.appendAuditLocked(audit); err != nil {
		return Candidate{}, err
	}
	candidate.Status = StatusMerged
	candidate.EvidenceRefs = dedupeEvidence(append(candidate.EvidenceRefs, evidenceRefs...))
	candidate.MergedRevision = mergedRevision
	candidate.UpdatedAt = s.now().UTC()
	s.candidates[claim.Candidate.ID] = candidate
	return cloneCandidate(candidate), nil
}

func (s *MemoryStore) fail(ctx context.Context, claim Claim, from Status, reason FailureReason, evidenceRefs []evidence.ArtifactID, audit auditRecord) (Candidate, error) {
	if err := ctx.Err(); err != nil {
		return Candidate{}, err
	}
	if !validFailureReason(reason) || len(evidenceRefs) == 0 {
		return Candidate{}, kernel.InvalidArgument("merge failure requires a valid reason and evidence refs")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.requireClaimLocked(claim); err != nil {
		return Candidate{}, err
	}
	candidate, ok := s.candidates[claim.Candidate.ID]
	if !ok {
		return Candidate{}, kernel.Error{Code: kernel.CodeNotFound, Message: "merge candidate not found"}
	}
	if candidate.Status == StatusFailed && candidate.FailureReason == reason {
		if err := s.appendAuditLocked(audit); err != nil {
			return Candidate{}, err
		}
		return cloneCandidate(candidate), nil
	}
	if candidate.Status != from || (from != StatusMergeCheck && from != StatusTargetedVerify) {
		return Candidate{}, kernel.Error{Code: kernel.CodeTransitionRejected, Message: "merge candidate cannot fail from current status", Recoverable: true}
	}
	candidate.Status = StatusFailed
	candidate.FailureReason = reason
	candidate.EvidenceRefs = dedupeEvidence(append(candidate.EvidenceRefs, evidenceRefs...))
	candidate.UpdatedAt = s.now().UTC()
	if err := s.appendAuditLocked(audit); err != nil {
		return Candidate{}, err
	}
	s.candidates[claim.Candidate.ID] = candidate
	return cloneCandidate(candidate), nil
}

func (s *MemoryStore) pendingAudits(ctx context.Context, projectID kernel.ProjectID, limit int) ([]auditRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]auditRecord, 0)
	for _, key := range s.auditOrder {
		audit := s.audits[key]
		if !audit.Delivered && (projectID == "" || audit.ProjectID == projectID) {
			out = append(out, cloneAudit(audit))
			if limit > 0 && len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func (s *MemoryStore) markAuditDelivered(ctx context.Context, key kernel.IdempotencyKey) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	audit, ok := s.audits[key]
	if !ok {
		return kernel.Error{Code: kernel.CodeNotFound, Message: "merge audit not found"}
	}
	audit.Delivered = true
	s.audits[key] = audit
	return nil
}

func (s *MemoryStore) appendAuditLocked(audit auditRecord) error {
	if audit.StableKey == "" || audit.Type == "" || audit.ProjectID == "" || audit.TaskID == "" || audit.WorkspaceRef == "" {
		return kernel.InvalidArgument("merge audit identity is required")
	}
	audit.ArtifactRefs = dedupeEvidence(audit.ArtifactRefs)
	audit.Payload = cloneStringMap(audit.Payload)
	if existing, ok := s.audits[audit.StableKey]; ok {
		if !equalAudit(existing, audit) {
			return kernel.IdempotencyConflict()
		}
		return nil
	}
	s.audits[audit.StableKey] = cloneAudit(audit)
	s.auditOrder = append(s.auditOrder, audit.StableKey)
	return nil
}

func equalAudit(left, right auditRecord) bool {
	left.Delivered = false
	right.Delivered = false
	leftJSON, _ := json.Marshal(left)
	rightJSON, _ := json.Marshal(right)
	return string(leftJSON) == string(rightJSON)
}

func cloneAudit(audit auditRecord) auditRecord {
	audit.Payload = cloneStringMap(audit.Payload)
	audit.ArtifactRefs = append([]evidence.ArtifactID(nil), audit.ArtifactRefs...)
	return audit
}

func cloneStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]string, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
}

func (s *MemoryStore) ReleaseClaim(ctx context.Context, claim Claim) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.repoClaims[claim.Candidate.TargetRepository]
	if ok && current.Candidate.ID == claim.Candidate.ID && current.Token == claim.Token {
		delete(s.repoClaims, claim.Candidate.TargetRepository)
	}
	return nil
}

func (s *MemoryStore) requireClaimLocked(claim Claim) error {
	current, ok := s.repoClaims[claim.Candidate.TargetRepository]
	if !ok || current.Candidate.ID != claim.Candidate.ID || current.Token != claim.Token {
		return kernel.LeaseConflict("merge claim is no longer current")
	}
	return nil
}

func validateEnqueue(req EnqueueRequest) error {
	if req.ID == "" || req.ProjectID == "" || req.TaskID == "" || req.WorkspaceRef == "" || req.VerifyResultRef == "" || req.DiffArtifactRef == "" || strings.TrimSpace(req.TargetRepository) == "" || strings.TrimSpace(req.BaseRevision) == "" || strings.TrimSpace(req.MainRevision) == "" || strings.TrimSpace(req.CandidateRevision) == "" {
		return kernel.InvalidArgument("merge candidate requires identity, workspace, verify/diff refs, target, and revisions")
	}
	if !validBranch(req.TargetBranch) {
		return kernel.InvalidArgument("target_branch is invalid")
	}
	if !validRevision(req.BaseRevision) || !validRevision(req.MainRevision) || !validRevision(req.CandidateRevision) {
		return kernel.InvalidArgument("merge candidate revisions must be immutable git object IDs")
	}
	return nil
}

func sameEnqueue(candidate Candidate, req EnqueueRequest) bool {
	branch := req.TargetBranch
	if branch == "" {
		branch = "main"
	}
	return candidate.ProjectID == req.ProjectID && candidate.TaskID == req.TaskID && candidate.WorkspaceRef == req.WorkspaceRef && candidate.VerifyResultRef == req.VerifyResultRef && candidate.DiffArtifactRef == req.DiffArtifactRef && candidate.TargetRepository == req.TargetRepository && candidate.TargetBranch == branch && candidate.BaseRevision == req.BaseRevision && candidate.MainRevision == req.MainRevision && candidate.CandidateRevision == req.CandidateRevision && equalEvidence(candidate.EvidenceRefs, dedupeEvidence(append([]evidence.ArtifactID{req.VerifyResultRef, req.DiffArtifactRef}, req.EvidenceRefs...)))
}

func validAdvance(from, to Status) bool {
	return (from == StatusMergeCheck && to == StatusTargetedVerify) || (from == StatusTargetedVerify && to == StatusMerged)
}

func validFailureReason(reason FailureReason) bool {
	return reason == FailureConflict || reason == FailurePermission || reason == FailureMainDrift || reason == FailureVerifyFailed
}

func cloneCandidate(candidate Candidate) Candidate {
	candidate.EvidenceRefs = append([]evidence.ArtifactID(nil), candidate.EvidenceRefs...)
	return candidate
}

func dedupeEvidence(refs []evidence.ArtifactID) []evidence.ArtifactID {
	seen := make(map[evidence.ArtifactID]struct{}, len(refs))
	out := make([]evidence.ArtifactID, 0, len(refs))
	for _, ref := range refs {
		if ref == "" {
			continue
		}
		if _, ok := seen[ref]; ok {
			continue
		}
		seen[ref] = struct{}{}
		out = append(out, ref)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func equalEvidence(left, right []evidence.ArtifactID) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func normalizeTargetRepository(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", kernel.InvalidArgument("target_repository is required")
	}
	abs, err := filepath.Abs(value)
	if err != nil {
		return "", kernel.InvalidArgument("target_repository is invalid")
	}
	resolved := abs
	if evaluated, evalErr := filepath.EvalSymlinks(abs); evalErr == nil {
		resolved = evaluated
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.IsDir() {
		return "", kernel.InvalidArgument("target_repository must be a directory")
	}
	resolved = filepath.Clean(resolved)
	if runtime.GOOS == "windows" {
		resolved = strings.ToLower(resolved)
	}
	return resolved, nil
}

func validBranch(branch string) bool {
	if branch == "" || strings.HasPrefix(branch, "-") || strings.HasSuffix(branch, ".") || strings.HasSuffix(branch, ".lock") || strings.Contains(branch, "..") || strings.ContainsAny(branch, " ~^:?*[\\") || strings.Contains(branch, "//") || strings.HasPrefix(branch, "/") || strings.HasSuffix(branch, "/") {
		return false
	}
	for _, char := range branch {
		if char < 0x20 || char == 0x7f {
			return false
		}
	}
	return true
}

func validRevision(revision string) bool {
	if len(revision) != 40 && len(revision) != 64 {
		return false
	}
	_, err := hex.DecodeString(revision)
	return err == nil
}

var _ Store = (*MemoryStore)(nil)
