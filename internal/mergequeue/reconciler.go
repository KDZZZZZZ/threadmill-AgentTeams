package mergequeue

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/evidence"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/workspace"
)

type Reconciler struct {
	store      Store
	workspaces WorkspaceReader
	verifier   TargetedVerifier
	backend    GitBackend
	artifacts  *evidence.ArtifactRegistry
	events     *evidence.EventLog
}

type Store interface {
	enqueue(context.Context, EnqueueRequest, auditRecord) (Candidate, error)
	Get(context.Context, CandidateID) (Candidate, error)
	pendingMergeOperation(context.Context, string) (mergeOperation, bool, error)
	ClaimNext(context.Context, string) (Claim, bool, error)
	RenewClaim(context.Context, Claim) (Claim, error)
	advance(context.Context, Claim, Status, Status, []evidence.ArtifactID, string) (Candidate, error)
	beginMergeOperation(context.Context, Claim, mergeOperationRequest) (mergeOperation, error)
	finalizeMergeOperation(context.Context, mergeOperation) (Candidate, error)
	abortMergeOperation(context.Context, mergeOperation) error
	markMergeOperationRecoveryRequired(context.Context, mergeOperation, string) (Candidate, error)
	fail(context.Context, Claim, Status, FailureReason, []evidence.ArtifactID, auditRecord) (Candidate, error)
	pendingAudits(context.Context, kernel.ProjectID, int) ([]auditRecord, error)
	markAuditDelivered(context.Context, kernel.IdempotencyKey) error
	ReleaseClaim(context.Context, Claim) error
}

type mergeOperation struct {
	ID                     string
	Token                  string
	CandidateID            CandidateID
	TargetRepository       string
	TargetBranch           string
	ExpectedMainRevision   string
	ExpectedMergedRevision string
	Status                 string
	EvidenceRefs           []evidence.ArtifactID
	Audit                  auditRecord
	RecoveryReason         string
}

type mergeOperationRequest struct {
	ExpectedMainRevision   string
	ExpectedMergedRevision string
	EvidenceRefs           []evidence.ArtifactID
	Audit                  auditRecord
}

func NewReconciler(store Store, workspaces WorkspaceReader, verifier TargetedVerifier, backend GitBackend, artifacts *evidence.ArtifactRegistry, events *evidence.EventLog) *Reconciler {
	return &Reconciler{store: store, workspaces: workspaces, verifier: verifier, backend: backend, artifacts: artifacts, events: events}
}

func (r *Reconciler) Enqueue(ctx context.Context, req EnqueueRequest) (Candidate, error) {
	if err := r.ready(); err != nil {
		return Candidate{}, err
	}
	principal := evidence.Principal{Role: evidence.RoleTaskManager, ProjectID: req.ProjectID, TaskID: req.TaskID}
	for _, ref := range dedupeEvidence(append([]evidence.ArtifactID{req.VerifyResultRef, req.DiffArtifactRef}, req.EvidenceRefs...)) {
		if !r.artifacts.CanRead(principal, ref) {
			return Candidate{}, kernel.Forbidden("merge candidate evidence is not readable in task scope")
		}
	}
	audit := auditRecord{
		StableKey:    kernel.IdempotencyKey("merge-candidate:" + string(req.ID) + ":queued"),
		Type:         "MergeCandidateQueued",
		ProjectID:    req.ProjectID,
		TaskID:       req.TaskID,
		WorkspaceRef: req.WorkspaceRef,
		Payload:      map[string]string{"candidate_id": string(req.ID), "main_revision": req.MainRevision},
		ArtifactRefs: dedupeEvidence(append([]evidence.ArtifactID{req.VerifyResultRef, req.DiffArtifactRef}, req.EvidenceRefs...)),
	}
	candidate, err := r.store.enqueue(ctx, req, audit)
	if err != nil {
		return Candidate{}, err
	}
	if err := r.flushAudits(ctx, req.ProjectID); err != nil {
		return Candidate{}, err
	}
	return candidate, nil
}

// ReconcileOne processes at most one candidate for a target repository. The
// store holds a repository fence across the irreversible Git write and the
// durable merged transition, so an expired lease cannot create two writers.
func (r *Reconciler) ReconcileOne(ctx context.Context, targetRepository string) (Candidate, bool, error) {
	if err := r.ready(); err != nil {
		return Candidate{}, false, err
	}
	if recovered, ok, err := r.recoverPendingMergeOperation(ctx, targetRepository); err != nil || ok {
		return recovered, ok, err
	}
	claim, claimed, err := r.store.ClaimNext(ctx, targetRepository)
	if err != nil || !claimed {
		return Candidate{}, claimed, err
	}
	candidate := claim.Candidate
	defer func() { _ = r.store.ReleaseClaim(context.Background(), claim) }()

	binding, err := r.workspaces.Get(ctx, candidate.WorkspaceRef)
	if err != nil {
		failed, failErr := r.fail(ctx, claim, candidate, candidate.Status, FailurePermission, err)
		return failed, true, failErr
	}
	if err := validateWorkspace(candidate, binding); err != nil {
		failed, failErr := r.fail(ctx, claim, candidate, candidate.Status, FailurePermission, err)
		return failed, true, failErr
	}

	prepared, err := r.backend.Prepare(ctx, candidate, binding)
	if err != nil {
		reason := failureReason(err, FailureConflict)
		failed, failErr := r.fail(ctx, claim, candidate, candidate.Status, reason, err)
		return failed, true, failErr
	}
	defer r.backend.Cleanup(prepared)

	claim, err = r.store.RenewClaim(ctx, claim)
	if err != nil {
		return Candidate{}, true, err
	}
	candidate = claim.Candidate
	if candidate.Status == StatusMergeCheck {
		candidate, err = r.store.advance(ctx, claim, StatusMergeCheck, StatusTargetedVerify, nil, "")
		if err != nil {
			return Candidate{}, true, err
		}
		claim.Candidate = candidate
	}
	verifyResult, verifyErr := r.verifier.Verify(ctx, TargetedVerifyRequest{
		Candidate:          candidate,
		WorkspaceRoot:      prepared.Root,
		LatestMainRevision: prepared.LatestMainRevision,
	})
	principal := evidence.Principal{Role: evidence.RoleTaskManager, ProjectID: candidate.ProjectID, TaskID: candidate.TaskID}
	trustedVerifyRefs := append([]evidence.ArtifactID(nil), verifyResult.EvidenceRefs...)
	for _, ref := range trustedVerifyRefs {
		if !r.artifacts.CanRead(principal, ref) {
			verifyErr = kernel.Forbidden("targeted verify evidence is outside task scope")
			trustedVerifyRefs = nil
			break
		}
	}
	if verifyErr != nil || !verifyResult.Passed || len(verifyResult.EvidenceRefs) == 0 {
		if verifyErr == nil {
			verifyErr = fmt.Errorf("targeted verify did not pass with evidence")
		}
		failed, failErr := r.fail(ctx, claim, candidate, StatusTargetedVerify, failureReason(verifyErr, FailureVerifyFailed), verifyErr, trustedVerifyRefs...)
		return failed, true, failErr
	}

	mergedRevision, err := r.backend.CreateMergeCommit(ctx, prepared, candidate)
	if err != nil {
		reason := failureReason(err, FailureConflict)
		failed, failErr := r.fail(ctx, claim, candidate, StatusTargetedVerify, reason, err)
		return failed, true, failErr
	}
	op, err := r.store.beginMergeOperation(ctx, claim, mergeOperationRequest{
		ExpectedMainRevision:   prepared.LatestMainRevision,
		ExpectedMergedRevision: mergedRevision,
		EvidenceRefs:           verifyResult.EvidenceRefs,
		Audit:                  mergeSucceededAudit(candidate, mergedRevision, prepared.LatestMainRevision, verifyResult.EvidenceRefs),
	})
	if err != nil {
		return Candidate{}, true, err
	}
	if err := r.backend.PushExact(ctx, prepared, op.ExpectedMergedRevision); err != nil {
		if failureReason(err, "") == FailureMainDrift {
			if abortErr := r.store.abortMergeOperation(ctx, op); abortErr != nil {
				return Candidate{}, true, abortErr
			}
			failed, failErr := r.fail(ctx, claim, candidate, StatusTargetedVerify, FailureMainDrift, err)
			return failed, true, failErr
		}
		contained, containsErr := r.backend.ContainsRevision(ctx, op.TargetRepository, op.TargetBranch, op.ExpectedMergedRevision)
		if containsErr != nil {
			return Candidate{}, true, containsErr
		}
		if !contained {
			blocked, markErr := r.store.markMergeOperationRecoveryRequired(ctx, op, err.Error())
			if markErr != nil {
				return Candidate{}, true, markErr
			}
			return blocked, true, kernel.Error{Code: kernel.CodeTransitionRejected, Message: "merge operation requires manual recovery before new claims", Recoverable: true}
		}
	}
	merged, err := r.store.finalizeMergeOperation(ctx, op)
	if err != nil {
		return Candidate{}, true, err
	}
	return merged, true, r.flushAudits(ctx, candidate.ProjectID)
}

func (r *Reconciler) recoverPendingMergeOperation(ctx context.Context, targetRepository string) (Candidate, bool, error) {
	op, ok, err := r.store.pendingMergeOperation(ctx, targetRepository)
	if err != nil || !ok {
		return Candidate{}, ok, err
	}
	candidate, getErr := r.store.Get(ctx, op.CandidateID)
	if getErr != nil {
		return Candidate{}, true, getErr
	}
	if op.Status == "recovery_required" {
		return candidate, true, kernel.Error{Code: kernel.CodeTransitionRejected, Message: "merge operation requires manual recovery before new claims", Recoverable: true}
	}
	contained, err := r.backend.ContainsRevision(ctx, op.TargetRepository, op.TargetBranch, op.ExpectedMergedRevision)
	if err != nil {
		return candidate, true, err
	}
	if !contained {
		blocked, markErr := r.store.markMergeOperationRecoveryRequired(ctx, op, "expected merge commit is not contained in target branch")
		if markErr != nil {
			return Candidate{}, true, markErr
		}
		return blocked, true, kernel.Error{Code: kernel.CodeTransitionRejected, Message: "merge operation requires manual recovery before new claims", Recoverable: true}
	}
	merged, err := r.store.finalizeMergeOperation(ctx, op)
	if err != nil {
		return Candidate{}, true, err
	}
	return merged, true, r.flushAudits(ctx, merged.ProjectID)
}

func (r *Reconciler) fail(ctx context.Context, claim Claim, candidate Candidate, from Status, reason FailureReason, cause error, extraEvidence ...evidence.ArtifactID) (Candidate, error) {
	body, err := json.Marshal(map[string]string{
		"candidate_id": string(candidate.ID),
		"reason":       string(reason),
		"detail":       cause.Error(),
	})
	if err != nil {
		return Candidate{}, err
	}
	artifact, err := r.artifacts.Register(ctx, evidence.RegisterArtifact{
		Type:        evidence.ArtifactGeneratedReport,
		ProjectID:   candidate.ProjectID,
		TaskID:      candidate.TaskID,
		Path:        path.Join("merge", string(candidate.ID), "failure.json"),
		ContentType: "application/json",
		Body:        body,
	})
	if err != nil {
		return Candidate{}, err
	}
	refs := dedupeEvidence(append(append([]evidence.ArtifactID(nil), extraEvidence...), artifact.ID))
	audit := auditRecord{
		StableKey:    kernel.IdempotencyKey("merge-candidate:" + string(candidate.ID) + ":failed:" + string(reason)),
		Type:         "MergeCandidateFailed",
		ProjectID:    candidate.ProjectID,
		TaskID:       candidate.TaskID,
		WorkspaceRef: candidate.WorkspaceRef,
		Payload:      map[string]string{"candidate_id": string(candidate.ID), "reason": string(reason)},
		ArtifactRefs: dedupeEvidence(append(candidateArtifacts(candidate), refs...)),
	}
	failed, err := r.store.fail(ctx, claim, from, reason, refs, audit)
	if err != nil {
		return Candidate{}, err
	}
	return failed, r.flushAudits(ctx, candidate.ProjectID)
}

func (r *Reconciler) flushAudits(ctx context.Context, projectID kernel.ProjectID) error {
	audits, err := r.store.pendingAudits(ctx, projectID, 64)
	if err != nil {
		return err
	}
	for _, audit := range audits {
		if _, err := r.events.Append(ctx, evidence.AppendEvent{
			StableKey:    audit.StableKey,
			Type:         audit.Type,
			ProjectID:    audit.ProjectID,
			TaskID:       audit.TaskID,
			WorkspaceRef: audit.WorkspaceRef,
			Payload:      cloneStringMap(audit.Payload),
			ArtifactRefs: append([]evidence.ArtifactID(nil), audit.ArtifactRefs...),
		}); err != nil {
			return err
		}
		if err := r.store.markAuditDelivered(ctx, audit.StableKey); err != nil {
			return err
		}
	}
	return nil
}

func (r *Reconciler) ready() error {
	if r == nil || r.store == nil || r.workspaces == nil || r.verifier == nil || r.artifacts == nil || r.events == nil {
		return kernel.InvalidArgument("merge queue dependencies are required")
	}
	return nil
}

func validateWorkspace(candidate Candidate, binding workspace.Binding) error {
	if binding.ID != candidate.WorkspaceRef || binding.TaskID != candidate.TaskID {
		return kernel.Forbidden("candidate does not own workspace binding")
	}
	if binding.Status != workspace.StatusSealed {
		return kernel.Forbidden("candidate workspace must be sealed")
	}
	if binding.ActivePhase != "" || binding.ActiveInvocation != "" {
		return kernel.Forbidden("candidate workspace still has an active phase lease")
	}
	if binding.PhaseLeases[workspace.PhaseVerify] == "" {
		return kernel.Forbidden("candidate workspace has no completed verify phase")
	}
	if binding.BaseRevision != candidate.BaseRevision || binding.CurrentRevision != candidate.CandidateRevision {
		return kernel.Forbidden("candidate revisions do not match sealed workspace")
	}
	for _, file := range binding.ObservedWrites.Files {
		clean := path.Clean(strings.ReplaceAll(file, "\\", "/"))
		if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") || path.IsAbs(clean) || !withinAllowed(clean, binding.AllowedDirs) {
			return kernel.Forbidden("observed write is outside allowed dirs")
		}
	}
	return nil
}

func withinAllowed(file string, allowed []string) bool {
	for _, dir := range allowed {
		dir = strings.Trim(path.Clean(strings.ReplaceAll(dir, "\\", "/")), "/")
		if dir != "" && (file == dir || strings.HasPrefix(file, dir+"/")) {
			return true
		}
	}
	return false
}

func candidateArtifacts(candidate Candidate) []evidence.ArtifactID {
	refs := []evidence.ArtifactID{candidate.VerifyResultRef, candidate.DiffArtifactRef}
	return dedupeEvidence(append(refs, candidate.EvidenceRefs...))
}

func mergeSucceededAudit(candidate Candidate, mergedRevision, mainRevision string, refs []evidence.ArtifactID) auditRecord {
	mergedRefs := dedupeEvidence(append(candidateArtifacts(candidate), refs...))
	return auditRecord{
		StableKey:    kernel.IdempotencyKey("merge-candidate:" + string(candidate.ID) + ":merged"),
		Type:         "MergeCandidateMerged",
		ProjectID:    candidate.ProjectID,
		TaskID:       candidate.TaskID,
		WorkspaceRef: candidate.WorkspaceRef,
		Payload: map[string]string{
			"candidate_id":    string(candidate.ID),
			"merged_revision": mergedRevision,
			"main_revision":   mainRevision,
		},
		ArtifactRefs: mergedRefs,
	}
}
