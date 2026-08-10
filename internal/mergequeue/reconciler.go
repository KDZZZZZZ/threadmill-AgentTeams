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
	ClaimNext(context.Context, string) (Candidate, bool, error)
	advance(context.Context, CandidateID, Status, Status, []evidence.ArtifactID, string) (Candidate, error)
	commitMerged(context.Context, CandidateID, []evidence.ArtifactID, string, auditRecord) (Candidate, error)
	fail(context.Context, CandidateID, Status, FailureReason, []evidence.ArtifactID, auditRecord) (Candidate, error)
	pendingAudits(context.Context) ([]auditRecord, error)
	markAuditDelivered(context.Context, kernel.IdempotencyKey) error
	ReleaseClaim(context.Context, string, CandidateID) error
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
	if err := r.flushAudits(ctx); err != nil {
		return Candidate{}, err
	}
	return candidate, nil
}

// ReconcileOne processes at most one candidate for a target repository. The
// store claim serializes all main writes for that repository.
func (r *Reconciler) ReconcileOne(ctx context.Context, targetRepository string) (Candidate, bool, error) {
	if err := r.ready(); err != nil {
		return Candidate{}, false, err
	}
	if err := r.flushAudits(ctx); err != nil {
		return Candidate{}, false, err
	}
	candidate, claimed, err := r.store.ClaimNext(ctx, targetRepository)
	if err != nil || !claimed {
		return Candidate{}, claimed, err
	}
	defer func() { _ = r.store.ReleaseClaim(context.Background(), candidate.TargetRepository, candidate.ID) }()

	binding, err := r.workspaces.Get(ctx, candidate.WorkspaceRef)
	if err != nil {
		failed, failErr := r.fail(ctx, candidate, candidate.Status, FailurePermission, err)
		return failed, true, failErr
	}
	if err := validateWorkspace(candidate, binding); err != nil {
		failed, failErr := r.fail(ctx, candidate, candidate.Status, FailurePermission, err)
		return failed, true, failErr
	}

	prepared, err := r.backend.Prepare(ctx, candidate, binding)
	if err != nil {
		reason := failureReason(err, FailureConflict)
		failed, failErr := r.fail(ctx, candidate, candidate.Status, reason, err)
		return failed, true, failErr
	}
	defer r.backend.Cleanup(prepared)

	if candidate.Status == StatusMergeCheck {
		candidate, err = r.store.advance(ctx, candidate.ID, StatusMergeCheck, StatusTargetedVerify, nil, "")
		if err != nil {
			return Candidate{}, true, err
		}
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
		failed, failErr := r.fail(ctx, candidate, StatusTargetedVerify, failureReason(verifyErr, FailureVerifyFailed), verifyErr, trustedVerifyRefs...)
		return failed, true, failErr
	}

	mergedRevision, err := r.backend.Merge(ctx, prepared, candidate)
	if err != nil {
		reason := failureReason(err, FailureMainDrift)
		failed, failErr := r.fail(ctx, candidate, StatusTargetedVerify, reason, err)
		return failed, true, failErr
	}
	mergedRefs := dedupeEvidence(append(candidateArtifacts(candidate), verifyResult.EvidenceRefs...))
	audit := auditRecord{
		StableKey:    kernel.IdempotencyKey("merge-candidate:" + string(candidate.ID) + ":merged"),
		Type:         "MergeCandidateMerged",
		ProjectID:    candidate.ProjectID,
		TaskID:       candidate.TaskID,
		WorkspaceRef: candidate.WorkspaceRef,
		Payload: map[string]string{
			"candidate_id":    string(candidate.ID),
			"merged_revision": mergedRevision,
			"main_revision":   prepared.LatestMainRevision,
		},
		ArtifactRefs: mergedRefs,
	}
	merged, err := r.store.commitMerged(ctx, candidate.ID, verifyResult.EvidenceRefs, mergedRevision, audit)
	if err != nil {
		return Candidate{}, true, err
	}
	return merged, true, r.flushAudits(ctx)
}

func (r *Reconciler) fail(ctx context.Context, candidate Candidate, from Status, reason FailureReason, cause error, extraEvidence ...evidence.ArtifactID) (Candidate, error) {
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
	failed, err := r.store.fail(ctx, candidate.ID, from, reason, refs, audit)
	if err != nil {
		return Candidate{}, err
	}
	return failed, r.flushAudits(ctx)
}

func (r *Reconciler) flushAudits(ctx context.Context) error {
	audits, err := r.store.pendingAudits(ctx)
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
