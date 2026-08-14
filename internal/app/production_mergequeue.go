package app

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"strings"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/evidence"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/mergequeue"
	mqintegration "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/mergequeue/integration"
	phasepkg "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime/phase"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/taskmanager"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/workspace"
)

type productionMergeQueue struct {
	db             *sql.DB
	projectID      kernel.ProjectID
	repositoryPath string
	targetBranch   string
	graph          *coordination.PostgresStore
	workspaces     *workspace.Service
	artifacts      evidence.ArtifactStore
	queue          *mergequeue.Reconciler
	verifySpaces   productionMergedVerifyWorkspaceProvisioner
}

type productionMergedVerifyWorkspaceProvisioner interface {
	EnsureMergedVerifyWorkspace(context.Context, kernel.TaskID, kernel.BindingRef, string) (kernel.BindingRef, error)
}

// productionMergeAwareSelection keeps the fixed Coordination graph small:
// merge is an internal delivery lifecycle, not a public graph node. For
// code_merge tasks it withholds only Verify until Merge Queue has durably
// written main and prepared the merged-revision verification workspace.
type productionMergeAwareSelection struct {
	db        *sql.DB
	projectID kernel.ProjectID
	inner     coordination.RuntimeSelectionRuntime
}

func (s productionMergeAwareSelection) SelectRunnable(ctx context.Context, request coordination.RuntimeSelectionRequest) ([]coordination.PhaseEndpoint, error) {
	if s.db == nil || s.inner == nil {
		return nil, kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "merge-aware scheduler is not configured", Recoverable: true}
	}
	filtered := make([]coordination.RuntimeCandidate, 0, len(request.Candidates))
	for _, candidate := range request.Candidates {
		if candidate.Endpoint.Ref.EndpointID == coordination.EndpointVerify {
			ready, err := s.verifyReady(ctx, candidate.Endpoint.Ref.TaskID)
			if err != nil {
				return nil, err
			}
			if !ready {
				continue
			}
		}
		filtered = append(filtered, candidate)
	}
	request.Candidates = filtered
	return s.inner.SelectRunnable(ctx, request)
}

func (s productionMergeAwareSelection) verifyReady(ctx context.Context, taskID kernel.TaskID) (bool, error) {
	var policy taskmanager.DeliveryPolicy
	if err := s.db.QueryRowContext(ctx, `SELECT delivery_policy FROM taskmanager_contracts WHERE project_id=$1 AND task_id=$2`, s.projectID, taskID).Scan(&policy); err != nil {
		return false, err
	}
	if policy != taskmanager.DeliveryPolicyCodeMerge {
		return true, nil
	}
	var ready bool
	err := s.db.QueryRowContext(ctx, `
SELECT EXISTS (
  SELECT 1
  FROM merge_candidates c
  JOIN production_merge_deliveries d
    ON d.project_id=c.project_id AND d.candidate_id=c.id
  WHERE c.project_id=$1 AND c.task_id=$2 AND c.status='merged' AND d.status='delivered'
)`, s.projectID, taskID).Scan(&ready)
	return ready, err
}

func (q *productionMergeQueue) EnqueueExecutedTask(ctx context.Context, evaluation productionPhaseEvaluationBoundary) error {
	if q == nil || q.graph == nil || q.workspaces == nil || q.artifacts == nil || q.queue == nil {
		return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "production Merge Queue is not configured", Recoverable: true}
	}
	if evaluation.Endpoint.EndpointID != coordination.EndpointExecute || evaluation.Output.OutputRef == "" || evaluation.Output.Receipt.WorkspaceRef == "" {
		return kernel.InvalidArgument("merge candidate requires satisfied execute output and workspace")
	}
	taskID := evaluation.Endpoint.TaskID
	snapshot, err := q.graph.Latest(ctx, q.projectID)
	if err != nil {
		return err
	}
	executeOutputRef := ""
	for _, result := range snapshot.Results {
		if result.Endpoint == evaluation.Endpoint && result.Verdict == coordination.VerdictSatisfied {
			executeOutputRef = result.OutputRef
		}
	}
	if executeOutputRef == "" || executeOutputRef != evaluation.Output.OutputRef {
		return kernel.TransitionRejected("code_merge candidate requires a satisfied execute result")
	}
	binding, err := q.workspaces.Get(ctx, kernel.BindingRef(evaluation.Output.Receipt.WorkspaceRef))
	if err != nil {
		return err
	}
	patch, err := productionMergeCandidatePatch(ctx, workspace.NewLocalGitBackend(), binding)
	if err != nil {
		return err
	}
	if strings.TrimSpace(patch) == "" {
		return kernel.TransitionRejected("code_merge candidate requires a non-empty workspace diff")
	}
	diffArtifact, err := q.artifacts.Register(ctx, evidence.RegisterArtifact{
		Type:        evidence.ArtifactDiffPatch,
		ProjectID:   q.projectID,
		TaskID:      taskID,
		Path:        "merge/" + string(taskID) + "/" + stableProductionSuffix(evaluation.Output.OutputRef, binding.CurrentRevision) + ".patch",
		ContentType: "text/x-diff",
		Body:        []byte(patch),
	})
	if err != nil {
		return err
	}
	if binding.Status != workspace.StatusSealed {
		binding, err = q.workspaces.Seal(ctx, binding.ID, binding.Revision)
		if err != nil {
			return err
		}
	}
	branch, err := q.branch(ctx)
	if err != nil {
		return err
	}
	mainRevision, err := gitRevision(ctx, q.repositoryPath, "refs/heads/"+branch)
	if err != nil {
		return err
	}
	refs := append([]evidence.ArtifactID(nil), stringRefsToArtifactIDs(evaluation.Output.Receipt.Output.EvidenceRefs)...)
	refs = append(refs, stringRefsToArtifactIDs(evaluation.Output.Receipt.Output.DeliveryRefs)...)
	if evaluation.Output.Receipt.Output.ReportRef != "" {
		refs = append(refs, evidence.ArtifactID(evaluation.Output.Receipt.Output.ReportRef))
	}
	candidateID := mergequeue.CandidateID(stableProductionSuffix(q.projectID, "merge-candidate-v3", taskID, evaluation.Output.OutputRef, binding.ID, binding.CurrentRevision, diffArtifact.ID))
	_, err = q.queue.Enqueue(ctx, mergequeue.EnqueueRequest{
		ID:                candidateID,
		ProjectID:         q.projectID,
		TaskID:            taskID,
		WorkspaceRef:      binding.ID,
		VerifyResultRef:   evidence.ArtifactID(evaluation.Output.OutputRef),
		DiffArtifactRef:   diffArtifact.ID,
		TargetRepository:  q.repositoryPath,
		TargetBranch:      branch,
		BaseRevision:      binding.BaseRevision,
		MainRevision:      mainRevision,
		CandidateRevision: binding.CurrentRevision,
		EvidenceRefs:      refs,
	})
	if err != nil {
		return err
	}
	return q.persistCompletionDelivery(ctx, candidateID, taskID, evaluation.Output.OutputRef, evaluation)
}

func productionMergeCandidatePatch(ctx context.Context, backend workspace.GitBackend, binding workspace.Binding) (string, error) {
	if len(binding.ObservedWrites.Files) == 0 {
		return "", kernel.TransitionRejected("code_merge candidate requires observed execute writes")
	}
	return backend.DiffPaths(ctx, binding, binding.ObservedWrites.Files)
}

func (q *productionMergeQueue) Reconcile(ctx context.Context) error {
	if q == nil || q.queue == nil {
		return nil
	}
	if err := q.dispatchMergedCompletionBacklog(ctx); err != nil {
		return err
	}
	candidate, processed, err := q.queue.ReconcileOne(ctx, q.repositoryPath)
	if err != nil {
		return err
	}
	if processed && candidate.Status == mergequeue.StatusMerged {
		return q.dispatchMergedCompletion(ctx, candidate)
	}
	if processed && candidate.Status == mergequeue.StatusFailed {
		return q.markCompletionDeliveryFailed(ctx, candidate.ID, string(candidate.FailureReason))
	}
	return nil
}

func (q *productionMergeQueue) persistCompletionDelivery(ctx context.Context, candidateID mergequeue.CandidateID, taskID kernel.TaskID, sourceResultRef string, lifecycle any) error {
	if q.db == nil {
		return nil
	}
	payload, err := json.Marshal(lifecycle)
	if err != nil {
		return err
	}
	hash := hashProductionBytes(payload)
	result, err := q.db.ExecContext(ctx, `
INSERT INTO production_merge_deliveries(
	project_id, candidate_id, task_id, verify_result_ref, completion_payload, payload_hash,
	status, created_at, updated_at
) VALUES ($1,$2,$3,$4,$5::jsonb,$6,'queued',now(),now())
ON CONFLICT (candidate_id) DO UPDATE SET updated_at = production_merge_deliveries.updated_at
WHERE production_merge_deliveries.payload_hash = EXCLUDED.payload_hash`,
		q.projectID, candidateID, taskID, sourceResultRef, string(payload), hash)
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err == nil && affected == 0 {
		return kernel.IdempotencyConflict()
	}
	return nil
}

func (q *productionMergeQueue) dispatchMergedCompletionBacklog(ctx context.Context) error {
	if q.db == nil {
		return nil
	}
	rows, err := q.db.QueryContext(ctx, `
SELECT id, project_id, task_id, workspace_ref, verify_result_ref, diff_artifact_ref,
       target_repository, target_branch, base_revision, main_revision, candidate_revision,
       status, coalesce(merged_revision, '')
FROM merge_candidates
WHERE project_id=$1 AND status IN ('merged','failed')
  AND EXISTS (
    SELECT 1 FROM production_merge_deliveries d
    WHERE d.project_id=merge_candidates.project_id AND d.candidate_id=merge_candidates.id
      AND d.status='queued'
  )
ORDER BY updated_at, id
LIMIT 16`, q.projectID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var c mergequeue.Candidate
		if err := rows.Scan(&c.ID, &c.ProjectID, &c.TaskID, &c.WorkspaceRef, &c.VerifyResultRef, &c.DiffArtifactRef,
			&c.TargetRepository, &c.TargetBranch, &c.BaseRevision, &c.MainRevision, &c.CandidateRevision, &c.Status, &c.MergedRevision); err != nil {
			return err
		}
		switch c.Status {
		case mergequeue.StatusMerged:
			if err := q.dispatchMergedCompletion(ctx, c); err != nil {
				return err
			}
		case mergequeue.StatusFailed:
			if err := q.markCompletionDeliveryFailed(ctx, c.ID, string(c.FailureReason)); err != nil {
				return err
			}
		}
	}
	return rows.Err()
}

func (q *productionMergeQueue) dispatchMergedCompletion(ctx context.Context, candidate mergequeue.Candidate) error {
	if q.verifySpaces == nil || q.graph == nil || q.db == nil {
		return nil
	}
	snapshot, err := q.graph.Latest(ctx, q.projectID)
	if err != nil {
		return err
	}
	for _, task := range snapshot.Tasks {
		if task.ID == candidate.TaskID && task.Outcome != coordination.TaskActive {
			return nil
		}
	}
	var status string
	if err := q.db.QueryRowContext(ctx, `
SELECT status
FROM production_merge_deliveries
WHERE project_id=$1 AND candidate_id=$2`,
		q.projectID, candidate.ID).Scan(&status); err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		return err
	}
	if status == "delivered" {
		return nil
	}
	_, err = q.verifySpaces.EnsureMergedVerifyWorkspace(ctx, candidate.TaskID, candidate.WorkspaceRef, candidate.MergedRevision)
	if err != nil {
		if markErr := q.recordCompletionDeliveryError(ctx, candidate.ID, err.Error()); markErr != nil {
			return markErr
		}
		return err
	}
	return q.markCompletionDeliveryReady(ctx, candidate.ID)
}

func (q *productionMergeQueue) markCompletionDeliveryReady(ctx context.Context, candidateID mergequeue.CandidateID) error {
	if q.db == nil {
		return nil
	}
	_, err := q.db.ExecContext(ctx, `
UPDATE production_merge_deliveries
SET status='delivered', updated_at=now()
WHERE project_id=$1 AND candidate_id=$2 AND status='queued'`,
		q.projectID, candidateID)
	return err
}

func (q *productionMergeQueue) recordCompletionDeliveryError(ctx context.Context, candidateID mergequeue.CandidateID, message string) error {
	if q.db == nil {
		return nil
	}
	if strings.TrimSpace(message) == "" {
		message = "dispatch failed"
	}
	_, err := q.db.ExecContext(ctx, `
UPDATE production_merge_deliveries
SET dispatch_attempt=dispatch_attempt+1, last_error=$3, updated_at=now()
WHERE project_id=$1 AND candidate_id=$2 AND status='queued'`,
		q.projectID, candidateID, message)
	return err
}

func (q *productionMergeQueue) markCompletionDeliveryFailed(ctx context.Context, candidateID mergequeue.CandidateID, reason string) error {
	if q.db == nil {
		return nil
	}
	if strings.TrimSpace(reason) == "" {
		reason = "merge candidate failed"
	}
	_, err := q.db.ExecContext(ctx, `
UPDATE production_merge_deliveries
SET status='failed', last_error=$3, updated_at=now()
WHERE project_id=$1 AND candidate_id=$2 AND status='queued'`,
		q.projectID, candidateID, reason)
	return err
}

func (q *productionMergeQueue) CodeMergeEvidence(ctx context.Context, taskID kernel.TaskID, mergedRevision string) (taskmanager.DeliveryEvidence, []string, bool, error) {
	if q == nil || q.db == nil {
		return taskmanager.DeliveryEvidence{}, nil, false, nil
	}
	rows, err := q.db.QueryContext(ctx, `
SELECT c.merged_revision, r.artifact_id
FROM merge_candidates c
LEFT JOIN merge_candidate_evidence_refs r ON r.candidate_id = c.id
WHERE c.project_id=$1 AND c.task_id=$2 AND c.status='merged' AND c.merged_revision=$3
ORDER BY c.updated_at DESC, c.id`, q.projectID, taskID, mergedRevision)
	if err != nil {
		return taskmanager.DeliveryEvidence{}, nil, false, err
	}
	defer rows.Close()
	matchedRevision := ""
	refs := []string{}
	for rows.Next() {
		var rev string
		var ref sql.NullString
		if err := rows.Scan(&rev, &ref); err != nil {
			return taskmanager.DeliveryEvidence{}, nil, false, err
		}
		if matchedRevision == "" {
			matchedRevision = rev
		}
		if ref.Valid && ref.String != "" {
			refs = append(refs, ref.String)
		}
	}
	if err := rows.Err(); err != nil {
		return taskmanager.DeliveryEvidence{}, nil, false, err
	}
	if matchedRevision == "" {
		return taskmanager.DeliveryEvidence{}, nil, false, nil
	}
	commitRef := "commit://" + matchedRevision
	return taskmanager.DeliveryEvidence{
		LatestMainVerified: true,
		MergeSucceeded:     true,
		MergeCommitRef:     commitRef,
		EvidenceRefs:       refs,
	}, append(refs, commitRef), true, nil
}

func (q *productionMergeQueue) branch(ctx context.Context) (string, error) {
	if q.targetBranch != "" {
		return q.targetBranch, nil
	}
	branch, err := gitRevision(ctx, q.repositoryPath, "--abbrev-ref", "HEAD")
	if err != nil || branch == "" || branch == "HEAD" {
		return "main", nil
	}
	return branch, nil
}

func productionAgentTeamsMergeVerifier(projectID kernel.ProjectID, bindings mqintegration.BindingRegistrar, runtime mqintegration.PhaseRuntime, artifacts evidence.ArtifactStore) mergequeue.TargetedVerifier {
	return mqintegration.Verifier{
		Bindings:  bindings,
		Runtime:   runtime,
		Revisions: mqintegration.GitRevisionReader{},
		Results:   productionTargetedVerifyResultAcceptor{projectID: projectID, artifacts: artifacts},
	}
}

type productionTargetedVerifyResultAcceptor struct {
	projectID kernel.ProjectID
	artifacts evidence.ArtifactStore
}

type productionTargetedVerifyReport struct {
	Schema   string   `json:"schema"`
	Verdict  string   `json:"verdict"`
	Checks   []string `json:"checks"`
	Evidence []string `json:"evidence_refs"`
}

func (a productionTargetedVerifyResultAcceptor) AcceptTargetedVerify(ctx context.Context, receipt phasepkg.OutputReceipt) (mergequeue.TargetedVerifyResult, error) {
	report, refs, err := a.readTargetedVerifyOutput(ctx, receipt.Endpoint.TaskID, receipt.Output)
	if err != nil {
		return mergequeue.TargetedVerifyResult{}, err
	}
	switch strings.ToLower(strings.TrimSpace(report.Verdict)) {
	case "pass", "passed":
		if !productionChecksNonEmpty(report.Checks) {
			return mergequeue.TargetedVerifyResult{}, kernel.InvalidArgument("passing targeted verify report requires checks")
		}
		return mergequeue.TargetedVerifyResult{Passed: true, EvidenceRefs: refs}, nil
	case "fail", "failed":
		return mergequeue.TargetedVerifyResult{Passed: false, EvidenceRefs: refs}, nil
	default:
		return mergequeue.TargetedVerifyResult{}, kernel.InvalidArgument("targeted verify report verdict must be pass or fail")
	}
}

// ValidateTargetedVerifyOutput runs before Controller.SubmitPhaseOutput makes
// the one-shot verifier invocation terminal. A negative report is not a
// recoverable delivery result: it must become a Runtime-bound orchestration
// proposal so Task Manager can safely reopen the failed round.
func (a productionTargetedVerifyResultAcceptor) ValidateTargetedVerifyOutput(ctx context.Context, taskID kernel.TaskID, output phasepkg.PhaseOutput) error {
	report, _, err := a.readTargetedVerifyOutput(ctx, taskID, output)
	if err != nil {
		return err
	}
	switch strings.ToLower(strings.TrimSpace(report.Verdict)) {
	case "pass", "passed":
		if !productionChecksNonEmpty(report.Checks) {
			return kernel.InvalidArgument("passing targeted verify report requires checks")
		}
		return nil
	case "fail", "failed":
		return kernel.TransitionRejected("failing targeted verify must call agent.proposeOrchestration before submitting a terminal PhaseOutput")
	default:
		return kernel.InvalidArgument("targeted verify report verdict must be pass or fail")
	}
}

func (a productionTargetedVerifyResultAcceptor) readTargetedVerifyOutput(ctx context.Context, taskID kernel.TaskID, output phasepkg.PhaseOutput) (productionTargetedVerifyReport, []evidence.ArtifactID, error) {
	if a.artifacts == nil {
		return productionTargetedVerifyReport{}, nil, kernel.InvalidArgument("targeted verify artifact store is required")
	}
	if taskID == "" || output.Phase != string(coordination.EndpointVerify) || output.ReportRef == "" {
		return productionTargetedVerifyReport{}, nil, kernel.StaleBinding("targeted verify receipt must include a verify report")
	}
	principal := evidence.Principal{Role: evidence.RoleTaskManager, ProjectID: a.projectID, TaskID: taskID}
	reportArtifact, body, err := a.artifacts.Open(ctx, principal, evidence.ArtifactID(output.ReportRef))
	if err != nil {
		return productionTargetedVerifyReport{}, nil, err
	}
	if reportArtifact.Type != evidence.ArtifactGeneratedReport || reportArtifact.ProjectID != a.projectID || reportArtifact.TaskID != taskID {
		return productionTargetedVerifyReport{}, nil, kernel.InvalidArgument("targeted verify report artifact is not a generated report for this task")
	}
	var report productionTargetedVerifyReport
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&report); err != nil {
		return productionTargetedVerifyReport{}, nil, kernel.InvalidArgument("targeted verify report is not valid JSON")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return productionTargetedVerifyReport{}, nil, kernel.InvalidArgument("targeted verify report has trailing JSON")
	}
	if report.Schema != "threadmill.targeted_verify.v1" {
		return productionTargetedVerifyReport{}, nil, kernel.InvalidArgument("targeted verify report schema is unsupported")
	}
	refs := []evidence.ArtifactID{evidence.ArtifactID(output.ReportRef)}
	refs = append(refs, stringRefsToArtifactIDs(output.EvidenceRefs)...)
	refs = append(refs, stringRefsToArtifactIDs(output.DeliveryRefs)...)
	refs = append(refs, stringRefsToArtifactIDs(report.Evidence)...)
	refs = dedupeProductionArtifactIDs(refs)
	if len(refs) == 0 {
		return productionTargetedVerifyReport{}, nil, kernel.InvalidArgument("targeted verify report requires readable evidence")
	}
	for _, ref := range refs {
		artifact, _, err := a.artifacts.Open(ctx, principal, ref)
		if err != nil {
			return productionTargetedVerifyReport{}, nil, err
		}
		if artifact.ProjectID != a.projectID || artifact.TaskID != taskID {
			return productionTargetedVerifyReport{}, nil, kernel.InvalidArgument("targeted verify evidence is not scoped to this task")
		}
	}
	return report, refs, nil
}

func productionChecksNonEmpty(checks []string) bool {
	for _, check := range checks {
		if strings.TrimSpace(check) != "" {
			return true
		}
	}
	return false
}

// productionMechanicalMergeVerifier is retained only as a fail-closed
// compatibility seam until production_runtime.go wires the AgentTeams-backed
// verifier returned by productionAgentTeamsMergeVerifier. It deliberately does
// not run tests locally: Merge Queue targeted verification must execute through
// the Phase Runtime so AgentTeams, native tools, artifact ACLs, and output
// receipts are all exercised on the production path.
type productionMechanicalMergeVerifier struct {
	artifacts evidence.ArtifactStore
}

func (productionMechanicalMergeVerifier) Verify(context.Context, mergequeue.TargetedVerifyRequest) (mergequeue.TargetedVerifyResult, error) {
	return mergequeue.TargetedVerifyResult{}, kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "AgentTeams targeted merge verifier is not wired", Recoverable: true}
}

func gitRevision(ctx context.Context, repo string, args ...string) (string, error) {
	all := append([]string{"-C", repo, "rev-parse"}, args...)
	cmd := exec.CommandContext(ctx, "git", all...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("git %s: %w: %s", strings.Join(all, " "), err, strings.TrimSpace(string(out)))
	}
	return strings.TrimSpace(string(out)), nil
}

func stringRefsToArtifactIDs(refs []string) []evidence.ArtifactID {
	out := make([]evidence.ArtifactID, 0, len(refs))
	for _, ref := range refs {
		if strings.TrimSpace(ref) != "" {
			out = append(out, evidence.ArtifactID(ref))
		}
	}
	return out
}

func dedupeProductionArtifactIDs(refs []evidence.ArtifactID) []evidence.ArtifactID {
	seen := make(map[evidence.ArtifactID]struct{}, len(refs))
	out := make([]evidence.ArtifactID, 0, len(refs))
	for _, ref := range refs {
		if strings.TrimSpace(string(ref)) == "" {
			continue
		}
		if _, ok := seen[ref]; ok {
			continue
		}
		seen[ref] = struct{}{}
		out = append(out, ref)
	}
	return out
}

var _ productionTaskMergeScheduler = (*productionMergeQueue)(nil)
var _ productionTaskMergeEvidenceReader = (*productionMergeQueue)(nil)
