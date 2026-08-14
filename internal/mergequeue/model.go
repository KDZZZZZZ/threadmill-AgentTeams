// Package mergequeue owns the only Threadmill path that writes an accepted
// execute candidate to a managed main branch. It deliberately has no
// Coordination Graph or Context Graph write dependency. Normal acceptance
// verification runs afterwards against the merged revision.
package mergequeue

import (
	"context"
	"fmt"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/evidence"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/workspace"
)

type CandidateID string

type Status string

const (
	StatusQueued         Status = "queued"
	StatusMergeCheck     Status = "merge_check"
	StatusTargetedVerify Status = "targeted_verify"
	StatusMerged         Status = "merged"
	StatusFailed         Status = "failed"
)

type FailureReason string

const (
	FailureConflict     FailureReason = "conflict"
	FailurePermission   FailureReason = "permission"
	FailureMainDrift    FailureReason = "main_drift"
	FailureVerifyFailed FailureReason = "verify_failed"
)

// Candidate persists references to immutable evidence and workspace state. It
// does not copy the WorkspaceBinding or verification payload.
type Candidate struct {
	ID           CandidateID       `json:"id"`
	ProjectID    kernel.ProjectID  `json:"project_id"`
	TaskID       kernel.TaskID     `json:"task_id"`
	WorkspaceRef kernel.BindingRef `json:"workspace_ref"`
	// VerifyResultRef is the legacy persisted column name for the accepted
	// source Phase result. New code_merge candidates store the execute result;
	// normal acceptance Verify runs after MergedRevision exists.
	VerifyResultRef   evidence.ArtifactID   `json:"verify_result_ref"`
	DiffArtifactRef   evidence.ArtifactID   `json:"diff_artifact_ref"`
	TargetRepository  string                `json:"target_repository"`
	TargetBranch      string                `json:"target_branch"`
	BaseRevision      string                `json:"base_revision"`
	MainRevision      string                `json:"main_revision"`
	CandidateRevision string                `json:"candidate_revision"`
	Status            Status                `json:"status"`
	FailureReason     FailureReason         `json:"failure_reason,omitempty"`
	MergedRevision    string                `json:"merged_revision,omitempty"`
	EvidenceRefs      []evidence.ArtifactID `json:"evidence_refs"`
	CreatedAt         time.Time             `json:"created_at"`
	UpdatedAt         time.Time             `json:"updated_at"`
}

type Claim struct {
	Candidate Candidate
	OwnerID   string
	Token     string
	ExpiresAt time.Time
}

type EnqueueRequest struct {
	ID                CandidateID
	ProjectID         kernel.ProjectID
	TaskID            kernel.TaskID
	WorkspaceRef      kernel.BindingRef
	VerifyResultRef   evidence.ArtifactID
	DiffArtifactRef   evidence.ArtifactID
	TargetRepository  string
	TargetBranch      string
	BaseRevision      string
	MainRevision      string
	CandidateRevision string
	EvidenceRefs      []evidence.ArtifactID
}

type WorkspaceReader interface {
	Get(context.Context, kernel.BindingRef) (workspace.Binding, error)
}

type TargetedVerifyRequest struct {
	Candidate          Candidate
	WorkspaceRoot      string
	LatestMainRevision string
	// AllowedWritePaths is empty for a clean replay. When Prepare found merge
	// conflicts, it contains only the exact conflicted candidate paths that the
	// verifier may resolve in the disposable workspace.
	AllowedWritePaths []string
	ConflictPaths     []string
}

type TargetedVerifyResult struct {
	Passed       bool
	EvidenceRefs []evidence.ArtifactID
}

// TargetedVerifier resolves a mechanically detected merge conflict. It may
// change only AllowedWritePaths inside the disposable workspace and produce
// evidence. It cannot commit or push; Merge Queue owns both actions. This is
// distinct from the Task's normal post-merge acceptance Verify.
type TargetedVerifier interface {
	Verify(context.Context, TargetedVerifyRequest) (TargetedVerifyResult, error)
}

// MainDrift reports that a latest-main-bound operation observed a different
// target revision. Keeping the concrete error private lets integration
// adapters classify drift without exporting another persistent domain object.
func MainDrift(expected, actual string) error {
	return backendFailure{
		reason: FailureMainDrift,
		err:    fmt.Errorf("main advanced from %s to %s", expected, actual),
	}
}
