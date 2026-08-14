package app

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/evidence"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/mergequeue"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/objectstore"
	phasepkg "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime/phase"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/taskmanager"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/workspace"
)

func TestProductionMergeQueuePreparesPostMergeVerifyWorkspaceExactlyOnce(t *testing.T) {
	databaseURL := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("THREADMILL_TEST_DATABASE_URL is required for the real PostgreSQL integration test")
	}
	ctx := context.Background()
	db := openProductionPhasePostgres(t, ctx, databaseURL)
	defer db.Close()

	projectID := kernel.ProjectID("project-merge-delivery")
	taskID := kernel.TaskID("task-merge-delivery")
	graph := coordination.NewPostgresStore(db)
	seedProductionCompletionTask(t, ctx, db, projectID, graph, taskID, taskmanager.DeliveryPolicyCodeMerge)

	repositoryPath := seedProductionPhaseBareRepo(t)
	workspaces := workspace.NewPostgresService(db)
	binding, err := workspaces.CreateGitWorktree(ctx, workspace.CreateRequest{
		TaskID:         taskID,
		Generation:     1,
		RepoPath:       repositoryPath,
		WorktreeParent: t.TempDir(),
		DeclaredWrites: workspace.WriteSet{Files: []string{"README.md"}},
		AllowedDirs:    []string{"README.md"},
	})
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	artifacts := evidence.NewPostgresArtifactRegistry(db, objectstore.NewMemoryStore(), "artifacts")
	verifyArtifact, err := artifacts.Register(ctx, evidence.RegisterArtifact{
		Type:        evidence.ArtifactGeneratedReport,
		ProjectID:   projectID,
		TaskID:      taskID,
		Path:        "verify/report.json",
		ContentType: "application/json",
		Body:        []byte(`{"ok":true}`),
	})
	if err != nil {
		t.Fatalf("register verify artifact: %v", err)
	}
	diffArtifact, err := artifacts.Register(ctx, evidence.RegisterArtifact{
		Type:        evidence.ArtifactDiffPatch,
		ProjectID:   projectID,
		TaskID:      taskID,
		Path:        "merge/candidate.patch",
		ContentType: "text/x-diff",
		Body:        []byte("diff --git a/README.md b/README.md\n"),
	})
	if err != nil {
		t.Fatalf("register diff artifact: %v", err)
	}

	candidateID := mergequeue.CandidateID("candidate-merge-delivery")
	now := time.Now().UTC()
	if _, err := db.ExecContext(ctx, `
INSERT INTO merge_candidates(
	id, project_id, task_id, workspace_ref, verify_result_ref, diff_artifact_ref,
	target_repository, target_branch, base_revision, main_revision, candidate_revision,
	status, merged_revision, created_at, updated_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,'main',$8,$8,$8,'merged',$8,$9,$9)`,
		candidateID, projectID, taskID, binding.ID, verifyArtifact.ID, diffArtifact.ID, repositoryPath, binding.BaseRevision, now); err != nil {
		t.Fatalf("insert merged candidate: %v", err)
	}
	completion := productionTaskCompletionBoundary{
		SourceInputRef: "manager-input:verify-evaluation",
		TaskID:         taskID,
		VerifyEndpoint: coordination.PhaseEndpointRef{TaskID: taskID, EndpointID: coordination.EndpointVerify},
		VerifyOutput:   productionPhaseOutputBoundary{OutputRef: string(verifyArtifact.ID), Receipt: phasepkg.OutputReceipt{WorkspaceRef: string(binding.ID), Endpoint: coordination.PhaseEndpointRef{TaskID: taskID, EndpointID: coordination.EndpointVerify}}},
	}
	payload, err := json.Marshal(completion)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
INSERT INTO production_merge_deliveries(
	project_id, candidate_id, task_id, verify_result_ref, completion_payload, payload_hash,
	status, created_at, updated_at
) VALUES ($1,$2,$3,$4,$5::jsonb,$6,'queued',$7,$7)`,
		projectID, candidateID, taskID, verifyArtifact.ID, string(payload), hashProductionBytes(payload), now); err != nil {
		t.Fatalf("insert delivery: %v", err)
	}

	verifySpaces := &productionMergedVerifyWorkspaceStub{}
	queue := &productionMergeQueue{
		db: db, projectID: projectID, graph: graph, verifySpaces: verifySpaces,
	}
	if err := queue.dispatchMergedCompletionBacklog(ctx); err != nil {
		t.Fatalf("dispatch backlog: %v", err)
	}

	var deliveryStatus, managerInputRef string
	if err := db.QueryRowContext(ctx, `SELECT status, COALESCE(manager_input_ref,'') FROM production_merge_deliveries WHERE candidate_id=$1`, candidateID).Scan(&deliveryStatus, &managerInputRef); err != nil {
		t.Fatalf("select delivery: %v", err)
	}
	if deliveryStatus != "delivered" || managerInputRef != "" {
		t.Fatalf("delivery status/ref = %q/%q, want delivered without manager input", deliveryStatus, managerInputRef)
	}
	if verifySpaces.calls != 1 || verifySpaces.taskID != taskID || verifySpaces.sourceRef != binding.ID || verifySpaces.mergedRevision != binding.BaseRevision {
		t.Fatalf("verify workspace request = %#v", verifySpaces)
	}
	if err := queue.dispatchMergedCompletionBacklog(ctx); err != nil {
		t.Fatalf("redispatch backlog: %v", err)
	}
	if verifySpaces.calls != 1 {
		t.Fatalf("verify workspace calls = %d, want idempotent 1", verifySpaces.calls)
	}
}

type productionMergedVerifyWorkspaceStub struct {
	calls          int
	taskID         kernel.TaskID
	sourceRef      kernel.BindingRef
	mergedRevision string
}

func (s *productionMergedVerifyWorkspaceStub) EnsureMergedVerifyWorkspace(_ context.Context, taskID kernel.TaskID, sourceRef kernel.BindingRef, mergedRevision string) (kernel.BindingRef, error) {
	s.calls++
	s.taskID = taskID
	s.sourceRef = sourceRef
	s.mergedRevision = mergedRevision
	return kernel.BindingRef("binding://post-merge-verify"), nil
}
