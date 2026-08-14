package app

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/evidence"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/objectstore"
	phasepkg "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime/phase"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/workspace"
)

func TestProductionMergeCandidatePatchContainsCandidateWritesOnly(t *testing.T) {
	ctx := context.Background()
	service := workspace.NewService()
	binding, err := service.CreateGitWorktree(ctx, workspace.CreateRequest{
		TaskID: "task-candidate-patch", Generation: 1,
		RepoPath: seedProductionPhaseBareRepo(t), WorktreeParent: t.TempDir(),
		AllowedDirs: []string{"retry/policy.go"}, DeclaredWrites: workspace.WriteSet{Files: []string{"retry/policy.go"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{
		"plan/plan.md": "# plan\n", "evidence/verify.txt": "ok\n", "retry/policy.go": "package retry\n",
	} {
		path := filepath.Join(binding.Root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	binding.ObservedWrites = workspace.WriteSet{Files: []string{"retry/policy.go"}}
	patch, err := productionMergeCandidatePatch(ctx, workspace.NewLocalGitBackend(), binding)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(patch, "retry/policy.go") || strings.Contains(patch, "plan/plan.md") || strings.Contains(patch, "evidence/verify.txt") {
		t.Fatalf("candidate patch mixed phase artifacts into code diff:\n%s", patch)
	}
}

func TestProductionTargetedVerifyResultAcceptorRequiresStructuredPassingReport(t *testing.T) {
	ctx := context.Background()
	projectID := kernel.ProjectID("project-a")
	taskID := kernel.TaskID("task-a")
	registry := evidence.NewArtifactRegistry(objectstore.NewMemoryStore(), "artifacts")
	report, err := registry.Register(ctx, evidence.RegisterArtifact{
		Type:        evidence.ArtifactGeneratedReport,
		ProjectID:   projectID,
		TaskID:      taskID,
		Path:        "merge/candidate/targeted-verify.json",
		ContentType: "application/json",
		Body:        []byte(`{"schema":"threadmill.targeted_verify.v1","verdict":"pass","checks":["go test ./..."],"evidence_refs":[]}`),
	})
	if err != nil {
		t.Fatalf("register report: %v", err)
	}
	receipt := phasepkg.OutputReceipt{
		Output: phasepkg.PhaseOutput{
			Phase:     string(coordination.EndpointVerify),
			ReportRef: string(report.ID),
		},
		Endpoint: coordination.PhaseEndpointRef{TaskID: taskID, EndpointID: coordination.EndpointVerify},
	}

	got, err := (productionTargetedVerifyResultAcceptor{projectID: projectID, artifacts: registry}).AcceptTargetedVerify(ctx, receipt)
	if err != nil {
		t.Fatalf("AcceptTargetedVerify() error = %v", err)
	}
	if !got.Passed || len(got.EvidenceRefs) != 1 || got.EvidenceRefs[0] != report.ID {
		t.Fatalf("targeted verify result = %#v, want passed report evidence", got)
	}
}

func TestProductionTargetedVerifyOutputValidatorKeepsFailingInvocationOpenForReplan(t *testing.T) {
	ctx := context.Background()
	projectID := kernel.ProjectID("project-a")
	taskID := kernel.TaskID("task-a")
	registry := evidence.NewArtifactRegistry(objectstore.NewMemoryStore(), "artifacts")
	report, err := registry.Register(ctx, evidence.RegisterArtifact{
		Type: evidence.ArtifactGeneratedReport, ProjectID: projectID, TaskID: taskID,
		Path: "merge/candidate/targeted-verify-failed.json", ContentType: "application/json",
		Body: []byte(`{"schema":"threadmill.targeted_verify.v1","verdict":"fail","checks":["npm test failed"],"evidence_refs":[]}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	validator := productionTargetedVerifyResultAcceptor{projectID: projectID, artifacts: registry}
	err = validator.ValidateTargetedVerifyOutput(ctx, taskID, phasepkg.PhaseOutput{
		Phase: string(coordination.EndpointVerify), ReportRef: string(report.ID),
	})
	if !kernel.IsCode(err, kernel.CodeTransitionRejected) || !strings.Contains(err.Error(), "agent.proposeOrchestration") {
		t.Fatalf("failing targeted verify validation = %v, want replan transition rejection", err)
	}
}

func TestProductionTargetedVerifyResultAcceptorRejectsUnstructuredReport(t *testing.T) {
	ctx := context.Background()
	projectID := kernel.ProjectID("project-a")
	taskID := kernel.TaskID("task-a")
	registry := evidence.NewArtifactRegistry(objectstore.NewMemoryStore(), "artifacts")
	report, err := registry.Register(ctx, evidence.RegisterArtifact{
		Type:        evidence.ArtifactGeneratedReport,
		ProjectID:   projectID,
		TaskID:      taskID,
		Path:        "merge/candidate/freeform.md",
		ContentType: "text/markdown",
		Body:        []byte("looks good"),
	})
	if err != nil {
		t.Fatalf("register report: %v", err)
	}
	receipt := phasepkg.OutputReceipt{
		Output: phasepkg.PhaseOutput{
			Phase:        string(coordination.EndpointVerify),
			ReportRef:    string(report.ID),
			EvidenceRefs: []string{string(report.ID)},
		},
		Endpoint: coordination.PhaseEndpointRef{TaskID: taskID, EndpointID: coordination.EndpointVerify},
	}

	if _, err := (productionTargetedVerifyResultAcceptor{projectID: projectID, artifacts: registry}).AcceptTargetedVerify(ctx, receipt); !kernel.IsCode(err, kernel.CodeInvalidRequest) {
		t.Fatalf("AcceptTargetedVerify() error = %v, want invalid_request", err)
	}
}

func TestProductionTargetedVerifyResultAcceptorRejectsUnknownFieldsAndTrailingJSON(t *testing.T) {
	ctx := context.Background()
	projectID := kernel.ProjectID("project-a")
	taskID := kernel.TaskID("task-a")
	registry := evidence.NewArtifactRegistry(objectstore.NewMemoryStore(), "artifacts")
	tests := []struct {
		name string
		body string
	}{
		{name: "unknown", body: `{"schema":"threadmill.targeted_verify.v1","verdict":"pass","checks":["go test"],"evidence_refs":[],"extra":true}`},
		{name: "trailing", body: `{"schema":"threadmill.targeted_verify.v1","verdict":"pass","checks":["go test"],"evidence_refs":[]} {}`},
		{name: "empty checks", body: `{"schema":"threadmill.targeted_verify.v1","verdict":"pass","checks":[" "],"evidence_refs":[]}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			report, err := registry.Register(ctx, evidence.RegisterArtifact{
				Type:        evidence.ArtifactGeneratedReport,
				ProjectID:   projectID,
				TaskID:      taskID,
				Path:        "merge/candidate/" + tt.name + ".json",
				ContentType: "application/json",
				Body:        []byte(tt.body),
			})
			if err != nil {
				t.Fatalf("register report: %v", err)
			}
			receipt := phasepkg.OutputReceipt{
				Output: phasepkg.PhaseOutput{
					Phase:     string(coordination.EndpointVerify),
					ReportRef: string(report.ID),
				},
				Endpoint: coordination.PhaseEndpointRef{TaskID: taskID, EndpointID: coordination.EndpointVerify},
			}
			if _, err := (productionTargetedVerifyResultAcceptor{projectID: projectID, artifacts: registry}).AcceptTargetedVerify(ctx, receipt); !kernel.IsCode(err, kernel.CodeInvalidRequest) {
				t.Fatalf("AcceptTargetedVerify() error = %v, want invalid_request", err)
			}
		})
	}
}

func TestProductionTargetedVerifyResultAcceptorRequiresGeneratedReportAndReadableEvidence(t *testing.T) {
	ctx := context.Background()
	projectID := kernel.ProjectID("project-a")
	taskID := kernel.TaskID("task-a")
	registry := evidence.NewArtifactRegistry(objectstore.NewMemoryStore(), "artifacts")
	toolJSON, err := registry.Register(ctx, evidence.RegisterArtifact{
		Type:        evidence.ArtifactToolOutput,
		ProjectID:   projectID,
		TaskID:      taskID,
		Path:        "evidence/tool.json",
		ContentType: "application/json",
		Body:        []byte(`{"schema":"threadmill.targeted_verify.v1","verdict":"pass","checks":["go test"],"evidence_refs":[]}`),
	})
	if err != nil {
		t.Fatalf("register tool json: %v", err)
	}
	receipt := phasepkg.OutputReceipt{
		Output: phasepkg.PhaseOutput{
			Phase:     string(coordination.EndpointVerify),
			ReportRef: string(toolJSON.ID),
		},
		Endpoint: coordination.PhaseEndpointRef{TaskID: taskID, EndpointID: coordination.EndpointVerify},
	}
	if _, err := (productionTargetedVerifyResultAcceptor{projectID: projectID, artifacts: registry}).AcceptTargetedVerify(ctx, receipt); !kernel.IsCode(err, kernel.CodeInvalidRequest) {
		t.Fatalf("tool report error = %v, want invalid_request", err)
	}

	foreignEvidence, err := registry.Register(ctx, evidence.RegisterArtifact{
		Type:        evidence.ArtifactTestOutput,
		ProjectID:   projectID,
		TaskID:      "task-other",
		Path:        "evidence/foreign.txt",
		ContentType: "text/plain",
		Body:        []byte("ok"),
	})
	if err != nil {
		t.Fatalf("register foreign evidence: %v", err)
	}
	report, err := registry.Register(ctx, evidence.RegisterArtifact{
		Type:        evidence.ArtifactGeneratedReport,
		ProjectID:   projectID,
		TaskID:      taskID,
		Path:        "merge/candidate/report.json",
		ContentType: "application/json",
		Body:        []byte(`{"schema":"threadmill.targeted_verify.v1","verdict":"pass","checks":["go test"],"evidence_refs":["` + string(foreignEvidence.ID) + `"]}`),
	})
	if err != nil {
		t.Fatalf("register report: %v", err)
	}
	receipt.Output.ReportRef = string(report.ID)
	if _, err := (productionTargetedVerifyResultAcceptor{projectID: projectID, artifacts: registry}).AcceptTargetedVerify(ctx, receipt); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("foreign evidence error = %v, want forbidden", err)
	}
}
