package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	adapter "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/adapters/agentteams"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/contextgraph"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/evidence"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/mergequeue"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	runtimepkg "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime"
	phasepkg "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime/phase"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/taskmanager"
)

func TestTargetedVerifyBindingRegistersStructuredLatestMainSpec(t *testing.T) {
	req := targetedVerifyRequest(t)
	source := &productionTargetedVerifyBindingSource{
		projectID: req.Candidate.ProjectID,
		contracts: targetedVerifyContractStoreFunc(func(context.Context, kernel.TaskID) (taskmanager.TaskContract, error) {
			return taskmanager.TaskContract{
				TaskID:      req.Candidate.TaskID,
				ContractRef: "contract-a",
				PhaseSpecs:  map[coordination.EndpointID]string{coordination.EndpointVerify: "verify-spec"},
			}, nil
		}),
		registry: newProductionTargetedVerifyRegistry(),
	}

	binding, err := source.RegisterTargetedVerify(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if binding.EndpointID != coordination.EndpointVerify || binding.WorkspaceRef != req.WorkspaceRoot || binding.WorkspaceRevision != req.LatestMainRevision {
		t.Fatalf("binding does not carry latest-main verify authority: %#v", binding)
	}
	var spec productionTargetedVerifySpec
	if err := json.Unmarshal([]byte(binding.PhaseSpec), &spec); err != nil {
		t.Fatal(err)
	}
	if spec.Schema != productionTargetedVerifySpecSchema || spec.Mode != "latest_main_candidate_targeted_verify" || spec.Report.Schema != productionTargetedVerifySpecSchema {
		t.Fatalf("phase spec is not strict targeted verify schema: %#v", spec)
	}
	if spec.Report.VerdictField != "verdict" || spec.Report.ChecksField != "checks" || spec.Report.EvidenceRefs != "evidence_refs" || spec.Report.PassAlias != "pass" || spec.Report.PassedAlias != "passed" {
		t.Fatalf("phase spec report contract does not match acceptor vocabulary: %#v", spec.Report)
	}
	if spec.Report.ArtifactType != evidence.ArtifactGeneratedReport ||
		spec.Report.ContentType != "application/json" ||
		spec.Report.RegisterTool != string(auth.ToolEvidenceRegister) ||
		spec.Report.RegisterBodyField != "body" ||
		spec.Report.PhaseOutputTool != string(auth.ToolAgentSubmitPhaseOutput) ||
		spec.Report.PhaseOutputRef != "report_ref" {
		t.Fatalf("phase spec report contract does not force generated_report registration: %#v", spec.Report)
	}
	rules := strings.Join(spec.Rules, "\n")
	if !strings.Contains(rules, "allowed_write_paths") || !strings.Contains(rules, "Do not commit") ||
		!strings.Contains(rules, "narrow merge-conflict resolution pass") ||
		!strings.Contains(rules, "only success condition is a conflict-free, syntactically valid working tree") ||
		!strings.Contains(rules, "Do not try to satisfy the union") ||
		!strings.Contains(rules, "candidate side as authoritative") ||
		!strings.Contains(rules, "post-merge Verify owns task acceptance") ||
		!strings.Contains(rules, "one smallest directly relevant command") ||
		!strings.Contains(rules, "agent.proposeOrchestration") ||
		!strings.Contains(rules, "do not retrieve the whole Context Graph") ||
		!strings.Contains(rules, "evidence.register with type=generated_report") ||
		!strings.Contains(rules, "body set to the strict v1 JSON report object only") ||
		!strings.Contains(rules, "do not register the final report as type=json or type=tool_output") ||
		!strings.Contains(rules, "agent.submitPhaseOutput.report_ref") {
		t.Fatalf("phase spec does not state authorized write/no-commit rules: %#v", spec.Rules)
	}
	command := productionTargetedVerifyCommand(req, binding)
	if entry, ok := source.registry.ByCommand(command.ID); !ok || entry.Binding.BindingRef != binding.BindingRef {
		t.Fatalf("registered command entry missing: ok=%v entry=%#v", ok, entry)
	}
}

func TestTargetedVerifyWorkspaceProjectorRejectsSourceChangesAndDropsEvidenceFiles(t *testing.T) {
	ctx := context.Background()
	req := targetedVerifyRequest(t)
	root := req.WorkspaceRoot
	writeFile(t, root, "main.go", []byte("package main\nfunc main() {}\n"))
	writeFile(t, root, "go.mod", []byte("module example.com/demo\n"))
	registry, execution := registerTargetedVerifyProjection(t, req)
	projector := &productionTargetedVerifyWorkspaceProjector{registry: registry}

	projection, err := projector.ExportExecutionFiles(ctx, execution)
	if err != nil {
		t.Fatal(err)
	}
	withEvidence := cloneProjection(projection)
	withEvidence.Files = append(withEvidence.Files, fileForProjection("evidence/report.json", []byte(`{"ok":true}`)))
	checkpoint, err := projector.ImportExecutionFiles(ctx, execution, withEvidence)
	if err != nil {
		t.Fatalf("evidence-only changes should be ignored: %v", err)
	}
	if checkpoint.WorkspaceRevision != req.LatestMainRevision {
		t.Fatalf("checkpoint revision = %q", checkpoint.WorkspaceRevision)
	}

	modified := cloneProjection(projection)
	replaceProjectionFile(t, &modified, "main.go", []byte("package main\nfunc main(){panic(\"changed\")}\n"))
	if _, err := projector.ImportExecutionFiles(ctx, execution, modified); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("modified source error = %v, want permission denied", err)
	}

	deleted := cloneProjection(projection)
	deleted.Files = deleted.Files[:len(deleted.Files)-1]
	if _, err := projector.ImportExecutionFiles(ctx, execution, deleted); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("deleted source error = %v, want permission denied", err)
	}

	added := cloneProjection(projection)
	added.Files = append(added.Files, fileForProjection("new-config.yml", []byte("x: y\n")))
	if _, err := projector.ImportExecutionFiles(ctx, execution, added); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("added source error = %v, want permission denied", err)
	}
}

func TestTargetedVerifyProjectorRejectsManifestIdentityMismatch(t *testing.T) {
	ctx := context.Background()
	req := targetedVerifyRequest(t)
	writeFile(t, req.WorkspaceRoot, "main.go", []byte("package main\n"))
	registry, execution := registerTargetedVerifyProjection(t, req)
	projector := &productionTargetedVerifyWorkspaceProjector{registry: registry}
	projection, err := projector.ExportExecutionFiles(ctx, execution)
	if err != nil {
		t.Fatal(err)
	}
	var manifest productionTargetedVerifyProjectionManifest
	if err := json.Unmarshal(projection.Manifest, &manifest); err != nil {
		t.Fatal(err)
	}
	manifest.LatestMainRevision = "other-main"
	projection.Manifest, _ = json.Marshal(manifest)
	if _, err := projector.ImportExecutionFiles(ctx, execution, projection); !kernel.IsCode(err, kernel.CodeStaleBinding) {
		t.Fatalf("manifest mismatch error = %v, want stale binding", err)
	}
}

func TestTargetedVerifyAuthorizedWritebackAllowsModifyDeleteAndAdd(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	writeFile(t, root, "main.go", []byte("package main\nfunc main() {}\n"))
	writeFile(t, root, "stale.txt", []byte("stale\n"))
	files, baselineFiles, err := readProductionTargetedVerifyFiles(root)
	if err != nil {
		t.Fatal(err)
	}
	baseline := productionTargetedVerifyBaseline{Root: root, Files: baselineFiles}
	projection := adapter.ExecutionFileProjection{Files: files}
	replaceProjectionFile(t, &projection, "main.go", []byte("package main\nfunc main() { println(\"resolved\") }\n"))
	removeProjectionFile(t, &projection, "stale.txt")
	projection.Files = append(projection.Files, fileForProjection("new.txt", []byte("new\n")))

	if err := applyProductionTargetedVerifyReturnedFiles(ctx, root, baseline, projection.Files, []string{"main.go", "stale.txt", "new.txt"}); err != nil {
		t.Fatal(err)
	}
	if got := readFile(t, root, "main.go"); !strings.Contains(string(got), "resolved") {
		t.Fatalf("main.go was not written back: %q", got)
	}
	if _, err := os.Stat(filepath.Join(root, "stale.txt")); !os.IsNotExist(err) {
		t.Fatalf("stale.txt exists after authorized delete: %v", err)
	}
	if got := readFile(t, root, "new.txt"); string(got) != "new\n" {
		t.Fatalf("new.txt = %q", got)
	}
}

func TestTargetedVerifyWritebackRejectsUnauthorizedChanges(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	writeFile(t, root, "main.go", []byte("package main\n"))
	writeFile(t, root, "secret.txt", []byte("secret\n"))
	files, baselineFiles, err := readProductionTargetedVerifyFiles(root)
	if err != nil {
		t.Fatal(err)
	}
	baseline := productionTargetedVerifyBaseline{Root: root, Files: baselineFiles}
	modified := adapter.ExecutionFileProjection{Files: cloneProjection(adapter.ExecutionFileProjection{Files: files}).Files}
	replaceProjectionFile(t, &modified, "secret.txt", []byte("changed\n"))
	if err := applyProductionTargetedVerifyReturnedFiles(ctx, root, baseline, modified.Files, []string{"main.go"}); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("unauthorized modify error = %v, want forbidden", err)
	}
	deleted := adapter.ExecutionFileProjection{Files: cloneProjection(adapter.ExecutionFileProjection{Files: files}).Files}
	removeProjectionFile(t, &deleted, "secret.txt")
	if err := applyProductionTargetedVerifyReturnedFiles(ctx, root, baseline, deleted.Files, []string{"main.go"}); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("unauthorized delete error = %v, want forbidden", err)
	}
	added := adapter.ExecutionFileProjection{Files: cloneProjection(adapter.ExecutionFileProjection{Files: files}).Files}
	added.Files = append(added.Files, fileForProjection("secret-new.txt", []byte("new\n")))
	if err := applyProductionTargetedVerifyReturnedFiles(ctx, root, baseline, added.Files, []string{"main.go"}); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("unauthorized add error = %v, want forbidden", err)
	}
}

func TestTargetedVerifyReturnedFilesIgnoreCrossPlatformReadWriteModeDrift(t *testing.T) {
	body := []byte("package main\n")
	baseline := productionTargetedVerifyBaseline{Files: map[string]productionTargetedVerifyFile{
		"main.go": fileSignature(body, 0o666),
	}}
	returned := []adapter.ExecutionFile{fileForProjection("main.go", body)}

	if err := validateProductionTargetedVerifyReturnedFileSet(baseline, returned, []string{"other.go"}); err != nil {
		t.Fatalf("read/write mode drift should not count as an unauthorized source change: %v", err)
	}
	returned[0].Mode = 0o755
	if err := validateProductionTargetedVerifyReturnedFileSet(baseline, returned, []string{"other.go"}); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("executable mode drift err=%v, want forbidden", err)
	}
}

func TestTargetedVerifyWritebackTreatsFileAuthorityAsExact(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	baseline := productionTargetedVerifyBaseline{Root: root, Files: map[string]productionTargetedVerifyFile{}}
	nested := []adapter.ExecutionFile{fileForProjection("workspace/a.txt/child.go", []byte("package child\n"))}
	if err := applyProductionTargetedVerifyReturnedFiles(ctx, root, baseline, nested, []string{"workspace/a.txt"}); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("file authority accepted nested child: %v, want forbidden", err)
	}
	if _, err := os.Stat(filepath.Join(root, "workspace", "a.txt", "child.go")); !os.IsNotExist(err) {
		t.Fatalf("forbidden nested child was written: %v", err)
	}
	if err := applyProductionTargetedVerifyReturnedFiles(ctx, root, baseline, nested, []string{"workspace/a.txt/"}); err != nil {
		t.Fatalf("directory authority rejected nested child: %v", err)
	}
}

func TestTargetedVerifyWritebackRejectsSymlinkEscape(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	outside := t.TempDir()
	writeFile(t, root, "main.go", []byte("package main\n"))
	if err := os.Symlink(outside, filepath.Join(root, "link")); err != nil {
		t.Skipf("symlink creation unavailable: %v", err)
	}
	files, baselineFiles, err := readProductionTargetedVerifyFiles(root)
	if !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("exporting workspace with symlink err=%v, want forbidden", err)
	}
	if len(files) != 0 || baselineFiles != nil {
		t.Fatalf("symlink workspace returned files=%d baseline=%#v", len(files), baselineFiles)
	}
	baseline := productionTargetedVerifyBaseline{Root: root, Files: map[string]productionTargetedVerifyFile{"main.go": fileSignature([]byte("package main\n"), 0o644)}}
	projection := []adapter.ExecutionFile{
		fileForProjection("main.go", []byte("package main\n")),
		fileForProjection("link/escape.txt", []byte("escape\n")),
	}
	if err := applyProductionTargetedVerifyReturnedFiles(ctx, root, baseline, projection, []string{"link/"}); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("symlink escape writeback err=%v, want forbidden", err)
	}
}

func TestTargetedVerifyWritebackPreservesGitHead(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	gitPhaseTest(t, root, "init")
	writeFile(t, root, "main.go", []byte("package main\n"))
	gitPhaseTest(t, root, "add", "main.go")
	gitPhaseTest(t, root, "-c", "user.email=test@example.com", "-c", "user.name=Test", "commit", "-m", "seed")
	before, ok, err := productionTargetedVerifyHead(ctx, root)
	if err != nil || !ok {
		t.Fatalf("head before = %q ok=%v err=%v", before, ok, err)
	}
	files, baselineFiles, err := readProductionTargetedVerifyFiles(root)
	if err != nil {
		t.Fatal(err)
	}
	projection := adapter.ExecutionFileProjection{Files: files}
	replaceProjectionFile(t, &projection, "main.go", []byte("package main\nfunc main(){}\n"))
	if err := applyProductionTargetedVerifyReturnedFiles(ctx, root, productionTargetedVerifyBaseline{Root: root, Files: baselineFiles}, projection.Files, []string{"main.go"}); err != nil {
		t.Fatal(err)
	}
	after, ok, err := productionTargetedVerifyHead(ctx, root)
	if err != nil || !ok || after != before {
		t.Fatalf("head after = %q ok=%v err=%v, want %q", after, ok, err, before)
	}
}

func TestTargetedVerifyRoutersUseTrustedRegistryNotAgentClaims(t *testing.T) {
	ctx := context.Background()
	req := targetedVerifyRequest(t)
	writeFile(t, req.WorkspaceRoot, "main.go", []byte("package main\n"))
	registry, execution := registerTargetedVerifyProjection(t, req)
	targetedProjector := &productionTargetedVerifyWorkspaceProjector{registry: registry}
	regularProjector := &fakeExecutionProjector{owns: true, export: adapter.ExecutionFileProjection{Manifest: []byte("regular")}}
	projectorRouter := productionTargetedVerifyProjectorRouter{Regular: regularProjector, Targeted: targetedProjector}

	owned, err := projectorRouter.OwnsExecution(ctx, execution)
	if err != nil || !owned {
		t.Fatalf("targeted execution ownership = %v, %v", owned, err)
	}
	if _, err := projectorRouter.ExportExecutionFiles(ctx, execution); err != nil {
		t.Fatal(err)
	}
	if regularProjector.exports != 0 {
		t.Fatalf("targeted execution should not route to regular projector, regular exports=%d", regularProjector.exports)
	}

	unknown := adapter.AgentTeamsExecutionRef{InvocationID: "inv-unknown", AgentTeamsTaskID: "agent-claim"}
	if _, err := projectorRouter.ExportExecutionFiles(ctx, unknown); err != nil {
		t.Fatal(err)
	}
	if regularProjector.exports != 1 {
		t.Fatalf("unknown invocation should route to regular projector, regular exports=%d", regularProjector.exports)
	}

	targetedRuntime := &fakePhaseRuntime{receipt: phasepkg.OutputReceipt{InvocationID: execution.InvocationID}}
	regularRuntime := &fakePhaseRuntime{receipt: phasepkg.OutputReceipt{InvocationID: "regular"}}
	runtimeRouter := productionTargetedVerifyRuntimeRouter{Regular: regularRuntime, Targeted: targetedRuntime, Registry: registry}
	if got, err := runtimeRouter.SubmitPhaseOutput(ctx, execution.InvocationID, phasepkg.PhaseOutput{Phase: "verify", ReportRef: "art"}); err != nil || got.InvocationID != execution.InvocationID {
		t.Fatalf("targeted runtime route = %#v, %v", got, err)
	}
	if got, err := runtimeRouter.SubmitPhaseOutput(ctx, "inv-unknown", phasepkg.PhaseOutput{Phase: "verify", ReportRef: "art"}); err != nil || got.InvocationID != "regular" {
		t.Fatalf("regular runtime route = %#v, %v", got, err)
	}
	if targetedRuntime.submits != 1 || regularRuntime.submits != 1 {
		t.Fatalf("runtime submits targeted=%d regular=%d", targetedRuntime.submits, regularRuntime.submits)
	}
}

func TestTargetedVerifyRegistryIdempotentRegisterPreservesTerminalAndActiveSnapshot(t *testing.T) {
	req := targetedVerifyRequest(t)
	registry, execution := registerTargetedVerifyProjection(t, req)
	entry, ok := registry.ByInvocation(execution.InvocationID)
	if !ok {
		t.Fatal("registered invocation missing")
	}
	if got := registry.ActiveInvocations(); len(got) != 1 || got[0] != execution.InvocationID {
		t.Fatalf("active invocations before terminal = %#v", got)
	}
	registry.MarkTerminal(execution.InvocationID)
	if err := registry.RegisterBinding(context.Background(), req, entry.Command, entry.Binding); err != nil {
		t.Fatal(err)
	}
	if !registry.IsTerminal(execution.InvocationID) {
		t.Fatal("idempotent register cleared terminal state")
	}
	if got := registry.ActiveInvocations(); len(got) != 0 {
		t.Fatalf("terminal invocation leaked into active snapshot: %#v", got)
	}
}

func TestTargetedVerifyBundleReusesInjectedRegistryAndProjector(t *testing.T) {
	registry := newProductionTargetedVerifyRegistry()
	projector := &productionTargetedVerifyWorkspaceProjector{registry: registry}
	bundle, err := buildProductionTargetedVerifyBundle(productionTargetedVerifyBundleOptions{
		ProjectID: "project-a",
		Contracts: targetedVerifyContractStoreFunc(func(context.Context, kernel.TaskID) (taskmanager.TaskContract, error) {
			return taskmanager.TaskContract{}, nil
		}),
		Invocations:     &fakeInvocationStore{},
		Assembler:       &runtimepkg.Assembler{},
		Host:            &fakePhaseHost{},
		Recovery:        &fakeRecoveryStore{},
		Contexts:        fakeContextRuntime{},
		ArtifactRouter:  fakeArtifactRouter{},
		OutputValidator: targetedVerifyOutputValidatorFunc(func(context.Context, kernel.TaskID, phasepkg.PhaseOutput) error { return nil }),
		WorkspaceSync:   fakeWorkspaceSync{},
		Registry:        registry,
		Projector:       projector,
	})
	if err != nil {
		t.Fatal(err)
	}
	if bundle.Registry != registry || bundle.Projector != projector || bundle.Bindings.registry != registry {
		t.Fatalf("bundle did not reuse injected registry/projector: %#v", bundle)
	}
}

type targetedVerifyOutputValidatorFunc func(context.Context, kernel.TaskID, phasepkg.PhaseOutput) error

func (f targetedVerifyOutputValidatorFunc) ValidateTargetedVerifyOutput(ctx context.Context, taskID kernel.TaskID, output phasepkg.PhaseOutput) error {
	return f(ctx, taskID, output)
}

func TestTargetedVerifyOutputByCommandReturnsTerminalFailureAfterFail(t *testing.T) {
	req := targetedVerifyRequest(t)
	registry, execution := registerTargetedVerifyProjection(t, req)
	runtime := &productionTargetedVerifyPhaseRuntime{
		controller:    phasepkg.NewController(phasepkg.Config{}),
		registry:      registry,
		workspaceSync: fakeWorkspaceSync{},
	}
	runtime.registry.MarkFailed(execution.InvocationID, kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "boom", Recoverable: true})
	entry, ok := registry.ByInvocation(execution.InvocationID)
	if !ok {
		t.Fatal("registered invocation missing")
	}
	receipt, done, err := runtime.OutputByCommand(context.Background(), entry.Command.ID)
	if !done || receipt.InvocationID != "" || !kernel.IsCode(err, kernel.CodeExecutorUnavailable) {
		t.Fatalf("failed output by command = receipt=%#v done=%v err=%v", receipt, done, err)
	}
	if got := registry.ActiveInvocations(); len(got) != 0 {
		t.Fatalf("failed invocation leaked into active snapshot: %#v", got)
	}
}

func TestTargetedVerifySubmitOrchestrationIntentPersistsTrustedProposal(t *testing.T) {
	req := targetedVerifyRequest(t)
	registry, execution := registerTargetedVerifyProjection(t, req)
	entry, ok := registry.ByInvocation(execution.InvocationID)
	if !ok {
		t.Fatal("registered invocation missing")
	}
	invocation := runtimepkg.Invocation{
		ID: execution.InvocationID, ProjectID: req.Candidate.ProjectID, TaskID: req.Candidate.TaskID,
		EndpointID: coordination.EndpointVerify, Generation: uint64(entry.Binding.Generation), Role: auth.RoleVerifier,
		Status: runtimepkg.InvocationRunning, BindingRef: entry.Binding.BindingRef, LeaseID: entry.Binding.LeaseRef,
	}
	dispatcher := &fakeTargetedProposalDispatcher{}
	runtime := &productionTargetedVerifyPhaseRuntime{
		projectID: req.Candidate.ProjectID, invocations: &fakeInvocationStore{invocation: invocation, ok: true},
		graph: fakeTargetedGraph{revision: 17}, proposals: dispatcher, artifactRouter: fakeArtifactRouter{},
		registry: registry,
	}
	intent := phasepkg.OrchestrationIntent{
		OrchestrationAdvice: phasepkg.OrchestrationReplan,
		DeliverySpecAdvice:  "split conflict fix from verification",
		ReportSpecAdvice:    "verify after conflict resolution",
		Rationale:           "latest main conflict requires task manager replan",
		EvidenceRefs:        []string{"art_conflict"},
	}
	proposal, err := runtime.SubmitOrchestrationIntent(context.Background(), auth.Principal{
		ProjectID: req.Candidate.ProjectID, Role: auth.RoleVerifier, TaskID: req.Candidate.TaskID, InvocationID: execution.InvocationID,
	}, auth.BoundScope{ProjectID: req.Candidate.ProjectID, TaskID: req.Candidate.TaskID, InvocationID: execution.InvocationID}, intent)
	if err != nil {
		t.Fatal(err)
	}
	if proposal.FromEndpoint.TaskID != req.Candidate.TaskID || proposal.FromEndpoint.EndpointID != coordination.EndpointVerify ||
		proposal.FromInvocationID != execution.InvocationID || proposal.BasedOnGraphRevision != 17 ||
		proposal.BasedOnWorkspaceRevision != req.LatestMainRevision || proposal.BasedOnInputRevision != entry.Binding.Inputs.InputRevision {
		t.Fatalf("proposal trusted fields mismatch: %#v", proposal)
	}
	if len(dispatcher.inputs) != 1 {
		t.Fatalf("dispatcher inputs = %d, want 1", len(dispatcher.inputs))
	}
	input := dispatcher.inputs[0]
	if input.Kind != "phase_orchestration" || input.TargetKind != "phase_orchestration" || input.TargetRef != proposal.ProposalID ||
		input.SeenRevision != 17 || input.SelectedEndpoint == nil || *input.SelectedEndpoint != proposal.FromEndpoint {
		t.Fatalf("persisted follow-up input mismatch: %#v", input)
	}
	var persisted productionTargetedVerifyProposalBoundary
	if err := json.Unmarshal(input.Payload, &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.ProposalID != proposal.ProposalID || persisted.Rationale != intent.Rationale ||
		persisted.SourceKind != productionTargetedVerifyProposalSource || persisted.CandidateID != req.Candidate.ID {
		t.Fatalf("persisted payload = %#v, want proposal %#v", persisted, proposal)
	}
	if _, done, err := runtime.OutputByCommand(context.Background(), entry.Command.ID); !done || !kernel.IsCode(err, kernel.CodeExecutorUnavailable) {
		t.Fatalf("output after replan proposal done=%v err=%v, want terminal executor_unavailable", done, err)
	}
	if _, err := runtime.AwaitInputs(context.Background(), execution.InvocationID, phasepkg.AwaitInputsRequest{}); !kernel.IsCode(err, kernel.CodeExecutorUnavailable) {
		t.Fatalf("await after replan proposal err=%v, want executor_unavailable", err)
	}
	if _, err := runtime.SubmitPhaseOutput(context.Background(), execution.InvocationID, phasepkg.PhaseOutput{Phase: "verify", ReportRef: "art"}); !kernel.IsCode(err, kernel.CodeExecutorUnavailable) {
		t.Fatalf("output submit after replan proposal err=%v, want executor_unavailable", err)
	}
}

func TestTargetedVerifySubmitOrchestrationIntentAcceptsDurableCapacityWait(t *testing.T) {
	req := targetedVerifyRequest(t)
	registry, execution := registerTargetedVerifyProjection(t, req)
	entry, ok := registry.ByInvocation(execution.InvocationID)
	if !ok {
		t.Fatal("registered invocation missing")
	}
	invocation := runtimepkg.Invocation{
		ID: execution.InvocationID, ProjectID: req.Candidate.ProjectID, TaskID: req.Candidate.TaskID,
		EndpointID: coordination.EndpointVerify, Generation: uint64(entry.Binding.Generation), Role: auth.RoleVerifier,
		Status: runtimepkg.InvocationRunning, BindingRef: entry.Binding.BindingRef, LeaseID: entry.Binding.LeaseRef,
	}
	dispatcher := &fakeTargetedProposalDispatcher{stored: persistedProductionInput{
		InputRef: "manager-input:durable", InvocationID: "tm-invocation", Status: "pending",
	}, err: kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "manager carrier is warming", Recoverable: true}}
	runtime := &productionTargetedVerifyPhaseRuntime{
		projectID: req.Candidate.ProjectID, invocations: &fakeInvocationStore{invocation: invocation, ok: true},
		graph: fakeTargetedGraph{revision: 17}, proposals: dispatcher, artifactRouter: fakeArtifactRouter{}, registry: registry,
	}
	intent := phasepkg.OrchestrationIntent{
		OrchestrationAdvice: phasepkg.OrchestrationReplan, DeliverySpecAdvice: "fresh execute round",
		ReportSpecAdvice: "verify after re-execution", Rationale: "conflict cannot be resolved without violating the task contract",
	}
	if _, err := runtime.SubmitOrchestrationIntent(context.Background(), auth.Principal{
		ProjectID: req.Candidate.ProjectID, Role: auth.RoleVerifier, TaskID: req.Candidate.TaskID, InvocationID: execution.InvocationID,
	}, auth.BoundScope{ProjectID: req.Candidate.ProjectID, TaskID: req.Candidate.TaskID, InvocationID: execution.InvocationID}, intent); err != nil {
		t.Fatalf("durable proposal with downstream capacity wait returned error: %v", err)
	}
	if _, done, err := runtime.OutputByCommand(context.Background(), entry.Command.ID); !done || !kernel.IsCode(err, kernel.CodeExecutorUnavailable) {
		t.Fatalf("proposal did not terminate targeted verify: done=%v err=%v", done, err)
	}
}

func TestTargetedVerifySubmitOrchestrationIntentRejectsForgedScope(t *testing.T) {
	req := targetedVerifyRequest(t)
	registry, execution := registerTargetedVerifyProjection(t, req)
	runtime := &productionTargetedVerifyPhaseRuntime{
		projectID: req.Candidate.ProjectID, invocations: &fakeInvocationStore{}, graph: fakeTargetedGraph{revision: 3},
		proposals: &fakeTargetedProposalDispatcher{}, artifactRouter: fakeArtifactRouter{}, registry: registry,
	}
	intent := phasepkg.OrchestrationIntent{OrchestrationAdvice: phasepkg.OrchestrationReplan, DeliverySpecAdvice: "delivery", ReportSpecAdvice: "report", Rationale: "replan"}
	if _, err := runtime.SubmitOrchestrationIntent(context.Background(), auth.Principal{
		ProjectID: req.Candidate.ProjectID, Role: auth.RoleVerifier, TaskID: "forged-task", InvocationID: execution.InvocationID,
	}, auth.BoundScope{ProjectID: req.Candidate.ProjectID, TaskID: "forged-task", InvocationID: execution.InvocationID}, intent); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("forged task scope err=%v, want forbidden", err)
	}
	if _, err := runtime.SubmitOrchestrationIntent(context.Background(), auth.Principal{
		ProjectID: req.Candidate.ProjectID, Role: auth.RoleExecutor, TaskID: req.Candidate.TaskID, InvocationID: execution.InvocationID,
	}, auth.BoundScope{ProjectID: req.Candidate.ProjectID, TaskID: req.Candidate.TaskID, InvocationID: execution.InvocationID}, intent); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("forged role err=%v, want forbidden", err)
	}
}

func TestTargetedVerifyRuntimeRouterRoutesProposalByRegistry(t *testing.T) {
	req := targetedVerifyRequest(t)
	registry, execution := registerTargetedVerifyProjection(t, req)
	targeted := &fakeProposalRuntime{proposal: phasepkg.OrchestrationProposal{FromInvocationID: execution.InvocationID}}
	regular := &fakeProposalRuntime{proposal: phasepkg.OrchestrationProposal{FromInvocationID: "regular"}}
	router := productionTargetedVerifyRuntimeRouter{
		Regular: &fakePhaseRuntime{}, RegularProposal: regular, Targeted: targeted, Registry: registry,
	}
	intent := phasepkg.OrchestrationIntent{OrchestrationAdvice: phasepkg.OrchestrationReplan, DeliverySpecAdvice: "delivery", ReportSpecAdvice: "report", Rationale: "replan"}
	if got, err := router.SubmitOrchestrationIntent(context.Background(), auth.Principal{InvocationID: execution.InvocationID}, auth.BoundScope{InvocationID: execution.InvocationID}, intent); err != nil || got.FromInvocationID != execution.InvocationID {
		t.Fatalf("targeted proposal route = %#v, %v", got, err)
	}
	if got, err := router.SubmitOrchestrationIntent(context.Background(), auth.Principal{InvocationID: "regular-invocation"}, auth.BoundScope{InvocationID: "regular-invocation"}, intent); err != nil || got.FromInvocationID != "regular" {
		t.Fatalf("regular proposal route = %#v, %v", got, err)
	}
	if targeted.calls != 1 || regular.calls != 1 {
		t.Fatalf("proposal calls targeted=%d regular=%d", targeted.calls, regular.calls)
	}
}

type targetedVerifyContractStoreFunc func(context.Context, kernel.TaskID) (taskmanager.TaskContract, error)

func (f targetedVerifyContractStoreFunc) TaskContract(ctx context.Context, taskID kernel.TaskID) (taskmanager.TaskContract, error) {
	return f(ctx, taskID)
}

func targetedVerifyRequest(t *testing.T) mergequeue.TargetedVerifyRequest {
	t.Helper()
	return mergequeue.TargetedVerifyRequest{
		Candidate: mergequeue.Candidate{
			ID:                "candidate-a",
			ProjectID:         "project-a",
			TaskID:            "task-a",
			VerifyResultRef:   "art_verify",
			DiffArtifactRef:   "art_diff",
			TargetRepository:  filepath.Join(t.TempDir(), "repo.git"),
			TargetBranch:      "dev",
			CandidateRevision: "candidate-sha",
			EvidenceRefs:      []evidence.ArtifactID{"art_prior"},
		},
		WorkspaceRoot:      t.TempDir(),
		LatestMainRevision: "main-sha",
	}
}

func registerTargetedVerifyProjection(t *testing.T, req mergequeue.TargetedVerifyRequest) (*productionTargetedVerifyRegistry, adapter.AgentTeamsExecutionRef) {
	t.Helper()
	registry := newProductionTargetedVerifyRegistry()
	binding := phasepkg.BindingSnapshot{
		ProjectID:         req.Candidate.ProjectID,
		ActorPrincipalID:  "phase-agent://project-a/task-a/verify/1",
		TaskID:            req.Candidate.TaskID,
		EndpointID:        coordination.EndpointVerify,
		Generation:        productionTargetedVerifyGeneration(req),
		BindingRef:        productionTargetedVerifyBindingRef(req),
		LeaseRef:          productionTargetedVerifyLeaseRef(req),
		WorkspaceRef:      req.WorkspaceRoot,
		WorkspaceRevision: req.LatestMainRevision,
		Inputs:            phasepkg.PhaseInputSet{InputRevision: productionTargetedVerifyInputRevision(req)},
	}
	command := productionTargetedVerifyCommand(req, binding)
	if err := registry.RegisterBinding(context.Background(), req, command, binding); err != nil {
		t.Fatal(err)
	}
	return registry, adapter.AgentTeamsExecutionRef{
		InvocationID:     deterministicPhaseInvocationID(command.ID),
		AgentTeamsTaskID: "agentteams-task-a",
		HostRef:          "host-a",
	}
}

func writeFile(t *testing.T, root, rel string, body []byte) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatal(err)
	}
}

func readFile(t *testing.T, root, rel string) []byte {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func fileForProjection(path string, body []byte) adapter.ExecutionFile {
	sum := sha256.Sum256(body)
	return adapter.ExecutionFile{Path: path, Mode: 0o644, Content: append([]byte(nil), body...), SHA256: hex.EncodeToString(sum[:])}
}

func fileSignature(body []byte, mode uint32) productionTargetedVerifyFile {
	sum := sha256.Sum256(body)
	return productionTargetedVerifyFile{Mode: productionTargetedVerifyComparableMode(mode), SHA256: hex.EncodeToString(sum[:])}
}

func cloneProjection(in adapter.ExecutionFileProjection) adapter.ExecutionFileProjection {
	out := adapter.ExecutionFileProjection{Manifest: append([]byte(nil), in.Manifest...), Files: make([]adapter.ExecutionFile, len(in.Files))}
	for i := range in.Files {
		out.Files[i] = adapter.ExecutionFile{
			Path:    in.Files[i].Path,
			Mode:    in.Files[i].Mode,
			Content: append([]byte(nil), in.Files[i].Content...),
			SHA256:  in.Files[i].SHA256,
		}
	}
	return out
}

func replaceProjectionFile(t *testing.T, projection *adapter.ExecutionFileProjection, path string, body []byte) {
	t.Helper()
	for i := range projection.Files {
		if projection.Files[i].Path == path {
			projection.Files[i] = fileForProjection(path, body)
			return
		}
	}
	t.Fatalf("projection file %s not found", path)
}

func removeProjectionFile(t *testing.T, projection *adapter.ExecutionFileProjection, path string) {
	t.Helper()
	for i := range projection.Files {
		if projection.Files[i].Path == path {
			projection.Files = append(projection.Files[:i], projection.Files[i+1:]...)
			return
		}
	}
	t.Fatalf("projection file %s not found", path)
}

type fakeExecutionProjector struct {
	owns    bool
	export  adapter.ExecutionFileProjection
	exports int
	imports int
}

func (f *fakeExecutionProjector) OwnsExecution(context.Context, adapter.AgentTeamsExecutionRef) (bool, error) {
	return f.owns, nil
}

func (f *fakeExecutionProjector) ExportExecutionFiles(context.Context, adapter.AgentTeamsExecutionRef) (adapter.ExecutionFileProjection, error) {
	f.exports++
	return f.export, nil
}

func (f *fakeExecutionProjector) ImportExecutionFiles(context.Context, adapter.AgentTeamsExecutionRef, adapter.ExecutionFileProjection) (adapter.ExecutionWorkspaceCheckpoint, error) {
	f.imports++
	return adapter.ExecutionWorkspaceCheckpoint{WorkspaceRevision: "regular"}, nil
}

type fakePhaseRuntime struct {
	receipt phasepkg.OutputReceipt
	submits int
	awaits  int
}

func (f *fakePhaseRuntime) AwaitInputs(context.Context, kernel.InvocationID, phasepkg.AwaitInputsRequest) (phasepkg.InputWaitResult, error) {
	f.awaits++
	return phasepkg.InputWaitResult{}, nil
}

func (f *fakePhaseRuntime) SubmitPhaseOutput(context.Context, kernel.InvocationID, phasepkg.PhaseOutput) (phasepkg.OutputReceipt, error) {
	f.submits++
	return f.receipt, nil
}

type fakeInvocationStore struct {
	invocation runtimepkg.Invocation
	ok         bool
}

func (*fakeInvocationStore) Create(context.Context, runtimepkg.Invocation) error {
	return nil
}

func (f *fakeInvocationStore) Get(_ context.Context, invocationID kernel.InvocationID) (runtimepkg.Invocation, bool, error) {
	if f.ok && f.invocation.ID == invocationID {
		return f.invocation, true, nil
	}
	return runtimepkg.Invocation{}, false, nil
}

func (*fakeInvocationStore) GetByLease(context.Context, kernel.LeaseID) (runtimepkg.Invocation, bool, error) {
	return runtimepkg.Invocation{}, false, nil
}

func (*fakeInvocationStore) Transition(context.Context, kernel.InvocationID, runtimepkg.InvocationStatus, runtimepkg.InvocationStatus) error {
	return nil
}

type fakePhaseHost struct{}

func (*fakePhaseHost) Dispatch(context.Context, phasepkg.DispatchRequest) error  { return nil }
func (*fakePhaseHost) Rehydrate(context.Context, phasepkg.DispatchRequest) error { return nil }
func (*fakePhaseHost) Suspend(context.Context, kernel.InvocationID) error        { return nil }
func (*fakePhaseHost) Stop(context.Context, phasepkg.StopRequest) (phasepkg.StopResult, error) {
	return phasepkg.StopResult{}, nil
}
func (*fakePhaseHost) Fence(context.Context, kernel.InvocationID) error  { return nil }
func (*fakePhaseHost) Revoke(context.Context, kernel.InvocationID) error { return nil }

type fakeRecoveryStore struct{}

func (*fakeRecoveryStore) RecordActiveInvocation(context.Context, phasepkg.ActiveInvocation) error {
	return nil
}
func (*fakeRecoveryStore) RecoverActiveInvocation(context.Context, phasepkg.PhaseCommand, phasepkg.BindingSnapshot) (phasepkg.ActiveInvocation, bool, error) {
	return phasepkg.ActiveInvocation{}, false, nil
}
func (*fakeRecoveryStore) RecordOutputReceipt(context.Context, phasepkg.ActiveInvocation, phasepkg.OutputReceipt) error {
	return nil
}
func (*fakeRecoveryStore) GetOutputReceipt(context.Context, kernel.InvocationID, string) (phasepkg.OutputReceipt, bool, error) {
	return phasepkg.OutputReceipt{}, false, nil
}
func (*fakeRecoveryStore) RecordStopEvidence(context.Context, phasepkg.ActiveInvocation, phasepkg.PhaseCommand, phasepkg.StopResult) error {
	return nil
}
func (*fakeRecoveryStore) GetStopEvidence(context.Context, kernel.InvocationID, string) (phasepkg.StopResult, bool, error) {
	return phasepkg.StopResult{}, false, nil
}
func (*fakeRecoveryStore) ClearActiveInvocation(context.Context, kernel.InvocationID) error {
	return nil
}
func (*fakeRecoveryStore) ValidateResume(context.Context, phasepkg.PhaseCommand, phasepkg.BindingSnapshot) error {
	return nil
}

type fakeContextRuntime struct{}

func (fakeContextRuntime) EnsureInitialSlice(context.Context, auth.Principal, []string) (contextgraph.ContextSlice, error) {
	return contextgraph.ContextSlice{}, nil
}
func (fakeContextRuntime) InspectSubscriptions(context.Context, auth.Principal, kernel.InvocationID) ([]contextgraph.SubscriptionInspection, error) {
	return nil, nil
}
func (fakeContextRuntime) MaterializeRuntimeContext(context.Context, auth.Principal) (contextgraph.ContextSlice, error) {
	return contextgraph.ContextSlice{}, nil
}
func (fakeContextRuntime) ListTaskCandidates(context.Context, auth.Principal) (contextgraph.TaskMemoryBufferView, error) {
	return contextgraph.TaskMemoryBufferView{}, nil
}
func (fakeContextRuntime) EndInvocation(context.Context, auth.Principal, kernel.InvocationID) error {
	return nil
}

type fakeArtifactRouter struct{}

func (fakeArtifactRouter) Route(context.Context, phasepkg.ActiveInvocation, string) (string, error) {
	return "art_routed", nil
}

type fakeWorkspaceSync struct{}

func (fakeWorkspaceSync) SyncWorkspace(context.Context, kernel.InvocationID) (adapter.ExecutionWorkspaceCheckpoint, error) {
	return adapter.ExecutionWorkspaceCheckpoint{}, nil
}

type fakeTargetedGraph struct {
	revision kernel.Revision
}

func (f fakeTargetedGraph) Latest(context.Context, kernel.ProjectID) (coordination.GraphSnapshot, error) {
	return coordination.GraphSnapshot{Revision: f.revision}, nil
}

type fakeTargetedProposalDispatcher struct {
	inputs []productionInput
	stored persistedProductionInput
	err    error
}

func (f *fakeTargetedProposalDispatcher) persistAndDispatch(_ context.Context, input productionInput) (persistedProductionInput, error) {
	f.inputs = append(f.inputs, input)
	if f.stored.InputRef != "" || f.err != nil {
		return f.stored, f.err
	}
	return persistedProductionInput{InputRef: "manager-input:" + input.RequestID, InvocationID: "tm-invocation"}, nil
}

type fakeProposalRuntime struct {
	proposal phasepkg.OrchestrationProposal
	calls    int
}

func (f *fakeProposalRuntime) SubmitOrchestrationIntent(context.Context, auth.Principal, auth.BoundScope, phasepkg.OrchestrationIntent) (phasepkg.OrchestrationProposal, error) {
	f.calls++
	return f.proposal, nil
}

func (f *fakeProposalRuntime) AwaitInputs(context.Context, kernel.InvocationID, phasepkg.AwaitInputsRequest) (phasepkg.InputWaitResult, error) {
	return phasepkg.InputWaitResult{}, nil
}

func (f *fakeProposalRuntime) SubmitPhaseOutput(context.Context, kernel.InvocationID, phasepkg.PhaseOutput) (phasepkg.OutputReceipt, error) {
	return phasepkg.OutputReceipt{}, nil
}
