package workspace

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

func TestNativeSnapshotPhaseBoundaries(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	service, binding := createNativeSnapshotBinding(t, []string{"workspace"})
	if _, err := service.BindPhase(ctx, binding.ID, PhasePlan, "inv-plan"); err != nil {
		t.Fatalf("bind plan: %v", err)
	}
	planSnapshot := mustExportNativeSnapshot(t, service, "inv-plan")
	planSnapshot.Files = append(planSnapshot.Files, nativeFile("plan/notes.md", "plan\n"))
	if _, err := service.ImportNativeSnapshot(ctx, "inv-plan", planSnapshot); err != nil {
		t.Fatalf("import plan snapshot: %v", err)
	}
	if got := readFile(t, filepath.Join(binding.Root, "plan", "notes.md")); got != "plan\n" {
		t.Fatalf("plan file = %q", got)
	}
	badPlan := mustExportNativeSnapshot(t, service, "inv-plan")
	badPlan.Files = append(badPlan.Files, nativeFile("workspace/app.go", "package main\n"))
	if _, err := service.ImportNativeSnapshot(ctx, "inv-plan", badPlan); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("plan import outside plan error = %v, want forbidden", err)
	}
	if _, err := os.Stat(filepath.Join(binding.Root, "workspace", "app.go")); !os.IsNotExist(err) {
		t.Fatalf("forbidden plan import created workspace/app.go: %v", err)
	}
	if _, err := service.CompletePhase(ctx, binding.ID, PhasePlan, "inv-plan"); err != nil {
		t.Fatalf("complete plan: %v", err)
	}

	if _, err := service.BindPhase(ctx, binding.ID, PhaseExecute, "inv-execute"); err != nil {
		t.Fatalf("bind execute: %v", err)
	}
	executeSnapshot := mustExportNativeSnapshot(t, service, "inv-execute")
	executeSnapshot.Files = append(executeSnapshot.Files, nativeFile("workspace/app.go", "package main\n"))
	if _, err := service.ImportNativeSnapshot(ctx, "inv-execute", executeSnapshot); err != nil {
		t.Fatalf("import execute snapshot: %v", err)
	}
	if got := readFile(t, filepath.Join(binding.Root, "workspace", "app.go")); got != "package main\n" {
		t.Fatalf("execute file = %q", got)
	}
	badExecute := mustExportNativeSnapshot(t, service, "inv-execute")
	badExecute.Files = replaceNativeFile(badExecute.Files, nativeFile("README.md", "changed\n"))
	if _, err := service.ImportNativeSnapshot(ctx, "inv-execute", badExecute); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("execute import outside allowed dirs error = %v, want forbidden", err)
	}
	if got := normalizeNewlines(readFile(t, filepath.Join(binding.Root, "README.md"))); got != "seed\n" {
		t.Fatalf("forbidden execute import changed README.md to %q", got)
	}
	if _, err := service.CompletePhase(ctx, binding.ID, PhaseExecute, "inv-execute"); err != nil {
		t.Fatalf("complete execute: %v", err)
	}

	if _, err := service.BindPhase(ctx, binding.ID, PhaseVerify, "inv-verify"); err != nil {
		t.Fatalf("bind verify: %v", err)
	}
	beforeVerify, err := service.Get(ctx, binding.ID)
	if err != nil {
		t.Fatalf("get pre-verify binding: %v", err)
	}
	verifySnapshot := mustExportNativeSnapshot(t, service, "inv-verify")
	verifySnapshot.Files = append(verifySnapshot.Files, nativeFile("evidence/tests.txt", "ok\n"))
	verified, err := service.ImportNativeSnapshot(ctx, "inv-verify", verifySnapshot)
	if err != nil {
		t.Fatalf("import verify snapshot: %v", err)
	}
	if verified.CurrentRevision != beforeVerify.CurrentRevision {
		t.Fatalf("verify scratch evidence changed candidate revision from %q to %q", beforeVerify.CurrentRevision, verified.CurrentRevision)
	}
	if !reflect.DeepEqual(verified.ObservedWrites, beforeVerify.ObservedWrites) {
		t.Fatalf("verify scratch evidence changed candidate writes from %#v to %#v", beforeVerify.ObservedWrites, verified.ObservedWrites)
	}
	if _, err := os.Stat(filepath.Join(binding.Root, "evidence", "tests.txt")); !os.IsNotExist(err) {
		t.Fatalf("verify scratch evidence entered authoritative candidate workspace: %v", err)
	}
	badVerify := mustExportNativeSnapshot(t, service, "inv-verify")
	badVerify.Files = replaceNativeFile(badVerify.Files, nativeFile("workspace/app.go", "tampered\n"))
	if _, err := service.ImportNativeSnapshot(ctx, "inv-verify", badVerify); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("verify import outside evidence error = %v, want forbidden", err)
	}
	if got := readFile(t, filepath.Join(binding.Root, "workspace", "app.go")); got != "package main\n" {
		t.Fatalf("forbidden verify import changed workspace/app.go to %q", got)
	}
}

func TestNativeSnapshotSeparatesPhaseArtifactsFromCandidateWrites(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	service, binding := createNativeSnapshotBinding(t, nil)
	if _, err := service.BindPhase(ctx, binding.ID, PhasePlan, "inv-plan"); err != nil {
		t.Fatalf("bind plan: %v", err)
	}
	planSnapshot := mustExportNativeSnapshot(t, service, "inv-plan")
	planSnapshot.Files = append(planSnapshot.Files,
		nativeFile("plan/plan.md", "# plan\n"),
		nativeFile("plan/declared-writes.json", `{"files":["retry/policy.go"]}`),
	)
	planned, err := service.ImportNativeSnapshot(ctx, "inv-plan", planSnapshot)
	if err != nil {
		t.Fatalf("import plan snapshot: %v", err)
	}
	if len(planned.ObservedWrites.Files) != 0 {
		t.Fatalf("plan artifacts entered candidate observed writes: %v", planned.ObservedWrites.Files)
	}
	if _, err := service.CompletePhase(ctx, binding.ID, PhasePlan, "inv-plan", planned.Revision); err != nil {
		t.Fatalf("complete plan: %v", err)
	}
	planned, err = service.Get(ctx, binding.ID)
	if err != nil {
		t.Fatal(err)
	}
	planned, err = service.AuthorizeExecuteWrites(ctx, binding.ID, WriteSet{Files: []string{"retry/policy.go"}}, planned.Revision)
	if err != nil {
		t.Fatalf("authorize execute writes: %v", err)
	}
	if _, err := service.BindPhase(ctx, binding.ID, PhaseExecute, "inv-execute", planned.Revision); err != nil {
		t.Fatalf("bind execute: %v", err)
	}
	executeSnapshot := mustExportNativeSnapshot(t, service, "inv-execute")
	executeSnapshot.Files = append(executeSnapshot.Files, nativeFile("retry/policy.go", "package retry\n"))
	executed, err := service.ImportNativeSnapshot(ctx, "inv-execute", executeSnapshot)
	if err != nil {
		t.Fatalf("import execute snapshot: %v", err)
	}
	if !reflect.DeepEqual(executed.ObservedWrites.Files, []string{"retry/policy.go"}) {
		t.Fatalf("candidate observed writes = %v, want execute code only", executed.ObservedWrites.Files)
	}
}

func TestNativeSnapshotRejectsSourceDeletionAndRetainsFile(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	service, binding := createNativeSnapshotBinding(t, []string{"workspace"})
	if _, err := service.BindPhase(ctx, binding.ID, PhasePlan, "inv-plan"); err != nil {
		t.Fatalf("bind plan: %v", err)
	}
	snapshot := mustExportNativeSnapshot(t, service, "inv-plan")
	snapshot.Files = withoutNativeFile(snapshot.Files, "README.md")
	if _, err := service.ImportNativeSnapshot(ctx, "inv-plan", snapshot); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("source deletion error = %v, want forbidden", err)
	}
	if got := normalizeNewlines(readFile(t, filepath.Join(binding.Root, "README.md"))); got != "seed\n" {
		t.Fatalf("README.md after rejected deletion = %q", got)
	}
}

func TestExportNativeSnapshotExcludesProtectedPaths(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	service, binding := createNativeSnapshotBinding(t, []string{"workspace"})
	bindCompletedPlan(t, service, binding.ID)
	writeFile(t, filepath.Join(binding.Root, ".env"), "secret\n")
	writeFile(t, filepath.Join(binding.Root, "logs", "agent.log"), "secret\n")
	writeFile(t, filepath.Join(binding.Root, "workspace", "visible.txt"), "visible\n")
	if _, err := service.BindPhase(ctx, binding.ID, PhaseExecute, "inv-execute"); err != nil {
		t.Fatalf("bind execute: %v", err)
	}
	snapshot := mustExportNativeSnapshot(t, service, "inv-execute")
	paths := nativeSnapshotPaths(snapshot)
	for _, forbidden := range []string{".env", "logs/agent.log", ".git/config"} {
		if containsString(paths, forbidden) {
			t.Fatalf("export included protected path %q in %v", forbidden, paths)
		}
	}
	if !containsString(paths, "README.md") || !containsString(paths, "workspace/visible.txt") {
		t.Fatalf("export missing expected safe files: %v", paths)
	}
}

func TestImportNativeSnapshotRejectsInvalidFiles(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	service, binding := createNativeSnapshotBinding(t, []string{"workspace"})
	bindCompletedPlan(t, service, binding.ID)
	if _, err := service.BindPhase(ctx, binding.ID, PhaseExecute, "inv-execute"); err != nil {
		t.Fatalf("bind execute: %v", err)
	}
	base := mustExportNativeSnapshot(t, service, "inv-execute")

	traversal := base
	traversal.Files = append(append([]NativeSnapshotFile(nil), base.Files...), nativeFile("../escape.txt", "x"))
	if _, err := service.ImportNativeSnapshot(ctx, "inv-execute", traversal); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("traversal import error = %v, want forbidden", err)
	}

	duplicate := base
	duplicate.Files = append(append([]NativeSnapshotFile(nil), base.Files...), nativeFile("workspace/a.txt", "a"), nativeFile("workspace/./a.txt", "b"))
	if _, err := service.ImportNativeSnapshot(ctx, "inv-execute", duplicate); !kernel.IsCode(err, kernel.CodeInvalidRequest) {
		t.Fatalf("duplicate import error = %v, want invalid request", err)
	}

	symlink := base
	link := nativeFile("workspace/link", "target")
	link.Mode = uint32(os.ModeSymlink | 0o777)
	symlink.Files = append(append([]NativeSnapshotFile(nil), base.Files...), link)
	if _, err := service.ImportNativeSnapshot(ctx, "inv-execute", symlink); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("symlink mode import error = %v, want forbidden", err)
	}

	hashMismatch := base
	badHash := nativeFile("workspace/hash.txt", "hash")
	different := sha256.Sum256([]byte("different"))
	badHash.SHA256 = hex.EncodeToString(different[:])
	hashMismatch.Files = append(append([]NativeSnapshotFile(nil), base.Files...), badHash)
	if _, err := service.ImportNativeSnapshot(ctx, "inv-execute", hashMismatch); !kernel.IsCode(err, kernel.CodeInvalidRequest) {
		t.Fatalf("hash mismatch import error = %v, want invalid request", err)
	}
}

func TestImportNativeSnapshotRejectsStaleProjectionRevision(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	service, binding := createNativeSnapshotBinding(t, []string{"workspace"})
	bindCompletedPlan(t, service, binding.ID)
	if _, err := service.BindPhase(ctx, binding.ID, PhaseExecute, "inv-execute"); err != nil {
		t.Fatalf("bind execute: %v", err)
	}
	stale := mustExportNativeSnapshot(t, service, "inv-execute")
	tools := NewAgentTools(service, "git")
	if _, err := tools.Write(ctx, "inv-execute", WriteRequest{Path: "workspace/authoritative.txt", Content: "newer\n"}); err != nil {
		t.Fatalf("write newer authoritative state: %v", err)
	}
	stale.Files = append(stale.Files, nativeFile("workspace/from-stale.txt", "stale\n"))
	if _, err := service.ImportNativeSnapshot(ctx, "inv-execute", stale); !kernel.IsCode(err, kernel.CodeRevisionConflict) {
		t.Fatalf("stale projection import error = %v, want revision conflict", err)
	}
	if _, err := os.Stat(filepath.Join(binding.Root, "workspace", "from-stale.txt")); !os.IsNotExist(err) {
		t.Fatalf("stale projection changed workspace: %v", err)
	}
}

func TestImportNativeSnapshotAllowsExactReplayAfterSuccessfulImport(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	service, binding := createNativeSnapshotBinding(t, []string{"workspace"})
	bindCompletedPlan(t, service, binding.ID)
	if _, err := service.BindPhase(ctx, binding.ID, PhaseExecute, "inv-execute"); err != nil {
		t.Fatalf("bind execute: %v", err)
	}
	snapshot := mustExportNativeSnapshot(t, service, "inv-execute")
	snapshot.Files = append(snapshot.Files, nativeFile("workspace/replayed.txt", "stable\n"))
	first, err := service.ImportNativeSnapshot(ctx, "inv-execute", snapshot)
	if err != nil {
		t.Fatalf("first import: %v", err)
	}
	replayed, err := service.ImportNativeSnapshot(ctx, "inv-execute", snapshot)
	if err != nil {
		t.Fatalf("exact replay: %v", err)
	}
	if replayed.Revision != first.Revision || replayed.CurrentRevision != first.CurrentRevision {
		t.Fatalf("replay binding = %#v, want revision/head %#v", replayed, first)
	}
}

func TestImportNativeSnapshotAddsModifiesDeletesAndRefreshesObservedWrites(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	service, binding := createNativeSnapshotBinding(t, []string{"workspace"})
	writeFile(t, filepath.Join(binding.Root, "workspace", "old.txt"), "old\n")
	writeFile(t, filepath.Join(binding.Root, "workspace", "modify.txt"), "before\n")
	if _, err := service.BindPhase(ctx, binding.ID, PhasePlan, "inv-plan"); err != nil {
		t.Fatalf("bind plan: %v", err)
	}
	if _, err := service.CompletePhase(ctx, binding.ID, PhasePlan, "inv-plan"); err != nil {
		t.Fatalf("complete plan: %v", err)
	}
	if _, err := service.BindPhase(ctx, binding.ID, PhaseExecute, "inv-execute"); err != nil {
		t.Fatalf("bind execute: %v", err)
	}

	snapshot := mustExportNativeSnapshot(t, service, "inv-execute")
	snapshot.Files = withoutNativeFile(snapshot.Files, "workspace/old.txt")
	snapshot.Files = replaceNativeFile(snapshot.Files, nativeFile("workspace/modify.txt", "after\n"))
	snapshot.Files = append(snapshot.Files, nativeFile("workspace/new.txt", "new\n"))
	updated, err := service.ImportNativeSnapshot(ctx, "inv-execute", snapshot)
	if err != nil {
		t.Fatalf("import execute native snapshot: %v", err)
	}
	if _, err := os.Stat(filepath.Join(binding.Root, "workspace", "old.txt")); !os.IsNotExist(err) {
		t.Fatalf("old.txt survived native deletion: %v", err)
	}
	if got := readFile(t, filepath.Join(binding.Root, "workspace", "modify.txt")); got != "after\n" {
		t.Fatalf("modify.txt = %q", got)
	}
	if got := readFile(t, filepath.Join(binding.Root, "workspace", "new.txt")); got != "new\n" {
		t.Fatalf("new.txt = %q", got)
	}
	wantObserved := []string{"workspace/modify.txt", "workspace/new.txt"}
	sort.Strings(updated.ObservedWrites.Files)
	if !reflect.DeepEqual(updated.ObservedWrites.Files, wantObserved) {
		t.Fatalf("observed writes = %v, want %v", updated.ObservedWrites.Files, wantObserved)
	}
	if updated.CurrentRevision == snapshot.WorkspaceRevision {
		t.Fatalf("native import did not create a new checkpoint commit: %q", updated.CurrentRevision)
	}
	if got := strings.TrimSpace(gitOutputForNativeTest(t, binding.Root, "show", updated.CurrentRevision+":workspace/new.txt")); got != "new" {
		t.Fatalf("checkpoint commit missing native file: %q", got)
	}
}

func gitOutputForNativeTest(t *testing.T, dir string, args ...string) string {
	t.Helper()
	output, err := gitOutput(context.Background(), dir, args...)
	if err != nil {
		t.Fatal(err)
	}
	return output
}

func createNativeSnapshotBinding(t *testing.T, allowedDirs []string) (*Service, Binding) {
	t.Helper()
	service := NewService()
	binding, err := service.CreateGitWorktree(context.Background(), CreateRequest{
		TaskID:         kernel.TaskID("task-native-" + strings.ReplaceAll(t.Name(), "/", "-")),
		Generation:     1,
		RepoPath:       seedBareRepo(t),
		WorktreeParent: t.TempDir(),
		AllowedDirs:    allowedDirs,
	})
	if err != nil {
		t.Fatalf("create binding: %v", err)
	}
	return service, binding
}

func mustExportNativeSnapshot(t *testing.T, service *Service, invocationID kernel.InvocationID) NativeSnapshot {
	t.Helper()
	snapshot, err := service.ExportNativeSnapshot(context.Background(), invocationID)
	if err != nil {
		t.Fatalf("export native snapshot: %v", err)
	}
	return snapshot
}

func bindCompletedPlan(t *testing.T, service *Service, bindingID kernel.BindingRef) {
	t.Helper()
	if _, err := service.BindPhase(context.Background(), bindingID, PhasePlan, "inv-plan-prereq"); err != nil {
		t.Fatalf("bind plan prerequisite: %v", err)
	}
	if _, err := service.CompletePhase(context.Background(), bindingID, PhasePlan, "inv-plan-prereq"); err != nil {
		t.Fatalf("complete plan prerequisite: %v", err)
	}
}

func nativeFile(path, body string) NativeSnapshotFile {
	content := []byte(body)
	sum := sha256.Sum256(content)
	return NativeSnapshotFile{
		Path:    path,
		Mode:    0o644,
		Content: content,
		SHA256:  hex.EncodeToString(sum[:]),
	}
}

func replaceNativeFile(files []NativeSnapshotFile, replacement NativeSnapshotFile) []NativeSnapshotFile {
	out := append([]NativeSnapshotFile(nil), files...)
	for index, file := range out {
		if file.Path == replacement.Path {
			out[index] = replacement
			return out
		}
	}
	return append(out, replacement)
}

func withoutNativeFile(files []NativeSnapshotFile, path string) []NativeSnapshotFile {
	out := make([]NativeSnapshotFile, 0, len(files))
	for _, file := range files {
		if file.Path != path {
			out = append(out, file)
		}
	}
	return out
}

func nativeSnapshotPaths(snapshot NativeSnapshot) []string {
	paths := make([]string, 0, len(snapshot.Files))
	for _, file := range snapshot.Files {
		paths = append(paths, file.Path)
	}
	sort.Strings(paths)
	return paths
}

func containsString(values []string, needle string) bool {
	for _, value := range values {
		if value == needle {
			return true
		}
	}
	return false
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(content)
}

func normalizeNewlines(value string) string {
	return strings.ReplaceAll(value, "\r\n", "\n")
}
