package mcpapi

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/evidence"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

type fakeEvidenceRegistrar struct {
	request evidence.RegisterArtifact
	calls   int
}

func (f *fakeEvidenceRegistrar) Register(_ context.Context, request evidence.RegisterArtifact) (evidence.Artifact, error) {
	f.request = request
	f.calls++
	return evidence.Artifact{ID: "artifact-1", Type: request.Type}, nil
}

func TestEvidenceToolInjectsAuthenticatedOwnership(t *testing.T) {
	registrar := &fakeEvidenceRegistrar{}
	registry, err := NewRegistry(EvidenceToolSpec(registrar))
	if err != nil {
		t.Fatal(err)
	}
	principal := principalWithTools(auth.RoleVerifier, auth.ToolEvidenceRegister)
	payload := evidenceRegisterRequest{Type: evidence.ArtifactTestOutput, Path: "evidence/test.log", ContentType: "text/plain", Body: "ok"}
	if _, err := registry.Invoke(context.Background(), principal, auth.ToolEvidenceRegister, auth.Scope{ProjectID: "project-a", TaskID: "task-a"}, mustJSON(t, payload)); err != nil {
		t.Fatal(err)
	}
	if registrar.request.ProjectID != principal.ProjectID || registrar.request.TaskID != principal.TaskID || registrar.request.AgentInvocationID != principal.InvocationID {
		t.Fatalf("trusted evidence ownership=%#v", registrar.request)
	}
	if string(registrar.request.Body) != "ok" || registrar.request.Path != "evidence/test.log" {
		t.Fatalf("evidence content=%#v", registrar.request)
	}
}

func TestEvidenceToolRejectsCallerSuppliedOwnership(t *testing.T) {
	registrar := &fakeEvidenceRegistrar{}
	registry, err := NewRegistry(EvidenceToolSpec(registrar))
	if err != nil {
		t.Fatal(err)
	}
	principal := principalWithTools(auth.RoleVerifier, auth.ToolEvidenceRegister)
	for _, payload := range []json.RawMessage{
		json.RawMessage(`{"type":"test_output","body":"ok","task_id":"task-other"}`),
		json.RawMessage(`{"type":"test_output","body":"ok","agent_invocation_id":"inv-other"}`),
		json.RawMessage(`{"type":"test_output","body":"ok","project_id":"project-other"}`),
	} {
		if _, err := registry.Invoke(context.Background(), principal, auth.ToolEvidenceRegister, auth.Scope{ProjectID: "project-a", TaskID: "task-a"}, payload); !kernel.IsCode(err, kernel.CodeInvalidRequest) {
			t.Errorf("spoof evidence err=%v, want invalid_request", err)
		}
	}
	if registrar.calls != 0 {
		t.Fatalf("spoofed evidence reached registrar %d times", registrar.calls)
	}
}

func TestEvidenceToolDescriptionGuidesTargetedVerifyGeneratedReport(t *testing.T) {
	description := toolDescription(auth.ToolEvidenceRegister)
	for _, required := range []string{
		"type=generated_report",
		"content_type=application/json",
		"threadmill.targeted_verify.v1",
		"report_ref",
	} {
		if !strings.Contains(description, required) {
			t.Fatalf("evidence.register description missing %q: %s", required, description)
		}
	}
}
