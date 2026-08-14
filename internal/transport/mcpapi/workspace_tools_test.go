package mcpapi

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/workspace"
)

type fakeWorkspacePort struct {
	invocationID kernel.InvocationID
	path         workspace.PathRequest
	write        workspace.WriteRequest
	run          workspace.RunRequest
	calls        int
}

func (f *fakeWorkspacePort) List(_ context.Context, invocationID kernel.InvocationID, req workspace.PathRequest) (workspace.ListResult, error) {
	f.invocationID, f.path = invocationID, req
	f.calls++
	return workspace.ListResult{Entries: []workspace.Entry{{Path: "workspace/a.txt", Kind: "file"}}}, nil
}

func (f *fakeWorkspacePort) Read(_ context.Context, invocationID kernel.InvocationID, req workspace.PathRequest) (workspace.ReadResult, error) {
	f.invocationID, f.path = invocationID, req
	f.calls++
	return workspace.ReadResult{Path: req.Path, Content: "content"}, nil
}

func (f *fakeWorkspacePort) WritePlan(_ context.Context, invocationID kernel.InvocationID, req workspace.WriteRequest) (workspace.WriteResult, error) {
	f.invocationID, f.write = invocationID, req
	f.calls++
	return workspace.WriteResult{Path: req.Path}, nil
}

func (f *fakeWorkspacePort) Write(_ context.Context, invocationID kernel.InvocationID, req workspace.WriteRequest) (workspace.WriteResult, error) {
	f.invocationID, f.write = invocationID, req
	f.calls++
	return workspace.WriteResult{Path: req.Path}, nil
}

func (f *fakeWorkspacePort) Run(_ context.Context, invocationID kernel.InvocationID, req workspace.RunRequest) (workspace.RunResult, error) {
	f.invocationID, f.run = invocationID, req
	f.calls++
	return workspace.RunResult{Command: req.Command}, nil
}

func (f *fakeWorkspacePort) Diff(_ context.Context, invocationID kernel.InvocationID, req workspace.PathRequest) (workspace.DiffResult, error) {
	f.invocationID, f.path = invocationID, req
	f.calls++
	return workspace.DiffResult{}, nil
}

func TestWorkspaceToolsUseOnlyRuntimeBoundInvocation(t *testing.T) {
	port := &fakeWorkspacePort{}
	registry, err := NewRegistry(WorkspaceToolSpecs(port)...)
	if err != nil {
		t.Fatal(err)
	}
	principal := principalWithTools(auth.RoleExecutor, auth.ToolWorkspaceWrite, auth.ToolWorkspaceRun)
	scope := auth.Scope{ProjectID: "project-a", TaskID: "task-a"}
	write := workspace.WriteRequest{Path: "workspace/a.txt", Content: "hello"}
	if _, err := registry.Invoke(context.Background(), principal, auth.ToolWorkspaceWrite, scope, mustJSON(t, write)); err != nil {
		t.Fatal(err)
	}
	if port.invocationID != principal.InvocationID || port.write != write {
		t.Fatalf("invocation=%q write=%#v", port.invocationID, port.write)
	}
	run := workspace.RunRequest{Command: []string{"go", "test", "./..."}, WorkDir: "workspace"}
	if _, err := registry.Invoke(context.Background(), principal, auth.ToolWorkspaceRun, scope, mustJSON(t, run)); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(port.run, run) {
		t.Fatalf("run=%#v", port.run)
	}
}

func TestWorkspaceToolsRejectAuthorityAndUnknownFields(t *testing.T) {
	port := &fakeWorkspacePort{}
	registry, err := NewRegistry(WorkspaceToolSpecs(port)...)
	if err != nil {
		t.Fatal(err)
	}
	principal := principalWithTools(auth.RoleExecutor, auth.ToolWorkspaceWrite, auth.ToolWorkspaceRun)
	payloads := []struct {
		tool    auth.Tool
		payload json.RawMessage
	}{
		{auth.ToolWorkspaceWrite, json.RawMessage(`{"path":"workspace/a.txt","content":"x","task_id":"task-other"}`)},
		{auth.ToolWorkspaceWrite, json.RawMessage(`{"path":"workspace/a.txt","content":"x","binding_ref":"binding-other"}`)},
		{auth.ToolWorkspaceRun, json.RawMessage(`{"command":["go","test"],"invocation_id":"inv-other"}`)},
	}
	for _, test := range payloads {
		if _, err := registry.Invoke(context.Background(), principal, test.tool, auth.Scope{ProjectID: "project-a", TaskID: "task-a"}, test.payload); !kernel.IsCode(err, kernel.CodeInvalidRequest) {
			t.Errorf("%s payload err=%v, want invalid_request", test.tool, err)
		}
	}
	if port.calls != 0 {
		t.Fatalf("spoofed workspace payload reached port %d times", port.calls)
	}
}
