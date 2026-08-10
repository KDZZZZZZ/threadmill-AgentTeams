package workspace

import (
	"context"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

// PathRequest is always relative to the Workspace Binding resolved from the
// trusted InvocationID. It never carries a TaskID, BindingRef, or phase lease.
type PathRequest struct {
	Path string `json:"path,omitempty"`
}

type WriteRequest struct {
	Path    string `json:"path"`
	Content string `json:"content"`
}

// RunRequest is argv-shaped so the Workspace implementation can enforce its
// allow policy without first interpreting a caller-provided shell string.
type RunRequest struct {
	Command []string `json:"command"`
	WorkDir string   `json:"work_dir,omitempty"`
}

type Entry struct {
	Path string `json:"path"`
	Kind string `json:"kind"`
	Size int64  `json:"size,omitempty"`
}

type ListResult struct {
	Entries           []Entry `json:"entries"`
	WorkspaceRevision string  `json:"workspace_revision"`
}

type ReadResult struct {
	Path              string `json:"path"`
	Content           string `json:"content"`
	WorkspaceRevision string `json:"workspace_revision"`
}

type WriteResult struct {
	Path              string `json:"path"`
	WorkspaceRevision string `json:"workspace_revision"`
}

type RunResult struct {
	Command           []string `json:"command"`
	ExitCode          int      `json:"exit_code"`
	Stdout            string   `json:"stdout,omitempty"`
	Stderr            string   `json:"stderr,omitempty"`
	WorkspaceRevision string   `json:"workspace_revision"`
}

type DiffResult struct {
	Patch             string   `json:"patch"`
	ObservedWrites    WriteSet `json:"observed_writes"`
	WorkspaceRevision string   `json:"workspace_revision"`
}

// AgentToolPort is the formal Runtime-to-Workspace seam. Implementations must
// resolve binding, phase, AllowedDirs, Declared Write Set, and lease solely
// from invocationID; MCP payloads cannot expand authority.
type AgentToolPort interface {
	List(context.Context, kernel.InvocationID, PathRequest) (ListResult, error)
	Read(context.Context, kernel.InvocationID, PathRequest) (ReadResult, error)
	WritePlan(context.Context, kernel.InvocationID, WriteRequest) (WriteResult, error)
	Write(context.Context, kernel.InvocationID, WriteRequest) (WriteResult, error)
	Run(context.Context, kernel.InvocationID, RunRequest) (RunResult, error)
	Diff(context.Context, kernel.InvocationID, PathRequest) (DiffResult, error)
}
