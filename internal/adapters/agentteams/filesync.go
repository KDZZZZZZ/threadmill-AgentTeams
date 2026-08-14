package agentteams

import "context"

// ExecutionFileProjection is the provider-neutral carrier payload used to put
// one invocation's native workspace into AgentTeams shared storage. The
// opaque manifest is owned by the Runtime-side Workspace bridge; the adapter
// never interprets it as Workspace or graph authority.
type ExecutionFileProjection struct {
	Manifest []byte
	Files    []ExecutionFile
}

type ExecutionFile struct {
	Path    string
	Mode    uint32
	Content []byte
	SHA256  string
}

// FileTransport mirrors the bounded execution carrier. Workspace authority,
// phase ACL decisions, revision checks, Artifact registration, and graph
// mutations stay in their owning Threadmill modules.
type FileTransport interface {
	PrepareExecution(context.Context, AgentTeamsExecutionRef, PreparedInvocation) error
	PullExecution(context.Context, AgentTeamsExecutionRef) (ExecutionWorkspaceCheckpoint, error)
	ReadResult(context.Context, string) ([]byte, error)
}

// ExecutionWorkspaceCheckpoint is the only Workspace fact that crosses back
// through the provider adapter. It is produced by the authoritative Workspace
// Service after a full native snapshot has passed lease and phase ACL checks.
type ExecutionWorkspaceCheckpoint struct {
	WorkspaceRevision string
}

// ExecutionFileProjector is implemented by the Runtime-side Workspace bridge.
// It may only resolve authority from the trusted invocation identity carried
// by AgentTeamsExecutionRef; provider task IDs and manifests are carriers.
type ExecutionFileProjector interface {
	OwnsExecution(context.Context, AgentTeamsExecutionRef) (bool, error)
	ExportExecutionFiles(context.Context, AgentTeamsExecutionRef) (ExecutionFileProjection, error)
	ImportExecutionFiles(context.Context, AgentTeamsExecutionRef, ExecutionFileProjection) (ExecutionWorkspaceCheckpoint, error)
}
