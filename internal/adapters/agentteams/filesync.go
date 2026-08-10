package agentteams

import "context"

// FileTransport mirrors only the shared task directory needed for collection.
// Workspace authority, Artifact registration, and hash verification stay in
// their owning Threadmill modules.
type FileTransport interface {
	PullExecution(context.Context, string) error
	ReadResult(context.Context, string) ([]byte, error)
}
