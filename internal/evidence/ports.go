package evidence

import (
	"context"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/outbox"
)

type EventStore interface {
	Append(context.Context, AppendEvent) (Event, error)
	Replay(context.Context, Cursor, int) ([]Event, Cursor, error)
	ReplayTask(context.Context, kernel.ProjectID, kernel.TaskID, Cursor, int) ([]Event, Cursor, error)
}

type EventStoreWithOutbox interface {
	EventStore
	AppendWithOutbox(context.Context, AppendEvent, []outbox.Event) (Event, error)
}

type TaskEventReader interface {
	ReadTask(context.Context, Principal, kernel.TaskID, Cursor, int) ([]Event, Cursor, error)
}

type ArtifactStore interface {
	Register(context.Context, RegisterArtifact) (Artifact, error)
	Open(context.Context, Principal, ArtifactID) (Artifact, []byte, error)
	CanRead(Principal, ArtifactID) bool
}
