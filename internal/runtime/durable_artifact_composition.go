package runtime

import (
	"errors"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/artifacts"
)

// NewDurableArtifactRegistry composes the one RuntimeStateRepository's
// authoritative metadata store with a process-local blob publisher. The
// caller obtains object-store configuration from its deployment secret
// boundary; no configuration or credential is stored in Runtime SQLite.
func NewDurableArtifactRegistry(repository RuntimeStateRepository, publisher artifacts.BlobPublisher) (*artifacts.DurableRegistry, error) {
	if repository == nil || publisher == nil {
		return nil, errors.New("runtime repository and blob publisher are required")
	}
	return artifacts.NewDurableRegistry(repository.ArtifactStore(), publisher), nil
}
