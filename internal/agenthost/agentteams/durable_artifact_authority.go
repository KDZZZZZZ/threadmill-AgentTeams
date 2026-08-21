package agentteams

import (
	"errors"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/artifacts"
	phasemcp "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/mcp/phase"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime"
)

// DurableArtifactAuthority is the only production artifact authority for one
// Runtime composition. It combines that composition's single durable Runtime
// repository with its process-local blob publisher. The same registry is then
// supplied to both Phase MCP and AgentTeams evidence ingestion.
//
// InMemoryRegistry remains suitable only for explicit unit tests and
// non-durable fixtures; callers constructing this authority must not also
// install a second artifact registrar.
type DurableArtifactAuthority struct {
	registry *artifacts.DurableRegistry
}

func NewDurableArtifactAuthority(repository runtime.RuntimeStateRepository, publisher artifacts.BlobPublisher) (*DurableArtifactAuthority, error) {
	registry, err := runtime.NewDurableArtifactRegistry(repository, publisher)
	if err != nil {
		return nil, err
	}
	return &DurableArtifactAuthority{registry: registry}, nil
}

// NewPhaseHandler creates a Phase MCP handler whose artifact.register and
// submitPhaseOutput reference validation use this authority. Observer events
// remain non-authoritative projections; ArtifactRegistered comes only from the
// registry's repository transaction.
func (a *DurableArtifactAuthority) NewPhaseHandler(bindings *phasemcp.BindingRegistry, observer artifacts.EventRecorder, confirmer phasemcp.PackageConsumptionConfirmer) (*phasemcp.Handler, error) {
	if a == nil || a.registry == nil {
		return nil, errors.New("durable artifact authority is required")
	}
	services := []interface{}{a.registry}
	if observer != nil {
		services = append(services, observer)
	}
	if confirmer != nil {
		services = append(services, confirmer)
	}
	return phasemcp.NewHandler(bindings, services...)
}

// EvidenceIngestor uses the identical durable registrar. Its optional observer
// records AgentTeams execution-plane evidence only; it is never the authority
// for ArtifactRegistered or ArtifactRef access.
func (a *DurableArtifactAuthority) EvidenceIngestor(observer artifacts.EventRecorder) (ArtifactEvidenceIngestor, error) {
	if a == nil || a.registry == nil {
		return ArtifactEvidenceIngestor{}, errors.New("durable artifact authority is required")
	}
	return ArtifactEvidenceIngestor{Registrar: a.registry, Recorder: observer}, nil
}
