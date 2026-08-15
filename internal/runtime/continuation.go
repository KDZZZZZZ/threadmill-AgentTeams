package runtime

import (
	"context"
	"errors"
	"sync"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/artifacts"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/phaseagent"
)

// ContinuationMaterial is controlled logical state addressed by a
// ContinuationRef. It intentionally contains references and audit material,
// never a token, credential, private HTTP header, model state, or carrier ID.
type ContinuationMaterial struct {
	Endpoint            phaseagent.PhaseEndpointRef `json:"endpoint"`
	WorkspaceRef        string                      `json:"workspace_ref,omitempty"`
	ContextSliceRef     string                      `json:"context_slice_ref,omitempty"`
	ContextBaselineRef  string                      `json:"context_baseline_ref,omitempty"`
	TaskMemoryBufferRef string                      `json:"task_memory_buffer_ref,omitempty"`
	ArtifactRefs        []artifacts.ArtifactRef     `json:"artifact_refs,omitempty"`
	EventRefs           []string                    `json:"event_refs,omitempty"`
	EvidenceRefs        []string                    `json:"evidence_refs,omitempty"`
}

// ContinuationResolver resolves Runtime-owned logical continuation material.
// A durable implementation belongs behind this seam; M4-C supplies an
// in-memory implementation only for local reconstruction tests.
type ContinuationResolver interface {
	ResolveContinuation(context.Context, ContinuationRef) (ContinuationMaterial, error)
}

var ErrContinuationNotFound = errors.New("continuation reference was not found")

type InMemoryContinuationStore struct {
	mu       sync.RWMutex
	material map[ContinuationRef]ContinuationMaterial
}

func NewInMemoryContinuationStore() *InMemoryContinuationStore {
	return &InMemoryContinuationStore{material: make(map[ContinuationRef]ContinuationMaterial)}
}

func (s *InMemoryContinuationStore) Put(ref ContinuationRef, material ContinuationMaterial) error {
	if ref == "" {
		return errors.New("continuation reference is required")
	}
	if err := material.Endpoint.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.material[ref]; exists {
		return ErrWaitingRecordExists
	}
	s.material[ref] = copyContinuationMaterial(material)
	return nil
}

func (s *InMemoryContinuationStore) ResolveContinuation(_ context.Context, ref ContinuationRef) (ContinuationMaterial, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	material, found := s.material[ref]
	if !found {
		return ContinuationMaterial{}, ErrContinuationNotFound
	}
	return copyContinuationMaterial(material), nil
}

func copyContinuationMaterial(in ContinuationMaterial) ContinuationMaterial {
	in.ArtifactRefs = append([]artifacts.ArtifactRef(nil), in.ArtifactRefs...)
	in.EventRefs = append([]string(nil), in.EventRefs...)
	in.EvidenceRefs = append([]string(nil), in.EvidenceRefs...)
	return in
}
