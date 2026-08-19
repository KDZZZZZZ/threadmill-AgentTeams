package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/phaseagent"
)

// StoredPhaseInputSet is one immutable full input projection. Sequence is
// store-owned ordering because InputRevision values are opaque strings.
type StoredPhaseInputSet struct {
	Inputs                  phaseagent.PhaseInputSet
	AwaitConditionSatisfied bool
	TerminalReason          string
}

// InMemoryPhaseInputStore is an MVP implementation of the authoritative input
// resolver contract. It preserves complete immutable revisions and ordering,
// but is not durable across Runtime processes.
type InMemoryPhaseInputStore struct {
	mu        sync.RWMutex
	revisions map[WaitingKey][]StoredPhaseInputSet
}

func NewInMemoryPhaseInputStore() *InMemoryPhaseInputStore {
	return &InMemoryPhaseInputStore{revisions: make(map[WaitingKey][]StoredPhaseInputSet)}
}

func (s *InMemoryPhaseInputStore) Put(key WaitingKey, value StoredPhaseInputSet) error {
	if key.TaskID == "" || key.InvocationID == "" || key.Generation <= 0 || value.Inputs.InputRevision == "" {
		return errors.New("input store key and input revision are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, existing := range s.revisions[key] {
		if existing.Inputs.InputRevision == value.Inputs.InputRevision {
			return errors.New("input revision already exists")
		}
	}
	value.Inputs = copyPhaseInputSet(value.Inputs)
	s.revisions[key] = append(s.revisions[key], value)
	return nil
}

func (s *InMemoryPhaseInputStore) ResolveRehydrationInputs(_ context.Context, record WaitingRecord) (RehydrationInputSnapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	values := s.revisions[record.Key]
	if len(values) == 0 {
		return RehydrationInputSnapshot{}, errors.New("no authoritative input set exists for waiting invocation")
	}
	previousIndex := -1
	for index, value := range values {
		if value.Inputs.InputRevision == record.InputRevision {
			previousIndex = index
			break
		}
	}
	if previousIndex < 0 {
		return RehydrationInputSnapshot{}, fmt.Errorf("waiting input revision %q is not in authoritative history", record.InputRevision)
	}
	latest := values[len(values)-1]
	previous := values[previousIndex]
	return RehydrationInputSnapshot{
		Inputs:                  copyPhaseInputSet(latest.Inputs),
		NewlyDelivered:          newlyDelivered(previous.Inputs.Delivered, latest.Inputs.Delivered),
		RevisionIsNewer:         previousIndex < len(values)-1,
		AwaitConditionSatisfied: latest.AwaitConditionSatisfied,
		TerminalReason:          latest.TerminalReason,
	}, nil
}

// InMemoryContinuationBindingStore creates immutable B2-style bindings for
// tests and single-process MVP wiring. Durable binding storage remains an
// external Runtime responsibility.
type InMemoryContinuationBindingStore struct {
	mu       sync.Mutex
	next     uint64
	bindings map[string]ContinuationBinding
}

func NewInMemoryContinuationBindingStore() *InMemoryContinuationBindingStore {
	return &InMemoryContinuationBindingStore{bindings: make(map[string]ContinuationBinding)}
}

func (s *InMemoryContinuationBindingStore) RebindInputsForContinuation(_ context.Context, request ContinuationBinding) (ContinuationBinding, error) {
	if request.BindingRef != "" {
		return ContinuationBinding{}, errors.New("binding service owns the new binding_ref")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.next++
	request.BindingRef = fmt.Sprintf("binding-continuation-%d", s.next)
	if err := ValidateContinuationRebind(request); err != nil {
		return ContinuationBinding{}, err
	}
	s.bindings[request.BindingRef] = request
	return request, nil
}

func (s *InMemoryContinuationBindingStore) Resolve(bindingRef string) (ContinuationBinding, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.bindings[bindingRef]
	return value, ok
}

func newlyDelivered(previous, current []phaseagent.InputDelivery) []phaseagent.InputDelivery {
	seen := make(map[string]struct{}, len(previous))
	for _, delivery := range previous {
		seen[delivery.InputID] = struct{}{}
	}
	result := make([]phaseagent.InputDelivery, 0)
	for _, delivery := range current {
		if _, exists := seen[delivery.InputID]; !exists {
			result = append(result, copyInputDelivery(delivery))
		}
	}
	return result
}

func copyPhaseInputSet(value phaseagent.PhaseInputSet) phaseagent.PhaseInputSet {
	delivered := value.Delivered
	value.Required = append([]phaseagent.InputRequirement(nil), value.Required...)
	value.Delivered = make([]phaseagent.InputDelivery, len(delivered))
	for index, delivery := range delivered {
		value.Delivered[index] = copyInputDelivery(delivery)
	}
	value.Pending = append([]phaseagent.PendingInput(nil), value.Pending...)
	return value
}

func copyInputDelivery(value phaseagent.InputDelivery) phaseagent.InputDelivery {
	value.ArtifactRefs = append([]string(nil), value.ArtifactRefs...)
	return value
}
