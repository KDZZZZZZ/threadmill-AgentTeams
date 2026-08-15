// Package runtime owns Runtime-internal await/rehydration coordination. It is
// deliberately separate from phaseagent: a logical Phase Invocation remains a
// public domain concept, while carriers, leases, and waiting persistence are
// Runtime concerns.
package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/artifacts"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/phaseagent"
)

// ExecutionEpoch identifies one physical carrier for a logical Invocation.
// Rehydrating after awaitInputs increments this Runtime-internal value while
// preserving the InvocationID and Phase Agent generation.
type ExecutionEpoch int

// ContinuationRef is an opaque Runtime-owned reference to controlled logical
// state needed to rehydrate an awaited invocation. It must never encode an
// execution token, credential, private header, provider session, hidden
// reasoning, or backend process identity.
type ContinuationRef string

// AwaitState is Runtime persistence state, not phaseagent.InvocationState.
// Relinquishing is an internal fence that prevents two callers from tearing
// down the same physical carrier concurrently.
type AwaitState string

const (
	AwaitStateRunning        AwaitState = "running"
	AwaitStatePreparingAwait AwaitState = "preparing_await"
	AwaitStateRelinquishing  AwaitState = "relinquishing"
	AwaitStateWaiting        AwaitState = "waiting"
	AwaitStateRehydrating    AwaitState = "rehydrating"
	AwaitStateTerminal       AwaitState = "terminal"
)

// WaitingKey identifies the one logical invocation that may wait and later be
// rehydrated. Generation is deliberately part of the key so it cannot be
// confused with a checkpoint/resume invocation.
type WaitingKey struct {
	TaskID       string `json:"task_id"`
	InvocationID string `json:"invocation_id"`
	Generation   int    `json:"generation"`
}

// WaitingRecord is the durable-replaceable Runtime representation of an
// awaited logical invocation. Every field is either logical identity, an
// opaque reference, or non-sensitive coordination metadata. In particular it
// intentionally contains no execution token, MCP credential, private header,
// QwenPaw session secret, hidden reasoning, or backend process ID.
type WaitingRecord struct {
	Key                 WaitingKey                  `json:"key"`
	ExecutionEpoch      ExecutionEpoch              `json:"execution_epoch"`
	Endpoint            phaseagent.PhaseEndpointRef `json:"endpoint"`
	PreviousBindingRef  string                      `json:"previous_binding_ref"`
	InputRevision       string                      `json:"input_revision"`
	PendingInputIDs     []string                    `json:"pending_input_ids"`
	ContinuationRef     ContinuationRef             `json:"continuation_ref"`
	State               AwaitState                  `json:"state"`
	WorkspaceRef        string                      `json:"workspace_ref,omitempty"`
	ContextSliceRef     string                      `json:"context_slice_ref,omitempty"`
	TaskMemoryBufferRef string                      `json:"task_memory_buffer_ref,omitempty"`
	ArtifactRefs        []artifacts.ArtifactRef     `json:"artifact_refs,omitempty"`
	EventRefs           []string                    `json:"event_refs,omitempty"`
	EvidenceRefs        []string                    `json:"evidence_refs,omitempty"`
	Revision            int64                       `json:"revision"`
	CreatedAt           time.Time                   `json:"created_at"`
	UpdatedAt           time.Time                   `json:"updated_at"`
}

func (r WaitingRecord) Validate() error {
	if r.Key.TaskID == "" || r.Key.InvocationID == "" || r.Key.Generation <= 0 {
		return errors.New("waiting record task_id, invocation_id, and generation are required")
	}
	if r.ExecutionEpoch <= 0 {
		return errors.New("waiting record execution_epoch must be positive")
	}
	if err := r.Endpoint.Validate(); err != nil {
		return fmt.Errorf("waiting record endpoint: %w", err)
	}
	if r.Endpoint.TaskID != r.Key.TaskID {
		return errors.New("waiting record endpoint task_id must match key task_id")
	}
	if r.PreviousBindingRef == "" || r.InputRevision == "" || r.ContinuationRef == "" {
		return errors.New("waiting record binding_ref, input_revision, and continuation_ref are required")
	}
	if r.State == "" {
		return errors.New("waiting record state is required")
	}
	return nil
}

// WaitingStore is the durable-store seam. CompareAndSwap uses Revision to
// reject stale writers and make transitions safe across Runtime processes.
type WaitingStore interface {
	Create(context.Context, WaitingRecord) (WaitingRecord, error)
	Get(context.Context, WaitingKey) (WaitingRecord, bool, error)
	CompareAndSwap(context.Context, WaitingKey, int64, WaitingRecord) (WaitingRecord, bool, error)
	Delete(context.Context, WaitingKey, int64) (bool, error)
}

var (
	ErrWaitingRecordExists = errors.New("waiting record already exists")
	ErrInvalidTransition   = errors.New("invalid waiting state transition")
)

// InMemoryWaitingStore is for local tests and MVP wiring only. It preserves
// the same duplicate and CAS semantics required of a future durable store.
type InMemoryWaitingStore struct {
	mu      sync.RWMutex
	records map[WaitingKey]WaitingRecord
	now     func() time.Time
}

func NewInMemoryWaitingStore() *InMemoryWaitingStore {
	return &InMemoryWaitingStore{records: make(map[WaitingKey]WaitingRecord), now: time.Now}
}

func (s *InMemoryWaitingStore) Create(_ context.Context, record WaitingRecord) (WaitingRecord, error) {
	if err := record.Validate(); err != nil {
		return WaitingRecord{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.records[record.Key]; exists {
		return WaitingRecord{}, ErrWaitingRecordExists
	}
	now := s.now().UTC()
	record.Revision = 1
	record.CreatedAt = now
	record.UpdatedAt = now
	record = copyWaitingRecord(record)
	s.records[record.Key] = record
	return copyWaitingRecord(record), nil
}

func (s *InMemoryWaitingStore) Get(_ context.Context, key WaitingKey) (WaitingRecord, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	record, ok := s.records[key]
	return copyWaitingRecord(record), ok, nil
}

func (s *InMemoryWaitingStore) CompareAndSwap(_ context.Context, key WaitingKey, expectedRevision int64, next WaitingRecord) (WaitingRecord, bool, error) {
	if err := next.Validate(); err != nil {
		return WaitingRecord{}, false, err
	}
	if next.Key != key {
		return WaitingRecord{}, false, errors.New("waiting record key cannot change")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, exists := s.records[key]
	if !exists || current.Revision != expectedRevision {
		return WaitingRecord{}, false, nil
	}
	if !validAwaitTransition(current.State, next.State) {
		return WaitingRecord{}, false, fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, current.State, next.State)
	}
	next.Revision = current.Revision + 1
	next.CreatedAt = current.CreatedAt
	next.UpdatedAt = s.now().UTC()
	next = copyWaitingRecord(next)
	s.records[key] = next
	return copyWaitingRecord(next), true, nil
}

func (s *InMemoryWaitingStore) Delete(_ context.Context, key WaitingKey, expectedRevision int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, exists := s.records[key]
	if !exists || current.Revision != expectedRevision {
		return false, nil
	}
	delete(s.records, key)
	return true, nil
}

func validAwaitTransition(from, to AwaitState) bool {
	if from == to {
		return true
	}
	switch from {
	case AwaitStateRunning:
		return to == AwaitStatePreparingAwait || to == AwaitStateTerminal
	case AwaitStatePreparingAwait:
		return to == AwaitStateRelinquishing || to == AwaitStateTerminal
	case AwaitStateRelinquishing:
		return to == AwaitStateWaiting || to == AwaitStateTerminal
	case AwaitStateWaiting:
		return to == AwaitStateRehydrating || to == AwaitStateTerminal
	case AwaitStateRehydrating:
		return to == AwaitStateRunning || to == AwaitStateWaiting || to == AwaitStateTerminal
	default:
		return false
	}
}

func copyWaitingRecord(in WaitingRecord) WaitingRecord {
	in.PendingInputIDs = append([]string(nil), in.PendingInputIDs...)
	in.ArtifactRefs = append([]artifacts.ArtifactRef(nil), in.ArtifactRefs...)
	in.EventRefs = append([]string(nil), in.EventRefs...)
	in.EvidenceRefs = append([]string(nil), in.EvidenceRefs...)
	return in
}
