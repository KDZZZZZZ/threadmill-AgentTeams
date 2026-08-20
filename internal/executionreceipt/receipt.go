package executionreceipt

import (
	"context"
	"errors"
	"sync"
	"time"
)

type Key struct {
	TaskID         string
	InvocationID   string
	Generation     int
	ExecutionEpoch int64
}

type Submission struct {
	PackageDigest   string `json:"package_digest"`
	SessionIdentity string `json:"session_identity"`
	Consumed        bool   `json:"consumed"`
}

type Receipt struct {
	TaskID          string    `json:"task_id"`
	InvocationID    string    `json:"invocation_id"`
	Generation      int       `json:"generation"`
	ExecutionEpoch  int64     `json:"execution_epoch"`
	BindingRef      string    `json:"binding_ref"`
	InputRevision   string    `json:"input_revision"`
	PackageDigest   string    `json:"package_digest"`
	SessionIdentity string    `json:"session_identity"`
	Consumed        bool      `json:"consumed"`
	RecordedAt      time.Time `json:"recorded_at"`
	Revision        int64     `json:"revision"`
}

func (r Receipt) Key() Key {
	return Key{TaskID: r.TaskID, InvocationID: r.InvocationID, Generation: r.Generation, ExecutionEpoch: r.ExecutionEpoch}
}

type Store interface {
	PutIfAbsent(context.Context, Receipt) (Receipt, bool, error)
	Get(context.Context, Key) (Receipt, bool, error)
}

type InMemoryStore struct {
	mu      sync.RWMutex
	records map[Key]Receipt
}

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{records: make(map[Key]Receipt)}
}

func (s *InMemoryStore) PutIfAbsent(_ context.Context, value Receipt) (Receipt, bool, error) {
	if value.TaskID == "" || value.InvocationID == "" || value.Generation <= 0 || value.ExecutionEpoch <= 0 || value.BindingRef == "" || value.InputRevision == "" || value.PackageDigest == "" || value.SessionIdentity == "" || !value.Consumed {
		return Receipt{}, false, errors.New("complete consumed package receipt is required")
	}
	key := value.Key()
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.records[key]; ok {
		if existing.BindingRef == value.BindingRef && existing.InputRevision == value.InputRevision && existing.PackageDigest == value.PackageDigest && existing.SessionIdentity == value.SessionIdentity && existing.Consumed == value.Consumed {
			return existing, false, nil
		}
		return existing, false, errors.New("conflicting package consumption receipt")
	}
	value.RecordedAt = time.Now().UTC()
	value.Revision = 1
	s.records[key] = value
	return value, true, nil
}

func (s *InMemoryStore) Get(_ context.Context, key Key) (Receipt, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.records[key]
	return value, ok, nil
}
