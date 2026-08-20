package runtime

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

// PhysicalExecutionKey identifies one carrier epoch of a logical invocation.
// It is deliberately internal to Runtime: Phase Agent identity remains the
// TaskID/InvocationID/Generation triple.
type PhysicalExecutionKey struct {
	TaskID         string
	InvocationID   string
	Generation     int
	ExecutionEpoch ExecutionEpoch
}

type PhysicalExecutionState string

const (
	PhysicalExecutionProvisioning PhysicalExecutionState = "provisioning"
	PhysicalExecutionDelegated    PhysicalExecutionState = "delegated"
	PhysicalExecutionAccepted     PhysicalExecutionState = "accepted"
	PhysicalExecutionRunning      PhysicalExecutionState = "running"
	PhysicalExecutionTearingDown  PhysicalExecutionState = "tearing_down"
	PhysicalExecutionTerminated   PhysicalExecutionState = "terminated"
	PhysicalExecutionFailed       PhysicalExecutionState = "failed"
)

// PhysicalExecutionTeardown is redacted evidence of carrier cleanup. It has
// no token, credential value, private header, or model-session material.
type PhysicalExecutionTeardown struct {
	TeamHarnessTaskCancelled bool
	WorkerDeleted            bool
	MCPCleaned               bool
	CredentialRevoked        bool
	TokenRevoked             bool
	LeaseReleased            bool
}

func (e PhysicalExecution) Key() PhysicalExecutionKey {
	return PhysicalExecutionKey{TaskID: e.TaskID, InvocationID: e.InvocationID, Generation: e.Generation, ExecutionEpoch: e.ExecutionEpoch}
}

// PhysicalExecutionStore is the authoritative epoch-aware carrier history.
// Legacy InvocationTaskMap may expose a current fresh task, but must not be
// used as rehydration history.
type PhysicalExecutionStore interface {
	Create(context.Context, PhysicalExecution) (PhysicalExecution, error)
	Get(context.Context, PhysicalExecutionKey) (PhysicalExecution, bool, error)
	CompareAndSwap(context.Context, PhysicalExecutionKey, int64, PhysicalExecution) (PhysicalExecution, bool, error)
	ListByInvocation(context.Context, string, string, int) ([]PhysicalExecution, error)
}

type InMemoryPhysicalExecutionStore struct {
	mu      sync.RWMutex
	records map[PhysicalExecutionKey]PhysicalExecution
}

func NewInMemoryPhysicalExecutionStore() *InMemoryPhysicalExecutionStore {
	return &InMemoryPhysicalExecutionStore{records: make(map[PhysicalExecutionKey]PhysicalExecution)}
}

func (s *InMemoryPhysicalExecutionStore) Create(_ context.Context, value PhysicalExecution) (PhysicalExecution, error) {
	if err := validatePhysicalExecution(value); err != nil {
		return PhysicalExecution{}, err
	}
	key := value.Key()
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.records[key]; exists {
		return PhysicalExecution{}, errors.New("physical execution already exists")
	}
	now := time.Now().UTC()
	value.Revision = 1
	value.CreatedAt = now
	value.UpdatedAt = now
	s.records[key] = value
	return value, nil
}
func (s *InMemoryPhysicalExecutionStore) Get(_ context.Context, key PhysicalExecutionKey) (PhysicalExecution, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.records[key]
	return value, ok, nil
}
func (s *InMemoryPhysicalExecutionStore) CompareAndSwap(_ context.Context, key PhysicalExecutionKey, expected int64, replacement PhysicalExecution) (PhysicalExecution, bool, error) {
	if replacement.Key() != key {
		return PhysicalExecution{}, false, errors.New("physical execution key cannot change")
	}
	if err := validatePhysicalExecution(replacement); err != nil {
		return PhysicalExecution{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.records[key]
	if !ok || current.Revision != expected {
		return current, false, nil
	}
	replacement.Revision = current.Revision + 1
	replacement.CreatedAt = current.CreatedAt
	replacement.UpdatedAt = time.Now().UTC()
	s.records[key] = replacement
	return replacement, true, nil
}
func (s *InMemoryPhysicalExecutionStore) ListByInvocation(_ context.Context, taskID, invocationID string, generation int) ([]PhysicalExecution, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := []PhysicalExecution{}
	for key, value := range s.records {
		if key.TaskID == taskID && key.InvocationID == invocationID && key.Generation == generation {
			result = append(result, value)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ExecutionEpoch < result[j].ExecutionEpoch })
	return result, nil
}

func ValidatePhysicalExecutionReady(value PhysicalExecution) bool {
	if value.WorkerID == "" || value.WorkerName == "" || value.TeamHarnessTaskID == "" || value.TeamHarnessAssignedTo == "" || value.TeamHarnessAssignedTo != value.WorkerName || value.BindingRef == "" || value.InputRevision == "" || value.AgentSessionRef == "" || value.AgentPackageDigest == "" || !value.PackageConsumed || value.CredentialBindingRef == "" || value.ExecutionAuthorizationRef == "" || value.MCPClientID == "" {
		return false
	}
	if !value.Teardown.LeaseReleased && value.WorkspaceLeaseRef == "" {
		return false
	}
	if value.DesiredRuntimeGeneration <= 0 || value.AppliedRuntimeGeneration != value.DesiredRuntimeGeneration {
		return false
	}
	return value.ObservedTaskStatus == "in_progress" || (value.ObservedTaskStatus == "assigned" && value.TaskAcknowledged)
}

func validatePhysicalExecution(value PhysicalExecution) error {
	key := value.Key()
	if key.TaskID == "" || key.InvocationID == "" || key.Generation <= 0 || key.ExecutionEpoch <= 0 {
		return errors.New("physical execution identity is required")
	}
	if value.State == "" {
		return errors.New("physical execution state is required")
	}
	return nil
}
