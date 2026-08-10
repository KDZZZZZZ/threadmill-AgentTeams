package workspace

import (
	"context"
	"sync"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

// BindingStore is the authoritative persistence boundary for Workspace
// Bindings. Implementations must make UpdateCAS atomic and must serialize
// callbacks using the same lock key across all process instances.
type BindingStore interface {
	WithLock(context.Context, string, func(context.Context) error) error
	Insert(context.Context, Binding) error
	Get(context.Context, kernel.BindingRef) (Binding, error)
	GetByRound(context.Context, kernel.TaskID, int) (Binding, bool, error)
	GetByInvocation(context.Context, kernel.InvocationID) (Binding, bool, error)
	UpdateCAS(context.Context, Binding, kernel.Revision) (Binding, error)
}

type MemoryStore struct {
	mu         sync.Mutex
	bindings   map[kernel.BindingRef]Binding
	byRound    map[roundKey]kernel.BindingRef
	lockMu     sync.Mutex
	keyedLocks map[string]*sync.Mutex
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		bindings:   make(map[kernel.BindingRef]Binding),
		byRound:    make(map[roundKey]kernel.BindingRef),
		keyedLocks: make(map[string]*sync.Mutex),
	}
}

func (s *MemoryStore) WithLock(ctx context.Context, key string, fn func(context.Context) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if key == "" || fn == nil {
		return kernel.InvalidArgument("workspace lock key and callback are required")
	}
	s.lockMu.Lock()
	lock := s.keyedLocks[key]
	if lock == nil {
		lock = &sync.Mutex{}
		s.keyedLocks[key] = lock
	}
	s.lockMu.Unlock()
	lock.Lock()
	defer lock.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	return fn(ctx)
}

func (s *MemoryStore) Insert(ctx context.Context, binding Binding) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := roundKey{taskID: binding.TaskID, generation: binding.Generation}
	if _, ok := s.bindings[binding.ID]; ok {
		return kernel.IdempotencyConflict()
	}
	if _, ok := s.byRound[key]; ok {
		return kernel.IdempotencyConflict()
	}
	s.bindings[binding.ID] = cloneBinding(binding)
	s.byRound[key] = binding.ID
	return nil
}

func (s *MemoryStore) Get(ctx context.Context, id kernel.BindingRef) (Binding, error) {
	if err := ctx.Err(); err != nil {
		return Binding{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	binding, ok := s.bindings[id]
	if !ok {
		return Binding{}, workspaceNotFound()
	}
	return cloneBinding(binding), nil
}

func (s *MemoryStore) GetByRound(ctx context.Context, taskID kernel.TaskID, generation int) (Binding, bool, error) {
	if err := ctx.Err(); err != nil {
		return Binding{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.byRound[roundKey{taskID: taskID, generation: generation}]
	if !ok {
		return Binding{}, false, nil
	}
	return cloneBinding(s.bindings[id]), true, nil
}

func (s *MemoryStore) GetByInvocation(ctx context.Context, invocationID kernel.InvocationID) (Binding, bool, error) {
	if err := ctx.Err(); err != nil {
		return Binding{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, binding := range s.bindings {
		if binding.ActiveInvocation == invocationID {
			return cloneBinding(binding), true, nil
		}
	}
	return Binding{}, false, nil
}

func (s *MemoryStore) UpdateCAS(ctx context.Context, next Binding, expected kernel.Revision) (Binding, error) {
	if err := ctx.Err(); err != nil {
		return Binding{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.bindings[next.ID]
	if !ok {
		return Binding{}, workspaceNotFound()
	}
	if err := kernel.CheckExpectedRevision(expected, current.Revision); err != nil {
		return Binding{}, err
	}
	for id, binding := range s.bindings {
		if id != next.ID && next.ActiveInvocation != "" && binding.ActiveInvocation == next.ActiveInvocation {
			return Binding{}, kernel.LeaseConflict("invocation is already bound to another workspace")
		}
	}
	next.Revision = current.Revision.Next()
	s.bindings[next.ID] = cloneBinding(next)
	return cloneBinding(next), nil
}

var _ BindingStore = (*MemoryStore)(nil)
