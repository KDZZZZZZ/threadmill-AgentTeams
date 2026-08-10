package fake

import (
	"context"
	"sync"

	adapter "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/adapters/agentteams"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

type InvocationSource struct {
	mu    sync.RWMutex
	items map[string]adapter.PreparedInvocation
}

func NewInvocationSource() *InvocationSource {
	return &InvocationSource{items: make(map[string]adapter.PreparedInvocation)}
}

func (s *InvocationSource) Put(ref string, invocation adapter.PreparedInvocation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	invocation.RequiredCapabilities = append([]string(nil), invocation.RequiredCapabilities...)
	s.items[ref] = invocation
}

func (s *InvocationSource) LoadPreparedInvocation(ctx context.Context, ref string) (adapter.PreparedInvocation, error) {
	if err := ctx.Err(); err != nil {
		return adapter.PreparedInvocation{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	invocation, ok := s.items[ref]
	if !ok {
		return adapter.PreparedInvocation{}, kernel.Error{Code: kernel.CodeNotFound, Message: "prepared invocation not found"}
	}
	invocation.RequiredCapabilities = append([]string(nil), invocation.RequiredCapabilities...)
	return invocation, nil
}
