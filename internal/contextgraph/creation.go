package contextgraph

import (
	"context"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

// WritePort is the Context Service internal mutation seam. It is deliberately
// narrower than public tools: callers provide trusted auth.Principal and
// NodeCreationContext; agents cannot self-report creation context.
type WritePort interface {
	CreateNodes(context.Context, auth.Principal, CreateNodesRequest) (CreateNodesResult, error)
}
