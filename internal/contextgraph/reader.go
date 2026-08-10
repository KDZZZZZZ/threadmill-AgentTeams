package contextgraph

import (
	"context"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

// ContextGraphReader is the read/explore seam available to ordinary agents.
// Subscription lifecycle is implemented by a later W1-B/C batch; this first
// batch intentionally keeps only already-designed read operations executable.
type ContextGraphReader interface {
	ListSubgraphs(context.Context, auth.Principal, ListSubgraphsRequest) ([]ContextSubgraph, error)
	Explore(context.Context, auth.Principal, ExploreRequest) (ContextSliceDelta, error)
}
