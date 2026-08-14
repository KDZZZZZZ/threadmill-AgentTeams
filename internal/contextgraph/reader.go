package contextgraph

import (
	"context"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

// ContextGraphReader is the read/explore/subscribe seam available to ordinary
// agents. Search stays isolated in ContextGraphSearcher for Context Agent only.
type ContextGraphReader interface {
	ListSubgraphs(context.Context, auth.Principal, ListSubgraphsRequest) ([]ContextSubgraph, error)
	Explore(context.Context, auth.Principal, ExploreRequest) (ContextSliceDelta, error)
	Subscribe(context.Context, auth.Principal, SubscribeRequest) (ContextSubscription, error)
	Unsubscribe(context.Context, auth.Principal, string) error
}

type ContextGraphSnapshotReader interface {
	ProjectContextSnapshot(context.Context, kernel.ProjectID) (ContextGraphSnapshot, error)
}
