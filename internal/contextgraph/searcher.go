package contextgraph

import (
	"context"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

// ContextGraphSearcher is exposed only to Context Agent. Service-side role and
// capability checks still reject direct calls from Phase or Task Manager agents.
type ContextGraphSearcher interface {
	Search(context.Context, auth.Principal, SearchRequest) (ContextSearchResult, error)
}
