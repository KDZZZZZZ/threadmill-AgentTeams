package contextgraph

import (
	"context"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

// ContextGraphCurator is the controlled general-only CRUD seam for Context
// Agent. Task subgraphs and task-member nodes are rejected by implementation.
type ContextGraphCurator interface {
	GetSubgraph(context.Context, auth.Principal, GetSubgraphRequest) (ContextSubgraph, error)
	GetNode(context.Context, auth.Principal, GetNodeRequest) (ContextNode, error)
	CreateNode(context.Context, auth.Principal, CreateGeneralNodeRequest) (ContextNodeRef, error)
	UpdateNode(context.Context, auth.Principal, UpdateGeneralNodeRequest) (ContextNodeRef, error)
	DeleteNode(context.Context, auth.Principal, DeleteGeneralNodeRequest) error
	CreateSubgraph(context.Context, auth.Principal, CreateGeneralSubgraphRequest) (ContextSubgraph, error)
	UpdateSubgraph(context.Context, auth.Principal, UpdateGeneralSubgraphRequest) (ContextSubgraph, error)
	DeleteSubgraph(context.Context, auth.Principal, DeleteGeneralSubgraphRequest) error
}
