package phaseagent

import "context"

// ContextGraphReader is the provider-neutral Context Graph read and
// subscription seam supplied to a Phase Agent. Its method and DTO semantics
// are authoritative in docs/context-graph.md §6.1.1. In particular, it does
// not expose Search: semantic retrieval belongs to ContextAgent.
type ContextGraphReader interface {
	ListSubgraphs(ctx context.Context, req ListSubgraphsRequest) ([]ContextSubgraph, error)
	Explore(ctx context.Context, req ExploreRequest) (ContextSliceDelta, error)
	Subscribe(ctx context.Context, req SubscribeRequest) (ContextSubscription, error)
	Unsubscribe(ctx context.Context, subscriptionID string) error
}

// ListSubgraphsRequest filters the caller's visible subgraphs. An empty
// Filter means all visible subgraphs.
type ListSubgraphsRequest struct {
	Filter string `json:"filter"`
}

// ContextSubgraph is the authoritative Context Graph subgraph value. It is
// repeated here only because this repository has no shared Context domain
// package yet; its field set must remain identical to context-graph.md §3.5.
type ContextSubgraph struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Summary  string `json:"summary"`
	Revision int64  `json:"revision"`
	Kind     string `json:"kind"`
}

// ExploreRequest asks the Context service to expand an anchor or current
// slice. Runtime-supplied budgets are intentionally not represented here.
type ExploreRequest struct {
	AnchorRef string `json:"anchor_ref"`
	Depth     int    `json:"depth"`
}

// ContextNode is the authoritative Context Graph node value. It is required
// only as the result element of ContextSliceDelta and is otherwise not a
// Phase Agent write model. Its fields follow context-graph.md §3.1 exactly.
type ContextNode struct {
	ID             string   `json:"id"`
	Kind           string   `json:"kind"`
	Statement      string   `json:"statement"`
	Status         string   `json:"status"`
	SubgraphIDs    []string `json:"subgraph_ids"`
	SourceRefs     []string `json:"source_refs"`
	CreatorAgentID string   `json:"creator_agent_id"`
}

// ContextSliceDelta is a point-in-time exploration or retrieval result.
type ContextSliceDelta struct {
	Nodes         []ContextNode `json:"nodes"`
	Frontier      []string      `json:"frontier"`
	GraphRevision int64         `json:"graph_revision"`
}

// SubscribeRequest requests a change feed for visible subgraphs. The consumer
// Invocation identity is bound by Runtime, never supplied by the agent.
type SubscribeRequest struct {
	SubgraphIDs []string `json:"subgraph_ids"`
	EventKinds  []string `json:"event_kinds"`
}

// ContextSubscription is owned by ConsumerInvocationID. It expires when that
// Invocation ends; overlapping subscriptions contribute a distinct union of
// subgraphs, and Unsubscribe affects only this subscription.
type ContextSubscription struct {
	ID                   string   `json:"id"`
	ConsumerInvocationID string   `json:"consumer_invocation_id"`
	SubgraphIDs          []string `json:"subgraph_ids"`
	EventKinds           []string `json:"event_kinds"`
	PermissionSnapshot   string   `json:"permission_snapshot"`
}

// ContextAgent is the distinct, provider-neutral semantic retrieval seam.
// The Phase Agent supplies only a natural-language Query; conversion to a
// mechanical graph search request remains inside the Context Agent.
type ContextAgent interface {
	Retrieve(ctx context.Context, req ContextRetrieveRequest) (ContextRetrieveResult, error)
}

type ContextRetrieveRequest struct {
	Query string `json:"query"`
}

type ContextRetrieveResult struct {
	Slice           ContextSliceDelta `json:"slice"`
	SubscriptionIDs []string          `json:"subscription_ids"`
	Explanation     string            `json:"explanation"`
}

// ContextDelta is a Runtime-to-active-executor knowledge update from an
// existing subscription. It is not a formal input and therefore never changes
// PhaseInputSet or InputRevision.
type ContextDelta struct {
	SubscriptionID string `json:"subscription_id"`
	SubgraphID     string `json:"subgraph_id"`
	Revision       int64  `json:"revision"`
	Changes        []any  `json:"changes"`
}

// InputsChanged is the Runtime-to-active-executor formal-input update. Inputs
// is the complete latest set, replacing the executor's previous input view.
type InputsChanged struct {
	Inputs PhaseInputSet `json:"inputs"`
}

// ExecutionUpdateReceiver is optionally implemented by an active
// PhaseExecutor. Runtime delivery binds ContextDelta to an already active
// Invocation; it does not create a subscription, persist a queue, or provide
// a transport mechanism here. Implementations incorporate either update on a
// subsequent model turn, await re-hydration, or checkpoint resume. Context
// updates do not retroactively alter a model call already in progress.
type ExecutionUpdateReceiver interface {
	OnContextDelta(ctx context.Context, delta ContextDelta) error
	OnInputsChanged(ctx context.Context, update InputsChanged) error
}
