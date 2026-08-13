package mcpapi

import (
	"context"
	"encoding/json"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/contextagent"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/contextgraph"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

// ContextAgentRetrieveDispatcher is the Agent Runtime seam behind the
// contextAgent.retrieve MCP tool. The caller principal is the original
// consumer (phase or task-manager invocation); the dispatcher is responsible
// for starting or selecting a trusted Context Agent retrieve invocation and
// binding Search auto-subscriptions back to this consumer.
type ContextAgentRetrieveDispatcher interface {
	RetrieveForConsumer(context.Context, auth.Principal, contextagent.ContextRetrieveRequest) (contextagent.ContextRetrieveResult, error)
}

func ContextAgentRetrieveToolSpec(dispatcher ContextAgentRetrieveDispatcher) ToolSpec {
	return ToolSpec{ID: auth.ToolContextAgentRetrieve, Handler: HandlerFunc(func(ctx context.Context, principal auth.Principal, _ auth.BoundScope, payload json.RawMessage) (any, error) {
		var req contextagent.ContextRetrieveRequest
		if err := decodePayload(payload, &req); err != nil {
			return nil, err
		}
		if err := contextagent.ValidateRetrieveRequest(req); err != nil {
			return nil, err
		}
		return dispatcher.RetrieveForConsumer(ctx, principal, req)
	})}
}

func AgentContextToolSpecs(reader contextgraph.ContextGraphReader, dispatcher ContextAgentRetrieveDispatcher) []ToolSpec {
	specs := ContextReaderToolSpecs(reader)
	return append(specs, ContextAgentRetrieveToolSpec(dispatcher))
}

func ContextAgentToolSpecs(reader contextgraph.ContextGraphReader, curator contextgraph.ContextGraphCurator, searcher contextgraph.ContextGraphSearcher, reviewer contextgraph.ContextCandidateReviewer) []ToolSpec {
	specs := ContextNavigationToolSpecs(reader)
	return append(specs, ContextAgentGraphToolSpecs(curator, searcher, reviewer)...)
}

func ContextAgentGraphToolSpecs(curator contextgraph.ContextGraphCurator, searcher contextgraph.ContextGraphSearcher, reviewer contextgraph.ContextCandidateReviewer) []ToolSpec {
	return []ToolSpec{
		{ID: auth.ToolContextGetSubgraph, Handler: HandlerFunc(func(ctx context.Context, principal auth.Principal, _ auth.BoundScope, payload json.RawMessage) (any, error) {
			var req contextgraph.GetSubgraphRequest
			if err := decodePayload(payload, &req); err != nil {
				return nil, err
			}
			return curator.GetSubgraph(ctx, principal, req)
		})},
		{ID: auth.ToolContextGetNode, Handler: HandlerFunc(func(ctx context.Context, principal auth.Principal, _ auth.BoundScope, payload json.RawMessage) (any, error) {
			var req contextgraph.GetNodeRequest
			if err := decodePayload(payload, &req); err != nil {
				return nil, err
			}
			return curator.GetNode(ctx, principal, req)
		})},
		{ID: auth.ToolContextSearch, Handler: HandlerFunc(func(ctx context.Context, principal auth.Principal, _ auth.BoundScope, payload json.RawMessage) (any, error) {
			var req contextgraph.SearchRequest
			if err := decodePayload(payload, &req); err != nil {
				return nil, err
			}
			return searcher.Search(ctx, principal, req)
		})},
		{ID: auth.ToolContextCreateNode, Handler: HandlerFunc(func(ctx context.Context, principal auth.Principal, _ auth.BoundScope, payload json.RawMessage) (any, error) {
			var req contextgraph.CreateGeneralNodeRequest
			if err := decodePayload(payload, &req); err != nil {
				return nil, err
			}
			return curator.CreateNode(ctx, principal, req)
		})},
		{ID: auth.ToolContextUpdateNode, Handler: HandlerFunc(func(ctx context.Context, principal auth.Principal, _ auth.BoundScope, payload json.RawMessage) (any, error) {
			var req contextgraph.UpdateGeneralNodeRequest
			if err := decodePayload(payload, &req); err != nil {
				return nil, err
			}
			return curator.UpdateNode(ctx, principal, req)
		})},
		{ID: auth.ToolContextDeleteNode, Handler: HandlerFunc(func(ctx context.Context, principal auth.Principal, _ auth.BoundScope, payload json.RawMessage) (any, error) {
			var req contextgraph.DeleteGeneralNodeRequest
			if err := decodePayload(payload, &req); err != nil {
				return nil, err
			}
			return nil, curator.DeleteNode(ctx, principal, req)
		})},
		{ID: auth.ToolContextCreateSubgraph, Handler: HandlerFunc(func(ctx context.Context, principal auth.Principal, _ auth.BoundScope, payload json.RawMessage) (any, error) {
			var req contextgraph.CreateGeneralSubgraphRequest
			if err := decodePayload(payload, &req); err != nil {
				return nil, err
			}
			return curator.CreateSubgraph(ctx, principal, req)
		})},
		{ID: auth.ToolContextUpdateSubgraph, Handler: HandlerFunc(func(ctx context.Context, principal auth.Principal, _ auth.BoundScope, payload json.RawMessage) (any, error) {
			var req contextgraph.UpdateGeneralSubgraphRequest
			if err := decodePayload(payload, &req); err != nil {
				return nil, err
			}
			return curator.UpdateSubgraph(ctx, principal, req)
		})},
		{ID: auth.ToolContextDeleteSubgraph, Handler: HandlerFunc(func(ctx context.Context, principal auth.Principal, _ auth.BoundScope, payload json.RawMessage) (any, error) {
			var req contextgraph.DeleteGeneralSubgraphRequest
			if err := decodePayload(payload, &req); err != nil {
				return nil, err
			}
			return nil, curator.DeleteSubgraph(ctx, principal, req)
		})},
		{ID: auth.ToolContextSubmitReview, Handler: HandlerFunc(func(ctx context.Context, principal auth.Principal, _ auth.BoundScope, payload json.RawMessage) (any, error) {
			var req contextgraph.CandidateReviewSubmission
			if err := decodePayload(payload, &req); err != nil {
				return nil, err
			}
			return reviewer.SubmitReview(ctx, principal, req)
		})},
	}
}
