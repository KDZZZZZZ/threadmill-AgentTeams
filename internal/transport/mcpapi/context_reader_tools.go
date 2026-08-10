package mcpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/contextgraph"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

type unsubscribeRequest struct {
	SubscriptionID string `json:"subscription_id"`
}

func ContextReaderToolSpecs(reader contextgraph.ContextGraphReader) []ToolSpec {
	specs := ContextNavigationToolSpecs(reader)
	return append(specs,
		ToolSpec{ID: auth.ToolContextSubscribe, Handler: HandlerFunc(func(ctx context.Context, principal auth.Principal, _ auth.BoundScope, payload json.RawMessage) (any, error) {
			var req contextgraph.SubscribeRequest
			if err := decodePayload(payload, &req); err != nil {
				return nil, err
			}
			return reader.Subscribe(ctx, principal, req)
		})},
		ToolSpec{ID: auth.ToolContextUnsubscribe, Handler: HandlerFunc(func(ctx context.Context, principal auth.Principal, _ auth.BoundScope, payload json.RawMessage) (any, error) {
			var req unsubscribeRequest
			if err := decodePayload(payload, &req); err != nil {
				return nil, err
			}
			if strings.TrimSpace(req.SubscriptionID) == "" {
				return nil, kernel.InvalidArgument("subscription_id is required")
			}
			return nil, reader.Unsubscribe(ctx, principal, req.SubscriptionID)
		})},
	)
}

func ContextNavigationToolSpecs(reader contextgraph.ContextGraphReader) []ToolSpec {
	return []ToolSpec{
		{ID: auth.ToolContextListSubgraphs, Handler: HandlerFunc(func(ctx context.Context, principal auth.Principal, _ auth.BoundScope, payload json.RawMessage) (any, error) {
			var req contextgraph.ListSubgraphsRequest
			if err := decodePayload(payload, &req); err != nil {
				return nil, err
			}
			return reader.ListSubgraphs(ctx, principal, req)
		})},
		{ID: auth.ToolContextExplore, Handler: HandlerFunc(func(ctx context.Context, principal auth.Principal, _ auth.BoundScope, payload json.RawMessage) (any, error) {
			var req contextgraph.ExploreRequest
			if err := decodePayload(payload, &req); err != nil {
				return nil, err
			}
			return reader.Explore(ctx, principal, req)
		})},
	}
}

func decodePayload(payload json.RawMessage, target any) error {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil
	}
	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return kernel.InvalidArgument("invalid MCP tool payload")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return kernel.InvalidArgument("invalid MCP tool payload")
	}
	return nil
}
