package mcpapi

import (
	"context"
	"encoding/json"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime/phase"
)

// OrchestrationProposalRuntime derives all trusted proposal envelope fields
// from the active Invocation and Binding, then persists/routes the resulting
// OrchestrationProposal. The MCP caller supplies only OrchestrationIntent.
type OrchestrationProposalRuntime interface {
	SubmitOrchestrationIntent(context.Context, auth.Principal, auth.BoundScope, phase.OrchestrationIntent) (phase.OrchestrationProposal, error)
}

func OrchestrationProposalToolSpec(runtime OrchestrationProposalRuntime) ToolSpec {
	return ToolSpec{ID: auth.ToolAgentProposeOrchestration, Handler: HandlerFunc(func(ctx context.Context, principal auth.Principal, scope auth.BoundScope, payload json.RawMessage) (any, error) {
		var req phase.OrchestrationIntent
		if err := decodePayload(payload, &req); err != nil {
			return nil, err
		}
		if err := phase.ValidateOrchestrationIntent(req); err != nil {
			return nil, err
		}
		return runtime.SubmitOrchestrationIntent(ctx, principal, scope, req)
	})}
}
