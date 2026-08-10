package mcpapi

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/contextgraph"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime/phase"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/taskmanager"
)

type finalizeTaskMemoryRequest struct {
	TaskID kernel.TaskID `json:"task_id"`
}

type registerTaskSubgraphRequest struct {
	TaskID kernel.TaskID `json:"task_id"`
}

type PhaseRuntime interface {
	AwaitInputs(context.Context, kernel.InvocationID, phase.AwaitInputsRequest) (phase.InputWaitResult, error)
	SubmitPhaseOutput(context.Context, kernel.InvocationID, phase.PhaseOutput) (phase.OutputReceipt, error)
}

type RequirementSubmitter interface {
	SubmitRequirement(context.Context, auth.Principal, taskmanager.Requirement) (any, error)
}

func TaskMemoryToolSpecs(reader contextgraph.TaskMemoryBufferReader, submitter contextgraph.CandidateSubmitter) []ToolSpec {
	return []ToolSpec{
		{ID: auth.ToolAgentListTaskMemoryCandidates, Handler: HandlerFunc(func(ctx context.Context, principal auth.Principal, _ auth.BoundScope, payload json.RawMessage) (any, error) {
			var req struct{}
			if err := decodePayload(payload, &req); err != nil {
				return nil, err
			}
			return reader.ListTaskCandidates(ctx, principal)
		})},
		{ID: auth.ToolAgentSubmitMemoryCandidate, Handler: HandlerFunc(func(ctx context.Context, principal auth.Principal, _ auth.BoundScope, payload json.RawMessage) (any, error) {
			var req contextgraph.SubmitCandidateRequest
			if err := decodePayload(payload, &req); err != nil {
				return nil, err
			}
			return submitter.SubmitCandidate(ctx, principal, req)
		})},
	}
}

func PhaseRuntimeToolSpecs(runtime PhaseRuntime) []ToolSpec {
	return []ToolSpec{
		{ID: auth.ToolRuntimeAwaitInputs, Handler: HandlerFunc(func(ctx context.Context, principal auth.Principal, _ auth.BoundScope, payload json.RawMessage) (any, error) {
			var req phase.AwaitInputsRequest
			if err := decodePayload(payload, &req); err != nil {
				return nil, err
			}
			return runtime.AwaitInputs(ctx, principal.InvocationID, req)
		})},
		{ID: auth.ToolAgentSubmitPhaseOutput, Handler: HandlerFunc(func(ctx context.Context, principal auth.Principal, _ auth.BoundScope, payload json.RawMessage) (any, error) {
			var req phase.PhaseOutput
			if err := decodePayload(payload, &req); err != nil {
				return nil, err
			}
			return runtime.SubmitPhaseOutput(ctx, principal.InvocationID, req)
		})},
	}
}

func RequirementToolSpec(submitter RequirementSubmitter) ToolSpec {
	return ToolSpec{ID: auth.ToolAgentSubmitRequirement, Handler: HandlerFunc(func(ctx context.Context, principal auth.Principal, _ auth.BoundScope, payload json.RawMessage) (any, error) {
		var req taskmanager.Requirement
		if err := decodePayload(payload, &req); err != nil {
			return nil, err
		}
		if strings.TrimSpace(req.Text) == "" {
			return nil, kernel.InvalidArgument("text is required")
		}
		return submitter.SubmitRequirement(ctx, principal, req)
	})}
}

func TaskContextToolSpecs(writer contextgraph.TaskContextWriter, finalizer contextgraph.TaskMemoryFinalizer) []ToolSpec {
	return []ToolSpec{
		{ID: auth.ToolContextRegisterTaskSubgraph, Handler: HandlerFunc(func(ctx context.Context, principal auth.Principal, _ auth.BoundScope, payload json.RawMessage) (any, error) {
			var req registerTaskSubgraphRequest
			if err := decodePayload(payload, &req); err != nil {
				return nil, err
			}
			return writer.RegisterTaskSubgraph(ctx, principal, req.TaskID)
		})},
		{ID: auth.ToolContextProjectTaskContext, Handler: HandlerFunc(func(ctx context.Context, principal auth.Principal, _ auth.BoundScope, payload json.RawMessage) (any, error) {
			var req contextgraph.ProjectTaskContextRequest
			if err := decodePayload(payload, &req); err != nil {
				return nil, err
			}
			return writer.ProjectTaskContext(ctx, principal, req)
		})},
		{ID: auth.ToolContextFinalizeTaskMemory, Handler: HandlerFunc(func(ctx context.Context, principal auth.Principal, _ auth.BoundScope, payload json.RawMessage) (any, error) {
			var req finalizeTaskMemoryRequest
			if err := decodePayload(payload, &req); err != nil {
				return nil, err
			}
			return finalizer.FinalizeTaskMemory(ctx, principal, req.TaskID)
		})},
	}
}
