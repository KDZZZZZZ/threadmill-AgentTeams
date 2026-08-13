package mcpapi

import (
	"reflect"
	"strings"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/contextagent"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/contextgraph"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime/phase"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/taskmanager"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/workspace"
)

type toolDefinition struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
}

func definitionForTool(tool auth.Tool) toolDefinition {
	inputSchema := schemaForType(reflect.TypeOf(toolInputPrototype(tool)), map[reflect.Type]bool{})
	if required := requiredFieldsForTool(tool); len(required) > 0 {
		inputSchema["required"] = required
	}
	return toolDefinition{
		Name:        string(tool),
		Description: toolDescription(tool),
		InputSchema: inputSchema,
	}
}

func toolInputPrototype(tool auth.Tool) any {
	switch tool {
	case auth.ToolContextListSubgraphs:
		return contextgraph.ListSubgraphsRequest{}
	case auth.ToolContextExplore:
		return contextgraph.ExploreRequest{}
	case auth.ToolContextSubscribe:
		return contextgraph.SubscribeRequest{}
	case auth.ToolContextUnsubscribe:
		return unsubscribeRequest{}
	case auth.ToolContextAgentRetrieve:
		return contextagent.ContextRetrieveRequest{}
	case auth.ToolContextGetSubgraph:
		return contextgraph.GetSubgraphRequest{}
	case auth.ToolContextGetNode:
		return contextgraph.GetNodeRequest{}
	case auth.ToolContextSearch:
		return contextgraph.SearchRequest{}
	case auth.ToolContextCreateNode:
		return contextgraph.CreateGeneralNodeRequest{}
	case auth.ToolContextUpdateNode:
		return contextgraph.UpdateGeneralNodeRequest{}
	case auth.ToolContextDeleteNode:
		return contextgraph.DeleteGeneralNodeRequest{}
	case auth.ToolContextCreateSubgraph:
		return contextgraph.CreateGeneralSubgraphRequest{}
	case auth.ToolContextUpdateSubgraph:
		return contextgraph.UpdateGeneralSubgraphRequest{}
	case auth.ToolContextDeleteSubgraph:
		return contextgraph.DeleteGeneralSubgraphRequest{}
	case auth.ToolContextSubmitReview:
		return contextgraph.CandidateReviewSubmission{}
	case auth.ToolRuntimeAwaitInputs:
		return phase.AwaitInputsRequest{}
	case auth.ToolAgentProposeOrchestration:
		return phase.OrchestrationIntent{}
	case auth.ToolAgentSubmitRequirement:
		return taskmanager.Requirement{}
	case auth.ToolAgentListTaskMemoryCandidates:
		return struct{}{}
	case auth.ToolAgentSubmitMemoryCandidate:
		return contextgraph.SubmitCandidateRequest{}
	case auth.ToolAgentSubmitPhaseOutput:
		return phase.PhaseOutput{}
	case auth.ToolCoordinationSnapshot:
		return coordinationSnapshotRequest{}
	case auth.ToolTaskManagerSubmitDecision:
		return taskmanager.TaskManagerDecision{}
	case auth.ToolCoordinationReplacePending:
		return PendingSubgraphIntent{}
	case auth.ToolCoordinationTransition:
		return struct{}{}
	case auth.ToolContextRegisterTaskSubgraph:
		return registerTaskSubgraphRequest{}
	case auth.ToolContextProjectTaskContext:
		return contextgraph.ProjectTaskContextRequest{}
	case auth.ToolContextFinalizeTaskMemory:
		return finalizeTaskMemoryRequest{}
	case auth.ToolWorkspaceList, auth.ToolWorkspaceRead, auth.ToolWorkspaceDiff:
		return workspace.PathRequest{}
	case auth.ToolWorkspaceWritePlan, auth.ToolWorkspaceWrite:
		return workspace.WriteRequest{}
	case auth.ToolWorkspaceRun:
		return workspace.RunRequest{}
	case auth.ToolEvidenceRegister:
		return evidenceRegisterRequest{}
	default:
		return struct{}{}
	}
}

func schemaForType(t reflect.Type, active map[reflect.Type]bool) map[string]any {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == reflect.TypeOf(time.Time{}) {
		return map[string]any{"type": "string", "format": "date-time"}
	}
	switch t.Kind() {
	case reflect.Struct:
		if active[t] {
			return map[string]any{"type": "object"}
		}
		active[t] = true
		defer delete(active, t)
		properties := make(map[string]any)
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			if field.PkgPath != "" {
				continue
			}
			name, _, skip := jsonField(field)
			if skip {
				continue
			}
			properties[name] = schemaForType(field.Type, active)
		}
		schema := map[string]any{
			"type":                 "object",
			"properties":           properties,
			"additionalProperties": false,
		}
		return schema
	case reflect.Slice, reflect.Array:
		return map[string]any{"type": "array", "items": schemaForType(t.Elem(), active)}
	case reflect.Map:
		return map[string]any{"type": "object", "additionalProperties": schemaForType(t.Elem(), active)}
	case reflect.Bool:
		return map[string]any{"type": "boolean"}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return map[string]any{"type": "integer"}
	case reflect.Float32, reflect.Float64:
		return map[string]any{"type": "number"}
	case reflect.String:
		return map[string]any{"type": "string"}
	default:
		return map[string]any{}
	}
}

// requiredFieldsForTool is explicit because encoding/json accepts omitted
// fields as zero values. Requiredness belongs to each tool's semantic
// validator, not to the presence or absence of `omitempty` on a Go DTO.
func requiredFieldsForTool(tool auth.Tool) []string {
	required := map[auth.Tool][]string{
		auth.ToolContextSubscribe:            {"subgraph_ids"},
		auth.ToolContextUnsubscribe:          {"subscription_id"},
		auth.ToolContextAgentRetrieve:        {"query"},
		auth.ToolContextGetSubgraph:          {"subgraph_id"},
		auth.ToolContextGetNode:              {"node_id"},
		auth.ToolContextCreateNode:           {"statement", "kind", "source_refs", "subgraph_ids"},
		auth.ToolContextUpdateNode:           {"node_id", "source_revision", "statement", "kind", "source_refs", "subgraph_ids", "status"},
		auth.ToolContextDeleteNode:           {"node_id", "source_revision", "reason"},
		auth.ToolContextCreateSubgraph:       {"name", "summary", "node_ids"},
		auth.ToolContextUpdateSubgraph:       {"subgraph_id", "revision", "name", "summary", "node_ids"},
		auth.ToolContextDeleteSubgraph:       {"subgraph_id", "revision", "reason"},
		auth.ToolContextSubmitReview:         {"decisions"},
		auth.ToolAgentProposeOrchestration:   {"orchestration_advice", "delivery_spec_advice", "report_spec_advice", "rationale"},
		auth.ToolAgentSubmitRequirement:      {"text"},
		auth.ToolAgentSubmitMemoryCandidate:  {"candidate"},
		auth.ToolAgentSubmitPhaseOutput:      {"phase", "report_ref"},
		auth.ToolTaskManagerSubmitDecision:   {"action", "reason"},
		auth.ToolCoordinationReplacePending:  {"endpoints"},
		auth.ToolContextRegisterTaskSubgraph: {"task_id"},
		auth.ToolContextProjectTaskContext:   {"projection"},
		auth.ToolContextFinalizeTaskMemory:   {"task_id"},
		auth.ToolWorkspaceRead:               {"path"},
		auth.ToolWorkspaceWritePlan:          {"path"},
		auth.ToolWorkspaceWrite:              {"path"},
		auth.ToolWorkspaceRun:                {"command"},
		auth.ToolEvidenceRegister:            {"type"},
	}
	return append([]string(nil), required[tool]...)
}

func jsonField(field reflect.StructField) (name string, optional bool, skip bool) {
	tag := field.Tag.Get("json")
	parts := strings.Split(tag, ",")
	if len(parts) > 0 && parts[0] == "-" {
		return "", false, true
	}
	name = field.Name
	if len(parts) > 0 && parts[0] != "" {
		name = parts[0]
	}
	for _, option := range parts[1:] {
		if option == "omitempty" || option == "omitzero" {
			optional = true
		}
	}
	return name, optional, false
}

func toolDescription(tool auth.Tool) string {
	descriptions := map[auth.Tool]string{
		auth.ToolContextListSubgraphs:          "List context subgraphs visible to the current invocation.",
		auth.ToolContextExplore:                "Expand visible context from an anchor at a bounded depth.",
		auth.ToolContextSubscribe:              "Subscribe the current invocation to visible context subgraphs.",
		auth.ToolContextUnsubscribe:            "Cancel a context subscription owned by the current invocation.",
		auth.ToolContextAgentRetrieve:          "Ask the Context Agent to retrieve context for the current consumer.",
		auth.ToolRuntimeAwaitInputs:            "Release execution while waiting for authoritative runtime inputs.",
		auth.ToolAgentProposeOrchestration:     "Submit orchestration advice; Runtime binds authority fields.",
		auth.ToolAgentSubmitRequirement:        "Submit a new requirement to the Task Manager.",
		auth.ToolAgentListTaskMemoryCandidates: "List task memory candidates for the current task.",
		auth.ToolAgentSubmitMemoryCandidate:    "Submit a sourced task memory candidate.",
		auth.ToolAgentSubmitPhaseOutput:        "Submit structured output and artifact references for this phase.",
		auth.ToolCoordinationSnapshot:          "Read the Task Manager's visible coordination graph snapshot.",
		auth.ToolTaskManagerSubmitDecision:     "Persist a structured Task Manager decision.",
		auth.ToolCoordinationReplacePending:    "Replace only the not-yet-executed coordination subgraph.",
		auth.ToolCoordinationTransition:        "Apply a persisted decision as one graph state transition.",
		auth.ToolContextRegisterTaskSubgraph:   "Register the authoritative context subgraph for a task.",
		auth.ToolContextProjectTaskContext:     "Project verified task context into task subgraphs.",
		auth.ToolContextFinalizeTaskMemory:     "Freeze a task memory batch and start final review.",
		auth.ToolContextGetSubgraph:            "Read one visible general context subgraph.",
		auth.ToolContextGetNode:                "Read one visible general context node.",
		auth.ToolContextSearch:                 "Search general context by keywords, scope, and anchors.",
		auth.ToolContextCreateNode:             "Create a controlled general context node.",
		auth.ToolContextUpdateNode:             "Update a controlled general context node.",
		auth.ToolContextDeleteNode:             "Delete a controlled general context node.",
		auth.ToolContextCreateSubgraph:         "Create a controlled general context subgraph.",
		auth.ToolContextUpdateSubgraph:         "Update a controlled general context subgraph.",
		auth.ToolContextDeleteSubgraph:         "Delete a controlled general context subgraph.",
		auth.ToolContextSubmitReview:           "Submit final Context Agent review for task memory candidates.",
		auth.ToolWorkspaceList:                 "List controlled paths in the current invocation workspace.",
		auth.ToolWorkspaceRead:                 "Read a file from the current invocation workspace.",
		auth.ToolWorkspaceWritePlan:            "Write a planner-approved file.",
		auth.ToolWorkspaceWrite:                "Write a file allowed by the current workspace lease.",
		auth.ToolWorkspaceRun:                  "Run an argv command in the current workspace.",
		auth.ToolWorkspaceDiff:                 "Read the controlled diff for the current workspace.",
		auth.ToolEvidenceRegister:              "Register evidence produced by the current invocation.",
	}
	if description := descriptions[tool]; description != "" {
		return description
	}
	return "Threadmill invocation-scoped tool " + string(tool) + "."
}
