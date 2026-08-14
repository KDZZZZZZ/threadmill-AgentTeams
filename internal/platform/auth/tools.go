package auth

import "sort"

var canonicalTools = []Tool{
	ToolContextListSubgraphs,
	ToolContextExplore,
	ToolContextSubscribe,
	ToolContextUnsubscribe,
	ToolContextAgentRetrieve,
	ToolRuntimeAwaitInputs,
	ToolAgentProposeOrchestration,
	ToolAgentSubmitRequirement,
	ToolAgentListTaskMemoryCandidates,
	ToolAgentSubmitMemoryCandidate,
	ToolAgentSubmitPhaseOutput,
	ToolCoordinationSnapshot,
	ToolTaskManagerSubmitDecision,
	ToolCoordinationReplacePending,
	ToolCoordinationTransition,
	ToolContextRegisterTaskSubgraph,
	ToolContextProjectTaskContext,
	ToolContextFinalizeTaskMemory,
	ToolContextGetSubgraph,
	ToolContextGetNode,
	ToolContextSearch,
	ToolContextCreateNode,
	ToolContextUpdateNode,
	ToolContextDeleteNode,
	ToolContextCreateSubgraph,
	ToolContextUpdateSubgraph,
	ToolContextDeleteSubgraph,
	ToolContextSubmitReview,
	ToolWorkspaceList,
	ToolWorkspaceRead,
	ToolWorkspaceWritePlan,
	ToolWorkspaceWrite,
	ToolWorkspaceRun,
	ToolWorkspaceDiff,
	ToolEvidenceRegister,
}

// CanonicalTools returns the closed set of tool IDs understood by the current
// Threadmill runtime. Callers receive a copy and cannot change policy state.
func CanonicalTools() []Tool {
	tools := append([]Tool(nil), canonicalTools...)
	sort.Slice(tools, func(i, j int) bool { return tools[i] < tools[j] })
	return tools
}

// RoleCapabilityTools returns the maximum role-level capability. Invocation
// policy must still intersect it with loaded Skill tools and runtime presence.
func RoleCapabilityTools(role Role) map[Tool]struct{} {
	tools := make(map[Tool]struct{})
	for _, tool := range canonicalTools {
		if roleAllowsTool(role, tool) {
			tools[tool] = struct{}{}
		}
	}
	return tools
}

// InvocationCapabilityTools returns the exact maximum for a role/operation
// pair before Skill and runtime-availability intersection.
func InvocationCapabilityTools(role Role, operation string) map[Tool]struct{} {
	tools := make(map[Tool]struct{})
	for _, tool := range canonicalTools {
		if operationAllowsTool(role, operation, tool) {
			tools[tool] = struct{}{}
		}
	}
	return tools
}

func IsCanonicalTool(tool Tool) bool {
	for _, candidate := range canonicalTools {
		if candidate == tool {
			return true
		}
	}
	return false
}
