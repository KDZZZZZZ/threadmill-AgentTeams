package mcpapi

import (
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/contextgraph"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/workspace"
)

// RuntimeToolDependencies names the authoritative ports behind every
// canonical Agent MCP tool. AllRuntimeToolSpecs is intentionally the only
// full-registry factory so contract tests and application wiring fail when a
// canonical tool lacks a real adapter.
type RuntimeToolDependencies struct {
	ContextReader      contextgraph.ContextGraphReader
	ContextRetrieve    ContextAgentRetrieveDispatcher
	ContextCurator     contextgraph.ContextGraphCurator
	ContextSearcher    contextgraph.ContextGraphSearcher
	ContextReviewer    contextgraph.ContextCandidateReviewer
	TaskMemoryReader   contextgraph.TaskMemoryBufferReader
	CandidateSubmitter contextgraph.CandidateSubmitter
	TaskContextWriter  contextgraph.TaskContextWriter
	MemoryFinalizer    contextgraph.TaskMemoryFinalizer
	Phase              PhaseRuntime
	Requirement        RequirementSubmitter
	Orchestration      OrchestrationProposalRuntime
	TaskManager        TaskManagerAgentRuntime
	Workspace          workspace.AgentToolPort
	Evidence           EvidenceRegistrar
}

func AllRuntimeToolSpecs(deps RuntimeToolDependencies) []ToolSpec {
	specs := ContextReaderToolSpecs(deps.ContextReader)
	specs = append(specs, ContextAgentRetrieveToolSpec(deps.ContextRetrieve))
	specs = append(specs, ContextAgentGraphToolSpecs(deps.ContextCurator, deps.ContextSearcher, deps.ContextReviewer)...)
	specs = append(specs, PhaseRuntimeToolSpecs(deps.Phase)...)
	specs = append(specs, RequirementToolSpec(deps.Requirement))
	specs = append(specs, OrchestrationProposalToolSpec(deps.Orchestration))
	specs = append(specs, TaskMemoryToolSpecs(deps.TaskMemoryReader, deps.CandidateSubmitter)...)
	specs = append(specs, TaskContextToolSpecs(deps.TaskContextWriter, deps.MemoryFinalizer)...)
	specs = append(specs, TaskManagerCoordinationToolSpecs(deps.TaskManager)...)
	specs = append(specs, WorkspaceToolSpecs(deps.Workspace)...)
	specs = append(specs, EvidenceToolSpec(deps.Evidence))
	return specs
}
