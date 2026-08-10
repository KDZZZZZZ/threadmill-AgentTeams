package auth

import (
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

type PrincipalKind string

const (
	PrincipalOperator PrincipalKind = "operator"
	PrincipalAgent    PrincipalKind = "agent"
)

type Role string

const (
	RoleOperator    Role = "operator"
	RoleTaskManager Role = "task_manager"
	RoleContext     Role = "context_agent"
	RolePlanner     Role = "planner"
	RoleExecutor    Role = "executor"
	RoleVerifier    Role = "verifier"
)

func (r Role) IsPhase() bool {
	return r == RolePlanner || r == RoleExecutor || r == RoleVerifier
}

type Tool string

const (
	ToolContextListSubgraphs          Tool = "context.listSubgraphs"
	ToolContextExplore                Tool = "context.explore"
	ToolContextSubscribe              Tool = "context.subscribe"
	ToolContextUnsubscribe            Tool = "context.unsubscribe"
	ToolContextAgentRetrieve          Tool = "contextAgent.retrieve"
	ToolRuntimeAwaitInputs            Tool = "runtime.awaitInputs"
	ToolAgentProposeOrchestration     Tool = "agent.proposeOrchestration"
	ToolAgentSubmitRequirement        Tool = "agent.submitRequirement"
	ToolAgentListTaskMemoryCandidates Tool = "agent.listTaskMemoryCandidates"
	ToolAgentSubmitMemoryCandidate    Tool = "agent.submitMemoryCandidate"
	ToolAgentSubmitPhaseOutput        Tool = "agent.submitPhaseOutput"
	ToolCoordinationSnapshot          Tool = "coordination.snapshot"
	ToolTaskManagerSubmitDecision     Tool = "taskManager.submitDecision"
	ToolCoordinationReplacePending    Tool = "coordination.replacePending"
	ToolCoordinationTransition        Tool = "coordination.transition"
	ToolContextRegisterTaskSubgraph   Tool = "context.registerTaskSubgraph"
	ToolContextProjectTaskContext     Tool = "context.projectTaskContext"
	ToolContextFinalizeTaskMemory     Tool = "context.finalizeTaskMemory"
	ToolContextGetSubgraph            Tool = "context.getSubgraph"
	ToolContextGetNode                Tool = "context.getNode"
	ToolContextSearch                 Tool = "context.search"
	ToolContextCreateNode             Tool = "context.createNode"
	ToolContextUpdateNode             Tool = "context.updateNode"
	ToolContextDeleteNode             Tool = "context.deleteNode"
	ToolContextCreateSubgraph         Tool = "context.createSubgraph"
	ToolContextUpdateSubgraph         Tool = "context.updateSubgraph"
	ToolContextDeleteSubgraph         Tool = "context.deleteSubgraph"
	ToolContextSubmitReview           Tool = "context.submitReview"
)

type Principal struct {
	ActorPrincipalID kernel.ActorPrincipalID
	Kind             PrincipalKind
	ProjectID        kernel.ProjectID
	Role             Role
	TaskID           kernel.TaskID
	InvocationID     kernel.InvocationID
	Tools            map[Tool]struct{}
	AuthenticatedAt  time.Time
}

func (p Principal) HasTool(tool Tool) bool {
	_, ok := p.Tools[tool]
	return ok
}

func cloneTools(tools map[Tool]struct{}) map[Tool]struct{} {
	copied := make(map[Tool]struct{}, len(tools))
	for tool := range tools {
		copied[tool] = struct{}{}
	}
	return copied
}

func ToolSet(tools ...Tool) map[Tool]struct{} {
	set := make(map[Tool]struct{}, len(tools))
	for _, tool := range tools {
		set[tool] = struct{}{}
	}
	return set
}

type Capability struct {
	ProjectID    kernel.ProjectID
	TaskID       kernel.TaskID
	InvocationID kernel.InvocationID
	Role         Role
	Tools        map[Tool]struct{}
	ExpiresAt    time.Time
}

type Scope struct {
	ProjectID    kernel.ProjectID
	TaskID       kernel.TaskID
	InvocationID kernel.InvocationID
}

type BoundScope struct {
	ProjectID    kernel.ProjectID
	TaskID       kernel.TaskID
	InvocationID kernel.InvocationID
}
