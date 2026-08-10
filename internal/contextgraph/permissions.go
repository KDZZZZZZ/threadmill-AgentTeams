package contextgraph

import (
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

func requireTool(principal auth.Principal, tool auth.Tool, projectID kernel.ProjectID) error {
	_, err := auth.RequireTool(principal, tool, auth.Scope{ProjectID: projectID})
	return err
}

func canSeeSubgraph(principal auth.Principal, subgraph SubgraphRecord) bool {
	if principal.ProjectID != subgraph.ProjectID {
		return false
	}
	if subgraph.Subgraph.Kind == string(SubgraphKindGeneral) {
		return true
	}
	if principal.TaskID == "" {
		return false
	}
	return subgraph.TaskID == principal.TaskID
}

func canSeeNode(principal auth.Principal, subgraphs map[string]SubgraphRecord, record NodeRecord) bool {
	if principal.ProjectID != record.ProjectID {
		return false
	}
	if len(record.Node.SubgraphIDs) == 0 {
		return true
	}
	for _, subgraphID := range record.Node.SubgraphIDs {
		subgraph, ok := subgraphs[subgraphID]
		if ok && canSeeSubgraph(principal, subgraph) {
			return true
		}
	}
	return false
}

func requireContextAgent(principal auth.Principal, tool auth.Tool, projectID kernel.ProjectID) error {
	if principal.Role != auth.RoleContext {
		return kernel.Forbidden("context graph mutation requires Context Agent")
	}
	return requireTool(principal, tool, projectID)
}

func validateNodeKind(kind string) error {
	switch NodeKind(kind) {
	case NodeKindDirective, NodeKindFact, NodeKindHypothesis:
		return nil
	default:
		return kernel.InvalidArgument("node kind must be directive, fact, or hypothesis")
	}
}

func validateNodeStatus(status string) error {
	switch NodeStatus(status) {
	case NodeStatusAccepted, NodeStatusDisputed, NodeStatusSuperseded, NodeStatusOutdated:
		return nil
	default:
		return kernel.InvalidArgument("node status must be accepted, disputed, superseded, or outdated")
	}
}

func validateSubgraphKind(kind string) error {
	switch SubgraphKind(kind) {
	case SubgraphKindGeneral, SubgraphKindTask:
		return nil
	default:
		return kernel.InvalidArgument("subgraph kind must be general or task")
	}
}
