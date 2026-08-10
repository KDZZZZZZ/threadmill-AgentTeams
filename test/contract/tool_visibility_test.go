package contract

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime/promptcatalog"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/transport/mcpapi"
)

func TestSkillToolsMatchRegistryAndRoleBoundaries(t *testing.T) {
	t.Parallel()

	noop := mcpapi.HandlerFunc(func(context.Context, auth.BoundScope, json.RawMessage) (any, error) { return nil, nil })
	specs := make([]mcpapi.ToolSpec, 0, len(auth.CanonicalTools()))
	for _, tool := range auth.CanonicalTools() {
		specs = append(specs, mcpapi.ToolSpec{ID: tool, Handler: noop})
	}
	registry, err := mcpapi.NewRegistry(specs...)
	if err != nil {
		t.Fatalf("build canonical MCP registry: %v", err)
	}
	catalog, err := promptcatalog.Load(filepath.Join("..", ".."), registry.AvailableTools())
	if err != nil {
		t.Fatalf("load prompt and Skill catalog: %v", err)
	}

	profiles := []struct {
		role      auth.Role
		operation string
		want      []auth.Tool
	}{
		{auth.RoleTaskManager, "", toolList(
			auth.ToolContextListSubgraphs, auth.ToolContextExplore,
			auth.ToolContextSubscribe, auth.ToolContextUnsubscribe,
			auth.ToolContextAgentRetrieve, auth.ToolCoordinationSnapshot,
			auth.ToolTaskManagerSubmitDecision, auth.ToolCoordinationReplacePending,
			auth.ToolCoordinationTransition, auth.ToolContextRegisterTaskSubgraph,
			auth.ToolContextProjectTaskContext, auth.ToolContextFinalizeTaskMemory,
		)},
		{auth.RoleContext, "retrieve", toolList(
			auth.ToolContextListSubgraphs, auth.ToolContextExplore,
			auth.ToolContextGetSubgraph, auth.ToolContextGetNode, auth.ToolContextSearch,
		)},
		{auth.RoleContext, "curate", toolList(
			auth.ToolContextListSubgraphs, auth.ToolContextExplore,
			auth.ToolContextGetSubgraph, auth.ToolContextGetNode,
			auth.ToolContextCreateNode, auth.ToolContextUpdateNode, auth.ToolContextDeleteNode,
			auth.ToolContextCreateSubgraph, auth.ToolContextUpdateSubgraph, auth.ToolContextDeleteSubgraph,
		)},
		{auth.RoleContext, "review", toolList(
			auth.ToolContextListSubgraphs, auth.ToolContextExplore,
			auth.ToolContextGetSubgraph, auth.ToolContextGetNode,
			auth.ToolContextSearch, auth.ToolContextSubmitReview,
		)},
		{auth.RolePlanner, "", phaseTools(
			auth.ToolWorkspaceList, auth.ToolWorkspaceRead,
			auth.ToolWorkspaceWritePlan, auth.ToolWorkspaceDiff, auth.ToolEvidenceRegister,
		)},
		{auth.RoleExecutor, "", phaseTools(
			auth.ToolWorkspaceList, auth.ToolWorkspaceRead, auth.ToolWorkspaceWrite,
			auth.ToolWorkspaceRun, auth.ToolWorkspaceDiff, auth.ToolEvidenceRegister,
		)},
		{auth.RoleVerifier, "", phaseTools(
			auth.ToolWorkspaceList, auth.ToolWorkspaceRead,
			auth.ToolWorkspaceRun, auth.ToolWorkspaceDiff, auth.ToolEvidenceRegister,
		)},
	}
	for _, profile := range profiles {
		bundle, err := catalog.Bundle(profile.role, profile.operation)
		if err != nil {
			t.Fatalf("bundle %s/%s: %v", profile.role, profile.operation, err)
		}
		if !reflect.DeepEqual(bundle.EffectiveTools, profile.want) {
			t.Errorf("%s/%s tools = %v, want exact %v", profile.role, profile.operation, bundle.EffectiveTools, profile.want)
		}
		for _, tool := range bundle.EffectiveTools {
			if _, ok := registry.AvailableTools()[tool]; !ok {
				t.Errorf("%s/%s exposes unregistered tool %s", profile.role, profile.operation, tool)
			}
			if _, ok := auth.InvocationCapabilityTools(profile.role, profile.operation)[tool]; !ok {
				t.Errorf("%s/%s exposes tool outside invocation capability: %s", profile.role, profile.operation, tool)
			}
		}
	}

	assertVisible(t, catalog, auth.RoleTaskManager, "", auth.ToolCoordinationReplacePending, true)
	assertVisible(t, catalog, auth.RoleExecutor, "", auth.ToolCoordinationReplacePending, false)
	assertVisible(t, catalog, auth.RoleContext, "retrieve", auth.ToolContextSearch, true)
	assertVisible(t, catalog, auth.RoleTaskManager, "", auth.ToolContextSearch, false)
	assertVisible(t, catalog, auth.RoleExecutor, "", auth.ToolContextSearch, false)
	assertVisible(t, catalog, auth.RolePlanner, "", auth.ToolWorkspaceWritePlan, true)
	assertVisible(t, catalog, auth.RolePlanner, "", auth.ToolWorkspaceWrite, false)
	assertVisible(t, catalog, auth.RoleVerifier, "", auth.ToolWorkspaceWrite, false)

	for _, forbidden := range []auth.Tool{
		"runtime.startPhase",
		"runtime.stopPhase",
		"runtime.resumePhase",
		"coordination.createTask",
		"coordination.deleteEdge",
		"merge.execute",
		"agentteams.dispatch",
	} {
		if auth.IsCanonicalTool(forbidden) {
			t.Errorf("internal/forbidden tool became canonical Agent MCP surface: %s", forbidden)
		}
	}

	missingRequired := registry.AvailableTools()
	delete(missingRequired, auth.ToolContextExplore)
	if _, err := promptcatalog.Load(filepath.Join("..", ".."), missingRequired); err == nil {
		t.Fatal("catalog loaded while a required registered tool was missing")
	}
}

func phaseTools(delivery ...auth.Tool) []auth.Tool {
	common := []auth.Tool{
		auth.ToolContextListSubgraphs, auth.ToolContextExplore,
		auth.ToolContextSubscribe, auth.ToolContextUnsubscribe,
		auth.ToolContextAgentRetrieve, auth.ToolRuntimeAwaitInputs,
		auth.ToolAgentProposeOrchestration, auth.ToolAgentSubmitRequirement,
		auth.ToolAgentListTaskMemoryCandidates, auth.ToolAgentSubmitMemoryCandidate,
		auth.ToolAgentSubmitPhaseOutput,
	}
	return toolList(append(common, delivery...)...)
}

func toolList(tools ...auth.Tool) []auth.Tool {
	result := append([]auth.Tool(nil), tools...)
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func assertVisible(t *testing.T, catalog *promptcatalog.Catalog, role auth.Role, operation string, tool auth.Tool, want bool) {
	t.Helper()
	bundle, err := catalog.Bundle(role, operation)
	if err != nil {
		t.Fatal(err)
	}
	got := false
	for _, candidate := range bundle.EffectiveTools {
		if candidate == tool {
			got = true
			break
		}
	}
	if got != want {
		t.Fatalf("visibility %s/%s tool %s = %v, want %v", role, operation, tool, got, want)
	}
}
