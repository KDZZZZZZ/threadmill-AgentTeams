package promptcatalog

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

func TestLoadAndResolveAllCanonicalAssets(t *testing.T) {
	catalog := loadTestCatalog(t)
	if len(catalog.Prompts) != 6 {
		t.Fatalf("prompt count = %d, want 6", len(catalog.Prompts))
	}
	if len(catalog.Skills) != 15 {
		t.Fatalf("skill count = %d, want 15", len(catalog.Skills))
	}
	for id, prompt := range catalog.Prompts {
		if prompt.Body == "" || len(prompt.SHA256) != 64 {
			t.Fatalf("prompt %s is not content-addressed", id)
		}
	}
}

func TestInvocationBundlesAndToolIntersection(t *testing.T) {
	catalog := loadTestCatalog(t)

	planner, err := catalog.Bundle(auth.RolePlanner, "")
	if err != nil {
		t.Fatalf("planner bundle: %v", err)
	}
	wantPlannerTools := []auth.Tool{
		auth.ToolAgentListTaskMemoryCandidates,
		auth.ToolAgentProposeOrchestration,
		auth.ToolAgentSubmitMemoryCandidate,
		auth.ToolAgentSubmitPhaseOutput,
		auth.ToolAgentSubmitRequirement,
		auth.ToolContextExplore,
		auth.ToolContextListSubgraphs,
		auth.ToolContextSubscribe,
		auth.ToolContextUnsubscribe,
		auth.ToolContextAgentRetrieve,
		auth.ToolEvidenceRegister,
		auth.ToolRuntimeAwaitInputs,
		auth.ToolWorkspaceDiff,
		auth.ToolWorkspaceList,
		auth.ToolWorkspaceRead,
		auth.ToolWorkspaceWritePlan,
	}
	if !reflect.DeepEqual(planner.EffectiveTools, wantPlannerTools) {
		t.Fatalf("planner tools = %v, want %v", planner.EffectiveTools, wantPlannerTools)
	}

	retrieve, err := catalog.Bundle(auth.RoleContext, "retrieve")
	if err != nil {
		t.Fatalf("context retrieve bundle: %v", err)
	}
	for _, forbidden := range []auth.Tool{auth.ToolContextSubscribe, auth.ToolContextCreateNode} {
		if containsTool(retrieve.EffectiveTools, forbidden) {
			t.Fatalf("context retrieve unexpectedly includes %s", forbidden)
		}
	}
	if !containsTool(retrieve.EffectiveTools, auth.ToolContextSearch) {
		t.Fatal("context retrieve is missing context.search")
	}
}

func TestRenderMakesMissingOptionalBlocksExplicitAndHashesStable(t *testing.T) {
	catalog := loadTestCatalog(t)
	bundle, err := catalog.Bundle(auth.RoleVerifier, "")
	if err != nil {
		t.Fatal(err)
	}
	first := bundle.Render(RenderData{RuntimeEnvelope: `{"invocation_id":"inv-1"}`})
	second := bundle.Render(RenderData{RuntimeEnvelope: `{"invocation_id":"inv-1"}`})
	if first.SHA256 != second.SHA256 {
		t.Fatalf("render hash is unstable: %s != %s", first.SHA256, second.SHA256)
	}
	if !strings.Contains(first.Text, MissingValue) {
		t.Fatal("rendered prompt does not mark missing optional blocks")
	}
	if strings.Contains(first.Text, "{{") {
		t.Fatalf("rendered prompt still contains a placeholder: %s", first.Text)
	}
}

func TestPhaseRenderInjectsStartOrResumeInput(t *testing.T) {
	catalog := loadTestCatalog(t)
	bundle, err := catalog.Bundle(auth.RoleExecutor, "")
	if err != nil {
		t.Fatal(err)
	}
	input := `{"kind":"resume","checkpoint_ref":"checkpoint://task-a/execute/2"}`
	rendered := bundle.Render(RenderData{StartOrResumeInput: input})
	if !strings.Contains(rendered.Text, input) {
		t.Fatalf("phase prompt is missing Start/Resume input: %s", rendered.Text)
	}
	if strings.Contains(rendered.Text, "{{START_OR_RESUME_INPUT}}") {
		t.Fatal("phase prompt retained Start/Resume placeholder")
	}
}

func TestUnknownSkillToolAndDependencyCycleFail(t *testing.T) {
	catalog := loadTestCatalog(t)
	if _, err := catalog.ResolveSkills([]string{"does-not-exist"}); err == nil {
		t.Fatal("unknown skill resolved successfully")
	}

	parsed, err := parseSkill([]byte("---\nname: bad\ndescription: bad\n---\n## 工具\n- `unknown.tool`\n"))
	if err != nil {
		t.Fatalf("parse unknown-tool fixture: %v", err)
	}
	if auth.IsCanonicalTool(parsed.Tools[0]) {
		t.Fatal("unknown tool became canonical")
	}

	cyclic := &Catalog{Skills: map[string]Skill{
		"a": {ID: "a", Dependencies: []string{"b"}},
		"b": {ID: "b", Dependencies: []string{"a"}},
	}}
	if _, err := cyclic.ResolveSkills([]string{"a"}); err == nil {
		t.Fatal("dependency cycle resolved successfully")
	}
}

func loadTestCatalog(t *testing.T) *Catalog {
	t.Helper()
	available := make(map[auth.Tool]struct{})
	for _, tool := range auth.CanonicalTools() {
		available[tool] = struct{}{}
	}
	catalog, err := Load(filepath.Join("..", "..", ".."), available)
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	return catalog
}

func containsTool(tools []auth.Tool, target auth.Tool) bool {
	for _, tool := range tools {
		if tool == target {
			return true
		}
	}
	return false
}
