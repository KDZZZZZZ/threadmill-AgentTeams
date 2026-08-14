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

func TestRenderedPromptsRequireAtomicContextReuseAndMemoryExternalization(t *testing.T) {
	catalog := loadTestCatalog(t)
	planner, err := catalog.Bundle(auth.RolePlanner, "")
	if err != nil {
		t.Fatal(err)
	}
	plannerText := planner.Render(RenderData{}).Text
	for _, required := range []string{
		"Context 不是装饰信息",
		"一个阶段通常会产生多个候选",
		"一条 Candidate 只写一个可独立判断真假的陈述",
		"不要只在结尾提交一条总括候选",
		"删除测试",
		"默认在首次写入计划产物或提交新知识证据前",
		"两张图是独立的权威资源边界",
		"不得用原生文件、shell、数据库、HTTP",
		"node:<NodeID>",
		"当前有效订阅并集",
		"统一使用宿主提供的原生工具",
		"不要调用 Threadmill `workspace.*` MCP 文件工具形成第二条工作区版本线",
	} {
		if !strings.Contains(plannerText, required) {
			t.Fatalf("planner prompt is missing atomic memory rule %q", required)
		}
	}

	manager, err := catalog.Bundle(auth.RoleTaskManager, "")
	if err != nil {
		t.Fatal(err)
	}
	managerText := manager.Render(RenderData{}).Text
	for _, required := range []string{
		"一个巨型节点", "每条约束、验收项", "Context 写工具不暴露给你", "task_policies", "稳定 ProjectionID",
		"plan→execute→verify 是 Runtime 内建顺序", "只表达跨 Task 依赖", "不得扫描 AgentTeams 历史任务",
		"phase_output -> submitted", "selected_endpoint", "ArtifactRef、output ref、command ID",
	} {
		if !strings.Contains(managerText, required) {
			t.Fatalf("task manager prompt is missing context lifecycle rule %q", required)
		}
	}
	for _, forbidden := range []auth.Tool{auth.ToolContextRegisterTaskSubgraph, auth.ToolContextProjectTaskContext, auth.ToolContextFinalizeTaskMemory} {
		for _, tool := range manager.EffectiveTools {
			if tool == forbidden {
				t.Fatalf("task manager agent unexpectedly exposes Runtime-internal context write tool %s", forbidden)
			}
		}
	}

	retriever, err := catalog.Bundle(auth.RoleContext, "retrieve")
	if err != nil {
		t.Fatal(err)
	}
	text := retriever.Render(RenderData{}).Text
	for _, required := range []string{
		"不能用空关键词退化成整图返回",
		"不得选择 TaskID、ProjectID、InvocationID",
		"显式给出“机械关键词”",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("context retrieve prompt is missing lexical-search rule %q", required)
		}
	}
}

func TestTaskManagerPromptRoutesFailedMergeToReopenRound(t *testing.T) {
	catalog := loadTestCatalog(t)
	bundle, err := catalog.Bundle(auth.RoleTaskManager, "")
	if err != nil {
		t.Fatal(err)
	}
	text := bundle.Render(RenderData{}).Text
	for _, required := range []string{
		"Targeted Verify 重编排动作约束",
		"verify 也可以仍是 `pending`",
		"Runtime 原子重开同一 Task 的 execute+verify",
		"不能用来重启已经 satisfied/rejected 的 execute",
		"正常 Verify 判断",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("task manager prompt is missing targeted verify rule %q", required)
		}
	}
}

func TestVerifierPromptRequiresGeneratedReportRegistrationForTargetedVerify(t *testing.T) {
	catalog := loadTestCatalog(t)
	bundle, err := catalog.Bundle(auth.RoleVerifier, "")
	if err != nil {
		t.Fatal(err)
	}
	text := bundle.Render(RenderData{PhaseSpec: `{"schema":"threadmill.targeted_verify.v1"}`}).Text
	for _, required := range []string{
		"evidence.register(type=generated_report",
		"content_type=application/json",
		"body=<strict threadmill.targeted_verify.v1 JSON>",
		"type=json",
		"type=tool_output",
		"agent.submitPhaseOutput.report_ref",
		"不得再提交 verdict=fail 的 PhaseOutput",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("verifier prompt is missing targeted generated_report rule %q", required)
		}
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
