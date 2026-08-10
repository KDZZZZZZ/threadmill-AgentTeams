# 设计—代码—测试追踪表

本文用于回答“工具、接口和对象是否按设计文档实现”。权威关系是：设计文档冻结语义和边界，Go 类型/OpenAPI 冻结机器契约，contract/E2E 测试阻止漂移。GUI DTO 只是可重建投影，不新增领域对象。

## 核心对象

| 设计对象 | 权威文档 | 唯一代码模型 | 自动证据 |
| --- | --- | --- | --- |
| `Task`、`PhaseEndpointRef`、`PhaseEndpoint`、`Edge`、`Blocker`、`PhaseResult` | `coordination-graph.md` §2 | `internal/coordination/types.go` | `internal/coordination/*_test.go` |
| `PhaseCommand(start/stop/resume)`、`GraphSnapshot`、`PendingSubgraph`、`GraphTransition` | `coordination-graph.md` §2–4 | `internal/coordination/types.go` | `internal/coordination/runtime_test.go`、`service_test.go` |
| `Invocation`、lease、BindingRef 与 phase output envelope | `phase-agent.md`、`agent-runtime.md` | `internal/runtime/invocation.go`、`internal/runtime/phase/`、`internal/evidence/envelope.go` | `internal/runtime/*_test.go`、`internal/runtime/phase/*_test.go` |
| Context node/subgraph、Subscription、Context Slice、TaskMemoryBuffer candidate | `context-graph.md` | `internal/contextgraph/` | `internal/contextgraph/*_test.go`、`internal/contextagent/*_test.go` |
| Workspace binding、ArtifactRef、Event | `workspace-merge.md`、`event-artifact-store.md` | `internal/workspace/`、`internal/evidence/` | 对应包测试与 `test/security/` |
| Capacity | `scheduler-budget.md`、ADR 0006 | `internal/scheduler.Capacity`；`uiprojection.CapacityState` 仅展示 waiting/degraded 等派生字段 | `internal/scheduler/scheduler_test.go`、GUI E2E |
| UI snapshot、Inspector、UiEvent | ADR 0006、OpenAPI | `internal/uiprojection/`；HTTP 类型直接 alias，不复制图/Context 模型 | `test/contract/openapi_projection_test.go`、`web/e2e/` |

## 边界接口

| 接口 | 调用者 → 实现者 | 设计与代码 | 约束证据 |
| --- | --- | --- | --- |
| `TaskManagerGraph` | Task Manager → Coordination Module | `coordination-graph.md` §3.1；`internal/coordination/types.go` | 只有 Task Manager capability；`ReplacePending` 无 CRUD/patch；DecisionRef 先持久化 |
| `PhaseController.Apply` | 内部 GraphRuntime → Agent Runtime/Adapter | `coordination-graph.md` §3.2；`internal/coordination/types.go` | 单一 `PhaseCommand` 覆盖 start/stop/resume；Phase Agent 不能反向调用 |
| Phase Runtime `Apply/SubmitPhaseOutput` | Adapter/执行宿主 → phase runtime | `phase-agent.md`；`internal/runtime/phase/` | stale lease/binding、checkpoint、输出路由测试 |
| Task Manager application seam | Runtime → Task Manager Agent decision | `task-manager-agent.md`；`internal/taskmanager/app.go` | 浏览器文本不进入 `TaskManagerDecision`；project-level no-change 不写图 |
| Context Agent 与 Context Graph ports | Agent Runtime/Context Agent → Context Module | `context-graph.md`、Adapter 设计 | 订阅绑定 ConsumerInvocationID；unsubscribe 后不再进入并集 |
| Agent MCP registry | 受限 Invocation → application ports | `agent-runtime.md`、各 Agent 文档；`internal/transport/mcpapi/` | Registry 可见性与调用同表；严格 JSON；服务端注入 project/task/invocation scope |
| Browser HTTP/SSE | operator session → command/query ports | ADR 0006、`api/openapi/threadmill-v1.yaml` | 浏览器只有四类 command；图/Inspector 为只读；SSE cursor 可恢复 |
| AgentTeams Adapter | Threadmill Runtime → QwenPaw/taskflow | `threadmill-agentteams-adapter-design.md` | Adapter 不持有图写凭据；第三方目录不修改 |

## Canonical Agent 工具

工具闭集由 `internal/platform/auth.CanonicalTools()` 定义，真实 handler 聚合只能来自 `internal/transport/mcpapi.AllRuntimeToolSpecs()`；`test/contract/tool_visibility_test.go` 要求两者精确相等，缺失、重复、空 handler 或额外工具都会失败。

| 工具组 | 工具 | 设计归属 |
| --- | --- | --- |
| Context 读取与订阅 | `context.listSubgraphs`、`context.explore`、`context.subscribe`、`context.unsubscribe`、`contextAgent.retrieve` | Phase/Task Manager/Context Agent 按 capability 使用；retrieve 由 Context Agent 执行 |
| Phase/Agent 提交 | `runtime.awaitInputs`、`agent.submitPhaseOutput`、`agent.submitRequirement`、`agent.proposeOrchestration` | Runtime/Phase/Task Manager 文档；proposal 只接收意图字段 |
| Task memory | `agent.listTaskMemoryCandidates`、`agent.submitMemoryCandidate` | 同 Task 共享、跨 Task 禁止 |
| Task Manager 图控制 | `coordination.snapshot`、`taskManager.submitDecision`、`coordination.replacePending`、`coordination.transition` | 只注入 Task Manager；revision、DecisionRef 由 Runtime scope 注入 |
| Context Agent 写入与终审 | `context.registerTaskSubgraph`、`context.projectTaskContext`、`context.finalizeTaskMemory`、node/subgraph CRUD、`context.search`、`context.submitReview` | 这些是 Context Graph 自身的受权工具，不是 Coordination Graph CRUD |
| Workspace | `workspace.list/read/writePlan/write/run/diff` | 绑定当前 Invocation/Workspace；调用方不能自报 Task/Binding |
| Evidence | `evidence.register` | project/task/invocation 由可信 scope 注入，大正文写 Artifact |

具体字符串常量以 `internal/platform/auth/types.go` 为机器权威；角色、Skill 和 Runtime 实际可用工具取三者交集，不能只凭工具名越权。

## GUI 验收闭环

`internal/app/fakehost` 只装配上述 canonical store/ports；`cmd/threadmilld serve --fake` 同源托管 `/v1/*`、SSE 和 `web/dist`。`web/e2e/threadmill-console.spec.ts` 验证：

1. capacity revision 改变但 graph revision 不变；
2. 浏览器不调用任何 Coordination Graph mutation endpoint；
3. Manager hold 后 Endpoint 为 `held`、旧 Invocation 为 `stopped`，resume 创建新 generation/Invocation；
4. Inspector 分开显示订阅、Context Slice 和 created-by-invocation candidate；
5. 有限 SSE 断开后 EventSource 自动重连；HTTP 合同测试验证 `Last-Event-ID` 覆盖首连 `after`。

未宣称完成的范围：生产 PostgreSQL/MinIO wiring、真实 AgentTeams 凭据 smoke、跨进程崩溃恢复和部署清单。它们仍按全仓实现计划的 W4 验收。
