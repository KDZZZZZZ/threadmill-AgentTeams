# Task Manager Agent 详细设计

版本：v0.4
状态：Draft

> **语义权威**：本文语义以 [docs/threadmill-unified-design.md](./threadmill-unified-design.md) 为准；当本文与早期草案冲突时，以统一设计为准。本文第 9 节说明 AgentTeams 的 TeamHarness Leader/Manager 工具如何收口为受控 adapter，并区分直接复用、适配封装、Threadmill 新建与不应复用四类。

---

## 1. 定位

Task Manager Agent 是 **Coordination Graph 的唯一写入口**，也是**默认编排者**。它不是绕过 Runtime 的后台服务，而是经 Agent Runtime 启动、授权、观测和记录的系统 Agent Invocation。

它的责任是：把 Requirement 规整为 Task Contract，以 Phase Endpoint 为粒度编排 Coordination Graph，规定每个 endpoint 的 DeliverySpec 与 ReportSpec，读取 completed endpoint 的产出决定后续编排，并审批运行中 Agent 提交的 `OrchestrationProposal`。

它的硬边界：

> Task Manager 只负责"把需求转成任务契约（what + why + done）"并编排图，绝不产出 how；how 是 plan 阶段 planner 的专属职责。Task Manager 不旁观 phase 运行过程、不选实现方案、不写 Context Graph、不操作 Workspace、不宣布 Task 完成。

## 2. 默认编排职责

```text
Requirement（人类或 agent 提交，保存来源）
  -> Task Manager 规整为 Task Contract（what / why / done / 边界）
  -> 创建 Task、Phase/Decision Endpoint、edge 和 blocker，并创建轮次（Round）
  -> 为每个 phase endpoint 写入 DeliverySpec 与 ReportSpec
  -> 读取所有 completed endpoint 的 PhaseOutput / 报告 / 证据，决定后续编排
  -> 审批 OrchestrationProposal，接受后热修改图并明确当前 Invocation 处置
```

编排是默认动作，不是特例：新 Requirement 到达、endpoint 完成、建议被接受、结果失效，都由 Task Manager 推进图。Scheduler 只从图中选择 runnable endpoint 并请求 Runtime 启动 Invocation，不创建或修改图。

Task Manager 与 phase agent 一样使用 Context Slice、图探索、检索、订阅和自动 Delta 获取外部记忆，但不读取 phase 的未提交过程上下文。

## 3. 完成报告可见，过程上下文不可见

```text
Task Manager 可以读取：
  - Requirement（来源与原始表达）
  - 所有 completed endpoint 的 PhaseOutput、交付物引用、报告引用、证据引用
  - 自己可见的 Context Subgraph 列表、描述与 Context Slice / Delta
  - Coordination Graph 全量（它是唯一写入口，天然读全图）

Task Manager 不可以读取：
  - plan / execute / verify 运行中的推理、工具输出、探索轨迹、未提交上下文
  - 未进入 PhaseOutput 的任何中间状态
```

这是由 Runtime 边界保证的：Runtime 归一化 Agent Event 并保存 transcript、tool output、diff 和测试证据，但**不向 Task Manager 暴露未提交的 phase 过程上下文**。进入 Task Manager 视野的只有两类结构化边界输出：

1. 阶段结束时的 `PhaseOutput`；
2. 运行中主动提交的 `OrchestrationProposal`。

## 4. 编排建议审批

运行中的 phase agent 发现编排不再合适（拆分机会、缺少前置、串并调整、执行/验证失败、计划失效）时，统一提交 `OrchestrationProposal`：

```go
type OrchestrationProposal struct {
    FromEndpoint         PhaseEndpointRef `json:"from_endpoint"`
    OrchestrationAdvice  string           `json:"orchestration_advice"` // split, dependency, serial/parallel, replan...
    DeliverySpecAdvice   string           `json:"delivery_spec_advice"`
    ReportSpecAdvice     string           `json:"report_spec_advice"`
    Rationale            string           `json:"rationale"`
    EvidenceRefs         []string         `json:"evidence_refs"`
}
```

Task Manager 的审批流程：

```text
收到建议
  -> 校验来源 endpoint、当前 graph revision、理由和 evidence
  -> 接受：热修改图
       - split       -> 创建必要 Task/endpoint，为每个新 phase 写 DeliverySpec/ReportSpec，连接边
       - dependency  -> 创建前置 Task 或补边，边连到最早消费结果的 endpoint
       - serial/parallel -> 调整尚待执行的计划与受影响 endpoint
       - replan      -> 使受影响旧结果失效，调整 plan/execute/verify 编排
  -> 改写：把建议按图现状修正后再落地（返回结构化说明）
  -> 拒绝：只返回结构化理由
  -> 无论哪种结果，都明确当前 Invocation 是继续、阻塞还是取消
```

关键语义：

- **建议不结束当前 phase。** 若允许继续，phase agent 最终仍须提交原 endpoint 的 `PhaseOutput`。
- 建议是自由文本意图、理由和对未来 endpoint 契约的建议，不是图命令；phase agent 不决定创建哪些 Task、如何连边或哪些 endpoint 失效。
- **失败、拆分、前置、串并调整没有分叉协议**——不按原因拆出 Split Request、Failure Request 或 Rework Task 等实体，全部走 `OrchestrationProposal` 一个通道。
- Scheduler 不解释建议，只在图更新后重算 runnable endpoint。
- Runtime 只记录并转交建议（同一建议重复提交只转交一次），不做编排判断。
- 图变更历史由 Event Log 审计，但审计机制不限制 Coordination Graph 的运行时热修改。

## 5. Context 接口使用

Task Manager 与所有 phase agent 使用**同一个 Context 接口**：

```go
type ContextService interface {
    ListSubgraphs(ctx context.Context, req ListSubgraphsRequest) ([]SubgraphSummary, error)
    Explore(ctx context.Context, req ExploreRequest) (ContextSliceDelta, error)
    Retrieve(ctx context.Context, req RetrieveRequest) (ContextRetrieveResult, error)
    Subscribe(ctx context.Context, req SubscribeRequest) (ContextSubscription, error)
}
```

- 初始 Context Slice 由 Context service 按 role/purpose/权限自动生成，并对切片包含的子图自动建立与 Invocation 同寿命的订阅；
- Task Manager 可以像 phase agent 一样列出可见子图、沿 frontier 探索、请求 Ctx Manager 检索（检索结果所含子图自动订阅）、主动选择子图订阅；
- 订阅更新由自动化订阅执行器产生 Context Delta 并自动推送，Ctx Manager 不逐条判断、不主动提示；
- **Task Manager 不写 Context Graph**——只有显式 `MemoryCandidate` 经 Ctx Manager 准入后才能更新 Context Graph；Ctx Manager 只响应检索与准入 MemoryCandidate 两个边界。

## 6. 唯一写入口与热修改

```text
只有 Task Manager Agent 能写 Coordination Graph。

  Scheduler        -> 只读图，选择 runnable endpoint
  Runtime          -> 只记录事件与转交 PhaseOutput / OrchestrationProposal
  phase agent      -> 只提交 PhaseOutput / OrchestrationProposal，或读取可见图视图
  Verifier         -> 通过 Runtime 产出 Verify Result；不写图
  Merge Queue      -> 只提交冲突/合入证据；不写图
  Ctx Manager      -> 不写 Coordination Graph
```

Coordination Graph 是可热修改的当前编排：Task Manager 每次审批建议后的落地都是图修改，包括创建 Task/endpoint、写契约、连边、使旧结果失效、标记 blocked、设置 done。Event Log 自动记录每次 mutation 供审计，但审计不构成对热修改的限制。

## 7. 输入契约

### 7.1 Requirement（统一入口）

人类与 phase agent 都提交 Requirement；Task Manager 负责规整为 Task Contract，把原始表达作为 provenance 保留（链接或引用），不把"用户需求本身"当成可调度 Task。

```go
type Requirement struct {
    Source      string   `json:"source"` // human | plan | execute | verify | merge_queue
    Text        string   `json:"text"`
    Goal        string   `json:"goal,omitempty"`
    Constraints []string `json:"constraints,omitempty"`
    EvidenceRefs []string `json:"evidence_refs,omitempty"`
    PriorityHint string   `json:"priority_hint,omitempty"`
}
```

没有"human 宽松模式 / agent 严格模式"的分叉：所有来源都进入同一规整流程，差异只体现在 Task Manager 对来源证据的校验强度。Requirement 的验收意图由 Task Manager 提炼为可测验收标准；phase agent 若对"怎么算完成"有补充，通过 `OrchestrationProposal` 的 `DeliverySpecAdvice` / `ReportSpecAdvice` 提出，由 Task Manager 决定是否写入 endpoint 契约。

### 7.2 编排建议与阶段输出

- `OrchestrationProposal`：运行中主动提交（见第 4 节）。
- `PhaseOutput`：阶段结束时的端点输出，必须满足该 endpoint 的 `DeliverySpec` / `ReportSpec`，否则 Runtime 校验不通过、不得进入 completed。

## 8. 写入前的编排检查规则

Task Manager 在修改 Coordination Graph 前，至少检查以下内容：

### 8.1 是否真的需要新 Task

默认把工作留在当前 phase 的执行范围内。只有工作具备独立验收、独立失败或重试、跨时间等待、不同权限或 Workspace、被其他 Task 直接依赖，或生命周期超过当前 phase Invocation 时，才建立新 Task。文件读取、一次工具调用、局部摘要和同一批准计划中的连续命令，不应仅因为可观察就被提升为 Task。

### 8.2 边是否连接到最早需要结果的位置

依赖必须落到真正消费结果的 Phase Endpoint：

```text
B.verify -> A.plan     A 的方案依赖 B 的已验证结论
B.verify -> A.execute  A 可以先规划，但实施必须等待 B
B.verify -> A.verify   A 可以先实施，但最终验收必须包含 B
```

每条边都必须说明 source endpoint、target endpoint、控制条件、沿边传递的交付物/报告/证据，以及条件为 false 或结果过期时的处理。

### 8.3 如何处理失败和过期结果

```text
- 局部实现或验证失败：失效旧输出并为同一 Task Contract 重开轮次。
- Task Contract 不完整或自相矛盾：阻塞受影响 endpoint，请求澄清或重新立约。
- verify 暴露出独立工作：接受 OrchestrationProposal(split)，登记新 Task 并连接到消费其结果的 endpoint。
- candidate 相对新 revision 已过期：使旧验证失效，重新 verify 或重新 plan。
- 高风险决定缺少权限：创建或关联 human decision endpoint，不得推断已经批准。
```

### 8.4 必须拒绝的模式

```text
- 每个 agent 或 tool call 建一个 Task。
- 只在 prompt 中描述依赖，不写入图。
- 只有 execute 或 verify 受影响，却阻塞整个 Task。
- 为表达 parent/child 所有权而制造环。
- 把 worker summary 当作验证 evidence。
- 每次 attempt 失败都创建新 Task。
- acceptance 和 merge 条件未满足就标记 done。
- 为迁就某个实现方案而修改 Requirement 或 Task Contract 内容。
```

每次写入必须能回答：这次 mutation 接受了什么、创建或关联了哪个 Task、增加了哪些 endpoint / edge / blocker、每项变更的理由，以及哪些 endpoint 的 runnable 状态发生了变化。

## 9. TeamHarness Leader/Manager 工具收口为受控 adapter

### 9.1 AgentTeams 现状

AgentTeams 的编排由两个角色直接持有工具：**Manager**（`third_party/agentteams/manager/agent/AGENTS.md`，通过 `agt` CLI 管理 Worker/Team，经 `agentteams-manager-tools` QwenPaw 插件注册 `projectflow` / `taskflow` / `message` / `filesync` 四个工具）和 **Team Leader**（`third_party/agentteams/manager/agent/team-leader-agent/AGENTS.md`，用 `projectflow` 管 Project 生命周期与 DAG/Loop 状态、`taskflow` 管任务委派与结果检查、`filesync` 管共享文件、`message` 管跨房间消息）。Worker 只持有 `taskflow` 的 `ack_task` / `submit_task` 与 `filesync`（`third_party/agentteams/manager/agent/copaw-worker-agent/AGENTS.md`）。工具注册路径：

- TeamHarness MCP server：`third_party/agentteams/plugins/teamharness/mcp/server.py`（`TOOL_NAMES = [health, message, roomflow, filesync, artifact, projectflow, taskflow]`，按 runtime role 做可见性与动作门控）；
- Manager 工具插件：`third_party/agentteams/plugins/agentteams-manager-tools/plugin.py`（`api.register_tool()` 注册四工具，后端为 `copaw_worker.hooks.tools.*`）；
- 插件清单：`third_party/agentteams/plugins/teamharness/plugin.yaml`。

AgentTeams 的"Leader 在提示词循环里决定下一步 DAG"模式，以及"结果到达后由 Leader 检查并接受"的 Event Resume Contract（`third_party/agentteams/docs/teamharness-project-task-runtime-design.md`），是 Threadmill 收口的对象。

### 9.2 收口原则

Threadmill 不让 phase agent 持有原始 `projectflow` / `taskflow` 写工具。所有图写入收敛到一个受控 adapter——它是 Task Manager 专属的**写工具面**，不是新 agent，也不是新实体：

```text
phase agent（plan / execute / verify）
  只读图视图（resolve_project / ready_nodes / check_active_tasks 的读语义）
  + Context 接口（切片 / 探索 / 检索 / 订阅）
  + OrchestrationProposal 提交通道
  （不持有任何 projectflow / taskflow 写工具）

Task Manager
  唯一持有受控写工具面：adapter
  -> adapter 校验角色与路径，映射为 agentteams 动作序列
  -> 图状态经 agentteams 存储协议落盘，变更记入 Event Log

Worker（若复用 agentteams worker 运行时）
  taskflow ack_task / submit_task 原样保留，由 Task Manager 的委派链路驱动
```

### 9.3 动作映射（适配封装）

| Task Manager 意图 | Adapter 映射的 agentteams 动作 | 类别 |
| --- | --- | --- |
| 创建 Task 容器 | `projectflow(action=create_project)`（ProjectMeta + reply_route） | 直接复用 |
| 初始图 / 热修改（拆分、补前置、重排） | `projectflow(action=plan_dag\|plan_loop)`（整体替换 + 图校验，原子） | 直接复用 |
| 计算可运行 endpoint | `projectflow(action=ready_nodes\|ready_loop_nodes)`（pending 且依赖被接受） | 直接复用 |
| 委派执行并写入 Task Contract | `taskflow(action=delegate_task)`（幂等；spec.md 头部由 adapter 注入 Task Contract 与 DeliverySpec/ReportSpec；自动 Team Room 通知） | 适配封装 |
| 检查提交结果 | `taskflow(action=check_task)`（读取校验 result.md，返回 effective，不改图） | 直接复用 |
| 接受结果（verify passed 后推进图） | `projectflow(action=accept_task_result)`（唯一推进入口，节点 → completed） | 直接复用 |
| 失败重排 / 拆分落地 | 依据 `OrchestrationProposal` 再 `plan_dag` / `plan_loop`；REVISION_NEEDED / BLOCKED 不自动推进图，由 Task Manager 决策 | 适配封装 |
| 阻塞与人工决定 | `pause_project` / `resume_project`；loop 用 `record_loop_iteration(ask_user / stop_blocked)`；DAG blocker 经 plan_dag 重排或暂停 | 直接复用 |
| 完成 | `complete_project` + requester report（`reply_route` + `mark_requester_report_sent`） | 直接复用 |
| 阶段输出落盘 | `submit_task` 的 result.md 协议（STATUS/SUMMARY/DELIVERABLES）作为 PhaseOutput 载体 | 适配封装 |
| 过程上下文 / 记忆 | 不复用（AgentTeams 无 Context Graph）；Threadmill 新建 Context Graph，MemoryCandidate 走 Ctx Manager | Threadmill 新建 |
| 审计与调度 | Event Log、Scheduler 为 Threadmill 新建；`ready_nodes` 只作为 Scheduler 输入 | Threadmill 新建 |
| 结果修订 / 打断 | `cancel_task`（leader）对应 Invocation 取消；`INTERRUPTED` 结果由控制面产生，不视为正常完成 | 适配封装 |

### 9.4 分类汇总

```text
直接复用（AgentTeams 已证实）：
  projectflow 的 create_project / plan_dag / plan_loop / ready_nodes /
  ready_loop_nodes / record_loop_iteration / accept_task_result /
  pause_project / resume_project / complete_project / resolve_project /
  mark_requester_report_sent / check_active_tasks
  taskflow 的 delegate_task / check_task / ack_task / submit_task / cancel_task
  MCP role 门控（leader / worker / remote-member）
  filesync 与 shared/ 存储协议（meta.json / plan.md / spec.md / result.md）

适配封装：
  spec.md 头部注入 Task Contract 与 DeliverySpec/ReportSpec
  result.md 协议承载 PhaseOutput
  TaskMeta / plan 节点状态机投影为 Phase Endpoint
  REVISION_NEEDED / BLOCKED / INTERRUPTED -> OrchestrationProposal 语义

Threadmill 新建（AgentTeams 没有，不宣称已有）：
  Coordination Graph 语义存储、graph revision / input revision
  OrchestrationProposal 通道、DeliverySpec/ReportSpec 契约
  Event Log、Scheduler、Context Graph / Ctx Manager / 切片订阅推送
  Merge Queue、git worktree 隔离、Workspace Binding 生命周期

不应复用：
  - 手工编辑 shared/projects/**、shared/tasks/**（旁路写图）
  - message / roomflow 作为 agent 间编排通道（Threadmill 无 mailbox；
    仅保留 requester report 与分配通知边界）
  - heartbeat 推进项目状态
  - "Leader 在提示词里决定下一步"的编排模式（编排权归 Task Manager）
```

### 9.5 Adapter 不变量

```text
1. 写工具面只对 Task Manager 可见；phase agent 与 Worker 拿不到写动作。
2. Adapter 不做编排判断：不解释 OrchestrationProposal，不决定创建哪些 Task / 如何连边。
3. Adapter 只做映射、角色校验、路径安全与幂等，并维护 Threadmill 侧 revision 计数。
4. Adapter 的每次写都产生 Event Log 审计记录（Threadmill 新建的 Event Log）。
5. AgentTeams 的角色门控直接复用：delegate/check/cancel 仅 leader 面，ack/submit 仅 worker 面。
6. Adapter 不得把 agentteams 的矩阵消息通道当作编排语义使用。
```

## 10. 不变量

```text
1. 所有 agent invocation 都必须经 Agent Runtime，包括 Task Manager Agent 与 Ctx Manager Agent。
2. 除 Task Manager Agent 外，任何角色不得直接写 Coordination Graph；图可热修改。
3. Task Manager 不产出 how；how 属于 plan 阶段。
4. Task Manager 只读取 completed endpoint 的 PhaseOutput / 报告 / 证据；不读取未提交过程上下文。
5. 失败、拆分、前置、串并调整统一走 OrchestrationProposal；不存在其他建议协议或实体。
6. 每个 phase endpoint 必须有 DeliverySpec 与 ReportSpec，否则不可调度。
7. Task Manager 与 phase agent 使用同一 Context 读接口；Task Manager 不写 Context Graph。
8. Ctx Manager 只响应检索与准入 MemoryCandidate；订阅来自切片自动订阅或 Agent 主动订阅，推送自动执行。
9. 实现型 Task 无可验收标准不得进入执行。
10. 冲突与失败只暴露和编排，不由 Task Manager 替人拍板；高风险决定必须出现 decision endpoint。
11. 所有图 mutation 都产生 Event Log 事件，便于追溯。
12. Task Manager 经受控 adapter 复用 agentteams 已证实能力；禁止旁路写 agentteams 状态文件。
```
