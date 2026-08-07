# Scheduler / Budget 详细设计

版本：v1.0-draft
状态：Draft
定位：语义以 docs/threadmill-unified-design.md（第 6、7 节）为准。AgentTeams（third_party/agentteams）是归档基座，本文只复用其已证实能力，不延续其 Leader 调度模式。

---

## 1. 定位

Scheduler 是 Coordination Graph 的只读消费者。它读取图，选择当前可运行的 Phase Endpoint，按 Worker Capacity、预算和默认优先级排序，然后请求 Agent Runtime 启动对应的 Agent Invocation。

Scheduler 不做：

- 不创建、修改或删除 Task、Task Attempt、Phase Endpoint、edge、blocker；
- 不解释 OrchestrationProposal，不决定 replan、拆分或失败处置；这些由 Task Manager 裁决；
- 不启动 Ctx Manager 做普通探索或提示；初始 Context Slice 与自动订阅由 Context service 在 Invocation 创建时完成，Ctx Manager 只响应 Agent 的 Retrieve 请求与准入 MemoryCandidate；
- 不把 Agent 与 Task 永久绑定；不操作 Workspace；不合并 main。

Scheduler 与 Task Manager 的唯一协调媒介是 Coordination Graph：Task Manager 写图（含审批 OrchestrationProposal 后的热修改），Scheduler 在图 revision 更新后重算 runnable endpoint。循环为：

```text
Manager 改图 -> Scheduler 重算 runnable endpoint -> Runtime 启动 Invocation
  -> PhaseOutput / OrchestrationProposal 回到 Manager -> Manager 改图 -> …
```

Budget Model 约束系统可投入的 token、时间、并发、retry 和 verify 强度。预算只改变 Scheduler 的取舍与吞吐，不改变 Coordination Graph 语义。

## 2. Scheduler 输入

```text
- Coordination Graph（只读投影：endpoint 状态、edge condition、blocker、
  DeliverySpec/ReportSpec 完整性）
- Worker Capacity 与 worker health（可用 Agent Invocation 并发额度，见 §10 映射）
- agent capability profiles（role / 能力）
- budget status
- 冲突信号（Workspace write set 重叠）
- latest-main 待复验/合并候选状态（由图投影表达）
```

## 3. Scheduler 输出

唯一输出：向 Agent Runtime 提交 run request，启动某个 runnable Phase Endpoint 的 Agent Invocation（含 endpoint 引用、role、预算、权限；Context Slice 由 Context service 自动生成）。

不再存在以下 Scheduler 直接动作：

```text
- start planner / start executor / start verifier
    -> 统一为"启动 endpoint Invocation"，role 由 endpoint 决定；
- start ctx_manager to create context pack
    -> Ctx Manager 只在被检索调用或接收 MemoryCandidate 时工作；
       初始切片/订阅由 Context service 在 Invocation 创建时完成；
- create worktree
    -> Runtime / Workspace Service 在 Invocation 启动时创建或复用 Workspace Binding；
- pause task / replan task / merge task
    -> 都是 Task Manager 的图操作；Scheduler 只消费图更新后的新投影。
```

## 4. 调度原则

```text
1. blocked endpoint 不调度。
2. edge condition 未满足（依赖未完成）的 endpoint 不调度。
3. 高优先级 endpoint 优先（默认顺序见 §8）。
4. verify 优先于新 execute，因为 verify 可以解锁依赖它的 blocked endpoint。
5. latest-main 待复验/合并候选优先。
6. 有 write set 冲突风险的 endpoint 降低并发。
7. budget 不足时减少探索性工作，优先保护 verify 与 merge。
8. capability 不匹配的 agent 不接 Invocation。
9. DeliverySpec / ReportSpec 未规定的 endpoint 不可调度。
10. Worker Capacity 只影响同时运行多少 endpoint，不改变依赖含义。
```

## 5. 用户操作语义

### agent +1

用户点击：

```text
agent +1
```

系统行为：

```text
增加 Worker Capacity（健康 worker 的并发 Invocation 额度）。
Scheduler 依默认优先级选择下一个 runnable endpoint，请求 Runtime 启动 Invocation。
```

这不是给新 agent 手动分配任务，而是增加系统吞吐。

### 提交新需求

用户输入：

```text
"需求：支持 Codex wrapper。"
```

系统行为：

```text
登记 Requirement。
Runtime(role=task_manager) 启动 Task Manager Invocation；
Task Manager 将 Requirement 规整为 Task Contract，创建 Task / Attempt /
Phase Endpoint / edge / blocker，并为每个 endpoint 写入 DeliverySpec 与 ReportSpec；
图更新后 Scheduler 重算 runnable endpoint。
不一定立刻启动 agent。
```

## 6. Budget Model

预算不只是金钱，也包括：

```text
- token
- 时间
- 并发数
- shell 执行成本
- retry 次数
- verify 强度
```

### BudgetPolicy

```go
type BudgetPolicy struct {
	// MaxTokens 限制 prompt + completion token 总量。
	MaxTokens int `json:"max_tokens,omitempty"`
	// MaxCostUSD 限制本次任务或阶段预算。
	MaxCostUSD float64 `json:"max_cost_usd,omitempty"`
	// MaxWallTimeMS 限制墙钟时间。
	MaxWallTimeMS int `json:"max_wall_time_ms,omitempty"`
	// MaxAgentInvocations 限制可启动 Agent Invocation 次数。
	MaxAgentInvocations int `json:"max_agent_invocations,omitempty"`
	// MaxRetries 限制失败重试次数。
	MaxRetries int `json:"max_retries,omitempty"`

	// VerifyLevel 决定验收强度。
	VerifyLevel VerifyLevel `json:"verify_level"`
	// ExplorationLevel 决定探索/检索深度。
	ExplorationLevel ExplorationLevel `json:"exploration_level"`
}
```

用户表达：

```text
"我需要这个功能，投入 5 个 agent，最多跑 30 分钟。"
```

系统转化为：

```text
worker_capacity = 5
wall_time_budget = 30min
scheduler_policy = maximize_verified_tasks
```

## 7. 图变化与重算（替代旧"Replan 触发器"）

Scheduler 不主动 replan。以下事件由 Task Manager 处理后产生图更新；Scheduler 在每次图 revision 更新后重算 runnable endpoint 与优先级：

```text
1. PhaseOutput 提交：Manager 决定继续、创建新 Attempt 或新 Task。
2. OrchestrationProposal：execute 发现 approved plan 依赖的事实错误、
   verify 失败且非局部修复可解决、冲突影响当前 Task 的 write set、
   预算不足需要缩小目标等，由运行中的 phase agent 提交；
   Manager 校验来源 endpoint、图 revision、理由与证据后接受/改写/拒绝，
   接受则热修改图。
3. Context Delta 证明当前编排或计划失效：收到 Delta 的 Agent 提交
   OrchestrationProposal，走第 2 条。
4. 外部输入：新 Requirement、人工决定（decision endpoint）。
```

## 8. 优先级策略（默认）

```text
1. latest-main 上待复验/合并的 candidate。
2. 能解除其他 blocker 的 verify。
3. 已执行待验收的 verify。
4. 已批准且低冲突风险的 execute。
5. 新 Task 的 plan。
6. 探索性工作。
```

## 9. Scheduler 不变量

```text
1. 不调度 blocked endpoint。
2. 不把 Invocation 分配给 capability 不匹配的 agent。
3. 不让 execute 跳过 plan。
4. 不让 merge 跳过 verify。
5. Worker Capacity 只影响吞吐，不改变 Coordination Graph 语义或
   Task Manager 的依赖编排权。
6. 不直接启动任何 agent；只向 Agent Runtime 提交 run request。
7. budget 不足时优先保护 verify 和 merge，而不是继续开新探索。
8. 不解释 OrchestrationProposal、不改图、不启动 Ctx Manager 做普通探索。
```

## 10. AgentTeams 实现映射

AgentTeams 是归档基座，只复用已证实能力。处置分类：直接复用 / 适配封装 / Threadmill 新建 / 不应复用。

| 能力 | AgentTeams 位置 | 处置 |
| --- | --- | --- |
| runnable endpoint 投影 | `third_party/agentteams/copaw/src/copaw_worker/task.py::ready_nodes`（返回依赖已被接受的 pending DAG 节点；项目 paused 时不返回）；MCP 暴露于 `copaw/src/copaw_worker/hooks/tools/projectflow.py`（`action == "ready_nodes"`）；TeamHarness 纯函数版 `plugins/teamharness/mcp/server.py`（`_ready_nodes` / `_ready_loop_nodes`） | 适配封装：依赖已满足才可运行的判定逻辑可复用为 Coordination Graph runnable 投影的参考；但它是 Leader 调用的工具且配合直接委派，不是独立 Scheduler |
| 图写入工具（plan_dag / accept_task_result） | `copaw/src/copaw_worker/task.py::plan_dag`；`plugins/teamharness/mcp/server.py`（projectflow actions，`accept_task_result` 等） | 适配封装：等价于 Task Manager 写图动作的基座参考；Threadmill 中写图入口只能是 Task Manager，不接受 Leader 直接调用 |
| Worker 健康（Ready 条件、生命周期、活动状态） | `agentteams-controller/api/v1beta1/types.go`：`TeamStatus.ReadyWorkers / TotalWorkers / LeaderReady`、成员 `Ready`（镜像 backend Running/Ready）、`Phase / ContainerState / Message`；`agentteams-controller/internal/backend/interface.go`（`WorkerResult.Status` / `Message`，Ready condition False 时填充）；`agentteams-controller/internal/backend/kubernetes.go::podReadyCondition`；`agentteams-controller/internal/controller/auto_sleep_controller.go`（idle → Sleeping） | 直接复用：作为 Worker Capacity 的健康输入（可用 worker 数）。注意它度量 worker 实例健康与资源规格（`WorkerSpec.Resources`），不是 Invocation 并发容量 |
| Worker Capacity（并发 Invocation 额度）与记账 | 不存在；基座中 Leader 靠 `agentteams-controller/internal/agentconfig/coordination.go` 的提示（"Use team-state.json as the source of truth for task activity before deciding whether a worker is idle"）人工判断空闲 | Threadmill 新建：Capacity = 可并发的 Agent Invocation 数，由 Scheduler 记账 |
| Scheduler 本体（选择 + 优先级 + 预算） | 不存在；基座中 Leader 兼任写图（plan_dag / accept_task_result）、选节点（ready_nodes）、直接启动（delegate_task：`copaw/src/copaw_worker/task.py::delegate_task` 与 `hooks/tools/taskflow.py`） | Threadmill 新建：拆分为 Task Manager（写图）→ Scheduler（只读选择）→ Agent Runtime（启动 Invocation） |
| OrchestrationProposal 协议与 Manager 热修改后重算 | 不存在；基座是 Leader 拒绝/接受结果后手动 plan_dag 再 ready_nodes | Threadmill 新建 |
| Budget Model | 不存在 | Threadmill 新建 |

不应复用：Leader 亲自调度模式（Leader 兼任写图、选节点与直接启动）、Matrix mailbox 式通信。

基座不提供的边界（不要假设存在）：Event Log、Context Graph、git worktree 服务、Merge Queue、Scheduler、Agent mailbox / AgentMessage 推送通道。AgentTeams 的 Matrix room 通知只是通信通道，不等于 mailbox 协议。
