# Task Manager Agent 定义与工具

版本：v0.1
状态：Draft
定位：本文只定义 Task Manager Agent、裁决协议及其所需工具。Coordination Graph 对象与接口以 [coordination-graph.md](./coordination-graph.md) 为准；Phase Agent 的 Requirement、OrchestrationProposal 和 PhaseOutput 以 [phase-agent.md](./phase-agent.md) 为准；Context 读写 seams 以 [context-graph.md](./context-graph.md) 为准。

---

## 1. 定义

**Task Manager Agent** 是 Threadmill 中负责把结构化输入裁决为 Coordination Graph 变更的 Agent，也是 `TaskManagerGraph` 的唯一调用者。

Task Manager Agent 可以：

1. 将 Requirement 规整为 Task Contract，并创建固定的 `plan / execute / verify` endpoint、入边、blocker、DeliverySpec 和 ReportSpec；
2. 读取 completed PhaseOutput、报告、交付物和 evidence，裁决 endpoint、Blocker 与 Task 的封闭状态转换；
3. 审批 OrchestrationProposal，在接受或改写后替换尚未执行的子图，并明确运行中 endpoint 的 hold/stop/release 顺序；
4. 根据 Verify、Merge、Human Decision 或 ContextDelta 证据失效旧结果、重开 generation 或结束 Task；
5. 经 Context Service 注册 task 子图、投影已生效的 directive/fact，并在权威 `done` 后触发 Task Memory 终审；
6. 与 Phase Agent 使用相同的 Context 列表、探索、订阅和自然语言检索路径。

Task Manager Agent 是临时 Agent Invocation，不拥有图存储、Scheduler 或 Runtime 状态。Runtime 每次只派发一个已持久化 `inputRef`，并注入身份、capability、预算、可见 evidence、Context Slice 和当前 decision scope。Task Manager 的跨 Invocation 连续性来自 Coordination Graph、DecisionRef、Event Log 和 Context Graph，不来自模型会话。

Task Manager Agent 不能：

- 调用 `PhaseController`，或直接 start/stop/resume Phase Agent；暂停只能写 `held`，后续由内部 `GraphRuntime` 执行 stop；
- 读取或修改 Scheduler、phase lease、PhaseCommand 日志、Invocation、worker 或 GraphRuntime 内部状态；
- 读取 phase transcript、未提交工具输出、未提交 Workspace 现场或 Matrix 聊天文本；
- 选择实现方案、修改 Workspace、运行 merge 或写 main；
- 使用对象级 Task/Endpoint/Edge/Blocker CRUD、任意字段 patch 或跳过 graph revision；
- CRUD general Context 对象、读取 Task Memory Buffer 语义内容，或绕过 Context Service 写 task 子图；
- 创建第四种 phase，或把 Runtime 观察直接当作图状态。

---

## 2. 工具包装

Task Manager 使用的 Graph 对象、请求/响应字段和校验规则由权威 Module 文档定义。本文不复制 DTO；MCP/Runtime Adapter 只把这些 seams 包装成 Task Manager 可调用工具。

| Agent 工具 | 权威 seam | 包装职责 |
| --- | --- | --- |
| `coordination.snapshot` | `TaskManagerGraph.Snapshot` | 原样返回 revision-consistent `GraphSnapshot` |
| `coordination.replacePending` | `TaskManagerGraph.ReplacePending` | 原样转发 `PendingSubgraph`；Runtime 注入当前 DecisionRef 作为 RequestID |
| `coordination.transition` | `TaskManagerGraph.Transition` | 把当前 DecisionRef 作为 transitionRef 转发，不接受任意 Runtime Event |
| `taskManager.submitDecision` | Agent Runtime decision capture | 持久化 `TaskManagerDecision`，返回本次图写可使用的 DecisionRef |
| `context.listSubgraphs` | `ContextGraphReader.ListSubgraphs` | 与 Phase Agent 相同的可见子图列表 |
| `context.explore` | `ContextGraphReader.Explore` | 与 Phase Agent 相同的受控图探索 |
| `context.subscribe` | `ContextGraphReader.Subscribe` | 建立绑定当前 Task Manager Invocation 的订阅 |
| `contextAgent.retrieve` | `ContextAgent.Retrieve` | 列表/探索不足时请求 Context Agent 做自然语言语义检索 |
| `context.registerTaskSubgraph` | `TaskContextWriter.RegisterTaskSubgraph` | Task 创建成功后显式注册唯一 task 子图绑定 |
| `context.projectTaskContext` | `TaskContextWriter.ProjectTaskContext` | 投影已写入权威来源的 directive/fact，不复制易变运行状态 |
| `context.finalizeTaskMemory` | `TaskMemoryFinalizer.FinalizeTaskMemory` | Task 权威 done 后冻结并触发同一候选批次终审 |

包装层必须保持薄：

- 不重命名 Graph 字段、不补业务默认值、不把多次图 mutation 合并成隐藏工作流；
- Runtime 只附加可信身份、inputRef、DecisionRef、capability 和调用审计；这些值不允许 Agent 自报；
- `taskManager.submitDecision` 是结构化业务输出，不是 Agent 直写 Event Log；Runtime 自动捕获提交、图写结果和最终 disposition；
- 每次 Coordination Graph 写入前必须先提交一份 decision；同一 DecisionRef 最多关联一次 `ReplacePending` 或一次 `Transition`，不能同时驱动两种 mutation；
- revision 冲突、scope 校验失败或 transition 拒绝原样返回。Agent 必须重新 `Snapshot` 并形成新 decision，不得自动套用旧决定；
- Coordination Graph 提交与 task Context 投影不是跨服务事务：先写权威图，再显式注册/投影；投影失败只重试投影，不回滚已生效图决定；
- Runtime 只注入 Task Manager 可读的结构化边界输入及其 artifact/evidence 引用，不提供原始 Event Log 查询、transcript 浏览或任意 Artifact Store 枚举。

### 2.1 工具选择

- 读取图：`coordination.snapshot`；
- 新建 Task、调整尚未执行子图、接受拆分/重排建议：`taskManager.submitDecision` → `coordination.replacePending`；
- submitted/satisfied/rejected/reopened/held/released/stopped、Blocker 和 Task 终态：`taskManager.submitDecision` → `coordination.transition`；
- Context 普通读：`context.listSubgraphs`、`context.explore`、`context.subscribe`；
- 自然语言语义检索：`contextAgent.retrieve`；Task Manager 不持有 `ContextGraphSearcher.Search`；
- Task 定向投影：`context.registerTaskSubgraph`、`context.projectTaskContext`；
- done 后候选终审：`context.finalizeTaskMemory`。

Task Manager 可以组合多次只读调用形成一次调查，但每次 graph mutation 都必须有独立 DecisionRef 和明确 base revision。运行中的 endpoint 必须先走 `held → stopped`，待新 generation 无活动 lease 后才能进入 `ReplacePending` scope；Agent 不通过工具组合模拟直接 stop。

---

## 3. 输入与裁决协议

Task Manager 不轮询系统。Requirement、OrchestrationProposal、PhaseOutput、PhaseInvocationFailed/Stopped、Verify、Merge、Human Decision 和 ContextDelta 都先持久化为结构化边界输入，再由 Runtime 以稳定 `inputRef` 派发：

```go
// Runtime 派发回调；其他 Agent 不直接调用。
type TaskManagerAgent interface {
    Handle(ctx context.Context, inputRef string) error
}

// Task Manager 唯一新增的语义对象；不是 Graph 节点，也不复制 Graph mutation 载荷。
type TaskManagerDecision struct {
    Action       string   `json:"action"`                  // replace_pending | Coordination Graph 允许的 transition | reject | defer | no_change
    TargetRef    string   `json:"target_ref,omitempty"`    // transition 必填；replace_pending/reject/defer/no_change 省略
    EvidenceRefs []string `json:"evidence_refs,omitempty"` // 只能引用当前可见的权威边界证据
    Reason       string   `json:"reason"`                  // 必填，说明为何作出该决定
}

// Runtime 工具：持久化 decision 并返回 Graph 写入使用的稳定引用。
func SubmitTaskManagerDecision(decision TaskManagerDecision) (decisionRef string, err error)
```

Runtime 自动把 DecisionRef 绑定到当前 `inputRef`、Task Manager 身份、Invocation、graph mutation 使用的 expected revision 和实际 mutation 结果，Agent 不填写这些字段。`TaskManagerDecision` 只表达语义决定：`PendingSubgraph`、GraphSnapshot、Transition evidence、Requirement 和 OrchestrationProposal 继续使用各自权威定义，不嵌进 decision。`TargetRef` 必须解析为当前 GraphSnapshot 中恰好一个 Task、Phase Endpoint 或 Blocker；未知、自由文本或跨 revision target 整体拒绝。

裁决规则：

1. `replace_pending`：提交 decision 后，把其 DecisionRef 作为 `PendingSubgraph.RequestID`；graph mutation 成功后才能注册/更新 task Context 投影。
2. Coordination Graph transition：提交 decision 后，把 DecisionRef 作为 `transitionRef`；Graph Module 从已持久化 decision 校验 target、action、evidence 和控制顺序。
3. `reject | defer | no_change`：只持久化 decision，不调用 Graph 写接口；Runtime 将结果路由回原输入方。
4. mutation 返回 revision/scope/transition 冲突时，该 DecisionRef 保留为“未应用”审计事实；Task Manager 读取新快照后必须提交新 decision，不能修改或复用旧 ref。
5. 重复派发同一 inputRef 时，Runtime 先恢复其 DecisionRef 与 mutation 结果；已应用或已拒绝的输入不重复裁决，未完成的同一 mutation 只按原 RequestID/transitionRef 重试。

### 3.1 核心流程

```text
# 新 Requirement
inputRef
  -> coordination.snapshot
  -> submitDecision(replace_pending)
  -> coordination.replacePending（一次提交完整 Task + plan/execute/verify）
  -> context.registerTaskSubgraph
  -> context.projectTaskContext（Requirement 与 Contract directive）

# Proposal、ContextDelta 或运行中重排
inputRef
  -> snapshot + completed boundary evidence
  -> 若 endpoint 在运行：transition(held)
  -> 等待新的 PhaseInvocationStopped inputRef
  -> transition(stopped) -> replacePending/reopened -> transition(released)
  -> GraphRuntime 自行选择 start 或 resume

# PhaseOutput / Verify / Merge
inputRef
  -> snapshot + report/delivery/evidence
  -> submitDecision + transition
  -> 接受的结果投影为 task fact
  -> DeliveryPolicy 与 merge evidence 均满足后才 transition(done)
  -> done 成功后 finalizeTaskMemory
```

`submitted`、`satisfied` 与 `done` 必须是三个独立裁决。Task Manager 不能因收到 PhaseOutput 就跳过 verifier/DeliveryPolicy，也不能因 Runtime started/failed/stopped 观察直接宣布业务完成。

### 3.2 失败与恢复

- **Runtime 或 Agent 崩溃**：重新派发同一 inputRef；DecisionRef、graph request ID 和已生效 revision 从持久记录恢复，不依赖模型会话。
- **并发图修改**：revision conflict 后重新 Snapshot、重新评估、提交新 DecisionRef；禁止盲重试旧 mutation。
- **Graph 成功、Context 投影失败**：Graph 保持权威，重试相同 ProjectionID/SourceRevision；不撤销 Task 或 endpoint。
- **done 成功、候选终审失败**：Task 保持 done，`FinalizeTaskMemory` 对同一冻结批次幂等重试。
- **证据不足或来源过期**：使用 `defer` 或 `reject`，不能从 transcript、聊天文本或模型记忆补写事实。
- **权限或 capability 失效**：停止本次 Invocation；Adapter 不缓存可脱离 Task Manager 身份复用的图写凭据。

---

## 4. 工具清单

Task Manager Agent 需要以下工具：

```text
coordination.snapshot
coordination.replacePending
coordination.transition
taskManager.submitDecision
context.listSubgraphs
context.explore
context.subscribe
contextAgent.retrieve
context.registerTaskSubgraph
context.projectTaskContext
context.finalizeTaskMemory
```

Task Manager 不持有 PhaseController、GraphRuntime、Scheduler、Workspace、Merge Queue、ContextGraphSearcher、ContextGraphCurator、TaskMemoryBufferReader、原始 Event Log 或图存储工具。所有写入都经权威 Module seam、capability、revision、DecisionRef 和审计边界执行。
