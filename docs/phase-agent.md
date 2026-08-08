# Phase Agent Interface

> 状态：Draft
> 定位：定义 planner、executor、verifier 在 AgentTeams 宿主中使用的 Agent-facing interface。它以 Runtime 注入的 MCP 工具与回调表达，不预设 HTTP、gRPC 或最终 Go 类型。
> 语义权威：[统一设计](./threadmill-unified-design.md)。本文与统一设计冲突时，以统一设计为准。

## 1. 设计目标

Phase Agent 是一次受控 Invocation 内的计算者，不是编排者。它只需理解一个深 interface：

```text
# Runtime 注入契约、工作区、上下文和正式输入
接收当前 endpoint 契约、Workspace、Context 与输入集
  -> 执行当前可完成的工作
  -> 等待已声明但尚未交付的并行输入，或提出新的编排意图
  -> 汇总正式交付，提交阶段输出
```

这个 interface 向 Agent 隐藏 Coordination Graph 的拓扑、调度、边状态、worker 生命周期、Artifact Store 和上游 Invocation 的中间现场。其目标是：

- **Leverage**：同一输入、等待和提交 interface 覆盖 plan、execute、verify 与不同 AgentTeams 承载方式。
- **Locality**：输入满足、等待恢复、交付新鲜度和 worker 释放全部由 Runtime 一处实现。
- **可测试性**：Agent 行为只依赖启动输入、Context 回调和四个出站调用；宿主可用替身 Runtime 验证，不必模拟整张协调图。

## 2. 权限与责任

| 决定 | Owner | Phase Agent 可见结果 |
| --- | --- | --- |
| endpoint 是否可启动、入边和交付规定 | Task Manager + Coordination Graph | `StartPhaseInput.inputs` |
| 何时派发和恢复 Invocation | Scheduler + Runtime | `runtime.startPhase`、`runtime.onInputsChanged` |
| 输入已到达、失效或无法到达 | Runtime | `PhaseInputSet` 与 `runtime.awaitInputs` 结果 |
| 当前工作如何完成、何时等待 | Phase Agent | 输出、等待调用或编排建议 |
| 是否新增依赖、Task、endpoint 或更改图 | Task Manager | Proposal 裁决结果 |
| endpoint 是否满足、Task 是否 done | 授权方与 DeliveryPolicy | PhaseOutput 后续状态 |

Phase Agent 没有 Coordination Graph、Context Graph、main、mailbox 或 phase 跳转的写 interface。AgentTeams worker 或临时 agent 只是 Invocation 的执行宿主，不是编排者。

## 3. 启动 Interface

### 3.1 `runtime.startPhase`（Runtime 回调）

Scheduler 选中 runnable Phase Endpoint 后，Runtime 调用 Agent。Agent 不调用此接口。

```ts
interface StartPhaseInput {
  invocationId: string; // 当前受控 Invocation 的唯一标识
  endpointRef: string; // 被调度的 Phase Endpoint
  taskId: string; // 持久 Task 身份
  phase: "plan" | "execute" | "verify"; // 当前执行阶段
  contractRef: string; // Task Contract、DeliverySpec 和 ReportSpec
  workspaceRef: string; // 当前轮次共享的 Workspace Binding
  contextSliceRef: string; // Runtime 装配的初始上下文切片
  inputs: PhaseInputSet; // 入边交付的只读投影
}

interface PhaseInputSet {
  inputRevision: string; // 输入集合和新鲜度的版本
  required: InputRequirement[]; // 当前 endpoint 声明需要的全部输入
  delivered: InputDelivery[]; // 已到达的正式交付
  pending: PendingInput[]; // 尚未到达的 completion 输入
}

interface InputRequirement {
  inputId: string; // 输入要求的稳定标识
  fromEndpoint: string; // 负责交付的上游 endpoint
  requiredArtifacts: string[]; // 该输入必须包含的 artifact 类型或引用
  requiredBy: "start" | "completion"; // 启动前满足，或最终提交前满足
}

interface InputDelivery {
  inputId: string; // 对应的输入要求
  fromEndpoint: string; // 实际交付的上游 endpoint
  phaseOutputRef: string; // 上游正式 PhaseOutput 引用
  artifactRefs: string[]; // 可消费的正式 artifact 引用
  sourceRevision: string; // 上游交付所基于的 revision
}

interface PendingInput {
  inputId: string; // 尚未到达的输入要求
  fromEndpoint: string; // 预计交付的上游 endpoint
  requiredBy: "completion"; // 仅允许并行等待，不能缺失提交
}
```

`inputs` 是当前 endpoint 已声明入边的只读投影：它明确告诉 Agent 谁应交付什么、哪些正式交付已经到达、哪些并行输入仍待到达。Agent 不读取或推断 Coordination Graph。

- `requiredBy: "start"`：所有此类输入到达后，endpoint 才 runnable；它们不会出现在已启动 Invocation 的 `pending` 中。
- `requiredBy: "completion"`：source 和 target 可以并行启动；target 的最终 PhaseOutput 提交前必须取得对应交付。
- `phaseOutputRef` 与 `artifactRefs` 只引用上游正式边界输出，不暴露其过程上下文或工作目录。
- `inputRevision` 随 delivered/pending 集合、输入 artifact 或新鲜度变化而变。Runtime 将它绑定到最终输出，防止旧输入静默复用。
- `contractRef` 指向 Task Contract 及当前 endpoint 的 DeliverySpec、ReportSpec。缺少两者之一，endpoint 不可调度。
- `workspaceRef` 标识同一轮次共享的 Workspace Binding；`contextSliceRef` 标识 Runtime 注入的初始 Context Slice。

Runtime 还在宿主侧强制 token/时间预算、工具白名单、可写目录和 phase lease；这些是实现细节，不进入 Agent 的公共 interface。

### 3.2 `runtime.stopPhase`（Runtime 回调）

Runtime 可因取消、lease 失效、预算耗尽、输入终止或 Task Manager 裁决终止当前 Invocation。

```ts
interface StopPhaseInput {
  invocationId: string; // 要终止的 Invocation
  reason: string; // 取消、lease 失效或裁决原因
}
```

Agent 收到后停止写入。已产生的受控产物仅能通过正常输出或 evidence 载体被引用；Agent 不自行推进 `plan -> execute -> verify`，也不宣布 Task 完成。

## 4. 输入 Join Interface

### 4.1 `runtime.awaitInputs`

Agent 完成当前可执行工作、且只缺少启动输入中已声明的 completion 输入时，调用：

```ts
// Agent 主动等待已经声明的 completion 输入
runtime.awaitInputs({
  inputIds?: string[]; // 省略表示等待全部 pending 输入
}) -> InputWaitResult

interface InputWaitResult {
  inputRevision: string; // 恢复时使用的最新输入版本
  delivered: InputDelivery[]; // 本次等待期间新增或确认的交付
  pending: PendingInput[]; // 仍未到达的输入
  terminalReason?: "source_failed" | "source_cancelled" | "input_stale" | "lease_expired" | "deadline_exceeded"; // 无法继续等待时的原因
}
```

这是一个小而深的 join interface，不是编排建议：

- 省略 `inputIds` 表示等待全部 `pending` completion 输入；指定时只能使用当前 `PhaseInputSet` 中已有的 `inputId`。
- Runtime 根据既有 Coordination Edge 监听 source endpoint 的正式 `PhaseOutput`。新交付到达后，返回最新 `InputWaitResult`；Agent 汇总新旧交付后继续执行，必要时可再次等待。
- 逻辑 Invocation 可以处于 waiting，但 Runtime 必须释放模型调用、线程和 worker capacity。恢复时可创建新的 AgentTeams 执行调用，重新注入当前 Workspace、Context 和最新输入；不要求模型进程或 worker 长驻。
- `terminalReason` 表示已声明输入不能正常到达。Agent 可基于已有输入继续、提交编排建议，或将缺口写入最终报告；不得伪造已收交付。
- `agent.submitPhaseOutput` 会拒绝仍缺少 completion 输入的最终输出。

这满足 Agent 的自主性：Agent 自己决定何时已把本体工作做到不能继续，自己发起等待并在输入到达后汇总。控制面不干预每一次等待；它只持续拥有依赖事实、输入有效性和资源调度，避免 Agent 持有无法审计的长连接或轮询消息。

### 4.2 发现新前置

`runtime.awaitInputs` 不能创造依赖。若 Agent 发现当前输入集未声明、但确实需要的新前置，必须调用 `agent.proposeOrchestration`：

```text
# 只有未知前置才进入编排面；已知输入直接等待
未知缺口
  -> agent.proposeOrchestration(advice: "dependency")
  -> Task Manager 审批、改写或拒绝
  -> 接受时更新 Coordination Graph 与目标 endpoint 输入契约
  -> Scheduler/Runtime 按新图重新调度
```

## 5. Context Interface

Runtime 通过 `threadmill-ctx` MCP server 向 AgentTeams 宿主注入以下只读工具。它们共享当前 Invocation、角色、权限快照、预算与 Context Graph revision，不改变 `PhaseInputSet`。

| 工具 | 用途 | 结果 |
| --- | --- | --- |
| `context.listSubgraphs(filter?)` | 列出可见 Context Subgraph | `SubgraphSummary[]` |
| `context.explore({ anchor, depth?, tokenBudget? })` | 从当前切片、frontier 或子图渐进展开 | `ContextSliceDelta` |
| `context.retrieve({ intent, scope, reasoningAnchor })` | 请求 Ctx Manager 语义检索 | `RetrieveResult` |
| `context.subscribe({ subgraphIds, eventKinds? })` | 订阅可见子图后续更新 | `Subscription` |

`context.retrieve` 是唯一需要 Ctx Manager 判断的读调用。成功检索的子图自动订阅；订阅绑定当前逻辑 Invocation，并在其结束时过期。普通探索、检索和 Delta 不是 Task 依赖交付，不能替代 `PhaseInputSet`。

### 5.1 `runtime.onContextDelta`（Runtime 回调）

```ts
interface ContextDelta {
  subscriptionId: string; // 当前 Context 订阅
  subgraphId: string; // 发生变化的子图
  revision: number; // Context Graph revision
  changes: unknown[]; // 可合并、可重放的增量内容
}
```

订阅执行器在 Context Graph revision 提交后匹配订阅，再由 Runtime 推送 Delta。Delta 必须可合并、可重放，且没有订阅外旁路推送。若 Delta 证明计划或编排失效，Agent 使用 `agent.proposeOrchestration`，不直接改图。

### 5.2 `runtime.onInputsChanged`（Runtime 回调）

Runtime 在 Agent 未调用 `runtime.awaitInputs` 的正常运行期间，也可以在已声明 completion 输入到达或失效时更新可见输入：

```ts
interface InputsChanged {
  inputs: PhaseInputSet; // 正式输入交付变化后的完整投影
}
```

这是与 `ContextDelta` 分离的正式交付通道。Agent 应以新的 `inputRevision` 重新评估当前工作；它不表示 Agent 可以读取 source 的过程现场。

## 6. 出站 Interface

所有出站调用都经 Runtime 校验、记录并路由。Agent 不直接访问 Task Manager、Ctx Manager 或 Artifact Store 的内部写 interface。

### 6.1 `agent.submitPhaseOutput`

阶段完成且所有 completion 输入到达时调用。

```ts
interface PhaseOutput {
  phase: "plan" | "execute" | "verify"; // 产生输出的阶段
  deliveryRefs: string[]; // 满足 DeliverySpec 的交付物
  reportRef: string; // 满足 ReportSpec 的报告
  evidenceRefs: string[]; // 支撑交付和判断的证据
}

// 提交不等于通过；后续状态由授权方判定
agent.submitPhaseOutput(output: PhaseOutput) -> Accepted
```

- Runtime 将输出绑定到当前 Task Contract、endpoint、Input Revision、Context Slice 和 Workspace 轮次；Agent 不填写或改写这些绑定字段。
- `deliveryRefs`、`reportRef`、`evidenceRefs` 必须满足当前 endpoint 的 DeliverySpec 和 ReportSpec。
- Runtime 校验所有 required completion input 已交付且 source revision 仍满足约束。
- 接受提交不等于通过；批准、拒绝、失效和 Task done 仍由授权方决定。

### 6.2 `agent.proposeOrchestration`

运行中发现新前置、需要拆分、重排、重试或计划失效时调用。

```ts
interface OrchestrationProposal {
  proposalId: string; // 幂等转交和裁决的标识
  advice: "split" | "dependency" | "replan" | "retry" | "serial_parallel"; // 编排意图
  rationale: string; // 为什么需要调整
  evidenceRefs: string[]; // 已注册的支撑证据
}

// 这是意图，不是 Coordination Graph 命令
agent.proposeOrchestration(proposal: OrchestrationProposal) -> Accepted
```

Proposal 是意图，不是图命令。Runtime 对重复 `proposalId` 只转交一次；Task Manager 检查 graph revision、输入 revision 与证据后接受、改写或拒绝。Proposal 不结束当前 phase，除非裁决明确取消；已知输入等待仍由 `runtime.awaitInputs` 自主处理。

### 6.3 `agent.submitRequirement`

发现具有独立交付与验收的新工作时调用：

```ts
interface Requirement {
  text: string; // 新需求的原始描述
  goal?: string; // 可选目标
  constraints?: string[]; // 可选约束
  evidenceRefs?: string[]; // 可选来源证据
}

// Requirement 交给 Task Manager 规整，不能直接调度
agent.submitRequirement(requirement: Requirement) -> Accepted
```

Runtime 记录来源为当前 phase，Task Manager 将其规整为 Task Contract。Requirement 本身不可调度，不能用于修改当前 endpoint 的既有验收或输入契约。

### 6.4 `agent.submitMemoryCandidate`

标注可复用知识时调用：

```ts
interface MemoryCandidate {
  statement: string; // 一句话知识陈述
  kind: string; // fact、decision、constraint 等候选类型
  sourceRefs: string[]; // 必须能追溯到证据
  whyReusable: string; // 为什么值得跨 Invocation 复用
}

// 这是记忆候选，不是 Context Node
agent.submitMemoryCandidate(candidate: MemoryCandidate) -> Accepted
```

这是候选，不是 Context Node。Runtime 记录候选；Ctx Manager 负责 evidence、权限与价值判断，以及 create/revise/supersede/dispute/reject。`sourceRefs` 缺失时默认拒绝。

### 6.5 Artifact / Evidence 引用

Agent 在当前 Task 的受控目录产出文件，并在 `result.md` 或等价载体中填写路径引用。Runtime 做以下转换：

```text
# 受控路径只在 Runtime 校验后变成跨模块引用
受控路径
  -> Runtime 路径校验
  -> Artifact Store 注册
  -> 跨模块使用不透明 artifact ID
```

最终 Artifact ID、哈希、去重和存储布局由人类设计。Agent 不创建 `ArtifactRef`，也不直接写对象存储。

## 7. 不提供的 Interface

Phase Agent 不提供：

- Coordination Graph 或 Context Graph 的写 interface；
- main 写入、合并、done 判定；
- mailbox、Agent 间消息、订阅外推送；
- phase 跳转、lease、Workspace 或 worker 生命周期管理；
- 读取未提交的其他 Agent 过程上下文；
- `Attempt`、`Split`、`Failure`、`Rework`、`Execution Graph` 等额外实体 interface。

## 8. 人类待决项

本文故意不冻结：

- MCP、HTTP、gRPC 或本地进程等传输协议；
- `requiredArtifacts`、检索请求/响应、`ContextDelta.changes` 的完整字段；
- `InputWaitResult` 的持久化、恢复令牌、截止时间与错误码；
- Artifact ID、内容哈希、存储与去重策略；
- 预算、重试、超时和可观测性字段；
- DeliverySpec、ReportSpec、WriteSet 的完整结构。

### 8.1 Phase Agent 内部配置（待进一步研究）

Phase Agent 的上下文管理与运行内部配置需要结合具体宿主能力进一步研究，本文不冻结：

- **上下文装配**：ContextSlice 的构建、seed subgraph 选择、frontier 预算、按 role/purpose 的注入重排与正文注入量；
- **订阅生命周期**：订阅绑定逻辑 Invocation、过期与清理时机、Delta 合并与重放策略；
- **等待恢复**：`InputWaitResult` 的持久化、恢复令牌、截止时间与重试策略；
- **预算与门控**：token/时间预算、工具白名单、重试与超时参数。

### 8.2 AgentTeams 集成（待进一步研究）

AgentTeams 承载的具体集成细节需要结合实际能力与部署形态调研后定案，本文不冻结：

- taskflow worker 包装链的具体时序与状态映射（`delegate_task` / `ack_task` / `submit_task` / `check_task`）；
- workerflow 临时 agent 的创建、上下文注入与清理时机；
- MCP allow policy 的具体工具清单、AccessEntries / AllowedDirs 的映射规则；
- `result.md` 载荷格式与 PhaseOutput / 报告引用的映射；
- 角色门控的动作黑名单（`plan_dag`、`accept_task_result` 等）与 Matrix 人工通道的具体接入。

这些主题不改变 interface 的三面边界与控制权，但实现前必须完成专项调研。

### 8.3 AgentTeams 适配与强制控制（待定）

AgentTeams 承载方式与控制保障同样未定案，本文不冻结：

```text
# 图决定可运行性，Runtime 承载调用，Agent 只提交结构化结果
Coordination Graph
  -> Scheduler selects runnable endpoint
  -> Runtime assembles input set and controlled invocation
  -> AgentTeams taskflow worker or workerflow temporary agent
  -> agent waits, proposes, or submits structured output
  -> Runtime validates and routes it
```

Runtime 可将 Invocation 适配为两种 AgentTeams 载体：

- 持久 worker：`delegate_task -> ack_task -> submit_task -> check_task`。
- 临时 agent：`workflow_run` 创建独立 agent，结束后清理。

承载方式不改变 interface 或控制权；下表只是设计方向，具体落点与保证待研究：

| 控制层 | AgentTeams 落点 | 保证 |
| --- | --- | --- |
| 角色门控 | taskflow 权限与 Runtime 适配器 | 无 `plan_dag`、`accept_task_result` 等写图动作 |
| 工具门控 | MCP allow policy | 各 phase 只看到允许工具 |
| 路径门控 | AccessEntries + AllowedDirs | execute 只能改批准目录；plan/verify 不改实现 |
| lease 门控 | Runtime + Workspace Service | 同一轮次任一时刻只有一个有效写 lease |
| 输入门控 | `PhaseInputSet` + Runtime 校验 | 上游只以正式交付引用进入；未满足输入不可完成 |
| 通信门控 | worker 默认禁用 message 工具 | 无 Agent mailbox；Matrix 只用于人工可见性和最终报告 |

### 8.4 阶段权限（待定）

阶段可写范围与产物基线待定，本文不冻结：

| 阶段 | 可写 | 禁止 | 主要产物 |
| --- | --- | --- | --- |
| plan | 结构化计划产物 | 修改实现、改写 Task Contract、写 Coordination Graph | Submitted Plan、Declared Write Set、验证计划、Requirement |
| execute | Approved Plan 与 AllowedDirs 范围内实现 | 静默扩 scope、写 main、宣布完成 | diff/候选产物、证据、MemoryCandidate |
| verify | evidence 产物；可运行检查 | 修改实现以让自己通过、自我批准旧 revision | Verify Result、测试/检查证据、风险与缺口 |

同一轮次的三个阶段共享 Workspace Binding。`verify passed` 仅代表获得后续 Merge Queue 资格；`done` 仍由 Task Manager 和 DeliveryPolicy 决定。

这些实现选择不改变 interface 的核心不变量：输入由图声明、正式交付经 Runtime 注入、Agent 可以自主 join 已知输入、未知依赖只能提案、Task 完成不由 Agent 宣布。

## 9. 参考文档

- [统一设计](./threadmill-unified-design.md)：两张图、输入边、三阶段、订阅与 Runtime 的语义权威。
- [Agent Runtime](./agent-runtime.md)：AgentTeams 的 taskflow / workerflow 映射、lease、MCP 注入与 result.md 载体。
- [Task Manager Agent](./task-manager-agent.md)：唯一写图、Requirement 与 Proposal 裁决。
- [Workspace 与 Merge Queue](./workspace-merge.md)：Workspace Binding、路径和 WriteSet 边界。
- [Event Log 与 Artifact Store](./event-artifact-store.md)：artifact 注册与证据链。
- [总体架构](./architecture.md)：五节点与控制链。
