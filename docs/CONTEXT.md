# Threadmill 领域语言

Threadmill 协调软件工作，但不把工作绑定在执行它的 agent session 上。仓库中的设计文档、接口和 Skill 应统一使用下面这些词。语义以 docs/threadmill-unified-design.md 为准；本文是其词汇表。AgentTeams（third_party/agentteams）是归档基座，其术语只出现在实现映射中，不进入领域语言。

## 工作

**Requirement（需求）**:
对目标、动机和约束的原始表达。Requirement 用来保留来源，不是可直接调度的工作。
_Avoid_: Request、prompt、ticket

**Task Contract（任务契约）**:
一个工作单元的稳定约定，包括要改变什么、为什么、允许的边界和验收条件。
_Avoid_: Task prompt、plan

**Task（任务）**:
由一个 Task Contract 约束、通过 phase endpoint 协调的持久工作单元。Task 的寿命长于任何参与执行的 agent。
_Avoid_: Agent、session、thread

**轮次（Round）**:
对同一个 Task Contract 的一次有界尝试，由该轮次的 Workspace Binding 标识，不创建独立持久实体。验证失败后由 Task Manager 失效旧输出、重开 execute→verify 轮次，而不是创建新的 task。
_Avoid_: Retry task、Task Attempt

**Phase Endpoint（阶段端点）**:
Task 生命周期中的命名协调点，其他工作可以向它提供输入或依赖它产生的结果；需要人工或外部决定的端点即 Decision Endpoint。
_Avoid_: Status flag、agent state

**Coordination Graph（协调图）**:
由 task、phase endpoint、edge（condition、data、on_false、freshness）、blocker 和 decision 构成的持久图，记录尚未履行的因果义务。Task Manager Agent 是唯一写入口；可热修改；Scheduler 只读。
_Avoid_: Workflow diagram、agent chat graph

## 编排协议

**PhaseOutput（阶段输出）**:
阶段结束时 endpoint 提交的结构化输出载荷，含交付物/报告/证据引用、Workspace revision 与 Context Graph revision；是进入 Task Manager 视野的两类结构化输出之一。
_Avoid_: Agent summary、task result dump

**OrchestrationProposal（编排建议）**:
运行中的 phase agent 主动提交的结构化建议（拆分、缺前置、串/并调整、replan 等），含理由与证据引用。它是自由文本意图，不是图命令；Task Manager 审批后热修改 Coordination Graph。拆分、失败、计划失效都使用同一种建议协议。
_Avoid_: Split Request、Failure Request、Rework Task

**DeliverySpec（交付规定） / ReportSpec（报告规定）**:
Task Manager 为每个 Phase Endpoint 规定的交付物与报告要求；未规定二者的 endpoint 不可调度。
_Avoid_: Job spec、prompt

**blocker（阻塞）**:
指向具体 endpoint 的权威阻塞声明（如 `A.execute blocked by B.verify`），含解除条件与失败策略。"Task blocked" 只是投影。
_Avoid_: Task status、busy flag

## 运行

**Agent Invocation（Agent 调用）**:
在明确角色、阶段、工作区、上下文切片、权限和预算下对 agent 的一次有界使用。它是可替换的计算资源，不是持久项目身份。
_Avoid_: Agent task

**Thread（会话线程）**:
某个 provider 为一次 Agent Invocation 保留的局部对话状态。丢弃 Thread 不应丢失 Task 或已经接受的项目事实。
_Avoid_: Task、project memory

**Worker Capacity（工作容量）**:
Scheduler 当前可以并发使用的 Agent Invocation 数量。容量只改变吞吐，不改变 Coordination Graph 的含义。
_Avoid_: Agent assignment

**Agent Runtime（Agent 运行时）**:
所有 Agent Invocation 的统一执行边界：启动/取消、phase 权限与写 lease、Workspace 装配、事件记录、输出形状校验。不判断业务完成，不解释编排建议，不写任一图，不合并 main。
_Avoid_: Orchestrator、Task runner

**Workspace Binding（工作区绑定）**:
一个 Task 轮次的可变执行现场（git worktree、独立目录、容器等实现形式可替换）；同一轮次的 plan/execute/verify 共享，轮次间隔离。
_Avoid_: Sandbox、checkout

**Merge Queue（合并队列）**:
main 的唯一写入口；对 verify passed 的候选做 latest-main 机械应用检查、targeted verify 与串行合入。不修冲突、不写 Coordination/Context Graph。
_Avoid_: PR bot、auto-merge tool

## 证据和上下文

**Evidence（证据）**:
用于判断某项主张的可观察结果，例如 diff、测试结果、tool output 或人工决定，并且可以追溯来源。
_Avoid_: Agent summary

**Project Fact（项目事实）**:
在相应验收或决策边界通过后，获准供后续工作复用的主张。
_Avoid_: Memory、note

**Event Log（事件日志）**:
运行事件与图变更的审计记录；Context Graph 是 Event Log / Artifact Store 的可追溯投影。
_Avoid_: Chat history

**Artifact Store（产物存储）**:
交付物、报告与证据引用的存放处；PhaseOutput 与 Coordination Edge 只携带引用。
_Avoid_: File dump、shared folder

**MemoryCandidate（记忆候选）**:
Agent 在工作中标注的、值得持久化的结构化候选（陈述、理由、scope、子图归属、来源引用、置信度）。只有 Ctx Manager 有权准入为 Context Node；其余一律只留审计事件。
_Avoid_: Note、memory write

**Context Graph（上下文图）**:
从运行证据中提炼出的知识节点及其逻辑关系的持久图。Ctx Manager Agent 是唯一写入口；所有 Agent 可列表、探索、检索和订阅，但不可直接写。
_Avoid_: Memory store、KB dump

**Context Node / Context Edge（上下文节点/边）**:
图中的知识陈述与逻辑关系（logical_adjacent、supports、contradicts、supersedes、derived_from 等）。Agent 只能通过 MemoryCandidate 间接影响它们；embedding 相似不能单独建立语义边。
_Avoid_: Chat excerpt、snippet

**Context Subgraph（上下文子图）**:
可重叠的逻辑视图，不复制节点；是切片、检索与订阅的操作单位。
_Avoid_: Folder、tag collection

**Context Slice（上下文切片）**:
Context service 为一次 Agent Invocation 按 role/purpose/权限生成的只读快照（节点、子图概要、Frontier、冲突、graph revision）；`Explore` 沿 slice 或子图展开并返回 ContextSliceDelta。切片不是复制出来的新知识库。
_Avoid_: Prompt dump、full project history

**ContextSubscription（上下文订阅）**:
绑定 Invocation 与权限快照的受控订阅关系。来源只有两种：切片/检索时自动订阅，或 Agent 主动选择；匹配更新由自动化订阅执行器推送 Context Delta，不建立 Agent mailbox。
_Avoid_: Notification、SearchJob、Delivery

**Context Delta（上下文增量）**:
已订阅子图发生匹配更新后推送给活动 Invocation 的增量载荷；增量、可合并、可重放。系统不提供订阅之外的旁路推送。
_Avoid_: Message、push notification

## 已删除术语

**Execution Graph（执行图）**:
已删除。phase 内执行步骤与过程上下文属于 Agent Runtime 的内部现场，不需要独立持久实体；阶段结束只提交 PhaseOutput，运行中只提交 OrchestrationProposal。
_替代_: Coordination Graph（持久编排）、Agent Runtime 内部现场

**Context Block / Context Pack（上下文块/包）**:
已删除。统一为 Context Node（知识陈述）、Context Slice（Invocation 绑定的只读快照）与 ContextSubscription（受控订阅）。
_替代_: Context Node、Context Slice、ContextSubscription

## AgentTeams 基座术语边界

AgentTeams（third_party/agentteams）是归档基座：只复用已证实能力。以下术语是基座实现语汇，仅在实现映射与映射表中出现，不作为领域语言使用：

| 基座术语 | 基座含义与位置 | Threadmill 处置 |
| --- | --- | --- |
| Worker / Leader / Team | controller 管理的 worker 实例与 team 的 leader 角色（`agentteams-controller/api/v1beta1/types.go`） | Worker 实例映射为可承载 Agent Invocation 的资源（Worker Capacity 的健康输入）；Leader 亲自调度模式不延续 |
| ready_nodes | 返回依赖已被接受的 pending DAG 节点（`copaw/src/copaw_worker/task.py::ready_nodes`；`copaw/src/copaw_worker/hooks/tools/projectflow.py`；`plugins/teamharness/mcp/server.py`） | runnable endpoint 投影的参考逻辑，适配封装；不直接作为 Scheduler 使用 |
| plan_dag / accept_task_result / delegate_task | Leader 的写图与委派工具（`copaw/src/copaw_worker/task.py`；`copaw/src/copaw_worker/hooks/tools/taskflow.py`） | 写图归 Task Manager、启动归 Agent Runtime；不接受 Leader 直接调用 |
| TaskStore / plan.md / team-state.json | 基座文件协议（`copaw/src/copaw_worker/task.py::FileSystemTaskStore`；`agentteams-controller/internal/agentconfig/coordination.go`） | 归档实现细节，不进入领域模型 |
| Matrix room / notification | 基座通信通道 | 不等于 mailbox；Threadmill 无 Agent mailbox / AgentMessage |
| Scheduler、Event Log、Context Graph、git worktree 服务、Merge Queue | 基座均不存在 | Threadmill 新建；勿假设基座已有 |
