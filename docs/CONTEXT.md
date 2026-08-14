# Threadmill 领域语言

Threadmill 协调软件工作，但不把工作绑定在执行它的 agent session 上。仓库中的设计文档、接口和 Skill 应统一使用下面这些词。语义以 docs/threadmill-unified-design.md 为准；本文是其词汇表。AgentTeams（third_party/agentteams）是归档基座，其术语只出现在实现映射中，不进入领域语言。

## 工作

**Requirement（需求）**:
对目标、动机和约束的原始表达。Requirement 用来保留来源，不是可直接调度的工作。Requirement 是要求，不等于 preference：原始目标、约束与验收意图映射为 `directive`（投影时引用 Requirement 原件）；其中稳定风格取舍属于 `directive` 的软约束，通过来源/字段区分，不是独立 Kind。
_Avoid_: Request、prompt、ticket

**Task Contract（任务契约）**:
一个工作单元的稳定约定，包括要改变什么、为什么、允许的边界和验收条件。
_Avoid_: Task prompt、plan

**Task（任务）**:
由一个 Task Contract 约束、固定包含 `plan / execute / verify` 三个工作阶段的持久工作单元。`prepared / done` 是派生门控状态；人工或外部决定是 blocker/decision 条件，不增加第四阶段。Task 的寿命长于任何 Agent Invocation。
_Avoid_: Agent、session、thread

**轮次（Round）**:
对同一个 Task Contract 的一次有界尝试，由该轮次的 Workspace Binding 标识，不创建独立持久实体。验证失败后由 Task Manager 失效旧输出、重开 execute→verify 轮次，而不是创建新的 task。
_Avoid_: Retry task、Task Attempt

**Phase Endpoint（阶段端点）**:
Task 内固定的 `plan / execute / verify` 三个命名协调端点；其他工作可向它们提供输入或依赖其结果。人工或外部决定表示为 blocker/decision 条件，不是 Phase Endpoint。
_Avoid_: Status flag、agent state

**Coordination Graph（协调图）**:
由 task、phase endpoint、edge（condition、data、on_false、freshness）、blocker 和 decision 构成的持久图，记录尚未履行的因果义务。Task Manager Agent 是唯一写入口；可热修改；Scheduler 只读。
_Avoid_: Workflow diagram、agent chat graph

## 编排协议

**PhaseOutput（阶段输出）**:
阶段结束时提交的结构化输出载荷，含交付物/报告/证据引用，并由 Runtime 绑定完整 `PhaseResultBinding`：Task Contract、phase、Input Revision、Workspace Binding/Head、`ContextSliceRef` 与 `TaskMemoryBufferRef`。
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
main 的唯一写入口；对 verify passed 的候选做 latest-main 机械应用检查与串行合入。无 main drift 时复用原 verify；有 drift 或冲突时才启动 Targeted Verifier。Targeted Verifier 只能在 allowed/conflict paths 内解冲突；不能 commit/push 或写 Coordination/Context Graph，无法安全处理时向 Manager 提案重新编排。
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

**Hard Gate（候选入口机械校验）**:
Context Service 在 MemoryCandidate 入 Task 缓冲前执行的同步确定性校验：字段结构、Statement、Kind、SourceRefs、权限、敏感信息和 general-only 目标。不调用 LLM，不判断长期价值；失败只返回 error、记审计且不入缓冲。
_Avoid_: Memory review、semantic scoring

**MemoryCandidate（记忆候选）**:
Agent 提交的结构化记忆主张；通过硬门槛后进入当前 Task 的 append-only 候选缓冲，返回 `CandidateBufferedReceipt{CandidateID}`。该 Task 固定的 plan/execute/verify 三阶段可经 `TaskMemoryBufferReader` 只读，跨 Task 不可见；候选不落图、不参与订阅或推送。Task done 后冻结终审，接受者由 Context Service 原子落图。
_Avoid_: Note、memory write

**Preference（偏好）**:
用户明确且稳定的风格取舍，表示为 `directive` 的软约束（通过来源/字段与硬约束、任务契约区分），不是独立 Kind。preference 不等于 requirement：原始目标、约束与验收意图不因“用户偏好某风格”而变成 preference；只有稳定风格取舍本身才记作 preference。
_Avoid_: Style note、requirement

**ContextNode 节点 Kind（directive / fact / hypothesis）**:
所有 ContextNode 全局统一使用这三类 Kind，与 Subgraph.Kind（`general` | `task`）正交。Context Agent 可经 Context Service CRUD general 节点和 general 子图；`TaskContextWriter` 定向投影只写 `task` 子图，两条写路径目标不相交。`directive`：规范性陈述，定义必须/应当/期望做什么，包括用户 Requirement、稳定偏好，以及 Task Manager 已写入 Coordination Graph 的 Task Contract 与 endpoint DeliverySpec/ReportSpec 的上下文投影（权威在 Coordination Graph/Requirement 原件，节点必须引用来源）。`fact`：已经成立、发生或经相应验收边界接受/验证的描述性陈述。`hypothesis`：尚待证据验证的描述性推测，不得承载任务或用户要求。
_Avoid_: task fact、phase fact、hypothesis task

**Context Graph（上下文图）**:
从运行证据中提炼出的知识节点及其逻辑关系的持久图。所有 Agent 可列表、探索和订阅；Context Agent 额外持有机械 Search，并可经 Context Service CRUD general 节点和 general 子图、审查 done 后冻结候选。task 子图及其节点只接受 Task Manager 经 `TaskContextWriter` 定向投影；任何持久化 mutation 均由 Context Service 执行。
_Avoid_: Memory store、KB dump

**Context Node / Context Edge（上下文节点/边）**:
图中的知识陈述与逻辑关系（logical_adjacent、derives_from_subgraph、supports、contradicts、supersedes 等）。所有节点统一使用 `directive | fact | hypothesis`；同一 general 节点可属于多个 general 子图。普通 Agent 只能通过 MemoryCandidate 间接影响节点；Context Agent 可经受控 CRUD 管理 general 节点，不能操作 task 子图节点。
_Avoid_: Chat excerpt、snippet

**Context Subgraph（上下文子图）**:
可重叠的逻辑视图，不复制节点；是切片、检索与订阅的操作单位。
_Avoid_: Folder、tag collection

**Recipient Binding（接收者绑定）**:
Task Manager 写 `task` 子图时声明的稳定编排接收者，即 `TaskContextRecipient{TaskID, EndpointRefs}`（`EndpointRefs` 为空表示该 Task 的全部 endpoint）；每个 Task 创建时由 Context Service 注册唯一 `TaskContextSubgraphBinding{TaskID, SubgraphID}`。接收者只引用稳定 Task / Phase Endpoint，不引用 Agent、worker、session 或 Invocation。结构与规则见 [context-graph.md](./context-graph.md) 的 Task 定向投影章节。
_Avoid_: recipient agent、assignee、mention

**Task-directed Projection（Task 定向投影）**:
Task Manager 经受控写接口（`TaskContextWriter.ProjectTaskContext`）写给指定 Task / endpoint 的 `task` 子图节点写入；Context Service 按 `TaskID + EndpointRef` 确定性匹配，把命中的定向节点并入初始 Context Slice（`ContextSliceRef`），不依赖 Agent 行为或 embedding。投影必须引用权威来源，不替代 Coordination Graph、Requirement 原件或 PhaseOutput。该路径不经过候选缓冲，也不经过 Context Agent；候选缓冲与定向投影是两条目标不相交的写路径。
_Avoid_: context push、memory injection、prompt injection

**Task Memory Buffer（Task 候选记忆缓冲）**:
每个 Task 一份 append-only 工作记忆，由该 Task 固定的 `plan / execute / verify` 三阶段共享。通过硬门槛的 MemoryCandidate 在 Task done 前可经 `TaskMemoryBufferReader` 只读；跨 Task 不可见。它不是 Context Graph，不参与图检索、revision、订阅或 ContextDelta；done 后冻结并批量终审。
_Avoid_: Context Subgraph、candidate graph、Agent private memory

**Context Slice（上下文切片）**:
Context service 为一次 Agent Invocation 按 role/purpose/权限生成的已落图只读快照（节点、子图概要、Frontier、冲突、graph revision）。它与同 Task 的 `TaskMemoryBufferRef` 分离：前者可探索/订阅，后者只读未终审候选；两者都不能覆盖 ContractRef/Inputs。
_Avoid_: Prompt dump、full project history

**ContextSubscription（上下文订阅）**:
绑定 Invocation 与权限快照的受控订阅关系。来源只有两种：切片/检索时自动订阅（经 `contextAgent.retrieve` 的检索路径中，Search 命中子图的自动订阅绑定原请求方 Invocation，不是 Context Agent 自己；可信 consumer binding 由 Runtime 在 Context Agent 调用 Search 时附加，不放入 SearchRequest），或 Agent 主动选择；Context Graph 在节点/边事务提交并递增受影响 subgraph revision 后主动触发内部订阅执行器匹配已存在订阅并生成 Context Delta，Runtime 只负责送达活动 Invocation，不建立 Agent mailbox。
_Avoid_: Notification、SearchJob、Delivery

**Context Delta（上下文增量）**:
已订阅子图发生匹配更新后，由 Context Graph 主动触发内部订阅执行器生成、经 Runtime 送达活动 Invocation 的增量载荷；增量、可合并、可重放；Context Agent 不推送。系统不提供订阅之外的旁路推送。Task 工作期间的候选只入缓冲，不产生 Delta；只有 Task done 后审查落图的节点变更触发推送。
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
