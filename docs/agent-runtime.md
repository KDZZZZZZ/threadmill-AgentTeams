# Agent Runtime 设计（AgentTeams 基座）

版本：v0.6（重写）
状态：Draft
定位：Agent Runtime 是所有 Agent Invocation 的统一执行边界。本文说明 Threadmill 的 Runtime 语义如何在 third_party/agentteams（AgentTeams v1.2.x 归档基座）上实现。
> 语义以 docs/threadmill-unified-design.md 为准（下称《统一设计》），本文只补充实现映射；术语冲突时以《统一设计》为准。

---

## 1. 定位

《统一设计》第 16 节：Runtime 是所有 Agent Invocation 的统一边界，包括 Task Manager、Context Agent（即此前 Ctx Manager Agent / 图中 ctx agent）、planner、executor 和 verifier。它负责 provider detect/auth/capability、按 role/purpose 组装 prompt、Context Slice 和输出契约、创建 Invocation 并从 Workspace Service 取得轮次 Workspace、施加 phase 权限与写 lease、运行/取消/恢复/替换 Agent、归一化事件、观察真实 write set、执行 Context 读请求并传递自动订阅的 Context Delta、把 `PhaseOutput` / `OrchestrationProposal` / Requirement 交给相应唯一 owner、把 MemoryCandidate 写入 Task 级候选缓冲（硬门槛后返回 `CandidateBufferedReceipt`，Task 工作期间不审查、不落图、不推送）。Runtime 不判断 Task 是否完成，不写 Coordination Graph，不替 Context Agent 检索或接受记忆，不合并 main，不创建任何业务对象。

AgentTeams 基座提供的不是"与 CLI 无关的通用 adapter 层"，而是一套可部署、可观测的 agent 运行时与协作基座：

| AgentTeams 组件 | 作用 |
| --- | --- |
| agentteams-controller | Go 算子：Worker/Team/Human/Manager CR 的 reconcile、REST API（:8090）、worker 容器生命周期、`agt` CLI |
| Worker | 持久 worker 容器（OpenClaw 或 QwenPaw runtime）；无状态，配置与记忆都在 MinIO，可随时销毁重建 |
| QwenPaw worker daemon | 期望状态 apply 循环、心跳、对象存储同步（`qwenpaw_worker` 包） |
| TeamHarness | 协作协议：角色 prompt、team skills、MCP 工具（health / message / roomflow / filesync / artifact / projectflow / taskflow） |
| WorkerFlow | 单个 Worker 内部的临时 agent 编排（`worker_agentflow`） |

Threadmill 的 Runtime 在这套基座上分两层落地：

```text
基座层（直接复用）:
  controller 生命周期 / QwenPaw 进程与 API / TeamHarness 委派-提交协议
  / WorkerFlow 临时 agent / MinIO 同步 / 心跳与 trace 关联

Threadmill 适配层（新建）:
  Invocation 记录与事件投影 / phase 语义与写 lease / Context Slice 组装与 Context 读工具
  / 输出契约（PhaseOutput 形状）/ OrchestrationProposal 转交 / 观察 write set
```

AgentTeams 没有 Event Log、Context Graph、Scheduler、git worktree 或 Merge Queue；这些概念在本文与 workspace-merge.md 中一律标注为 Threadmill 新建，不声称基座已提供。

---

## 2. 基座能力盘点与复用判定

每份能力都按四类判定：**直接复用**、**适配封装**、**Threadmill 新建**、**不应复用**。

### 2.1 直接复用

| 能力 | 位置（third_party/agentteams） | 说明 |
| --- | --- | --- |
| Worker 生命周期 | `agentteams-controller/api/v1beta1/types.go`（Worker / WorkerSpec / WorkerStatus / TeamMemberStatus）；`agentteams-controller/cmd/agt/`（`agt create/apply worker`） | Worker CR → Pod 的 reconcile；`spec.state` Running/Sleeping/Stopped；`spec.containerManaged`、`backendRuntime=pod`、`deployMode` Local/Edge；状态 `phase`：Pending/Starting/Running/Updating/Stopping/Sleeping/Stopped/Failed，`lastHeartbeat`、`lastActiveAt` |
| 期望状态注入 | `qwenpaw/src/qwenpaw_worker/worker.py`、`update.py`（MemberRuntimeConfig）；`docs/member-runtime-config-contract.md` | controller 写 `runtime.yaml`（默认 `agents/{memberName}/runtime/runtime.yaml`）；daemon 每 5s apply `desired.model / mcpServers / channelPolicy / agentPackage`，不重启 Pod |
| Agent 进程与 API | `qwenpaw/src/qwenpaw_worker/api.py`；`qwenpaw/README.md` §1.1 | QwenPaw localhost HTTP API：`/api/version`（必须精确 2.0.1）、`/api/agents`、`/api/mcp`、`/api/access-control/agentteams_matrix`、`/api/agents/default/agent-status`、Skill API |
| 委派-执行-提交协议 | `plugins/teamharness/mcp/server.py`（taskflow）；`plugins/teamharness/skills/team/task-execution/SKILL.md` | `delegate_task / ack_task / submit_task / check_task / cancel_task`；TaskMeta `assigned → in_progress → submitted`；`submit_task` 记录结构化状态并自动发布 result.md 与 deliverables 为 Matrix `m.file` |
| 角色提示词与团队契约 | `plugins/teamharness/prompts/agent/worker.md`、`leader.md`、`remote-member.md`；`prompts/team/TEAMS.md`；`prompts/manager/AGENTS.md`、`TOOLS.md`、`HEARTBEAT.md` | Worker/Leader/Remote Member 角色边界、Matrix 提及纪律、Credential Safety（不可覆盖规则） |
| 文件同步与共享目录 | `qwenpaw/src/qwenpaw_worker/sync.py`（FileSync）；`shared/lib/worker-file-sync.sh`；`worker/scripts/worker-entrypoint.sh` | 写者推送 + @mention 按需拉取；排除 credentials / sessions / logs / tool results / media / file store / runtime cache；`.last-pull` 标记防回推 |
| 心跳与就绪 | `qwenpaw/src/qwenpaw_worker/heartbeat.py` | 本地 `heartbeat.json`；`POST /api/v1/workers/{name}/ready`、`/heartbeat`；`lastActiveAt` 取自 agent-status |
| 输出清洗与敏感拦截 | `plugins/teamharness/adapters/qwenpaw/plugin.py`（`AGENTTEAMS_OUTPUT_SANITIZE_KEYWORDS`）；`plugins/teamharness/mcp/server.py`（SENSITIVE_ARTIFACT_NAME_RE / TEXT_RE） | artifact 发布前按名称/正文拦截 secret、token、private key、Authorization 头等 |
| 运行关联 trace | `plugins/teamharness/adapters/qwenpaw/task_trace.py` | OTel SpanProcessor 给每轮 entry span 打 `agentteams.task.id` / `agentteams.project.id`，从 `shared/tasks/*/meta.json` 解析 |
| 会话隐私 | `qwenpaw/src/qwenpaw_worker/worker.py`（SESSION_FILE_PROMPT_POLICY） | agent 被禁止读取 `sessions/` 下的会话文件 |
| 工具权限 | `agentteams-controller/api/v1beta1/types.go`（AccessEntry、CredentialBinding.ToolWhitelist、MCPServer）；`qwenpaw/README.md` §1.3 | 默认对象存储权限 scoped 到 `agents/<name>/*` 与 `shared/*`；MCP 客户端与 allow policy 经 `/api/mcp` 与 MCP Policy API |

### 2.2 适配封装

| Threadmill 语义 | 适配方式 |
| --- | --- |
| Phase Endpoint invocation ↔ taskflow 委派 | 一次 `delegate_task` = 一次有界执行；`spec.md` 由 Task Manager 写入 Task Contract + DeliverySpec/ReportSpec + phase lease 声明；worker 按 spec 执行并提交 |
| PhaseOutput 与 submit_task/result.md 的边界 | `submit_task(summary, deliverables, status)`、`result.md` 和 `check_task` 只提供 physical execution evidence；正式 PhaseOutput 必须由 Agent 通过 `agent.submitPhaseOutput` 提交，并由 Threadmill Runtime 校验与持久化。不得从 TeamHarness 状态或 result.md 自动推导 PhaseOutput。 |
| Agent Invocation（临时计算资源）↔ WorkerFlow 临时 agent | `workflow_run / create_temp_agent` 创建 `tmp-` 前缀 agent（独立 workspace、自定义 AGENTS.md/skills 模板），bounded 任务结束即删除 |
| Context 读工具注入 ↔ QwenPaw MCP 机制 | 注入机制直接复用（`desired.mcpServers` → `/api/mcp`）；工具本身是 Threadmill 新建的 `threadmill-ctx` MCP server |
| Workspace 目录语义 ↔ shared/tasks 布局 | `shared/tasks/{task_id}/{workspace_ref}/`（含 workspace/、progress/、result.md）作为轮次 Workspace 的目录落点；目录所有权规则直接复用 task-execution SKILL.md |
| 输出状态映射 ↔ TaskResult 状态机 | `SUCCESS → passed`；`SUCCESS_WITH_NOTES → passed + notes`；`REVISION_NEEDED / BLOCKED / FAILED → failed`（见 §6.2） |
| 验收机械部分 ↔ check_task | `check_task` 返回 `effective = status==submitted && 无 deliverable 校验错误`（deliverables 必须位于 `shared/tasks/{task_id}` 下）；语义验收由 Threadmill verifier 完成 |

### 2.3 Threadmill 新建

- AgentInvocation 记录与 run 生命周期投影（AgentTeams 无此实体）。
- phase lease 的声明与强制（AgentTeams 无 phase 概念，见 §5.1）。
- Context Slice 组装、Context Graph 读接口与自动订阅，以及同 Task 三阶段共享的 `TaskMemoryBufferReader` / `TaskMemoryBufferRef` 装配。
- PhaseOutput / OrchestrationProposal / Requirement 路由；MemoryCandidate 硬门槛、Task 级 append-only 缓冲读写与 done 后冻结终审转交。
- Event Log 投影（AgentTeams 没有 Event Log；从 heartbeat、trace、meta.json/result.md、委派事件投影）。
- Observed Write Set 观察器（目录快照对比 / git diff / deliverables 交叉核对）。
- 输出 JSON Schema 校验（针对 result.md 内嵌载荷）。

### 2.4 不应复用

1. TeamHarness 的 project DAG/Loop 与 `ready_nodes` 不应作为 Coordination Graph。它是 Leader 维护的团队协作视图（`shared/projects/{id}/plan.md`），可被改写；Coordination Graph 的唯一写入口是 Task Manager。
2. Leader/Manager 的房间状态记忆（"从 room 推断身份/项目"）不应作为权威编排状态；Leader prompt 本身就禁止从 session 猜状态。
3. `message` MCP 工具不应成为 Agent mailbox。基座事实：worker/remote-member 的 `message` 工具被禁用（server.py `MESSAGE_TOOL_BLOCKED_ROLES = {"worker", "remote-member"}`）。Threadmill 不提供替代 mailbox；外部记忆只来自切片、探索、订阅、自动 Delta，以及列表/探索不足时经 `contextAgent.retrieve` 请求的语义检索。
4. Matrix 房间线程不应作为过程上下文审计通道。运行过程上下文留在 Invocation 内（QwenPaw `sessions/` 私有、Threadmill 不读）。
5. QwenPaw 原生 subagent（共享同一 workspace）不应承担需要隔离的 phase 执行；需要隔离时用 WorkerFlow 临时 agent 或独立 worker。
6. `accept_task_result` 不应等于 done 或 merge（见 workspace-merge.md §4.2）。
7. 不应把 `runtime.yaml` 或 TeamHarness meta.json 当作 Coordination/Context Graph 的存储：图状态只存在于 Threadmill 侧。

---

## 3. 组件映射（可执行）

| Threadmill 职责 | AgentTeams 基座实现 | 路径 | 复用类型 |
| --- | --- | --- | --- |
| 创建/复用执行宿主 | Worker CR + Pod；`spec.state`、`containerManaged`、`backendRuntime`、`deployMode` | `agentteams-controller/api/v1beta1/types.go`；`agentteams-controller/internal/service/deployer.go` | 直接复用 |
| 检测可用性与容量 | `Worker.Status.Phase / LastHeartbeat / LastActiveAt`；Team `readyWorkers / totalWorkers` | `api/v1beta1/types.go`（WorkerStatus、TeamStatus） | 直接复用 |
| 启动 agent 进程 | QwenPaw app（`/api/version`=2.0.1 就绪）或 OpenClaw gateway（`openclaw gateway run`） | `qwenpaw/src/qwenpaw_worker/worker.py`；`worker/scripts/worker-entrypoint.sh` | 直接复用 |
| 注入模型/MCP/ACL/skill | `desired.*` apply 循环（model、mcpServers、channelPolicy、agentPackage） | `qwenpaw/src/qwenpaw_worker/update.py` | 直接复用 |
| 发起 phase invocation | `taskflow delegate_task`（含 spec）；`worker_agentflow workflow_run`（临时 agent） | `plugins/teamharness/mcp/server.py`；`plugins/workerflow/mcp/server.py` | 适配封装 |
| 取消/替换 | `taskflow cancel_task(reason[, replacementTaskId])`；`delete_temp_agent`；Worker `spec.state=Stopped` | 同上；`agentteams-controller/api/v1beta1/types.go` | 直接复用 |
| 输出确认 | `ack_task / submit_task / check_task`；TaskResult 状态机 | `plugins/teamharness/mcp/server.py`（`_task_result_from_meta`） | 直接复用 |
| Context 读工具 | Threadmill 新建 `threadmill-ctx` MCP server，经 `desired.mcpServers` 注入；`ContextGraphSearcher.Search` 仅注入 Context Agent，不注入普通 worker / Task Manager | 注入机制：`qwenpaw_worker/update.py`；工具：Threadmill 新建 | 工具新建、机制复用 |
| 事件记录 | 无 Event Log；从 heartbeat / trace / meta.json / result.md / 委派事件投影 | `task_trace.py`；`heartbeat.py` | Threadmill 新建投影 |
| Scheduler | AgentTeams 无 Scheduler；controller 只 reconcile Worker 生命周期，不调度 phase | — | Threadmill 新建 |

### 3.1 三种执行形态

1. **持久 worker 上的委派（默认）**：Scheduler 选择 runnable endpoint 与匹配的 Worker Capacity，Runtime 经 controller REST API 选择或创建执行宿主，再用 `taskflow delegate_task` 委派 phase；worker 常驻并可承载多次 Invocation，但不拥有 Task、轮次或持久 Agent 身份。适合 plan / execute / verify 的长任务与需要稳定工具环境的执行。
2. **WorkerFlow 临时 agent（ephemeral）**：`workflow_run` 创建 `tmp-` agent（独立 workspace、自定义 AGENTS.md/skills 模板），bounded 任务结束 `workflow_finish/fail` 后 `delete_temp_agent` 清理。适合一次性探索、并行检查、隔离验证。这是《统一设计》“Agent Invocation 是可替换的临时计算资源”的现成形态。
3. **直接执行**：Runtime 在已选执行宿主中启动当前 endpoint 的 bounded Invocation。适合不拆分的 phase，但仍须经过 Invocation 记录、Context Slice、权限、输出契约和事件投影。

### 3.2 持久 worker 与 ephemeral invocation 的适配

- **持久 worker 不等于持久 Agent 身份**。worker 是执行宿主：无状态，任何 Pod 重建后从 MinIO 恢复（`worker/README.md`）。Task / 轮次身份在 Threadmill 侧（Coordination Graph + Workspace Binding），worker 容器可替换。
- **每次 phase invocation 在 worker 内是独立会话/委派轮次**。丢弃 Thread 不丢失 Task（见 docs/CONTEXT.md 的 Thread 语义）：QwenPaw 会话文件位于 `sessions/`，agent 被禁止读取（SESSION_FILE_PROMPT_POLICY），Threadmill 也不把它们当编排输入。
- **临时 agent 的 run 级共享目录**（`<default-workspace>/shared/workerflow/<runId>/`，含 `inputs/`、`outputs/<agent-id>/`）可作为轮次级临时 Workspace；`runId` 可关联 WorkspaceRef（workspace-merge.md §3.2）。
- **形态选择规则**：需要稳定工具环境或长时运行 → 复用持久 worker 作为执行宿主；需要隔离/并行/一次性检查 → 临时 agent；两者都不提供持久 Agent 身份，且都必须经过同一 Threadmill 适配层（Invocation 记录、Context Slice、权限、输出契约、事件投影）。

---

## 4. 状态转换

### 4.1 Worker 生命周期（直接复用，只读）

```text
Worker CR 创建 -> controller reconcile -> Pod 启动
  -> Pending -> Starting -> Running（/api/version 就绪后 POST ready）
  -> Updating（desired-state apply）-> Stopping/Sleeping/Stopped（spec.state）
  -> Failed（容器异常）-> 删除/重建（MinIO 恢复，无状态）
```

Threadmill 侧只读 `phase / lastHeartbeat / lastActiveAt` 做容量与健康判断，不写入。

### 4.2 Invocation 生命周期（适配封装）

委派形态：

```text
Scheduler 选择 runnable endpoint 与匹配容量，Runtime 选择执行宿主
  -> taskflow delegate_task(spec)            TaskMeta: assigned
  -> worker ack_task                          in_progress（Invocation 开始，task 目录就绪）
  -> worker 执行（受 phase lease 与工具策略约束）
  -> submit_task(summary, deliverables)       submitted（result.md 已写）
  -> check_task                               返回 effective + validationErrors
  -> 验收决策（verify passed / revision / blocked）
```

- 取消路径：`cancel_task(reason[, replacementTaskId])`；worker 掉线 → `LastHeartbeat` 超时 → Threadmill 标记 invocation failed，Task Manager 失效旧输出并重开 execute→verify 轮次（新委派轮次、新 Workspace）。
- `submit_task` 是终态动作：提交后 worker 不得继续编辑旧 task（task-execution SKILL.md）；修订必须由 Leader 重新委派。

临时 agent 形态：

```text
workflow_run(subagents | nodes)
  -> create_temp_agent(tmp-*) + 发送 submit prompt
  -> 每完成一个 node：workflow_update(steps: done)
       -> 返回 readyInstructions 时继续派发下游 node
  -> workflow_finish / workflow_fail
  -> delete_temp_agent + cleanup_shared（finally 语义）
```

- 失败补偿：`workflow_run` 创建失败会回滚调用 `workflow_fail` 并清理已建 agent（`plugins/workerflow/mcp/server.py` `_fail_workflow_run_spawn`）。
- 临时 agent 状态记录在 `<default-workspace>/shared/workerflow/<runId>/workflow.json`（status: running/done/failed；subagents/nodes/steps 行；readyInstructions/waitingInstructions；Matrix card eventId）。

### 4.3 轮次 / phase 状态（Threadmill 新建）

记录在 Coordination Graph 与 WorkspaceBinding（Threadmill 侧），不在 AgentTeams meta.json 中扩展：

```text
prepared -> plan(invoked/running/passed)
  -> execute(invoked/running/passed)
  -> verify(invoked/running/passed)
  -> done
任意 phase failed -> OrchestrationProposal（Task Manager 裁决：失效旧输出、重开轮次）
```

一次委派轮次（§4.2）承载一个 phase invocation；Task 轮次生命周期跨多个委派轮次，由 Task Manager 编排（统一设计 §3.1、§3.2）。

---

## 5. 权限、事件与过程上下文隔离

### 5.1 权限与 phase lease（安全边界）

AgentTeams 直接复用的权限原语：

- **角色工具可见性**：`MESSAGE_TOOL_BLOCKED_ROLES = {"worker", "remote-member"}`（server.py）——worker 无跨会话 `message` 工具，只能回复当前房间。Threadmill 依赖此事实实现"无 Agent mailbox"。
- **通道 ACL**：`desired.channelPolicy`（group/dm allow/deny）apply 到 `agentteams_matrix` ACL 命名空间（`/api/access-control/agentteams_matrix`）。
- **存储权限**：AccessEntries 默认 object-storage scoped `agents/<name>/*` 与 `shared/*`（types.go AccessEntry 注释）；CredentialBinding + ToolWhitelist 按工具白名单授权。
- **凭据**：`runtime.yaml` credentials 段只写 env 名/文件路径，不写值；Matrix token、gateway key、storage key 经 env / SA token 注入。
- **敏感输出**：artifact 发布前 SENSITIVE_ARTIFACT_NAME_RE/TEXT_RE 拦截；`AGENTTEAMS_OUTPUT_SANITIZE_KEYWORDS` 输出清洗；Credential Safety 规则在 TEAMS.md 中不可覆盖。
- **会话隐私**：SESSION_FILE_PROMPT_POLICY 禁止 agent 读取 `sessions/`。

Threadmill 新建的 phase lease（基座无此概念）——一个轮次任一时刻只有一个有效写 lease，实现分三层：

```text
a) 委派轮次隔离（强）：每 phase 一次 taskflow 委派给不同 worker / 每 phase 一个 WorkerFlow 临时 agent
   -> 容器或进程级隔离，天然满足"一个写 lease"；
b) 工具级（中）：MCP allow policy（QwenPaw MCP Policy API）+ 目录 ACL（AccessEntries）
   -> plan 只授只读工具与 plan 目录写权；execute 才授实现写权；verify 只授检查工具与 evidence 目录写权；
c) 提示词级（弱，兜底）：worker.md + phase prompt 声明只读/只写边界。
```

lease 记录在 `WorkspaceBinding.PhaseLeases`（phase → invocation id）。Task Manager 通过图激活或失效 endpoint；Runtime 在启动已调度 Invocation 前向 Workspace Service 取得并校验 lease，phase 结束后释放（统一设计 §4.3）。

### 5.2 事件

AgentTeams 原生事件面：

- 心跳：本地 `heartbeat.json` + `POST /api/v1/workers/{name}/ready|heartbeat`（heartbeat.py）。
- 运行关联 trace：`AgentTeamsTaskSpanProcessor` 给每轮 entry span 打 `agentteams.task.id` / `agentteams.project.id`（task_trace.py）。
- 协作事实：Matrix 房间消息、`shared/tasks/*/meta.json`、`result.md`、`workflow.json`。

Threadmill 新建 Event Log 投影：Runtime 把委派/ack/submit/取消、心跳超时、trace 关联与文件事实投影为统一 invocation 事件（invoked / acked / submitted / failed / cancelled + 证据 refs）。**AgentTeams 没有 Event Log**；投影器是 Threadmill 组件，消费上述原生事实流。

### 5.3 过程上下文隔离

- 运行过程上下文（中间推理、工具输出、探索轨迹、未提交文件状态）留在 Invocation 内：QwenPaw `sessions/` 私有；Matrix 线程对房间成员可见但不进入编排。
- Task Manager 只能看到：它自己写的 spec、`result.md`、deliverables、`check_task` 返回、EvidenceRefs——即《统一设计》§5.5 的两个结构化边界：`PhaseOutput` 与 `OrchestrationProposal`。
- worker 主动提交 `OrchestrationProposal`：在 result.md 内嵌块或独立 deliverable 中提交（Threadmill 扩展 result 契约），Runtime 只校验形状与必填引用并转交 Task Manager；Task Manager 审批后热修改 Coordination Graph（Coordination Graph 可热修改、Task Manager 唯一写）。

---

## 6. Context 读操作与结构化输出

### 6.1 Context Graph 与 Task 工作记忆读操作

AgentTeams 没有这两块能力。Runtime 为每个 `plan / execute / verify` Invocation 注入：

- `ContextGraphReader`：读取已落图的 ListSubgraphs / Explore，并管理 Subscribe / Unsubscribe；Search 仍只注入 Context Agent；
- `TaskMemoryBufferReader`：只读取 Runtime 绑定 TaskID 对应的候选缓冲，不接受调用方指定 TaskID；
- 启动时分别装配 `ContextSliceRef` 与 `TaskMemoryBufferRef`。前者先建立初始子图订阅，再按当前 Invocation 的有效订阅子图并集物化；后者只是 append-only 缓冲快照。

Task Manager Invocation 获得同一组 `ContextGraphReader` 工具，包括 `Unsubscribe`，但不获得 `TaskMemoryBufferReader`；其 task 投影和终审触发仍走各自授权 seam。

候选追加不改变 graph revision、不触发 ContextDelta；同 Task 后续阶段可见，跨 Task 不可见。

Runtime 上下文装配以当前 `ConsumerInvocationID` 为隔离键：把初始切片自动订阅、Context Agent 检索自动订阅和 Agent 显式订阅的 `SubgraphIDs` 取去重并集，再交给 Context Service 做权限、revision、recipient 与预算过滤。这里的 Runtime 是承载 Agent 的 **Agent Runtime**，不是负责 Phase 调度与 start/stop/resume 的 `GraphRuntime`。订阅或取消订阅成功后，Agent Runtime 在下一次模型调用、等待重承载或 resume 装配前重算；不跨 Agent、Task 或 Invocation 合并，也不以最近一次订阅覆盖此前仍有效的订阅。

`context.unsubscribe(subscriptionId)` 只能取消 Runtime 绑定的当前 consumer 自己的订阅。Context Service 原子标记取消并记录审计后，Agent Runtime 不再注入该订阅独占的子图，并在 Delta 投递前重验 active 状态；若同一子图仍被其他有效订阅覆盖，则继续保留。已经送入当前模型调用的内容不能追溯删除。

### 6.2 结构化输出

TaskResult 状态机（server.py `ALLOWED_TASK_RESULT_STATUSES`）只描述 TeamHarness task 的物理执行结果：

| TaskResult（submit_task） | Threadmill 解释 |
| --- | --- |
| `SUCCESS` | execution evidence：worker 已成功提交 TeamHarness task |
| `SUCCESS_WITH_NOTES` | execution evidence：worker 已提交并附注 |
| `REVISION_NEEDED` | execution evidence：本次 carrier 请求修订 |
| `BLOCKED` | execution evidence：本次 carrier 报告阻塞 |
| `FAILED` / `PARTIAL` | execution evidence：本次 carrier 失败或部分完成 |

历史适配草案曾建议从 result.md 内嵌块构造 PhaseOutput；当前正式边界不再采用该路径。result.md 可以保留人类可读报告或执行证据引用，但正式输出只能通过 `agent.submitPhaseOutput` 进入 Runtime：

```jsonc
{
  "phase": "execute",
  "delivery_refs": ["artifact-ref"],
  "report_ref": "report-artifact-ref",
  "evidence_refs": ["evidence-ref"]
}
```

Runtime 通过当前 execution binding 校验身份、BindingRef、InputRevision、引用 ownership 与 completion input 条件，不信任 agent 自报内部绑定字段。TeamHarness `deliverables` 若同时使用，仍必须位于 `shared/tasks/{task_id}` 下（`_validate_task_deliverables` 强制），但它们不会自动成为正式 `delivery_refs`。

### 6.3 输出契约

DeliverySpec / ReportSpec 由 Task Manager 在委派时写入 `spec.md` 与 prompt；未规定二者的 endpoint 不可调度（统一设计 §5.6）。worker 按 spec 交付；`check_task` 的 result contract 校验（status + deliverable 前缀）复用为 VerifyResult 的机械部分，语义判断由 Threadmill verifier 完成（workspace-merge.md §4.2）。

---

## 7. 不变量

1. 所有 Agent（含 Task Manager）都经 Runtime 边界执行：Task Manager 在 Manager/controller 侧，phase agent 在 worker/临时 agent 侧，两侧都经过同一适配层（Context 注入、输出契约、事件投影）。
2. Runtime 不判断 Task 完成、不解释编排建议、不写 Coordination Graph / Context Graph、不合并 main。
3. worker 无 mailbox；外部知识来自 Context Graph，Task 内工作记忆来自当前 Task 候选缓冲，两者不混用。
4. 每个 Task 固定 `plan / execute / verify` 三阶段；Runtime 不创建第四阶段。
5. 同 Task 三阶段可经 `TaskMemoryBufferReader` 读取共享候选缓冲；TaskID 由 Runtime 绑定，跨 Task 读取必须拒绝。
6. 候选追加不改变 Context Graph revision、不触发订阅 Delta；done 后冻结终审才可能落图。
7. 每次 Invocation 有 workspace、权限与输出契约边界；Observed Write Set 以 Runtime 观察为准。
8. AgentTeams 委派协议不承载 Threadmill 的图或 Task 缓冲状态。
9. 订阅只保存最小路由/过滤/权限字段并随 ConsumerInvocationID 指向的 Invocation 结束；推送必须由已存在订阅与成功图事务触发。

---

## 8. 数据契约速查

| 契约 | 位置 | 关键字段 |
| --- | --- | --- |
| runtime.yaml（期望状态） | `agents/{memberName}/runtime/runtime.yaml`（qwenpaw worker 默认从对象存储拉取；`docs/member-runtime-config-contract.md` 推荐 `shared/runtime/members/{memberName}/runtime.yaml`） | `metadata.generation`；`team.*`（name/teamRoomId/leaderName/admin）；`member.*`（name/runtimeName/role/runtime/matrixUserId/personalRoomId）；`desired.model / mcpServers / channelPolicy / agentPackage / state`；`storage.*`（endpoint/bucket/prefixes）；`credentials.*`（仅 env 名/路径） |
| TaskMeta | `shared/tasks/{task_id}/meta.json` | `task_id`、`project_id`、`assigned_to`、`room_id`、`status`（assigned/in_progress/submitted）、`depends_on`、`spec_path`、`result_path` |
| ProjectMeta | `shared/projects/{project_id}/meta.json` | `project_id`、`status`、`mode`（quick/project）、`plan_type`（dag/loop）、`reply_route`、`parent_task_id`、`requester_report` |
| spec / result | `shared/tasks/{task_id}/spec.md`、`result.md` | Leader 拥有 meta.json/spec.md；worker 拥有 workspace/、progress/、result.md |
| WorkerFlow 状态 | `<default-workspace>/shared/workerflow/<runId>/workflow.json` | `status`（running/done/failed）、`subagents`/`nodes`/`steps`、`readyInstructions`/`waitingInstructions`、`eventId` |
| 心跳 | 本地 `heartbeat.json`；controller `POST /api/v1/workers/{name}/ready|heartbeat` | process/API 可达性、`lastActiveAt` |
| Invocation 记录（Threadmill 新建） | Event Log + Artifact Store | invocation id、形态（delegate/workflow_run/direct）、phase、worker/temp agent id、事件流 refs、PhaseOutput refs |

## Rehydrated package consumption

For a rehydrated physical execution, Matrix `TASK_ASSIGNED` is notification and activation transport: its text is only a preview. TeamHarness persists the complete task in `shared/tasks/<task-id>/spec.md`, and `ack_task` returns that complete specification to the fresh agent loop. A successful acknowledgement proves task activation, not package consumption.

Formal completion remains a separate Runtime-owned boundary. Only an authenticated, binding-scoped `agent.submitPhaseOutput` can persist the accepted output and the single authoritative `PhaseOutputSubmitted` event. Acceptance moves the logical waiting record from `running` to `terminal`, then reclaims the physical carrier through `running -> tearing_down -> terminated`. TeamHarness task completion, Worker/MCP credential deletion, exact execution-token revocation and workspace lease release are normal completion cleanup, not rollback. A cleanup failure leaves durable terminal/tearing-down evidence and is retried without returning the invocation to `waiting` or recreating token material.

After parsing the complete specification, the fresh session calls `runtime.confirmPackageConsumption`. Threadmill resolves the opaque execution token and derives Task, Invocation, Generation, ExecutionEpoch, BindingRef, and InputRevision from the trusted binding. It validates the canonical package digest and the Controller-derived `matrix:<room-id>` session against the epoch-aware physical execution before recording an idempotent receipt. Tokens, credentials, private headers, controller authentication, CAS revisions, hidden reasoning, and provider conversation state are never receipt fields. Rehydration starts a new session; it does not restore the old model session.
