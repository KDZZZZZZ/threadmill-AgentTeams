# Threadmill Phase Agent 与 AgentTeams 适配设计

> 状态：设计稿  
> 日期：2026-08-09  
> 范围：以 Threadmill 的 `phase-agent.md`、`threadmill-unified-design.md`，以及 AgentTeams 的 TeamHarness、WorkerFlow、QwenPaw 代码为依据。本文不把未在 AgentTeams 代码中出现的能力视为已存在。

## 0. 结论与标记约定

推荐把 **Threadmill 作为控制面和领域模型的唯一 owner**，把 **AgentTeams 作为受控执行承载层**。适配器应位于 Threadmill Agent Runtime 的 provider/host adapter 层，而不是放入 Task Manager、Coordination Graph 或 Phase Agent 内。

首个 MVP 只采用 TeamHarness `taskflow` 承载一个 Phase Invocation；WorkerFlow 只作为一个 Phase 内部、短生命周期的可选并行实现，不能承担 Threadmill 的阶段调度、等待恢复或任务完成判定。

本文使用四种明确标记：

| 标记 | 含义 |
| --- | --- |
| **已存在** | 所列 AgentTeams 源码中已可见、可直接调用的能力。 |
| **需要 Adapter** | 已有能力可使用，但字段、生命周期或权限语义必须由 Threadmill 进行转换和约束。 |
| **需要新增** | AgentTeams 未提供，须由 Threadmill（或其集成插件）实现。 |
| **暂不实现** | 不进入 MVP；也不能以“可能有”替代实现。 |

## 1. 已核对的事实基础

| 系统 | 已确认能力 | 不能据此推导的能力 |
| --- | --- | --- |
| TeamHarness | **已存在**：`taskflow` 具有 `delegate_task`、`ack_task`、`submit_task`、`check_task`、`cancel_task`；Worker 的任务目录包含 `spec.md`、`workspace/`、`progress/`、`result.md`；提交状态为 `SUCCESS`、`SUCCESS_WITH_NOTES`、`REVISION_NEEDED`、`BLOCKED`。 | **需要新增**：Task Contract、Phase Endpoint、PhaseInputSet、PhaseOutput、输入 revision、输出新鲜度、phase lease、DeliveryPolicy。 |
| TeamHarness | **已存在**：`filesync`、`artifact`、Matrix 房间/消息和项目/任务文件发布机制。 | **需要新增**：内容寻址 Artifact Store、ArtifactRef、跨阶段来源/哈希/权限校验、Threadmill 的正式输入边。 |
| WorkerFlow | **已存在**：`worker_agentflow.workflow_run` 可创建临时 QwenPaw agents、共享 run 目录、可选 DAG 节点；当前 Worker 负责收到结果后调用 `workflow_update` 并继续下游。 | **需要新增**：持久 Phase Endpoint 调度、等待输入后释放并恢复 Invocation、跨 run 的 Contract/Output 语义。 |
| QwenPaw API | **已存在**：MCP 客户端的创建/更新、工具列表和 policy 配置；Agent 配置与启停；插件与技能刷新。 | **需要新增**：已验证的“按 Phase Invocation 动态工具/目录/预算/lease 强制”语义。源码不能证明该 API 已提供这种粒度。 |

依据：[Phase Agent interface](https://github.com/KDZZZZZZ/threadmill-AgentTeams/blob/dev/docs/phase-agent.md)、[Threadmill unified design](https://github.com/KDZZZZZZ/threadmill-AgentTeams/blob/dev/docs/threadmill-unified-design.md)、[TeamHarness MCP](https://github.com/agentscope-ai/AgentTeams/blob/main/plugins/teamharness/mcp/server.py)、[Task execution skill](https://github.com/agentscope-ai/AgentTeams/blob/main/plugins/teamharness/skills/team/task-execution/SKILL.md)、[WorkerFlow skill](https://github.com/agentscope-ai/AgentTeams/blob/main/plugins/workerflow/skills/agent/worker-internal-workflow/SKILL.md)、[QwenPaw API client](https://github.com/agentscope-ai/AgentTeams/blob/main/qwenpaw/src/qwenpaw_worker/api.py)。

## 2. 职责边界

| 领域 | Threadmill | AgentTeams | 适配规则 |
| --- | --- | --- | --- |
| Requirement / Task Contract | **需要新增**：规整需求、稳定契约、DeliverySpec、ReportSpec、DeliveryPolicy。 | 不承担。TeamHarness 的 task `spec.md` 是执行说明，不是 Contract。 | Adapter 将 Contract 的只读投影写入任务 spec。 |
| 编排 | **需要新增**：Task Manager 唯一写 Coordination Graph，以 Phase Endpoint 和输入边决定 runnable。 | TeamHarness 可委派离散任务；WorkerFlow 可在一个 Worker 内推进临时 DAG。 | 禁止将 `projectflow` 或 `workflow_run` DAG 当作 Coordination Graph。 |
| 生命周期 | **需要新增**：Invocation、round、input revision、lease、取消、等待/恢复、失效。 | **已存在**：taskflow 的委派/确认/提交/取消和临时 agent 清理。 | Adapter 维护二者的关联与幂等。 |
| 计算执行 | 选择角色、组装输入、授权工具/目录/预算，记录事件。 | **已存在**：Worker 执行任务；**已存在**：WorkerFlow 临时 agent。 | **需要 Adapter**：生成可执行任务和收集结果。 |
| Workspace | **需要新增**：Workspace Binding、轮次、基线、write set 和单写 lease。 | **已存在**：任务 workspace 与 WorkerFlow shared run 目录，但语义不同。 | MVP 将 Threadmill workspace 映射为受控目录；不把共享目录本身当作 Binding。 |
| Context | **需要新增**：Context Graph、Context Slice、检索、订阅、Delta 与 Memory Candidate 准入。 | 无对应 Context Graph。 | 通过 Threadmill MCP 注入只读上下文工具/快照。 |
| Artifact / evidence | **需要新增**：注册、hash、权限、ArtifactRef、来源链和交付校验。 | **已存在**：文件同步/发布。 | Adapter 只把受控路径注册后返回 ArtifactRef。 |
| 通过与 done | **需要新增**：授权方判定 endpoint satisfied、invalidated、Task done/merge。 | `submit_task` 仅记录 Worker 提交；`check_task` 不能替代 Threadmill DeliveryPolicy。 | AgentTeams 的成功状态绝不直接等于 Phase 通过或 Task done。 |

## 3. Adapter 的落点

```text
Task Manager / Coordination Graph          Context Service
              |                                  |
              +------ Threadmill Agent Runtime --+
                         |
             AgentTeamsHostAdapter  <--- 唯一适配层
                    |                   |
       TeamHarnessTaskflowAdapter   WorkerFlowAdapter（可选）
                    |                   |
             AgentTeams MCP / QwenPaw runtime
```

| 位置 | 决策 | 标记 | 原因 |
| --- | --- | --- | --- |
| Threadmill Agent Runtime 下 | 放置 `AgentTeamsHostAdapter`。 | **需要新增** | Runtime 已是 Invocation、权限、Workspace、结果路由的执行边界。 |
| Task Manager 内 | 不放 adapter。 | **暂不实现** | 否则 Task Manager 会耦合 AgentTeams 任务 ID、房间和 Worker 状态，破坏唯一写图边界。 |
| Phase Agent prompt/skill 内 | 不放 adapter。 | **暂不实现** | Agent 只能调用 `awaitInputs`、提交 output/proposal；不得自行 delegate、改图或决定完成。 |
| AgentTeams TeamHarness 插件内 | 不修改其领域模型。可在部署侧注册一个 `threadmill-phase` MCP。 | **需要 Adapter** | 保持 AgentTeams 可独立演进，Threadmill contract 由自己的 MCP/Runtime 持有。 |

## 4. 生命周期映射

| Threadmill 生命周期 | AgentTeams 映射 | 标记 | 关键约束 |
| --- | --- | --- | --- |
| Scheduler 选中 runnable endpoint | Adapter 创建 `agentteams_task_id`，调用 `delegate_task`。 | **需要 Adapter** | task ID 与 `invocation_id` 一对多：一次恢复可产生新 task ID。 |
| `runtime.startPhase(StartPhaseInput)` | 将输入投影写为 `spec.md` + `phase-envelope.json`，Worker 收到 `TASK_ASSIGNED`。 | **需要 Adapter** | spec 是载体，不是 Threadmill Contract 的权威副本。 |
| Worker 开始 | Worker 调用 `ack_task`。 | **已存在** | Adapter 记录为 `running`；不能据此改变 endpoint 状态。 |
| Phase 工作中 | Worker 使用 Threadmill 注入 MCP：context 只读、artifact register、proposal、requirement、memory candidate。 | **需要新增** | 这些工具不属于 TeamHarness/WorkerFlow。 |
| 已知 completion input 不足 | Agent 通过受限 MCP 调用 `runtime.awaitInputs`；Runtime 持久化 continuation 并回收当前 AgentTeams carrier。 | **M4 已落地** | AgentTeams 没有等价的持久、无占用等待/恢复；新正式输入形成新的不可变 BindingRef 与新 task/Worker epoch。 |
| 正式完成 | Agent 通过受限 MCP 调用 `agent.submitPhaseOutput`；`result.md` / `submit_task` 若存在只保留为 physical execution evidence。 | **M3.8 / M4-E3 已落地** | `submit_task` 成功不等于 `PhaseOutput` 被接受。 |
| 输出被接受 | Runtime 校验当前 trusted binding、输入 revision 和 Artifact ownership，持久化 PhaseOutput / `PhaseOutputSubmitted`，随后正常回收 physical carrier。 | **M4-E3 已落地** | 不调用 AgentTeams project acceptance 作为权威。 |
| 取消/超时/lease 失效 | Runtime 停止执行并调用 `cancel_task`（可用时），撤销 token/写 lease。 | **需要 Adapter + 需要新增** | `cancel_task` 已有；lease 与强制停止语义未在 AgentTeams 中确认。 |
| Phase 内部并行（非 MVP） | 当前 Worker 调用 `workflow_run`，自行 `workflow_update`、合并和清理 tmp agents。 | **需要 Adapter** | 其子 agent 只产生 phase 内辅助材料；最终仍由父 Worker 提交一次 PhaseOutput。 |

## 5. 必须新增的接口

### 5.1 Threadmill Phase Runtime MCP（Phase Agent 可见）

以下为目标接口集；其中 `runtime.awaitInputs`、`artifact.register`、`agent.submitPhaseOutput` 和 `runtime.confirmPackageConsumption` 已由当前 Phase MCP 实现，其余仍按各自 milestone 演进。所有接口均由 Runtime 校验 Invocation token、角色、输入 revision、权限和 lease：

```text
runtime.awaitInputs({ inputIds? }) -> InputWaitResult
agent.submitPhaseOutput({ phase, deliveryRefs, reportRef, evidenceRefs }) -> Accepted
agent.proposeOrchestration({ proposalId, clientRef, ..., advice, evidenceRefs }) -> Accepted
agent.submitRequirement({ text, goal?, constraints?, evidenceRefs? }) -> Accepted
agent.submitMemoryCandidate({ statement, kind, sourceRefs, whyReusable }) -> Accepted
artifact.register({ controlledPath, kind, mediaType? }) -> ArtifactRef
context.listSubgraphs / explore / retrieve / subscribe -> Threadmill context responses
```

`agent.submitPhaseOutput` 的 binding（TaskID、ContractRef、WorkspaceRef、InputRevision、ContextSliceRef、WorkspaceHead）必须由 Runtime 补入，不能让 Worker 填写。

### 5.2 Runtime 内部接口

| 接口 | 标记 | 用途 |
| --- | --- | --- |
| `AgentTeamsHostAdapter.dispatch(invocation)` | **需要新增** | 将一个 runnable Invocation 变为 TeamHarness task。 |
| `collectResult(agentteamsTaskID)` | **需要 Adapter** | 读取 `result.md`、受控 deliverables 和 taskflow meta，返回未信任的 `AgentTeamsResult`。 |
| `cancel(invocation, reason)` | **需要 Adapter** | 转换为 taskflow cancel 并执行 Threadmill 本地清理。 |
| `resumeWaitingInvocation(invocation)` | **需要新增** | 输入 revision 改变后创建新的 AgentTeams execution task。 |
| `PolicyEnforcer.issue/revoke` | **需要新增** | 签发短期 MCP token、工具 allowlist、目录许可和写 lease。 |
| `ArtifactStore.register/resolve` | **需要新增** | 将受控路径转换为可审计 ArtifactRef。 |

## 6. StartPhaseInput 到 AgentTeams task 的转换

**推荐载体：TeamHarness taskflow（MVP）。** 不直接把 StartPhaseInput 映射为 TeamHarness 项目计划或 WorkerFlow DAG。

| StartPhaseInput 字段 | AgentTeams task 字段/文件 | 标记 | 规则 |
| --- | --- | --- | --- |
| `InvocationID` | `phase-envelope.json.invocationId`、关联表 | **需要 Adapter** | 不使用 task ID 替代 Invocation ID。 |
| `EndpointRef`, `TaskID`, `Phase` | `spec.md` 标头、envelope | **需要 Adapter** | 只读；Worker 无权改写。 |
| `ContractRef` | `contract_snapshot_ref`（或摘要 + URI） | **需要 Adapter** | Contract 正本留在 Threadmill。 |
| `WorkspaceRef` | Runtime 已绑定的 workspace root/挂载 + envelope | **需要 Adapter** | 按 phase 发放只读或允许写目录。 |
| `ContextSliceRef` | envelope + 初始 slice 摘要；MCP token | **需要新增** | AgentTeams 无 ContextSlice。 |
| `Inputs.Required/Delivered/Pending` | envelope 的 `inputs` 只读 JSON；已交付 artifact 为 ArtifactRef/受控只读挂载 | **需要新增** | 不暴露 source 的未提交 workspace 或过程上下文。 |
| token、deadline、allowlist、lease | 私有 runtime envelope / MCP policy | **需要新增** | QwenPaw 有 MCP policy API，但未证实此组合可按 Invocation 强制执行。 |

建议的任务目录：

```text
shared/tasks/{agentteams-task-id}/
  spec.md                         # 给 Worker 的可读任务说明
  threadmill/phase-envelope.json  # Runtime 生成，只读
  workspace/                      # 映射的受控 Workspace Binding
  progress/                        # Worker 临时记录；不作跨阶段输入
  result.md                        # Worker 的人类可读报告
  deliverables/                    # 仅受控可写产物
```

`spec.md` 必须明确：不得编辑 `phase-envelope.json`；不得使用 TeamHarness `projectflow` 或自行 `delegate_task`；只可通过 Threadmill MCP 提交 PhaseOutput/提案；路径先注册才能成为交付引用。

## 7. AgentTeams result 与 PhaseOutput 的边界

| AgentTeams 结果 | Threadmill 转换 | 标记 |
| --- | --- | --- |
| `submit_task.status=SUCCESS` 或 `SUCCESS_WITH_NOTES` | 仅作为“Worker 已提交”的候选信号。 | **需要 Adapter** |
| `result.md` | 人类可读报告或 physical execution evidence；不自动注册或映射为正式 PhaseOutput。 | **已明确边界** |
| `deliverables[]` | 可作为候选文件，仍须 Agent 通过 `artifact.register` 取得受控 ArtifactRef。 | **M3.8 已落地** |
| 运行日志、测试输出、diff | 可注册为 `EvidenceRefs`，但 Runtime 不信任任意路径。 | **M3.8 已落地** |
| `REVISION_NEEDED` / `BLOCKED` | 转为结构化失败/阻塞证据；必要时要求 Worker 提交 Proposal。 | **需要 Adapter** |
| `PhaseOutput` | Agent 调用 `agent.submitPhaseOutput`；Runtime 以权威 binding 校验 completion inputs、InputRevision、Artifact ownership 和 lease 后接受。 | **M4-E3 已落地** |

正式输出路径：

```text
artifact = artifact.register(controlledPath)
agent.submitPhaseOutput({ phase, deliveryRefs, reportRef, evidenceRefs })
runtime.acceptPhaseOutput(currentTrustedBinding, candidate)
```

若缺 report、未满足 `DeliverySpec`、completion input 仍 pending、输入已过期或写 lease 已失效，则 Runtime 拒绝正式提交；不将 AgentTeams 的“SUCCESS”升级为 Phase 成功。

## 8. Context / Input / Artifact 接入

| 面 | 实施方案 | 标记 |
| --- | --- | --- |
| Context 初始注入 | Runtime 选择 Context Slice，把小摘要放入 spec/envelope；完整探索与检索经 `threadmill-ctx` MCP。 | **需要新增** |
| Context 更新 | Context subscription executor 向有效 Invocation 推送 Delta；若处于 waiting，下次恢复时重新装配 slice。 | **需要新增** |
| 正式 Input | Coordination Graph 生成 `PhaseInputSet`。已交付内容只能以 `PhaseOutputRef + ArtifactRef` 投影到 envelope/read-only mount。 | **需要新增** |
| 已知输入等待 | 调用 `runtime.awaitInputs` 后回收本次 AgentTeams carrier；Runtime 保留 waiting 记录，不保留 Worker 线程，并以新 epoch 重新承载。 | **M4 已落地** |
| 未知前置 | `agent.proposeOrchestration(advice=dependency)`；只有 Task Manager 改图后才形成新 Input。 | **需要新增** |
| 产物生成 | Worker 写入受控目录；TeamHarness 文件共享可作为运输通道。 | **已存在 + 需要 Adapter** |
| 产物正式化 | Runtime 路径验证、hash、存储、权限和 ArtifactRef 注册。 | **需要新增** |

## 9. 可直接复用、不可复用和缺口

### 可直接复用

- **已存在**：TeamHarness `delegate_task` / `ack_task` / `submit_task` / `cancel_task` 的任务执行握手。
- **已存在**：Worker-owned `result.md`、`workspace/`、`progress/` 和提交 deliverables 的文件约定。
- **已存在**：TeamHarness 文件同步与 Artifact 发布能力，可用于传输或人工可见性。
- **已存在**：WorkerFlow 的临时 QwenPaw agent、run 级共享目录、DAG ready/unblock、结束清理。
- **已存在**：QwenPaw 的 MCP 管理、MCP tool 列表/policy 配置、Agent 配置和启停 API。

### Threadmill 必须补充

- **需要新增**：Task Contract、Requirement 规整、Task/round、Phase Endpoint、Coordination Graph 和唯一写图的 Task Manager。
- **需要新增**：StartPhaseInput、PhaseInputSet、InputWaitResult、PhaseOutput 和 result binding/revision 校验。
- **需要新增**：Runtime 的等待/恢复、lease、预算、取消、可观测和 provider-neutral host abstraction。
- **需要新增**：Workspace Binding、基线/轮次隔离、AllowedDirs、Declared/Observed WriteSet。
- **需要新增**：Artifact Store 与 DeliverySpec/ReportSpec 校验。
- **需要新增**：Context Graph、Context Slice、读接口、订阅 Delta、Memory Candidate 准入。
- **暂不实现**：把 TeamHarness Matrix 消息作为 Phase Agent 间 mailbox；Threadmill 明确禁止该旁路通信。
- **暂不实现**：把 WorkerFlow 的 DAG 持久化为 Threadmill Coordination Graph；它是当前 Worker 内部实现细节。
- **暂不实现**：MVP 的复杂 Context Graph 缓存、读侧整理和完整推送策略；先支持初始 slice 与显式检索即可。

## 10. 分阶段开发计划

| 阶段 | 范围 | 交付 | 标记 |
| --- | --- | --- | --- |
| P0：契约骨架 | 定义 Threadmill domain types、SQLite/事件存储、ArtifactRef、Invocation↔AgentTeams task 关联。 | 可持久化 StartPhaseInput / PhaseOutput binding；无真正调度。 | **需要新增** |
| P1：MVP 执行 | 一个 endpoint → 一个 TeamHarness taskflow worker；生成 spec/envelope，`ack`/`submit`，受控 result 转换。 | `plan -> execute -> verify` 串行 happy path；人工批准 plan/verify。 | **需要 Adapter + 需要新增** |
| P2：输入与 Workspace | Coordination Edge 投影为 PhaseInputSet；Artifact register；Workspace Binding、AllowedDirs、基础 revision 校验。 | 跨 phase 正式 artifact 输入，拒绝越权路径/过期输入。 | **需要新增** |
| P3：暂停恢复与失败 | `awaitInputs`、等待记录、输入到达后新 invocation；proposal/requirement；cancel/retry。 | 不占 Worker capacity 的 join；Task Manager 裁决。 | **需要新增** |
| P4：Context | Context Slice、`list/explore/retrieve`、MemoryCandidate；先不做 Delta 推送。 | 每次 phase 有受控上下文读面。 | **需要新增** |
| P5：完整控制 | Context subscriptions/Delta、lease 强制、预算、写集审计、Merge Queue 与 DeliveryPolicy。 | 完整 Threadmill 生命周期。 | **需要新增** |
| P6：WorkerFlow 加速 | 在 execute/verify 内允许临时 subagents；父 Worker 统一合并。 | 可选 fan-out，失败也不影响 PhaseOutput 单一边界。 | **需要 Adapter** |

## 11. 时序图（MVP + 等待恢复）

```mermaid
sequenceDiagram
  participant TM as "Task Manager"
  participant CG as "Coordination Graph"
  participant RT as "Threadmill Runtime"
  participant AD as "AgentTeams Adapter"
  participant TH as "TeamHarness taskflow"
  participant W as "AgentTeams Worker"
  participant AS as "Artifact Store"

  TM->>CG: "create endpoint, contract, input edges"
  CG-->>RT: "runnable endpoint + input projection"
  RT->>RT: "assemble StartPhaseInput and policy envelope"
  RT->>AD: "dispatch(invocation)"
  AD->>TH: "delegate_task(spec.md, taskId)"
  TH-->>W: "TASK_ASSIGNED"
  W->>TH: "ack_task(taskId)"
  W->>RT: "Threadmill MCP: context/artifact/proposal"
  alt "completion input is pending"
    W->>RT: "awaitInputs(inputIds)"
    RT->>AD: "finish/release execution task"
    RT->>RT: "mark invocation waiting"
    Note over RT,CG: "Input arrives; inputRevision changes"
    CG-->>RT: "updated input projection"
    RT->>AD: "dispatch(new execution task)"
  end
  W->>AS: "artifact.register(controlled path)"
  W->>RT: "agent.submitPhaseOutput(ArtifactRefs)"
  RT->>RT: "validate trusted binding, input revision and ownership"
  RT->>AD: "normal task/worker/credential teardown"
  RT-->>TM: "accepted PhaseOutput or rejection"
  TM->>CG: "satisfy, reject, invalidate, or replan endpoint"
```

## 12. 目录结构建议

建议在 Threadmill 代码库中新建以下边界清晰的模块；不修改 AgentTeams 上游的核心领域模型。

```text
threadmill/
  domain/
    contracts.py                 # Requirement, TaskContract, Delivery/ReportSpec
    coordination.py              # Task, PhaseEndpoint, CoordinationEdge
    phase_types.py               # StartPhaseInput, PhaseInputSet, PhaseOutput
    workspace.py                 # WorkspaceBinding, WriteSet, Lease
  runtime/
    invocation_service.py        # start/stop/wait/resume
    policy_enforcer.py           # token, tools, dirs, budgets
    output_validator.py
    event_log.py
  artifact/
    store.py
    registry.py
  context/
    slice_service.py             # P4
    graph_service.py             # P4/P5
    subscription_service.py      # P5
  adapters/
    agentteams/
      host_adapter.py            # provider-neutral facade implementation
      taskflow_adapter.py        # delegate/ack/submit/cancel mapping
      workerflow_adapter.py      # P6 only
      result_parser.py
      task_spec_renderer.py
      qwenpaw_policy_adapter.py
      correlation_store.py
  mcp/
    phase_runtime_server.py      # injected agent-facing Threadmill tools
  tests/
    contract/
    integration/agentteams/

deploy/
  agentteams/
    threadmill-phase-mcp.yaml    # deployment registration, not domain contract
    worker-phase-skill.md        # worker operating instructions
```

`adapters/agentteams/` 只能依赖 `domain/` 的公开类型与 `runtime/` 端口；`domain/` 绝不能导入 TeamHarness、QwenPaw、Matrix 或 WorkerFlow。这样日后替换 AgentTeams 承载时不会改变 Threadmill 的 Task Contract、图、Context 或 Phase 接口。

## 13. 验收门槛

MVP 上线前至少验证：

1. Worker 不能通过篡改 `spec.md`/envelope 把输入 revision、ContractRef 或 Phase 改成其他值。
2. `SUCCESS` 但没有合规 report/delivery 的结果被 Runtime 拒绝。
3. waiting 后旧 AgentTeams task 无法继续写入；恢复后的新 execution task 使用最新 InputRevision。
4. 上游未提交 workspace 文件不能作为下游输入；只有注册后的 ArtifactRef 可见。
5. WorkerFlow 子 agent 的输出只能先归入父 phase 的受控目录，不能直接提交或满足 Threadmill endpoint。
6. Task Manager 是唯一能使 endpoint satisfied、重排或标记 Task done 的组件。

## Activation versus package consumption

The adapter delegates the complete rehydrated package through the existing TeamHarness task specification. Matrix carries a truncated assignment preview; `ack_task` pulls and returns the complete `spec.md`. The adapter retains the Controller's real room identity as `matrix:<room-id>` and never manufactures a `qwenpaw://` success URI.

Package consumption is not Phase completion. The fresh agent must separately invoke Threadmill `agent.submitPhaseOutput`; the Runtime validates the current Task/Invocation/Generation/Epoch/BindingRef/InputRevision, persists the formal output and authoritative event, and only then performs normal AgentTeams task/Worker/credential cleanup. `cancel_task` is currently the AgentTeams reclamation primitive for both rollback and completed-task cleanup, but the adapter uses distinct reasons and does not classify accepted output as rollback.

Two independent gates are required. TeamHarness assignment/acknowledgement proves physical activation. The Threadmill Phase MCP tool `runtime.confirmPackageConsumption` proves that the fresh session parsed the authoritative package. Runtime validates that call with token-bound logical identity plus the epoch-aware physical record and stores immutable per-epoch evidence. Neither gate substitutes for the other, and neither carries secret transport state.

The focused M4-E4 path also exercises the upstream half of this sequence: a real Epoch-A QwenPaw agent calls `runtime.awaitInputs` through the projected private-header MCP client; Runtime cancels task-A, deletes Worker-A, revokes credential/token-A and releases lease-A before accepting a newer formal input projection. Epoch-B is then reconstructed and provisioned through the existing Controller/TeamHarness path. The old room/provider conversation is not an input to reconstruction.
