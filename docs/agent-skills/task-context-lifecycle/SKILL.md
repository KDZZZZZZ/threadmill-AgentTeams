---
name: task-context-lifecycle
description: 注册 task 子图、投影权威上下文，并在 Task done 后触发候选记忆终审。Task Manager 管理 Task 上下文生命周期时使用。
---

# task-context-lifecycle

## 目的

在 Coordination Graph 权威决定生效后，维护该 Task 的定向 Context 投影和 done 后候选终审。

## 依赖

- `coordination-control`

## 工具

本 Skill 不向 Task Manager Agent 暴露 Context 写工具。注册、投影和终审触发均由 Runtime 在可信图 mutation 后内部调用 Context Service。

## 注册与投影

1. `coordination.replacePending` 和受控 transition 成功后由 Runtime 以稳定 ProjectionID 自动 register/project；Task Manager Agent 不再拥有 `context.registerTaskSubgraph` 或 `context.projectTaskContext`，因此不能重复写第二套同义节点。
2. 缺失或失败投影由 Runtime 的幂等补偿路径重试原 TaskID、ProjectionID 与 SourceRevision；Agent 只报告失败，不自拟 ID 修复。
3. Requirement、Task Contract、DeliverySpec、ReportSpec 的权威投影使用 directive。
4. 已接受的 PhaseOutput、交付物、报告和验证 evidence 的权威投影使用 fact。
5. task 要求不得写成 hypothesis；不复制 runnable、waiting、blocked 等易变运行状态。
6. 节点只引用权威来源，不复制大段 artifact 内容。
7. 每条约束、验收项、Contract 决定、Phase Spec 绑定以及被接受输出中的独立结论分别投影为一个节点。禁止把完整 Requirement、Contract、JSON、报告或多个论断塞入一个 Statement。
8. ProjectionID 对应稳定语义单元；同一单元用新 SourceRevision 修订，新增单元使用新 ProjectionID，不用覆盖一条“总览节点”模拟增量。

## done 与终审

1. 只有 coordination-control 已成功持久化权威 done，Runtime 才自动调用 finalizeTaskMemory。
2. 首次调用冻结当前缓冲为 frozen-unreviewed；Runtime 失败重试同一批次，不重新收集候选。
3. 终审失败不回滚 Task done；reviewed 后同一 TaskID 幂等返回既有回执。
4. 候选不允许长期停留在 frozen-unreviewed；Runtime 必须以同一批次重试真实 Context Agent review，直到 reviewed 或形成明确不可恢复失败。

## 失败

Graph mutation 与 Context 投影不是跨服务事务。图成功、投影失败时，只以相同 ProjectionID/SourceRevision 重试投影，不撤销已生效图决定。
