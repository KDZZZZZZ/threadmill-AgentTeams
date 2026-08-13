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

- `context.registerTaskSubgraph`
- `context.projectTaskContext`
- `context.finalizeTaskMemory`

## 注册与投影

1. Task 创建成功后显式 registerTaskSubgraph。只传当前 TaskID；SubgraphID 由 Context Service 决定。
2. Requirement、Task Contract、DeliverySpec、ReportSpec 的权威投影使用 directive。
3. 已接受的 PhaseOutput、交付物、报告和验证 evidence 的权威投影使用 fact。
4. task 要求不得写成 hypothesis；不复制 runnable、waiting、blocked 等易变运行状态。
5. 节点只引用权威来源，不复制大段 artifact 内容。

## done 与终审

1. 只有 coordination-control 已成功持久化权威 done，才能 finalizeTaskMemory。
2. 首次调用冻结当前缓冲为 frozen-unreviewed；失败重试同一批次，不重新收集候选。
3. 终审失败不回滚 Task done；reviewed 后同一 TaskID 幂等返回既有回执。

## 失败

Graph mutation 与 Context 投影不是跨服务事务。图成功、投影失败时，只以相同 ProjectionID/SourceRevision 重试投影，不撤销已生效图决定。
