---
name: context-subscription
description: 在 Task Manager 或 Phase Agent Invocation 中建立、取消和合并 Context 订阅，并处理 ContextDelta。需要跟踪后续图变化或释放订阅时使用。
---

# context-subscription

## 目的

管理当前 Task Manager 或 Phase Invocation 的显式订阅，并正确处理 ContextDelta。

## 依赖

- `context-navigation`

## 工具

- `context.subscribe`
- `context.unsubscribe`

## 流程

1. 只订阅会影响当前工作判断的可见 SubgraphIDs。EventKinds 为空表示全部可见变更。
2. 保存工具返回的 ContextSubscription.ID。订阅只属于当前 ConsumerInvocationID，不能替其他 Invocation 管理。
3. 当前上下文范围是所有有效订阅 SubgraphIDs 的去重并集，包含初始、检索自动订阅和显式订阅；不要把最后一次订阅当成完整范围。
4. 取消订阅时，只使用初始 ContextSlice、显式 subscribe 或 contextAgent.retrieve 返回给当前 Invocation 的 ID。
5. 取消成功后，Runtime 在下一次模型调用、等待重承载或 resume 前重算并集；重叠订阅仍覆盖的子图继续保留。
6. ContextDelta 只来自有效订阅。合并可重放 changes；若它使计划或编排失效，Phase Agent 交给 orchestration-escalation，Task Manager 交给 coordination-control。两者都不从本 Skill 直接改图。

## 错误

- 未知或其他 Invocation 的 ID 返回 subscription_not_found；不得据此推断其存在性或 owner。
- 已送入当前模型调用的内容不能追溯删除；取消后不得再依赖该订阅独占内容。
