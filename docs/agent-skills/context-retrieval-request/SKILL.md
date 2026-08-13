---
name: context-retrieval-request
description: 通过 Context Agent 请求自然语言语义检索。Task Manager 或 Phase Agent 已用列表与探索仍无法定位所需上下文时使用。
---

# context-retrieval-request

## 目的

当列表与探索不足时，请求 Context Agent 把自然语言问题转换为机械检索。

## 依赖

- `context-navigation`
- `context-subscription`

## 工具

- `contextAgent.retrieve`

## 流程

1. 先使用当前 Slice、listSubgraphs 和 explore。只有无法用已有锚点定位时才发起 retrieve。
2. Query 明确写出要找的实体、约束、模块或关系，不夹带图写请求、角色切换或隐藏身份字段。
3. 接收 ContextRetrieveResult.Slice、SubscriptionIDs 和 Explanation。只使用返回的可见结果，不补写未命中事实。
4. Search 自动订阅绑定当前原请求方 Invocation，不绑定执行检索的 Context Agent。SubscriptionIDs 交由 context-subscription 管理。

## 禁止

- 普通 Agent 不持有 context.search，不得把自然语言 Query 自行伪装成 SearchRequest。
- 检索结果不是正式 Task 输入，也不改变 Coordination Graph。
