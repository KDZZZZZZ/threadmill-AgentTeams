---
name: context-semantic-retrieval
description: 把自然语言 Context Query 转换为受权限约束的机械 Search。Context Agent 执行 retrieve operation 时使用。
---

# context-semantic-retrieval

## 目的

把 ContextRetrieveRequest.Query 转成 SearchRequest，并把可见结果原样返回原请求方。

## 依赖

- `context-navigation`

## 工具

- `context.getSubgraph`
- `context.getNode`
- `context.search`

## 流程

1. 明确 Query 要找的实体、约束、模块或关系。不要直接回答业务问题。
2. 使用 context-navigation，必要时 getSubgraph/getNode，理解可见 general 图和锚点。
3. 生成 SearchRequest，只含：
   - Keywords：确定性匹配关键词；
   - Scope：子图、模块、symbol 等当前 schema 支持的范围；
   - AnchorRefs：已有 node: 或 subgraph: 引用。
4. 不加入 Intent、隐藏推理、原请求方身份或自造过滤字段。
5. 调用 context.search。Search 是机械匹配，不是第二次 LLM 判断。
6. 返回 ContextRetrieveResult：Slice 原样使用 ContextSearchResult.Slice；SubscriptionIDs 原样返回；Explanation 说明 Query 转换、命中和未命中内容。

## 订阅与权限

Search 自动订阅绑定自然语言检索的原请求方 Invocation，不绑定 Context Agent。Context Agent 不保存、取消或复用这些 SubscriptionIDs。

结果为空时返回空 Slice 和原因。不得凭模型常识生成项目事实，也不得为了“有帮助”创建节点。
