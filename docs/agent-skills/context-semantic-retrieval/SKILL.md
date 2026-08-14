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
2. 读完权威 invocation spec 后立即构造 SearchRequest。只有 Query 已携带真实 general NodeRef/SubgraphRef、且机械 Search 需要校验该锚点时，才先用 getSubgraph/getNode；不得先扫描仓库、Task 文件、历史或 provider memory 来猜关键词。
3. 生成 SearchRequest，只含：
   - Keywords：确定性匹配关键词；
   - Scope：子图、模块、symbol 等当前 schema 支持的范围；
   - AnchorRefs：已有 node: 或 subgraph: 引用。
   Keywords 使用 AND 字面子串匹配。Query 显式给出“机械关键词”时优先原样采用其中最具体的一个；否则只取一个预期会原样存在于节点 Statement 的稳定实体名、接口名或核心术语。不得选择 TaskID、ProjectID、InvocationID、请求来源、任务标签，或“上游/既有/相关结论”之类包装词。不要把整句摘要或同义改写当成关键词。
4. 不加入 Intent、隐藏推理、原请求方身份或自造过滤字段。
5. 调用 context.search。Search 是机械匹配，不是第二次 LLM 判断。
6. 返回 ContextRetrieveResult：Slice 原样使用 ContextSearchResult.Slice；SubscriptionIDs 原样返回；Explanation 说明 Query 转换、命中和未命中内容。

## 订阅与权限

Search 自动订阅绑定自然语言检索的原请求方 Invocation，不绑定 Context Agent。Context Agent 不保存、取消或复用这些 SubscriptionIDs。

结果为空时返回空 Slice、实际 Keywords 和原因，让原 Consumer 能用另一个业务主题词串行发起新的 retrieve。不得在同一 Invocation 内自行改写重试，不得凭模型常识生成项目事实，也不得为了“有帮助”创建节点。
