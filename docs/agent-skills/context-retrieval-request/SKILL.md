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
2. Query 明确写出要找的实体、约束、模块或关系，并显式给出一个预期会原样存在于节点 Statement 的“机械关键词”，例如“查找 Context Graph 节点粒度的可复用判断；机械关键词：granularity”。机械关键词不得使用 TaskID、ProjectID、InvocationID、来源任务名或 `upstream-memory` 这类任务标签。复杂问题由原 Consumer 拆成多次 `contextAgent.retrieve`，每次只找一个原子主题，不夹带图写请求、角色切换或隐藏身份字段。多次 retrieve 必须串行执行：等待前一次返回后再决定是否需要下一次，禁止并行占用同一 Context host。每个 Phase 开始时先发起一次最高价值的原子查询。Context Agent 的每次 retrieve Invocation 只执行一次最终 Search。
3. 若返回空 Slice，先读取 Explanation 中实际使用的关键词，再以不同的业务主题词串行重试，最多三次；不得改用任务标识、空关键词或同一句话的包装性改写。每次重试都必须缩小到一个可能命中原子节点的概念，例如按 `granularity`、`precision`、`Search` 分开检索。
4. 接收 ContextRetrieveResult.Slice、SubscriptionIDs 和 Explanation。只使用返回的可见结果，不补写未命中事实。
5. Search 自动订阅绑定当前原请求方 Invocation，不绑定执行检索的 Context Agent。SubscriptionIDs 交由 context-subscription 管理。
6. 在后续 Memory Candidate 的 `source_refs` 中用 `node:<NodeID>` 记录真正影响判断的 NodeRef，并保留 evidence/SourceRef；PhaseOutput 通过 EvidenceRef 引用包含这些精确引用的正式证据。用结果改变了方案时说明改变了什么。Runtime 会校验节点属于当前有效订阅并集，不能只证明“调用过检索”。

## 禁止

- 普通 Agent 不持有 context.search，不得把自然语言 Query 自行伪装成 SearchRequest。
- 检索结果不是正式 Task 输入，也不改变 Coordination Graph。
