# Context Agent 定义与工具

版本：v0.1  
状态：Draft  
定位：本文只定义 Context Agent 及其所需工具。Context Graph 对象字段以 [context-graph.md](./context-graph.md) 为准。

---

## 1. 定义

**Context Agent** 是 Threadmill 中负责 Context Graph 语义检索、受控探索、`general` 节点管理和 `general` 子图管理的 Agent。

Context Agent 可以：

1. 响应其他 Agent 的自然语言记忆检索请求；
2. 使用列表、探索和机械检索理解当前可见 Context Graph；
3. 对 `general` 节点执行创建、读取、更新和删除；
4. 对 `general` 子图执行创建、读取、更新和删除；
5. 审查 Task 达到权威 `done` 后冻结的 general MemoryCandidate 批次。

Context Agent 是临时 Agent Invocation，不拥有图存储。所有工具调用都经过 Agent Runtime 和 Context Service；身份、权限、预算和 graph revision 由 Runtime 注入。Context Service 负责权限校验、revision 校验、原子 mutation、审计和订阅推送。

Context Agent 不能：

- 读取或修改 `task` 子图及其专属节点归属；
- 绕过 Context Service 访问图存储；
- 修改 Coordination Graph、Task Contract、Task 状态或 Workspace；
- 管理 Task Memory Buffer 的写入、冻结或生命周期；
- 主动无界巡图、主动提示 Agent、执行订阅或推送。

---

## 2. 工具包装

Context Agent 使用的接口、请求/响应字段和校验规则全部由 [context-graph.md §6](./context-graph.md) 定义。本文不另立 DTO 或服务接口；`threadmill-ctx` MCP server 只把 Graph seams 包装成 Agent 可调用工具。

| MCP 工具 | Graph 权威方法 | 包装职责 |
| --- | --- | --- |
| `context.listSubgraphs` | `ContextGraphReader.ListSubgraphs` | 原样转发 `ListSubgraphsRequest`，返回可见 `ContextSubgraph[]` |
| `context.getSubgraph` | `ContextGraphCurator.GetSubgraph` | 原样转发 `GetSubgraphRequest` |
| `context.getNode` | `ContextGraphCurator.GetNode` | 原样转发 `GetNodeRequest` |
| `context.explore` | `ContextGraphReader.Explore` | 原样转发 `ExploreRequest` |
| `context.search` | `ContextGraphSearcher.Search` | 原样转发 `SearchRequest`；只注入 Context Agent |
| `context.createNode` | `ContextGraphCurator.CreateNode` | 原样转发 `CreateGeneralNodeRequest` |
| `context.updateNode` | `ContextGraphCurator.UpdateNode` | 原样转发 `UpdateGeneralNodeRequest` |
| `context.deleteNode` | `ContextGraphCurator.DeleteNode` | 原样转发 `DeleteGeneralNodeRequest` |
| `context.createSubgraph` | `ContextGraphCurator.CreateSubgraph` | 原样转发 `CreateGeneralSubgraphRequest` |
| `context.updateSubgraph` | `ContextGraphCurator.UpdateSubgraph` | 原样转发 `UpdateGeneralSubgraphRequest` |
| `context.deleteSubgraph` | `ContextGraphCurator.DeleteSubgraph` | 原样转发 `DeleteGeneralSubgraphRequest` |
| `context.submitReview` | `ContextCandidateReviewer.SubmitReview` | 原样转发 `CandidateReviewSubmission` |

包装层必须保持薄：

- 不重命名字段、不增加默认业务值、不把多个 Graph mutation 拼成隐藏工作流；
- Runtime 只附加可信 Invocation、权限、预算、原请求方 consumer binding 和 revision 上下文；这些字段不暴露给 Agent 自报；
- Graph 接口返回的权限拒绝、revision 冲突和校验错误原样返回；工具层不得降级为成功或做部分重试；
- `context.search` 命中的自动订阅绑定自然语言检索的原请求方 Invocation，不绑定 Context Agent；
- 所有写工具只包装 Context Service mutation，不直连存储。task 子图及其节点的拒绝规则由 `ContextGraphCurator` 强制执行。

### 2.1 工具选择

- 列表/探索：`listSubgraphs`、`getSubgraph`、`getNode`、`explore`；
- 机械检索：`search`；
- general 节点管理：`createNode`、`updateNode`、`deleteNode`；
- general 子图管理：`createSubgraph`、`updateSubgraph`、`deleteSubgraph`；
- 冻结候选批量审查：`submitReview`。

Context Agent 可组合多个只读调用形成调查，但每个 mutation 工具始终对应一次同名 Graph 方法调用。子图拆分、合并或批量迁移没有独立工具；需要时由 Context Agent 发出一组显式、逐次受 revision 保护的 CRUD 调用。

---

## 3. 自然语言检索接口

其他 Agent 在列表与探索不足时调用：

```go
type ContextAgent interface {
    Retrieve(ctx context.Context, req ContextRetrieveRequest) (ContextRetrieveResult, error)
}

type ContextRetrieveRequest struct {
    Query string `json:"query"`
}

type ContextRetrieveResult struct {
    Slice           ContextSliceDelta `json:"slice"`
    SubscriptionIDs []string          `json:"subscription_ids"` // 为原请求方建立的自动订阅句柄
    Explanation     string            `json:"explanation"`
}
```

Context Agent 将 Query 转换为 SearchRequest 后调用 `context.search`。Runtime 将 Search 自动订阅绑定到原请求方 Invocation，不绑定 Context Agent，并把 `ContextSearchResult.SubscriptionIDs` 原样返回原请求方，使 Phase Agent 或 Task Manager Agent 可以通过 `context.unsubscribe` 取消。返回内容必须来自可见检索结果，不得凭模型记忆补写项目事实。

---

## 4. 工具清单

Context Agent 需要以下工具：

```text
context.listSubgraphs
context.getSubgraph
context.getNode
context.explore
context.search
context.createNode
context.updateNode
context.deleteNode
context.createSubgraph
context.updateSubgraph
context.deleteSubgraph
context.submitReview
```

所有写工具都是 Context Service 的受控 mutation seam，不是图存储直连。Context Agent 不持有 `TaskContextWriter`、Task Memory Buffer 写工具、Coordination Graph 写工具、推送工具或订阅执行工具。
