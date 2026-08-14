---
name: general-context-curation
description: 创建、更新或删除 general Context 节点与子图。Context Agent 执行 curate operation 时使用。
---

# general-context-curation

## 目的

按当前 curate 请求，对 general 节点和 general 子图执行显式、逐次、受 revision 保护的 CRUD。

## 依赖

- `context-navigation`

## 工具

- `context.getSubgraph`
- `context.getNode`
- `context.createNode`
- `context.updateNode`
- `context.deleteNode`
- `context.createSubgraph`
- `context.updateSubgraph`
- `context.deleteSubgraph`

## 流程

1. 先读取目标及当前 revision，确认它存在、可见、可写且不属于任何 task 子图。
2. Node Kind 只能是 directive、fact、hypothesis。create/update 的 SourceRefs 必须非空、可读并直接支持 Statement。
3. hypothesis 不能承载用户 Requirement 或任务契约。
4. update 的 Status 只能是 accepted、disputed、superseded、outdated。
5. SubgraphIDs 和 NodeIDs 只能引用可写 general 对象。
6. update/delete 使用工具要求的当前 SourceRevision 或 subgraph Revision，不猜 revision。
7. CreateGeneralSubgraphRequest 允许空 NodeIDs；UpdateGeneralSubgraphRequest.NodeIDs 是完整成员集合，不是增量 patch，修改前先读取当前集合。
8. 每个 mutation 工具只执行一次同名 Graph 方法。多步拆分、合并或迁移要逐次执行，每步后读取新 revision。
9. revision conflict 时停止当前写序列，重新读取并重新判断；不盲重试旧载荷。
10. 删除节点前说明理由和影响。node delete 会原子移除 general 归属和关联边；subgraph delete 只移除成员归属及相关 derives_from_subgraph 边，不删除成员节点。

任何 task 子图、属于 task 子图的节点、不可见对象或越权目标都必须拒绝。不得从错误差异推断隐藏对象是否存在。
