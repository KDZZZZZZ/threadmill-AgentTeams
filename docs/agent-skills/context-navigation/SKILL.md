---
name: context-navigation
description: 在 Threadmill Invocation 中列出可见 Context 子图并按锚点渐进探索。由五类 Agent 在现有 Context Slice 不足、需要受权限约束的只读导航时使用。
---

# context-navigation

## 目的

在当前权限、Context Graph revision 和预算内列出可见子图，并从当前 Slice、frontier 或子图锚点渐进探索。

## 工具

- `context.listSubgraphs`
- `context.explore`

## 流程

1. 先判断当前 Context Slice 是否已足够；足够时不扩大读取范围。
2. 需要定位子图时调用 listSubgraphs。Filter 只表达可见子图过滤，不推断隐藏内容。
3. 需要展开时调用 explore，使用当前 Slice、frontier、node: 或 subgraph: AnchorRef；默认一跳，Depth 只取完成当前判断所需的最小值。
4. 对齐响应的 GraphRevision。过期结果不与新 revision 静默拼接。
5. Context Agent 只能消费 general 可见对象；若 Adapter 意外暴露 task 对象，停止读取并报告边界错误。
6. 导航的目标是减少后续 Workspace 读取：命中节点已回答问题时立即停止探索，保留 NodeRef/SourceRef 并直接用于判断；不得再到仓库寻找同一事实的另一份表述。

## 不变量

- 列表和探索是只读操作，不创建节点、边或订阅。
- 权限隐藏内容不得通过名称、摘要、路径、数量差异或 frontier 反向泄露。
- Context 不能替代 Task Contract、Phase Spec 或 PhaseInputSet。
