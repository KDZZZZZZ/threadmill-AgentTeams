---
name: task-memory
description: 读取当前 Task 的候选记忆，并追加可能跨 Task 复用的 general 知识候选。Phase Agent 需要共享阶段知识时使用。
---

# task-memory

## 目的

读取当前 Task 的 append-only 候选缓冲，并提交可能跨 Task 复用的 general 知识候选。

## 工具

- `agent.listTaskMemoryCandidates`
- `agent.submitMemoryCandidate`

## 读取

1. TaskID 由 Runtime 绑定，不指定其他 Task。
2. plan、execute、verify 共享同一缓冲；后阶段可见前阶段候选。
3. 候选不是 ContextNode，不参与 Explore/Search/Subscribe，不改变 graph revision。

## 提交

提交结构只有：

```json
{
  "statement": "单一、可独立理解的知识陈述",
  "kind": "directive | fact | hypothesis",
  "source_refs": ["已注册 evidence"],
  "subgraph_ids": ["建议 general 子图，可为空"]
}
```

至少满足一项才提交：影响后续计划/实现/验证；记录难以从代码恢复的决定与理由；保存已验证接口、约束或运行事实；给出可复现失败模式及规避；保存稳定项目偏好；连接已有 general 子图；修正旧节点。

默认不提交：临时进度、计划或报告全文、单次命令输出、可从代码廉价恢复的细节、无 evidence 主张、未区分事实与假设的推测、敏感信息或 task 子图目标。

提交成功只表示候选进入当前 Task 缓冲，不表示已接受或已落图。
