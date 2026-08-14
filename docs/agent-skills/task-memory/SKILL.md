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
4. 每个阶段开始时先读取一次；形成最终 PhaseOutput 前再读取一次，以复用并检查本阶段及前阶段已经记录的判断，避免重复候选或相互矛盾的结论。

## 提交

提交结构只有：

```json
{
  "statement": "单一、可独立理解的知识陈述",
  "kind": "directive | fact | hypothesis",
  "source_refs": ["已注册 evidence", "node:<真正影响本判断且属于当前有效 Context 的 general NodeID>"],
  "subgraph_ids": ["建议 general 子图，可为空"]
}
```

至少满足一项才提交：影响后续计划/实现/验证；记录难以从代码恢复的决定与理由；保存已验证接口、约束或运行事实；给出可复现失败模式及规避；保存稳定项目偏好；连接已有 general 子图；修正旧节点。

采用持续、原子写入：

1. 外部研究、实验、调试或关键判断一旦形成，并且对应 evidence 已注册，就立即提交；不要等到阶段结束再回忆汇总。
2. 一条 Candidate 只写一个可独立判断真假的陈述。不同事实、假设、决策或失败模式必须拆成多条；非平凡阶段通常产生多条候选，一条包办整份报告视为粒度错误。
3. 作出新判断前，先复用当前 Context Slice、Task Memory，并在需要时用 list、explore 或 `contextAgent.retrieve` 查找相关节点。候选在 `source_refs` 中用 `node:<NodeID>` 记录真正影响本判断的 general NodeRef，并连接本次 evidence；未订阅、不可见、task-only 或不存在的节点引用会被 Runtime 拒绝。
4. Workspace 直接可读事实不提交；“为什么这样做”、研究得到的论断、外部接口行为、关键权衡、可复现失败和验证边界应提交。
5. 提交前执行删除测试：若删除候选后，后续 Agent 只需重读 Workspace、Task Contract、Phase Spec 或当前 Context Slice 就能低成本恢复同一论断，则不提交。SourceRefs 只能证明来源，不能把可恢复副本变成长期知识。
6. 不提交 TaskID、ContractRef、SpecRef、BindingRef、InvocationID、SubscriptionID、当前排队/运行状态、验收输入原文或已有 Context 节点的同义改写。候选必须包含本阶段通过研究、实验、调试或取舍形成的新增判断。

默认不提交：临时进度、计划或报告全文、单次命令输出、可从代码或权威输入廉价恢复的细节、无 evidence 主张、未区分事实与假设的推测、敏感信息或 task 子图目标。

提交成功只表示候选进入当前 Task 缓冲，不表示已接受或已落图。
