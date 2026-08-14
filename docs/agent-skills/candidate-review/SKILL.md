---
name: candidate-review
description: 原子审查 frozen-unreviewed Task Memory 候选批次并提交决策。Context Agent 执行 review operation 时使用。
---

# candidate-review

## 目的

对 Runtime 提供的完整 frozen-unreviewed 批次逐项裁决，并通过一次原子 SubmitReview 落图或拒绝。

## 依赖

- `context-navigation`

## 工具

- `context.getSubgraph`
- `context.getNode`
- `context.search`
- `context.submitReview`

## 审查

1. 对批次中每个 CandidateID 恰好作出一次决定，不添加、删除或跨 Task 获取候选。
2. 检查 Statement、Kind、SourceRefs 和建议 SubgraphIDs。入口硬门槛已通过，不代表值得长期保存。
3. 使用可见 general 图检查重复、冲突和可修订目标。不得读取 task 子图。
4. Action 只能是：
   - create：创建 general 节点；
   - revise：用更强证据修订已有节点；
   - supersede：候选取代已有节点；
   - dispute：候选与已有节点形成有证据争议；
   - reject：不落图，只保留审计结论。
5. revise、supersede、dispute 必须填写当前可见 TargetNodeID。SubgraphIDs 只能是可写 general 子图。
6. Statement 是可独立理解的单一陈述；Kind 与语义一致；Reason 说明 evidence、复用价值、重复/冲突判断和 action 理由。
7. 保持候选的原子粒度。多个有独立证据和复用价值的论断分别落为多个节点，不为了减少节点数把批次合并成总结；Context Graph 预期拥有大量细粒度节点。

每份 CandidateReviewDecision 只含：CandidateID、Action、Statement、Kind、SubgraphIDs、TargetNodeID、Reason。不要添加评分、置信度或图命令字段。

## 价值判断

优先保留：会改变后续计划/实现/验证的知识、难以恢复的决定理由、已验证接口与约束、可复现失败模式、稳定项目偏好、图连接或旧节点修正。

默认拒绝：临时进度、单次命令输出、TaskID/InvocationID/SubscriptionID 等运行标识、当前排队或健康状态、Task Contract/Phase Spec/Context Slice 的复述、可从 Workspace 或权威输入廉价恢复的细节、已有节点的同义改写、无新增价值近重复、事实/假设混淆、敏感信息和权威对象全文复制。带 SourceRef 只证明可追溯，不免除这些拒绝条件。

## 提交

形成覆盖整个批次的 CandidateReviewSubmission 后，只调用一次 context.submitReview。不要先用 CRUD 预写部分结果。

review 中调用 context.search 时，任何返回的 SubscriptionIDs 都由 Runtime 绑定原请求方；Context Agent 不保存、取消或复用。

SubmitReview 失败时，批次仍为 frozen-unreviewed。报告原始错误，让 Runtime 重试同一批次；不得重新收集候选或伪造 AuditRef。
