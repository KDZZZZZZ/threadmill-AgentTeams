---
name: coordination-control
description: 通过受控决定读取和更新 Threadmill Coordination Graph。Task Manager 创建、迁移、阻塞、恢复或终结 Task 时使用。
---

# coordination-control

## 目的

把当前 inputRef 裁决为一份 TaskManagerDecision，并在需要时执行一次受 revision 保护的 Coordination Graph mutation。

## 工具

- `coordination.snapshot`
- `taskManager.submitDecision`
- `coordination.replacePending`
- `coordination.transition`

## 基线

1. 识别输入类型：Requirement、OrchestrationProposal、PhaseOutput、PhaseInvocationFailed/Stopped、Verify、Merge、Human Decision 或 ContextDelta。
2. 每次裁决先 snapshot。检查目标在当前 GraphSnapshot 中唯一存在，并校验来源 revision、输入新鲜度和 evidence。
3. 信息分为权威事实、未证实主张和缺失信息。证据不足、来源过期或目标不唯一时使用 reject 或 defer。

## Decision

```json
{
  "action": "replace_pending | 当前 Graph 允许的 transition | reject | defer | no_change",
  "target_ref": "transition 时填写当前快照中的唯一目标；其他动作省略",
  "evidence_refs": ["当前可见的权威 evidence"],
  "reason": "决定、依据和未采用其他动作的原因"
}
```

每次 graph mutation 前先 submitDecision。一份 DecisionRef 最多驱动一次 replacePending 或一次 transition，不能同时驱动两类 mutation，也不能跨 inputRef 复用。

## 工作流

## 新 Requirement

1. 保留 Requirement 原意，明确目标、范围、约束、验收与 DeliveryPolicy；Agent 设计不得伪装成用户要求。
2. 一次性创建完整 Task，只含 plan、execute、verify 三类 endpoint。
3. 每个 endpoint 同时定义 DeliverySpec 与 ReportSpec；把已知依赖写成入边，把人工或外部条件写成 blocker。
4. submitDecision(action=replace_pending)，再以返回的 DecisionRef 调用一次 replacePending。
5. 图写成功后交给 task-context-lifecycle 注册和投影。

## Proposal、ContextDelta 或运行中重排

1. Proposal 只是意图。校验来源、graph/workspace/input revision、理由和 evidence 后接受、改写、拒绝或延后。
2. 受影响 endpoint 正在运行时，先独立裁决 held。本次结束，不调用 stop、不轮询。
3. 以后收到新的 PhaseInvocationStopped inputRef，再按当前快照分别裁决 stopped、replacePending/reopened 和 released；每次 mutation 使用新的 DecisionRef。
4. GraphRuntime 自行选择 start 或 resume。Task Manager 不选择 worker 或 checkpoint。

## PhaseOutput、Verify 与 Merge

1. 检查 report、delivery 和 evidence 是否满足当前 Spec，且 input/workspace revision 仍有效。
2. submitted、satisfied、done 是独立裁决。PhaseOutput 到达不能跳过 verifier 或 DeliveryPolicy。
3. verify passed 只满足 Verify gate；code_merge Task 只获得 Merge Queue 资格。
4. DeliveryPolicy、必要 merge evidence、人工决定和其他终态条件全部满足后才能 transition(done)。
5. 权威结果交给 task-context-lifecycle 投影；done 成功后再触发候选终审。

## 并发与恢复

- revision conflict、scope 失败或 transition 拒绝：保留旧 DecisionRef 为未应用审计事实，重新 snapshot、评估并提交新 decision。禁止盲重试旧 mutation。
- 同一 inputRef 重复派发：先恢复已保存 DecisionRef 与 mutation 结果；已应用或已拒绝的输入不重复裁决。
- reject、defer、no_change 只持久化 decision，不写图。
