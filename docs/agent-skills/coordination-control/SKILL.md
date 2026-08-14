---
name: coordination-control
description: 通过受控决定读取和更新 Threadmill Coordination Graph。Task Manager 创建、迁移、阻塞、恢复或终结 Task 时使用。
---

# coordination-control

## Targeted Verify 重编排

- Targeted Verifier 的 proposal 到达而被选中 verify 仍为 `pending` 时，只能选择 `replace_pending`（或 reject/defer/no_change）；`reopen_round` 只允许 verify 已为 `satisfied` 或 `rejected` 的已完成轮次。replacement 的具体 Task 与依赖仍由 Task Manager 自主设计。

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
  "target_ref": "Phase transition 精确填写 selected_endpoint.task_id/selected_endpoint.endpoint_id；done 精确填写被选中的 TaskID；其他动作省略",
  "evidence_refs": ["当前可见的权威 evidence"],
  "reason": "决定、依据和未采用其他动作的原因"
}
```

每次 graph mutation 前先 submitDecision。一份 DecisionRef 最多驱动一次 replacePending 或一次 transition，不能同时驱动两类 mutation，也不能跨 inputRef 复用。

## 工作流

## 新 Requirement

1. 保留 Requirement 原意，明确目标、范围、约束、验收与 DeliveryPolicy；Agent 设计不得伪装成用户要求。
2. 一次性创建完整 Task，只含 plan、execute、verify 三类 endpoint。
3. 为每个新 Task 在 `task_policies` 中明确选择唯一 DeliveryPolicy：非代码报告/研究/设计产物用 `non_code_artifact`，代码入库用 `code_merge`，人工验收用 `human_acceptance`，外部交付用 `external_delivery`。不得把所有 Task 默认为代码合并。
4. Requirement 中的交付与报告验收由 Runtime 冻结进 Task Contract/Phase Spec；Agent 意图不提交 ContractRef、SpecRef、BindingRef 或额外 spec 对象。
5. plan→execute→verify 是 Runtime 内建顺序，禁止为同一 Task 创建固定顺序边；`edges` 只提交跨 Task 依赖，人工或外部条件写成 blocker。
6. submitDecision(action=replace_pending)，再以返回的 DecisionRef 调用一次 replacePending。
7. 图写成功后的 Task Context 注册与投影由 Runtime 自动完成。

## Proposal、ContextDelta 或运行中重排

1. Proposal 只是意图。校验来源、graph/workspace/input revision、理由和 evidence 后接受、改写、拒绝或延后。
2. 受影响 endpoint 正在运行时，先独立裁决 held。本次结束，不调用 stop、不轮询。
3. 以后收到新的 PhaseInvocationStopped inputRef，再按当前快照分别裁决 stopped、replacePending/reopened 和 released；每次 mutation 使用新的 DecisionRef。
4. GraphRuntime 自行选择 start 或 resume。Task Manager 不选择 worker 或 checkpoint。

## PhaseOutput、Verify 与 Merge

1. 只使用当前持久化边界输入、当前 snapshot 与已注册 evidence。不得扫描 AgentTeams 历史任务、旧 result、provider history、其他 Task 目录或共享项目寻找裁决样例。
2. 严格按边界类型决定一次动作：`phase_output -> submitted`、`phase_evaluation -> satisfied | rejected`、`phase_stopped -> stopped`、`stop_release -> released`、`task_completion -> done`；不得在 `phase_output` 上直接判 `satisfied`。
3. 前四类 transition 的 `target_ref` 必须精确等于边界输入 `selected_endpoint.task_id + "/" + selected_endpoint.endpoint_id`；`task_completion` 的 `target_ref` 必须精确等于被选中的 TaskID。ArtifactRef、output ref、command ID 和边界载荷自己的 `target_ref` 都不是决定目标。
4. 检查 report、delivery 和 evidence 是否满足当前 Spec，且 input/workspace revision 仍有效。
5. submitted、satisfied、done 是独立裁决。PhaseOutput 到达不能跳过 verifier 或 DeliveryPolicy。
6. verify passed 只满足 Verify gate；code_merge Task 只获得 Merge Queue 资格。
7. 可信 targeted boundary 可使用 `reopen_round` 原子重开同一 Task 的 execute+verify；来源必须是 Merge Queue Targeted Verifier 的 proposal。普通 proposal 不得重开已终态节点。
8. DeliveryPolicy、必要 merge evidence、人工决定和其他终态条件全部满足后才能 transition(done)。
9. 权威结果交给 task-context-lifecycle 投影；done 成功后再触发候选终审。

## 并发与恢复

- revision conflict、scope 失败或 transition 拒绝：保留旧 DecisionRef 为未应用审计事实，重新 snapshot、评估并提交新 decision。禁止盲重试旧 mutation。
- 同一 inputRef 重复派发：先恢复已保存 DecisionRef 与 mutation 结果；已应用或已拒绝的输入不重复裁决。
- reject、defer、no_change 只持久化 decision，不写图。
- graph mutation 成功后不得再调用 snapshot 或其他 Threadmill MCP 确认结果；成功响应已经携带权威 revision，且 Runtime 会立即 fence 本 Invocation 的一次性 bearer。直接用该响应执行 TeamHarness `submit_task` 并结束。
