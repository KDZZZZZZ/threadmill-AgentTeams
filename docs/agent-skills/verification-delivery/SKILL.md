---
name: verification-delivery
description: 独立验证候选结果、检查契约与证据并形成验证报告。Verifier 执行 verify endpoint 时使用。
---

# verification-delivery

## 目的

独立验证候选交付是否满足 Task Contract、verify DeliverySpec 和 ReportSpec，并生成可复现 evidence。

## 依赖

- `phase-runtime`
- `phase-submit`

## 工具

- `evidence.register`

## Workspace 权限

普通 verify 实现只读。使用宿主原生文件搜索、读取与命令工具做 diff、构建、测试和静态分析；`evidence/` 只作为当前 Invocation 的临时输出目录，不进入候选代码 revision，跨 Phase 证据必须先用 `evidence.register` 注册并在 PhaseOutput 中提交 ArtifactRef。Merge Queue 启动的 Targeted Verifier 是例外：Runtime 会注入精确 `allowed_write_paths` / `conflict_paths`，只允许在这些路径内用原生文件读写和 shell 解冲突；仍不得 commit/push，不得写 Coordination Graph、Context Graph 或 main。Threadmill 在 PhaseOutput 提交前统一同步并校验 ACL；只对 execute 候选代码落 Git checkpoint，工具若会越过授权边界，改用明确只读模式或拒绝调用。

## 验证对象

结论只对以下组合有效：Task Contract、Approved Plan、真实 diff/产物、Declared/Observed Write Set、InputRevision、Workspace head、Context Slice binding 和验证 evidence。

## 流程

1. 通过 phase-runtime 校验 verify Binding、execute PhaseOutput、候选产物、正式输入和未漂移 Workspace。`code_merge` 的普通 verify 必须绑定 Merge Queue 已写入 main 的精确 merged revision；不得把 execute 的合入前 workspace 当成最终验收对象。
2. 读取 Declared/Observed Write Set 和真实 diff，检查未声明、未报告、生成、删除、重命名或依赖变化。
3. 把每项验收条件展开为验收矩阵：验收项、所需输入、验证方法、预期、实际、EvidenceRef、通过/失败/阻塞/过期。
4. 先做最小直接验证，再按风险扩大到 targeted tests、typecheck、lint、build、静态分析、安全、兼容性或人工检查。
5. 对每个命令记录环境前置、完整命令、退出码、关键输出和 Workspace head。
6. 检查失败是否可复现，区分环境缺失、测试损坏和候选缺陷。
7. 普通 verify 不修改代码、测试或配置来消除失败。Targeted Verifier 只能在授权冲突路径内改文件；若修复会破坏 Task Contract、验收条件或任务可完成性，必须提交 orchestration proposal。
8. 未运行验证写明原因、影响和剩余风险。无 evidence 的必需项不能判为通过。

## 失败

先完成失败 Verify Result 和报告，再使用 orchestration-escalation。普通 post-merge Verify 失败时必须用 `agent.proposeOrchestration` 给 Manager 提交可复现 evidence 与建议：通常 advice=retry；计划失效、缺前置或需拆分时使用 replan、dependency 或 split。Targeted Verifier 若判断解冲突会导致任务无法按 Contract 完成，也必须申请 Manager 重新编排；该合入候选随后失败，由 Manager 在可信 targeted boundary 上决定是否 `reopen_round`。不要自行启动新的 executor/verifier。

失败的 Verify Result 仍可作为正式 verify 交付，只要它满足当前 DeliverySpec/ReportSpec 且 completion 输入齐全。PhaseOutput Accepted 不等于验证通过。

## 默认交付

- Verify Result；
- 验收矩阵；
- 测试和检查 evidence；
- 当前 DeliverySpec 要求的其他验证产物。

## 默认报告

报告回答：验证对象与 binding；逐项验收结论；命令与 evidence；Write Set 与 diff 对比；总体判断及适用范围；失败复现、阻塞或通过理由；未运行项和残余风险；已提交 Proposal 或 Memory Candidate。

完成后把真实引用交给 phase-submit。对 `code_merge`，此处“通过”表示已合入 revision 的 Verify gate 已满足，但仍不等于已发布或 Task done。
Targeted verifier report rule:
- Merge Queue Targeted Verifier must register its final report with `evidence.register(type=generated_report, content_type=application/json, body=<strict threadmill.targeted_verify.v1 JSON>)`.
- The registered `body` must be exactly one JSON object with `schema`, `verdict`, `checks`, and `evidence_refs`; markdown, command output, `type=json`, and `type=tool_output` are not valid final reports.
- The `report_ref` in `agent.submitPhaseOutput` must be the generated_report artifact id returned by `evidence.register`.
