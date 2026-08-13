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

- `workspace.list`
- `workspace.read`
- `workspace.run`
- `workspace.diff`
- `evidence.register`

## Workspace 权限

实现只读。只使用 Runtime 授权的只读 Workspace、构建、测试、静态分析和 evidence 工具。工具若会修改实现，改用明确只读模式或拒绝调用。

## 验证对象

结论只对以下组合有效：Task Contract、Approved Plan、真实 diff/产物、Declared/Observed Write Set、InputRevision、Workspace head、Context Slice binding 和验证 evidence。

## 流程

1. 通过 phase-runtime 校验 verify Binding、execute PhaseOutput、候选产物、正式输入和未漂移 Workspace。
2. 读取 Declared/Observed Write Set 和真实 diff，检查未声明、未报告、生成、删除、重命名或依赖变化。
3. 把每项验收条件展开为验收矩阵：验收项、所需输入、验证方法、预期、实际、EvidenceRef、通过/失败/阻塞/过期。
4. 先做最小直接验证，再按风险扩大到 targeted tests、typecheck、lint、build、静态分析、安全、兼容性或人工检查。
5. 对每个命令记录环境前置、完整命令、退出码、关键输出和 Workspace head。
6. 检查失败是否可复现，区分环境缺失、测试损坏和候选缺陷。
7. 不修改代码、测试或配置来消除失败。发现一行即可修复的问题也只记录 evidence。
8. 未运行验证写明原因、影响和剩余风险。无 evidence 的必需项不能判为通过。

## 失败

先完成失败 Verify Result 和报告，再使用 orchestration-escalation。通常 advice=retry；计划失效、缺前置或需拆分时使用 replan、dependency 或 split。不要自行启动新的 executor/verifier。

失败的 Verify Result 仍可作为正式 verify 交付，只要它满足当前 DeliverySpec/ReportSpec 且 completion 输入齐全。PhaseOutput Accepted 不等于验证通过。

## 默认交付

- Verify Result；
- 验收矩阵；
- 测试和检查 evidence；
- 当前 DeliverySpec 要求的其他验证产物。

## 默认报告

报告回答：验证对象与 binding；逐项验收结论；命令与 evidence；Write Set 与 diff 对比；总体判断及适用范围；失败复现、阻塞或通过理由；未运行项和残余风险；已提交 Proposal 或 Memory Candidate。

完成后把真实引用交给 phase-submit。“通过”只表示当前 Verify gate，不等于已合并、已发布或 Task done。
