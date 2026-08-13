---
name: execution-delivery
description: 在批准范围内实施计划、运行必要检查并生成可审计证据。Executor 执行 execute endpoint 时使用。
---

# execution-delivery

## 目的

按 Approved Plan 实施满足 execute DeliverySpec 的最小变更，并留下 verifier 可复现的证据。

## 依赖

- `phase-runtime`
- `phase-submit`

## 工具

- `workspace.list`
- `workspace.read`
- `workspace.write`
- `workspace.run`
- `workspace.diff`
- `evidence.register`

## Workspace 权限

只能在 Workspace Binding、AllowedDirs、Declared Write Set 和有效 lease 内写入。路径或命令被拒绝时不得绕过。

## 流程

1. 通过 phase-runtime 校验 execute Binding、Approved Plan、正式输入、Workspace 基线和 lease。
2. 检查工作区状态。保留已有无关改动，不覆盖、不回退、不做范围外清理。
3. 阅读现有实现、测试和约定。优先使用已有工具、模式和依赖；未经授权不新增依赖或架构层。
4. 按计划逐步修改，每步保持小、可审查、可回滚。
5. 写入前确认目标在 AllowedDirs 和 Declared Write Set 内。必须偏离时停止相关写入，交给 orchestration-escalation；不事后包装越界修改。
6. 不提交生成物、缓存、密钥、凭据、本地环境文件或未要求的批量格式化。
7. 行为变更配套测试。修复类任务优先添加能复现旧问题的回归测试。
8. 每组变更先运行最小针对性检查，修复范围内失败后再扩大到 typecheck、lint、build、静态分析或测试。
9. 记录命令、退出结果、关键输出和对应 Workspace revision。未运行项写明原因。
10. 持续维护 Observed Write Set，并与 Declared Write Set 对比。

## 范围变化

计划缺失、绑定过期、lease 无效、未知依赖、计划失效、范围冲突或可恢复失败时，停止受影响写入并使用 orchestration-escalation。不要自行重开 generation。

## 默认交付

- 候选实现、diff/commit 或其他 execute 产物；
- Observed Write Set；
- 测试与检查 evidence；
- 当前 DeliverySpec 要求的其他产物。

## 默认报告

报告回答：Contract/Plan/Input/Workspace 基线；实际变更；Declared 与 Observed Write Set 对比；计划偏差；验证命令与结果；交付物；兼容性、风险、回滚和未解决问题；已提交 Proposal、Requirement 或 Memory Candidate。

完成后把真实引用交给 phase-submit。不要自我宣布 satisfied、verify passed 或 Task done。
