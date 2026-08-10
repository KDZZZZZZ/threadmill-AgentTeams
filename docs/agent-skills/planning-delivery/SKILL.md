---
name: planning-delivery
description: 根据 Task Contract 和输入产出计划、Declared Write Set 与验证计划。Planner 执行 plan endpoint 时使用。
---

# planning-delivery

## 目的

把 Task Contract 转化为 executor 可直接执行、verifier 可独立验收的计划产物。

## 依赖

- `phase-runtime`
- `phase-submit`

## Workspace 权限

实现只读。只使用 Runtime 授权的代码检查工具，以及计划、报告和 evidence 产物写入路径。

## 流程

1. 通过 phase-runtime 校验 plan Binding、正式输入和 Workspace 基线。
2. 阅读 Contract、Phase Spec、正式交付、仓库规则和 task-memory；检查现有实现、测试、依赖与模式。
3. Context 不足时使用共享 Context Skill。Context 与代码或权威输入冲突时记录各自 revision，不静默择一。
4. 明确目标、非目标、事实、假设和待确认项。保留 Requirement 原意。
5. 选择一个可行主方案。只有存在真实取舍时才列备选，不为形式凑选项。
6. 把步骤绑定到具体模块、文件、接口或待确认探索点，并写明每步完成证据。
7. 产出 Declared Write Set，按当前 schema 覆盖 files、modules、symbols、contracts、tests、owners。
8. 设计能证明 Contract 的验证计划，列出目标测试、typecheck、lint、build、静态分析或人工检查及判据。
9. 对迁移、删除、安全、权限、兼容性和数据风险写前置条件与回滚方式。
10. 把 Agent 自主选择的命名、路径、接口、依赖或策略列为设计与假设，说明理由和影响。

## 默认交付

- Submitted Plan；
- Declared Write Set；
- 验证计划；
- 当前 DeliverySpec 要求的其他计划产物。

## 默认报告

报告回答：目标与验收理解；输入及 revision；选定方案与取舍；实施步骤；Declared Write Set；验证判据；依赖、权限、风险和回滚；Agent 设计与假设；pending 输入或未决项；使用或提交的 Memory Candidate。

完成后把真实引用交给 phase-submit。不要把未执行命令写成已验证，也不要把计划完成写成 Task done。
