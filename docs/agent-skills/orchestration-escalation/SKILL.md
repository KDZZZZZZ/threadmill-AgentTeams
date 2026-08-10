---
name: orchestration-escalation
description: 提交依赖、重排、拆分、重试等编排意图或独立新需求。Phase Agent 发现当前输入或编排不足时使用。
---

# orchestration-escalation

## 目的

当前 endpoint 的既有输入或编排不足时，提交可审计意图；发现独立新工作时，提交 Requirement。

## 工具

- `agent.proposeOrchestration`
- `agent.submitRequirement`

## 选择

- 已知 completion 输入未到：使用 phase-runtime.awaitInputs，不提 dependency。
- 未声明的新前置：dependency。
- 当前计划或前提失效：replan。
- 工作需要独立交付、验收、权限或 Workspace：split；若它是独立新工作，也可 submitRequirement。
- 可恢复失败需要重开同类 endpoint：retry。
- 串并行关系需要调整：serial_parallel。

## Proposal

1. Proposal 是意图，不是图命令。不得决定创建哪些 Task、如何连边、哪些 endpoint 失效。
2. 使用 Runtime 提供的 ProposalID、ClientRef、FromEndpoint、FromInvocationID、BasedOnGraphRevision、BasedOnWorkspaceRevision 和 BasedOnInputRevision；不得自造 revision 或身份。
3. OrchestrationAdvice、DeliverySpecAdvice、ReportSpecAdvice 和 Rationale 必须具体说明未来 endpoint 需要什么以及为什么。
4. EvidenceRefs 只能引用已注册证据。
5. Proposal 被接收不表示图已改变，也不自动结束当前 phase。

## Requirement

Requirement 只描述具有独立交付与验收的新工作，包含 Text、可选 Goal、Constraints 和 EvidenceRefs。它不能修改当前 Task Contract，也不能直接调度。
