---
name: phase-submit
description: 提交符合当前 DeliverySpec 与 ReportSpec 的正式 PhaseOutput。Planner、Executor 或 Verifier 完成阶段交付时使用。
---

# phase-submit

## 目的

提交满足当前 endpoint DeliverySpec 与 ReportSpec 的正式 PhaseOutput。

## 工具

- `agent.submitPhaseOutput`

## 前置条件

1. 当前角色交付 Skill 已完成规定产物和报告。
2. 所有 required completion 输入已到达。
3. 产物基于最新 InputRevision、当前 Binding 和未漂移的 Workspace head。
4. DeliveryRefs、ReportRef、EvidenceRefs 已由 Runtime 从受控路径注册。
5. 未把 Proposal、Requirement 或 Memory Candidate 当作阶段交付替代品。

## 提交

```json
{
  "phase": "plan | execute | verify，与当前 endpoint 严格一致",
  "delivery_refs": ["满足 DeliverySpec 的真实引用"],
  "report_ref": "满足 ReportSpec 的真实引用",
  "evidence_refs": ["支撑交付和判断的真实引用；复用 Context 时证据正文包含准确 NodeRef/SourceRef"]
}
```

不要填写 TaskID、Endpoint、Generation、BindingRef、InputRevision、WorkspaceHead、lease 或 InvocationID；Runtime 会绑定。

Accepted 只表示提交被接收。后续 submitted、satisfied、rejected、invalidated、verify passed 和 Task done 由授权方裁决。
Targeted verifier report_ref rule:
- When the PhaseSpec is `threadmill.targeted_verify.v1`, `report_ref` must be the artifact id returned by `evidence.register(type=generated_report, content_type=application/json, body=<strict v1 JSON>)`.
- Do not put a local path, markdown report, `type=json`, `type=tool_output`, or command-output artifact in `report_ref` for targeted verify.
