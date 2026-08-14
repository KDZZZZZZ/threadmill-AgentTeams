---
name: phase-runtime
description: 处理 Planner、Executor 或 Verifier 的 start、resume、输入等待、动态输入和 stop 生命周期。每次 Phase Invocation 均应加载。
---

# phase-runtime

## 目的

在当前 Binding 下处理 start/resume、已声明输入等待、InputsChanged、ContextDelta 和受控 stop。

## 工具

- `runtime.awaitInputs`

## 启动与恢复

1. 确认 Endpoint.phase 与当前角色匹配，InvocationID、Generation、BindingRef、Workspace Binding 和 lease 一致。
2. 检查所有 requiredBy=start 输入已正式 delivered。只消费 PhaseInputSet 中的 PhaseOutputRef 和 ArtifactRefs。
3. 记录 InputRevision、Workspace revision/head、Context Slice binding 和 Task Memory Buffer revision。
4. resume 使用新的 Invocation 和当前 Binding。checkpoint 只包含已完成工作、待办、已消费输入和下一安全恢复点，不能覆盖当前 Contract、Spec、输入或 Workspace。
5. checkpoint 缺失、non_resumable 或与当前 Binding 不兼容时，报告 stale_checkpoint；不做隐式 resume。

## 输入等待

1. 只有已声明的 requiredBy=completion 输入可等待。先把不依赖它们的工作推进到当前极限。
2. 调用 awaitInputs 时，InputIDs 只能来自当前 PhaseInputSet；省略表示等待全部 pending。
3. 返回后合并 Delivered、Pending 和新的 InputRevision，再重新评估工作。
4. terminalReason 为 source_failed、source_cancelled、input_stale、lease_expired 或 deadline_exceeded 时，不伪造交付；按当前合同继续、交给 orchestration-escalation，或生成允许的阻塞报告。
5. completion 输入仍缺失时，phase-submit 不得提交最终 PhaseOutput。

## 动态事件

- InputsChanged：以完整的新 PhaseInputSet 和 InputRevision 重新评估已有工作。
- ContextDelta：合并知识变化；它不能替代正式输入。使计划或编排失效时，交给 orchestration-escalation。

## stop

收到 Runtime stop 控制后，停止新的普通工具调用，只通过受控状态通道刷出：已完成工作、待办、已消费输入、当前 revision、产物路径和下一安全恢复点。不要保存隐藏推理、模型会话或旧 lease。

Phase Agent 不调用 startPhase、stopPhase 或 resumePhase；这些是 Runtime 回调。
