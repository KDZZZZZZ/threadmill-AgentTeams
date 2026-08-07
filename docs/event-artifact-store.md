# Event Log / Artifact Store 详细设计

版本：v0.1
状态：Draft

---

## 1. 定位

Event Log 是系统事实来源，由 Agent Runtime **自动记录**所有 agent 活动和状态变化——agent 不显式"写日志"，它们的消息、工具调用、结论和状态变更被自动捕获。这里的所有 agent 包括 Task Manager Agent、Ctx Manager Agent / Ctx Agent、planner、executor 和 verifier。

task 表、ctxlib、UI 状态、进度面板都可以视为 event log 的 projection。

Artifact Store 保存大对象，例如 transcript、tool output、test output、diff patch 和 screenshots。

---

## 2. 设计原则

```text
1. Event Log 由 Agent Runtime 自动记录所有 agent 活动和状态变化，agent 不显式写日志。
2. 大对象进 Artifact Store，Event Log 只保存引用。
3. ctxlib 只从 Event Log / Artifact Store 提取记忆，不接受 agent 直接写入。
4. Task Graph 由经 Agent Runtime 授权的 Task Manager Agent 权威写入，其写入动作也被自动记入 log。
5. merge、verify、conflict、human decision 都必须可追溯。
6. 系统状态应尽可能能从 Event Log 重放。
```

---

## 3. 核心事件

```text
TaskCreated
TaskPrepared
TaskPlanned
TaskBlocked
TaskUnblocked
TaskExecuting
TaskVerifying
TaskVerifyPassed
TaskVerifyFailed
TaskConflictDetected
TaskMergeQueued
TaskMerged
TaskDone

AgentStarted
AgentFinished
AgentFailed
TaskGraphExpanded

ContextBlockCreated
ContextBlockSuperseded
ContextPackSelected
ContextQueryExecuted

WorktreeCreated
WorktreeDiffObserved
WorktreeAbandoned

ConflictContextBroadcasted
HumanDecisionRequested
HumanDecisionRecorded
```

---

## 4. Event 数据模型

```go
type Event struct {
	// ID 是事件全局唯一标识。
	ID string `json:"id"`
	// Type 是事件类型；上层投影按类型分发。
	Type string `json:"type"`

	// 以下外键均可选，用于把事件关联到 task、agent run、worktree 或 context block。
	TaskID string `json:"task_id,omitempty"`
	AgentInvocationID string `json:"agent_invocation_id,omitempty"`
	WorktreeID string `json:"worktree_id,omitempty"`
	ContextBlockID string `json:"context_block_id,omitempty"`

	// Payload 保存小型结构化载荷；大对象必须进入 Artifact Store。
	Payload any `json:"payload"`
	// ArtifactRefs 指向 transcript、diff、test output 等大对象。
	ArtifactRefs []string `json:"artifact_refs"`
	CreatedAt time.Time `json:"created_at"`
}
```

---

## 5. Artifact Store 保存内容

Artifact Store 保存较大的对象：

```text
- 原始日志
- agent transcript
- tool output
- test output
- diff patch
- screenshots
- benchmark result
- generated reports
```

ctxlib 只引用 artifact，不直接内嵌大文件。

---

## 6. Artifact 数据模型

```go
type Artifact struct {
	// ID 是 artifact 全局唯一标识。
	ID string `json:"id"`
	// Type 标识 artifact 内容类型，便于 UI 和验证器解释。
	Type ArtifactType `json:"type"`
	// PathOrBlobRef 指向本地路径或对象存储引用。
	PathOrBlobRef string `json:"path_or_blob_ref"`
	// ContentHash 用于去重、完整性校验和审计。
	ContentHash string `json:"content_hash"`

	TaskID string `json:"task_id,omitempty"`
	AgentInvocationID string `json:"agent_invocation_id,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

type ArtifactType string

const (
	ArtifactAgentTranscript ArtifactType = "agent_transcript"
	ArtifactToolOutput ArtifactType = "tool_output"
	ArtifactTestOutput ArtifactType = "test_output"
	ArtifactDiffPatch ArtifactType = "diff_patch"
	ArtifactScreenshot ArtifactType = "screenshot"
	ArtifactBenchmarkResult ArtifactType = "benchmark_result"
	ArtifactGeneratedReport ArtifactType = "generated_report"
)
```

---

## 7. Projection

Event Log 可以生成多个 projection：

```text
TaskProjection:
  当前 task 状态、phase、依赖和 blocker。

AgentProjection:
  当前 active agents、历史 invocation 和结果。

CtxProjection:
  当前可用 context blocks、supersede 关系和索引。

MergeProjection:
  当前 merge queue、已合入 task 和冲突关系。

UIPanelProjection:
  用户界面展示的进度、预算和风险。
```

---

## 8. 与 ctxlib 的关系

ctxlib 不直接从 agent session 读取长期记忆，也不接受 agent 主动推送；它**只从 Event Log / Artifact Store 中提取**结构化 context，由 Ctx Agent（含 Context Curator）负责。

```text
AgentFinished / TaskVerifyFailed / TaskMerged 等事件（Agent Runtime 自动记录）
  -> Agent Runtime(role=ctx_manager) 启动 Ctx Manager Agent 读取 log 中的 summary，并按 ref 取 artifact
  -> 提炼 / 去重 / supersede，生成 ContextBlock
  -> ContextBlock 的产生本身也作为事件被记录
```

Event Log 是 ctxlib 的唯一上游；经 Agent Runtime 授权的 Ctx Manager Agent / Ctx Agent 是 ctxlib 的唯一写入者。

---

## 9. 不变量

```text
1. 关键状态变化必须进入 Event Log（由 Agent Runtime 自动记录，非 agent 显式写）。
2. Task Manager Agent 和 Ctx Manager Agent 也必须经 Agent Runtime 运行，它们不是日志旁路。
3. 大对象必须进 Artifact Store，并用 ref 引用。
4. Verify failure 必须可追溯到测试输出或人工判断。
5. Merge 必须可追溯到 verify result、diff 和 commit。
6. Human decision 必须显式记录。
7. ctxlib 中的高影响 context 必须有 event 或 artifact evidence。
8. ctxlib 只从 Event Log / Artifact Store 构建，不接受普通 agent 直接写入。
```
