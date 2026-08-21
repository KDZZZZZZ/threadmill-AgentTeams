# Event Log / Artifact Store 详细设计

版本：v1.0
状态：Draft
定位：本文描述 Threadmill 的 Event Log 与 Artifact Store。**语义以 docs/threadmill-unified-design.md 为准**；本文只展开实现细节与 third_party/agentteams 的复用边界。AgentTeams 本身**没有事件流模型**（无 Event Log / 事件总线 / Artifact 注册表，全仓检索零命中），因此 Event Log 与 Artifact 注册表是 Threadmill 新建服务，AgentTeams 只提供原始证据源与物理存储底座。

---

## 1. 定位

Event Log 是系统事实来源，由 Agent Runtime **自动记录**所有 agent 活动和状态变化——agent 不显式“写日志”，它们的 Invocation 生命周期、结构化边界输出、提交的 MemoryCandidate 和状态变更被自动捕获。这里的所有 Agent 包括 Task Manager Agent、Context Agent、planner、executor 和 verifier。

Artifact Store 保存大对象：transcript、tool output、test output、diff patch、screenshots、PhaseOutput 引用的交付物/报告/证据。Event Log 只保存引用（ArtifactRef），不内嵌大对象。

```text
Agent Runtime 归一化 Agent Event，保存 transcript、tool output、diff 和测试证据
  -> Event Log（追加式审计/证据流）
  -> 各投影：Coordination Graph 审计、Context Graph 证据、UI 状态、进度面板
```

Event Log 的语义边界（同统一设计）：

- **自动捕获**：Runtime 记录的是 Agent 的边界活动与结构化输出，不记录也不暴露未提交的 phase 过程上下文（中间推理、工具输出、探索轨迹）；
- **审计**：Coordination Graph 的热修改历史、MemoryCandidate 的缓冲与审查、Context Graph 的写入事务都由 Event Log 审计；审计机制不限制 Coordination Graph 的运行时热修改（§5.7）；
- **证据链**：verify、merge、human decision、Context Node 的 SourceRefs 都回溯到事件与 artifact；
- **回放**：系统状态应尽可能能从 Event Log 重放。

---

## 2. 自动事件捕获

### 2.1 捕获点（Threadmill 新建的 EventLogAdapter）

Event Log 不是由 agent 显式写日志，而是 Runtime 在以下边界自动归一化事件：

| 捕获点 | 产生的事件 | 说明 |
| --- | --- | --- |
| Invocation 生命周期 | AgentInvocationStarted / Finished / Failed | Runtime 启动、取消、恢复、替换 Agent 时 |
| Phase Endpoint 编排 | PhaseActivated | Scheduler 选中 runnable endpoint 并请求 Workspace Service 创建/复用该轮次的 Workspace Binding |
| 结构化边界输出 | PhaseOutputSubmitted | 每个 phase 按 DeliverySpec / ReportSpec 提交输出；Runtime 只校验形状与必填引用 |
| 编排建议 | OrchestrationProposalSubmitted / OrchestrationProposalDecided | 运行中 Agent 主动提交建议；Task Manager 裁决（接受/改写/拒绝）并明确当前 Invocation 处置 |
| Task Manager 裁决 | TaskManagerDecisionSubmitted / TaskManagerDecisionFinalized | Runtime 将 DecisionRef 绑定 inputRef、expected graph revision、Graph mutation 结果与最终 disposition；Agent 不直接写日志 |
| MemoryCandidate | `MemoryCandidateBuffered` / `MemoryCandidateRejected`、`CandidateBufferFrozen`、`CandidateReviewAccepted` / `CandidateReviewRejected` | 入缓冲后对同 Task plan/execute/verify 可读，跨 Task 不可见；不代表 ContextNode。done 后冻结、终审并原子落图 |
| Context Graph 写入 | ContextGraphCommitted | 节点/边变更与 graph/subgraph revision 的原子提交 |
| 订阅与推送 | ContextSubscriptionCreated / Cancelled / Expired、ContextDeltaDelivered / Consumed | `Cancelled` 记录 consumer 显式取消，`Expired` 记录 Invocation 结束失效；Runtime 记录订阅关系与 Delta 是否被 Agent 消费 |
| 验证与合并 | VerifyPassed / VerifyFailed、MergeCandidateQueued / Merged | merge event 附 commit/diff/test evidence |
| 人工决定 | HumanDecisionRequested / HumanDecisionRecorded | 显式记录，含理由与 revision |
| 结果失效 | PhaseResultInvalidated | Task Contract、依赖结果、代码基线、Workspace Head 或高影响上下文变化后由 Task Manager 按影响范围触发 |

### 2.2 原始证据源（AgentTeams 现状，只作证据不当作事件流）

AgentTeams 没有统一事件模型；它散落着四类**原始证据源**，由 Threadmill 的 EventLogAdapter 归一化消费：

| 原始证据源 | 内容 | 依据路径 |
| --- | --- | --- |
| Matrix 房间时间线 | 所有 agent 消息、m.file 媒体事件、人工干预；房间拓扑 `TASK：<projectId>` / `project:{id}` | third_party/agentteams/plugins/teamharness/mcp/server.py；third_party/agentteams/plugins/teamharness/mcp/roomflow_tool.py |
| 各运行时私有 sessions/ | 每 agent 的私有对话历史与转出消息记录（`workspace_dir/sessions/<channel>/`） | third_party/agentteams/plugins/teamharness/mcp/server.py（`_record_outbound_to_session`）；third_party/agentteams/qwenpaw/src/qwenpaw_worker/worker.py（SESSION_FILE_PROMPT_POLICY） |
| heartbeat 快照 | 每 worker 的 `heartbeat.json`（status/message/details/updatedAt）与控制器 ready 上报 | third_party/agentteams/qwenpaw/src/qwenpaw_worker/heartbeat.py |
| taskflow/projectflow 文件态 | `shared/projects/{id}/meta.json`、`shared/tasks/{id}/meta.json` 的分配→进行中→提交状态迁移（当前态快照，无 revision/审计） | third_party/agentteams/copaw/src/copaw_worker/task.py（FileSystemTaskStore）；third_party/agentteams/plugins/teamharness/mcp/server.py（projectflow/taskflow） |

此外，OTel span（third_party/agentteams/plugins/teamharness/adapters/qwenpaw/task_trace.py 把 room_id 关联到 task/project 打在每次 turn 根 span）可复用于**事件关联**，但 span 是观测信号不是事件流：它不保证顺序、不可重放、无业务事件类型。EventLogAdapter 把这些证据归一化成追加式事件后才具有审计与回放语义。

---

## 3. Event 数据模型

```go
type Event struct {
	// ID 是事件全局唯一标识。
	ID string `json:"id"`
	// Type 是事件类型；上层投影按类型分发。
	Type string `json:"type"`

	// 以下外键均可选，用于把事件关联到 task、agent run、workspace 或 context graph 对象。
	TaskID string `json:"task_id,omitempty"`
	WorkspaceRef string `json:"workspace_ref,omitempty"` // 轮次标识
	PhaseEndpoint string `json:"phase_endpoint,omitempty"`
	AgentInvocationID string `json:"agent_invocation_id,omitempty"`
	WorkspaceID string `json:"workspace_id,omitempty"`

	// Payload 保存小型结构化载荷；大对象必须进入 Artifact Store。
	Payload any `json:"payload"`
	// ArtifactRefs 指向 transcript、diff、test output 等大对象。
	ArtifactRefs []string `json:"artifact_refs"`
	// GraphRevision 记录事件相关的 Coordination/Context Graph revision（如适用）。
	GraphRevision int64 `json:"graph_revision,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}
```

关键事件清单见 §2.1。事件类型命名与统一设计的实体一一对应（PhaseOutput、OrchestrationProposal、MemoryCandidate、ContextSubscription、Context Delta、Verify Result、MergeCandidate），不新增设计外实体。

---

## 4. Artifact Store

当前 Runtime implementation 的 durable boundary 是：bytes 不复制进 Runtime SQLite；SQLite 保存 ArtifactRef、type、content hash、opaque blob ref、origin logical owner 与 TaskID+InvocationID access grant。`ArtifactRegistered` 与 metadata/access mutation 同事务进入 Runtime outbox。blob 先 publish、后提交 metadata，因此 crash 可留下可 GC orphan blob，但 metadata 不得从 bytes/path 猜测 owner/access，也不得指向未验证 blob。Runtime snapshots 是 online authority，outbox 是 audit/projection/reconciliation input，不是 full event sourcing；dispatcher cursor/ack 另行演进。

M5-C3-2A 提供 `internal/artifacts.S3BlobPublisher`，对 AgentTeams embedded deployment 已有的 MinIO S3-compatible API 做最小适配。它从进程内 secret/config boundary 接收 endpoint、bucket、prefix、access key 与 secret key；这些配置不写入 Runtime record、metadata 或 outbox。发布器对受控源文件重算 SHA-256，以 `threadmill/artifacts/sha256/<hash>`（可配置前缀）作为内容寻址 key，签名 `PUT` 后以签名 `HEAD` 验证 metadata hash，再返回稳定的 `s3://<bucket>/<key>` opaque ref。该 ref 不包含 workspace absolute path，因而可跨 Runtime/Worker/epoch replacement 使用。embedded MinIO 由 `third_party/agentteams/manager/scripts/init/start-minio.sh` 启动，installer 将 endpoint/bucket/worker credentials 投影为 `AGENTTEAMS_FS_*`；Threadmill 的控制面必须通过自己的 deployment secret/config boundary 提供 publisher 配置，而不读取或持久化 Worker credential。

`runtime.NewDurableArtifactRegistry(repository, publisher)` 明确组合同一个 `RuntimeStateRepository.ArtifactStore()` 与该 publisher；它不创建第二个 metadata authority。本 slice 尚未将 MCP `artifact.register` 的 production registrar 切换至该组合，C3-2B 才负责该 cutover。publish 成功、SQLite transaction 失败时允许 orphan blob 留给后续 GC；publish/verify 失败时不调用 durable metadata mutation。

### 4.1 ArtifactRef 与内容哈希

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

ArtifactRef 出现在统一设计的各处引用点上：

- `PhaseOutput`：DeliveryRefs / ReportRef / EvidenceRefs（endpoint 输出的交付物、报告、证据）；
- `CoordinationEdge.Data`：沿边传递的交付物/报告/证据；
- `MemoryCandidate.SourceRefs` 与 `ContextNode.SourceRefs`：Context Graph 的证据溯源（`ContextEdge` 不含 `SourceRefs`，边来源由创建事件与订阅记录重建，见 [context-graph.md](./context-graph.md) §3.4）；
- `Event.ArtifactRefs`：事件载荷的大对象引用。

**ContentHash 注册表是 Threadmill 新建语义**：AgentTeams 的 MinIO 是路径寻址对象存储，没有内容哈希注册、没有 ArtifactType 索引、没有"同内容只存一份"的引用语义。Threadmill 在 MinIO 之上登记 `Artifact{ID, Type, ContentHash, PathOrBlobRef}`，实现：

- 去重：相同 ContentHash 的写入返回既有 ID，避免重复上传；
- 完整性校验：读取时核对哈希；
- 审计：PhaseOutput / Verify Result / Merge 可回溯到确切字节内容。

### 4.2 物理存储（直接复用 MinIO）

Artifact 的物理层直接复用 AgentTeams 的 MinIO + filesync 协议：

- 统一文件系统布局（`agents/<name>/`、`shared/tasks/{id}/` 的 meta/spec/result/workspace/deliverables、`workers/` 前缀）——third_party/agentteams/docs/k8s-native-agent-orch.md §3.6；
- 写者推送 + 增量 mirror + 回退拉取的同步协议——third_party/agentteams/shared/lib/worker-file-sync.sh、third_party/agentteams/qwenpaw/src/qwenpaw_worker/sync.py、third_party/agentteams/worker/scripts/worker-entrypoint.sh；
- 路径约束与敏感扫描（发布前拒绝敏感文件名/内容）——third_party/agentteams/plugins/teamharness/mcp/server.py。

**不应复用**：AgentTeams 的 `artifact` 工具（`publish_file` 把工作区文件作为 m.file 上传到 Matrix 房间媒体）是**人工可见的交付通道**，不是 Artifact Store 的写入路径；Threadmill 的 Artifact Store 走对象存储 + 注册表，Matrix m.file 只作为可选的对外发布动作。

---

## 5. Process Transcript 隔离

### 5.1 原则

Runtime 保存 transcript、tool output、diff 和测试证据，**但不向 Task Manager 暴露未提交的 phase 过程上下文**（统一设计 §16）。Task Manager 只能读取：Requirement、所有 completed PhaseOutput / report / evidence、自己的 Context Slice / Delta 和可见 Context Graph。运行中的 phase agent 的中间推理、工具输出、探索轨迹和未提交上下文默认留在 Invocation 内；只有以下结构化边界输出可以离开 Invocation 进入 Task Manager：

1. 阶段结束时的 `PhaseOutput`；
2. 运行中主动提交的 `OrchestrationProposal`；
3. 显式 `MemoryCandidate`：进入该 Task 三阶段共享缓冲，可由同 Task Phase Agent 读取，但 Task Manager 不读取其语义内容；done 后冻结终审。

### 5.2 AgentTeams 的现成隔离策略（直接复用语义）

QwenPaw 运行时的 sessions/ 目录策略与 Threadmill 的隔离目标一致，可原样采纳为 Runtime 约束：

```text
Do not read, list, grep, glob, summarize, copy, or expose files under sessions/.
Session files are runtime-private state and may contain private conversation history.
```

——third_party/agentteams/qwenpaw/src/qwenpaw_worker/worker.py（SESSION_FILE_PROMPT_POLICY，注入 AGENTS.md / SOUL.md）

AgentTeams 中 session 文件（`workspace_dir/sessions/<channel>/`，含外发消息记录）是**运行时私有**的；Threadmill 沿用这一隔离：transcript 作为 Artifact 保存（类型 `agent_transcript`），但**只有审计/重放侧可读，不进入 Task Manager 的编排输入，也不直接进入 Context Graph**（Context Graph 只接受受控 MemoryCandidate，见 [context-graph.md](./context-graph.md)）。

### 5.3 哪些事件可被 Manager 看

| 可见性 | 事件/内容 | 依据 |
| --- | --- | --- |
| Task Manager 可读 | 所有 completed PhaseOutput 及其 DeliveryRefs/ReportRef/EvidenceRefs；OrchestrationProposalSubmitted / Decided；VerifyPassed/Failed；MergeCandidateQueued/Merged；HumanDecisionRequested/Recorded；自己的 Context Slice/Delta 与可见 Context Graph | 统一设计 §6、§7.1 |
| Context Agent 可读 | Event Log、Artifact Store、权限策略（MemoryCandidateBuffered / CandidateBufferFrozen / CandidateReviewAccepted / CandidateReviewRejected、ContextGraphCommitted） | 统一设计 §6 |
| 任何人不可读（运行时私有） | 未提交的 phase 过程上下文：中间推理、单步工具输出、探索轨迹、sessions/ transcript 内容 | 统一设计 §5.5、§16；worker.py SESSION_FILE_PROMPT_POLICY |
| 仅审计侧 | 全部事件（含被拒绝的 MemoryCandidate、订阅关系、Delta 消费记录） | 统一设计 §5.7、§14.2 |

人工可见性（AgentTeams 的 Matrix 房间全员可见模型）与 Task Manager 的编排读取是两个通道：房间消息对人工是特性，但 Manager 的编排决策输入只来自结构化边界事件，不解析聊天文本。

---

## 6. 适配边界：MinIO / filesync / Matrix / session / OTel

| 基座 | 边界 | 依据路径 |
| --- | --- | --- |
| MinIO | **直接复用为物理存储**：对象存储 + 统一文件布局 + 敏感扫描。**不提供** Artifact 注册表、ContentHash 索引、ArtifactType 分类——这些由 Threadmill 新建的 ArtifactStoreAdapter 覆盖 | third_party/agentteams/docs/k8s-native-agent-orch.md §3.6；third_party/agentteams/plugins/teamharness/mcp/server.py |
| filesync | **直接复用为同步协议**：写者推送 + watermark 增量 + 回退拉取。**不承担**事件顺序、游标语义或事件流——Event Log 的追加顺序由 EventLogAdapter 自己保证；`.last-pull`/mtime 水位不能当事件游标（third_party/agentteams/docs/issue-1107-file-sync-io-amplification.md §4.1 已证伪） | third_party/agentteams/shared/lib/worker-file-sync.sh；third_party/agentteams/worker/scripts/worker-entrypoint.sh |
| Matrix | **只作原始证据源与人工可见性通道**：房间时间线可被 EventLogAdapter 归一化，m.file 可作可选对外发布。**不是事件总线、不是 Context Graph、不是控制面协议**：消息文本（mention、TASK_COMPLETED/TASK_BLOCKED）不解析为编排或记忆信号 | third_party/agentteams/plugins/teamharness/mcp/server.py；third_party/agentteams/plugins/teamharness/mcp/message_tool.py |
| session | **直接复用隔离语义**：sessions/ 运行时私有，agent 与 Manager 均不可读（SESSION_FILE_PROMPT_POLICY）；transcript 落 Artifact Store 但仅审计/重放侧可读 | third_party/agentteams/qwenpaw/src/qwenpaw_worker/worker.py；third_party/agentteams/scripts/export-debug-log.py（证明 transcript 分散于 Matrix 时间线与各运行时 sessions/，无统一 Event Log） |
| OTel | **只作观测与关联信号**：task_trace.py 的 span 关联逻辑可复用于事件关联；CMS/OTel 通道承载 trace/metrics。**不是事件流**：span 无业务事件类型、不可重放、不保证顺序，Event Log 的审计/回放语义必须新建 | third_party/agentteams/plugins/teamharness/adapters/qwenpaw/task_trace.py；third_party/agentteams/docs/cms-integration.md |

---

## 7. Projection

Event Log 可以生成多个 projection（与统一设计的两图一 Runtime 对齐）：

```text
CoordinationProjection:
  当前 Coordination Graph 状态、Phase Endpoint runnable/blocker、阶段交付满足度；
  图的热修改历史由 Event Log 审计（审计不限制热修改）。

AgentProjection:
  当前 active Invocations、历史 Invocation 和结果。

ContextProjection:
  当前 Context Graph revision、节点/子图索引；MemoryCandidate 缓冲与审查记录。

MergeProjection:
  当前 Merge Queue、已合入 candidate 与冲突关系。

UIPanelProjection:
  用户界面展示的进度、预算和风险。
```

---

## 8. 不变量

1. 关键状态变化必须进入 Event Log（由 Agent Runtime 自动记录，非 agent 显式写）；AgentTeams 无此机制，EventLogAdapter 为新建服务。
2. Task Manager Agent 和 Context Agent 也必须经 Agent Runtime 运行，它们不是日志旁路。
3. 大对象必须进 Artifact Store，并用 ArtifactRef 引用；Event Log 不内嵌大对象。
4. Verify failure 必须可追溯到测试输出或人工判断；Merge 必须可追溯到 verify result、diff 和 commit。
5. Human decision 必须显式记录。
6. Process transcript 是 Artifact（`agent_transcript`），但运行时私有：Task Manager 只读结构化边界输出（PhaseOutput / OrchestrationProposal / 已完成 endpoint 的 report、delivery、evidence），不读未提交过程上下文。
7. Context Graph 的高影响节点必须有 Event 或 Artifact 证据（SourceRefs），只经候选缓冲准入后落图。
8. 订阅与 Delta 的记录（创建、显式取消、Invocation 过期、投递、消费）必须进入 Event Log；Runtime 按当前 consumer 的有效订阅子图并集提供上下文，Delta 增量、可合并、可重放。
9. 图变更历史由 Event Log 审计，但审计机制不限制 Coordination Graph 的运行时热修改。
10. 系统状态应尽可能能从 Event Log 重放；事件顺序由 EventLogAdapter 保证，不复用 filesync 水位或 Matrix 时间线作游标。
11. 每 Task 一份候选缓冲，固定的 plan/execute/verify 三阶段共享，跨 Task 隔离；缓冲不属于 Context Graph。done 后冻结终审，成功落图后才触发图 revision 与推送。
