# Runtime Durable State（M5-B）

状态：M5-C4-1。本文冻结单 Runtime / 单 control-plane deployment 的 durable Runtime state contract；不改变 Phase Agent public API，也不实现 external carrier 的 restart reconciliation。

## 1. Authority 与边界

Runtime 的在线 recovery authority 是同一个 durable repository 中的快照记录：WaitingRecord、immutable PhaseInputSet 与 continuation binding、ContinuationMaterial、PhysicalExecution history、package-consumption receipt、PhaseOutput 与 teardown progress。它们使用既有 TaskID / InvocationID / Generation / ExecutionEpoch identity，不由 Worker、TeamHarness task、Matrix、runtime.yaml 或 QwenPaw session 反推。

Artifact bytes、Context 内容、Task Memory 内容和 Workspace 文件不复制到 Runtime database。Runtime 只保存已有的 opaque ref、workspace identity/revision 和 AllowedDirs；后续 recovery 必须向各自 authority 重新 materialize。Worker CR、Controller status、runtime.yaml 和 TeamHarness task 是 execution-plane evidence，供后续 reconciliation 核验，不是 logical state authority。

永远不得持久化 raw execution token、credential value、private header value、controller auth、provider key、provider conversation、hidden reasoning，或 process/container/private-session state。允许的字段仅为 binding/credential/authorization opaque ref、header name、redacted hash/identifier、epoch、revision 与受控 logical refs。

## 2. Repository 与 SQLite

`internal/runtime` 公开 backend-neutral `RuntimeStateRepository` seam。SQLite implementation 位于 Runtime-internal package，SQL 类型和 `database/sql` 不泄漏到 `phaseagent` 或 domain contracts。MVP 使用一个可配置的 DB file，打开时启用 `foreign_keys=ON`、WAL journal mode 和明确 busy timeout；WAL 适合单机 crash durability 与读写并发，但不宣称 multi-writer leader election。

迁移由可重复、事务性的版本 runner 管理。empty database 升级至 latest；已是 latest 时 no-op；未知更高版本 fail-closed。domain identity 永远是显式 key，SQLite rowid/autoincrement 不作为领域身份。

## 3. CAS 与 recovery claim

WaitingRecord 与 PhysicalExecution 的 revision 是 authoritative fencing value。比较更新必须执行等价于 `UPDATE ... WHERE key=? AND revision=?` 的原子 SQL；零行更新是 conflict，不能写成功 event。receipt 和 PhaseOutput 分别以既有 key 做 idempotent insert：完全相同返回既有 record，不同内容 fail closed。

schema 预留 recovery claim owner/ref、claim revision 和 epoch 边界，但 M5-B 不实现 distributed leader election 或 restart reconciliation。单 writer 是部署限制，不是弱化 CAS/epoch identity 的理由。

## 4. Transactional outbox

已经接入 repository-owned transaction 的关键 mutation，在同一个 SQLite transaction 中提交 authoritative snapshot 和 append-only `runtime_events` outbox：

- Waiting created/state transitioned；
- PhysicalExecution created/state transitioned；
- package receipt recorded；
- PhaseOutput recorded；

其余 lifecycle state 的 durable outbox 接入按各 slice 逐步补齐；不能因某个 typed store 自己可持久化就推断它已经拥有跨 aggregate transaction。

event envelope 包含 EventID、EventType、occurredAt、TaskID、InvocationID、Generation、optional ExecutionEpoch、aggregate key、resulting revision、payload version 与 redacted payload。Event Log 不是唯一 recovery authority：snapshots 用于在线恢复，outbox 用于审计、projection rebuild 和后续 reconciliation input。transaction rollback 或 CAS conflict 时不得留下 state 或 success event。

## 5. M4 与后续 M5 边界

M4 的 InMemory stores 继续保留给 unit tests 和 single-process fixture。M5-B 只保证 fresh repository instance 可以 cold-load durable facts，并继续执行 CAS；它不创建 Worker、重新签 token/credential、重放 QwenPaw session、reconcile Controller，或决定应继续/rollback/recreate carrier。

M5-C1 将上述七类 M4 logical-state seam 收敛为 `DurableLifecycleState`：同一 repository 同时提供 Waiting、Continuation、immutable input/binding（含 cold-reopen 读取）、PhysicalExecution、receipt 和 PhaseOutput store，禁止 recovery path 混用 durable 与 process-local authority。协调器继续接收既有窄接口，因而不改变 `phaseagent` public API；InMemory store 仍只用于 unit tests 和显式 fixture。

后续 M5-C2/D 才补齐 input/binding/continuation 事件 outbox、recovery claim、partial-teardown retry、stale carrier fencing 和 restart reconciliation；QwenPaw session 始终 disposable，任何新 carrier 都必须重新 materialize context/workspace/memory 并获得新 capability material。

M5-C2-4 将 rehydrated activation 收敛为 repository-owned transaction：receipt 已确认的 epoch PhysicalExecution 从 `accepted` 转为 `running` 时，WaitingRecord 同时从 `rehydrating` 转为 `running`，并写入唯一 `PhysicalExecutionActivated` outbox event。任何 CAS、identity、state 或 outbox failure 都 rollback 两个 snapshot；相同已完成 activation 的 retry 是幂等的。外部 carrier 创建仍在 transaction 外，Runtime snapshot 仍是 recovery authority；outbox 仅供 audit、projection 与后续 reconciliation input。

M5-C2-5 将正式 PhaseOutput acceptance 收敛为 repository-owned transaction。`LifecycleMutationStore.AcceptPhaseOutput` 在同一 SQLite transaction 中完成三项 authoritative mutation：以 logical TaskID / InvocationID / Generation key 写入唯一 PhaseOutput、将同一 WaitingRecord 从 `running` 转为 `terminal`，并追加唯一 `PhaseOutputSubmitted` outbox event。BindingRef、InputRevision、Generation、ExecutionEpoch、Waiting revision 和状态都在提交前验证；stale、CAS conflict 与不同 output fail closed。outbox insert 失败会回滚 output 和 Waiting transition，因而 failure 不会启动 completion cleanup。相同 output 的 retry 返回既有 acceptance，不覆盖 output，也不会追加第二个 success event。

关闭并重新打开 repository 后，PhaseOutput、terminal WaitingRecord 和唯一 outbox event 仍是同一 acceptance 的 recovery authority；重开本身不重新提交 output、不重新发 success event，也不自动启动 logical completion。`PhaseOutputCompletionCoordinator` 在配置 durable mutation seam 时不再使用旧的 output → external event → Waiting CAS 分步 authority；只有该 transaction 成功后才进入现有 normal cleanup。

M5-C2-6 将 normal completion 与 await relinquish 的 PhysicalExecution cleanup 接入 `LifecycleMutationStore.AdvanceTeardown`。它以 physical key + expected revision 围栏，且每次 transaction 只持久化一个动作：`running` → `tearing_down` intent、一个已成功外部副作用的 redacted step completion，或在全部六个 step 已完成后 `tearing_down` → `terminated`。每一动作与对应 outbox event 原子写入；identity、state、revision 或 outbox failure 都回滚，不会留下 progress/event 半状态。已持久化的 step retry 返回既有记录且不产生第二个 success event。

外部 cleanup 始终在 transaction 外，顺序保持 task → Worker → MCP → credential → token → workspace lease。Runtime 先 durable-record `tearing_down` intent，再调用外部 idempotent port；调用成功后才 durable-record 该 step。进程在两者之间退出时，新的 Runtime 从 PhysicalExecution teardown flags 读取第一个未完成 step 并可能重新调用该 idempotent effect；一旦 completion flag 已提交，cold reopen/retry 绝不重做它。final termination 只在全部 step flags 已提交后记录。opaque authorization/binding/credential refs 可以重新解析；raw token、credential、private header、controller auth、provider conversation 和旧 QwenPaw session 都不进入 database，旧 session 永远不会恢复。

M5-C3-3 为已有 transactional outbox 增加 durable projection/dispatch seam，但不改变 snapshot authority。每个 Runtime event 在产生它的原事务中写入 `runtime_events` 及数据库分配的单调 `EventSequence`；读者按 sequence 而非时钟或 EventID 排序。旧 schema 的 event 在 v2→v3 migration 中一次性获得稳定 sequence，之后的 event 不会重排。payload 必须为已支持的 version 和有效 JSON；未知 version 或畸形 payload fail closed，不能被投影器静默跳过。

`RuntimeEventOutbox` 提供有界 ordered read、consumer checkpoint、lease claim 与严格单调 ack。一个 logical consumer 在有效 lease 内只允许一个 owner dispatch；lease 过期后新的 process 可接管。dispatcher 在 SQLite transaction 外调用外部 sink，成功后才单条 ack，因此 delivery 是 **at-least-once**：sink 已成功但 process 在 ack 前退出时，同一 `EventID` 会在 cold reopen 后再次投递。sink/projection 必须以 `EventID` 去重；checkpoint 只在 ack transaction 成功后推进。ack、claim 或 outbox 写入失败不改变 authoritative snapshots，也不制造已确认的 checkpoint。

Runtime snapshots/records 继续是 online recovery authority；outbox feed 只用于 audit、projection rebuild、reconciliation input 和可恢复 dispatch，不把本系统改成 full event sourcing。`ArtifactRegistered`（metadata/access transaction）、`PhaseOutputSubmitted`（output + terminal Waiting transaction）与 PhysicalExecution teardown 事件已经随各自 authoritative mutation 原子写入；dispatch 状态不能反推 artifact ownership、PhaseOutput 或 teardown truth。C3-3 也不负责新的 external sink、global multi-node leader election 或重放 QwenPaw session。

M5-C4-1 新增 Runtime-internal durable `RecoveryClaim`，以 logical `TaskID + InvocationID + Generation` 为唯一 key；`ExecutionEpoch` 只作为 claim 时的 observed physical epoch，不允许不同 epoch 并行取得同一 logical invocation 的 recovery ownership。claim 保存 recovery actor `OwnerID`、expiry、单调 `Fence`、CAS `Revision` 与审计时间；它不是 Worker lease、workspace lease、execution token 或 agent authorization。active claim 拒绝其他 owner；过期或显式释放后的接管递增 Fence；renew/release/assert 都要求当前 owner、Fence、Revision 和有效 lease 一致，故过期后恢复的旧 actor 不能再提交后续 authoritative recovery mutation。

repository 的 `LoadRecoverySnapshot` 在一次 SQLite read transaction 中读取 Waiting、latest PhaseInputSet、continuation/binding refs、physical epoch history/current epoch、receipt、PhaseOutput 和可见 ArtifactRefs，并生成 aggregate revision fingerprint。snapshot 从不包含 workspace absolute path、token、credential value、private header、controller auth、provider conversation、hidden reasoning 或 QwenPaw session state。C4-1 classifier 只按该 durable truth 产生 `AwaitingInput`、`RelinquishmentIncomplete`、`RehydrationIncomplete`、`CarrierProvisioningIncomplete`、`CarrierActiveNeedsObservation`、`CarrierConsumedNoOutput`、`ContinueTerminalTeardown`、`TerminalNoOp` 或 `FailedPhysicalExecutionNeedsRecoveryDecision`；矛盾状态 fail closed。它不查询 AgentTeams、不分配 epoch、不执行 cleanup 或重建 carrier。external observation、terminal cleanup drive、lost-carrier fencing 与 fresh provisioning 属于后续 C4 slices。

Runtime snapshots/records 仍是 recovery authority；outbox 仅用于 audit、projection 和后续 reconciliation input。Worker/task/runtime.yaml 与 QwenPaw session 只能作为 execution-plane observed evidence，不能反推下一 cleanup step。C2-6 不实现 distributed leader election：SQLite 单 writer + revision CAS 使同一 expected revision 只有一个 teardown claimant 成功；其他 claimant 必须 reload/retry。restart recovery 只继续 cleanup，不重新接受 PhaseOutput、不重新发 `PhaseOutputSubmitted`，也不重放 agent work。

M5-C3-1 将 Artifact 的 logical metadata、hash→ref mapping 与 TaskID+InvocationID access grant 纳入同一 Runtime SQLite authority。artifact bytes 仍属于外部 blob/object store；publisher 成功但 SQLite metadata transaction 失败会留下可 GC orphan blob，反向绝不提交指向未验证 blob 的 metadata。metadata insert、origin-owner grant 和唯一 `ArtifactRegistered` outbox 在一个 transaction 中完成；相同 content hash 的相同 metadata/grant retry 幂等，但不同 ArtifactType 或 blob ref fail closed。ExecutionEpoch 仅可作为审计 evidence，绝不进入 logical ownership key。此处仍不是 full event sourcing：snapshot 是 online recovery authority，outbox 仅为 audit/projection/reconciliation input；dispatcher/cursor 不属于 C3-1。

M5-C3-2A 补齐 C3-1 所需的 production-capable bytes publisher 与组合 seam：`artifacts.S3BlobPublisher` 使用 S3-compatible MinIO `PUT` + `HEAD` verification 发布 content-addressed bytes，`runtime.NewDurableArtifactRegistry` 只将该 publisher 与同一 repository 的 `ArtifactStore()` 组合。S3 endpoint/bucket/prefix/access key/secret key 是进程内 deployment configuration；credential 不进入 SQLite、outbox 或日志。MCP handler 暂未切换 registrar，因而 InMemoryRegistry 仍是当前未完成 C3-2 的旧 production path，不能被误认为 durable authority。C3-2B 必须在该 publisher 已验证的前提下完成唯一 registrar cutover，且仍不得改变 Phase Agent/MCP public contract。

M5-C3-2B 将 `NewDurableArtifactAuthority` 作为 AgentTeams production composition：同一 RuntimeStateRepository 的 ArtifactStore 与同一 S3 publisher 只构造一个 DurableRegistry，随后同时注入 Phase MCP handler 和 AgentTeams evidence ingestion。MCP token resolve 后的 `InvocationBinding` 是 TaskID、InvocationID、Generation 与 workspace fence 的唯一来源；taskflow result、agent request body、Worker/session/epoch 都不能重定 owner。`DurableRegistry` 先经 canonical/symlink-safe workspace fence 读取控制文件并计算 hash，publisher 复用同一已打开文件重新校验 hash 后上传，故文件在两次读取之间变化会 fail closed，metadata 不会把 A 的 hash 指向 B 的 bytes。重开 repository 后同一 TaskID+InvocationID 的新 Epoch 仍有 access；另一 Invocation 被拒绝。C3-2B 不改变 public MCP schema，也不把 InMemoryRegistry 作为 durable mirror。
