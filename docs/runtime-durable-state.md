# Runtime Durable State（M5-B）

状态：M5-B MVP。本文冻结单 Runtime / 单 control-plane deployment 的 durable Runtime state contract；不改变 Phase Agent public API，也不实现 restart 后的执行 reconciliation。

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

关键 mutation 在同一个 SQLite transaction 中提交 authoritative snapshot 和 append-only `runtime_events` outbox：

- Waiting created/state transitioned；
- PhysicalExecution created/state transitioned；
- package receipt recorded；
- PhaseOutput recorded；
- teardown progress recorded。

event envelope 包含 EventID、EventType、occurredAt、TaskID、InvocationID、Generation、optional ExecutionEpoch、aggregate key、resulting revision、payload version 与 redacted payload。Event Log 不是唯一 recovery authority：snapshots 用于在线恢复，outbox 用于审计、projection rebuild 和后续 reconciliation input。transaction rollback 或 CAS conflict 时不得留下 state 或 success event。

## 5. M4 与后续 M5 边界

M4 的 InMemory stores 继续保留给 unit tests 和 single-process fixture。M5-B 只保证 fresh repository instance 可以 cold-load durable facts，并继续执行 CAS；它不创建 Worker、重新签 token/credential、重放 QwenPaw session、reconcile Controller，或决定应继续/rollback/recreate carrier。

M5-C/D 才根据这些 records 实现 recovery claim、partial-teardown retry、stale carrier fencing 和 restart reconciliation；QwenPaw session 始终 disposable，任何新 carrier 都必须重新 materialize context/workspace/memory 并获得新 capability material。
