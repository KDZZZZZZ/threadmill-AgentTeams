# Runtime Development Status

状态：C4-6 healthy-carrier restart recovery 已通过真实 Docker/process fixture。

## 1. Purpose

本文件是 Runtime 当前开发状态与接手索引，供新加入工程师或全新 Codex 在不依赖历史聊天的情况下回答：

- Runtime 当前做到哪里；
- 哪些行为已经由 tests/fixtures 证明；
- 当前 architecture authority boundary 是什么；
- 接手时应先读哪些文件；
- 下一步应推进哪个 slice。

本文件不是完整设计规范，也不引入新的架构决策。设计原因、领域语义和 AgentTeams 映射仍以 [agent-runtime.md](agent-runtime.md)、[runtime-durable-state.md](runtime-durable-state.md)、[event-artifact-store.md](event-artifact-store.md) 和 [threadmill-agentteams-adapter-design.md](threadmill-agentteams-adapter-design.md) 为准。

## 2. Source of Truth

权威顺序如下：

1. Current code + tests；
2. Architecture/design docs；
3. `docs/runtime-development-status.md`；
4. Git commit history；
5. Historical chat/context。

如果本文件与 code/tests 冲突，以 code/tests 为准，并更新本文件。接手时必须先执行 `git status`；如果 worktree 有未提交改动，不得 reset、checkout、stash、clean 或覆盖这些改动。

## 3. Runtime Authority Model

### Runtime logical authority

SQLite-backed `internal/runtime.RuntimeStateRepository` 是 Runtime crash/recovery 后的 logical authority。当前接口聚合的 durable seams 包括：

- `WaitingStore`：logical invocation 的等待、rehydration、running 与 terminal 状态；
- `DurablePhaseInputStore`：immutable input revisions 与 continuation binding；
- `DurableContinuationStore`：continuation material 与 opaque refs；
- `PhysicalExecutionStore`：每个 execution epoch 的 carrier history、状态与 teardown flags；
- package `ReceiptStore`：按 logical identity + epoch 保存 package consumption receipt；
- `PhaseOutputStore`：正式且唯一的 PhaseOutput acceptance；
- `ArtifactStore`：durable artifact metadata 与 logical access；
- `LifecycleMutationStore`：跨 aggregate transactional mutation；
- `RuntimeEventOutbox`：有序 runtime events、consumer lease/checkpoint/ack；
- `RecoveryStateStore`：candidate discovery、RecoveryClaim 与 consistent snapshot；
- `DurableReconstructionStore`：Workspace、lease、Context Slice、Task Memory 和 execution descriptor 的 durable refs。

Snapshot records 是 online recovery authority。Outbox 用于 audit、projection、rebuild 和 at-least-once dispatch，不反向决定 snapshot truth，也不把 Runtime 变成 full event-sourced system。

### External execution evidence

AgentTeams Controller、Worker、TeamHarness task、Matrix room、QwenPaw runtime/runtime.yaml 是 execution-plane observed evidence。它们用于核验 physical carrier 是否存在、是否 Ready、identity 是否匹配、runtime generation/MCP 是否 applied、TeamHarness task 是否 current。

这些 evidence 不能创建、覆盖或反推 Runtime logical truth。特别是：

- Worker Ready 不等于 logical continuation 已恢复；
- TeamHarness `in_progress` 不等于 package receipt；
- TeamHarness `submitted`/result.md 不等于正式 PhaseOutput；
- Matrix/QwenPaw session 不拥有 Task、Invocation、Generation 或 epoch；
- runtime.yaml 与 Controller status 不能覆盖 SQLite snapshot。

### Ephemeral / forbidden durable data

当前 durable contract 明确禁止持久化：

- raw execution token、token value 或 private authorization header value；
- MCP credential value、Controller bearer auth、provider key；
- provider conversation/session state、QwenPaw conversation/session；
- hidden reasoning、private process/container/session state；
- workspace absolute/runtime-local filesystem path；
- raw Matrix/private transport credential。

允许持久化的是 opaque binding/credential/authorization/lease refs、redacted identity/hash、header name、logical refs、revision、epoch 和受控 artifact metadata。

## 4. Identity Model

### Logical invocation

`WaitingKey{TaskID, InvocationID, Generation}` 是 logical invocation recovery key，也是 RecoveryClaim 的唯一 key：

- `TaskID`：所属 Threadmill task；
- `InvocationID`：跨 carrier replacement 保持不变的 logical invocation；
- `Generation`：logical invocation generation，replacement 不自动递增；
- `BindingRef` + `InputRevision`：当前可信输入/binding identity，所有 receipt/output/mutation 必须匹配。

### Physical execution attempt

`PhysicalExecutionKey{TaskID, InvocationID, Generation, ExecutionEpoch}` 标识一次 physical carrier attempt。`ExecutionEpoch`：

- 在同一 logical invocation 内单调区分 carrier attempts；
- 是 stale writer、receipt、binding、teardown 与 replacement fencing 的一部分；
- 不是 RecoveryClaim 主键；claim 只记录 observed epoch 并以 owner/fence/revision 围栏；
- healthy-carrier restart 必须保留当前 epoch；confirmed lost carrier 才能 durable-reserve successor epoch。

Artifact ownership 是 logical ownership：`TrustedOwner` 包含 TaskID、InvocationID、Generation，不包含 ExecutionEpoch；`TestDurableArtifactAccessIsLogicalAcrossEpochAndFencedAcrossInvocation` 证明 artifact 可跨同一 logical invocation 的 epoch 使用，同时隔离其他 invocation。

### Deterministic external identities

Production helper/observer 当前使用：

```text
Worker:          tm-<InvocationID>-g<Generation>-e<ExecutionEpoch>
TeamHarness task: tm-phase-<taskflow-safe InvocationID>-g<Generation>-e<ExecutionEpoch>
```

实现位置：`internal/runtime/provisioning.go`、`internal/agenthost/agentteams/controller_reprovision.go`、`internal/agenthost/agentteams/controller_observer.go`。Observer 同时核对 durable Worker ID/name、TeamHarnessTaskID、room ref、desired/applied generation 和 MCP client ID。

## 5. Completed Milestones

### M4 — Await / rehydration / provisioning foundation

- authenticated `runtime.awaitInputs` relinquishes current carrier and retains logical refs；
- Waiting CAS、input revision rebinding 和 continuation material preserve logical identity；
- rehydration prepares a fresh epoch without restoring old token/session；
- production provisioning composes workspace lease、authorization、credential、Worker、runtime gate、MCP discovery、TeamHarness activation、package materialization/receipt；
- partial provisioning rolls back in reverse order；formal activation is distinct from package consumption and PhaseOutput。

关键 tests：`await_coordinator_test.go`、`rehydration_test.go`、`provisioning_test.go`、`package_consumption_test.go`，以及共享 Docker fixture `testdata/m4_d_rehydration_e2e/`。

### M5 — Durable Runtime State

- backend-neutral `RuntimeStateRepository` 与 SQLite schema/migrations 已落地；当前 schema version 为 6；
- WAL、foreign keys、busy timeout、explicit domain keys、revision CAS 与 cold reopen 已测试；
- Waiting、inputs/bindings、continuation、PhysicalExecution、receipt、PhaseOutput、teardown 和 reconstruction refs 均可 cold-load；
- `ActivatePhysicalExecution` 原子推进 Waiting/Physical；
- `AcceptPhaseOutput` 原子写 output、terminal Waiting 与 outbox；
- `AdvanceTeardown` 一次持久化一个 restart-safe step，并允许只重试未提交 step；
- malformed/newer schema、secret-at-rest、CAS conflict 和 outbox failure 均 fail closed/rollback。

关键 tests：`durable_repository_test.go`、`durable_lifecycle_state_test.go`、`lifecycle_mutations_test.go`、`completion_test.go`、`reconstruction_store_test.go`。

### C3 — Durable artifact / outbox

- artifact bytes publish/verify 与 metadata/access transaction 分离；metadata 不能先于 verified publish；
- durable registry 以 opaque ArtifactRef 暴露产物，不向 agent 返回 path/blob ref；
- registration/access 可 cold reopen，冲突 metadata、path escape、publisher/outbox failure fail closed；
- runtime outbox 按 database-assigned `EventSequence` 有序读取；
- consumer lease、strict monotonic ack/checkpoint、cold-reopen cursor 与 at-least-once redispatch 已测试；
- outbox dispatch/replay 不改变 authoritative snapshots。

关键 tests：`durable_artifacts_test.go`、`runtime_event_outbox_test.go`、`internal/agenthost/agentteams/durable_artifact_authority*_test.go`。

### C4 — Restart Recovery

#### C4-1 Recovery claim + classification

- RecoveryClaim 以 logical key 唯一，持有 owner、expiry、fence、revision 与 observed epoch；
- active claim 拒绝 concurrent claimant；expiry/takeover increments fence；stale owner 不能 assert/renew/release；
- `LoadRecoverySnapshot` 在一致 SQLite read transaction 中装配 current durable truth；
- classifier 覆盖 awaiting、incomplete relinquishment/rehydration/provisioning、active observation、consumed-no-output、terminal teardown/no-op、failed physical 与 reserved replacement；contradiction fail closed；
- claim、snapshot 与 classification 均有 cold-reopen/concurrency tests。

关键 tests：`recovery_test.go`、`restart_recovery_test.go`。

#### C4-2 Terminal/teardown recovery

- restart 后从 durable teardown flags 继续第一个未完成 step；
- fixed order 为 task → Worker → MCP → credential → token → lease → terminated；
- external effect 成功但 progress 未提交时允许幂等重试；已提交 step 不重做；
- 每次 external effect 和 authoritative mutation 前后都由 claim/fingerprint 围栏；
- accepted output、artifact、receipt 与 epoch history 在 cleanup 后保留。

关键 tests：`recovery_coordinator_test.go` 中 terminal、cold-reopen、outbox failure、claim/snapshot fencing cases。

#### C4-3 External carrier observation

- `ControllerPhysicalExecutionObserver` 是 production read-only adapter；
- 真实读取 Controller Worker status、desired/applied runtime generation 和 MCP applied readback；
- 通过 TeamHarness `check_task` 读取 task state/identity；
- 明确归一化 absent/provisioning/ready/terminating/failed 与 assigned/in-progress/completed/failed/cancelled；
- 404 与 timeout/network/5xx 严格区分；unknown evidence 不推导 lost；
- identity mismatch、generation pending 和 MCP not-applied 不会被误判为 healthy。

关键 tests：`internal/agenthost/agentteams/controller_observer_test.go`、`recovery_coordinator_test.go` observation cases。

#### C4-4 Lost-carrier fencing / epoch allocation

- `FenceAndAllocateReplacement` 在 claim-fenced SQLite transaction 中将 old carrier 标为 failed，并从完整 physical history 最大值分配 successor epoch；
- reservation 为 durable `PhysicalExecutionReserved`，记录 replaced epoch 并要求 fresh package receipt；
- retry/cold reopen 复用同一 reservation，不重复分配；
- late old-epoch mutation、stale claim、accepted output、unknown/healthy/terminating observation 均被围栏或拒绝；
- old receipt/artifact evidence 保留，但不能 authorize successor epoch。

这些能力已由 focused tests 证明，尚未由真实 lost-carrier Docker restart E2E 证明。关键 tests：`recovery_replacement_test.go`、`recovery_reconcile_invocation_test.go`。

#### C4-5 Reconstruction / reprovision / reconciliation

- repository 保存 durable Workspace、WorkspaceLease、ContextSlice、TaskMemory 和 ExecutionDescriptor refs；
- `DurableHostEnvelopeResolver` 只从 repository authority 重建 agent-visible HostEnvelope/package，并拒绝 binding/mount expansion；
- `ReplacementReprovisionCoordinator` 只消费已存在的 reserved epoch，claim-fence 每个 external provisioning boundary；
- failure 回到同一 reservation，不创建新 epoch；cold reopen 保留 reservation；
- `RecoveryCoordinator.ReconcileInvocation` 汇合 terminal、active observation、lost-carrier reservation 与 replacement reprovision action；
- `RestartRecoverySupervisor` 提供有界 candidate discovery/retry/convergence reporting，并使用 per-invocation claim 而非 global lock。

关键 tests：`reconstruction_store_test.go`、`durable_host_envelope_test.go`、`recovery_reprovision_test.go`、`recovery_reconcile_invocation_test.go`、`restart_recovery_test.go`。

#### C4-6 Restart E2E — Healthy Carrier

最新真实 checkpoint 由 `testdata/c4_6_restart_e2e/run-active-runtime-smoke.ps1` 启动，并复用 `testdata/m4_d_rehydration_e2e/run.ps1 -ActiveRuntimeSmoke` 的 Docker topology/cleanup。实际 PASS 路径为：

1. 启动真实 Docker AgentTeams embedded Controller；
2. 创建 deterministic real Worker，等待 Controller `Ready/running`；
3. 调用真实 TeamHarness `delegate_task`、worker `ack_task`、leader `check_task`，确认 task `in_progress`；
4. 取得 redacted Worker/task/runtime/MCP descriptor；
5. descriptor 跨 OS process 传给 Runtime A；
6. Runtime A 自行打开 SQLite，写 durable continuation/input、Waiting 与 PhysicalExecution；
7. 记录唯一 package receipt；atomic activation 将 Waiting/PhysicalExecution 都推进为 `running`；
8. cold reopen 后确认唯一 physical、Waiting、receipt，PhaseOutput absent；
9. parent 以 OS `Process.Kill` 非正常终止 Runtime A，不调用 Runtime Close/teardown；
10. fresh Controller readback 确认 Worker 仍 `Ready/running`、generation applied、MCP applied；
11. fresh real TeamHarness `check_task` 确认同一 task 仍 `in_progress`；
12. 启动独立 Runtime B，PID 与 parent/Runtime A 均不同；
13. Runtime B 自行 reopen 同一 SQLite，durable discovery 得到唯一 candidate；
14. `RestartRecoverySupervisor` → RecoveryClaim → snapshot/classification → production `ControllerPhysicalExecutionObserver`；
15. Worker/task/deterministic identity/runtime generation/MCP 全部 verified；
16. `ReconcileInvocation` 返回 `carrier_still_active`，supervisor convergence 为 `stable_active`；
17. RecoveryClaim release；TaskID/InvocationID/Generation/Epoch/BindingRef/InputRevision/Worker/TeamHarness task 均不变；
18. 没有 replacement、第二 PhysicalExecution、第二 receipt、PhaseOutput 或 teardown progress；
19. recovery 验证完成后 fixture 才删除 Worker/credential/container/network/volume。

直接证据：

- `internal/runtime/restart_process_harness_test.go`：Runtime A crash、process isolation、same SQLite、durable invariants 与 final marker；
- `internal/runtime/restart_production_observer_test.go`：Runtime B production composition/readback、supervisor result、claim release 与 no-duplicate checks；
- `internal/agenthost/agentteams/controller_observer.go`：production observer；
- `testdata/c4_6_restart_e2e/run-active-runtime-smoke.ps1` 与共享 `testdata/m4_d_rehydration_e2e/run.ps1`：真实 Docker/TeamHarness orchestration。

## 6. Current C4-6 Verified Invariants

- [x] Runtime A、Runtime B 与 parent 是不同 OS processes（`TestC46RealWorkerDescriptorDurablePhysicalExecution`）。
- [x] Runtime A abnormal kill 后同一 SQLite 可由 Runtime B reopen；parent 不打开 Runtime repository。
- [x] crash 不触发 Runtime teardown；真实 Worker/task 在 Runtime A 死亡后继续 active。
- [x] Runtime B discovery 只得到一个 recoverable candidate。
- [x] Runtime B composition 使用真实 `ControllerPhysicalExecutionObserver`，不是 fake/stub。
- [x] Worker deterministic name、TeamHarness deterministic task ID 与 durable identity 对齐。
- [x] Controller Worker `Ready/running`、runtime desired/applied generation 和 MCP applied 被真实 readback。
- [x] TeamHarness `check_task` 返回同一 task `in_progress`。
- [x] healthy recovery 返回 `carrier_still_active` / `stable_active`。
- [x] RecoveryClaim 在 reconciliation 后 release。
- [x] ExecutionEpoch、BindingRef、InputRevision 和 physical carrier identity 不变。
- [x] 不创建 successor reservation、replacement Worker 或第二 TeamHarness task。
- [x] physical history 仍只有一条；receipt 仍唯一且 revision 为 1。
- [x] Waiting/PhysicalExecution 仍为 `running`；PhaseOutput absent；teardown 未开始。
- [x] package-consumption/activation duplicate paths 不产生第二 success event；focused supervisor tests 也证明 healthy recovery 不 replay duplicate effects。
- [x] closed SQLite secret scan 未发现 raw token、private header、controller auth 或 provider/session state。

## 7. Current Tests / Fixtures Map

### Unit / focused tests

- `durable_repository_test.go` / `durable_lifecycle_state_test.go`：schema、CAS、secret rejection、cold reopen 与全套 M4 logical authority。
- `lifecycle_mutations_test.go` / `completion_test.go`：activation、PhaseOutput acceptance、teardown transaction/outbox/idempotency。
- `runtime_event_outbox_test.go`：ordered feed、lease、ack/checkpoint、at-least-once 与 cold reopen。
- `durable_artifacts_test.go`：artifact registration/access、logical cross-epoch ownership、publish/outbox failure。
- `recovery_test.go`：RecoveryClaim、consistent snapshot、classifier 与 schema migration。
- `recovery_coordinator_test.go`：terminal recovery、observation、claim/fingerprint fencing。
- `recovery_replacement_test.go`：lost decision、old carrier fencing、history-max epoch reservation、stale mutation rejection。
- `recovery_reprovision_test.go`：reserved replacement consumption、fresh activation、failure/cold-reopen behavior。
- `recovery_reconcile_invocation_test.go`：single reconciliation entrypoint and actions。
- `restart_recovery_test.go`：discovery、bounded supervisor、cross-process/cold-reopen、concurrent supervisors。
- `controller_observer_test.go`：production observer state normalization and identity verification。
- `restart_process_harness_test.go`：C4-6 process/SQLite/crash parent harness。
- `restart_production_observer_test.go`：C4-6 Runtime B production observer/supervisor child。

### Docker fixtures

- `testdata/c4_6_restart_e2e/run-smoke.ps1`：只验证 embedded Controller bootstrap、CLI token presence、real workers endpoint 与 cleanup；不创建 Worker、不打开 Runtime SQLite。
- `testdata/c4_6_restart_e2e/run-worker-smoke.ps1`：创建 real Worker，验证 Ready/runtime/MCP、real TeamHarness active task 与 descriptor-backed Runtime A durable state；不执行 active Runtime crash recovery。
- `testdata/c4_6_restart_e2e/run-active-runtime-smoke.ps1`：在 Worker smoke 基础上 kill Runtime A，启动 production Runtime B，完成 healthy-carrier restart recovery 与 final cleanup。

### Shared M4 fixture

C4-6 wrappers 暂时复用 `testdata/m4_d_rehydration_e2e/run.ps1` 的 Controller/AppService/Matrix/MinIO/network bootstrap、real Worker/TeamHarness helpers、test credential lifecycle 和 `finally` cleanup。该共享避免复制 Docker topology，但 C4-6-specific behavior must not silently redefine M4 semantics；新增 crash/recovery 行为应继续由 C4-6 tests/wrappers 显式选择。

`testdata/c4_6_restart_e2e/README.md` 已同步 Controller smoke、Worker lifecycle/durable descriptor smoke 和 active Runtime healthy-carrier recovery 三个入口；实际行为仍以 tests 和 scripts 为最高依据。

## 8. Known Gaps / Not Yet Proven

### C4-6 lost-carrier Docker restart E2E

Focused code/tests 已覆盖 lost observation decision、old carrier fencing、successor reservation、fresh reprovision、activation 与 stale old mutation rejection；但尚未在一个真实 Docker/process restart fixture 中证明完整链路：

```text
Runtime A active → crash
→ external Worker/task confirmed lost/failed
→ Runtime B production observation
→ old epoch fenced
→ one successor epoch reserved
→ fresh credential/Worker/TeamHarness task/package receipt
→ durable activation
→ stale old epoch cannot mutate
```

### Crash matrix

当前真实 fixture 只覆盖 receipt-backed active/running carrier 的 kill point。尚未用真实 Docker/process fixture 覆盖 provisioning、delegated、accepted、receipt/activation transaction 边界、replacement reservation/reprovision 中途，以及 terminal teardown 各 external-effect/progress-commit 边界。

### Full production startup composition

`RestartRecoverySupervisor.RecoverStartup` 已实现并在 unit/process/Docker fixture 中组合，但 repository search 未发现它接入最终 production Runtime startup service 或 admission gate。当前 C4-6 Runtime B 是测试进程 composition，不是常驻 production launcher。

### Docker/controller E2E coverage

- 尚未真实证明 Controller 404/failed Worker + missing/failed task 导致完整 replacement path；
- 尚未真实证明 Runtime B replacement 后新的 Worker/task/package receipt/activation；
- 尚未真实证明旧 Worker/session 在 replacement 后尝试 authoritative mutation 会被拒绝；
- 尚无多次连续 Runtime crash/takeover 的真实 Docker recovery fixture。

## 9. Next Recommended Slice

**C4-6 lost-carrier restart E2E**。

目标：复用现有 active-runtime Docker/process fixture，在 Runtime A crash 后让 external Worker/task 形成可确认的 lost/failed evidence；由独立 Runtime B 使用 production observer 和现有 reconciliation/reprovision composition，证明 old epoch fenced、唯一 successor reservation、fresh carrier/package activation，以及 stale old epoch mutation rejection。

本 slice 不应先扩展 public API、MCP schema 或 third_party；若真实 fixture 暴露更小的 production-composition prerequisite，应先以最小内部 seam 补齐并记录。

## 10. New Chat / Handoff Procedure

新工程师或全新 Codex 应：

1. 在当前 worktree 运行 `git status` 和 `git diff --stat`，保留所有未提交改动；
2. 运行 `git log --oneline -15`；
3. 阅读本文件；
4. 阅读第 1 节链接的 architecture docs；
5. 阅读 Current C4-6 与 Next Recommended Slice 对应 tests/fixtures；
6. 运行与目标 slice 匹配的 focused tests；
7. 只继续第 9 节的 Next Recommended Slice，除非 code/tests 已显示状态变化。

不要依赖历史聊天恢复 source of truth；任何冲突都回到 current code + tests。
