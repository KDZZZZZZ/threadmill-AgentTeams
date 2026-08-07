# Workspace 与合并设计（AgentTeams 基座）

版本：v0.4（重写）
状态：Draft
定位：本文说明《统一设计》的 Workspace 与 Merge Queue 语义如何在 third_party/agentteams（AgentTeams v1.2.x 归档基座）上实现：一 Attempt 一 Workspace、三 phase 共享、phase lease、四种 Workspace 实现形态、write set、Merge Queue。
> 语义以 docs/threadmill-unified-design.md 为准（下称《统一设计》，见其第 4、8 节），本文只补充实现映射；术语冲突时以《统一设计》为准。

---

## 1. 定位与基座现实

《统一设计》的核心规则：

1. 同一 Task Attempt 的 plan、execute、verify 默认共享同一个 Workspace Binding（§4.1）。
2. Workspace 实现形式可替换：Git 仓库（`git worktree + branch`，默认）、独立 clone/目录、容器加持久 volume、远程 sandbox；上层契约相同（稳定 Workspace ID、固定基线、允许写范围、可观测变更、阶段间持久、Attempt 间隔离）（§4.1）。
3. 权限随 phase lease 切换：plan 默认只读源码，execute 可写批准范围，verify 默认不可修改候选实现；任何阶段只有一个有效写 lease（§4.3）。
4. `verify passed` 只获得进入 Merge Queue 的资格；Merge Queue 是 main 的唯一写入口，在 latest main 上机械检查、targeted verify、串行合并（§8.2）。
5. 冲突或复验失败产生 evidence，由 Task Manager 将受影响阶段编排回 plan/execute/verify 或 waiting_human（§8.2、§8.3）。

**AgentTeams 基座现实（先讲清楚，避免虚构现成能力）：**

- AgentTeams 只有四类隔离原语：**目录隔离**（workspace 目录树）、**MinIO 对象存储隔离**（`agents/<name>/*`、`shared/*`）、**Pod 隔离**（每 worker 一个 Pod）、**WorkerFlow 临时 agent 隔离**（`tmp-` agent + run 级共享目录）。
- AgentTeams **没有** git worktree 抽象、**没有** git 自动合并、**没有** Merge Queue、**没有** main 分支概念。git 协作在 AgentTeams 中是“委派给 worker 的技能”（`manager/agent/worker-skills/git-delegation`、`manager/agent/worker-skills/github-operations`；`tests/test-13-git-delegation.sh`、`tests/test-14-git-collab.sh` 演示多 worker 通过共享 git 仓库人工协作），平台本身不做合并裁决。
- AgentTeams 的结果合流是 Leader `check_task` + `accept_task_result`（LLM/人工判断）+ requester report（`ProjectMeta.requester_report`），没有机械化合入。

因此：**Workspace Binding、phase lease、write set、Merge Queue 全部是 Threadmill 新建**；AgentTeams 提供的是可复用的隔离原语与执行宿主。下文按"直接复用 / 适配封装 / Threadmill 新建 / 不应复用"四类标注每一能力。

---

## 2. 隔离原语盘点（直接复用）

| 原语 | 位置（third_party/agentteams） | 能力与边界 |
| --- | --- | --- |
| Worker Pod | `agentteams-controller/api/v1beta1/types.go`（Worker/WorkerSpec/WorkerVolumeSpec）；`agentteams-controller/internal/service/deployer.go` | 每 worker 一个 Pod（`backendRuntime=pod`）；`spec.state` Running/Sleeping/Stopped；`deployMode` Local/Edge；`resources`、`labels`、`env`。worker 无状态：配置与记忆都在 MinIO，Pod 可随时销毁重建（`worker/README.md`） |
| 目录树 | `worker/scripts/worker-entrypoint.sh`（HOME=`agents/<workerName>/`）；`plugins/teamharness/mcp/server.py`（`_project_dir`/`_task_dir`、global-shared 只读）；`plugins/teamharness/skills/team/task-execution/SKILL.md` | `agents/<name>/` = worker 私有 workspace；`shared/projects/{id}/`（Leader 拥有 meta.json/plan.md/result.md）；`shared/tasks/{id}/`（Leader 拥有 meta.json/spec.md，worker 拥有 workspace/、progress/、result.md）；`global-shared/` 只读 |
| MinIO 对象存储 | `qwenpaw/src/qwenpaw_worker/sync.py`（FileSync）；`shared/lib/worker-file-sync.sh`；`qwenpaw/README.md` §1.2 | mc mirror 拉取/推送；排除 credentials、sessions、logs、tool results、media、file store、runtime cache；`.last-pull` 标记防回推；“写者推送 + @mention 按需拉取”（`worker/scripts/worker-entrypoint.sh` File Sync Design Principle） |
| WorkerFlow 临时隔离 | `plugins/workerflow/mcp/server.py`（`create_temp_agent`、`_safe_cleanup_workspace`、`_safe_cleanup_shared`、`_setup_shared_dir`）；`plugins/workerflow/skills/agent/worker-internal-workflow/SKILL.md` | 临时 agent id 必须 `tmp-` 前缀；独立 workspace（不得指向默认 workspace）；run 级共享目录 `<default-workspace>/shared/workerflow/<runId>/`（`inputs/`、`outputs/<agent-id>/`）；`cleanup_shared` 只允许该根下；`cleanupWorkspace` 随 `delete_temp_agent` |
| 存储权限 | `agentteams-controller/api/v1beta1/types.go`（AccessEntry 注释、CredentialBinding.ToolWhitelist） | 默认 object-storage scoped `agents/<name>/*` 与 `shared/*`；凭据值永不进入 runtime.yaml |

**不应复用**：MinIO 文件覆盖推送不是合并；`accept_task_result` 不是 merge；AgentTeams 的 git 委派技能不是平台 merge 能力；Worker Pod 是 per-worker 持久宿主，不等于 per-attempt 容器 workspace。

---

## 3. Threadmill Workspace Binding（新建）

### 3.1 数据模型

沿用《统一设计》§4.2：

```go
type WorkspaceBinding struct {
    ID              string            `json:"id"`
    TaskID          string            `json:"task_id"`
    AttemptID       string            `json:"attempt_id"`
    Kind            string            `json:"kind"` // git_worktree | clone | container | remote
    Root            string            `json:"root"`
    BranchName      string            `json:"branch_name,omitempty"`
    ContainerID     string            `json:"container_id,omitempty"`
    VolumeRefs      []string          `json:"volume_refs,omitempty"`
    BaseRevision    string            `json:"base_revision"`
    CurrentRevision string            `json:"current_revision"`
    AllowedDirs     []string          `json:"allowed_dirs"`
    DeclaredWrites  WriteSet          `json:"declared_writes"`
    ObservedWrites  WriteSet          `json:"observed_writes"`
    PhaseLeases     map[string]string `json:"phase_leases"` // phase -> invocation id
    Status          string            `json:"status"`
}
```

Kind 与基座实现映射：

| Kind | Threadmill 语义 | 基座落点 | 复用类型 |
| --- | --- | --- | --- |
| `git_worktree` | 默认方案：独立 worktree + branch | Threadmill 新建 git 层；目标仓库可以是委派给 worker 的共享 git 仓库（复用 git-delegation 技能的操作方式），但 worktree 创建/分支管理/合并裁决是 Threadmill 的 | 操作技能复用，worktree 语义新建 |
| `clone` | 非 Git 或强隔离任务 | 目录复制进 task workspace，经 MinIO 同步分发；基座无 clone 概念 | 隔离原语复用，语义新建 |
| `container` | 容器 + 持久 volume | 复用 QwenPaw/OpenClaw worker 镜像与启动机制（`qwenpaw/Dockerfile`、`worker/Dockerfile`）；per-attempt 容器生命周期由 Threadmill 新建调用层（或复用 Worker CR 的 state 状态机管理执行宿主） | 镜像/启动复用，生命周期新建 |
| `remote` | 远程 sandbox | 复用 controller `deployMode=Edge` 的远程宿主通道（types.go DeployMode 注释）；sandbox 语义 Threadmill 新建 | 通道复用，语义新建 |

### 3.2 一 Attempt 一 Workspace 的落点

- **Attempt 创建**：Task Manager 在 Coordination Graph 中创建 Attempt；Scheduler 选择 runnable endpoint 后，Runtime 向 Workspace Service 请求创建 Binding，并经 controller 选择或创建执行宿主。默认逻辑落点为 `shared/tasks/{attempt_id}/`，复用 TeamHarness 存储布局与 MinIO 物理同步，但 Workspace 身份、基线和 revision 由 Threadmill 记录。
- **三 phase 共享**：plan / execute / verify 的委派都绑定同一 Workspace ID。Workspace Service 在每个 phase 启动前物化并校验该 Binding 的指定 revision；MinIO 只承担物理传输与恢复，不作为并发合并或 revision 权威。Pod 重建或更换 worker 后，Runtime 仍从同一 Binding 恢复 Attempt 现场。
- **目录内阶段约定**（Threadmill 约定，避免与 TeamHarness 的 spec/result 冲突）：

```text
shared/tasks/{attempt_id}/
  meta.json          # TeamHarness 委派状态（直接复用，不写 Threadmill 图状态）
  spec.md            # Task Contract + DeliverySpec/ReportSpec + phase lease 声明（适配封装）
  plan/              # plan 产物：Approved Plan、Declared Write Set、验证计划
  workspace/         # execute 候选实现现场（默认实现区）
  evidence/          # verify 检查证据
  result.md          # PhaseOutput 载荷
```

- **Attempt 隔离**：新 Attempt 一律新目录（`{attempt_id+1}`），不能在验证失败的旧现场上无审计地继续修改（统一设计 §4.4）；旧目录封存为 evidence，按保留策略清理（复用 MinIO 生命周期管理）。
- **临时 Attempt**：WorkerFlow run 级共享目录（`<default-workspace>/shared/workerflow/<runId>/`）可作为一次性 Attempt 的 Workspace；`runId` 关联 AttemptID，结束后 `cleanup_shared` 清理（agent-runtime.md §3.2）。

### 3.3 phase lease

语义：任一时刻一个 Attempt 只有一个有效写 lease；plan 默认只读源码（可写 `plan/` 产物），execute 可写批准范围（`workspace/`），verify 默认只读实现（可写 `evidence/`）。**AgentTeams 没有 phase lease 概念**，Threadmill 用三层 enforcement：

```text
a) 委派轮次隔离（强）：每 phase 一次 taskflow 委派给不同 worker / 每 phase 一个 WorkerFlow 临时 agent
   -> 容器或进程级隔离，天然满足"一个写 lease"；
b) 工具级（中）：MCP allow policy（QwenPaw MCP Policy API）+ 目录 ACL（AccessEntries）+ AllowedDirs
   -> plan 只授只读工具与 plan/ 写权；execute 授 workspace/ 写权；verify 只授检查工具与 evidence/ 写权；
c) 提示词级（弱，兜底）：phase prompt 声明只读/只写边界。
```

状态转换：

```text
prepared
  -> plan_leased -> plan_done        （plan 通过后冻结 Approved Plan 与 Declared Write Set）
  -> execute_leased -> execute_done  （复用同一 Workspace；观察 diff 与 Observed Write Set）
  -> verify_leased -> verify_passed  （→ Merge Queue 资格，§4）
                   -> verify_failed  （→ 新 Attempt 或 OrchestrationProposal）
```

lease 记录在 `WorkspaceBinding.PhaseLeases`；Task Manager 通过图激活或失效 endpoint，Runtime 在启动已调度 Invocation 前向 Workspace Service 取得并校验 lease，phase 结束后释放。Task Contract / 依赖结果 / 代码基线 / Workspace Head / 高影响上下文变化后，Task Manager 使相关 phase 失效（统一设计 §3.3），Scheduler 只执行该决定。

### 3.4 write set

沿用统一设计的 Declared / Observed 二分（WriteSet 字段：files / modules / symbols / contracts / tests / owners）：

- **Declared Write Set**：plan 阶段产出（`plan/declared-writes.json`），Task Manager 校验合理性后随 execute 委派下发。
- **Observed Write Set**：Threadmill Runtime 观察器（新建）从三处交叉核对：`workspace/` 目录快照 diff、git diff（若为 git workspace）、`submit_task` deliverables 清单。agent 自报只能作参考。
- **校验规则**（统一设计 §8.1）：

```text
1. observed ⊆ AllowedDirs（目录 ACL 与 AccessEntries 之外一律违规）；
2. observed 与 declared 的差异必须由 verifier 接受或拒绝（explain-or-reject）；
3. observed 与 active task / queued candidate 重叠 -> 风险信号，触发合并阶段检查（§4.4）；
4. contract / schema / ownership 变化必须提升 verify 强度。
```

---

## 4. Merge Queue（Threadmill 新建）

### 4.1 语义与边界

- `verify passed`（check_task `effective=true` + Threadmill verifier 语义判断 + evidence）→ MergeCandidate 资格（统一设计 §8.2）。
- Merge Queue 是 main 的唯一写入口（Threadmill 新建 Go 服务；"main"指 Threadmill 管理的目标仓库或目标目录，不是 AgentTeams 概念）。
- Merge Queue 不修冲突、不重写 Coordination Graph、不直接写 Context Graph。

流程（统一设计 §8.2）：

```text
Verify passed on Attempt Workspace
  -> MergeCandidate
  -> 临时 merge-check workspace（Threadmill git worktree / clone 新建）
  -> latest main 上机械应用检查（文件冲突 / 权限边界 / 写集合重叠 / main drift）
  -> targeted verify on latest main + candidate（复用 AgentTeams 执行基础设施：委派或临时 agent）
  -> 串行合入 main
  -> merge event + commit/diff/test evidence（Event Log 投影 + Artifact Store）
  -> Task Manager 计算 done
```

### 4.2 与 AgentTeams 的接口

| 步骤 | 基座 | 复用类型 |
| --- | --- | --- |
| 机械验收 | `taskflow check_task`（effective + validationErrors；deliverables 前缀校验） | 直接复用 |
| 语义验收 | Threadmill verifier 新委派（持久 worker 或 WorkerFlow 临时 agent） | 执行宿主复用，角色新建 |
| 验收确认点 | `projectflow accept_task_result`（Leader 显式接受，project 状态推进的唯一入口） | 直接复用——**但它不等于 merge**：它只是 TeamHarness 的验收；main 写入只属于 Merge Queue |
| 冲突/失败通知 | `ProjectMeta.requester_report`（pending → sent）复用为 done/blocked 通知通道 | 直接复用 |
| 合入执行 | Threadmill Merge Queue：git push（目标为 git 仓库）或文件写回 + MinIO 同步（目标为目录） | Threadmill 新建 |
| 证据 | result.md、deliverables（自动发布 Matrix `m.file`）、`shared/tasks/{id}/evidence/` | 直接复用 |

`accept_task_result` 与 merge 的边界必须保持：Leader 接受结果只推进 TeamHarness 的 project/task 状态；Threadmill 的 done 由 Task Manager 在 merge event 之后计算（统一设计 §8.2）。

### 4.3 状态转换

```text
MergeCandidate: queued
  -> merge_check      （latest main 机械应用；失败 -> failed(conflict|permission|main_drift)）
  -> targeted_verify  （latest main + candidate；失败 -> failed(verify_failed)）
  -> merged           （串行合入 main，产生 merge event）
  -> failed           （evidence 交 Task Manager 编排回 plan/execute/verify 或 waiting_human）
```

任何失败都不在 Merge Queue 内修代码；重试必须重新经过 plan → execute → verify。

### 4.4 并发与冲突语义（统一设计 §8.3）

- 已通过 verify 并进入队列的 candidate 优先尝试合入；candidate 必须在 latest main 上仍可应用并通过 targeted verify。
- 后合入 Task 的旧验证因相关 main revision 改变而失效。
- write set 重叠是风险信号，真正 gate 是机械冲突与 targeted verify。
- 合并后的新事实经 Event Log 投影进入 Context Graph，并推送给订阅相关子图的 active Agent（订阅推送自动执行，见 agent-runtime.md §6.1）。

---

## 5. 安全边界与不变量

1. 普通 agent 不写 main；Merge Queue 是 main 唯一写入口（Threadmill 新建服务）。
2. 普通 agent 不写 Coordination Graph / Context Graph；只提交 `PhaseOutput` / `OrchestrationProposal` / MemoryCandidate。
3. Observed Write Set 以 Runtime 观察为准；agent 自报（submit_task deliverables）只能作参考。
4. 隔离叠加：Pod 隔离（每 worker 一 Pod）+ 存储 ACL（AccessEntries scoped `agents/<name>/*`、`shared/*`）+ AllowedDirs + MCP allow policy + phase lease。
5. worker 无状态：任何 Pod 重建后从 MinIO 恢复 Attempt 现场；Attempt 身份与 Workspace 记录在 Threadmill 侧，不随容器丢失。
6. 敏感路径（credentials/、sessions/、logs/、tool results/）在任何同步、观察与合并检查中排除（sync.py / worker-file-sync.sh 排除清单直接复用）。
7. 冲突视为 verify gate 不通过，必须回到 plan → execute → verify 或等待人工决定；Merge Queue 不修冲突。
8. 所有 merge、冲突、失败、权限违规都必须有事件记录与 evidence refs（Threadmill Event Log 投影 + Artifact Store）。
9. AgentTeams 的 meta.json / result.md 不承载 Threadmill 的图状态；Merge Queue 状态只存 Threadmill 侧。

---

## 6. 数据契约速查

| 契约 | 位置 | 关键字段 |
| --- | --- | --- |
| WorkspaceBinding（Threadmill 新建） | Threadmill 侧存储 | §3.1 全字段；`PhaseLeases`、`Declared/ObservedWrites` |
| WriteSet | `plan/declared-writes.json`；result.md 内嵌 observed 块 | files / modules / symbols / contracts / tests / owners |
| TaskMeta | `shared/tasks/{task_id}/meta.json` | `status`（assigned/in_progress/submitted）、`spec_path`、`result_path`、`room_id`、`assigned_to` |
| ProjectMeta | `shared/projects/{project_id}/meta.json` | `status`、`plan_type`、`reply_route`、`requester_report`（pending/reason/report_path/sent_at） |
| spec / result | `shared/tasks/{task_id}/spec.md`、`result.md` | Leader 拥有 spec；worker 拥有 result；目录所有权见 §3.2 |
| WorkerFlow 状态 | `<default-workspace>/shared/workerflow/<runId>/workflow.json` | `status`、`subagents`/`nodes`/`steps`、`readyInstructions` |
| MergeCandidate（Threadmill 新建） | Threadmill 侧存储 | attempt_id、verify_result_ref、diff_artifact_ref、base/main revision、status（queued/merge_check/targeted_verify/merged/failed）、evidence_refs |
| 心跳/就绪 | 本地 `heartbeat.json`；`POST /api/v1/workers/{name}/ready|heartbeat` | Pod 健康与 `lastActiveAt`（容量判断输入） |
