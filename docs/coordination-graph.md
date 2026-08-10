# Coordination Graph Module

版本：v0.7
状态：Draft

本文定义 Coordination Graph Module 的持久核心对象、内部 `GraphRuntime` 和两个边界 Interface。场景语义见 [统一设计](./threadmill-unified-design.md)，Agent 出站载荷见 [Phase Agent Module](./phase-agent.md)，事件载荷见 [Event Log / Artifact Store](./event-artifact-store.md)。

## 1. 所有权与边界

Coordination Graph 持久化尚未履行的编排义务，并把图校验、差异计算、可运行性、结果失效和审计原子性藏在一个深 Module 内。

唯一能操作图的外部主体是 Task Manager Agent。它通过 `TaskManagerGraph` 读取快照、替换尚未执行的子图，并提交封闭状态转换；该 capability 只注入 Task Manager。传输 Adapter 只能转发其已认证身份，不能持有可独立使用的图写凭据。

`GraphRuntime` 是 Coordination Graph Module 内部的核心对象，不是外部 Module，也不暴露 `Reconcile`、`Runnable`、lease 或命令日志接口。它在图提交、容量变化、Runtime 观察或恢复扫描后被内部唤醒，计算 runnable endpoint，并通过 `PhaseController` 控制 Phase Agent 的 start/stop/resume。

因此公共边界只有两条：

- `TaskManagerGraph`：唯一入站图 Interface，只授予 Task Manager Agent；
- `PhaseController`：内部出站执行 Interface，只由 `GraphRuntime` 调用，Agent Runtime/Adapter 实现，Phase Agent 只接收控制。

Scheduler、phase lease、命令日志和恢复循环都封装在 `GraphRuntime` 内。编排与运行控制的最小持久粒度都是 `PhaseEndpointRef + Generation`；phase 内工具调用和临时步骤不进入公共模型。

## 2. 核心对象

### 2.1 持久对象与内部命令

```go
type PhaseEndpointRef struct {
    TaskID     string `json:"task_id"`
    EndpointID string `json:"endpoint_id"` // plan | execute | verify
}

type Task struct {
    ID          string `json:"id"`
    ContractRef string `json:"contract_ref"` // 包含 DeliveryPolicy
    Outcome     string `json:"outcome"`      // active | done | canceled | failed
}

type PhaseEndpoint struct {
    Ref        PhaseEndpointRef `json:"ref"`
    SpecRef    string           `json:"spec_ref"`    // 当前 phase 的 DeliverySpec + ReportSpec
    BindingRef string           `json:"binding_ref"` // 本 generation 的不可变执行绑定
    Generation int             `json:"generation"`
    State      string           `json:"state"`       // pending | submitted | satisfied | rejected
    RunPolicy  string           `json:"run_policy"`  // enabled | held
}

type Edge struct {
    From          PhaseEndpointRef `json:"from"`
    To            PhaseEndpointRef `json:"to"`
    Signal        string           `json:"signal"`          // phase_satisfied | task_done
    RequiredBy    string           `json:"required_by"`     // start | completion
    ArtifactKinds []string         `json:"artifact_kinds"`
    OnFalse       string           `json:"on_false"`        // block | replan | cancel
}

type Blocker struct {
    ID         string           `json:"id"`
    Target     PhaseEndpointRef `json:"target"`
    RequiredBy string           `json:"required_by"` // start | completion
    OnFalse    string           `json:"on_false"`    // block | replan | cancel
    State      string           `json:"state"`       // active | resolved | denied | obsolete
}

type PhaseResult struct {
    ID         string           `json:"id"`
    Endpoint   PhaseEndpointRef `json:"endpoint"`
    BindingRef string           `json:"binding_ref"`
    OutputRef  string           `json:"output_ref"`
    Verdict    string           `json:"verdict"` // submitted | satisfied | rejected | invalidated
}

// GraphRuntime 唯一发布给 PhaseController 的执行命令；start/stop/resume 使用同一模型。
type PhaseCommand struct {
    ID         string           `json:"id"`          // 幂等命令 ID
    Endpoint   PhaseEndpointRef `json:"endpoint"`
    Generation int              `json:"generation"`
    BindingRef string           `json:"binding_ref"`
    LeaseRef   string           `json:"lease_ref"`   // 本 generation 的执行授权
    Action     string           `json:"action"`      // start | stop | resume
    CauseRef   string           `json:"cause_ref"`   // graph revision、控制事件或恢复依据
}
```

`SpecRef` 指向不可变 phase 契约，不在每个对象重复 DeliverySpec 和 ReportSpec 字段。`BindingRef` 指向不可变执行绑定，集中保存 Task Contract revision、SpecRef、generation、已解析输入结果、Workspace revision、Context Slice 和 Task Memory Buffer revision；可恢复 stop 之后的新 BindingRef 还固定 Runtime 生成的 `CheckpointRef`。Endpoint、PhaseResult 和 PhaseCommand 只携带同一个 BindingRef，不重复展开这些字段。

`PhaseResult.OutputRef` 指向 Agent Runtime 记录的输出信封；信封包含 PhaseOutput 与该次执行产出的 Workspace revision。下游 BindingRef 固定实际消费的输出信封，不在 Edge 或 endpoint 上复制输出修订。

`PhaseCommand` 是 Graph Runtime 与 Agent Runtime 之间唯一的执行命令。`Endpoint + Generation + BindingRef + LeaseRef` 同时约束 start、stop 和 resume，避免为三种动作建立不同命令对象。`resume` 只表示从 BindingRef 固定的 checkpoint 创建新 Invocation；它不会复用旧 Invocation、模型进程或会话状态，也不需要额外的 `ResumeRef`。

Edge 不保存 freshness 或 source result revision。Module 在生成 `BindingRef` 时把实际消费的 source result 固定下来；source 变化会产生新 BindingRef，并使依赖旧 BindingRef 的结果失效。

Blocker 表达不能自然建模为上游 PhaseResult 的人工或外部门控。`active` 阻塞，`resolved` 放行，`denied` 按 `OnFalse` 处理，`obsolete` 不再参与判断。Blocker 本身不执行回调，也不直接暂停 Invocation。

### 2.2 内部核心对象 `GraphRuntime`

`GraphRuntime` 是行为对象，不是持久 DTO，因此不公开字段结构。它内部拥有四项职责：

1. 根据最新图 revision 计算 runnable，并用内置 Scheduler 做容量选择；
2. 以 `Endpoint + Generation` 管理唯一 phase lease；
3. 持久化并幂等投递统一 `PhaseCommand`；
4. 根据命令日志、lease 与 Event Log 恢复未完成的 start/stop/resume。

`GraphRuntime` 不保存第二份 Task、Endpoint、Blocker、PhaseResult 或 `running` 状态，也不能自行产生图状态转换。它随 Module 启动并可从持久图、命令日志、lease 和事件完整重建。

## 3. 边界 Interface

### 3.1 给 Task Manager Agent 的唯一图 Interface

```go
type TaskManagerGraph interface {
    Snapshot(ctx context.Context, revision int64) (GraphSnapshot, error)
    ReplacePending(ctx context.Context, next PendingSubgraph) (revision int64, err error)
    Transition(ctx context.Context, expectedRevision int64, transitionRef string) (revision int64, err error)
}

type GraphSnapshot struct {
    Revision  int64           `json:"revision"`
    Tasks     []Task          `json:"tasks"`
    Endpoints []PhaseEndpoint `json:"endpoints"`
    Edges     []Edge          `json:"edges"`
    Blockers  []Blocker       `json:"blockers"`
    Results   []PhaseResult   `json:"results"`
}

type PendingSubgraph struct {
    RequestID    string          `json:"request_id"`
    BaseRevision int64           `json:"base_revision"`
    Tasks        []Task          `json:"tasks,omitempty"`
    Endpoints    []PhaseEndpoint `json:"endpoints"` // 既是 scope，也是完整期望状态
    Edges        []Edge          `json:"edges"`     // To 位于 scope 的期望入边全集
    Blockers     []Blocker       `json:"blockers"`  // Target 位于 scope 的期望 blocker 全集
}
```

`TaskManagerGraph` 必须同时校验 Task Manager capability 和 graph revision。`Snapshot(revision=0)` 返回 latest；其他 revision 返回对应一致快照。其他 Agent、`GraphRuntime` 和 Agent Runtime 均不持有该 Interface；Adapter 只负责保留并转发调用身份。

`ReplacePending` 直接提交尚未执行 phase 切片的完整期望状态，不接收对象级 CRUD 或 JSON Patch。`Endpoints` 就是 scope；例如只调整 `A.verify` 时，只提交 `A.verify` 及所有目标为它的期望 Edge/Blocker，不连带提交 `A.plan` 和 `A.execute`。

既有 endpoint 必须为 pending、Generation 与当前快照一致且没有活动 lease。运行中的 phase 必须先通过 `Transition(held)` 触发内部停止流程，待停止确认和 generation 轮换后才能进入 scope。新增 Task 必须以 active outcome 一次提交固定的 `plan / execute / verify`；既有 Outcome、State、RunPolicy 和 blocker 决议不能由 `ReplacePending` 改写。

`Transition` 只接受 Task Manager 已持久化的封闭状态转换引用，不接受任意 Runtime Event、字段 patch 或调用方自报状态：

| 目标 | 允许转换 |
| --- | --- |
| Phase Endpoint | submitted、satisfied、rejected、reopened、held、released、stopped |
| Blocker | resolved、denied、obsolete |
| Task | done、canceled、failed |

Runtime 观察先进入 Event Log，再由 Task Manager 结合证据产生 transitionRef。`PhaseInvocationStarted` 同时记录本次来源是 start 还是 resume，只用于审计和恢复；`PhaseOutputSubmitted`、`PhaseInvocationFailed`、`PhaseInvocationStopped` 分别成为 submitted、reopened/failed、stopped 转换的 evidence。PhaseOutput submitted、PhaseResult satisfied 和 Task done 始终是三个独立转换。

### 3.2 控制 Phase Agent 的内部 Interface

```go
// 仅 GraphRuntime 调用；Agent Runtime / Adapter 实现；Phase Agent 不调用。
type PhaseController interface {
    Apply(ctx context.Context, command PhaseCommand) error
}
```

`PhaseController.Apply` 用同一方法执行 `start`、`stop` 和 `resume`。返回只表示命令被可靠接收，实际 started、failed、output 或 stopped 结果异步进入 Event Log；它们不能直接写图。

相同 Command ID 必须幂等；同 ID 不同内容返回 `command_conflict`，generation/binding 过期返回 `stale_command`，resume 的 checkpoint 缺失或与 BindingRef 不兼容返回 `stale_checkpoint`，lease 不匹配返回 `lease_conflict`，执行宿主暂不可用返回可重试的 `executor_unavailable`。不提供独立 Start/Stop/Pause/Resume 方法。

## 4. `GraphRuntime` 内部执行模型

`GraphRuntime` 没有公共 Interface。图提交、capacity/lease 变化、Runtime 观察和恢复计时器都通过 Module 内部通知唤醒它；通知只表示“重新调和”，不能携带调用方指定的 Task、Endpoint 或 desired state。

### 4.1 核心调和逻辑（带注释伪代码）

以下伪代码冻结执行顺序和不变量，不冻结数据库、队列、锁或 Scheduler 的具体实现。`runtimeView` 只是从持久图、命令日志、lease 和 Event Log 组装出的单次一致性读视图，不是新增的持久对象；所有 helper 都是 Module 私有方法，唯一跨 Module 调用仍是 `PhaseController.Apply`。

```go
// reconcile 是 GraphRuntime 的唯一主循环。
// trigger 只负责唤醒；所有决定都必须从最新持久事实重新计算，不能相信 trigger 载荷。
func (r *GraphRuntime) reconcile(ctx context.Context) error {
    view, err := r.store.LoadRuntimeView(ctx)
    if err != nil {
        return err // 读不到一致视图时不作任何控制决定。
    }

    // 1. 先折叠已持久化观察：started 只确认命令，终态观察才允许释放 lease。
    //    这里只更新内部命令/lease 记录，绝不直接改 Task、Endpoint 或 PhaseResult。
    view, err = r.foldObservations(ctx, view)
    if err != nil {
        return err
    }

    // 2. 修复崩溃留下的 command/lease 配对，但此时不投递。
    //    helper 同步更新本轮 view；所有命令必须先经过下面的 stop 优先判定。
    if err := r.repairDecisionPairs(ctx, &view); err != nil {
        return err
    }
    deliver := make([]PhaseCommand, 0)

    // 3. stop 优先：先处理 held、过期 binding、失效 lease 或非 active Task。
    //    一旦决定 stop，同 generation 的未确认 start/resume 就被抑制，不再投递。
    for _, lease := range view.ActiveLeases() {
        if !r.mustStop(view, lease) {
            continue
        }

        cmd, err := r.store.GetOrCreateStopCommand(ctx, view.Revision(), lease)
        if err != nil {
            return err
        }
        view.SuppressRun(lease.Endpoint, lease.Generation)
        deliver = append(deliver, cmd)
    }

    // 4. 恢复仍然有效的未决命令。即使 Apply 的上次响应丢失，也只重投同一命令。
    for _, cmd := range view.PendingCommands() {
        if cmd.Action != "stop" && view.RunSuppressed(cmd.Endpoint, cmd.Generation) {
            continue // stop 已胜出，不能再让旧 start/resume 穿透。
        }
        deliver = append(deliver, cmd)
    }

    // 5. 从图事实计算 runnable，再交给内部 Scheduler 做容量选择。
    //    runnable 不等于立即运行；最终 claim 仍须在持久层做条件竞争。
    candidates := r.graphRunnable(view)
    for _, endpoint := range r.scheduler.Select(candidates, view.AvailableCapacity()) {
        cmd, claimed, err := r.claimRun(ctx, view, endpoint)
        if err != nil {
            if errors.Is(err, ErrStaleCheckpoint) {
                // 禁止把失败的 resume 静默降级为 start；记录证据并等待 Task Manager reopened。
                r.events.RecordDispatchRejection(ctx, endpoint.Ref, err)
                continue
            }
            return err
        }
        if claimed {
            deliver = append(deliver, cmd)
        }
    }

    // 6. 所有网络调用都在图/lease 事务之外发生，避免外部 Runtime 卡住图写事务。
    //    uniqueByCommandID 只去除本轮重复项，不会改写持久命令身份。
    for _, cmd := range uniqueByCommandID(deliver) {
        r.deliver(ctx, cmd)
    }
    return nil
}

// foldObservations 把 Event Log 中的新事实折叠进内部恢复记录。
func (r *GraphRuntime) foldObservations(ctx context.Context, view runtimeView) (runtimeView, error) {
    for _, event := range view.UnfoldedObservations() {
        switch event.Kind {
        case "PhaseInvocationStarted":
            // Apply 已实际生效，但 Invocation 仍持有 lease；只确认命令，不释放资源。
            if err := r.store.MarkCommandObserved(ctx, event.CommandID, event.ID); err != nil {
                return runtimeView{}, err
            }

        case "PhaseOutputSubmitted", "PhaseInvocationFailed":
            // 终态事实已持久化，可以原子完成命令并释放 phase lease。
            // 图仍由 Task Manager 通过 Transition 推进，GraphRuntime 不代写状态。
            if err := r.store.CompleteCommandAndReleaseLease(ctx, event.CommandID, event.LeaseRef, event.ID); err != nil {
                return runtimeView{}, err
            }

        case "PhaseInvocationStopped":
            // stop 必须留下可恢复 checkpoint 或明确 non_resumable；两者都没有时不得释放 lease。
            if event.CheckpointRef == "" && !event.NonResumable {
                return runtimeView{}, ErrIncompleteStopEvidence
            }
            if err := r.store.CompleteCommandAndReleaseLease(ctx, event.CommandID, event.LeaseRef, event.ID); err != nil {
                return runtimeView{}, err
            }
        }
    }

    // 折叠改变了命令和 lease 事实；重新读取后再做 stop/runnable 判断，避免使用半旧视图。
    return r.store.LoadRuntimeView(ctx)
}

// mustStop 只根据持久控制事实判断是否终止当前 Invocation。
func (r *GraphRuntime) mustStop(view runtimeView, lease phaseLease) bool {
    endpoint := view.Endpoint(lease.Endpoint)
    task := view.Task(lease.Endpoint.TaskID)

    // Blocker 变为 active 不在这里：Blocker 只门控 start/completion，不停止已运行 Invocation。
    return task.Outcome != "active" ||
        endpoint.RunPolicy == "held" ||
        endpoint.Generation != lease.Generation ||
        endpoint.BindingRef != lease.BindingRef ||
        view.LeaseExpired(lease)
}

// graphRunnable 是纯计算；它不抢 lease、不创建命令，也不调用 Agent Runtime。
func (r *GraphRuntime) graphRunnable(view runtimeView) []PhaseEndpoint {
    return filter(view.Endpoints(), func(endpoint PhaseEndpoint) bool {
        return view.Task(endpoint.Ref.TaskID).Outcome == "active" &&
            endpoint.State == "pending" &&
            endpoint.RunPolicy == "enabled" &&
            view.BindingAndSpecValid(endpoint) &&
            view.BuiltinPredecessorSatisfied(endpoint) &&
            view.StartEdgesAndBlockersSatisfied(endpoint) &&
            !view.HasActiveLease(endpoint.Ref, endpoint.Generation) &&
            !view.HasPendingRunCommand(endpoint.Ref, endpoint.Generation) &&
            !view.HasTerminalObservation(endpoint.Ref, endpoint.Generation)
    })
}

// claimRun 在一个内部事务中同时 claim lease 并追加 PhaseCommand。
// CAS 失败表示其他调和轮次已先行动作，不是业务错误；调用方刷新视图即可。
func (r *GraphRuntime) claimRun(
    ctx context.Context,
    view runtimeView,
    endpoint PhaseEndpoint,
) (PhaseCommand, bool, error) {
    binding := view.Binding(endpoint.BindingRef)
    action := "start"

    // stopped transition 明确留下 non_resumable 时，必须先 reopened 生成 fresh binding。
    // 这里不能因为没有 CheckpointRef 就直接当作普通 start。
    if binding.NonResumable {
        return PhaseCommand{}, false, ErrStaleCheckpoint
    }
    if binding.CheckpointRef != "" {
        if !view.CheckpointCompatible(binding) {
            return PhaseCommand{}, false, ErrStaleCheckpoint
        }
        action = "resume" // resume 仍创建新 Invocation、新 lease；不复活旧进程。
    }

    // store 在同一事务中复验 revision、runnable 条件、唯一 lease 和无未决 run 命令，
    // 再生成 Command ID/LeaseRef；这里不引入另一套公开 request 对象。
    return r.store.ClaimLeaseAndAppendCommand(
        ctx, view.Revision(), endpoint, action, view.RevisionRef(),
    )
}

// deliver 实现至少一次投递。失败处理只改变内部投递记录，不改图，也不更换 Command ID。
func (r *GraphRuntime) deliver(ctx context.Context, cmd PhaseCommand) {
    err := r.phaseController.Apply(ctx, cmd)
    switch {
    case err == nil:
        // accepted 只确认 Agent Runtime 已可靠收件；命令仍保留到 started/terminal 观察闭环。
        r.store.MarkCommandAccepted(ctx, cmd.ID)
    case errors.Is(err, ErrExecutorUnavailable):
        r.store.ScheduleRetry(ctx, cmd.ID) // 可重试；下次仍投递完全相同的命令。
    case errors.Is(err, ErrStaleCommand), errors.Is(err, ErrLeaseConflict):
        r.wake() // 先刷新持久事实；禁止凭旧视图生成替代命令。
    case errors.Is(err, ErrStaleCheckpoint):
        r.events.RecordDispatchRejection(ctx, cmd.Endpoint, err) // 等待 Task Manager reopened。
    case errors.Is(err, ErrCommandConflict):
        r.store.QuarantineCommand(ctx, cmd.ID, err) // 同 ID 不同内容是内部不变量破坏，不能自动重试。
    default:
        r.store.ScheduleRetry(ctx, cmd.ID) // 未确认接收按同 ID 重投，由重试策略限流。
    }
}
```

`repairDecisionPairs` 只有两条 fail-closed 规则：有 lease、无命令时，若已有远端 Invocation 观察则补建 stop，否则释放孤立 lease；有未决命令、无匹配 lease 时绝不投递，并把命令标记为不可执行等待恢复审计。它只修复持久配对并同步本轮 view，不能直接调用 `PhaseController`、猜测图状态或创建 fresh start。`PendingCommands` 同时包含从未 accepted 的命令，以及 accepted 后超过观察期限仍没有 started/terminal 事件的命令，因此二者都按原 ID 重投。调和循环可以并发运行，因为最终决定由 `ExpectedRevision + Endpoint + Generation` 的持久 CAS 和唯一 lease 约束，而不是进程内互斥保证。

### 4.2 正常启动与恢复

1. `GraphRuntime` 从内部存储读取同一 revision 的图状态，计算 graph-runnable：Task active、endpoint pending、RunPolicy enabled、BindingRef/SpecRef 有效、内建前序 satisfied，且所有 start Edge/Blocker 已满足。
2. 内置 Scheduler 结合 capacity 选择 endpoint；随后重验没有活动 lease、未决 start/resume 命令或该 generation 的终态失败记录。
3. BindingRef 没有 checkpoint lineage 时选择 `Action: "start"`；BindingRef 固定了兼容的 `CheckpointRef` 时选择 `Action: "resume"`。两者都必须在调用外部执行器前取得新 `LeaseRef` 并持久化命令；lease claim 与命令记录形成可恢复的同一决定。
4. `GraphRuntime` 调用 `PhaseController.Apply`。重复投递沿用 Command ID，Agent Runtime 不得创建第二个 Invocation；resume 也必须创建当前 generation 的新 Invocation，不能重新激活旧 Invocation。
5. Agent Runtime 把 started、failed 和 PhaseOutput 记录到 Event Log。确定失败后 `GraphRuntime` 释放 lease；再次执行必须等待 Task Manager 通过 `Transition(reopened)` 轮换 generation。

Coordination Graph 不增加 `running` 状态或 Invocation 对象。当前执行事实只存在于内部命令日志、phase lease 与 Event Log，避免图状态和 Runtime 状态双写。

### 4.3 hold、停止与 release

1. Task Manager 调用 `Transition(held)`；图事务设置 `RunPolicy=held`，提交后内部唤醒 `GraphRuntime`。从该 revision 起不再启动该 endpoint，也拒绝当前 generation 的新 PhaseOutput。
2. 若存在活动 lease，`GraphRuntime` 持久化 `PhaseCommand{Action: "stop"}` 并调用 `PhaseController.Apply`。stop 优先于同 generation 尚未确认的 start 或 resume。
3. Agent Runtime 先禁止新的普通工具调用，要求 Phase Agent 刷出受控 resume state，再固定 Workspace revision 并生成权威 `CheckpointRef`；随后围栏旧 Invocation 的写权限、终止 Invocation，并持久化带 `CheckpointRef` 和 resumable 标记的 `PhaseInvocationStopped`。`GraphRuntime` 只在该事件持久化后释放 phase lease。硬停止无法得到安全 checkpoint 时，事件必须显式标记 `non_resumable`，不能伪装成可恢复停止。
4. Task Manager 基于该 evidence 调用 `Transition(stopped)`；图递增 generation、生成新 BindingRef，并保持 `pending + held`。可恢复 evidence 的新 BindingRef 固定 `CheckpointRef`，不可恢复 evidence 则固定 `non_resumable` 事实。此后可 `ReplacePending`。
5. Task Manager 调用 `Transition(released)` 后，RunPolicy 才恢复 enabled。若新 BindingRef 仍固定兼容 checkpoint，`GraphRuntime` 发布 `resume`；若 checkpoint 缺失、失效，或 `ReplacePending` 改变了 Contract、Spec、输入或 Workspace 基线，Task Manager 必须先通过 `Transition(reopened)` 建立 fresh BindingRef，随后只能 `start`。

这里的 stop 是可恢复控制协议，不是 Task cancellation：它停止当前 Invocation、保留 checkpoint，并允许 release 后创建新 Invocation。恢复的是已持久化的 Workspace 和结构化 resume state，不是模型进程。Blocker 只门控 start 或 completion；它不会调用 `PhaseController`，也不能停止运行中的 Phase Agent。

### 4.4 崩溃恢复

- `GraphRuntime` 重启后扫描未决 PhaseCommand、活动 lease 和对应观察事件：未确认命令按原 ID 重投，孤立 lease 补建命令或释放，已有终态事件的命令不再执行。
- start/stop/resume 交付采用至少一次投递、幂等生效；同一 Endpoint + Generation 最多存在一个 start 或 resume Command ID 和一个活动 lease。
- Agent Runtime 或全部 Agent 进程退出不影响恢复；Runtime 事实不能直接写回图，仍须由 Task Manager 调用 `Transition`。

## 5. Interface 契约

| 错误 | 含义 |
| --- | --- |
| `revision_conflict` | 请求基于过期 graph revision |
| `scope_not_pending` | scope 含 submitted、satisfied 或 rejected endpoint |
| `endpoint_in_flight` | Graph Runtime 的 lease store 仍持有目标 generation 的活动 lease |
| `scope_incomplete` | 变更会影响 scope 外 endpoint 或 Task 级字段未覆盖三阶段 |
| `invalid_graph` | 未知引用、缺 phase、环或非法枚举 |
| `stale_binding` | BindingRef 不再匹配当前 Contract、Spec、输入或 Workspace revision |
| `stale_checkpoint` | resume 所需 checkpoint 缺失、不可恢复或与当前 BindingRef 不兼容 |
| `incomplete_stop_evidence` | stopped 观察既没有 CheckpointRef，也没有明确 non_resumable，Runtime 不得释放 lease |
| `transition_rejected` | transitionRef、目标状态、generation、evidence 或控制顺序不满足转换条件 |

`ReplacePending` 与 `Transition` 都以 revision 做乐观并发控制，分别以 RequestID 和 transitionRef 保证幂等，并把图、级联失效、revision、audit outbox 与内部 Runtime 唤醒记录原子提交。`Snapshot` 返回 revision-consistent 结果；写入成本与受影响闭包相关，调用方不得假设为 O(1)，图事务也不得等待 Agent Runtime 回调。

必须始终成立：

1. Task Manager Agent 是 `TaskManagerGraph` 唯一调用者；`GraphRuntime` 不持有该 Interface，也不能绕过它写图。
2. 每个 Task 固定包含 plan、execute、verify，内建顺序不是可编辑 Edge。
3. 最小控制粒度是 Phase Endpoint + Generation；phase 内步骤不持久化。
4. PhaseResult 绑定不可变 BindingRef；相关输入变化后旧结果不能复用。
5. 每个 Endpoint + Generation 最多有一个 start 或 resume Command ID 和一个活动 lease；重投命令不能创建第二个 Invocation。
6. resume 必须使用新 generation、新 Invocation、新 lease 和 checkpoint-bound BindingRef；不得复用旧 Invocation 或只凭旧进程内状态恢复。
7. held endpoint 不可新启动、不可接受被停止 generation 的输出；stop 优先于未确认 start/resume，release 不得绕过活动 lease。
8. 杀掉 Graph Runtime、Agent Runtime 或全部 Agent 进程，不能丢失任何未完成义务、未决命令、停止意图、已确认 checkpoint 或 `non_resumable` 事实。

## 6. 不属于公共 Interface

- task、endpoint、edge、blocker 的独立 CRUD；
- 任意字段 patch、force apply 或绕过 revision/lease 的写入；
- 暴露给 Task Manager 或其他 Agent 的 start/stop/resume/reconcile 方法；Phase 控制只能由内部 `GraphRuntime` 经 `PhaseController` 执行；
- Graph Runtime 命令日志、lease、Invocation/worker 状态的 CRUD；它们是内部恢复记录，不是新的业务对象；
- 如何取消模型调用、停止 taskflow/temp-agent、撤销 MCP/ACL 或保存 Workspace checkpoint；
- 自动接受 OrchestrationProposal、自动批准 PhaseOutput、自动改图或自动判定 Task done。
