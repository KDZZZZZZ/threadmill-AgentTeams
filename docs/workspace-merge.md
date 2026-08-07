# Workspace / Git / Merge Queue 详细设计

版本：v0.3
状态：Draft

---

## 1. 定位

Workspace / Git / Merge Queue 不是一套独立调度系统，也不是一个复杂的“智能合并平台”。它是 **Agent Runtime 之上的合并安全策略**：把已经通过 verifier 的 task attempt，经过确定性的冲突校验、Task Graph 编排和最终机械化合入，安全地变成项目事实。

它只解决一件事：

```text
多个 agent 在隔离 workspace 中并发产出变更后，哪些变更可以进入 main，哪些变更必须回到 plan -> execute -> verify 循环。
```

核心边界：

```text
Agent Runtime
  负责启动 planner / executor / verifier / task_manager，提供 cwd/worktree/branch 隔离、工具权限、事件记录、artifact 记录和 observed write set。

Verifier
  负责证明某个 task attempt 满足 task contract，并产出 verify result、test evidence、diff summary 和 observed write set。

Merge Queue
  负责串行处理 verify passed 的 merge candidate，执行机械化冲突校验、最新 main 上的最终验证和无冲突合入。

Task Manager Agent
  负责根据冲突、依赖和验收关系改写 Task Graph。Merge Queue 不直接写 Task Graph，只通过 Agent Runtime(role=task_manager, tool=graph_write) 提交 requirement / conflict evidence。

Ctx Manager Agent / Ctx Agent
  负责从 Event Log / Artifact Store 提炼 merge context。Merge Queue 不直接写 ctxlib。
```

一句话：**agent 产出变更，verifier 验收变更，merge queue 做机械化冲突与合入，Task Manager Agent 负责冲突后的图编排，Agent Runtime 负责所有权限和运行边界。**

---

## 2. 基本规则

```text
1. planner / executor / verifier / task_manager 都必须通过 Agent Runtime 启动。
2. 每个 task attempt 只能在 Agent Runtime 分配的 cwd / worktree / branch / allowed dirs 内工作。
3. 普通 agent 没有 main branch 写权限，也没有 merge 权限。
4. 普通 agent 不直接写 Task Graph；只能提交 requirement、evidence 或 verify result。
5. Task Graph 的 task / phase / edge / blocker / done 只能由 Task Manager Agent 写入。
6. task attempt 必须先经过 verifier，通过后才能成为 MergeCandidate。
7. verify 通过不等于 done；done 必须等 merge 检查和相关图编排条件满足。
8. 当前 Task Graph 编排走到 verify 但验收未通过，视为异常 / 未满足验收，而不是正常完成分支；必须把失败证据交给 Task Manager Agent 继续编排后续步骤。
9. 冲突视为 verify gate 不通过，而不是 merge queue 自己修代码。
10. main branch 只接受 Merge Queue 这个 Go backend 受控服务写入。
11. merge 成功、失败、冲突、复验结果都必须进入 Event Log，并关联 artifact 证据。
```

这套规则让 workspace / git 成为 Agent Runtime 权限策略的延伸，而不是 agent 可以自由操作的外部环境。

---

## 3. Agent Runtime Workspace Binding / Merge Projection

Workspace Binding 是 Agent Runtime 为某个 task attempt 分配的隔离执行边界；Merge Projection 是 Merge Queue 为冲突校验读取的投影。它们不是独立于 Agent Runtime 的 workspace 子系统，也不是 task 状态，更不是 Task Graph 节点。

第一阶段的 worktree、git、cwd 和可写范围优先作为 CLI agent wrapper 能力包装；如果底层 CLI 不支持，再由 Agent Runtime 用 git worktree 或独立 clone 兜底。Merge Queue 只消费 Runtime 暴露出的 workspace binding、diff artifact 和 observed write set。

Workspace Projection 的字段可以用 Go 类型表达如下，但它只是 Agent Runtime / Merge Queue 的投影，不拥有独立生命周期：

```go
type Worktree struct {
    // ID 是隔离工作区标识，由 Go backend 分配。
    ID string `json:"id"`
    // TaskID 指向 Task Graph 中的 task。
    TaskID string `json:"task_id"`
    // AttemptID 指向本次 plan / execute / verify 尝试。
    AttemptID string `json:"attempt_id"`

    // Path 是本地 worktree 路径；Electron shell / frontend 只能展示，不直接写入。
    Path string `json:"path"`
    // BranchName 是该 attempt 对应的临时 git branch。
    BranchName string `json:"branch_name"`

    // BaseCommit 是 attempt 创建时的 main 或依赖基线 commit。
    BaseCommit string `json:"base_commit"`
    // HeadCommit 是 attempt 当前产出 commit；未产生提交时为空。
    HeadCommit string `json:"head_commit,omitempty"`

    // AllowedDirs 是 Agent Runtime 授权给该 attempt 的可写目录集合。
    AllowedDirs []string `json:"allowed_dirs,omitempty"`
    // DeclaredWriteSet 是 plan 阶段声明的预计影响面。
    DeclaredWriteSet WriteSet `json:"declared_write_set"`
    // ObservedWriteSet 是 Agent Runtime / verifier 从真实 diff 中提取的影响面。
    ObservedWriteSet WriteSet `json:"observed_write_set"`

    // Status 是 worktree 生命周期状态，不等于 task 状态。
    Status WorktreeStatus `json:"status"`
    CreatedAt time.Time `json:"created_at"`
    UpdatedAt time.Time `json:"updated_at"`
}
```

处理逻辑：

```text
1. Scheduler 读取 Task Graph，决定某个 phase 可运行。
2. Scheduler 向 Agent Runtime 提交 AgentRunParams。
3. Agent Runtime 创建或复用 worktree，注入 context pack、权限策略和工具策略。
4. planner 声明 plan 和 declared write set。
5. executor 在隔离 worktree 内产出 diff / commit / artifacts。
6. verifier 在同一 attempt 的结果上验收，并生成 VerifyResult。
7. verify passed 后，attempt 才能成为 MergeCandidate。
```

Projection 的唯一状态含义是“这次 attempt 在哪里运行、基于什么、实际改了什么”。task 是否完成、是否 blocked、是否需要 replan，全部由 Task Graph 表达。

---

## 4. Branch 命名

Branch 命名只用于人类可读和 git 排查，不承担状态语义。权威状态仍在 Task Graph / Event Log / Merge Queue projection 中。

```text
task/{task_id}-{short_slug}/attempt-{n}
```

例如：

```text
task/T123-agent-runtime-wrapper/attempt-01
```

规则：

```text
1. branch 由 Agent Runtime 创建或登记。
2. branch 只服务 attempt 隔离，不等于 task done。
3. branch 不允许直接合 main；必须进入 merge queue。
4. branch 名称变化不影响 Task Graph 依赖关系。
```

---

## 5. Merge Queue

Merge Queue 是受控 Go backend 服务，负责把 `verify passed` 的 MergeCandidate 串行推进到 main。它不是 agent，不写 Task Graph，也不替 verifier 或 Task Manager Agent 做判断性工作。

它的职责收敛为三段：

```text
1. 文件层面冲突校验：机械化判断 candidate 是否还能干净应用到最新 main。
2. Task Graph 编排触发：如果冲突或复验失败，把证据交给 Task Manager Agent 重新编排。
3. 最终机械化合入：无冲突且最终验证通过时，将代码写入 main 并记录事实。
```

### 5.1 合入流程

```text
1. verifier 通过 Agent Runtime 产出 VerifyResult(status=passed)。
2. Agent Runtime 记录 verify event、artifact refs、observed write set。
3. Scheduler / policy 将该 attempt 放入 merge queue。
4. Merge Queue 串行锁定一个 MergeCandidate。
5. Merge Queue 拉取 latest main，并在临时 merge check workspace 中尝试应用 candidate diff。
6. Merge Queue 执行文件层面冲突校验和权限边界校验。
7. 如果冲突校验失败：
   - 不 merge。
   - Merge Queue 产生 backend domain event，记录 conflict evidence refs。
   - 通过 Agent Runtime(role=task_manager, tool=graph_write) 向 Task Manager Agent 提交严格的 conflict intake。
   - conflict intake 必须带 idempotent client_ref、source task、target task、conflict type、required adaptation 和 evidence refs。
   - Task Manager Agent 根据逻辑关系重新编排 Task Graph。
8. 如果冲突校验通过：
   - Merge Queue 创建 latest main + candidate diff 的临时验证 workspace。
   - Scheduler / Control Plane 通过 Agent Runtime(role=verifier) 启动最终 targeted verifier，或消费一个等价的新鲜 verifier result。
   - 等价 verifier result 必须基于 latest main + candidate diff 产生，必须由 Agent Runtime(role=verifier) 记录，并且必须带 evidence refs。
   - Merge Queue 不拥有验收判断，只等待 verifier 在最新事实上给出 pass / fail。
9. 如果 targeted verifier 失败：
   - 不 merge。
   - 将失败视为 verify gate 不通过，也是当前图编排的异常 / 未满足验收结果。
   - verifier 产出失败证据、缺口说明和必要的后续 requirement。
   - Merge Queue 产生 backend domain event，记录 failure evidence refs。
   - 通过 Task Manager Agent 继续编排 replan / execute / verify、blocker、follow-up 或 waiting_human。
10. 如果 targeted verifier 通过：
   - Merge Queue 将 candidate 写入 main。
   - 产生 merge domain event，记录 commit ref、diff summary、test evidence refs。
   - 通过 Task Manager Agent 推进相关 phase / task 的 done 条件。
   - 触发 Ctx Manager Agent 从 Event Log / Artifact Store 提炼 merge context。
```

### 5.2 Merge 不变量

```text
1. 未通过 verifier 的 task attempt 不得进入 merge queue。
2. Merge Queue 是 main branch 的唯一写入口。
3. Merge Queue 不直接写 Task Graph，只向 Task Manager Agent 提交冲突、失败或合入证据。
4. Merge Queue 不直接写 ctxlib，只产生日志和 artifact，由 Ctx Manager Agent 提炼。
5. 合入前必须基于 latest main 做最终检查，而不是只相信 attempt 上的旧验证结果。
6. 最终 targeted verification 由 Agent Runtime 启动 verifier 完成；Merge Queue 只负责准备验证 workspace、收集结果和机械化合入。
7. 冲突视为 verify gate 不通过：进入新一轮 plan -> execute -> verify，而不是在 merge queue 里手工修。
8. merge 成功后必须能从 Event Log 追溯到 requirement、task、attempt、verify result、merge commit 和验证证据。
```

---

## 6. 并发与冲突协调

多 agent 并发时，冲突协调遵循一个简单原则：

```text
已经 verify passed 并进入 merge queue 的 candidate 拥有合入优先权；
仍在 planning / executing / verifying 的 active task 负责适配最新事实。
```

但这个优先权不是“强行合入”。candidate 只有在 latest main 上仍然能通过冲突校验和 targeted verify，才能进入 main。

处理逻辑：

```text
MergeCandidate A 进入 queue
  -> Merge Queue 在 latest main 上做文件冲突校验
  -> 如果 A 自身无法应用或复验失败：A 回到 replan / execute / verify
  -> 如果 A 可以合入，但影响 active task B：A 先合入，B 后续 verify 必须面对最新 main
  -> 如果 B 的 verify 依赖 A 的结果：Task Manager Agent 可以编排 A.done / A.verify -> B.verify
  -> 如果 B.verify 后 A 再验收：Task Manager Agent 可以编排 B.verify -> A.verify
  -> 如果 A 的 verifier 在最终验收时发现和 B 的结果冲突：A 的 verify 不通过，请求 Task Manager Agent 重新编排下一轮
```

这和 `task-graph.md` 的 phase 编排一致：冲突不是 merge queue 自己判断“谁对谁错”，而是变成 verify gate 和 Task Graph edge。例如：

```text
B.verify --signal: passed--> A.verify
B.verify --data: verification_summary--> A.verify
A.verify.active = all(required signals are true)
```

当 A 在接收 B 的验证结果后发现冲突，结果不是 A 强行覆盖 B，而是：

```text
A.verify failed(conflict)
  -> Agent Runtime 记录 verify failure / conflict evidence
  -> Task Manager Agent 重新编排 A 或 B 的 plan / execute / verify
  -> 新 attempt 再进入 merge queue
```

---

## 7. 冲突类型

Merge Queue 不需要复杂的语义推理。它只做足够稳定、可自动化的冲突识别，并把需要判断的部分交给 verifier 和 Task Manager Agent。

```text
file_conflict:
  candidate diff 无法干净应用到 latest main，或两个 candidate 修改同一文件的同一区域。
  处理：阻止 merge；视为 verify gate 不通过；提交 Task Manager Agent 重新编排。

write_set_overlap:
  candidate 的 observed write set 与 active task / queued candidate 的 declared 或 observed write set 重叠。
  处理：记录为风险；是否阻塞取决于是否影响当前 candidate 的最终 verify。

ownership_conflict:
  candidate 修改了不属于自身 task owner / allowed dirs / capability 的内容。
  处理：阻止 merge；记录权限或边界事件；要求 replan 或 reject attempt。

contract_conflict:
  candidate 修改 public API、协议、schema、CLI contract 或跨模块契约，导致依赖 task 的验收语义变化。
  处理：提升 verify 强度；通常由 Task Manager Agent 编排依赖 task 的重新 verify 或 replan。

test_conflict:
  candidate 修改测试预期，另一个 task 修改实现，导致“通过测试”不再等价于原验收标准。
  处理：阻止直接 done；由 verifier 重新定义 targeted verify 证据，必要时触发 Task Manager Agent 编排。

main_drift_conflict:
  attempt 基于旧 main 通过，但 latest main 已改变相关文件或事实。
  处理：在 latest main 上 targeted verify；失败则回到 plan -> execute -> verify。

permission_conflict:
  Agent Runtime 在 invocation 期间发现 agent 尝试访问未授权工具 / 路径，或 verifier 发现 observed write set 超出授权范围。
  处理：Runtime 立即记录权限事件并使 attempt 失败；这类 attempt 通常不得成为 MergeCandidate。Merge Queue 仍会把 observed write set vs allowed dirs 作为 defense-in-depth 复查，发现遗漏时阻止 merge 并要求 Task Manager Agent 编排 reject / replan。
```

分类的目的不是让 Merge Queue 做复杂决策，而是让后续 verifier、Task Manager Agent 和 UI 能看到清晰证据。

---

## 8. Write Set

Write Set 是冲突校验的核心输入，分为 plan 声明和实际观察两类。

### Declared Write Set

Declared Write Set 由 planner 在 plan 阶段声明，经 verifier 检查是否合理。它用于提前降低并发风险，但不能作为真实修改的权威来源。

```go
type WriteSet struct {
    // Files 是预计或实际修改的文件路径。
    Files []string `json:"files,omitempty"`
    // Modules 是预计或实际影响的模块 / package / bounded context。
    Modules []string `json:"modules,omitempty"`
    // Symbols 是预计或实际影响的函数、类型、接口、组件等符号。
    Symbols []string `json:"symbols,omitempty"`
    // Contracts 是 API、协议、配置 schema、数据库 schema 等对外契约。
    Contracts []string `json:"contracts,omitempty"`
    // Tests 是预计或实际修改 / 依赖的测试集合。
    Tests []string `json:"tests,omitempty"`
    // Owners 是涉及的 owner module，用于边界校验。
    Owners []string `json:"owners,omitempty"`
}
```

声明内容：

```text
- 预计修改的模块 / owner
- 预计修改的文件 / symbol
- 预计修改的 public contract
- 预计修改的数据库 / 配置 schema
- 预计新增或修改的测试
- 需要的工具 / 权限边界
```

### Observed Write Set

Observed Write Set 由 Agent Runtime 和 verifier 从实际 diff、工具调用、测试结果和 artifact 中提取，不由 executor 自报为准。

```text
- 实际修改文件
- 实际修改 symbol
- 实际修改 contract
- 实际修改测试
- 实际访问或写入的路径
- 实际产生的 artifact
```

校验逻辑：

```text
1. observed write set 必须被 declared write set 覆盖，或有 verifier 接受的解释。
2. observed write set 不得超出 Agent Runtime allowed dirs / capability policy。
3. observed write set 与 active tasks 或 queued candidates 重叠时，触发 conflict analysis。
4. contract / schema / ownership 变化必须提升 verify 强度。
5. 最终 merge 判断以 observed write set + latest main diff 为准。
```

---

## 9. Conflict Context Broadcast

Conflict Context Broadcast 是把冲突或已合入事实传递给仍在运行的 active task。它不是让 Merge Queue 直接改对方 task，而是通过 Task Manager Agent 编排图关系，并通过 Agent Runtime 给相关 agent 注入受控上下文。

```go
type ConflictContext struct {
    // SourceTaskID 是已经 queued / merged / rejected / verify_failed 的来源 task。
    SourceTaskID string `json:"source_task_id"`
    // TargetTaskID 是需要适配、复验或重新规划的活跃 task。
    TargetTaskID string `json:"target_task_id"`

    // SourceStatus 描述来源 task 当前 merge / verify 状态。
    SourceStatus SourceMergeStatus `json:"source_status"`
    // ConflictType 描述冲突类型，例如 file_conflict / contract_conflict。
    ConflictType ConflictType `json:"conflict_type"`
    // Severity 区分 notify / replan_required / blocking / human_decision。
    Severity ConflictSeverity `json:"severity"`

    // ChangedFiles / Modules / Contracts 描述来源 task 已改变或尝试改变的事实。
    ChangedFiles []string `json:"changed_files,omitempty"`
    ChangedModules []string `json:"changed_modules,omitempty"`
    ChangedContracts []string `json:"changed_contracts,omitempty"`

    // DiffSummary 是面向 agent 的精简 diff 摘要。
    DiffSummary string `json:"diff_summary"`
    // DecisionSummary 是 verifier / merge queue 给出的处理结论。
    DecisionSummary string `json:"decision_summary"`

    // RequiredAdaptation 是目标 task 必须采取的适配动作。
    RequiredAdaptation RequiredAdaptation `json:"required_adaptation"`
    // EvidenceRefs 指向 diff、test、verify result、merge decision 等证据。
    EvidenceRefs []string `json:"evidence_refs"`
}
```

广播流程：

```text
1. Merge Queue 或 verifier 发现 candidate 与 active task / latest main 有冲突。
2. Agent Runtime 记录 conflict event 和 evidence refs。
3. Merge Queue / verifier 通过 Agent Runtime(role=task_manager, tool=graph_write) 请求 Task Manager Agent：
   - 提交严格的 conflict intake，包含 idempotent client_ref 和 EvidenceRefs
   - 给 target task 增加 conflict edge / blocker / replan_required endpoint
   - 或创建 follow-up requirement
   - 或标记 waiting_human
4. Scheduler 读取更新后的 Task Graph。
5. 如果 target task 的 agent 仍在运行，Agent Runtime 注入 conflict context 或触发取消 / replan。
6. target task 的 planner / executor / verifier 按第 10 节规则处理。
```

---

## 10. 接收方处理规则

接收方不是自由解释 conflict context，而是按 Task Graph 状态和 Agent Runtime 权限处理。

```text
如果 conflict 只说明已有事实更新，但不影响当前 plan：
  target task -> continue
  Agent Runtime 注入 context；verifier 后续必须考虑该事实。

如果 conflict 影响实现细节，但不改变 task contract：
  target task -> adapt_execute
  executor 在当前 worktree 中调整实现；verifier 增加相关 targeted checks。

如果 conflict 影响 approved plan 或 declared write set：
  target task -> replan_required
  Scheduler 通过 Agent Runtime 启动 planner；planner 更新 plan 和 declared write set。

如果 conflict 使 task contract 不再成立：
  target task -> blocked / superseded
  通过 Task Manager Agent 编排 blocker、supersede 关系或 follow-up requirement。

如果 conflict 涉及 product / architecture / ownership 决策：
  target task -> waiting_human
  Task Manager Agent 记录 blocker，UI 展示需要人类决策的证据。

如果 conflict 暴露权限越界：
  target task -> permission_violation / rejected_attempt
  Agent Runtime 保留证据；Task Manager Agent 决定是否创建修复性 requirement 或要求重新 plan。
```

处理约束：

```text
1. target agent 不直接改 Task Graph，只提交 requirement / evidence。
2. target agent 不直接读 ctxlib 底层存储，只通过 Ctx Manager Agent pack / query。
3. target agent 不直接改 main，只能在自己的 worktree 内适配。
4. 任何 replan / blocked / superseded / done 状态，都由 Task Manager Agent 写入 Task Graph。
5. 冲突导致的重试必须重新经过 plan -> execute -> verify，而不是跳过 verifier 直接回 merge queue。
```

---

## 11. Git 不变量

```text
1. 每个 task attempt 独立 cwd / worktree / branch，并由 Agent Runtime 管理。
2. planner / executor / verifier / task_manager 都是 Agent Runtime invocation。
3. execute 不直接修改 main；main branch 只接受 Merge Queue 写入。
4. merge 前必须有 verifier passed 结果和可追溯 evidence refs。
5. merge 前必须基于 latest main 做文件冲突校验，并由 Agent Runtime 启动 verifier 做 targeted verify。
6. observed write set 必须由 Agent Runtime / verifier 从真实 diff 提取。
7. Merge Queue 不直接写 Task Graph；task done / blocker / conflict / follow-up 都由 Task Manager Agent 编排。
8. Merge Queue 不直接写 ctxlib；merge 事实进入 Event Log 后由 Ctx Manager Agent 提炼。
9. 冲突视为 verify gate 不通过，必须进入下一轮 plan -> execute -> verify 或等待 human decision。
10. 所有 merge、冲突、失败和权限违规都必须有 Event Log 事件和 artifact 证据。
```
