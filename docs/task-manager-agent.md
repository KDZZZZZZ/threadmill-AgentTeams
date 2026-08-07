# Task Manager Agent 详细设计

版本：v0.3
状态：Draft

---

## 1. 定位

Task Manager Agent 是 task graph 的**唯一写入口**，也是所有 requirement intake 和 task graph 编排的网关。它不是绕过 runtime 的后台服务，而是经 Agent Runtime 启动、授权、观测和记录的系统 agent。

本文件定义权责和数据契约；完整的 `task-manager` Skill 见 [`skills/task-manager/SKILL.md`](../skills/task-manager/SKILL.md)。实际运行时的拆分判断、endpoint 选取、失败处理和反例检查以本文件第 15 节为当前文档约束。两者冲突时，应先修正文档和 Skill，不能依赖 prompt 中的临时解释。

人类和其他 agent 都不直接写 task graph，而是通过 Agent Runtime(role=task_manager) 向 Task Manager Agent 提交 requirement。它在写入前拥有全局 task 视图，负责去重、依赖推断、阻塞关系判断、边界与验收校验。

它的一个硬边界是：

> Task Manager 只负责把"需求"转成"任务契约(what + why + done)"，绝不产出"how"。how 是 plan 阶段 planner 的专属职责。

---

## 2. 责任切分：需求 / 描述 / 计划

系统里有三个容易被混淆的东西，各有唯一 owner：

```text
需求 requirement   -> 归 requester(人类或 planner / executor / verifier)
  表达"我想要什么 + 为什么 + 硬约束 / 验收意图"。

任务描述 description -> 归 Task Manager Agent
  规整成 self-contained、可验收、边界清晰的工作单元。
  是"做什么 / 为什么 / 怎样算完成"，不是"怎么做"。

计划 plan          -> 归 plan 阶段的 planner
  决定"怎么做"：步骤、方案、write set 细节、是否需要向 Task Manager 提出新的 requirement。
```

一句话：**Task Manager 定义"做什么、算不算完成"，planner 定义"怎么做"。**

如果 requester 在需求里夹带了实现方案(how)，Task Manager 只把它作为 `hint / constraint` 存入上下文，不写成强制 plan，否则会架空 planner。

人类提出的自由文本首先是 requirement。requirement 会进入 provenance / event log；Task Manager 可以基于它创建一个或多个统一 task 节点，但不会把“用户需求本身”当成可调度 task。

---

## 3. 两种 intake 模式

Task Manager 对不同来源使用**不同权限**，这是本设计的核心。

### 3.1 Human Intent 模式(宽松)

- 输入：人类的自由文本需求，通常模糊、可能夹带实现建议。
- Task Manager 权限较大：可以规整、推断、提炼验收标准、做粗粒度拆分、判定重复并合并。

### 3.2 Agent Requirement 模式(严格)

- 输入：执行某个 task 的 planner / executor / verifier 在运行中提出的 requirement。
- 这些 requirement 是发起 agent 当前 phase 判断的一部分，必须和它的本地计划节点、验收缺口或执行阻塞严格对应。
- Task Manager 此时是**登记员 + 校验员 + 图编排员，不是编辑**：可以拒、可以要求补全，可以基于全局视图编排依赖和 blockers，但**不能改 requirement 内容**，否则会破坏发起方"client_ref ↔ 本地判断"的映射。
- planner / executor / verifier 不直接提交 task / edge；它们提交 requirement 和触发证据，依赖关系由 Task Manager 统一编排。

```text
人类需求   = 模糊意图，Task Manager 有规整余地
agent requirement = 严格契约，Task Manager 登记、校验并负责编排图关系
```

---

## 4. 内容 vs 图关系：分权模型

为了让"严格模式不被乱改"和"Task Manager 仍要维护全局一致性"两件事共存，把字段分成两类，各有唯一 owner：

```text
内容(发起方拥有，Task Manager 只读、不可改):
  - title / description
  - acceptance_criteria
  - declared scope / owner_module
  - 同批 requirement 之间的依赖意图 / 顺序提示(发起方视角，只作输入证据)
  - client_ref(发起方本地键)

图关系与元数据(Task Manager 拥有，只能"新增"，不能改内容):
  - 全局 task_id
  - task phase endpoint / decision endpoint
  - 跨 agent / 跨 task / 跨状态节点的依赖、阻塞
  - 全局重复的"关联标记"(link，不是合并)
  - 优先级、冲突 flag、调度信息
```

一句话：**Task Manager 能给 task 加"外部关系"，但不能动发起方声明的 what/why/done。**

---

## 5. 保证"对应得上"且"不被乱改"的两个机制

### 5.1 client_ref + 幂等

发起 agent 的每个 requirement 带一个自己的本地键 `client_ref`。Task Manager 必须：

```text
- 原样回显 client_ref。
- 保证幂等：同一 client_ref 再发一次，映射到同一个 task，不新建。
```

这样发起方的 phase 判断引用永远稳定，`client_ref -> requirement_id / task_id` 映射可靠。

### 5.2 严格模式下的封闭操作集合

严格模式下 Task Manager 只能做这些，**没有 edit，也没有 silent merge**；但它可以新增自己编排出的图关系：

```text
register        接受并登记 requirement(内容原样)
needs_fix       字段不完整 / 验收不可测，退回发起方改(不代改)
propose_change  Task Manager 有异议时"建议改动"，回给发起方决定，不自行落地
link_related    发现全局重复 / 重叠，建立关联 + 触发决策，不静默合并
compile_graph   将 requirement 编排为 task / phase endpoint / decision endpoint / edge / blocker
reject          越权、无效、与硬约束冲突
```

---

## 6. 输入契约

### 6.1 Human Intent（宽松）

```go
type RequirementIntake struct {
	// Requester 固定为 human，表示来自用户的自然语言需求。
	Requester string `json:"requester"`
	// Text 是自由文本需求。
	Text string `json:"text"`
	// Goal 是可选的显式目标。
	Goal string `json:"goal,omitempty"`
	// Constraints 是硬约束或验收意图。
	Constraints []string `json:"constraints,omitempty"`
	// PriorityHint 是用户给出的优先级提示，不直接等于调度优先级。
	PriorityHint PriorityHint `json:"priority_hint,omitempty"`
}
```

### 6.2 Agent Requirement（严格）

```go
type AgentRequirementIntake struct {
	// Requester 描述发起 requirement 的 agent 与来源 phase。
	Requester AgentRequirementRequester `json:"requester"`
	// ClientRef 是发起方本地键；Task Manager 必须回显并保证幂等。
	ClientRef string `json:"client_ref"`

	Title string `json:"title"`
	// Description 必须是 self-contained 的“需要什么”，不能只引用当前上下文。
	Description string `json:"description"`
	// AcceptanceIntent 由发起方填写，必须可测或可转换成可测验收。
	AcceptanceIntent []string `json:"acceptance_intent"`
	// DeclaredScope 描述 owner module、文件边界或决策边界。
	DeclaredScope []string `json:"declared_scope,omitempty"`

	// DependencyIntent 只是发起方视角的依赖意图，由 Task Manager 编译成真实 graph edge。
	DependencyIntent []DependencyIntent `json:"dependency_intent,omitempty"`
	// EvidenceRefs 指向触发该 requirement 的日志、失败、发现或计划节点。
	EvidenceRefs []string `json:"evidence_refs"`
}

type AgentRequirementRequester struct {
	AgentID string `json:"agent_id"`
	Role AgentRole `json:"role"`
	TaskID string `json:"task_id"`
	SourcePhase AgentPhase `json:"source_phase"`
	SourceStatus string `json:"source_status,omitempty"`
}

type DependencyIntent struct {
	LocalFrom string `json:"local_from,omitempty"`
	LocalTo string `json:"local_to,omitempty"`
	ExistingTaskID string `json:"existing_task_id,omitempty"`
	ExistingEndpoint string `json:"existing_endpoint,omitempty"`
	Reason string `json:"reason"`
}
```

---

## 7. 转化产物：任务契约

无论哪种模式，落库的 task 契约包含：

```text
保留:
- intent / goal(原始需求的"为什么"，尽量原样保留作 provenance)
- title + description(规整后的"做什么"，self-contained)
- acceptance_criteria(可测的完成条件；可由 human requirement 提炼，或由 agent requirement 的 acceptance_intent 转化)
- delivery_type(直接实现 / 提交 requirement 触发 graph expansion / 研究 / 设计决策 …)
- owner_module + 边界 / declared scope
- requirement_refs
- inferred graph edges / blockers
- priority / risk 提示

绝不写入:
- 实现步骤、技术方案、算法选择
- 具体改哪些文件、怎么改
- requester 提供的 how 只作 hint/constraint，不作强制 plan
```

原始需求必须作为 provenance 保留(链接或 context block)，planner 遇到歧义时能回看原话，而不是只看被压缩过的描述。

---

## 8. 验收标准归属

```text
人类需求     -> Task Manager 从 intent 提炼验收标准(可测)
agent requirement -> 发起 agent 自己写 acceptance intent(要和它的 phase 判断互锁)
                     Task Manager 只校验"是否可测、是否清晰"，不重写；如需转成 task 验收标准，只做结构化映射并保留原文
```

理由：如果让 planner 自己定义"算不算完成"，它会倾向定义成"我这个方案刚好满足"的样子，verify 就失去独立锚点，与"verifier 不能自我批准"的不变量冲突。因此人类需求的验收标准由 Task Manager 从意图提炼；而 agent 请求的验收标准由发起方编写，但发起方不是该 task 的 verifier，仍满足独立性。

---

## 9. 决策结果

```go
type TaskMutationResult struct {
	// ClientRef 严格模式必须回显，用于发起方幂等匹配。
	ClientRef string `json:"client_ref,omitempty"`
	TaskID string `json:"task_id,omitempty"`
	// TaskIDs 在宽松模式下可能由一个 requirement 创建多个 task。
	TaskIDs []string `json:"task_ids,omitempty"`

	// Decision 记录 Task Manager 的编排决策。
	Decision TaskMutationDecision `json:"decision"`
	// MergedIntoTaskID 仅宽松 human 模式允许，用于合并重复需求。
	MergedIntoTaskID string `json:"merged_into_task_id,omitempty"`
	// Overlaps 严格 agent requirement 模式只关联重叠 task，不自动 merge 内容。
	Overlaps []string `json:"overlaps,omitempty"`
	// InferredEdges 是 Task Manager 新增的图关系，不改 requirement 内容。
	InferredEdges []TaskEdge `json:"inferred_edges"`
	InferredBlockers []string `json:"inferred_blockers"`
	Reason string `json:"reason"`
}
```

注意：`merged_into` 只在**宽松模式**允许；严格模式对重复只能 `link_related`。

---

## 10. 去重与冲突：暴露而非替人拍板

发起方的两个 requirement，或两个不同 agent 的 requirement，在全局看可能"重复/重叠"。但**严格模式下不能自动合并**——各自的 phase 判断依赖各自的 client_ref 和证据链。

```text
- 保留两个 requirement / task 的独立身份和 client_ref。
- 用 link_related 标记重叠，把 write set 或 state endpoint 冲突交给冲突协调 / owner 决策。
- 由发起方(或人类)决定谁让路，Task Manager 不替它们做主。
```

这与冲突协调的总原则一致：Task Manager 只**暴露**冲突，不**替人拍板**；真正的单边适配/让路发生在 verify/merge 阶段。

---

## 11. 全局一致性职责

Task Manager 仍然要做全局层面的工作，但只能"新增关系"，不能改内容：

```text
1. 维护并读取全部 task 及状态的全局视图。
2. 去重：识别等价/重叠 task（宽松可合并，严格只关联）。
3. 依赖编排：补齐跨 task / 跨 agent / 跨 state endpoint 的 dependencies、blockers。
4. 冲突检测：标注 owner_module / write set / state endpoint 重叠。
5. 验收校验：实现型 task 无可验收标准不得进入 planned。
6. 事件记录：所有写入都产生事件，便于追溯。
```

---

## 12. 与其他模块的交互

### 12.1 谁能发需求

以下来源都可以向 Task Manager Agent 发需求：

```text
- 人类（Human UI）：宽松模式，自由文本需求。
- planner：严格模式，规划中发现需要拆解时提交 requirement。
- executor：严格模式，执行中发现遗漏 / 需要新工作时提交 requirement。
- verifier：严格模式，验收发现缺口 / 需要 follow-up 时提交 requirement。
```

planner、executor、verifier 三个阶段 agent 都能提交 requirement，但都走严格契约模式（带 client_ref、内容不可被改写）。它们不直接写 task graph，也不直接创建 task / edge；依赖关系由经 Agent Runtime 授权的 Task Manager Agent 根据全局视图编排。

### 12.2 事件解耦：活动自动进 log，ctxlib 只读 log

Task Manager 唯一的权威写动作是通过受控 graph_write service/tool **写 Task Graph**。它不主动"写"Event Log——Event Log 是 Agent Runtime 对 agent 活动和状态变化的**自动记录**，不是 agent 要调用的写接口。

runtime 自动记入 log 的内容包括：

```text
1. 收到的原始需求 / agent requirement（来自人类或 agent 的消息 / 调用本身）。
2. Task Manager 的 intake 决策（created / register / link_related / reject …，即它的输出）。
3. 各 agent 的工具调用描述、结论等活动痕迹。
```

上下文的沉淀由 Ctx Agent 从 log 中异步处理，Task Manager 不参与 ctxlib。

```text
Human UI / planner / executor / verifier
  -> Agent Runtime(role=task_manager, tool=graph_write)
  -> Task Manager Agent（intake + 校验 + 依赖编排）
       -> Task Graph（唯一权威写：requirement / task / phase endpoint / decision endpoint / edge）

runtime（自动记录，无需 agent 显式写）
  -> Event Log
       - 收到的需求 / agent requirement
       - Task Manager 的 intake 决策
       - 各 agent 的工具调用 / 结论

Agent Runtime(role=ctx_manager, tool=ctx_write)
  -> Ctx Manager Agent / Ctx Agent
       <- 只读 Event Log
       -> ctxlib（据 log 构建 / 更新 context block，如把原始需求存为 provenance）

Scheduler
  <- 读 Task Graph，决定何时向 Agent Runtime 提交 AgentRunParams（新 task ≠ 立即启动）
```

这样解耦的好处：

```text
- agent 不需要关心"怎么写日志"，只管做事，活动被自动记录。
- Task Manager 不需要知道 ctxlib 的结构，也不需要显式写 log。
- ctxlib 的唯一数据来源是 log，来源单一、可重放、可追溯。
- Ctx Agent 可以独立演进筛选 / 摘要 / provenance 策略，不影响 intake。
- 需求（意图）和 task（工作单元）都在 log 里留痕，不会因规整而丢失原话。
```

### 12.3 边界

```text
- Task Manager 作为 Agent Runtime invocation 权威写 Task Graph；其活动被 runtime 自动记入 Event Log；不写 ctxlib。
- Ctx Manager Agent / Ctx Agent 作为 Agent Runtime invocation 读 Event Log 写 ctxlib，不写 Task Graph。
- 需求原话作为 provenance，由 Ctx Agent 从 log 中提取，而非任何 agent 直接塞入 ctxlib。
```

---

## 13. 一个例子

```text
需求(人类，原话):
  "支持 codex，最好用 MCP 那种方式接进来，顺便把日志也统一一下"

Task Manager（宽松模式）产出，可能产生 2 个 task:

Task A
  title: 接入 Codex CLI 为统一 agent runtime 的一个 provider
  why: 用户希望除 Claude Code 外也能调度 Codex
  acceptance:
    - Codex 能以 headless 模式被 runtime 启动并回收结构化结果
    - capability profile 正确标注其能力
  delivery_type: code_change
  hint(来自需求，非强制): 用户倾向 MCP 接入方式   <- 交给 planner 判断
  depends_on: [Claude Code wrapper 已完成]

Task B
  title: 统一 agent runtime 的日志 / 事件记录
  why: 用户提到"日志统一"，与 Codex 接入是不同交付物
  acceptance: 所有 provider 的运行都产出统一 schema 的 event
  delivery_type: code_change
```

而当 Task A 的 planner 中途发现需要 A1/A2/A3 三个独立工作单元时，它以**严格模式**把三个 requirement 发给 Task Manager，各带 `client_ref`、验收意图和依赖意图；Task Manager 只登记、校验，并基于全局视图编排 task / phase endpoint / decision endpoint / edge。例如它可以生成 `Task A.verify depends_on Task A2.done`，并原样回显 `client_ref -> requirement_id / task_id`，不改动 A1/A2/A3 的内容。

---

## 14. 不变量

```text
1. 所有 agent invocation 都必须经 Agent Runtime，包括 Task Manager Agent 和 Ctx Manager Agent。
2. 除经 Agent Runtime 授权的 Task Manager Agent 外，任何角色不得直接写 task graph。
3. Task Manager 不产出 how；how 属于 plan 阶段。
4. 人类需求可规整；agent requirement 内容不可被改写。
5. 严格模式必须回显 client_ref 且保证幂等。
6. 严格模式对重复只 link_related，不 merge。
7. 内容字段归发起方；依赖编排、图关系与元数据归 Task Manager，且只可新增。
8. 实现型 task 无可验收标准不得进入 planned。
9. 冲突与重复只暴露和关联，不由 Task Manager 替人决策。
10. 所有 intake 决策都产生事件，便于追溯。
11. planner / executor / verifier 都可提交 requirement，但都走严格契约模式。
12. planner / executor / verifier 不直接创建 task / edge；依赖关系由 Task Manager Agent 编排。
13. task 使用固定 phase endpoint 表达生命周期；Task Manager 可以创建指向具体 endpoint 的依赖，例如 A.verify 依赖 B.done。
14. Task Manager 只通过 Agent Runtime 授权的 graph_write service/tool 权威写 Task Graph；其活动被 runtime 自动记入 Event Log；不写 ctxlib；ctxlib 只从 log 取数据。
```

---

## 15. 编排检查规则

Task Manager 在写入 Task Graph 前，至少检查以下内容：

### 15.1 是否真的需要新 task

默认把工作留在当前 phase 的执行范围内。只有工作具备独立验收、独立失败或重试、跨时间等待、不同权限或 owner、被其他 task 直接依赖，或生命周期超过当前 phase invocation 时，才建立新 task。

文件读取、一次工具调用、局部摘要和同一批准计划中的连续命令，不应仅因为可观察就被提升为 task。

### 15.2 edge 是否连接到最早需要结果的位置

依赖必须落到真正消费结果的 phase endpoint：

```text
B.verify -> A.plan     A 的方案依赖 B 的已验证结论
B.verify -> A.execute  A 可以先规划，但实施必须等待 B
B.verify -> A.verify   A 可以先实施，但最终验收必须包含 B
```

每条 edge 都必须说明 source endpoint、target endpoint、控制条件、传递的 evidence 或 message，以及条件为 false 或结果过期时的处理。

### 15.3 如何处理失败和过期结果

```text
- 局部实现或验证失败：为同一 Task Contract 创建新的 attempt。
- Task Contract 不完整或自相矛盾：阻塞受影响 endpoint，请求澄清或重新立约。
- verify 暴露出独立工作：登记新 task，并连接到消费其结果的 endpoint。
- candidate 相对新 revision 已过期：使旧验证失效，重新 verify 或重新 plan。
- 高风险决定缺少权限：创建或关联 human decision endpoint，不得推断已经批准。
```

### 15.4 必须拒绝的模式

```text
- 每个 agent 或 tool call 建一个 task。
- 只在 prompt 中描述依赖，不写入 graph。
- 只有 execute 或 verify 受影响，却阻塞整个 task。
- 为表达 parent/child 所有权而制造环。
- 把 worker summary 当作验证 evidence。
- 每次 attempt 失败都创建新 task。
- acceptance 和 merge 条件未满足就标记 done。
- 为迁就某个实现方案而修改 requirement 或 Task Contract 内容。
```

写入结果必须能回答：这次 mutation 接受了什么 requirement、创建或关联了哪个 task、增加了哪些 endpoint / edge / blocker、每项变更的理由是什么，以及哪些 endpoint 的 runnable 状态发生了变化。
