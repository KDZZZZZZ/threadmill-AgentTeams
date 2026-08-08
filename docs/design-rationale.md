# Threadmill 设计理由：为什么是两张持久图与一个执行边界

状态：Draft
定位：本文回答 Threadmill 架构中的“为什么”：为什么只保留两张持久图、为什么 Coordination Graph 可热修改、为什么 Manager 不看运行过程、为什么同一轮次共享 Workspace、为什么 Context Graph 是所有 Agent 的通信面，以及为什么在 AgentTeams 基座上不直接复用 Matrix 房间编排、TeamHarness DAG 和私有记忆。它不列接口，不描述 UI。

> **语义以 [threadmill-unified-design.md](./threadmill-unified-design.md) 为准**。本文解释统一设计的选择；模块文档可以演进，但不应悄悄改变这里定义的工作模型。

---

## 1. 为什么是“两张持久图 + 一个执行边界”

Threadmill 管理的三类对象必须分开持久化，因为它们回答不同的问题、有不同的生命周期和唯一写入口：

| 对象 | 回答的问题 | 为什么不能合并 |
| --- | --- | --- |
| Coordination Graph | 哪个 Phase Endpoint 可以运行、被什么阻塞、阶段交付是否满足 | 未履行的因果义务；跨 Task、跨 Agent、跨时间 |
| Context Graph | 哪些记忆可见、如何关联、如何检索、哪些订阅需要推送 | 已提炼的知识关系；供新 Invocation 复用 |
| Agent Runtime / Workspace | 一次 Invocation 如何在权限、工作区和预算内执行 | 运行现场；随 Invocation/轮次消失或封存 |

**职责分离的判据是“谁需要看到什么”**，不是数据量的多少：

- Coordination Graph 的消费者是 Task Manager 与 Scheduler，它们只关心阶段依赖、完成信号、交付物/报告要求与结果引用。它们不需要也不应看到一次运行的中间推理、工具输出或探索轨迹。
- Context Graph 的消费者是所有 Agent（包括 Task Manager 自己），它们需要的是从运行证据中提炼出的、有出处的知识，而不是事件流水本身。因此 Context Graph 是 Event Log / Artifact Store 的可追溯投影，而不是第二个 Event Log。
- Runtime/Workspace 的消费者只有当前 Invocation。运行现场默认留在 Invocation 内；轮次结束后 Workspace 封存为 evidence，按保留策略清理。

**为什么不建立 Execution Graph（phase 内持久执行图）**：phase 内的执行步骤、LLM 调用、工具链是 Runtime 的内部现场。把“这一次怎样运行”持久化成图，会引入第三个写入口、第三套失效语义，却不会让任何编排者变得更正确——编排者需要的全部信息已经由 `PhaseOutput`（阶段结束输出）与 `OrchestrationProposal`（运行中主动建议）承载。只有当内部工作获得独立生命周期（独立验收、独立重试、跨时间等待、单独授权、被其他 Task 直接依赖）时，Task Manager 才把它提升为新的 Task Contract 并写入 Coordination Graph。这是“执行结构可递归、持久图不膨胀”的准确含义。

同理，**不引入 Agent mailbox**：Agent 之间的一次性问答、通知通道会把协调义务从 Coordination Graph 泄漏到运行时消息里，制造第二个无法审计的“谁等谁”来源。外部记忆只来自切片、探索、检索、订阅与自动 Context Delta；协调义务只存在于 Coordination Graph 的边。

---

## 2. 为什么 Coordination Graph 可热修改，但 Task Manager 唯一写

**可热修改**：编排不是一次规划终身执行的。运行中的 Agent 会真实地发现：任务需要拆分、存在未声明的前置、串行可以改并行、验证失败需要重排。把这些发现冻结在“规划时就定死”的图里，等于让编排者事后失明。因此 Coordination Graph 是当前编排，允许被热修改。

**唯一写入口**：热修改能力必须与修改权分离。如果每个 Agent 都能连边、拆 Task、改 blocker，协调义务会随 Agent 数量平方增长，并出现“两个 Agent 互相给自己松绑”的循环。所以：

```text
运行中的 Agent 发现编排不再合适
  -> 提交 OrchestrationProposal（意图 + 理由 + evidence 引用，自由文本，不是图命令）
  -> Task Manager 结合当前图与可见证据裁决：接受（热修改图）或拒绝（结构化理由）
  -> Scheduler 只重算 runnable endpoint，不解释建议
```

为什么用“建议”而不是“Agent 直接发图变更请求”：拆分、连边、失效决策需要看到全局 Task 集合与历史验收，phase agent 只拥有局部视野。Task Manager 是唯一拥有全局视野的默认编排者，因此也是唯一写入口。拆分、缺前置、执行失败、验证失败、计划失效共用同一种建议协议，不新增 Split Request、Failure Request、Rework Task 等实体。

**为什么热修改不等于无审计**：图变更历史由 Event Log 审计，但审计机制不限制运行时热修改。审计回答“谁在什么时候改了什么”，不回答“该不该改”——后者永远是 Task Manager 的职责。

---

## 3. 为什么 Manager 不看运行过程

Task Manager 的职责是编排，不是旁观。允许它读取 phase agent 的中间推理、工具输出、探索轨迹或未提交上下文，会产生三个问题：

1. **信号污染**：中间过程充满试探与噪音，Manager 会基于“看起来努力”而不是“交付满足契约”做决策。
2. **权威分叉**：Manager 一旦能看到过程，就难以拒绝基于过程信息替 phase agent 做决定，最终 Manager 变成第二个 executor。
3. **成本失控**：把未提交上下文纳入 Manager 的观察面，等于要求 Manager 的 Context Slice 无限膨胀。

因此只有两类结构化边界输出可以进入 Task Manager 的视野：

```text
1. 阶段结束时的 PhaseOutput（DeliveryRefs / ReportRef / EvidenceRefs /
   WorkspaceRevision / ContextGraphRevision）
2. 运行中主动提交的 OrchestrationProposal
```

Runtime 只校验输出形状与必填引用，不解释内容；Task Manager 能读取所有 completed endpoint 的报告、交付物和证据引用，但读不到运行过程。这同时保护了 phase agent 的探索自由——失败尝试、被否方案、中间输出都属于 Invocation 内部现场，不构成项目事实。

---

## 4. 为什么同一轮次的 plan、execute、verify 共享 Workspace

**同一个 Task 轮次的 plan、execute、verify 默认共享同一个 Workspace Binding**——三个阶段可以由不同 Agent、不同 provider 或不同 Thread 执行，但它们看到的是同一份受控执行现场。

这解决四个实际问题：

1. **executor 直接消费 planner 的现场产物**：Approved Plan、Declared Write Set、基线信息就留在现场，不需要跨阶段转述。
2. **verifier 检查真实候选现场**：它验证的是 execute 留下的真实 diff、未提交文件和工具状态，而不是重新拼装的近似副本。
3. **阶段切换不丢状态**：未提交文件、生成物、工具状态在 plan → execute → verify 之间连续存在。
4. **Agent 可替换，Task 身份不变**：executor 崩溃后新的 Invocation 从同一 Workspace 继续，证据链不因换 Agent 而断裂。

**共享 Workspace 不等于共享权限**。权限随 phase lease 切换：plan 默认只读源码（可写结构化 plan artifact），execute 可写批准范围，verify 默认不可修改候选实现。任何阶段只能有一个有效写 lease；同一 Task 内需要并行时，只能并行只读准备，或由 Task Manager 拆为具有独立 Workspace 的 Task。

**为什么验证失败要重开轮次而不是在旧现场继续修**：旧现场是失败证据，新轮次从最新有效基线创建新 Workspace，保证“验证结果绑定哪个 revision”永远可回答。若运行中的 Agent 认为应局部修复、拆分或调整依赖，走 OrchestrationProposal；Agent 和 Runtime 都不能自行跳转 phase 或在旧现场无审计地继续。

**Workspace 为什么不是图节点**：Workspace 是轮次的执行现场，不是协调义务。把 Workspace 放进 Coordination Graph 会让“谁阻塞谁”和“文件在哪个目录”两种问题混在一个模型里；它也不承担跨 Agent 通信，通信由 Context Graph 与 Coordination Graph 各司其职。

---

## 5. 为什么 Context Graph 是所有 Agent 的通信面

### 5.1 一个共享的、有准入的外部记忆

临时 Invocation 的直接问题是：新 Agent 从哪里获得已经确认的项目事实？把历史 transcript 全部塞回 prompt 既昂贵，也会把猜测、旧结论和失败尝试混在一起。Context Graph 解决的是知识复用，不是“保存更多聊天”：

- 新 Agent 如何获得与当前 Task/phase 相关的知识切片；
- 新发现如何与已有知识建立逻辑邻接；
- Agent 如何逐步探索，而非一次注入全库；
- 如何控制准入、近重复、过时、冲突与垃圾。

### 5.2 为什么所有 Agent 使用同一接口，且都能探索

Task Manager、planner、executor、verifier 共享同一 Context 读接口（列表、探索、检索、订阅）。如果 Manager 与 phase agent 各有一套上下文机制，就会出现“Manager 依据的记忆”与“执行者依据的记忆”不一致——这正是统一设计要消灭的分叉。探索（`Explore`）与列表（`ListSubgraphs`）是受权限约束的普通读操作，不需要 Ctx Manager 逐次推理或批准，否则每次探索都是一次语义裁决，Ctx Manager 会成为吞吐瓶颈。

### 5.3 为什么 Ctx Manager 只响应检索与准入 MemoryCandidate

Ctx Manager 是 Context Graph 的唯一写入口，但它的语义判断只出现在两个边界：

1. **响应检索请求（Retrieve）**：Agent 在列表与探索不足时提交 intent、scope 与推理锚点，Ctx Manager 做多路召回并返回带 path explanation 的记忆子图切片。
2. **准入 MemoryCandidate**：Agent 标注“值得记住的东西”提交候选，Ctx Manager 校验证据、权限、价值，决定 create / revise / supersede / dispute / reject。

它不主动巡图、不主动提示、不决定普通探索与切片、不执行订阅或推送。原因：主动提示是“系统认为自己知道 Agent 需要什么”，它把知识判断从 Agent 的明确请求变成系统的隐式猜测，且无法审计。切片的生成是 Context service 按 role/purpose/权限的受控响应，不是 Ctx Manager 的观察行为。

### 5.4 为什么订阅只有两种来源，推送自动执行

`ContextSubscription` 只有两种来源：**切片自动订阅**（生成初始/检索切片时自动订阅所含子图）与 **Agent 主动订阅**（从可见子图列表中选择）。推送由自动化订阅执行器触发：图提交 revision → 执行器匹配子图、事件类型、权限与新鲜度 → 按 subgraph revision 合并 → Runtime 发出 Context Delta → 记录是否消费。

为什么不允许其他订阅/推送路径：订阅之外的旁路推送（一次性问答、mailbox、Ctx Manager 主动推送）会制造“Agent 收到但不知道来自哪个订阅”的不可追溯上下文，也会让 Ctx Manager 重新承担它不该承担的主动观察职责。自动推送是基础设施行为，不调用 Ctx Manager 做逐条判断；Delta 必须由已存在的订阅触发，增量、可合并、可重放。

### 5.5 推送与协调边的边界

- 已订阅子图更新 → Context Delta 推送（Task Manager 与 phase agent 语义相同）。
- target phase 必须等待 source 结果 → Coordination Edge，只引用 source endpoint 的 PhaseOutput。
- Delta 证明当前编排或计划失效 → 收到 Delta 的 Agent 提交 OrchestrationProposal，由 Task Manager 裁决并热修改图。

两条通道不混用：Delta 传递知识更新，边传递因果义务。

### 5.6 为什么是图的多重聚类，而不是文件持久化

把记忆存成分层文件（目录树 + 正文）时，Agent 找到所需知识的成本可拆成三个量：**最终相关记忆量 K**（任务真正需要的、去重后的记忆条数）、**相关维度数 Q**（任务需要从多少个彼此不能被同一目录层级自然覆盖的视角找知识，如 Task、Module/Symbol、历史失败、架构主题、Requirement；同一事实可能同时服务多个维度）、**平均导航深度 H**（每个维度从入口定位到有用记忆，平均要经历几轮"读索引/摘要→判断下一层去哪"的检索决策）。文件方案的 Agent 成本近似：

```text
C_file ≈ Q × H + Overfetch + K
```

其中 Overfetch 是为找到那 K 条有效记忆而被迫读入的无关同文件内容。一条记忆在文件树里只能挂一个目录下；覆盖 Q 个维度，就要在 Q 个入口各走 H 层。

Context Graph 用**多重聚类**把前两项移出 Agent：一条记忆在写入时同时登记到多个子图（Task、Module、失败模式、架构决定），任何一个维度的入口都能直达它，成本近似：

```text
C_graph ≈ K + F
```

其中 F 是为防止初始切片漏召回而给出的少量"未展开候选子图/边界摘要"，Agent 只在需要时继续展开。本质上，图把"运行时的 Q×H 导航"换成了"写入时的归属打标"，由 Ctx Manager 的准入与连边质量承担后者。

需要收紧三点：其一，**归属质量决定成本优势是否兑现**——归属打错比目录翻层更糟，这正是 MemoryCandidate 需要明确准入规则、相似不等于直接更新、Agent 不能自封可信记忆的原因；其二，公平的对比对象不是裸文件，而是"文件 + 向量检索"，图真正不可替代的是结构化关系（supports / contradicts / supersedes / derived_from）、可追溯来源（SourceRefs）与渐进披露（初始切片 + Frontier），而不是聚类本身；其三，上述公式只算了读取侧成本，图的建设成本（候选准入、去重、连边、版本与 supersede 链维护）是持续的写入侧投入，只有在记忆被反复复用时才回本——这正是 Threadmill"同一批项目知识被多个 Agent、多个 Task 反复消费"这一前提成立时，Context Graph 才值得做的原因。

---

## 6. 为什么不在 AgentTeams 基座上直接复用 Matrix / DAG / 私有记忆

AgentTeams（`third_party/agentteams`）是已验证的部署基座：Matrix 房间（Tuwunel）、AI 网关（Higress）、对象存储（MinIO）、Kubernetes 控制器与 Worker 容器生命周期都是可复用的成熟能力。但它的**编排与记忆模型**与 Threadmill 语义冲突，直接复用会把下面三个错误带进系统。

### 6.1 为什么不用 Matrix 房间作编排协议

AgentTeams 的协作模型是“Human + Manager + Worker 同在一个房间，通过消息推进”：委派、ack、提交、阻塞都表达为 `TASK_COMPLETED`/`TASK_BLOCKED` 文本消息（`third_party/agentteams/docs/teamharness-project-task-runtime-design.md` 的 Event Resume Contract），git 操作是 `git-request:`/`git-result:` 的 prompt 级代执行（`third_party/agentteams/manager/agent/skills/git-delegation-management/SKILL.md`）。

这恰恰是 Threadmill 要消灭的模型：

| AgentTeams 的房间模型 | Threadmill 的替代 |
| --- | --- |
| 协调义务以聊天文本存在，无法结构化查询与审计 | Coordination Graph 的边 + PhaseOutput 结构化输出 |
| 完成信号是消息文本，解析失败即状态丢失 | PhaseOutput 形状由 Runtime 校验，绑定 revision |
| 消息需要 mention/房间/ping-pong 防护，Agent 间可互相“对话” | 无 mailbox；Agent 只经 Context 接口获取外部记忆 |
| 验收是 Leader 在聊天里人工接受（`accept_task_result`） | verify 独立判断 + Merge Queue 机械合入 |

房间与消息**保留为人工可见性与 requester 最终报告通道**（`plugins/teamharness/mcp/message_tool.py` 的 `replyRoute` 报告路径、`roomflow_tool.py`），但不再承担控制面协议。AgentTeams 自身已证明 worker 角色禁用 message 工具（`plugins/teamharness/mcp/server.py` 的 `MESSAGE_TOOL_BLOCKED_ROLES`），与“phase agent 不直接通信”一致——这条边界直接继承。

### 6.2 为什么不用 TeamHarness DAG 直接当 Coordination Graph

TeamHarness 的 projectflow/taskflow（`third_party/agentteams/copaw/src/copaw_worker/task.py`、`plugins/teamharness/mcp/server.py`）有可复用的状态机内核：DAG 环检测、`_ready_nodes` 就绪计算、`delegate → ack → submit → check` 原子流转、`shared/projects|tasks/{id}/meta.json` 文件存储。但它缺 Threadmill 编排的四个关键语义：

1. **无 Phase/轮次维度**：TaskMeta 只有 `assigned → in_progress → submitted`，`REVISION_NEEDED` 回到同一目录继续修——没有“plan → execute → verify 共享现场、失败重开轮次”的模型。
2. **无 revision 与失效**：`meta.json` 是当前态快照，没有输入 revision、没有结果失效语义，无法回答“哪份验证结果仍有效”。
3. **Leader 双写**：Leader 既编排又直接写 meta.json/plan.md/result.md 并自行验收（`accept_task_result`）——这违反“Task Manager 唯一写入口”与“verify 独立于产生者”两条不变量。
4. **无 DeliverySpec/ReportSpec**：委派只传 spec.md，没有“该阶段必须交付什么、报告必须回答什么”的 endpoint 契约。

因此复用方式是**适配封装**：把 FileSystemTaskStore 的存储协议与 `_ready_nodes` 内核当作 Coordination Graph 的物理底座，在其上加 Phase/轮次维度、revision、endpoint 契约与唯一写入口（见 architecture.md 6.2/6.3）。

### 6.3 为什么不用 AgentTeams 的私有记忆

AgentTeams 的记忆全部是单 Agent 私有：OpenClaw 式 `MEMORY.md`/`memory/` 目录（`docs/declarative-resource-management.md`）、CoPaw 的 remelight 记忆后端（`copaw/src/matrix/config.py`）、Hermes 的 `memory_enabled`（`hermes/src/hermes_worker/bridge.py`）。它们随各自 worker 的 MinIO 前缀同步，没有共享视图、没有准入、没有子图、没有订阅。

Threadmill 需要的是**共享的、可追溯的、有准入的 Context Graph**：所有 Agent 经同一接口读取，知识从 Event Log/Artifact Store 提炼，MemoryCandidate 由 Ctx Manager 准入，订阅与 Delta 自动推送。私有记忆的直接复用会制造 N 个互不可见的“项目真相”，且无法回答“这条知识基于什么证据、什么时候失效”。`docs/k8s-native-agent-orch.md` 预留的 `shared/knowledge/` 前缀没有实现，只能作为 Context Graph 的物理落点，不能当作现成记忆服务。

### 6.4 复用边界的总原则

```text
复用 AgentTeams 的“承载”：
  MinIO 物理存储、文件同步管线、MCP 工具面、容器生命周期、
  Matrix/Element 人工界面、技能分发、迁移框架。

不复用 AgentTeams 的“编排与记忆语义”：
  房间消息协议、持久 Worker 身份、Leader 双写、DAG 状态词表、
  私有记忆。这些全部由 Threadmill 控制面新建。

不宣称 AgentTeams 已有（它没有）：
  Event Log、Context Graph、git worktree、Merge Queue、Scheduler。
  以上五项是 Threadmill 新建组件，落点见 architecture.md 6.3。
```

---

## 7. 设计检查

后续设计至少应能回答：

1. 杀掉所有 Agent 进程后，Coordination Graph（Task/Phase Endpoint/edge）、Workspace Binding 与 Event Log/Artifact Store 是否足以恢复未完成工作？
2. 某个结论来自 Task Contract、Agent 推断，还是已经验证的 evidence？
3. 每条 Coordination Edge 阻止哪个 Phase Endpoint、携带什么数据、解除条件是什么、source 失败时怎么办？
4. 失败是在重试同一 Task Contract（重开轮次），还是暴露了需要独立 Task 的新工作？
5. 每个 ContextSlice 绑定哪个 Context Graph revision 与 input revision，过期后谁触发重选或重验？
6. 每个 MemoryCandidate 是否有 SourceRefs、谁准入、准入后哪些订阅收到 Delta？
7. 最终写入 main 的决定能否追溯到 Requirement、真实 diff 和仍有效的验证结果？
8. Task Manager 是否只看 PhaseOutput 与 OrchestrationProposal，没有读取任何未提交过程上下文？
9. 是否还存在任何 Agent 直接写图、直接通信、或 Ctx Manager 主动提示的路径？

答不清这些问题时，不应该继续增加状态、Agent 角色或记忆策略；应先修正工作模型。
