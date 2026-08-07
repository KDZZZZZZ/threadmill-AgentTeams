# Multi-Agent Vibe Coding Control Plane 总体架构

版本：v0.2
状态：Draft
定位：本文件只描述产品判断、总体架构、模块简述和模块间关系。各模块详细设计见文末链接。

---

## 1. 产品判断

当前 vibe coding 的核心瓶颈不是“agent 不够聪明”，而是 **人类被迫承担了调度系统的工作**。

当多个 agent 同时工作时，用户通常需要手动处理这些事情：

1. 重新告诉新 session 当前项目状态。
2. 解释其他 agent 正在做什么。
3. 手动拆任务。
4. 手动同步上下文。
5. 手动避免 worktree 和代码冲突。
6. 手动判断哪个结果可以合并。
7. 手动判断新 task 是否重复、依赖谁、会阻塞谁。
8. 手动判断运行中的 agent 应该看哪些上下文。

这个系统的产品判断是：

> 用户不应该管理 session，也不应该手动调度每个 agent。用户只应该表达需求、预算和并发意图；系统负责把需求登记为 requirement，再生成 task graph（统一 task 节点 + 图关系），并调度可用 agent 完成、验证和合并。

因此，产品形态不是“多开几个 Claude/Codex 窗口”，而是一个 **Multi-Agent Control Plane**：

```text
用户输入：
  我需要什么 + 我愿意投入多少钱/多少 agent/多少时间

系统负责：
  通过 Agent Runtime 启动 Task Manager Agent，登记 requirement、创建任务并编排依赖
  通过 Agent Runtime 启动 Ctx Manager Agent / Ctx Agent，控制上下文存取和运行时查询
  通过 Agent Runtime 启动 planner / executor / verifier，并包装 CLI agent 自身的 worktree/tool/git 能力
  调度 agent、验收、处理冲突、合并
```

---

## 2. 核心目标

1. **需求和并发解耦**
   用户发布新任务，不等于手动开启新 agent；用户点击 `agent +1`，也不等于给某个 agent 分配具体任务。

2. **第一阶段先支持 Claude Code 基本包装**
   MVP 不先追求同时接入所有 CLI agent，而是先把 Claude Code CLI 的 headless 启动、输入输出、事件记录和能力声明跑通。

3. **所有 agent 都经 Agent Runtime 运行**
   Task Manager Agent、Ctx Manager Agent / Ctx Agent、planner、executor、verifier 都不是旁路服务；它们都是 Agent Runtime 管理的 agent invocation。差异只在 role、system prompt、context pack、tool/capability 授权和可调用的 backend service。

4. **worktree 属于 agent CLI 能力包装的一部分**
   系统不在 task graph 里重新实现一套独立 worktree 抽象；Agent Runtime 负责包装 CLI agent 自身的 worktree、tool、git 和执行目录能力，并把结果归一化给上层调度。

5. **用 task graph 管理工作，而不是用 session 管理工作**
   用户输入是 requirement；task graph 保存统一的 task、attempt 和 phase endpoint，拆解、依赖、阻塞、决策和冲突都通过图关系表达。

6. **所有 requirement 都经 Task Manager Agent 编排成 task graph**
   创建或更新 task graph 需要看到当前所有 task 及其状态，避免重复、错误依赖和不可验收拆分；人类和其他 agent 都向 Task Manager Agent 提交 requirement，由它统一创建 task、phase endpoint、decision endpoint、edge 或 blocker。

7. **用 ctxlib 管理项目记忆，而不是依赖 session memory**
   所有有价值的设计、判断、验收、失败和冲突信息沉淀到结构化上下文库。

8. **ctxlib 存取由 Ctx Manager Agent / Ctx Agent 控制**
   新 agent 启动前的 context pack 和运行中其他 agent 的 ctxlib 查询，都通过 Ctx Agent 受控访问。

9. **每个 task 用 CLI agent 包装出来的 worktree 能力隔离执行**
   agent 不直接修改 main；verify 通过后进入 merge queue。

10. **复杂任务允许通过提交 requirement 扩展 task graph 作为交付**
   执行 task 的 planner / executor / verifier 不必一次性解决所有细节，可以向 Task Manager Agent 提交新的 requirement；Task Manager Agent 负责把这些 requirement 编排成 task、phase endpoint、decision endpoint、依赖、阻塞和拆解关系。

11. **verify 是进入项目事实的闸门**
   task 只有通过验收后才允许 merge；失败则回到 plan 循环。

12. **并发冲突由系统协调**
   verify/merge 阶段检查活跃 task，如果冲突则广播 conflict context 给相关 active task，避免互相等待和死锁。

---

## 3. 非目标

本系统不追求：

1. 让所有 CLI agent 拥有完全一致的能力。
2. 把所有历史 session 原样塞进新 agent 上下文。
3. 让 agent 自由修改 main branch。
4. 让 execute agent 在没有 replan 的情况下任意扩大任务范围。
5. 用 embedding-only memory 替代结构化上下文管理。
6. 让人类继续手动协调每个 agent 的具体工作。

---

## 4. 总体架构

```text
┌──────────────────────────────────────────────┐
│                   Human UI                   │
│  需求输入 / agent+1 / 预算 / 进度 / 验收       │
└───────────────────────┬──────────────────────┘
                        │
                        ▼
┌──────────────────────────────────────────────┐
│                 Control Plane                │
│  Scheduler / Policy / Budget / Orchestration │
└───────────────────────┬──────────────────────┘
                        │ AgentRunParams(role/capability/tool policy)
                        ▼
┌──────────────────────────────────────────────┐
│                 Agent Runtime                │
│  invoke / permission / tool boundary / events│
└───────┬──────────────┬──────────────┬────────┘
        │              │              │
        ▼              ▼              ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│ Task Manager │ │ Ctx Manager  │ │ Worker Agents│
│ Agent        │ │ Agent        │ │ plan/exec/ver│
└──────┬───────┘ └──────┬───────┘ └──────┬───────┘
       │graph_write     │ctx_read/write  │worktree/tools
       ▼                ▼                ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│ Task Graph   │ │ Context Lib  │ │ CLI Worktree │
│ 状态与依赖    │ │ 项目级记忆    │ │ Tools / Git  │
└──────┬───────┘ └──────┬───────┘ └──────┬───────┘
       │                │                │
       ▼                ▼                ▼
┌──────────────────────────────────────────────┐
│              Verify / Merge Queue            │
│        Diff / Conflict / Project Facts       │
└───────────────────────┬──────────────────────┘
                        ▼
┌──────────────────────────────────────────────┐
│          Event Log / Artifact Store          │
│     Logs / Results / Diff / Test Evidence    │
└──────────────────────────────────────────────┘
```

一句话概括：

```text
Agent Runtime 是所有 agent invocation 的统一入口，包括 Task Manager Agent、Ctx Manager Agent 和 worker agents。
Task Manager Agent 接收 requirement，并决定是否以及如何编排成 task / phase endpoint / decision endpoint / edge / blocker。
Task Graph 记录现在有哪些 task、状态是什么、谁阻塞谁。
Ctx Manager Agent / Ctx Agent 决定 agent 应该知道什么，以及运行中能查什么。
Context Lib 保存可复用的项目记忆。
Agent Runtime 决定谁来做，并包装 CLI agent 自身的 worktree/tool/git 能力。
Verifier 和 Merge Queue 决定什么能进入项目事实。
Event Log 决定系统如何追溯和复盘。
```

---

## 4.1 技术栈边界

本项目技术栈确定为：**Go backend + Electron shell + React/TypeScript/Vite frontend**。

```text
Go backend
  - 承载 Control Plane、Task Manager Agent、Task Graph、Ctx Agent、ctxlib、Agent Runtime、Event/Artifact Store、Workspace/Merge Queue。
  - 后端接口、领域模型、调度状态机和权限策略以 Go 类型为设计源头。

Electron shell
  - 负责桌面壳、窗口生命周期、本地进程启动入口和与 Go backend 的本机通信边界。
  - 不承载核心调度规则，不直接写 task graph / ctxlib。

React + TypeScript + Vite frontend
  - 负责需求输入、task graph 可视化、agent 状态、日志流、diff/test 证据和人工审批 UI。
  - TypeScript 类型应从 Go backend API schema 生成或镜像，不作为核心领域模型的唯一事实来源。
```

文档中的接口设计代码块默认以 Go backend 为准；只有 Electron/React/Vite 专属 UI 代码才使用 TypeScript。

## 4.2 设计基线：工作不属于 Agent Session

Threadmill 管理的是尚未完成的工作，而不是某个 Agent 对话。即使一个 Agent 退出、换 provider 或被重试，下面这些问题仍然必须有明确答案：

- 原始 requirement 是什么，哪些约束不能在转述中丢失；
- 一个 task 是否必须等待另一个 task，以及等到哪个 phase endpoint；
- 当前 attempt 基于哪个输入 revision，实际改动是否越过批准范围；
- 哪一份验证结果仍然有效，谁有资格让结果进入主分支。

工作沿着下面的链路逐步收敛：

```text
Requirement -> Task Contract -> Task -> Task Attempt -> Agent Invocation
```

它们不是同一个对象的不同名字：

- `Requirement` 保存最初为什么要做；
- `Task Contract` 固定要交付什么、边界在哪里、怎样算完成；
- `Task` 是需要持续协调的工作身份；
- `Task Attempt` 是对同一契约的一次有界尝试；
- `Agent Invocation` 是某个阶段临时借用的一次计算能力。

Agent Invocation 可以重建，Task Contract、Task 和未完成的依赖不能随 session 一起消失。一次 invocation 只拿到完成当前职责所需的 Task Contract、attempt、允许工具、工作区、Context Pack 和输出约束。

### 领域术语边界

```text
Thread
  provider 为一次 Agent Invocation 保留的局部对话状态；丢弃 Thread 不应丢失 Task 或已接受的项目事实。

Worker Capacity
  Scheduler 当前可以并发使用的 Agent Invocation 数量；只改变吞吐，不改变 Task Graph 的含义。

Evidence
  用于判断主张的可观察结果，例如 diff、测试结果、tool output 或 human decision，并且可以追溯来源。

Project Fact
  在验收或决策边界通过后，获准供后续工作复用的主张。

Context Block
  从 Event 或 Artifact 中提炼出的、可追溯且可复用的陈述；可能被替代，不能自动视为 Project Fact。

Context Pack
  针对一次 Agent Invocation 及其精确工作边界选出的有限 Context Block 快照，不等于全量项目历史。
```

把 Agent 视为临时执行资源解决四个问题：

1. executor 崩溃后，新的 invocation 可以从显式状态恢复，而不是猜旧对话发生了什么；
2. planner、executor 和 verifier 可以由不同 provider 承担，调度不依赖某个 CLI 的 session 语义；
3. 并发 Agent 不会制造多个项目真相，任务依赖、验收结论和已合入事实仍有单一权威记录；
4. 权限可以按 invocation 收口：规划可只读，执行只写指定工作区，验证默认不能修改实现。

“临时”不等于每说一句话都新建 invocation。只要输入基线、角色、权限和目标没有变化，一个 invocation 可以持续完成它的局部工作；发生重规划、权限变化、上下文基线变化或角色切换时，才建立新的边界。

### Plan、Execute、Verify 的分工

三个阶段不是让不同 Agent 重复复述同一段话，而是分别固定三种不能由同一份输出证明的事实：

```text
Plan
  在修改前声明影响面、依赖事实、权限需求和验收证据。
  可以请求 Task Manager 扩展 graph，但不能按实现偏好改写 Task Contract。

Execute
  在隔离工作区内实施批准范围，产生候选 diff、测试输出、工具记录和新发现。
  不能静默扩大范围、直接改 graph 或宣布自己已经完成。

Verify
  同时读取 Task Contract、批准的 plan、真实 diff、测试证据和输入 revision，
  判断候选结果是否可接受，而不是判断 executor 是否看起来努力过。
```

验证失败通常为同一个 task 创建新 attempt；只有暴露出独立生命周期的工作时才创建新 task。`done` 不是执行阶段，而是验收、依赖和交付条件全部满足后的图结论。

### 各模块各自拥有一个决定

| 模块 | 它拥有的决定 | 它不能替谁决定 |
| --- | --- | --- |
| Task Manager Agent | requirement 是否形成 task，以及 endpoint 如何建立关系 | 不替 planner 选择实现方案 |
| Scheduler | 当前哪些 endpoint 值得占用 worker capacity | 不改 Task Contract 和 graph 语义 |
| Agent Runtime | invocation 如何在明确权限、工作区和预算内运行 | 不判断 task 是否完成 |
| Ctx Manager Agent | invocation 应获得哪些可追溯上下文 | 不把摘要直接宣布为项目事实 |
| Executor | 如何在批准范围内产生候选结果 | 不批准自己的结果，不写主分支 |
| Verifier | 候选结果是否满足契约、证据是否新鲜 | 不修改实现来让验证通过 |
| Merge Queue | 已获资格的候选是否能机械进入最新 main | 不修冲突，不重写 graph |

如果一个实现让某个模块替另一个模块作决定，权威来源就会分叉。

### 设计检查

后续设计至少应能回答：

1. 杀掉所有 Agent 进程后，哪些对象足以恢复未完成工作？
2. 某个结论来自 Task Contract、Agent 推断，还是已经验证的 evidence？
3. 每条 graph edge 阻止哪个 endpoint，携带什么数据，解除条件是什么？
4. 失败是在重试同一 Task Contract，还是暴露了新的独立工作？
5. Context Pack 绑定哪个 input revision，过期后谁触发重选或重验？
6. 最终写入 main 的决定能否追溯到 requirement、真实 diff 和仍有效的验证结果？

---

## 5. 模块简述

## 5.1 Human UI

Human UI 由 Electron shell 承载 React/TypeScript/Vite frontend，面向用户不暴露底层 session，而暴露需求、预算、agent capacity、task graph、active agents、verify 状态和 merge 状态。

核心操作：

```text
- 提交需求
- 增加/减少 agent 数量
- 调整预算
- 查看 task graph
- 查看阻塞和冲突
- 批准高风险操作
- 查看验收结果
```

---

## 5.2 Control Plane

Control Plane 是 Go backend 中的调度中枢，负责把用户需求、预算和 agent capacity 转成可执行调度。

它不直接创建 task，也不直接读写 ctxlib；这些动作分别通过 Agent Runtime 启动的专门 agent 完成：

```text
Control Plane -> Agent Runtime(role=task_manager)：提交 / 登记 requirement，由 Task Manager Agent 编排 task、phase endpoint、decision endpoint、edge、blocker 或 task 状态更新。
Control Plane -> Agent Runtime(role=ctx_manager)：请求 Ctx Manager Agent 为某个 task phase 选择 context pack，或处理运行时 ctx 查询。
Control Plane -> Agent Runtime(role=planner/executor/verifier)：启动 Claude Code planner / executor / verifier 等 CLI worker。
Control Plane -> Merge Queue：提交 verify passed 的结果进入合并流程。
Control Plane -> Event Log：记录所有关键事件。
```

第一阶段 Control Plane 的实现范围只需要覆盖：Claude Code wrapper、Task Manager Agent、Task Graph 调度、Ctx Agent、CtxLib 存取。

---

## 5.3 Task Manager Agent / Task Graph

Task Manager Agent 是 task graph 的唯一写入口，同时它自己也是经 Agent Runtime 启动和记录的系统 agent。把 requirement 编排成 task graph 变更之前，它必须看到当前所有 task 及其状态，判断新 task 是否重复、是否应该拆分、依赖谁、会阻塞谁，以及验收标准是否足够清晰。

Task Graph 是工作结构的存储和状态机。

- **requirement**：人类或 agent 提出的原始需求、目标、约束和验收意图；它是 provenance，不是可调度 task。
- **task contract**：固定要交付什么、为什么交付、允许的边界和怎样算完成；不包含 planner 的实现步骤。
- **task**：由 task contract 约束的持久工作身份，不区分 root / child 类型。
- **task attempt**：对同一个 task contract 的一次有界尝试；失败或输入过期通常创建新 attempt，而不是新 task。
- **phase endpoint**：`prepare / plan / execute / verify / done` 的编排锚点。
- **edge**：phase endpoint 之间的依赖、阻塞、冲突、替代或决策关系，并可携带 evidence。
- **blocked task**：等待其他 task、依赖、冲突处理或人类决策的任务。

复杂任务可以通过扩展 task graph 作为合法交付。当前 task 不因为新增相关 task 而完成，而是通过 blocker / edge 进入 blocked 状态，等相关 task 完成后再重新验收自身目标。

详见：[Task Manager Agent 详细设计](./task-manager-agent.md)、[Task Graph 详细设计](./task-graph.md)。

---

## 5.4 Agent Runtime

Agent Runtime 位于 Go backend，是所有 agent 的统一运行入口。它将 Claude Code、Codex、Gemini CLI 等不同 CLI agent 包装成统一 worker，也用同一套 invocation / permission / event / artifact 机制运行 Task Manager Agent 和 Ctx Manager Agent。第一阶段只实现 Claude Code 的基本包装。

统一不是指能力完全相同，而是每个 agent 暴露 capability profile，并把 CLI 自身能力包装给上层：

```text
- 是否支持 headless
- 是否支持 structured output
- 是否能编辑文件
- 是否能运行 shell
- 是否支持 MCP
- 是否支持或可包装 worktree / git / cwd 隔离能力
- 上下文窗口和成本模型
- 适合承担 planner/executor/verifier 中哪些角色
```

worktree 不作为独立于 agent 的抽象先行实现，而是先落在 Claude Code wrapper 的能力包装里。

详见：[Agent Runtime 详细设计](./agent-runtime.md)。

---

## 5.5 Ctx Agent / Context Lib

Ctx Manager Agent 是 runtime role 名称；Ctx Agent 是早期文档沿用的模块简称。

Ctx Manager Agent / Ctx Agent 是 ctxlib 的唯一受控访问入口，同时它自己也是经 Agent Runtime 启动和记录的系统 agent。它以 Event Log 为唯一数据来源构建 ctxlib（读 log、策展、去重、supersede、标注），对外只提供受控的 context pack 构建和查询。其他 agent 不直接读写 ctxlib，也不向 ctxlib 推送内容——它们的活动被自动记入 log，再由 Ctx Agent 从 log 中提炼。

Context Lib 是项目级上下文库，用来替代 session memory。它存储经过提取、标注和验证的项目记忆，例如：

```text
- 架构决策
- 模块摘要
- 任务摘要
- 验收结果
- 失败原因
- 冲突分析
- 用户偏好
- rejected approaches
```

新 agent 启动时不会加载全量 ctxlib，而是由 Ctx Agent 根据 task、phase、scope、confidence、freshness 和 risk 选择有限 context pack。运行中的其他 agent 也可以通过 Ctx Agent 查询 ctxlib，但不能直接读取 ctxlib 底层存储。

详见：[Context Lib 详细设计](./ctxlib.md)。

---

## 5.6 Workspace / Git / Merge Queue

Workspace / Git 不再被视为独立于 agent runtime 的第一阶段核心模块。第一阶段先把 worktree、git、cwd 和 tool 权限作为 Claude Code wrapper 的一部分包装。

Merge Queue 仍然负责 verify passed 结果的合并与冲突协调。

基本原则：

```text
- 每个 task attempt 使用 agent runtime 包装出来的 worktree/cwd 隔离能力。
- agent 不直接修改 main。
- verify 通过后才进入 merge queue。
- merge 前检查 active conflicts。
- merge 结果记入 Event Log，成为新的项目事实；ctxlib 由 Ctx Agent 从 log 提炼。
```

详见：[Workspace 与 Merge Queue 详细设计](./workspace-merge.md)。

---

## 5.7 Scheduler / Budget

Scheduler 根据 task graph、agent capacity、预算、风险和依赖关系决定下一步启动什么。

用户点击 `agent +1` 时，只是增加 worker capacity；Scheduler 决定新增 worker 去执行哪个 task phase。

预算不仅是金钱，还包括：

```text
- token
- 时间
- 并发数
- retry 次数
- verify 强度
- shell 执行成本
```

详见：[Scheduler 与 Budget 详细设计](./scheduler-budget.md)。

---

## 5.8 Event Log / Artifact Store

Event Log 是系统事实来源。task 表、ctxlib 索引、UI 状态都可以视为 event log 的 projection。

Artifact Store 保存大对象：

```text
- agent transcript
- tool output
- test output
- diff patch
- screenshots
- benchmark result
```

详见：[Event Log 与 Artifact Store 详细设计](./event-artifact-store.md)。

---

## 6. 模块间关系

## 6.1 提交新需求

```text
Human UI 或其他 agent
  -> Agent Runtime(role=task_manager)
  -> Task Manager Agent（查看全局 task，去重、编排依赖）
  -> Task Graph 写入 requirement / task / phase endpoint / decision endpoint / edge
  -> Ctx Agent 选择初始 context pack
  -> Scheduler 决定何时向 Agent Runtime 提交 planner AgentRunParams
```

关键判断：提交需求只是把 requirement 放入系统，并由 Task Manager Agent 决定是否创建 / 更新 task graph；这不等于立即开一个新 session。

---

## 6.2 增加 agent 数量

```text
Human UI: agent +1
  -> Control Plane 增加 worker capacity
  -> Scheduler 选择下一个可运行 task phase
  -> Agent Runtime 启动对应 CLI agent（第一阶段为 Claude Code）
```

关键判断：增加 agent 是增加系统吞吐，不是让用户手动指定“这个新 agent 去做什么”。

---

## 6.3 执行一个 task

```text
Task Graph 提供 task contract
  -> Agent Runtime(role=ctx_manager) 启动 Ctx Manager Agent 生成 context pack
  -> Agent Runtime 在包装出的 worktree 隔离环境启动 plan / execute / verify agent
  -> 运行中 agent 需要更多上下文时 -> 通过 Agent Runtime(role=ctx_manager) 受控查询 ctxlib
  -> Event Log 记录过程
  -> Verify 通过后进入 Merge Queue
```

---

## 6.4 复杂任务拆解

```text
Planner / Executor / Verifier 发现当前 task 需要拆解、补工作或补验收
  -> 通过 Agent Runtime(role=task_manager) 向 Task Manager Agent 提交新的 requirement（严格模式，带 client_ref 和触发证据）
  -> Task Manager Agent 校验 requirement，并编排 task / phase endpoint / decision endpoint / edge / blocker
  -> 当前 task 或当前 phase endpoint 进入 blocked（如果需要等待新增 task、特定状态或决策）
  -> Scheduler 调度新增 task 或等待依赖 endpoint 满足
  -> 相关 endpoint 满足后当前 task 回到 planning / executing / verifying
```

关键判断：扩展 task graph 是复杂任务的合法交付，不是失败；但 planner / executor / verifier 提交的是 requirement，不是 task / edge。依赖关系由 Task Manager Agent 统一编排，并且可以细到 phase endpoint，例如 `Task A.verify depends_on Task B.done`。

---

## 6.5 上下文沉淀与再利用

```text
Agent 输出 summary / verify failure / merge result
  -> Event Log 自动记录这些活动（agent 无需显式写日志）
  -> Agent Runtime(role=ctx_manager) 启动 Ctx Manager Agent（含 Context Curator）从 log 提取 context block
  -> Context Lib 标注、去重、supersede
  -> 后续 task phase 由 Ctx Agent 选择 context pack
```

关键判断：长期记忆属于 ctxlib，不属于某个 agent session；ctxlib 只从 log 构建，读写都经过 Ctx Agent。

---

## 6.6 并发冲突协调

```text
Task A verify passed
  -> Merge Queue 检查 active tasks
  -> 发现 Task B 有 write set 重叠
  -> 广播 conflict context 给 Task B
  -> Task B 单边 replan 或 adapt
```

关键判断：已经 verify passed 的 task 优先，仍在执行的 task 负责适配，避免双方互相等待。

---

## 7. 架构不变量

```text
1. 所有 agent invocation 都必须经 Agent Runtime，包括 Task Manager Agent、Ctx Manager Agent、planner、executor、verifier。
2. task 未通过 verify 不得 merge。
3. agent 不拥有长期记忆，ctxlib 拥有长期记忆。
4. agent 启动不加载全量 ctxlib。
5. 每个 task attempt 在 agent runtime 包装出的隔离环境执行。
6. execute 不直接修改 main。
7. verify agent 不自我批准 execute 结果。
8. 通过提交 requirement 扩展 task graph 是复杂任务的合法交付。
9. blocked 不是 failed。
10. merge 后必须产生可追溯事件和上下文沉淀。
11. 用户控制需求和资源，系统控制调度细节。
12. task graph 只能由经 Agent Runtime 授权的 Task Manager Agent 写入。
13. planner / executor / verifier 不直接创建 task / edge；依赖关系由 Task Manager Agent 编排。
14. task 使用固定的 phase endpoint 表达生命周期，依赖可以指向具体的 phase endpoint 或 decision endpoint。
15. ctxlib 只能由经 Agent Runtime 授权的 Ctx Manager Agent / Ctx Agent 读写。
```

---

## 8. MVP 分期总览

```text
MVP 0：Claude Code wrapper + Task Graph + CtxLib（第一步）
  - 基本包装 Claude Code CLI（headless 启动、输入输出、事件记录、能力声明）。
  - Task Manager Agent：经 Agent Runtime 启动，能看到全部 task 及状态，作为 task 创建的唯一入口。
  - Task Graph 调度：task 状态机、依赖、阻塞、基本推进。
  - Ctx Manager Agent + CtxLib：经 Agent Runtime 启动，提供 context block 的基本存取和受控查询。
  worktree/tool/git 先作为 Claude Code wrapper 的能力包装，不单独抽象。

MVP 1：Context Pack
  用结构化 context block 替代人工 session handoff，由 Ctx Agent 生成 pack。

MVP 2：运行时 ctxlib 检索
  允许运行中的其他 agent 通过 Ctx Agent 查询 ctxlib，并在必要时触发 replan。

MVP 3：多 CLI Agent + Worker Pool
  在 Claude Code 之外接入更多 CLI agent，实现 agent+1。

MVP 4：Conflict-Aware Merge Queue
  支持多 agent 安全并发和冲突广播。

MVP 5：Context Curator
  自动沉淀项目记忆、标注上下文、识别 supersede 和失败经验。
```

---

## 9. 详细设计文档

- [Task Manager Agent 详细设计](./task-manager-agent.md)
- [Task Graph 详细设计](./task-graph.md)
- [Agent Runtime 详细设计](./agent-runtime.md)
- [Context Lib 详细设计](./ctxlib.md)
- [Workspace 与 Merge Queue 详细设计](./workspace-merge.md)
- [Scheduler 与 Budget 详细设计](./scheduler-budget.md)
- [Event Log 与 Artifact Store 详细设计](./event-artifact-store.md)
