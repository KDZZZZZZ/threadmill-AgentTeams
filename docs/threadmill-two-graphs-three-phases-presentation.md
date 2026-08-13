# Threadmill 两张图与三阶段 Agent：PPT 介绍底稿

状态：Draft

用途：内部技术介绍、架构评审或产品技术沟通

建议时长：20 分钟

建议页数：14 页

## 1. 文档目标

这份文档用于解释 Threadmill 的三个核心机制：

- Coordination Graph（协调图）记录任务编排与运行约束；
- Context Graph（共享上下文图）保存可复用、可追溯的项目知识；
- 3-Phase Agent 将每个 Task 固定为 `plan -> execute -> verify` 三个受控阶段。

目标听众是研发、架构师、产品技术负责人，以及需要理解多 Agent 协作边界的项目成员。听众不需要提前掌握 Threadmill 的接口细节，但应理解 Task、Agent、依赖和验收等基本概念。

整场介绍需要传达一个核心判断：

> 多 Agent 系统的关键不是让更多 Agent 同时工作，而是把编排事实、共享知识和阶段执行放进不同的权威边界。

## 2. 一句话模型

Threadmill 可以概括为“两张持久图、三个执行阶段、一个闭环”：

```text
Coordination Graph 决定：做什么、何时做、被什么阻塞
Context Graph 决定：知道什么、知识如何关联、何时收到更新
3-Phase Agent 负责：计划、执行、独立验证
```

两张图保存持久状态。Agent Invocation 是临时计算资源，可以替换、停止和恢复；Task 不属于某个 Agent Session。

## 3. 三个机制解决三类问题

传统 Agent Team 往往把三类信息混在聊天或单个 Agent 的会话里：

1. 谁依赖谁、谁可以开始、谁被阻塞；
2. 项目已经确认了哪些事实，哪些结论已经过时；
3. 当前 Agent 做到了哪一步，产生了哪些中间结果。

混在一起后，会出现三个直接问题：

- 协调义务散落在消息里，无法稳定查询、恢复或审计；
- Agent 各自保存私有记忆，项目出现多份互不一致的“事实”；
- 产生结果的 Agent 同时负责验收，缺少独立验证边界。

Threadmill 按生命周期拆分这些信息：

| 机制 | 保存什么 | 主要消费者 | 不保存什么 |
| --- | --- | --- | --- |
| Coordination Graph | Task、Phase Endpoint、依赖、Blocker、结果引用 | Task Manager、Scheduler、Runtime | Agent 的中间推理和工具轨迹 |
| Context Graph | 有来源的 directive、fact、hypothesis，以及它们的子图关系 | 所有 Agent | 当前运行状态和未审查候选 |
| Agent Runtime / Workspace | 当前 Invocation 的执行现场 | 当前 Phase Agent | 长期编排事实和共享知识 |

## 4. 总体架构：两条链在 Phase Agent 交汇

系统包含两条主链路。

### 4.1 协调链：决定“做什么”

```text
Requirement
  -> Task Manager 形成 Task Contract
  -> 写入 Coordination Graph
  -> Scheduler 选择 runnable endpoint
  -> Runtime 创建受控 Invocation
  -> Phase Agent 提交 PhaseOutput 或 OrchestrationProposal
  -> Task Manager 裁决并更新图
```

这条链表达任务、依赖、阻塞、阶段契约和完成条件。Task Manager 是 Coordination Graph 的唯一写入口。Phase Agent 只能提交编排建议，不能直接拆 Task、修改依赖或宣布 Task 完成。

### 4.2 上下文闭环：决定“知道什么”

```text
Context Slice + 当前 Task 候选缓冲
  -> Phase Agent 读取、探索或订阅上下文
  -> 提交 MemoryCandidate
  -> 候选进入当前 Task 的共享缓冲
  -> Task 权威 done 后冻结候选
  -> Context Agent 批量审查
  -> Context Service 原子落图
  -> 已订阅 Invocation 收到 ContextDelta
```

这条链负责知识复用。它不承担任务依赖：知识更新通过 `ContextDelta` 传播，因果等待通过 Coordination Edge 表达，两者不能混用。

### 4.3 交汇点

一次 Phase Invocation 同时受两类信息约束：

- 协调图告诉它当前做什么、正式输入来自哪里、何时可以提交；
- 上下文图告诉它已有事实、历史决策和可继续探索的知识边界。

Phase Agent 在执行中把结果反馈给协调链，把可复用发现反馈给上下文闭环。

## 5. Coordination Graph：编排的事实来源

Coordination Graph 保存尚未完成的编排义务。它不是 Agent 的执行步骤清单，也不是聊天记录。

### 5.1 核心对象

- `Task`：持久工作身份，由 Task Contract 约束；
- `PhaseEndpoint`：固定为 `plan`、`execute`、`verify`；
- `Edge`：描述上游 endpoint 对下游 endpoint 的正式交付义务；
- `Blocker`：表示人工审批或外部条件等无法自然建模为 PhaseOutput 的门控；
- `PhaseResult`：保存阶段正式输出的引用与判定；
- `Generation + BindingRef`：把契约、输入、Workspace、上下文和结果绑定到同一版本。

### 5.2 控制由支撑层落地

Coordination Graph 本身不主动执行。控制过程由支撑层完成：

```text
Coordination Graph
  -> GraphRuntime 计算 runnable endpoint
  -> Scheduler 按容量和优先级选择
  -> Runtime 装配 BindingRef、输入、Workspace 与 Context
  -> 启动 Phase Agent Invocation
```

图中的 `requiredBy: start` 决定启动前必须到达的输入；`requiredBy: completion` 允许上下游并行，但下游在正式提交前必须等到交付。

### 5.3 受控热修改处理执行期变化

真实执行会暴露规划阶段看不到的信息：缺少前置、任务需要拆分、并行应改为串行、验证失败需要重开轮次。Phase Agent 此时提交 `OrchestrationProposal`，包含建议、理由、版本与证据引用。

Task Manager 根据当前图和证据决定接受、改写或拒绝。接受后更新 Coordination Graph，Scheduler 再计算 runnable endpoint。热修改改变的是“当前编排”，变更过程仍然可审计。

## 6. Shared Context Graph：共享知识的事实来源

本文中的“共享上下文图”正式名称是 Context Graph。它保存从执行证据中提炼出的知识，不保存完整 transcript，也不保存 Agent 的失败尝试和临时工具输出。

### 6.1 节点与子图

`ContextNode` 的知识类型只有三种：

- `directive`：必须、应当或期望怎样做；
- `fact`：已经成立或经过相应验收的事实；
- `hypothesis`：仍需证据验证的推测。

每个节点带有来源引用、创建者和状态。一个节点可以属于多个子图。子图分为：

- `general`：跨 Task 复用的知识，由 Context Agent 经 Context Service 管理；
- `task`：面向稳定 Task/Endpoint 的定向投影，只能由 Task Manager 通过受控接口写入。

### 6.2 Task Manager 与 Phase Agent 使用同一读面

Task Manager、planner、executor 和 verifier 使用同一套 Context 读接口：列表、探索、订阅和取消订阅。这样可以避免 Manager 与执行者依据不同的记忆做判断。Context Agent 可以列表和探索，并独占面向机械 Search 的受控调用路径；它不消费 Phase Invocation 的订阅生命周期。

普通 Agent 不直接调用机械 Search。列表和探索仍无法定位信息时，它通过 `contextAgent.retrieve` 提交自然语言请求；Context Agent 将请求转成明确的关键词、范围和锚点，再由 Context Service 检索。

### 6.3 订阅与增量

Agent 可以订阅可见子图。节点或边事务成功提交并递增 revision 后，Context Graph 的订阅执行器生成 `ContextDelta`，Runtime 将它送达仍然有效的 Invocation。

Delta 只来自有效订阅，不存在订阅之外的旁路推送。Agent 收到新知识后可以调整当前判断；如果新知识证明编排已经失效，它仍需提交 `OrchestrationProposal`，不能直接修改协调图。

## 7. 两块记忆：Context Slice 与 Task Memory Buffer

“共享上下文”需要区分两块生命周期不同的记忆。

| 记忆区 | 内容 | 可见范围 | 何时更新 | 是否触发 Delta |
| --- | --- | --- | --- | --- |
| Context Slice | 已落图、经过准入的上下文切片 | 当前 Invocation 按权限和订阅可见 | 图事务提交后更新 | 是 |
| Task Memory Buffer | 当前 Task 尚未终审的候选 | 同一 Task 的 plan/execute/verify 可读 | Agent 提交候选后追加 | 否 |

Task Memory Buffer 是 append-only 工作记忆，不是 Context Graph 的一部分。它不能被 Search、Explore 或 Subscribe 访问，也不改变图 revision。

只有 Task 达到权威 `done` 后，Task Manager 才冻结缓冲。Context Agent 随后批量判断候选应当创建、修订、替代、争议还是拒绝；Context Service 将通过审查的知识原子写入 Context Graph。

这个边界同时解决两个问题：后续阶段可以立即读到同一 Task 的发现，未经审查的内容又不会提前污染跨 Task 共享知识。

## 8. 3-Phase Agent：固定阶段，不固定 Agent

“3-Phase Agent”不表示一个常驻 Agent 同时拥有三种权限。准确含义是：每个 Task 固定经历 `plan -> execute -> verify` 三个阶段，每个阶段由一次受控 Agent Invocation 执行，也可以由不同模型、不同 Provider 或不同临时 Agent 承担。

| 阶段 | 核心问题 | 主要输入 | 主要产物 | 关键限制 |
| --- | --- | --- | --- | --- |
| plan | 准备怎样完成 Task | Task Contract、Context、正式上游输入 | Submitted Plan、Declared Write Set、验证计划 | 默认不修改实现 |
| execute | 按批准范围交付什么 | Approved Plan、AllowedDirs、Workspace、Context | 实现、交付物、证据、MemoryCandidate | 不静默扩 scope，不写 main |
| verify | 交付是否满足契约 | 候选产物、Task Contract、验证计划、证据 | Verify Result、测试证据、风险与缺口 | 不修改实现让自己通过，不自我批准 |

`prepared`、`done`、人工 decision 和外部 blocker 都是门控或派生状态，不是第四阶段。

### 8.1 同一接口覆盖三个阶段

Runtime 为每次 Invocation 注入不可变执行绑定：

- 当前 `Endpoint + Generation + BindingRef`；
- 正式输入投影 `PhaseInputSet`；
- 受控 Workspace；
- `ContextSliceRef`；
- `TaskMemoryBufferRef`；
- 当前权限、预算、工具和路径范围。

Phase Agent 不需要读取整张 Coordination Graph，也看不到上游 Agent 的过程现场。它只消费正式输入和已注册 artifact 引用。

### 8.2 三类结构化出站结果

Phase Agent 主要通过三类结果与系统交互：

1. `PhaseOutput`：阶段交付、报告和证据；
2. `OrchestrationProposal`：拆分、补依赖、重排、重试或重新规划建议；
3. `MemoryCandidate`：可能跨阶段或跨 Task 复用的知识候选。

接受 `PhaseOutput` 只表示提交成功，不表示 endpoint 已满足，更不表示 Task 已完成。最终判定由授权方和 DeliveryPolicy 完成。

## 9. 端到端示例：为 API 增加批量处理能力

下面的例子用于 PPT 演示机制关系，不声明实际性能数据。

1. 用户提出“为现有 API 增加批量处理能力”。Task Manager 将它规整为 Task Contract，并创建固定的 plan、execute、verify endpoint。
2. plan 阶段读取相关 API 规范、历史决策和当前 Task 候选缓冲，产出批准计划、修改范围和验证方案。
3. planner 发现批量接口依赖尚未统一的幂等策略，提交 `OrchestrationProposal`。Task Manager 决定新增前置 Task，并热修改 Coordination Graph。
4. 前置 Task 满足后，execute 阶段获得正式 PhaseOutput 引用，在批准目录内实现接口，同时把“批量请求必须复用单请求幂等键规则”提交为 MemoryCandidate。
5. verify 阶段在独立权限边界内运行测试，检查交付、错误处理和证据。验证失败时提交重试或重排建议，不直接修改实现。
6. DeliveryPolicy 得出 Task 权威 `done` 后，候选缓冲被冻结。Context Agent 审查候选，Context Service 将通过的知识写入 general 子图。
7. 订阅该 API 子图的后续 Invocation 收到 ContextDelta。下一项相关工作可以复用该规则，无需读取历史聊天。

这个过程说明三者的分工：协调图保存责任，上下文图沉淀知识，三阶段 Agent 完成交付与独立验证。

## 10. PPT 逐页大纲

### 第 1 页：标题页

标题：**Threadmill：两张图与三阶段 Agent**

副标题：让多 Agent 协作可编排、可共享、可验收

画面：Coordination Graph、Context Graph、3-Phase Agent 三个元素汇入一个闭环。

### 第 2 页：多 Agent 并行不等于可控

核心信息：当协调义务、共享知识和执行现场混在消息里，系统更容易出现状态分叉。

画面：左侧为聊天、私有记忆和自我验收；右侧为两张图与三阶段边界。

### 第 3 页：Threadmill 的总体模型

核心信息：五个核心节点承担编排与记忆语义，Runtime、Scheduler 和 Workspace 负责落地。

画面：Task Manager、Coordination Graph、Phase Agent、Context Agent、Context Graph 五节点图。

### 第 4 页：两条链在 Phase Agent 交汇

核心信息：协调链决定“做什么”，上下文闭环决定“知道什么”。

画面：蓝色协调链与绿色上下文链在 Phase Agent 汇合。

### 第 5 页：Coordination Graph 是编排事实来源

核心信息：Task、Phase Endpoint、Edge、Blocker 和 Result 共同决定 runnable 状态。

画面：一组 Task 的 plan、execute、verify 节点及跨 Task 依赖。

### 第 6 页：协调图支持受控热修改

核心信息：Agent 提建议，Task Manager 做全局裁决，Scheduler 重新计算；热修改不等于无审计。

画面：发现新依赖 → Proposal → Manager 裁决 → 图 revision 更新。

### 第 7 页：Context Graph 保存知识，不保存聊天

核心信息：节点必须有类型、状态、来源和子图归属；Task Manager 与三类 Phase Agent 使用同一读面。

画面：general/task 子图、ContextNode 和来源引用。

### 第 8 页：共享上下文包含两块记忆

核心信息：Context Slice 是已落图知识；Task Memory Buffer 是同一 Task 三阶段共享的未终审候选。

画面：两块记忆区并列，对比 revision、可见范围和 Delta 行为。

### 第 9 页：3-Phase 是三个受控阶段

核心信息：plan、execute、verify 可以由不同 Invocation 执行，权限和产物各不相同。

画面：三段横向流程，分别标注输入、权限和产物。

### 第 10 页：Runtime 装配一次 Invocation

核心信息：Agent 只接收当前阶段的不可变绑定，不读取整张图，也不读取其他 Agent 的过程现场。

画面：Graph + Context + Workspace → BindingRef → Phase Agent。

### 第 11 页：Agent 只提交结构化结果

核心信息：`PhaseOutput` 回报交付，`OrchestrationProposal` 回报编排意图，`MemoryCandidate` 回报知识候选。

画面：Phase Agent 向三个受控出口分流。

### 第 12 页：一个 Task 的完整闭环

核心信息：用“API 批量处理能力”案例串起建图、计划、依赖发现、执行、验证、候选审查和 Delta。

画面：端到端时间线；避免再画一张完整架构图。

### 第 13 页：五条关键边界

核心信息：

1. Task Manager 是 Coordination Graph 唯一写入口；
2. Phase Agent 不直接写任一图；
3. 不存在 Agent mailbox；
4. Agent 是临时计算资源，Task 身份独立；
5. `verify passed` 与 `Task done` 是不同判定。

画面：用“允许 / 禁止”关系图突出边界。

### 第 14 页：收束

标题：**可靠的 Multi-Agent 系统不是群聊，而是一个可恢复的控制闭环**

核心信息：

- Coordination Graph 管责任；
- Context Graph 管知识；
- 3-Phase Agent 管交付与独立验证。

画面：回到第一页的三元素闭环，并增加“可替换、可恢复、可审计、可复用”四个结果词。

## 11. 视觉与讲解规范

### 11.1 视觉语义

- 蓝色：协调、依赖与控制；
- 绿色：上下文、知识与订阅；
- 橙色：Blocker、Proposal、失效和重试；
- 实线箭头：正式控制或交付；
- 虚线箭头：Context 读取、订阅或 Delta；
- 圆角矩形：Agent 或服务；
- 圆柱或图形容器：持久图；
- 小标签：revision、BindingRef、artifact reference。

### 11.2 讲解用词

推荐使用：

- “协调图记录编排事实”；
- “上下文图保存经过准入的共享知识”；
- “Agent 是一次受控 Invocation”；
- “Phase Agent 提交建议，Task Manager 裁决”；
- “候选缓冲是 Task 工作记忆，不是 Context Graph”。

避免使用：

- “协调图主动启动 Agent”——实际由 GraphRuntime、Scheduler 和 Runtime 落地；
- “Context Agent 主动推送知识”——实际由订阅执行器生成 Delta；
- “三个 Agent 依次交接”——阶段可以由不同 Agent 承担，但交接依靠正式输入和共享 Workspace/Context，不依赖 Agent 直接通信；
- “verify 完成就是 Task done”——最终完成还受 DeliveryPolicy、合并和其他条件约束；
- “MemoryCandidate 已经写入知识图”——候选先进入 Task 缓冲，Task done 后才审查落图。

## 12. 建议开场与收尾

开场：

> 多 Agent 的难点不是让更多 Agent 同时工作，而是让它们共享同一套责任边界、事实来源和验收标准。

收尾：

> Coordination Graph 让系统知道谁应该做什么，Context Graph 让 Agent 知道哪些知识值得复用，3-Phase Agent 让每个 Task 从计划走到独立验证。

## 13. 参考文档

- [Threadmill 总体架构](./architecture.md)
- [统一设计](./threadmill-unified-design.md)
- [Coordination Graph Module](./coordination-graph.md)
- [Context Graph 节点创建与关系模型](./context-graph.md)
- [Phase Agent Interface](./phase-agent.md)
- [Task Manager Agent](./task-manager-agent.md)
- [设计理由](./design-rationale.md)
