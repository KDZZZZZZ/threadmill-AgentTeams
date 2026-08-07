# Threadmill 设计基线：工作不属于 agent session

状态：Draft

这份文档说明 Threadmill 为什么需要现在这些模块，以及这些模块之间必须保持什么边界。它不列接口，也不描述 UI。模块文档可以继续演进，但不应悄悄改变这里定义的工作模型。

## 1. Threadmill 管的是未完成的工作

设想一个普通改动：给 Webhook 增加幂等校验。负责规划的 agent 读代码后发现，真正的前置条件是先迁移一份旧配置；迁移尚未完成时，另一个 agent 又修改了配置 schema。此时第一个 agent 的 session 是否还活着并不重要，重要的是下面这些问题仍有明确答案：

- 原始需求是什么，哪些约束不能被后续转述改掉；
- 幂等改动是否必须等待配置迁移；
- 迁移完成到什么程度后，幂等改动才可以继续；
- 当前补丁基于哪个代码版本，实际改动是否越过计划范围；
- 哪一份验证结果仍然有效，谁有资格把结果合入主分支。

如果这些答案只存在于某个 agent 的对话里，换模型、重试、并发或进程退出都会改变项目状态。Threadmill 因此把 agent 降为执行资源，把工作及其约束放进独立于 session 的对象中。

工作沿着下面这条链路逐步收敛：

```text
Requirement -> Task Contract -> Task -> Task Attempt -> Agent Invocation
```

它们不是同一个对象的不同名字。

- Requirement 保存“最初为什么要做”。
- Task Contract 固定“要交付什么、边界在哪里、怎样算完成”。
- Task 是需要持续协调的工作身份。
- Task Attempt 是对同一契约的一次尝试。
- Agent Invocation 是某个阶段临时借用的一次计算能力。

最后一个对象可以随时重建，前四个不能随 session 一起消失。

## 2. 为什么 agent 是临时 thread

Threadmill 不为 agent 维护长期人格，也不把一个 task 永久绑定给某个 session。一次 invocation 只拿到完成当前职责所需的输入：Task Contract、当前 attempt、允许的工具、工作区、上下文快照和输出契约。

这样做解决四个实际问题。

第一，失败可以重试。一个 executor 崩溃后，新的 executor 从显式状态恢复，不需要猜测旧对话里发生了什么。

第二，角色可以替换。planner、executor 和 verifier 可以由不同 provider 承担，调度决策不依赖某个 CLI 的 session 语义。

第三，并发不会制造多个项目真相。agent 可以各自在局部上下文中工作，但任务依赖、验收结论和已合入事实只有一份权威记录。

第四，权限可以按 invocation 收口。规划可以只读，执行只能写指定工作区，验证默认不能修改实现。长期 session 很容易积累超出当前任务所需的权限和上下文。

“临时”并不等于每说一句话就新建一次调用。只要输入基线、角色、权限和目标没有改变，一个 invocation 可以持续完成它的局部工作。发生重规划、权限变化、上下文基线变化或角色切换时，才应结束旧 invocation，建立新的边界。

## 3. graph 表达因果关系，不表达热闹程度

Threadmill 需要区分两种 graph。当前文档中如果把它们都叫 task graph，容易把持久的工作关系和临时的运行步骤混在一起。

### 3.1 Coordination Graph

Coordination Graph 是持久对象。节点是 task 的 phase endpoint，边表达“目标 endpoint 为什么现在不能继续”以及“继续时需要消费什么结果”。它回答的是跨时间、跨 agent 仍然成立的问题。

例如，配置迁移 Task B 的验证结果是幂等改动 Task A 开始实现的前提：

```text
B.verify --passed + migration evidence--> A.execute
```

如果 A 可以先实现，只在最终验收时需要 B，则边应挂到更晚的位置：

```text
B.verify --passed + migration evidence--> A.verify
```

Manager 应把边连到最早真正需要该结果的 endpoint。连得过早会无端串行化；连得过晚会让 agent 在错误前提上工作。

### 3.2 Execution Graph

Execution Graph 是临时对象。Scheduler 为一次 phase 或 attempt 物化它，节点可以是 LLM invocation、确定性 tool，或者另一个 execution subgraph。它回答的是“这一次怎样运行”。

例如 A.plan 可以包含：

```text
读取仓库约束(tool)
  -> 分析影响面(planner invocation)
  -> 校验计划结构(tool)
  -> 提交新 requirement(task-manager invocation)
```

这些步骤不必全部成为持久 task。只有某一步需要独立验收、独立重试、跨时间等待、单独授权或被其他 task 依赖时，Manager 才把它提升为新的 Task Contract，并把关系写回 Coordination Graph。

这就是“plan 内部还能出现 plan + execute”的准确含义：phase 的运行体可以递归使用 subgraph；当内部工作获得独立生命周期时，它升级为 task，并拥有自己的 `prepare -> plan -> execute -> verify -> done`。递归发生在执行结构上，不需要发明 root task、child task 两套节点类型。

## 4. plan、execute、verify 分开的理由

三个阶段不是为了让三个 agent 轮流复述同一段话，而是为了分开三种不能由同一份输出同时证明的东西。

### Plan 固定承诺

Plan 在发生修改之前说明将影响哪些模块、依赖什么事实、需要哪些权限以及用什么证据验收。它可以请求 Manager 扩展 graph，但不能因为自己的实现偏好改写 Task Contract。

Plan 的价值是让后续能比较“原来承诺的影响”与“实际发生的影响”。没有这一步，所谓越界只能靠 executor 自己解释。

### Execute 产生候选结果

Execute 在隔离工作区中实施已批准的范围，并留下可观察结果。其输出是候选 diff、测试输出、工具记录和新发现，而不是“已经完成”的结论。

执行中发现契约缺口时，executor 可以提出 requirement 或请求 replan；它不能静默扩大范围，也不能直接改 graph。

### Verify 作出可接受性判断

Verify 同时读取 Task Contract、批准的 plan、真实 diff、测试证据和输入 revision。它判断的是候选结果是否满足契约，而不是 executor 是否看起来努力过。

验证失败通常产生同一个 task 的新 attempt。只有验收暴露出一个具有独立生命周期的新工作时，才创建新 task。把每次失败都建成新 task 会丢失“这些 attempt 在追求同一个契约”的事实。

`done` 不是执行阶段。它只是 Coordination Graph 在验收、依赖和合入条件都满足后得出的结论。

## 5. Manager 什么时候应该拆 task

拆分不是越细越好。每增加一个 task，系统就多出一份契约、一次上下文选择、一组状态、至少一次验收以及潜在的合并协调。

一段工作满足下面任一条件时，才值得获得独立 task 身份：

- 它有可以单独判断的完成条件；
- 它可以与当前工作独立失败或重试；
- 它要等待外部输入或人工决定；
- 它需要不同权限、工作区或责任人；
- 其他 task 需要直接依赖它的中间结果；
- 它的生命周期会超过当前 phase invocation。

反过来，读取文件、调用一次工具、生成局部摘要、执行同一计划中的连续命令，通常只应留在 Execution Graph。把这些动作都持久化成 task，只会把执行日志误当成项目计划。

## 6. ctxlib 为什么存在

临时 invocation 带来一个直接问题：新的 agent 从哪里知道已经确认的项目事实？把历史 transcript 全部塞回 prompt 既昂贵，也会把猜测、旧结论和失败尝试混在一起。

Threadmill 把三类信息分开：

- Event Log 保存发生过什么；
- Artifact Store 保存 diff、测试输出和 transcript 等大对象；
- ctxlib 保存从事件和 artifact 中提炼、可追溯、可被后续工作复用的 Context Block。

ctxlib 不是聊天记录搜索，也不是自动正确的知识库。Context Block 仍可能过时或相互矛盾。Context Pack 必须针对一次 invocation 的 Task Contract、phase endpoint、代码 revision、权限范围和预算生成；被省略的相关信息要可见，影响验收的矛盾不能被摘要掉。

因此，ctxlib 的根本作用不是“让 agent 记得更多”，而是让一个没有旧 session 的 agent 也能获得足够且有出处的项目状态，同时知道哪些内容不应被当成事实。

## 7. 各模块各自拥有一个决定

模块边界可以用它们有权作出的决定来检查：

| 模块 | 它拥有的决定 | 它不能替谁决定 |
| --- | --- | --- |
| Task Manager Agent | requirement 是否形成 task，以及 endpoint 之间如何建立关系 | 不替 planner 选择实现方案 |
| Scheduler | 当前哪些 endpoint 值得占用 worker capacity | 不改 Task Contract 和 graph 语义 |
| Agent Runtime | 一次 invocation 如何在明确权限、工作区和预算内运行 | 不判断 task 是否完成 |
| Ctx Manager Agent | 本次 invocation 应获得哪些可追溯上下文 | 不把摘要直接宣布为项目事实 |
| Executor | 如何在批准范围内产生候选结果 | 不批准自己的结果，不写主分支 |
| Verifier | 候选结果是否满足契约以及证据是否仍然新鲜 | 不修改实现来让验证通过 |
| Merge Queue | 已获得资格的候选是否能机械地进入最新主分支 | 不修冲突，不重写 graph |

如果一个实现让某个模块替别的模块作出决定，通常不是“方便的捷径”，而是权威来源开始分叉。

## 8. 设计检查

后续设计至少应能回答以下问题：

1. 杀掉所有 agent 进程后，哪些对象足以恢复未完成工作？
2. 某个结论来自任务契约、agent 推断，还是已经验证的证据？
3. 一条 graph edge 在阻止哪个 endpoint，携带什么数据，解除条件是什么？
4. 失败是在重试同一 Task Contract，还是暴露了新的独立工作？
5. Context Pack 绑定了哪个输入 revision，过期后谁触发重选或重验？
6. 最终写入主分支的决定能否追溯到原始 requirement、真实 diff 和仍然有效的验证结果？

答不清这些问题时，不应该继续增加状态、agent 角色或 memory 策略；应先修正工作模型。
