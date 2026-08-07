# Context Lib 详细设计

版本：v0.2
状态：Draft
定位：ctxlib 用结构化项目记忆取代 session memory。本文件给出一个**小内核 + 明确扩展缝**的原型：核心模型和接口尽量小而稳定，richer 能力(多维筛选、排序、embedding、过时判断等)都作为可插拔扩展，不进内核。

---

## 1. 设计原则

```text
1. 小内核：ContextBlock 只保留少量稳定字段，其余用开放结构表达。
2. 单一来源：ctxlib 只从 Event Log / Artifact 构建，不接受 agent 直接写。
3. 单一入口：对外只有经 Agent Runtime 启动和授权的 Ctx Manager Agent / Ctx Agent，且只暴露 pack / query 两个只读操作。
4. 缝在接口上：抽取、评分、存储都是可替换接口，扩展不改内核。
5. 一切可追溯：每个 block 都指回它的来源事件 / artifact。
```

一句话：**内核只回答"有哪些记忆、怎么取"，"记忆有多好、怎么排"交给可替换的策略。**

临时 invocation 带来一个直接问题：新的 agent 从哪里知道已经确认的项目事实？把历史 transcript 全部塞回 prompt 既昂贵，也会把猜测、旧结论和失败尝试混在一起。

Threadmill 把三类信息分开：

```text
Event Log 保存发生过什么；
Artifact Store 保存 diff、测试输出和 transcript 等大对象；
ctxlib 保存从 event 和 artifact 中提炼、可追溯、可被后续工作复用的 Context Block。
```

ctxlib 不是聊天记录搜索，也不是自动正确的知识库。Context Block 仍可能过时或相互矛盾；Context Pack 必须绑定一次 invocation 的 Task Contract、phase endpoint、input revision、权限范围和预算。被省略的相关信息要可见，影响验收的矛盾不能被摘要掉。

ctxlib 的作用不是让 agent 记得更多，而是让没有旧 session 的 agent 也能获得足够且有出处的项目状态，并知道哪些内容不应直接当成 Project Fact。

---

## 2. 核心数据模型

只有一个核心类型。可扩展性来自两处：`scope` 用**前缀约定的开放标签**，`attributes` 用**开放键值**。新增维度不改 schema。

```go
type ContextBlock struct {
	// ID 是上下文块稳定标识。
	ID string `json:"id"`
	// Kind 是开放字符串；原型约定 decision / summary / failure / conflict / preference / rejected 等。
	Kind string `json:"kind"`
	// Text 是可直接注入 prompt 的内容，通常是摘要、决策、失败原因或日志引用。
	Text string `json:"text"`
	// Scope 是轻量标签，用前缀表达“这段上下文关于什么”。
	Scope []string `json:"scope"`
	// Outdated 表示该 block 是否已过时。
	Outdated bool `json:"outdated"`
	// SourceRefs 指向 Event Log / Artifact 证据，保证上下文可追溯。
	SourceRefs []string `json:"source_refs"`
	// Attributes 是扩展位，例如 confidence、freshness、importance、visibility、risk。
	Attributes map[string]any `json:"attributes"`
	CreatedAt time.Time `json:"created_at"`
}
```

为什么这么设计：

```text
- 原来的 repo/module/file/symbol/task/phase_scope 六个字段 -> 合并成 scope 标签。
- 原来的 confidence/importance/freshness/validity/visibility/risk 等 -> 放 attributes。
- 结果：内核字段从 ~20 个降到 7 个；加新维度不再是 schema 变更。
```

---

## 3. Ctx Agent：唯一入口

Ctx Manager Agent / Ctx Agent 是 ctxlib 的唯一受控访问入口，也是唯一写入者。它本身经 Agent Runtime 启动、授权、观测和记录；它以 **Event Log 为唯一数据来源**构建 ctxlib，对外只提供两个只读操作。其他 agent 不直接读写底层存储，也不推送内容——它们的活动被自动记入 log，由 Ctx Agent 从 log 提炼。

```go
type ContextService interface {
	// Pack 在 agent 启动前为某个 task/phase 组装 context pack。
	Pack(ctx context.Context, req ContextPackRequest) (ContextResult, error)
	// Query 在运行中为受控 agent 提供按意图检索的上下文查询。
	Query(ctx context.Context, req ContextQueryRequest) (ContextResult, error)
}

type ContextPackRequest struct {
	TaskID string `json:"task_id"`
	// TaskContractRef 防止同一 task 的旧契约上下文混入新 attempt。
	TaskContractRef string `json:"task_contract_ref"`
	AttemptID string `json:"attempt_id"`
	// PhaseEndpoint 是本次 pack 服务的精确编排点。
	PhaseEndpoint string `json:"phase_endpoint"`
	Role AgentRole `json:"role"`
	// InputRevision 是代码、graph 和关键外部输入的组合 revision。
	InputRevision string `json:"input_revision"`
	// PermissionScope 用于在相关性排序前先做可见性过滤。
	PermissionScope []string `json:"permission_scope,omitempty"`
	// Budget 是 token/字符预算，由 Ctx Agent 用于裁剪注入内容。
	Budget int `json:"budget"`
}

type ContextQueryRequest struct {
	TaskID string `json:"task_id"`
	TaskContractRef string `json:"task_contract_ref"`
	AttemptID string `json:"attempt_id"`
	PhaseEndpoint string `json:"phase_endpoint"`
	Role AgentRole `json:"role"`
	InputRevision string `json:"input_revision"`
	PermissionScope []string `json:"permission_scope,omitempty"`
	// Intent 缩小检索目的，例如 api、conflict、decision。
	Intent string `json:"intent,omitempty"`
	// Scope 限定范围标签。
	Scope []string `json:"scope,omitempty"`
	Budget int `json:"budget"`
}

type ContextResult struct {
	// Binding 回显 pack/query 所绑定的工作边界；消费者不能把结果挪给另一 revision 使用。
	Binding ContextBinding `json:"binding"`
	// Blocks 是已选中、可注入的上下文块。
	Blocks []ContextBlock `json:"blocks"`
	// Omitted 是相关但因预算未注入的 block id。
	Omitted []string `json:"omitted"`
	// Conflicts 保留相互矛盾或可能过时的候选；不得在摘要时静默择一。
	Conflicts []ContextConflict `json:"conflicts,omitempty"`
	// Note 是发现矛盾或输入过期时给调度层的建议，例如 replan 或 human_decision。
	Note string `json:"note,omitempty"`
}

type ContextBinding struct {
	TaskID string `json:"task_id"`
	TaskContractRef string `json:"task_contract_ref"`
	AttemptID string `json:"attempt_id"`
	PhaseEndpoint string `json:"phase_endpoint"`
	Role AgentRole `json:"role"`
	InputRevision string `json:"input_revision"`
}

type ContextConflict struct {
	BlockIDs []string `json:"block_ids"`
	Reason string `json:"reason"`
}
```

对外没有 write / outdated 标记操作。要沉淀记忆的 agent 只管正常做事，活动经 Agent Runtime 进入 log；Ctx Manager Agent 负责提炼；某条记忆是否过时也由 Ctx Agent 从 log 中判断。

---

## 4. 三个可替换接口（扩展缝）

内核只依赖这三个接口的**签名**，不依赖其实现。原型给最简实现，之后各自独立演进。

### 4.1 Extractor：log -> block（怎么产生记忆）

```go
type Extractor interface {
	// Extract 从 Event Log 事件中提炼可复用 ContextBlock。
	Extract(ctx context.Context, event Event) ([]ContextBlock, error)
}
```

- 原型：几条规则型 extractor（verify 失败、merge 结果、人类需求）。
- 扩展：新增来源 = 新增一个 extractor 注册进来，内核不变。

### 4.2 Selector：给定请求挑 block（怎么取记忆）

```go
type Selector interface {
	// Select 从候选上下文中按 task/phase/intent/budget 选择注入集合。
	Select(ctx context.Context, candidates []ContextBlock, req ContextQueryRequest) ([]RankedContextBlock, error)
}
```

- 原型：`scope` 标签重叠 + 新鲜度 + 过滤 `outdated=true`，够用。
- 扩展：换成多路召回 + 打分重排 + embedding，只替换 Selector，接口不变。

### 4.3 Store：底层存取（记忆存哪）

```go
type Store interface {
	// Put 写入或更新上下文块；实现负责幂等和 supersede 关系。
	Put(ctx context.Context, block ContextBlock) error
	// Get 按 id 读取上下文块。
	Get(ctx context.Context, id string) (ContextBlock, error)
	// Find 按 kind/scope/outdated 等粗筛，排序交给 Selector。
	Find(ctx context.Context, filter ContextFilter) ([]ContextBlock, error)
}
```

- 原型：内存或单文件 / SQLite。
- 扩展：换成向量库 / 外部服务，只替换 Store。

---

## 5. 数据流

```text
Agent Runtime 自动记录所有 agent 活动 / 状态变化（包括 Ctx Manager Agent 自身）
  -> Event Log
       -> Extractor 提炼出 ContextBlock（去重、必要时标记旧 block 为 outdated）
       -> Store 保存

Agent Runtime(role=ctx_manager) 调用 Ctx Agent.pack / query
  -> Store.find 粗筛候选
  -> Selector 排序 + 裁到 budget
  -> ContextResult（blocks + omitted + note）
```

写入路径(Extractor->Store)只发生在 Ctx Agent 内部,由 log 驱动;对外只有 pack/query。

---

### 5.1 信息流图视角：生产者 / 消费者关系

ctxlib 不把信息流建模成 agent 直接互发消息，而是从 Event Log / Artifact Store 投影出生产者和消费者关系：

```text
agent_attempt
  --emits--> event
  --produces--> artifact
  --extracts_to--> context_block
  --included_in--> context_pack
  --consumed_by--> agent_attempt
```

和 Task Graph / Scheduler 以及 Agent Runtime IO 的关系：

```text
Agent invocation 输出结果和 artifact refs
  -> Agent Runtime 记录 AgentEvent / ArtifactRef
  -> Agent Runtime(role=ctx_manager) 启动 Ctx Manager Agent 从 Event / Artifact 提炼 ContextBlock
  -> pack / query 选择 ContextBlock，形成 ContextResult
  -> 下一个 phase invocation 以 context_ref / text block 消费
```

因此，ctxlib 的长期事实仍然来自 Event / Artifact 的可追溯投影；具体 invocation 的输入输出格式由 Agent Runtime 负责。这样既能表达“谁生产、谁消费了哪段上下文”，又不允许 agent 绕过 Ctx Agent 直接写记忆。

---
## 6. 不变量

```text
1. ctxlib 只从 Event Log / Artifact 构建，不接受普通 agent 直接写。
2. 只有经 Agent Runtime 授权的 Ctx Manager Agent / Ctx Agent 能访问底层存储；对外只有 pack / query 两个只读操作。
3. 每个 block 必须有 source_refs，可追溯到来源事件 / artifact。
4. outdated block 默认不进 pack（除非显式查历史）。
5. 扩展通过 Extractor / Selector / Store 三个接口完成，不改核心模型。
6. 访问 ctxlib 的行为本身也被 Agent Runtime 自动记入 log。
7. Ctx Manager Agent 不是 runtime 旁路；它与 planner / executor / verifier 一样是 Agent Runtime invocation，只是拥有 ctx_read / ctx_write capability。
8. 每个 ContextResult 必须绑定 Task Contract、attempt、phase endpoint、role 和 input revision；绑定变化后必须重新选择。
9. 相关但被预算省略的 block 和相互矛盾的 block 必须显式返回，不能在摘要时静默消失。
```

---

## 7. 原型如何长成完整设计（扩展映射）

说明"覆盖没减少"，只是从内核挪到了扩展缝：

```text
需要的能力            原型落点                       扩展方向
------------------   ---------------------------   -----------------------------
更多 context 类型     kind 加约定值                  分类体系 / 校验
更多范围维度          scope 加前缀                   scope 索引 / 命名规范
置信度/新鲜度/重要度   attributes 键                  打分权重、时间衰减
可见性/风险控制        attributes 键                  Selector 里做权限过滤
多路召回 + 重排        Selector 换实现                embedding + rerank
过时判断              outdated 字段 + Selector       矛盾检测、历史保留
运行时检索协议         query 的 intent/scope          结构化 intent 词表
不同存储后端          Store 换实现                    向量库 / 外部服务
```

内核（第 2、3 节）保持稳定；以上都在不改内核的前提下增量演进。
