# GUI 操作与边界

GUI 是权威状态的权限过滤投影，不是第三份 Coordination Graph，也没有图写能力。

## Agent capacity

`desired` 是项目级最大并发目标；`healthy` 是可用宿主容量；`active` 是当前执行中的 Invocation；`waiting` 是保留逻辑 Invocation 但已释放物理宿主的数量。降低 desired 只停止新的超额 dispatch，让现有执行自然 drain，不会 hold、stop 或 cancel Phase，也不会改变 graph revision。

## Manager 调图

浏览器只发送自然语言、可选 `EndpointRef` 和所见 graph revision。Runtime 把消息持久化为 `ManagerInputRef`，Task Manager 产生结构化决策并先保存 `DecisionRef`，之后才允许 `TaskManagerGraph` 写图。页面不会把文本直接解析成 graph patch，也没有独立 CRUD 或 stop/resume 按钮。

`hold` 是 Endpoint 的调度策略，表示禁止新执行并要求 Runtime 停止现有 lease；`stop` 是 Invocation 生命周期结果，必须带 checkpoint 或 non-resumable 证据；`resume` 会基于新 BindingRef 创建新的 generation、Invocation、lease、session 和订阅，不复用旧模型会话。

## Phase Inspector

点击节点后显示三个独立区域：

- `Subscription subgraphs`：该 Invocation 的 initial/retrieval/explicit 有效订阅；Runtime 按有效订阅子图并集物化上下文。
- `Context Slice`：实际交给该 Invocation 的项目上下文、revision、frontier 和省略原因。
- `TaskMemoryBuffer`：由当前 Invocation 创建的候选；它们属于 Task 生命周期，但在接受前不是 Context Graph 节点。

历史 generation 可查看但不会被标为 active；所有正文还要经过项目、Task 和 operator ACL 过滤。

## 实时恢复

页面先读取 snapshot 及 cursor，再打开 SSE。首连使用 `after`，EventSource 自动重连时 `Last-Event-ID` 覆盖旧的 bootstrap cursor；游标过期时客户端必须重新读取 snapshot。
