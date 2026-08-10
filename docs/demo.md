# Threadmill 可验收 Demo

## 启动

```powershell
go run ./cmd/threadmill-demo
```

浏览器打开 `http://localhost:8080`。运行时只使用 Go 标准库与原生 HTML/CSS/JavaScript；根目录的 npm 包只用于设计合同校验和图标检索，不进入 Demo 运行路径。

## GUI 验收

1. 初始 `desired=2`、`active=2`。点击 `-` 把期望并发降为 1，现有 active invocation 保持运行；再点击 `+` 提升到 3 或 4，新的 runnable endpoint 被调度。整个过程只增加 capacity revision，不改变 graph revision。
2. 在协调图点击任一节点。下方 Inspector 必须同时显示订阅子图与有效并集、由并集投影出的项目 Context Slice，以及带创建 invocation 来源的 TaskMemoryBuffer 候选；切换节点时内容随 endpoint 改变。
3. 在 TaskMemoryBuffer 使用“当前 invocation / 同 Task”切换范围。默认只显示所选 invocation 的创建来源；同 Task 视图可显示共享候选。
4. 选中 active endpoint，点击 Manager 区的“暂停并保存检查点”。节点进入 `held` 并生成 checkpoint，Manager 日志出现 `ManagerInputRef` 和 `DecisionRef`。
5. 点击“从检查点恢复继续”。节点从 checkpoint 创建新 generation、新 invocation 和新 lease；Inspector 中上一代订阅保留但为 historical，新一代订阅为 active。
6. 依次选中 `ep-execute` 和 `ep-review` 并点击“完成并通过”。只有两个前置都 satisfied 后，`ep-publish` 才会变为 runnable，并在容量允许时开始执行。
7. 点击“创建一个可运行 endpoint”。新 endpoint 只能经 Manager 决策加入协调图；页面没有独立图 CRUD 或 invocation 直控入口。

页面通过 `GET /api/events` 接收 SSE 状态事件，因此 capacity、协调图、Manager 日志和已选节点的 Inspector 会实时刷新。

## 自动验收

```powershell
go test -count=1 ./...
node --check internal/demo/web/app.js
npx --no-install designmd lint DESIGN.md
npx --no-install designmd export --format dtcg DESIGN.md
```

测试覆盖并发 drain/扩容调度、Manager 唯一写图、hold/stop 可恢复、resume 换代、前置解锁、订阅并集上下文投影、缓冲区来源，以及 graph/capacity CAS 冲突不写入。

GUI 的持久设计合同是根目录 `DESIGN.md`；项目级 skill 的来源与受限安装边界记录在 `.agents/skills/README.md`。桌面与移动端真实浏览器验收图保存在 `.impeccable/review/`。
