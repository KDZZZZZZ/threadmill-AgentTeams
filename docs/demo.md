# Threadmill 本地 GUI 验收

本地 fake-host 只替换外部基础设施，仍使用正式的 `TaskManagerGraph`、`GraphRuntime` 边界、`Invocation`、Event Log、UI Projection、OpenAPI HTTP/SSE 和 React GUI。仓库不再保留 `/api/*` demo 对象或独立图模型。

## 启动

```powershell
npm --prefix web ci
npm --prefix web run build
go run ./cmd/threadmilld serve --fake --http-addr 127.0.0.1:8080 --web-dist web/dist
```

浏览器打开 `http://127.0.0.1:8080/?project_id=demo-project`。

## GUI 验收

1. 点击 Agent capacity 的 `+` / `-`，确认 desired concurrency 实时变化，但 Graph revision 不变化。
2. 选择 `task-alpha / execute`，在 Manager 输入 `hold current execute`；确认节点策略显示 `held`，Inspector 的旧 Invocation 显示 `stopped`，会话中出现 `ManagerInputRef`、`DecisionRef` 和 graph revision。
3. 输入 `resume current execute`；确认节点进入新 generation，创建新的 Invocation、BindingRef 和 Context Slice，而不是复用旧会话。
4. 点击 Phase 节点，确认 Inspector 分开显示 `Subscription subgraphs`、`Context Slice` 和 `TaskMemoryBuffer`，且不会泄露另一个 Task 的候选。
5. 页面不存在图 CRUD、拖拽写图、直接 stop/resume 或 GraphRuntime 控制接口；图变化只能来自 Manager 消息经 Task Manager 决策后的服务端投影。

## 自动验收

```powershell
go test -count=1 ./...
npm --prefix web run test
npm --prefix web run e2e
npm run design:check
```

Playwright 会编译并启动同一个 `threadmilld serve --fake` 入口，验证浏览器没有调用任何 Coordination Graph mutation endpoint，并覆盖 SSE 断开后的自动重连。
