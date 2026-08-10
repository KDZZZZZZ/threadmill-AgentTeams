# Threadmill AgentTeams

Threadmill 把用户 Requirement 转成可审计的 Task 与固定 `plan / execute / verify` Phase Endpoint，并用内部 `GraphRuntime` 驱动 Agent Runtime。Task Manager 是 Coordination Graph 的唯一写入者；浏览器只能调整容量、提交 Manager 消息和读取权限过滤后的投影。

## 本地 GUI

```powershell
npm --prefix web ci
npm --prefix web run build
go run ./cmd/threadmilld serve --fake --http-addr 127.0.0.1:8080 --web-dist web/dist
```

打开 `http://127.0.0.1:8080/?project_id=demo-project`。fake-host 使用正式对象、OpenAPI 和 SSE，只把外部基础设施替换为内存实现；它不是第二套领域模型。

## 验证

```powershell
go test -count=1 ./...
go vet ./...
npm --prefix web run format:check
npm --prefix web run typecheck
npm --prefix web run test
npm --prefix web run e2e
npm run design:check
```

浏览器验收覆盖 Agent 并发调整、Manager hold/stop/resume、新 generation/Invocation、节点 Context 检查器和 SSE 重连，并断言没有调用图 mutation endpoint。

## 设计入口

- [统一设计](docs/threadmill-unified-design.md)
- [Coordination Graph](docs/coordination-graph.md)
- [Task Manager Agent](docs/task-manager-agent.md)
- [Phase Agent](docs/phase-agent.md)
- [Context Graph](docs/context-graph.md)
- [AgentTeams Adapter](docs/threadmill-agentteams-adapter-design.md)
- [GUI 与 SSE ADR](docs/adr/0006-web-ui-projection-and-sse.md)
- [设计—代码—测试追踪表](docs/traceability.md)

生产 PostgreSQL/MinIO wiring、真实 AgentTeams 凭据 smoke 和完整崩溃恢复尚未由本地 fake-host 验收覆盖；当前实现不会把这些状态标记为已完成。
