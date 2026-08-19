# QwenPaw Runtime Fixture（M3.5.2）

## 真实启动拓扑

`qwenpaw-worker` 是 AgentTeams 的守护进程，不是 management API。它在 `qwenpaw/src/qwenpaw_worker/worker.py` 的 `_run_qwenpaw` 中启动外部 `qwenpaw app --host 0.0.0.0 --port <console-port>`；该 app 才提供 `/api/version`、`/api/mcp`、`/api/mcp/policy/*`。worker 等待 API ready 后，通过 `QwenPawApiClient` 配置 agent、内置 MCP 和 runtime.yaml 中的 desired MCP。

官方可重复 E2E 是 `qwenpaw/tests/integration/` 的 Docker/Linux shell fixture：它启动 MinIO container 和 QwenPaw worker image，并用内部 QwenPaw app 的 `127.0.0.1:8088` API 验证 runtime.yaml 的 MCP 配置。M3.8-C 与 M4-D 已在 Windows PowerShell + Docker Desktop Linux Engine 上完成真实 QwenPaw worker、runtime.yaml MCP applied/readback、Threadmill tools/list 与 agent-originated MCP 调用验证；因此“当前 Windows 工作站没有 Docker、fixture 尚不可用”已不再成立。官方 shell fixture 仍要求 Docker/Linux shell，项目 focused runner 则复用 Docker Desktop 与等价的受控启动语义。

## Threadmill 验证入口

当官方 fixture 暴露 app API 后运行：

```powershell
$env:THREADMILL_IT_QWENPAW_URL = 'http://127.0.0.1:8088'
./scripts/run-qwenpaw-integration.ps1
```

该 integration test 仅接受真实 QwenPaw API：通过 `QwenPawMCPInjector` 创建 invocation-specific client、GET 验证 URL/enabled/transport、GET 验证 deny-by-default policy，然后删除并验证 404。没有 mock fallback。

QwenPaw 的 MCP registry 在当前源码中是 app 范围的 registry；`/api/mcp` 没有 invocation-to-agent 专属绑定字段。Threadmill 使用唯一 client key 和 opaque header 来隔离配置，但这尚不构成 QwenPaw 原生的 per-agent/per-invocation 强隔离；M7 需要进一步 hardening。

Threadmill 的 `internal/mcp/phase/http.go` 已提供 streamable HTTP MCP transport。它只从 `X-Threadmill-Execution-Token` header 读取 token，request body 不能声明 TaskID 或 InvocationID。其 `artifact.register` 和 `agent.submitPhaseOutput` 已经以真实 HTTP 测试验证，但该测试不是 QwenPaw worker execution。
