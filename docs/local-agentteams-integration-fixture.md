# 本地 AgentTeams 集成 Fixture（M3.5.1）

TeamHarness 正式 taskflow 并非单纯本地文件状态机：`plugins/teamharness/mcp/server.py` 中 `delegate_task` 调用 `_sync_task`；`ack_task`、`submit_task`、`cancel_task` 也调用 `_sync_task`；`check_task` 调用 `_pull_task`。两条路径均进入 `_filesync`，以 `mc mirror`/`mc cp` 操作对象存储。

所以完整真实 taskflow fixture 需要 MinIO 与 MinIO Client，而不是伪造同步命令。

Windows 使用：

```powershell
./scripts/run-teamharness-integration.ps1
```

该脚本下载官方 `minio.exe`、`mc.exe` 到 `%TEMP%\threadmill-agentteams-tools`（不提交到仓库），启动临时 MinIO、创建 bucket、运行 integration-tag Go 测试并清理临时数据。需要网络、Python、Go；不需要模型 API key。

QwenPaw fixture 尚未可用：官方 `qwenpaw-worker` daemon 要启动外部 `qwenpaw app`，并依赖 MinIO/运行时配置。当前仓库未提供可独立启动的官方 QwenPaw management API 二进制或轻量 fixture；不能用 mock HTTP server 宣称 QwenPaw MCP injection 已验证。
