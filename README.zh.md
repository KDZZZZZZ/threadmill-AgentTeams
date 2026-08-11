[English](README.md) | [中文](README.zh.md)

<div align="center">

# Threadmill

**把多 Agent 工作变成可审计、可恢复、可交付的控制平面。**

[![Go](https://img.shields.io/badge/Go-1.23.3-00ADD8?logo=go&logoColor=white)](https://github.com/KDZZZZZZ/threadmill-AgentTeams/tree/docs/readme-oops-dev-main)
[![React](https://img.shields.io/badge/React-19-149ECA?logo=react&logoColor=white)](https://github.com/KDZZZZZZ/threadmill-AgentTeams/tree/docs/readme-oops-dev-main/web)
[![Status](https://img.shields.io/badge/status-alpha-f0b429)](#状态)

requirement → plan → execute → verify → merge → done

</div>

> [!NOTE]
> 这份 README 是 main 分支对当前可运行实现的总览；实现代码来自 oops-dev，现保存在[预览快照](https://github.com/KDZZZZZZ/threadmill-AgentTeams/tree/docs/readme-oops-dev-main)。main 当前以设计文档为主，下面的本地启动命令请在预览快照执行。

## 一句话理解

多 Agent 开发容易启动，却很难治理：session 可能消失，verifier 可能检查错工作区，停止请求可能没有持久化检查点，看似成功的结果也可能没有安全进入 main 的路径。

Threadmill 把工作单位从聊天 session 改成持久化的 **Task**。Task 拥有自己的契约、依赖图、阶段生命周期、上下文边界、证据和交付策略；Agent 只是这些边界内的临时计算资源。

它把通常消失在对话里的关键状态显式化：

- 人类意图变成持久化的 Requirement 和 Task Contract；
- plan → execute → verify 变成明确且可恢复的生命周期；
- 上下文按当前 Invocation 隔离，而不是通过长期 session 泄漏；
- 代码只有经过验证、基于最新 main 的定向验证和 Merge Queue 才能进入 main。

## Threadmill 负责什么

| 边界 | Threadmill 的权威职责 |
| --- | --- |
| 工作身份 | 持久化 Task，而不是 Agent session |
| 协调 | 一个持久化 Coordination Graph，由 Task Manager 作为唯一外部写入者 |
| 执行 | Invocation 封装角色、阶段、工作区、上下文、能力、租约和证据 |
| 上下文 | 按订阅生成 Context Slice，并产生可审核的 Context Delta |
| 恢复 | 检查点、明确的 non_resumable 结果，以及重试时的新 generation |
| 交付 | verify → latest-main targeted verify → Merge Queue → main |
| 浏览器 | 过滤后的投影，以及四类受限写入：需求、容量、人类决策、Manager 消息 |

AgentTeams 作为 adapter 后的执行宿主存在。项目语义、授权边界、图 revision、证据和合并策略由 Threadmill 负责。

## 总体架构

两个持久化图保存业务语义；Runtime、Workspace 和 provider adapter 是围绕它们的执行边界。

~~~mermaid
flowchart TB
  human["人类操作者"]
  ui["React 操作台"]

  subgraph surface["策略过滤后的表面"]
    http["HTTP / JSON 命令"]
    projection["权限过滤后的 UI 投影"]
    sse["游标 SSE<br/>快照 + 恢复"]
  end

  subgraph core["Threadmill 控制平面"]
    manager["Task Manager<br/>Coordination Graph 唯一外部写入者"]
    coordination[("Coordination Graph<br/>任务 · 依赖 · 阶段")]
    runtime["GraphRuntime + Agent Runtime<br/>invocation · lease · evidence"]
    context["Context Service"]
    contextGraph[("Context Graph<br/>订阅 · 切片 · delta")]
    workspace["Workspace + Merge Queue<br/>latest-main gate"]
  end

  subgraph substrate["持久化与外部基座"]
    database[("PostgreSQL<br/>状态 · 事件 · outbox")]
    artifacts[("MinIO / S3<br/>大对象")]
    provider["AgentTeams / QwenPaw<br/>执行宿主"]
  end

  human --> ui
  ui -->|"受限命令"| http
  sse -->|"实时投影"| ui
  http --> manager
  manager -->|"DecisionRef + revision"| coordination
  coordination -->|"PhaseCommand"| runtime
  runtime -->|"受限 Invocation"| provider
  runtime --> context
  context -->|"检索与整理"| contextGraph
  contextGraph -->|"Context Delta"| runtime
  runtime --> workspace
  coordination --> database
  contextGraph --> database
  workspace --> database
  workspace --> artifacts
  database --> projection
  projection --> sse

  classDef actor fill:#10213e,color:#fff,stroke:#8ba4ff,stroke-width:2px;
  classDef surfaceNode fill:#e8efff,color:#10213e,stroke:#6f8ee8,stroke-width:1.5px;
  classDef authority fill:#d9f3ee,color:#102c2a,stroke:#139b8a,stroke-width:2px;
  classDef boundary fill:#fff3d8,color:#3b2a05,stroke:#d19a27,stroke-width:1.5px;
  classDef storage fill:#eee9ff,color:#241447,stroke:#8d75d6,stroke-width:1.5px;
  class human,ui actor;
  class http,projection,sse surfaceNode;
  class manager,coordination,runtime,context,contextGraph authority;
  class workspace,provider boundary;
  class database,artifacts storage;
~~~

### 两张图各自回答什么

- **Coordination Graph** 回答“下一步什么可以运行，什么在阻塞它？”它保存持久化 Task、阶段端点、依赖、阻塞、revision 和结果引用；GraphRuntime 消费它，但它不是浏览器或 Agent 的公开 API。
- **Context Graph** 回答“当前 Invocation 可以看到哪些持久知识？”有效订阅会生成经过权限过滤的 Context Slice，新信息以 Delta 进入，Task memory candidate 在审核前保持独立。

阶段控制面保持窄而明确：

~~~go
type PhaseController interface {
    Apply(ctx context.Context, command PhaseCommand) error
}
~~~

调用只确认命令已被可靠接受；started、stopped、failed 和 output 事件异步进入 Event Log，而不是由调用者偷偷改写图。

## 从需求到合并

交付路径是策略门，而不是约定。验证失败会产生证据和 Manager 决策，绝不会静默复用旧结果。

~~~mermaid
flowchart LR
  requirement["Requirement"] --> contract["Task Contract"]
  contract --> plan["plan"]
  plan --> execute["execute"]
  execute --> verify["verify"]
  verify --> decision{"契约满足？"}
  decision -->|"否：提出方案"| manager["Manager 决策<br/>新的 graph revision"]
  manager --> execute
  decision -->|"是"| policy{"DeliveryPolicy"}
  policy -->|"code_merge"| latest["基于最新 main<br/>定向验证"]
  latest --> queue["Merge Queue"]
  queue --> done["done"]
  policy -->|"其他交付"| done
  hold["hold / resume"] --> evidence["停止证据"]
  evidence --> generation["新 generation + Invocation"]
  generation --> execute

  classDef input fill:#10213e,color:#fff,stroke:#8ba4ff,stroke-width:2px;
  classDef phase fill:#d9f3ee,color:#102c2a,stroke:#139b8a,stroke-width:2px;
  classDef decision fill:#fff3d8,color:#3b2a05,stroke:#d19a27,stroke-width:1.5px;
  classDef delivery fill:#eee9ff,color:#241447,stroke:#8d75d6,stroke-width:1.5px;
  class requirement,contract input;
  class plan,execute,verify,generation phase;
  class decision,manager,policy,hold,evidence decision;
  class latest,queue,done delivery;
~~~

## 操作者看到什么

浏览器是投影和命令表面，不是第二个编排引擎。它可以展示容量、图状态、Manager 决策、端点检查、Context Slice 和可恢复事件流，但不会接收 Agent token、私有 transcript、原始工具输出，也没有 graph CRUD 权限。

~~~mermaid
sequenceDiagram
  participant O as 操作者
  participant UI as 操作台
  participant API as HTTP API
  participant TM as Task Manager
  participant LOG as Event Log

  O->>UI: 提交受限意图
  UI->>API: 幂等 JSON 命令 + revision
  API->>TM: 持久化输入并执行策略
  TM-->>API: DecisionRef + 新 revision
  API-->>UI: 命令已接受
  LOG-->>UI: 游标 SSE 投影
  UI->>API: 携带 Last-Event-ID 重连
  API-->>UI: 恢复游标或发送新快照
~~~

## 仓库导航

设计文档位于 main；实现目录链接到从 oops-dev 生成的预览快照，确保这份 README 在代码尚未晋升时仍然准确。

| 区域 | 作用 |
| --- | --- |
| [docs/architecture.md](https://github.com/KDZZZZZZ/threadmill-AgentTeams/blob/main/docs/architecture.md) | 系统边界与依赖方向 |
| [docs/task-graph.md](https://github.com/KDZZZZZZ/threadmill-AgentTeams/blob/main/docs/task-graph.md) | 持久化工作身份与图语义 |
| [docs/workspace-merge.md](https://github.com/KDZZZZZZ/threadmill-AgentTeams/blob/main/docs/workspace-merge.md) | Workspace 绑定与 Merge Queue 策略 |
| [docs/CONTEXT.md](https://github.com/KDZZZZZZ/threadmill-AgentTeams/blob/main/docs/CONTEXT.md) | 上下文和记忆模型 |
| [cmd/threadmilld](https://github.com/KDZZZZZZ/threadmill-AgentTeams/tree/docs/readme-oops-dev-main/cmd/threadmilld) | CLI：serve、migrate、check、bootstrap-operator |
| [internal/coordination](https://github.com/KDZZZZZZ/threadmill-AgentTeams/tree/docs/readme-oops-dev-main/internal/coordination) | Coordination Graph、revision、lease 与 runtime |
| [internal/contextgraph](https://github.com/KDZZZZZZ/threadmill-AgentTeams/tree/docs/readme-oops-dev-main/internal/contextgraph) | 订阅、切片、delta 与记忆审核 |
| [internal/taskmanager](https://github.com/KDZZZZZZ/threadmill-AgentTeams/tree/docs/readme-oops-dev-main/internal/taskmanager) | 需求入口与决策持久化 |
| [internal/workspace](https://github.com/KDZZZZZZ/threadmill-AgentTeams/tree/docs/readme-oops-dev-main/internal/workspace) | Workspace Binding 与仓库操作 |
| [internal/mergequeue](https://github.com/KDZZZZZZ/threadmill-AgentTeams/tree/docs/readme-oops-dev-main/internal/mergequeue) | latest-main 验证与 main 写入门 |
| [internal/uiprojection](https://github.com/KDZZZZZZ/threadmill-AgentTeams/tree/docs/readme-oops-dev-main/internal/uiprojection) | 权限过滤快照与 UI 事件 |
| [web](https://github.com/KDZZZZZZ/threadmill-AgentTeams/tree/docs/readme-oops-dev-main/web) | React + TypeScript + Vite 操作台 |
| [api/openapi](https://github.com/KDZZZZZZ/threadmill-AgentTeams/tree/docs/readme-oops-dev-main/api/openapi) | 浏览器端契约 |
| [third_party/agentteams](https://github.com/KDZZZZZZ/threadmill-AgentTeams/tree/main/third_party/agentteams) | 归档 provider 代码；根项目默认只读 |

## 快速开始：运行预览版

下面的命令针对从 oops-dev 生成的预览快照，不是以文档为主的 main 快照：

~~~powershell
git clone https://github.com/KDZZZZZZ/threadmill-AgentTeams.git
cd threadmill-AgentTeams
git fetch origin docs/readme-oops-dev-main
git switch --track origin/docs/readme-oops-dev-main

npm --prefix web ci
npm --prefix web run build
go run ./cmd/threadmilld serve --fake --http-addr 127.0.0.1:8080 --web-dist web/dist
~~~

打开 <http://127.0.0.1:8080/?project_id=demo-project>，查看实时 Coordination Graph、容量控制、Manager 面板、端点检查器和事件流。

本地验证命令：

~~~powershell
go test -count=1 ./...
go vet ./...
npm --prefix web run typecheck
npm --prefix web run test
npm --prefix web run build
~~~

仓库 GitHub Actions 已关闭，不会产生自动运行费用；上面的命令就是本地验证路径。

## 状态

| 能力 | 证据 | 状态 |
| --- | --- | --- |
| 持久化工作模型 | 设计文档定义 Requirement、Contract、Task、Attempt、Invocation 与阶段端点 | 设计进行中 |
| Coordination / Context Graph | 所有权、revision、订阅、切片和 delta 已明确分离 | 设计进行中 |
| 本地操作台 | 预览快照包含 fake host、OpenAPI、SSE、projection 和 React 验收路径 | 预览版 |
| 生产运行时 | PostgreSQL、MinIO/S3、真实 provider 凭据、跨进程恢复和部署仍需环境验证 | 尚未宣称完成 |

## 深入阅读

- [统一设计](https://github.com/KDZZZZZZ/threadmill-AgentTeams/blob/docs/readme-oops-dev-main/docs/threadmill-unified-design.md)
- [Coordination Graph](https://github.com/KDZZZZZZ/threadmill-AgentTeams/blob/docs/readme-oops-dev-main/docs/coordination-graph.md)
- [Context Graph](https://github.com/KDZZZZZZ/threadmill-AgentTeams/blob/docs/readme-oops-dev-main/docs/context-graph.md)
- [GUI 与 SSE](https://github.com/KDZZZZZZ/threadmill-AgentTeams/blob/docs/readme-oops-dev-main/docs/gui.md)
- [设计—代码—测试追踪](https://github.com/KDZZZZZZ/threadmill-AgentTeams/blob/docs/readme-oops-dev-main/docs/traceability.md)
- [架构设计依据](https://github.com/KDZZZZZZ/threadmill-AgentTeams/blob/main/docs/design-rationale.md)

## 贡献

保持改动小而可审查。领域语义变化时，请同步更新设计契约、公开 schema、实现和证据。除非任务明确针对 third_party/agentteams/，否则将其视为归档基础代码。

根项目目前没有单独的 license 文件；third_party/agentteams/ 下的许可证只适用于该归档组件。
