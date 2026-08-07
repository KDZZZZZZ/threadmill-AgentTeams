# Agent Runtime 核心设计

版本：v0.5
状态：Draft

---

## 1. 定位

Agent Runtime 负责把 Claude Code、Codex、Gemini CLI 和其他 headless CLI agent 包装成统一 worker，供 Control Plane 调度。系统内所有 agent 都经这条边界运行：Task Manager Agent、Ctx Manager Agent / Ctx Agent、planner、executor、verifier 都是 Agent Runtime 管理的 invocation。

它的目标不是抹平所有 agent 的能力差异，而是提供一条稳定边界：

```text
Control Plane / Scheduler
  -> Agent Runtime
  -> Agent Adapter / System Agent Role
  -> Claude Code / Codex / Gemini / Custom CLI
  -> Task Manager Agent / Ctx Manager Agent / planner / executor / verifier
```

第一阶段只实现 Claude Code 的最小完整包装：

```text
- 检测 Claude Code CLI 和认证状态。
- 声明能力。
- 在指定 worktree / cwd 中 headless 启动。
- 注入 system prompt、task prompt 和 context pack。
- 控制允许工具和权限模式。
- 解析 stream-json 输出为统一 AgentEvent。
- 记录 Event Log 和 Artifact Store 引用。
- 汇总 AgentResult 返回给 Control Plane；Task Manager Agent、Ctx Manager Agent、Verifier 和 Merge Queue 分别按权责消费。
- 对 Task Manager Agent / Ctx Manager Agent 也使用同一套启动、权限、事件、artifact 和取消语义。
```

---

## 2. 设计判断

参考 Open Design 的 agent runtime / adapter 架构，Agent Runtime 采用两层包装：

```text
1. Agent Adapter Layer
   面向不同 CLI agent，处理 detect / capabilities / run / cancel / resume / stream parse。

2. Runtime Orchestration Layer
   面向本系统，处理 task attempt、worktree、context pack、event log、artifact、result 汇总。
```

Open Design 对应关系：

```text
Open Design AgentAdapter.detect       -> 本系统 AgentAdapter.detect
Open Design AgentAdapter.capabilities -> 本系统 AgentCapabilities
Open Design AgentAdapter.run          -> 本系统 AgentAdapter.run(params)
Open Design AgentEvent                -> 本系统统一 AgentEvent，并额外保留 raw provider event
Open Design artifact workspace        -> 本系统 task attempt worktree / artifact refs
Open Design product run               -> 本系统 Control Plane / Scheduler 管理的 task phase attempt
```

本系统不会照搬 Open Design 的设计 artifact / preview / plugin marketplace，但 Agent Runtime 这部分需求基本一致：都是把外部 CLI/stdio agent 包装成可检测、可运行、可取消、可恢复、可观测的 worker。因此不只借鉴 adapter 接口，也可以借鉴它的 runtime 分层、run lifecycle、session/cancel 策略和事件归一化方式。

关键判断：

```text
1. adapter 是 CLI 归一化边界。
   Scheduler 不知道 claude/codex/gemini 的具体 flags。

2. capability 只表达调度和产品真正需要的能力。
   底层 flags 留在 adapter 内部。

3. event 要小而稳定。
   provider 原始事件保存在 raw 中，向上只暴露统一 AgentEvent。

4. workspace / artifact 是一等对象。
   Runtime 不只保存 stdout，还要观察 diff、文件写入、transcript 和测试证据。

5. fallback 不能静默发生。
   从一个 adapter 切到另一个 adapter 必须显式记录，并受策略控制。

6. Task Manager Agent 和 Ctx Manager Agent 不是 runtime 旁路。
   它们是带有特殊 tool/capability 授权的系统 agent invocation；Task Graph / ctxlib 的实际写入由 Go backend 受控 service/tool 承接。
```

---

### 2.1 Open Design Agent Runtime 架构可借鉴点

这一层可以更直接参考 Open Design，因为两边的 Agent Runtime 需求是同构的：

```text
- 管理外部 agent CLI / stdio runtime，而不是实现模型本身。
- 在运行前做 detect / version / auth / capability probe。
- 把统一的 AgentRunParams 翻译成 provider-specific argv / env / stdin / JSON-RPC。
- 管理 cwd、allowed dirs、prompt transport、session handle、cancel 和 process lifecycle。
- 把 provider stream 解析成小而稳定的 AgentEvent，并持久化 raw event / event log。
- 把权限、工具/MCP 注入、sandbox/trust/yolo 等危险能力收口到 runtime policy。
```

因此 Agent Runtime 建议按 Open Design 的 daemon runtime 结构拆成以下内部模块：

```text
AgentProviderSpec / RuntimeAgentDef
  声明 provider 如何 detect、如何 buildArgs、如何 parse stream、如何 resume/cancel。

AgentRegistry
  注册 Claude / Codex / Gemini / OpenCode / ACP 等 provider，并向 Control Plane 暴露稳定 AgentInfo。

AgentRunService
  维护 AgentAttempt / run state / pid / session handle / event refs，负责统一 invoke 和状态流转。

ProcessRunner / PromptTransport
  统一处理 cwd、env、stdio、stdin_text、stdin_jsonl、prompt_file、process group。

EventParser
  把 provider 原始输出解析成 AgentEvent，同时保留 raw provider event。

PermissionPolicy / ToolInjectionPolicy
  决定 allowed dirs、shell/file edit 权限、MCP/tool config 注入方式和是否允许 provider 的 bypass/trust/yolo 参数。
```

和 Task Graph 的边界保持不变：Open Design runtime 可以作为 Agent Runtime 的架构参考，但跨 task/phase 的 phase endpoint、dependency edge 和 blocker 仍属于 Task Graph / Scheduler。Agent Runtime 只执行一个已经被调度出来的 `AgentRunParams`，不负责持久工作关系的编排；MVP 由 Scheduler 直接选择可运行 endpoint。

---

### 2.2 Open Design 源码可借鉴点

Open Design 的源码参考固定到以下 commit，避免后续 upstream 变更导致链接漂移：

```text
repo: https://github.com/nexu-io/open-design
commit: 02c68415e29dcab659f1835e8a41ec1a37fce303
```

注意边界：这些源码用于借鉴 **agent runtime / CLI wrapper / adapter**。Task Graph 的 task、phase endpoint、edge 和 blocker 不进入 Agent Runtime；它们仍由 Task Graph / Scheduler 管理，Runtime 只负责受控 invocation 的输入、输出、权限、事件和 artifact。

| Open Design 源码 | 可以借鉴的内容 | 本系统落点 |
|---|---|---|
| [`docs/agent-adapters.md`](https://github.com/nexu-io/open-design/blob/02c68415e29dcab659f1835e8a41ec1a37fce303/docs/agent-adapters.md) | 高层 adapter 设计：detect / capabilities / run / cancel / resume / event stream / fallback / authorization boundary。文档里的 `AgentAdapter` 是概念接口。 | 本文的边界说明和 `AgentAdapter` 概念接口。 |
| [`apps/daemon/src/runtimes/types.ts#L95-L247`](https://github.com/nexu-io/open-design/blob/02c68415e29dcab659f1835e8a41ec1a37fce303/apps/daemon/src/runtimes/types.ts#L95-L247) | 源码里的真实 adapter 形态是声明式 `RuntimeAgentDef`，包含 `id/name/bin/versionArgs/buildArgs/streamFormat/promptViaStdin/eventParser/authProbe/capabilityFlags/resume` 等。它比每个 adapter 实现完整 `run()` 更薄。 | 可抽成 `AgentProviderSpec` / `AgentAdapterSpec`：provider 只声明如何发现、如何组 argv/env、如何解析流、如何 resume；统一 run loop 由 Agent Runtime 管。 |
| [`apps/daemon/src/runtimes/registry.ts`](https://github.com/nexu-io/open-design/blob/02c68415e29dcab659f1835e8a41ec1a37fce303/apps/daemon/src/runtimes/registry.ts) | 将 Claude / Codex / Cursor / OpenCode / ACP runtimes 注册到一个 `AGENT_DEFS` 列表，并检查重复 id。 | 本系统保留 provider registry，但 registry 只暴露 adapter 能力，不暴露 provider-specific flags 给 Scheduler。 |
| [`apps/daemon/src/runtimes/detection.ts#L171-L260`](https://github.com/nexu-io/open-design/blob/02c68415e29dcab659f1835e8a41ec1a37fce303/apps/daemon/src/runtimes/detection.ts#L171-L260) | detection 先解析真实可执行文件，再并行做 help capability、model list、auth probe；capability flags 通过 `--help` 探测后缓存，供 `buildArgs` 决定是否传新 flag。 | `AgentAdapter.detect()` 和 `AgentCapabilities` 不应只写死；要支持版本/能力探测，避免旧 CLI 因未知 flag 直接失败。 |
| [`packages/contracts/src/api/registry.ts#L82-L125`](https://github.com/nexu-io/open-design/blob/02c68415e29dcab659f1835e8a41ec1a37fce303/packages/contracts/src/api/registry.ts#L82-L125) | 对外暴露的 `AgentInfo` 只包含 available/auth/path/version/diagnostics/models/reasoningOptions/docsUrl/externalMcpInjection 等稳定字段。 | Control Plane / UI 只消费稳定能力和诊断，不直接消费 adapter 内部实现。 |
| [`apps/daemon/src/server.ts#L5412-L5427`](https://github.com/nexu-io/open-design/blob/02c68415e29dcab659f1835e8a41ec1a37fce303/apps/daemon/src/server.ts#L5412-L5427) | 统一 runner 在 spawn 前调用 `def.buildArgs(composed, images, allowedDirs, options, runtimeContext)`，runtimeContext 包含 cwd、prompt file、resumeSessionId、newSessionId。 | `AgentRunParams` 里明确区分 task 输入、workspace/cwd、allowed dirs、model/reasoning、session handle；adapter 只把这些翻译成 CLI argv/env。 |
| [`apps/daemon/src/server.ts#L5836-L5885`](https://github.com/nexu-io/open-design/blob/02c68415e29dcab659f1835e8a41ec1a37fce303/apps/daemon/src/server.ts#L5836-L5885) | 统一 spawn：计算 stdin 模式、合并 agent env、构造 command invocation、设置 cwd、stdio、process group。 | Agent Runtime 统一管理进程生命周期、cwd、env、stdio 和 pid；provider adapter 不直接管理 task 状态。 |
| [`apps/daemon/src/server.ts#L7309-L7342`](https://github.com/nexu-io/open-design/blob/02c68415e29dcab659f1835e8a41ec1a37fce303/apps/daemon/src/server.ts#L7309-L7342) | prompt 默认走 stdin，避免 Windows argv 长度限制；Claude stream-json 模式把 prompt 包成 JSONL user message，并保持 stdin 打开。 | 本系统抽象 `PromptTransport`：`stdin_text` / `stdin_jsonl` / `prompt_file` / `argv_small_only`。大 prompt 不走 argv。 |
| [`apps/daemon/src/runtimes/defs/claude.ts#L17-L97`](https://github.com/nexu-io/open-design/blob/02c68415e29dcab659f1835e8a41ec1a37fce303/apps/daemon/src/runtimes/defs/claude.ts#L17-L97) | Claude Code wrapper：`claude -p`、stream-json 输入输出、capability-gated `--include-partial-messages` / `--add-dir`、`--session-id` / `--resume`、permission mode、MCP 注入策略。 | Claude MVP adapter 的主要参考；但 permission mode 不能默认照抄 bypass，需要由本系统 `PermissionPolicy` 显式决定。 |
| [`apps/daemon/src/runtimes/defs/codex.ts#L61-L206`](https://github.com/nexu-io/open-design/blob/02c68415e29dcab659f1835e8a41ec1a37fce303/apps/daemon/src/runtimes/defs/codex.ts#L61-L206) | Codex wrapper：`codex exec --json`、stdin prompt、sandbox 策略、create/resume 参数差异、`-C cwd` / `--add-dir` 只在 create 时传、从 stream 捕获 thread id。 | Codex 后续 adapter 可借鉴 session resume 和 sandbox 翻译方式；尤其要避免把 create-only flags 传给 resume。 |
| [`apps/daemon/src/runtimes/defs/cursor-agent.ts#L31-L106`](https://github.com/nexu-io/open-design/blob/02c68415e29dcab659f1835e8a41ec1a37fce303/apps/daemon/src/runtimes/defs/cursor-agent.ts#L31-L106) | Cursor Agent wrapper：print/headless、stream-json、workspace 参数、`--trust` 通过 capability probe 决定、auth probe。 | 证明 adapter 需要“按版本探测再传 flag”，不能把所有 provider 统一成一套固定参数。 |
| [`apps/daemon/src/runtimes/defs/opencode.ts#L8-L90`](https://github.com/nexu-io/open-design/blob/02c68415e29dcab659f1835e8a41ec1a37fce303/apps/daemon/src/runtimes/defs/opencode.ts#L8-L90) | OpenCode wrapper：`opencode run --format json`、capture-style session resume、skip permission flag 探测、MCP 通过 env content 注入。 | 可借鉴“external tool/MCP 注入策略是 adapter capability”的表达，不要让上层直接知道 provider 配置格式。 |
| [`apps/daemon/src/runtimes/json-event-stream.ts`](https://github.com/nexu-io/open-design/blob/02c68415e29dcab659f1835e8a41ec1a37fce303/apps/daemon/src/runtimes/json-event-stream.ts) | 将 Codex / Cursor / Gemini / OpenCode 的 JSON stream 归一成 `status/text_delta/tool_use/tool_result/usage/error/raw`。 | `AgentEvent` parser 应保留 provider raw event，并只向上暴露稳定小 schema。 |
| [`apps/daemon/src/runtimes/claude-stream.ts`](https://github.com/nexu-io/open-design/blob/02c68415e29dcab659f1835e8a41ec1a37fce303/apps/daemon/src/runtimes/claude-stream.ts) | Claude stream-json 专用 parser，处理 init/status、assistant text、tool use/result、usage、session id、raw line。 | Claude MVP parser 的直接参考；本系统事件命名仍以本文 `AgentEvent` 为准。 |
| [`apps/daemon/src/runtimes/runs.ts#L28-L190`](https://github.com/nexu-io/open-design/blob/02c68415e29dcab659f1835e8a41ec1a37fce303/apps/daemon/src/runtimes/runs.ts#L28-L190) | run service 维护 run 状态、SSE clients、内存事件、per-run JSONL event log、child pid、process group、eventsLogPath。 | `AgentAttempt` 生命周期和 Event Log 写入可以借鉴；Threadmill 不需要照搬 SSE，但需要持久化事件 ref。 |
| [`apps/daemon/src/runtimes/runs.ts#L274-L405`](https://github.com/nexu-io/open-design/blob/02c68415e29dcab659f1835e8a41ec1a37fce303/apps/daemon/src/runtimes/runs.ts#L274-L405) | cancel 逻辑：close stdin、清理 pending retry、优先 RPC abort、再 SIGTERM、grace wait、最后 SIGKILL。 | `AgentRuntime.cancel()` 应有分层策略：adapter-level cancel / stdin close / process signal / force kill，并记录取消事件。 |
| [`docs/new-agent-runtime-acp.md`](https://github.com/nexu-io/open-design/blob/02c68415e29dcab659f1835e8a41ec1a37fce303/docs/new-agent-runtime-acp.md) | 推荐新 runtime 暴露 ACP over stdio CLI；daemon spawn 子进程，通过 stdin/stdout 说 JSON-RPC，stderr 只放日志。 | 如果未来自研 agent runtime，可优先做 stdio JSON-RPC wrapper，而不是让主进程直接绑定某个 SDK。 |
| [`apps/daemon/src/acp.ts#L1578-L1606`](https://github.com/nexu-io/open-design/blob/02c68415e29dcab659f1835e8a41ec1a37fce303/apps/daemon/src/acp.ts#L1578-L1606) | ACP session create/load：有 `resumeSessionId` 时走 `session/load`，否则 `session/new`，并从结果里取 durable session handle。 | 本系统 session resume 要区分 daemon-minted、provider-captured、ACP-load 三类，不要只有一个 boolean。 |
| [`apps/daemon/src/acp.ts#L1320-L1330`](https://github.com/nexu-io/open-design/blob/02c68415e29dcab659f1835e8a41ec1a37fce303/apps/daemon/src/acp.ts#L1320-L1330) | ACP `session/request_permission` 的自动选择逻辑。 | 可以借鉴 permission request 的事件形态，但是否自动 approve 必须由 Threadmill 的 `PermissionPolicy` 控制。 |
| [`apps/daemon/src/acp.ts#L1715-L1737`](https://github.com/nexu-io/open-design/blob/02c68415e29dcab659f1835e8a41ec1a37fce303/apps/daemon/src/acp.ts#L1715-L1737) | ACP cancel：有 session 时先发 `session/cancel`，无论如何关闭 stdin，让子 runtime 自己释放资源。 | adapter-level cancel 应优先给 provider 自清理机会，再走进程级 kill。 |

可以直接“抄结构”的部分：

```text
1. RuntimeAgentDef 这种声明式 provider spec。
2. detect/version/auth/capability/model probe 拆分。
3. buildArgs 只做 CLI 参数翻译，不拥有 task 状态。
4. prompt transport 默认 stdin / JSONL / file，避免 argv 大 prompt。
5. stream parser -> 小而稳定的 AgentEvent，同时保留 raw provider event。
6. session resume policy 区分 daemon-minted、provider-captured、ACP-load。
7. run lifecycle / session / cancel / event log 由统一 runtime 管。
8. external MCP / tool config 作为 adapter injection policy，而不是上层直接拼 provider config。
```

不能直接抄的部分：

```text
1. Open Design 的 artifact / preview / plugin marketplace 绑定。
2. 面向 Web UI 的 SSE shape。
3. 默认 bypass / trust / yolo 之类 provider 权限策略。
4. 把 graph/node/edge/control-flow 放进 Agent Runtime。
5. 让 agent 直接写 Task Graph、ctxlib 或 main branch。
```

---

## 3. 非目标

第一阶段不追求：

```text
1. 同时完整支持所有 CLI agent。
2. 暴露每个 CLI 的所有命令行参数给上层。
3. 自己重新实现一个独立于 CLI 的工具系统。
4. 自己重新实现一个独立于 CLI 的 worktree 抽象。
5. 让 executor 在没有 plan / requirement 的情况下扩大 scope。
6. 让 verifier 自我批准自己或同一 active context 的执行结果。
7. 让 agent 直接写 Task Graph、ctxlib 或 main branch。
```

---

## 4. 总体结构

```text
┌──────────────────────────────────────────────┐
│              Control Plane / Scheduler       │
│  task phase / system role / budget / capacity│
└───────────────────────┬──────────────────────┘
                        │
                        ▼
┌──────────────────────────────────────────────┐
│                 Agent Runtime                │
│  prepare / invoke / observe / summarize      │
└───────────┬──────────────────────┬───────────┘
            │                      │
            ▼                      ▼
┌──────────────────────┐  ┌────────────────────┐
│    Agent Adapter     │  │ Event / Artifact   │
│ detect/run/parse     │  │ log refs / blobs   │
└───────────┬──────────┘  └────────────────────┘
            │
            ▼
┌──────────────────────────────────────────────┐
│       Agent Invocation in controlled boundary│
│ task_manager / ctx_manager / plan/exec/verify│
│ Claude Code / Codex / Gemini / Custom        │
└───────────────────────┬──────────────────────┘
                        │
                        ▼
┌──────────────────────────────────────────────┐
│      AgentResult + observed write set        │
│  summary / status / artifacts / event refs   │
└──────────────────────────────────────────────┘
```

一句话概括：

```text
Agent Runtime 从 Control Plane / Scheduler 接收一个 AgentRunParams。这个 run 可以是普通 task phase attempt，也可以是 task_manager / ctx_manager 这类系统 agent invocation。Runtime 准备 context、workspace、权限和工具边界，选择合适 adapter 运行 agent，把输出解析成统一事件，观察实际写入和 diff，最后返回可验收的 AgentResult。
```

---

## 5. 核心接口

## 5.1 接口设计参考原则

核心接口也参考 Open Design，但参考的不是“每个 provider 都实现一个很厚的 `run()` 类”，而是源码里更实际的形态：

```text
RuntimeAgentDef / provider spec
  + centralized AgentRunService / daemon runner
  + provider-specific buildArgs / eventParser / probe / resume policy
```

也就是说：

```text
- provider 侧接口尽量声明式，描述如何 detect、如何拼命令、如何解析流、如何恢复 session。
- runtime 侧接口统一管理 run state、cwd/env/stdio、prompt transport、event log、cancel 和 result 汇总。
- Control Plane / Scheduler 只看 AgentInfo / AgentCapabilities / AgentResult，不接触 provider-specific flags。
```

`AgentAdapter` 仍可作为概念 facade，但实现上优先落成 `AgentProviderSpec + AgentRunService`。

---

## 5.2 AgentProviderSpec

`AgentProviderSpec` 对应 Open Design 源码里的 `RuntimeAgentDef` 思路，是 provider adapter 的主要实现单元。

```go
type AgentProviderSpec struct {
	// ID 是 runtime 内部稳定标识，例如 "claude-code" 或 "codex"。
	ID string `json:"id"`
	// DisplayName 是 Electron/React UI 展示名称。
	DisplayName string `json:"display_name"`
	// Provider 标记底层 CLI / stdio runtime 类型。
	Provider ProviderKind `json:"provider"`

	// DocsURL 指向 provider 官方文档或本地说明。
	DocsURL string `json:"docs_url,omitempty"`
	// Executable 描述可执行文件查找方式；PATH 探测留在 adapter 内。
	Executable ExecutableSpec `json:"executable"`
	// VersionArgs 是版本探测参数，例如 ["--version"]。
	VersionArgs []string `json:"version_args,omitempty"`

	// Detect / Probe 是声明式探测配置，不拥有 task 状态。
	Detect DetectionSpec `json:"detect"`
	AuthProbe *AuthProbeSpec `json:"auth_probe,omitempty"`
	CapabilityProbe *CapabilityProbeSpec `json:"capability_probe,omitempty"`
	ModelProbe *ModelProbeSpec `json:"model_probe,omitempty"`

	// PromptTransport 决定 prompt 通过 stdin、JSONL、文件还是 JSON-RPC 传入。
	PromptTransport PromptTransport `json:"prompt_transport"`
	// StreamFormat 决定 provider 原始输出格式，parser 据此转换成 AgentEvent。
	StreamFormat StreamFormat `json:"stream_format"`

	// BuildArgs 只把统一参数翻译成 argv/env/stdin，不直接调度 task。
	BuildArgs func(input AgentCommandBuildInput) (AgentCommand, error) `json:"-"`
	// ParseEvent 只做 raw provider event -> 统一 AgentEvent 的映射。
	ParseEvent func(raw RawProviderEvent, ctx EventParseContext) ([]AgentEvent, error) `json:"-"`

	// 生命周期、权限和工具注入策略都在 runtime 边界收口。
	SessionPolicy SessionPolicy `json:"session_policy"`
	CancelPolicy CancelPolicy `json:"cancel_policy"`
	PermissionMapping PermissionMapping `json:"permission_mapping"`
	ToolInjectionPolicy *ToolInjectionPolicy `json:"tool_injection_policy,omitempty"`
}
```

关键约束：

```text
1. build_args 只把 AgentRunParams / RuntimeContext 翻译成 argv/env/stdin 策略，不拥有 task 状态。
2. parse_event 只做 provider raw event -> AgentEvent 的映射，并保留 raw。
3. detect / auth / capability / model probe 可以按版本和 --help 结果动态决定能力。
4. session_policy 明确 create/resume/load 的差异，避免把 create-only flags 传给 resume。
5. permission_mapping 只能表达 provider 参数映射，最终是否允许 bypass/trust/yolo 由 PermissionPolicy 决定。
```

命令构建输入：

```go
type AgentCommandBuildInput struct {
	// Params 是 Control Plane 传入的稳定运行意图。
	Params AgentRunParams `json:"params"`
	// Detection 是 detect/version/auth 结果，供 build args 做兼容判断。
	Detection AgentDetection `json:"detection"`
	// Capabilities 是已探测出的 provider 能力。
	Capabilities AgentCapabilities `json:"capabilities"`
	// RuntimeContext 是本次运行的 cwd、权限目录和 session 上下文。
	RuntimeContext AgentRuntimeContext `json:"runtime_context"`
}

type AgentRuntimeContext struct {
	// CWD 是 agent 进程启动目录，通常是 attempt worktree。
	CWD string `json:"cwd"`
	// AllowedDirs 是 wrapper 允许 provider 读取/写入的目录边界。
	AllowedDirs []string `json:"allowed_dirs"`
	// PromptFile 是大 prompt 的临时文件路径，避免塞进 argv。
	PromptFile string `json:"prompt_file,omitempty"`
	// ResumeSessionID 是恢复已有 provider session 的句柄。
	ResumeSessionID string `json:"resume_session_id,omitempty"`
	// NewSessionID 是 runtime 生成的新 session 标识。
	NewSessionID string `json:"new_session_id,omitempty"`
	// EnvOverrides 是本次运行允许注入的环境变量白名单。
	EnvOverrides map[string]string `json:"env_overrides,omitempty"`
}

type AgentCommand struct {
	// ExecutablePath 是最终执行的 CLI 或 stdio runtime 路径。
	ExecutablePath string `json:"executable_path"`
	// Args 是 provider-specific 参数，只在 adapter 内生成。
	Args []string `json:"args"`
	// Env 是最小环境变量集合，不隐式泄漏全局环境。
	Env map[string]string `json:"env,omitempty"`
	// CWD 是进程工作目录。
	CWD string `json:"cwd"`
	// Stdin 描述 stdin_text / jsonl / json_rpc 等输入计划。
	Stdin *StdinPlan `json:"stdin,omitempty"`
}
```

Prompt transport 参考 Open Design 的 stdin / JSONL / prompt file 策略：

```go
type PromptTransportKind string

const (
	PromptTransportStdinText PromptTransportKind = "stdin_text"
	PromptTransportStdinJSONL PromptTransportKind = "stdin_jsonl"
	PromptTransportPromptFile PromptTransportKind = "prompt_file"
	PromptTransportArgvSmallOnly PromptTransportKind = "argv_small_only"
	PromptTransportJSONRPC PromptTransportKind = "json_rpc"
)

type PromptTransport struct {
	// Kind 决定 prompt 的传输方式；大 prompt 优先走 stdin 或文件。
	Kind PromptTransportKind `json:"kind"`
	// MessageShape 只在 JSONL 模式下使用。
	MessageShape string `json:"message_shape,omitempty"`
	// MaxBytes 只在 argv_small_only 模式下使用，超过必须失败或切换策略。
	MaxBytes int `json:"max_bytes,omitempty"`
}
```

Session / cancel 也作为显式接口，而不是藏在 provider flags 里：

```go
type SessionPolicy struct {
	// Mode 区分不支持 session、runtime 生成 session、provider 回传 session、ACP load 等模式。
	Mode SessionMode `json:"mode"`
	// CreateArg 是创建新 session 时使用的 provider 参数。
	CreateArg string `json:"create_arg,omitempty"`
	// ResumeArg 是恢复 session 时使用的 provider 参数。
	ResumeArg string `json:"resume_arg,omitempty"`
	// LoadMethod 是 JSON-RPC/ACP 类 runtime 的 session 加载方法名。
	LoadMethod string `json:"load_method,omitempty"`
	// CaptureFromEvent 表示 session id 需要从 provider event 中捕获。
	CaptureFromEvent bool `json:"capture_from_event,omitempty"`
}

type CancelPolicy struct {
	// PreferAdapterCancel 表示优先使用 provider 原生 cancel。
	PreferAdapterCancel bool `json:"prefer_adapter_cancel"`
	// CloseStdin 表示 cancel 时先关闭 stdin，让 provider 自行收尾。
	CloseStdin bool `json:"close_stdin"`
	// TerminateSignal 是平台相关的温和终止信号，例如 SIGTERM / CTRL_BREAK。
	TerminateSignal string `json:"terminate_signal,omitempty"`
	// GraceMS 是温和终止后的等待时间。
	GraceMS int `json:"grace_ms"`
	// ForceKill 表示超时后是否允许强杀进程树。
	ForceKill bool `json:"force_kill"`
}
```

---

## 5.3 AgentRegistry / AgentRunService

`AgentRegistry` 对应 Open Design 的 runtime registry：集中注册 provider，并只向上暴露稳定 `AgentInfo`。

```go
type AgentRegistry interface {
	// List 返回 UI / Scheduler 可见的 agent 摘要，不暴露 provider flags。
	List(ctx context.Context) ([]AgentInfo, error)
	// Get 按 provider_id 取声明式 provider spec。
	Get(ctx context.Context, providerID string) (*AgentProviderSpec, error)
	// Resolve 根据角色、能力和策略返回候选 provider。
	Resolve(ctx context.Context, requirement AgentRequirement) ([]AgentProviderSpec, error)
}

type AgentInfo struct {
	// ID 是 runtime 层 provider 标识。
	ID string `json:"id"`
	// DisplayName 是 UI 展示名称。
	DisplayName string `json:"display_name"`
	// Provider 是底层 agent 类型。
	Provider ProviderKind `json:"provider"`
	// Available 表示本机是否探测到可用 executable 和基础配置。
	Available bool `json:"available"`
	// AuthState 表示认证是否可用；unknown 不应静默当作 ok。
	AuthState AuthState `json:"auth_state"`
	ExecutablePath string `json:"executable_path,omitempty"`
	Version string `json:"version,omitempty"`
	Diagnostics []string `json:"diagnostics,omitempty"`
	Capabilities AgentCapabilities `json:"capabilities"`
	Models []AgentModelInfo `json:"models,omitempty"`
	DocsURL string `json:"docs_url,omitempty"`
}
```

`AgentRunService` 对应 Open Design 的 centralized runner：它消费 `AgentProviderSpec`，而不是让每个 provider 自己管理完整 run lifecycle。

```go
type AgentRunService interface {
	// Invoke 启动一次 agent run；事件通过只读 channel 流式返回。
	Invoke(ctx context.Context, providerID string, params AgentRunParams) (<-chan AgentEvent, error)
	// Resume 恢复已有 run/session；不支持时返回明确错误。
	Resume(ctx context.Context, runID string, message string) (<-chan AgentEvent, error)
	// Cancel 请求取消 run；实现必须记录 cancel 事件并清理进程资源。
	Cancel(ctx context.Context, runID string, reason string) error
	// GetRun 返回 runtime 维护的 run state 投影。
	GetRun(ctx context.Context, runID string) (*AgentRunState, error)
}

type AgentRunState struct {
	// RunID 是 runtime run 的唯一标识。
	RunID string `json:"run_id"`
	ProviderID string `json:"provider_id"`
	// Status 是 run lifecycle 状态，不等于 task 状态。
	Status AgentRunStatus `json:"status"`
	PID int `json:"pid,omitempty"`
	// SessionHandle 保存 provider session 的恢复信息。
	SessionHandle *AgentSessionHandle `json:"session_handle,omitempty"`
	CWD string `json:"cwd"`
	EventLogRef string `json:"event_log_ref,omitempty"`
	ArtifactRefs []string `json:"artifact_refs"`
	StartedAt *time.Time `json:"started_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
}
```

`AgentRunService` 负责：

```text
- 根据 AgentProviderSpec.build_args 生成 AgentCommand。
- 统一 spawn 进程或 stdio JSON-RPC 子 runtime。
- 按 PromptTransport 写 stdin / JSONL / prompt file。
- 调用 provider parser 生成 AgentEvent，并写入 Event Log。
- 维护 session_handle、pid、process group 和 run state。
- 按 CancelPolicy 做 adapter cancel -> close stdin -> terminate -> force kill。
- 汇总 AgentResult，但不直接修改 Task Graph。
```

---

## 5.4 AgentAdapter

`AgentAdapter` 是不同 CLI agent 的概念归一化边界。实际实现优先由 `AgentProviderSpec` 声明 provider 差异，再由统一 `AgentRunService` 执行。

```go
type AgentAdapter interface {
	// ID 返回 adapter 稳定标识。
	ID() string
	// DisplayName 返回 UI 展示名称。
	DisplayName() string
	// Provider 返回底层 agent 类型。
	Provider() ProviderKind

	// Detect 探测 CLI 是否安装、版本和认证状态。
	Detect(ctx context.Context) (*AgentDetection, error)
	// Capabilities 返回 adapter 已知能力。
	Capabilities(ctx context.Context) (AgentCapabilities, error)

	// Run 启动 provider 并流式返回统一 AgentEvent。
	Run(ctx context.Context, params AgentRunParams) (<-chan AgentEvent, error)
	// Cancel 取消正在运行的 provider session / process。
	Cancel(ctx context.Context, runID string) error
	// Resume 可选；不支持时返回明确错误，而不是静默新开 session。
	Resume(ctx context.Context, runID string, message string) (<-chan AgentEvent, error)
}
```

adapter / provider spec 负责：

```text
- 找到 CLI executable。
- 判断版本和认证状态。
- 构造 provider-specific spawn 命令。
- 决定 prompt 通过 argv、stdin、JSON-RPC 还是 HTTP 传递。
- 设置 cwd、env、权限、工具和可读目录。
- 解析 stdout / stderr / JSONL / JSON-RPC / SSE。
- 把 provider 原始事件映射成统一 AgentEvent。
```

---

## 5.5 AgentDetection

CLI 存在不代表可以无头运行，因此 detection 需要记录认证和配置状态。

```go
type AgentDetection struct {
	// Provider 是被探测的底层 agent 类型。
	Provider ProviderKind `json:"provider"`
	// ExecutablePath 是实际命中的可执行文件路径。
	ExecutablePath string `json:"executable_path"`
	// Version 是 provider 版本文本或规范化版本号。
	Version string `json:"version"`
	// ConfigDir 是 provider 本地配置目录。
	ConfigDir string `json:"config_dir,omitempty"`
	// NativeSkillsDir 是 provider 原生 skill/prompt 目录。
	NativeSkillsDir string `json:"native_skills_dir,omitempty"`
	// AuthState 是认证状态。
	AuthState AuthState `json:"auth_state"`
	// InstallHint 是不可用时给 UI 展示的安装/修复提示。
	InstallHint string `json:"install_hint,omitempty"`
	// Error 是探测失败的诊断文本。
	Error string `json:"error,omitempty"`
}
```

检测结果用于：

```text
- 判断 adapter 是否可调度。
- 在 UI 中提示缺少 CLI、缺少认证或版本不支持。
- 决定是否允许 fallback 到其他 adapter。
```

---

## 5.6 AgentCapabilities

Capability 不描述所有 CLI flags，只描述调度和上层产品需要知道的能力。

```go
type AgentCapabilities struct {
	// SupportsHeadless 表示是否可无交互运行。
	SupportsHeadless bool `json:"supports_headless"`
	// SupportsStreaming 表示是否能实时输出事件。
	SupportsStreaming bool `json:"supports_streaming"`
	// SupportsStructuredOutput 表示是否能约束结构化输出。
	SupportsStructuredOutput bool `json:"supports_structured_output"`
	SupportsToolCalling bool `json:"supports_tool_calling"`
	SupportsFileEdit bool `json:"supports_file_edit"`
	SupportsShell bool `json:"supports_shell"`
	SupportsMCP bool `json:"supports_mcp"`

	// Worktree/CWD/Git 隔离优先使用 CLI 自身能力；不支持时由 wrapper 兜底。
	SupportsGitWorktree bool `json:"supports_git_worktree"`
	SupportsAdditionalDirectories bool `json:"supports_additional_directories"`

	SupportsResume bool `json:"supports_resume"`
	SupportsNativeSkillLoading bool `json:"supports_native_skill_loading"`
	SupportsSurgicalEdit bool `json:"supports_surgical_edit"`

	// PermissionMode 是 provider 原生权限能力，不代表 runtime 一定允许使用。
	PermissionMode PermissionMode `json:"permission_mode"`
	ContextWindowHint int `json:"context_window_hint,omitempty"`
	CostModel *CostModel `json:"cost_model,omitempty"`
	// DefaultRoles 表示该 provider 默认适合承担哪些 agent 角色。
	DefaultRoles []AgentRole `json:"default_roles"`
}
```

调度使用 capability 做硬约束：

```text
- 需要改文件的 execute task 不能调度到不支持 file_edit 的 adapter。
- 需要运行测试的 verifier 不能调度到不支持 shell 的 adapter。
- 需要实时 UI 的运行优先选择 supports_streaming 的 adapter。
- skill 要求 native skill loading 时，不支持的 adapter 必须改用 prompt injection 或被排除。
```

---

## 5.7 AgentRunParams

`AgentRunParams` 是 adapter 的稳定输入。它表达运行意图，不暴露 provider-specific flags。

```go
type AgentRunParams struct {
	// RunID 是 runtime run id；InvocationID 是上层 task phase invocation id。
	RunID string `json:"run_id"`
	InvocationID string `json:"invocation_id"`

	// CWD 是 agent 运行目录；WorktreeID 关联 Workspace/Merge 模块。
	CWD string `json:"cwd"`
	WorktreeID string `json:"worktree_id,omitempty"`

	// Role / Phase 表达调度意图，不暴露 provider-specific flags。
	Role AgentRole `json:"role"`
	Phase AgentPhase `json:"phase"`

	// SystemPrompt / UserPrompt 是 runtime 注入 provider 的最终提示词。
	SystemPrompt string `json:"system_prompt"`
	UserPrompt string `json:"user_prompt"`

	// ContextPackDir 是 Ctx Agent 生成的只读上下文包目录。
	ContextPackDir string `json:"context_pack_dir,omitempty"`
	// SkillDir 是本次可注入 skill 的目录。
	SkillDir string `json:"skill_dir,omitempty"`

	AllowedTools []ToolCapability `json:"allowed_tools,omitempty"`
	TimeoutMS int `json:"timeout_ms,omitempty"`
	BudgetLimit *BudgetLimit `json:"budget_limit,omitempty"`
	// OutputSchema 是 verifier/planner 等结构化输出的 JSON Schema。
	OutputSchema *JSONSchema `json:"output_schema,omitempty"`
	// Metadata 绑定 task graph provenance。
	Metadata AgentRunMetadata `json:"metadata"`
}

type AgentRunMetadata struct {
	TaskID string `json:"task_id"`
	AttemptID string `json:"attempt_id"`
	RequirementRefs []string `json:"requirement_refs"`
}
```

provider-specific 设置留在 adapter 内部或 adapter config 中，例如 Claude Code adapter 可以内部选择：

```text
claude -p --output-format stream-json --verbose --permission-mode <mode>
```

而 Codex / Gemini adapter 可以选择自己的 headless 命令、stdin 策略和 stream parser。

---

## 5.8 AgentEvent

`AgentEvent` 是 Runtime 向 Event Log、UI 和 projection 暴露的统一流式事件。

```go
type AgentEventKind string

const (
	AgentEventThinking AgentEventKind = "thinking"
	AgentEventTextDelta AgentEventKind = "text_delta"
	AgentEventToolCall AgentEventKind = "tool_call"
	AgentEventToolResult AgentEventKind = "tool_result"
	AgentEventFileWrite AgentEventKind = "file_write"
	AgentEventError AgentEventKind = "error"
	AgentEventDone AgentEventKind = "done"
)

// AgentEvent 是 Go 后端持久化和推送给 Electron/React UI 的统一事件 envelope。
type AgentEvent interface {
	// Kind 返回事件类型，便于 Event Log、投影和前端渲染分发。
	Kind() AgentEventKind
	// RunID 返回所属 runtime run。
	RunID() string
}
```

### AgentThinkingEvent

```go
type AgentThinkingEvent struct {
	// Type 固定为 "thinking"，表示模型思考或中间推理摘要。
	Type AgentEventKind `json:"type"`
	RunIDValue string `json:"run_id"`
	Text string `json:"text"`
	// Raw 保留 provider 原始事件，便于审计和 parser 修正。
	Raw any `json:"raw,omitempty"`
}
```

### AgentTextDeltaEvent

```go
type AgentTextDeltaEvent struct {
	// Type 固定为 "text_delta"，表示可展示文本增量。
	Type AgentEventKind `json:"type"`
	RunIDValue string `json:"run_id"`
	Text string `json:"text"`
	Raw any `json:"raw,omitempty"`
}
```

### AgentToolCallEvent

```go
type AgentToolCallEvent struct {
	// Type 固定为 "tool_call"，表示 agent 请求调用工具。
	Type AgentEventKind `json:"type"`
	RunIDValue string `json:"run_id"`
	// ToolCallID 用于把 call 与 result 关联起来。
	ToolCallID string `json:"tool_call_id,omitempty"`
	Name string `json:"name"`
	Input any `json:"input,omitempty"`
	Raw any `json:"raw,omitempty"`
}
```

### AgentToolResultEvent

```go
type AgentToolResultEvent struct {
	// Type 固定为 "tool_result"，表示工具调用结果。
	Type AgentEventKind `json:"type"`
	RunIDValue string `json:"run_id"`
	ToolCallID string `json:"tool_call_id,omitempty"`
	Output any `json:"output,omitempty"`
	// IsError 表示工具执行失败，但不一定代表整个 run 失败。
	IsError bool `json:"is_error,omitempty"`
	Raw any `json:"raw,omitempty"`
}
```

### AgentFileWriteEvent

如果 provider 没有原生 file write event，Runtime 可以通过 write-set 观察合成该事件。

```go
type AgentFileWriteEvent struct {
	// Type 固定为 "file_write"；provider 不支持时由 runtime write-set 观察合成。
	Type AgentEventKind `json:"type"`
	RunIDValue string `json:"run_id"`
	Path string `json:"path"`
	Operation FileOperation `json:"operation,omitempty"`
	Raw any `json:"raw,omitempty"`
}
```

### AgentErrorEvent

```go
type AgentErrorEvent struct {
	// Type 固定为 "error"，表示 provider/runtime 层错误。
	Type AgentEventKind `json:"type"`
	RunIDValue string `json:"run_id"`
	Message string `json:"message"`
	Raw any `json:"raw,omitempty"`
}
```

### AgentDoneEvent

```go
type AgentDoneEvent struct {
	// Type 固定为 "done"，表示 run 事件流结束。
	Type AgentEventKind `json:"type"`
	RunIDValue string `json:"run_id"`
	Reason AgentDoneReason `json:"reason"`
	Raw any `json:"raw,omitempty"`
}
```

原则：

```text
1. 上层只依赖统一 AgentEvent。
2. provider 原始事件必须保留 raw，便于审计和后续 parser 修正。
3. 大输出不直接塞进 event，进入 Artifact Store，用 ref 关联。
4. Event Log 由 Runtime 自动记录，agent 不显式写日志。
```

---

## 5.9 AgentResult

`AgentResult` 是一次 invocation 的最终汇总。

```go
type AgentResult struct {
	// InvocationID / RunID 连接 task phase invocation 与 runtime run。
	InvocationID string `json:"invocation_id"`
	RunID string `json:"run_id"`
	TaskID string `json:"task_id"`
	AttemptID string `json:"attempt_id"`
	Phase AgentPhase `json:"phase"`

	// Status 是本次 invocation 的业务汇总状态。
	Status AgentResultStatus `json:"status"`
	Summary string `json:"summary"`
	// StructuredOutput 保存通过 output schema 解析出的结构化结果。
	StructuredOutput any `json:"structured_output,omitempty"`

	// TouchedFilesDeclared 是 agent 自报的修改范围。
	TouchedFilesDeclared []string `json:"touched_files_declared"`
	// TouchedFilesObserved 是 Runtime 从 git/write-set 观察到的真实修改范围。
	TouchedFilesObserved []string `json:"touched_files_observed"`

	// SubmittedRequirementRefs 是 agent 提交给 Task Manager 的 requirement；agent 不直接写 task/edge。
	SubmittedRequirementRefs []string `json:"submitted_requirement_refs"`
	ContextQueries []string `json:"context_queries"`
	ArtifactRefs []string `json:"artifact_refs"`
	EventRefs []string `json:"event_refs"`
	Usage *AgentUsage `json:"usage,omitempty"`
}

type AgentUsage struct {
	DurationMS int `json:"duration_ms,omitempty"`
	TokenUsage any `json:"token_usage,omitempty"`
	CostUSD float64 `json:"cost_usd,omitempty"`
}
```

`expanded_task_graph` 的含义不是 agent 直接写了 task graph，而是它提交了 requirement，Task Manager Agent 已经或将要把它编排成 task graph 变更。

---

## 6. Runtime 执行流程

一次 task phase attempt 的执行流程：

```text
1. Scheduler 选择可运行 task phase 和角色。
2. Runtime 根据角色和 capability 选择 adapter。
3. Runtime 准备 cwd/worktree、context pack、task contract 和 output schema。
4. Runtime 生成 system prompt 与 user prompt。
5. Runtime 调用 AgentRunService.invoke(provider_id, params)。
6. AgentRunService 根据 AgentProviderSpec 启动 CLI/stdio runtime，并解析输出为 AgentEvent。
7. Runtime 自动写入 Event Log，并把大对象写入 Artifact Store。
8. Runtime 观察 worktree diff 和 touched files。
9. Runtime 汇总 AgentResult。
10. Control Plane 路由 AgentResult：Task Manager Agent 负责 Task Graph 写入，Verifier 负责验收，Merge Queue 负责合并。
```

流程示意：

```text
Task Contract
  -> Context Pack
  -> Workspace Binding
  -> AgentRunParams
  -> AgentRunService.invoke()
  -> AgentEvent stream
  -> Event Log / Artifact Store
  -> Observed Write Set
  -> AgentResult
```

---

## 7. Workspace / Worktree / Git

第一阶段不单独实现一套与 agent 无关的 worktree 系统。worktree、git、cwd、可写范围和 tool 权限先作为 CLI wrapper 能力包装。

基本原则：

```text
1. 每个 task attempt 在独立 cwd/worktree 中执行。
2. 优先复用 CLI agent 自身的 worktree / cwd / git 能力。
3. 如果 CLI 不支持 worktree，则由 Runtime 用 git worktree 或独立 clone 兜底。
4. agent 不能直接修改 main branch。
5. Runtime 观察实际 diff，并将结果交给 Verify / Merge Queue。
6. Merge Queue 才能把 verify passed 的结果合入 main。
```

Workspace 绑定：

```go
type WorkspaceBinding struct {
	// WorktreeID 关联 Workspace/Merge 模块中的隔离工作区。
	WorktreeID string `json:"worktree_id"`
	// CWD 是实际传给 CLI agent 的执行目录。
	CWD string `json:"cwd"`
	// BaseRef 是 attempt 起始 commit / ref。
	BaseRef string `json:"base_ref"`
	// BranchName 是可选的 git 分支名。
	BranchName string `json:"branch_name,omitempty"`
	// WritableRoots 是 agent 被允许写入的目录白名单。
	WritableRoots []string `json:"writable_roots"`
	// ReadableRoots 是 agent 被允许读取的目录白名单。
	ReadableRoots []string `json:"readable_roots"`
}
```

观察结果：

```go
type ObservedWriteSet struct {
	// WorktreeID 标识被观察的隔离工作区。
	WorktreeID string `json:"worktree_id"`
	// ChangedFiles / CreatedFiles / DeletedFiles 来自 git diff 或文件系统观察。
	ChangedFiles []string `json:"changed_files"`
	CreatedFiles []string `json:"created_files"`
	DeletedFiles []string `json:"deleted_files"`
	// DiffArtifactRef 指向 Artifact Store 中的大 diff/patch。
	DiffArtifactRef string `json:"diff_artifact_ref,omitempty"`
}
```

---

## 8. Prompt / Context / Skill 注入

Runtime 不依赖 agent session memory。每次 invocation 都显式注入必要上下文。

上下文层次：

```text
1. Runtime policy / role boundary
2. Task contract
3. Context pack from Ctx Agent
4. Approved plan 或 acceptance criteria
5. Skill / workflow instruction（可选）
6. User prompt / phase-specific instruction
7. Output schema
```

Skill 注入支持多种模式：

```go
type SkillInjectionMode string

const (
	// SkillInjectionNative 表示安装或 symlink 到 agent 原生 skill 目录。
	SkillInjectionNative SkillInjectionMode = "native"
	// SkillInjectionPrompt 表示将 SKILL.md / references inline 到 prompt。
	SkillInjectionPrompt SkillInjectionMode = "prompt"
	// SkillInjectionProjectFile 表示写入 .cursorrules 等 agent-specific 项目文件。
	SkillInjectionProjectFile SkillInjectionMode = "project_file"
	// SkillInjectionUnsupported 表示该 provider 不支持 skill 注入。
	SkillInjectionUnsupported SkillInjectionMode = "unsupported"
)
```

选择规则：

```text
- adapter 支持 nativeSkillLoading 时，优先 native。
- 不支持 native 时，使用 prompt injection。
- 需要 agent-specific 规则文件时，使用 project_file。
- skill 要求的 capability 不满足时，不能调度或必须降级为受限模式。
```

---

## 9. Tool / Permission Policy

Runtime 用统一策略表达权限意图，adapter 负责翻译成具体 CLI flags。

```go
type ToolCapability struct {
	// Name 是工具能力名，例如 shell、file_edit、mcp:server。
	Name string `json:"name"`
	// Matcher 是可选匹配规则，用于限制命令、路径或 MCP tool 名称。
	Matcher string `json:"matcher,omitempty"`
	// Mode 明确允许或拒绝；默认值不应被当作 allow。
	Mode ToolCapabilityMode `json:"mode"`
}
```

```go
type PermissionPolicy struct {
	// Mode 是 runtime 允许的权限等级；危险 provider flag 必须经过这里收口。
	Mode PermissionMode `json:"mode"`
	// RequireHumanApprovalForHighRisk 表示 shell、跨目录写入、credential 访问等高风险动作需要人工批准。
	RequireHumanApprovalForHighRisk bool `json:"require_human_approval_for_high_risk"`
}
```

原则：

```text
1. planner 默认不能改代码。
2. executor 只能在 approved scope 内改代码。
3. verifier 默认不修改实现，只运行检查和报告结果。
4. 高风险操作必须显式人工批准。
5. `dont_ask` / `bypass` 只允许在明确授权的本地调试或受控执行中使用，并且不能覆盖高风险人工批准要求。
6. agent-generated edits 默认作为 pending changes / worktree diff，不直接进入 main。
```

---

## 10. Claude Code MVP Adapter

第一阶段 Claude Code adapter 是 reference adapter。

必须支持：

```text
- detect: claude 是否存在、版本、auth 状态。
- capabilities: headless、streaming、tool calling、file edit、shell、MCP、worktree。
- run: 使用 print/headless 模式启动。
- parse: 将 stream-json / JSONL 转成统一 AgentEvent。
- cancel: 终止进程。
- result: 汇总 final result、usage、cost、session id 和 touched files。
```

建议内部命令形态：

```text
claude -p
  --output-format stream-json
  --verbose
  --permission-mode <mode>
  --allowedTools / --disallowedTools ...
  --max-turns <n>
```

Claude Code 原始事件映射：

```text
system/init           -> Event Log 的 AgentStarted / metadata
assistant text        -> text_delta
assistant tool_use    -> tool_call
user tool_result      -> tool_result
result                -> done + AgentResult usage/status
hook events           -> raw event 或后续扩展事件
partial messages      -> text_delta 或 raw event
```

MVP 不要求完整支持 Claude Code 的全部 flags，但必须保留 raw 事件，避免后续无法补 schema。

---

## 11. Fallback 策略

Fallback 用于 adapter 不可用、认证失效、运行失败或 capability 不满足时。

```go
type FallbackPolicy struct {
	// AllowFallback 表示当前 provider 不可用时是否允许切换候选 provider。
	AllowFallback bool `json:"allow_fallback"`
	// RequireExplicitSwitch 表示 fallback 必须产生显式事件，不能静默发生。
	RequireExplicitSwitch bool `json:"require_explicit_switch"`
	// Candidates 是按优先级排列的候选 provider id。
	Candidates []string `json:"candidates"`
}
```

原则：

```text
1. 不静默切换 provider。
2. fallback 必须记录到 Event Log。
3. 如果任务依赖某个 provider 的特定能力，不能 fallback 到不支持该能力的 provider。
4. fallback 后的 result 必须标注实际执行 adapter。
```

---

## 12. 与其他模块关系

### Task Graph

```text
Task Graph 提供 task contract、phase、role、acceptance criteria 和状态。
Agent Runtime 返回 AgentResult 和 event refs。
Control Plane 只能路由结果；Task Graph 的状态、edge、blocker 写入仍由经 Agent Runtime 启动并授予 graph_write tool 的 Task Manager Agent 负责。
```

### Ctx Agent / Context Lib

```text
Ctx Manager Agent / Ctx Agent 也通过 Agent Runtime 运行，为 invocation 选择 context pack。
Agent Runtime 只消费 context pack，不直接读写 ctxlib。
agent 运行中需要更多上下文时，通过受控 ctx query 进入经 Agent Runtime 授权的 Ctx Manager Agent。
```

### Event Log / Artifact Store

```text
Runtime 自动记录 AgentStarted、AgentEvent、AgentFinished、AgentFailed、WorktreeDiffObserved 等事件。
transcript、stdout/stderr、大 tool output、diff patch、test output 进入 Artifact Store。
```

### Verify / Merge Queue

```text
Verifier 消费 AgentResult、ObservedWriteSet、test output 和 acceptance criteria。
Merge Queue 只合并 verify passed 的 worktree diff；Agent Runtime 不执行 main branch merge。
```

---

## 13. 不变量

```text
1. Agent Runtime 是 CLI agent 的唯一启动入口。
2. Scheduler / Task Graph 不依赖 provider-specific flags。
3. 普通 worker agent 不直接写 Task Graph；只能提交 requirement。
4. 只有经 Agent Runtime 授权的 Task Manager Agent 可以通过 graph_write service/tool 写 Task Graph。
5. 普通 worker agent 不直接写 ctxlib；ctxlib 只从 Event Log / Artifact Store 提炼。
6. 只有经 Agent Runtime 授权的 Ctx Manager Agent 可以通过 ctx service/tool 写 ctxlib。
7. agent 不直接修改 main branch。
8. 每次 invocation 必须有 workspace/cwd 边界、role 边界和 tool/permission 边界。
9. 每次 invocation 必须自动进入 Event Log。
10. 大对象必须进入 Artifact Store，Event Log 只保存 ref。
11. observed write set 以 Runtime 观察为准，agent 声明只能作为参考。
12. verifier 不能自我批准同一 active context 的执行结果。
13. fallback 不能静默发生。
14. 第一阶段以 Claude Code adapter 跑通最小闭环，再扩展 Codex / Gemini / custom。
```

---

## 14. MVP 范围

MVP 需要完成：

```text
1. AgentAdapter 接口。
2. Claude Code adapter detect / capabilities / run / cancel。
3. AgentRunParams、AgentEvent、AgentResult 数据结构。
4. stream-json 到 AgentEvent 的 parser。
5. workspace/cwd 绑定和 observed write set。
6. Event Log / Artifact Store 写入 refs。
7. role-based tool / permission policy。
8. plan / execute / verify 三类 role 的 prompt boundary。
9. AgentResult 作为 Task Manager Agent 推进 Task Graph 状态的输入证据。
```

MVP 可以暂缓：

```text
1. 完整 Codex / Gemini adapter。
2. 复杂 resume。
3. 多 provider 自动 fallback。
4. 完整 MCP server 编排。
5. UI 级实时细节展示。
6. 高级 skill/plugin marketplace。
```
