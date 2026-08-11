[English](README.md) | [中文](README.zh.md)

<div align="center">

# Threadmill

**A control plane that turns multi-agent work into an auditable, recoverable delivery chain.**

[![Go](https://img.shields.io/badge/Go-1.23.3-00ADD8?logo=go&logoColor=white)](https://github.com/KDZZZZZZ/threadmill-AgentTeams/tree/docs/readme-oops-dev-main)
[![React](https://img.shields.io/badge/React-19-149ECA?logo=react&logoColor=white)](https://github.com/KDZZZZZZ/threadmill-AgentTeams/tree/docs/readme-oops-dev-main/web)
[![Status](https://img.shields.io/badge/status-alpha-f0b429)](#status)

requirement → plan → execute → verify → merge → done

</div>

> [!NOTE]
> This README is the main-branch overview of the runnable implementation developed from oops-dev and preserved in the [preview snapshot](https://github.com/KDZZZZZZ/threadmill-AgentTeams/tree/docs/readme-oops-dev-main). The main branch is documentation-first; use the preview snapshot for the local console and Go commands below.

## The idea

Multi-agent development is easy to start and hard to govern. A session can disappear, a verifier can inspect the wrong workspace, a stop request can lose its checkpoint, and a successful result can still have no safe path into main.

Threadmill changes the unit of work from a chat session to a durable **Task**. A Task owns its contract, dependency graph, phase lifecycle, context boundary, evidence, and delivery policy. Agents are temporary compute resources that operate inside those boundaries.

The outcome is a control plane for the moments that usually disappear into chat:

- a human intent becomes a durable Requirement and Task Contract;
- plan → execute → verify is represented as an explicit, recoverable lifecycle;
- context is scoped to the current Invocation instead of leaking through a long-lived session;
- code reaches main only after verification, latest-main validation, and the Merge Queue.

## What Threadmill owns

| Boundary | Threadmill's authority |
| --- | --- |
| Work identity | A durable Task, not an Agent session |
| Coordination | One persistent Coordination Graph; Task Manager is its only external writer |
| Execution | Invocation envelopes with role, phase, workspace, context, capabilities, lease, and evidence |
| Context | Subscription-aware Context Slices and reviewable Context Deltas |
| Recovery | Checkpoints, explicit non_resumable outcomes, and new generations for retries |
| Delivery | Verify → latest-main targeted verify → Merge Queue → main |
| Browser | A filtered projection plus four bounded writes: requirement, capacity, human decision, and Manager message |

The AgentTeams codebase remains an execution host behind an adapter. Threadmill owns the project semantics, authorization boundaries, graph revisions, evidence, and merge policy.

## Architecture at a glance

Two durable graphs hold business meaning. Runtime, Workspace, and the provider adapter are execution boundaries around them.

~~~mermaid
flowchart TB
  human["Human operator"]
  ui["React operator console"]

  subgraph surface["Policy-filtered surface"]
    http["HTTP / JSON commands"]
    projection["Permission-filtered UI projection"]
    sse["Cursor SSE<br/>snapshot + resume"]
  end

  subgraph core["Threadmill control plane"]
    manager["Task Manager<br/>sole Coordination Graph writer"]
    coordination[("Coordination Graph<br/>tasks · dependencies · phases")]
    runtime["GraphRuntime + Agent Runtime<br/>invocation · lease · evidence"]
    context["Context Service"]
    contextGraph[("Context Graph<br/>subscriptions · slices · deltas")]
    workspace["Workspace + Merge Queue<br/>latest-main gate"]
  end

  subgraph substrate["Durable and external substrate"]
    database[("PostgreSQL<br/>state · events · outbox")]
    artifacts[("MinIO / S3<br/>large artifacts")]
    provider["AgentTeams / QwenPaw<br/>execution host"]
  end

  human --> ui
  ui -->|"bounded commands"| http
  sse -->|"live projection"| ui
  http --> manager
  manager -->|"DecisionRef + revision"| coordination
  coordination -->|"PhaseCommand"| runtime
  runtime -->|"scoped Invocation"| provider
  runtime --> context
  context -->|"curation"| contextGraph
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

### The two graphs

- **Coordination Graph** answers “what may run next, and what blocks it?” It stores durable Tasks, phase endpoints, dependencies, blockers, revisions, and result references. GraphRuntime consumes it; it is not a public browser or Agent API.
- **Context Graph** answers “what durable knowledge may this Invocation see?” Valid subscriptions are materialized into a permission-filtered Context Slice. New information arrives as Deltas; Task memory candidates remain separate until review.

The phase-control seam is intentionally narrow:

~~~go
type PhaseController interface {
    Apply(ctx context.Context, command PhaseCommand) error
}
~~~

The call acknowledges command acceptance. Started, stopped, failed, and output events return asynchronously through the Event Log rather than mutating the graph behind the caller's back.

## From requirement to merge

The delivery path is a policy gate, not a convention. A failed verification produces evidence and a Manager decision; it never silently recycles an old result.

~~~mermaid
flowchart LR
  requirement["Requirement"] --> contract["Task Contract"]
  contract --> plan["plan"]
  plan --> execute["execute"]
  execute --> verify["verify"]
  verify --> decision{"Contract satisfied?"}
  decision -->|"No: proposal"| manager["Manager decision<br/>new graph revision"]
  manager --> execute
  decision -->|"Yes"| policy{"DeliveryPolicy"}
  policy -->|"code_merge"| latest["Latest-main<br/>targeted verify"]
  latest --> queue["Merge Queue"]
  queue --> done["done"]
  policy -->|"other delivery"| done
  hold["hold / resume"] --> evidence["Stop evidence"]
  evidence --> generation["New generation + Invocation"]
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

## What the operator sees

The browser is a projection and command surface, not a second orchestration engine. It can show capacity, graph state, Manager decisions, endpoint inspection, Context Slices, and a reconnectable event stream without receiving Agent tokens, private transcripts, raw tool output, or graph CRUD authority.

~~~mermaid
sequenceDiagram
  participant O as Operator
  participant UI as Console
  participant API as HTTP API
  participant TM as Task Manager
  participant LOG as Event Log

  O->>UI: Submit bounded intent
  UI->>API: Idempotent JSON command + revision
  API->>TM: Persist input and evaluate policy
  TM-->>API: DecisionRef + new revision
  API-->>UI: Accepted command
  LOG-->>UI: Cursor SSE projection
  UI->>API: Reconnect with Last-Event-ID
  API-->>UI: Resume or fresh snapshot
~~~

## Repository map

The design documents live on main; the implementation map below links to the preview snapshot derived from oops-dev so this README remains honest while the code is promoted.

| Area | Purpose |
| --- | --- |
| [docs/architecture.md](https://github.com/KDZZZZZZ/threadmill-AgentTeams/blob/main/docs/architecture.md) | System boundaries and dependency direction |
| [docs/task-graph.md](https://github.com/KDZZZZZZ/threadmill-AgentTeams/blob/main/docs/task-graph.md) | Durable work identity and graph semantics |
| [docs/workspace-merge.md](https://github.com/KDZZZZZZ/threadmill-AgentTeams/blob/main/docs/workspace-merge.md) | Workspace binding and Merge Queue policy |
| [docs/CONTEXT.md](https://github.com/KDZZZZZZ/threadmill-AgentTeams/blob/main/docs/CONTEXT.md) | Context and memory model |
| [cmd/threadmilld](https://github.com/KDZZZZZZ/threadmill-AgentTeams/tree/docs/readme-oops-dev-main/cmd/threadmilld) | CLI: serve, migrate, check, bootstrap-operator |
| [internal/coordination](https://github.com/KDZZZZZZ/threadmill-AgentTeams/tree/docs/readme-oops-dev-main/internal/coordination) | Coordination Graph, revisions, leases, and runtime |
| [internal/contextgraph](https://github.com/KDZZZZZZ/threadmill-AgentTeams/tree/docs/readme-oops-dev-main/internal/contextgraph) | Subscriptions, slices, deltas, and memory review |
| [internal/taskmanager](https://github.com/KDZZZZZZ/threadmill-AgentTeams/tree/docs/readme-oops-dev-main/internal/taskmanager) | Requirement intake and decision persistence |
| [internal/workspace](https://github.com/KDZZZZZZ/threadmill-AgentTeams/tree/docs/readme-oops-dev-main/internal/workspace) | Workspace Binding and repository operations |
| [internal/mergequeue](https://github.com/KDZZZZZZ/threadmill-AgentTeams/tree/docs/readme-oops-dev-main/internal/mergequeue) | Latest-main verification and main-write gate |
| [internal/uiprojection](https://github.com/KDZZZZZZ/threadmill-AgentTeams/tree/docs/readme-oops-dev-main/internal/uiprojection) | Permission-filtered snapshots and UI events |
| [web](https://github.com/KDZZZZZZ/threadmill-AgentTeams/tree/docs/readme-oops-dev-main/web) | React + TypeScript + Vite operator console |
| [api/openapi](https://github.com/KDZZZZZZ/threadmill-AgentTeams/tree/docs/readme-oops-dev-main/api/openapi) | Browser-facing contract |
| [third_party/agentteams](https://github.com/KDZZZZZZ/threadmill-AgentTeams/tree/main/third_party/agentteams) | Archived provider code; read-only for root-project work |

## Quick start: runnable preview

The following commands target the preview snapshot derived from oops-dev, not the documentation-only main snapshot:

~~~powershell
git clone https://github.com/KDZZZZZZ/threadmill-AgentTeams.git
cd threadmill-AgentTeams
git fetch origin docs/readme-oops-dev-main
git switch --track origin/docs/readme-oops-dev-main

npm --prefix web ci
npm --prefix web run build
go run ./cmd/threadmilld serve --fake --http-addr 127.0.0.1:8080 --web-dist web/dist
~~~

Open <http://127.0.0.1:8080/?project_id=demo-project> to inspect the live coordination graph, capacity controls, Manager rail, endpoint inspector, and event stream.

Useful local checks:

~~~powershell
go test -count=1 ./...
go vet ./...
npm --prefix web run typecheck
npm --prefix web run test
npm --prefix web run build
~~~

GitHub Actions are intentionally disabled for this repository. The commands above are the local verification path.

## Status

| Capability | Evidence | Status |
| --- | --- | --- |
| Durable work model | Requirements, Contracts, Tasks, Attempts, Invocations, and phase endpoints are specified in the design docs | Active design |
| Coordination and Context Graphs | Ownership, revisions, subscriptions, slices, and deltas are explicitly separated | Active design |
| Local operator console | The preview snapshot contains the fake-host, OpenAPI, SSE, projection, and React acceptance path | Preview |
| Production runtime | PostgreSQL, MinIO/S3, real provider credentials, cross-process recovery, and deployment still require environment-backed validation | Not claimed complete |

## Further reading

- [Unified design](https://github.com/KDZZZZZZ/threadmill-AgentTeams/blob/docs/readme-oops-dev-main/docs/threadmill-unified-design.md)
- [Coordination Graph](https://github.com/KDZZZZZZ/threadmill-AgentTeams/blob/docs/readme-oops-dev-main/docs/coordination-graph.md)
- [Context Graph](https://github.com/KDZZZZZZ/threadmill-AgentTeams/blob/docs/readme-oops-dev-main/docs/context-graph.md)
- [GUI and SSE](https://github.com/KDZZZZZZ/threadmill-AgentTeams/blob/docs/readme-oops-dev-main/docs/gui.md)
- [Traceability](https://github.com/KDZZZZZZ/threadmill-AgentTeams/blob/docs/readme-oops-dev-main/docs/traceability.md)
- [Architecture rationale](https://github.com/KDZZZZZZ/threadmill-AgentTeams/blob/main/docs/design-rationale.md)

## Contributing

Keep changes small and reviewable. When domain semantics change, update the design contract, public schema, implementation, and evidence together. Treat third_party/agentteams/ as archived base code unless a task explicitly targets it.

The root project does not currently declare a license file. The license under third_party/agentteams/ applies to that archived component only.
