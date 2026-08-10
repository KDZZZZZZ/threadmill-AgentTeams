# Threadmill ADR Index

This directory records implementation decisions frozen before Threadmill MVP coding. All listed decisions are `Accepted`.

## Authority Order

Use this order when documents conflict:

1. `docs/threadmill-unified-design.md`: domain objects, invariants, and end-to-end semantics.
2. Module documents such as `docs/coordination-graph.md`, `docs/context-graph.md`, `docs/task-manager-agent.md`, `docs/phase-agent.md`, `docs/scheduler-budget.md`, `docs/event-artifact-store.md`, `docs/workspace-merge.md`, and `docs/agent-runtime.md`: object fields, interface signatures, permissions, and state machines for their module.
3. `docs/threadmill-agentteams-adapter-design.md`: MVP host and Adapter boundary. It overrides older WorkerFlow or multi-runtime references.
4. `docs/agent-prompts.md` and `docs/agent-skills/*/SKILL.md`: role prompts, skill dependencies, visible tools, and output behavior.
5. `third_party/agentteams/`: evidence of existing host capabilities only; it does not define Threadmill domain semantics.

## Decisions

| ADR | Status | Decision |
| --- | --- | --- |
| [0001](./0001-modular-monolith.md) | Accepted | Threadmill MVP runs as one modular `threadmilld` process with strict package seams. |
| [0002](./0002-postgres-outbox-minio.md) | Accepted | PostgreSQL owns transactional state, Event Log, outbox, cursors, and leases; MinIO/S3 stores large artifacts. |
| [0003](./0003-mcp-capability-auth.md) | Accepted | Browser identity and Agent MCP capabilities are separate; Runtime trims tools per invocation. |
| [0004](./0004-context-physical-model.md) | Accepted | Context uses split physical tables, Task-local candidate buffers, deterministic recent-node defaults, and provenance-only `derives_from_subgraph`. |
| [0005](./0005-agentteams-mvp.md) | Accepted | MVP provider is QwenPaw + TeamHarness taskflow through an internal Agent Runtime Adapter; WorkerFlow is excluded. |
| [0006](./0006-web-ui-projection-and-sse.md) | Accepted | GUI writes only idempotent HTTP commands and reads rebuildable UI Projection via snapshot/SSE. |
