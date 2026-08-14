# ADR 0003: MCP Capability and Browser Identity Boundaries

Status: Accepted

Date: 2026-08-10

## Context

Threadmill has two transport classes: browser/user HTTP and Agent MCP. They have different identities and authority. Browser users submit requirements, decisions, capacity changes, and Manager messages. Agents receive role-specific tools through Runtime-bound MCP capabilities.

Authoritative sources:

- `docs/plans/2026-08-10-threadmill-repository-implementation.md` §3, §4.2
- `docs/task-manager-agent.md`
- `docs/agent-runtime.md` §5
- `docs/threadmill-agentteams-adapter-design.md` §4, §9

## Decision

Browser sessions use same-origin secure HttpOnly session cookies or trusted reverse-proxy identity. SSE uses the same browser identity and project ACL as ordinary UI API requests. Bearer tokens are not placed in URLs.

Agent calls use invocation-scoped opaque tokens. Runtime derives visible MCP tools from:

1. role tools,
2. skill tools,
3. available tools,

then takes the intersection allowed by the current invocation, capability, project, Task, endpoint, generation, lease, workspace, budget, and ACL.

Stable audit identity is `ActorPrincipalID`. AgentTeams worker name, model session, taskflow ID, and Invocation ID are mapping attributes, not durable ownership identities.

Capability rules:

- Task Manager receives `TaskManagerGraph` tools and `taskManager.submitDecision`.
- Phase Agents receive phase output, proposal, context read/subscription, task memory read, and candidate submit tools allowed by the current endpoint.
- Context Agent receives mechanical Search and controlled general Context Service write/review tools.
- No Agent receives raw `GraphRuntime`, Scheduler, Workspace storage, Merge Queue, raw Event Log, or Adapter tools.

## Consequences

- Revoked invocation tokens stop working immediately at the gateway/service boundary.
- SSE, snapshot, and endpoint inspector authorization are consistent because they use the same project principal.
- Tool visibility is auditable per invocation and cannot be expanded by client-supplied TaskID or InvocationID.

## Rejected Options

- Putting browser bearer tokens into SSE query strings.
- Letting a browser or Agent call `GraphRuntime`, `PhaseController`, or Adapter methods.
- Binding durable graph ownership to AgentTeams worker/session names.
