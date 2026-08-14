# ADR 0005: AgentTeams MVP Provider

Status: Accepted

Date: 2026-08-10

## Context

Threadmill needs a real execution host for Task Manager, Context Agent, and Phase Agent invocations. The latest Adapter design fixes the MVP host to AgentTeams QwenPaw plus TeamHarness taskflow.

Authoritative sources:

- `docs/plans/2026-08-10-threadmill-repository-implementation.md` §3
- `docs/threadmill-agentteams-adapter-design.md`
- `docs/architecture.md` §6
- `docs/design-rationale.md` §6

## Decision

The MVP provider is AgentTeams QwenPaw + TeamHarness taskflow.

Threadmill uses:

- `agentteams-controller` to prepare Manager and Worker hosts.
- QwenPaw Manager hosts for Task Manager and Context Agent invocations.
- QwenPaw Worker hosts for Phase invocations.
- TeamHarness `taskflow` for one bounded execution per invocation.
- Higress for model and MCP routing.
- MinIO/FileSync for workspace, result, and artifact transport.
- Matrix only for AgentTeams internal notification and human visibility.

`AgentTeamsHostAdapter` is internal to Agent Runtime. It dispatches, terminates, collects, and observes execution carriers. Returned execution results are always `UntrustedExecutionResult`; Runtime supplies authoritative Threadmill binding, validates generation, input revision, lease, `DeliverySpec`, `ReportSpec`, and artifact references before any PhaseOutput becomes usable.

`third_party/agentteams/` is read-only source evidence for existing capabilities. Threadmill does not modify its domain model, release process, or directory structure for this ADR.

## Consequences

- AgentTeams provides capacity and transport, not Threadmill coordination semantics.
- Duplicate dispatch is handled by stable command/execution references and never creates a second effective Invocation.
- Worker or Adapter status does not mean endpoint satisfied, Task done, or merge complete.
- Waiting, stop, and resume revoke old tokens, sessions, subscriptions, execution tasks, and leases according to Runtime rules.

## Rejected Options

- WorkerFlow, OpenClaw, Hermes, projectflow, or multi-runtime provider support in MVP.
- Treating AgentTeams DAG, Matrix room messages, `TaskMeta`, or `SUCCESS` status as Coordination Graph state.
- Letting Adapter write Coordination Graph or decide Task completion.
