# ADR 0001: Modular Monolith Deployment

Status: Accepted

Date: 2026-08-10

## Context

Threadmill MVP needs recoverable coordination, auditable decisions, a realtime Web GUI, and a real AgentTeams execution host. The implementation plan freezes a single Go deployment unit before service extraction so graph mutation, leases, outbox writes, and projections can share one transaction boundary.

Authoritative sources:

- `docs/plans/2026-08-10-threadmill-repository-implementation.md` §3
- `docs/architecture.md` §1, §4, §5
- `docs/threadmill-unified-design.md` §1, §6, §7

## Decision

Threadmill MVP is one `threadmilld` process with internal module boundaries. The same process runs HTTP, MCP ingress, Coordination Graph, `GraphRuntime`, Scheduler, Agent Runtime, Context Service, UI Projection, outbox dispatch, reconcile workers, and Merge Queue workers.

The package boundaries remain strict:

- `transport` wraps application/query seams and does not contain business state transitions.
- Task Manager is the only writer of Coordination Graph state.
- `GraphRuntime` is private to the Coordination Graph module and calls only the internal `PhaseController` port.
- Scheduler reads graph, budget, capacity, and health inputs; it does not create or mutate graph objects.
- Runtime records events, enforces permissions, assembles context, and hands boundary outputs to their owners; it does not decide Task completion.
- Merge Queue is the only writer to `main` and does not write Coordination Graph or Context Graph directly.

## Consequences

- The MVP avoids distributed transactions and duplicate deployment objects.
- The code may later split by package boundary, but no external API may depend on in-process objects such as `GraphRuntime`.
- Background worker failures are recovered from persistent command, lease, event, and outbox records rather than from process memory.

## Rejected Options

- Microservices for graph, runtime, projection, and merge in the first MVP.
- Browser or operator endpoints that directly call `GraphRuntime`.
- A second frontend service with its own state authority.
