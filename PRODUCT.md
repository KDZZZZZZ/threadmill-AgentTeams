# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Stack

React, TypeScript, and Vite frontend hosted by the Go `threadmilld` service. The backend exposes HTTP/JSON commands, query endpoints, and SSE projections. Production execution is expected to adapt AgentTeams QwenPaw plus TeamHarness taskflow/workerflow.

## Users

Primary users are engineers and operators supervising multi-agent project execution. They need one interface to adjust available Agent concurrency, observe the Coordination Graph, influence future orchestration through the Task Manager, and inspect what context a selected Phase Agent actually received.

## Product Purpose

Threadmill turns multi-agent work into a controllable, recoverable, auditable execution system. Success means the user can move through `requirement -> plan -> execute -> verify -> merge -> done` without directly editing stores, calling graph CRUD, or reading private Agent sessions, while still tracing every control decision and context source.

## Positioning

Threadmill is built around two authoritative graphs plus controlled runtimes. Task Manager is the only Agent allowed to write the Coordination Graph. Agent Runtime materializes context from the union of active subscription subgraphs for each `ConsumerInvocationID`. GraphRuntime internally converts graph state into idempotent `start / stop / resume` `PhaseCommand`s. The GUI displays and calls these boundaries; it does not create another state authority.

## Operating Context

- Users continuously observe Tasks, fixed `plan / execute / verify` Phase Endpoints, blockers, revisions, leases, and Invocation state during project execution.
- Users send natural-language Manager messages to request hold, resume, replacement, or new prerequisites. The message may include a selected `PhaseEndpointRef` and seen graph revision, but never includes a graph patch.
- Selecting a Phase Endpoint lets the user inspect active subscription subgraphs, the materialized Context Slice, and Task Memory Buffer candidates created by that Invocation.
- Code delivery goes through the same-round workspace, independent verifier, latest-main targeted verify, and Merge Queue. Only the Merge Queue writes main.

## Capabilities and Constraints

- Users can increase or decrease desired Agent concurrency and distinguish desired, healthy, active, and waiting counts. Capacity changes affect throughput, not graph semantics.
- Task Manager is the only Agent write interface for Coordination Graph changes. Ordinary Agents, Scheduler, GraphRuntime, adapters, GUI, and HTTP clients do not receive graph CRUD authority.
- Phase Agents never call start, stop, or resume. Internal `PhaseController.Apply(PhaseCommand)` is the single phase-control entry. Resumable stop must produce checkpoint evidence; resume creates a fresh Invocation with a new generation, lease, session, and subscription context.
- Context Agent handles bounded semantic retrieval, general Context curation, and frozen-candidate review. It cannot write task subgraphs. Retrieval subscriptions bind to the original requester Invocation.
- Runtime materializes context from the union of every active subscription subgraph for the current `ConsumerInvocationID`. Cancelling one subscription removes only subgraphs no longer covered by another valid subscription.
- GUI snapshots, SSE, and inspector queries use the same project/operator ACL. The browser never receives Agent capability tokens, private transcripts, or session files.
- Objects, interfaces, and tools follow repository design documents. Display DTOs are permission-filtered projections only; they cannot become a third persistent graph.

## Brand Commitments

The product name is **Threadmill**. Interface language should be concise, direct, and auditable. Preserve domain terms such as `DecisionRef`, `BindingRef`, `Context Slice`, and `TaskMemoryBuffer`; do not mask boundaries with friendlier but ambiguous names.

## Evidence on Hand

- Authoritative system and Agent design documents live under `docs/`.
- The full implementation and acceptance plan lives at `docs/plans/2026-08-10-threadmill-repository-implementation.md`.
- `api/openapi/threadmill-v1.yaml` defines public user command, query, Manager conversation, UI projection, and event-stream contracts.
- `third_party/agentteams/` is read-only archived base code. It proves adaptable execution, isolation, taskflow, FileSync, and Worker capabilities; it does not make AgentTeams the authority for Threadmill business semantics.
- Local fake hosts, temporary git repositories, and browser tests can provide automated acceptance evidence. Real AgentTeams smoke must be explicitly marked as skipped when credentials/runtime are unavailable; it must not be reported as passed.

## Product Principles

1. Authoritative state has one source. Projections and adapters do not duplicate business decisions.
2. Control flows through auditable `command`, `inputRef`, `DecisionRef`, `revision`, and evidence boundaries.
3. Agents autonomously finish their scoped role work, but cannot expand authority, modify graphs, write main, or jump phases.
4. Context flows only by current Invocation permissions and active subscription union; Task, Phase, and project boundaries must remain explainable.
5. Failure, stop, conflict, disconnect, and restart are normal states. Each must be recoverable or explicitly marked non-resumable.

## Accessibility & Inclusion

Capacity controls, graph nodes, Manager conversation, and inspector panels must support keyboard operation. Color is never the only state encoding. The Coordination Graph must have an equivalent readable list. Dynamic updates are announced through accessible status regions. Text and interaction contrast must satisfy WCAG AA.
