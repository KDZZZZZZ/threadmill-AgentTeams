# ADR 0006: Web UI Projection and SSE

Status: Accepted

Date: 2026-08-10

## Context

The Web GUI must show capacity, Coordination Graph, Manager interaction, endpoint inspection, and recoverable realtime changes while preserving the Task Manager-only graph write invariant.

Authoritative sources:

- `docs/plans/2026-08-10-threadmill-repository-implementation.md` §3, §4.2
- `docs/event-artifact-store.md` §7
- `docs/coordination-graph.md` §1, §3, §4
- `docs/threadmill-unified-design.md` §5, §7, §14

## Decision

Browser actions are idempotent HTTP commands. Server changes are delivered through cursor-based SSE and reconstructed from `UIPanelProjection`.

The GUI may submit only:

- Requirement creation.
- Capacity adjustment.
- Human decision.
- Manager message with optional selected endpoint and observed graph revision.

The GUI may read:

- Capacity state.
- Task projection.
- Permission-filtered Coordination Graph snapshot.
- Endpoint inspector projection.
- Manager conversation projection.
- Event pages and SSE stream.
- Health endpoints.

`UIPanelProjection` is a rebuildable read model derived from authoritative domain state and Event Log. It is not a third persistent graph and has no mutation methods.

SSE uses stable event cursors and supports `Last-Event-ID`. On reconnect, the browser first fetches a permission-filtered snapshot and then resumes from cursor. If the cursor is expired, the server returns a cursor-expired error and the browser must refresh the snapshot.

Capacity semantics:

- `desired_concurrency` is the project-level maximum parallel Agent Invocation target.
- It is not a Worker Pod count.
- Increasing it allows Scheduler to claim more runnable endpoints if healthy capacity exists.
- Decreasing it stops new excess dispatch and lets running invocations drain naturally.
- It does not cancel, stop, hold, release, or resume a Phase.

## Consequences

- Realtime UI state has one authority: server events and snapshots.
- Browser UI cannot drag-edit graph state, send graph patches, or call GraphRuntime.
- Manager messages are indirect control inputs. Accepted graph changes are visible only through DecisionRef and later graph revision events.
- Endpoint inspector output is ACL-filtered and never exposes private transcripts, raw tool output, uncommitted workspace content, or other Task candidate bodies.

## Rejected Options

- WebSocket control protocol for MVP.
- Coordination Graph CRUD, JSON Patch, drag-to-write graph editing, or browser GraphRuntime controls.
- Browser stop/resume buttons that bypass Manager and Task Manager decisions.
