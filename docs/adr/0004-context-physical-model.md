# ADR 0004: Context Physical Model and Default Pending Decisions

Status: Accepted

Date: 2026-08-10

## Context

Context Graph is the shared, permission-filtered external memory surface. Task working memory is a separate append-only candidate buffer. The implementation plan requires F01 to freeze the physical model and default values for previously open Context decisions.

Authoritative sources:

- `docs/plans/2026-08-10-threadmill-repository-implementation.md` §3
- `docs/context-graph.md`
- `docs/threadmill-unified-design.md` §9, §10, §11
- `docs/design-rationale.md` §5

## Decision

Context storage is physically split into tables for nodes, edges, subgraphs, membership, recipients, projection idempotency, subscriptions, candidate buffers, and task bindings. `Recipient` is binding metadata; it is not a `ContextNode` field and not a new `ContextEdge.Kind`.

`MemoryCandidate` transfer fields are fixed to:

- `statement`
- `kind`
- `source_refs`
- `subgraph_ids`

Candidate buffering is per Task, append-only, and visible only to the same Task's `plan / execute / verify` phases through `TaskMemoryBufferReader`. It does not participate in Context Graph revision, Search, subscription, or Delta delivery.

The "recently created node" rule is:

- Partition by `project_id + scope + actor_principal_id`.
- Sort deterministically by `(created_at, node_id)`.
- Default configurable window: most recent one visible node within the same partition.
- Default configurable max age: 30 days.
- The defaults live in configuration, not in Context domain objects.

The `derives_from_subgraph` rule is:

- When a node is created from effective subscribed subgraphs, create one idempotent `derives_from_subgraph` edge per still-readable subscribed subgraph.
- The edge records provenance only.
- It does not add the new node to the source subgraph.
- It does not increment the source subgraph revision.
- It does not emit Delta for the source subgraph by itself.
- Delta is emitted only for subgraphs whose membership, node payload, recipient binding, or directly visible member edge changes in the committed transaction.

Budget/config fields for one Invocation are not new domain objects. These values belong to configuration and `BudgetPolicy`: max subscriptions, max automatic edges, Delta coalescing window, candidate retention, audit retention, wall-clock budget, concurrency, Invocation limit, retry limit, verify level, token limit, and cost limit.

The MVP freezes these configuration defaults:

| Setting | Default | Enforcement |
| --- | --- | --- |
| Maximum effective subscriptions per Invocation | 64 | Applies after initial slice, search-created subscriptions, and explicit subscriptions are merged into the current Invocation's effective subscription union. |
| Maximum automatic edges per node creation | 32 | Counts the single `logical_adjacent` edge plus all `derives_from_subgraph` edges produced by the node creation transaction. |
| Delta coalescing window | 250 ms | Subscription executor may merge matching updates within this window before delivering Context Delta. |
| Candidate retention | 30 days after Task terminal state and candidate review completion | Retains TaskMemoryBuffer records after authoritative Task terminal state and Context Agent final review; cleanup deletes only records older than the retention policy. |
| Audit/Event metadata retention | 365 days | Retains Event Log metadata, audit records, decision metadata, subscription metadata, and candidate review metadata. Artifact body retention remains governed by artifact policy. |

These defaults are deploy-time configuration defaults, not fields on `ContextNode`, `ContextEdge`, `ContextSubgraph`, `MemoryCandidate`, `CandidateBufferRecord`, or `ContextSubscription`.

Limit handling is fail closed. If a request, graph transaction, subscription merge, or node creation would exceed a configured limit, Threadmill returns a structured error and records audit evidence where applicable. It must not silently truncate subscriptions, automatic edges, candidates, or Delta content and report success.

Token and cost metrics are hard-accounted when available. If unavailable, Threadmill records them as unavailable and still enforces wall-clock, concurrency, Invocation, retry, and verify-level budgets. It must not estimate unavailable token or cost values as facts.

## Consequences

- Recent-node linking is deterministic under concurrent creation.
- Subscription-derived provenance does not create self-amplifying source-subgraph Delta traffic.
- Candidate memory remains Task-local until authoritative `done` freezes it for Context Agent review.
- Schema evolution can add retention or budget knobs without changing core Context objects.
- Exceeding a default or configured limit rejects the operation instead of producing a partial success.

## Rejected Options

- Storing `Recipient` as a Context node or edge kind.
- Treating TaskMemoryBuffer records as Context nodes.
- Emitting Context Delta from candidate buffer append.
- Estimating unavailable provider token/cost values.
