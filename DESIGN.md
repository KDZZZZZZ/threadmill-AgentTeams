---
version: alpha
name: Threadmill Control Room
description: 面向多 Agent 项目操作者的实时协调控制台。
colors:
  canvas: "#F4F6F8"
  panel: "#FFFFFF"
  muted: "#EEF1F5"
  border: "#D9DEE7"
  ink: "#121A2A"
  secondary: "#596579"
  primary: "#4B54B7"
  success: "#18794E"
  warning: "#9A5D08"
  danger: "#B42334"
typography:
  heading:
    fontFamily: "Inter, Segoe UI, system-ui, sans-serif"
    fontSize: 21px
    fontWeight: 700
    lineHeight: 1.2
  title:
    fontFamily: "Inter, Segoe UI, system-ui, sans-serif"
    fontSize: 16px
    fontWeight: 650
    lineHeight: 1.35
  body:
    fontFamily: "Inter, Segoe UI, system-ui, sans-serif"
    fontSize: 14px
    fontWeight: 400
    lineHeight: 1.5
  data:
    fontFamily: "JetBrains Mono, SFMono-Regular, Consolas, monospace"
    fontSize: 12px
    fontWeight: 500
    lineHeight: 1.45
rounded:
  control: 8px
  panel: 12px
  pill: 9999px
spacing:
  1: 4px
  2: 8px
  3: 12px
  4: 16px
  6: 24px
components:
  app-shell:
    backgroundColor: "{colors.canvas}"
    textColor: "{colors.ink}"
  button-primary:
    backgroundColor: "{colors.primary}"
    textColor: "{colors.panel}"
    rounded: "{rounded.control}"
    height: 40px
  panel:
    backgroundColor: "{colors.panel}"
    textColor: "{colors.ink}"
    rounded: "{rounded.panel}"
    padding: 16px
  divider:
    backgroundColor: "{colors.border}"
    height: 1px
  selection:
    backgroundColor: "{colors.muted}"
    textColor: "{colors.primary}"
    rounded: "{rounded.control}"
  metadata:
    textColor: "{colors.secondary}"
  status-success:
    textColor: "{colors.success}"
  status-warning:
    textColor: "{colors.warning}"
  status-danger:
    textColor: "{colors.danger}"
---

# Threadmill GUI Design System

## Product character

Threadmill is an operator-facing orchestration console for engineers supervising multi-agent work. The interface should feel like a calm control room: dense enough to explain live state, restrained enough that graph changes, blocked work, and permission boundaries remain obvious.

The GUI is a projection over authoritative server objects. It never invents a second Coordination Graph, Context Graph, Invocation, subscription, or candidate model. User actions submit documented commands or Manager messages; server snapshots and events are the only source of visible state.

## Design principles

1. **Authority is visible.** Show graph revision, DecisionRef, Invocation generation, binding status, and event freshness near the information they qualify.
2. **Control and observation stay distinct.** Capacity controls and Manager messages are actions. Graph nodes, subscriptions, Context Slice, and Task Memory Buffer are read models. Never style a pending client action as completed server state.
3. **Operational density without dashboard clutter.** Prefer a few stable regions, compact rows, and progressive disclosure. Avoid decorative KPI cards, gradients, glass effects, and oversized headings.
4. **State is never color-only.** Every status combines a label or icon with color. Graph nodes also expose the same information in an accessible list.
5. **One object, one vocabulary.** UI labels mirror the documented domain names. Do not rename `Phase Endpoint` to job, `Invocation` to worker, `Context Slice` to memory, or `TaskMemoryBuffer` candidates to graph nodes.

## Interface structure

Use a full-width application shell with four stable regions:

- **Command bar:** product name, project selector, connection freshness, current graph revision, and operator menu.
- **Capacity strip:** desired, healthy, active, and waiting Agent counts plus revision-safe decrement/increment controls.
- **Coordination workspace:** graph canvas as the primary surface, with a keyboard-accessible grouped list available alongside it.
- **Context rail:** switches between Manager conversation and the selected Phase Endpoint inspector. On wide screens it is persistent; on narrow screens it becomes a modal sheet.

Keep the graph visually dominant. Capacity is a compact horizontal control, not a grid of statistic cards. Manager conversation and endpoint inspection share the same rail so only one secondary task competes with the graph at a time.

## Visual language

### Typography

- UI sans: `Inter`, `Segoe UI`, system sans-serif.
- Technical identifiers: `JetBrains Mono`, `SFMono-Regular`, `Consolas`, monospace.
- Base size: 14px desktop, 15px touch layouts.
- Use sentence case. Page title 20-24px; section titles 14-16px; metadata 12-13px.
- Use tabular numerals for revisions, generations, capacity, and event cursors.

### Color

The default theme is a neutral, light operations surface with an optional dark equivalent. Use CSS variables rather than component-local literals.

| Token | Light intent | Usage |
| --- | --- | --- |
| `--surface-canvas` | cool off-white | application and graph background |
| `--surface-panel` | white | rails, menus, node bodies |
| `--surface-muted` | pale neutral | selected rows, code metadata |
| `--border-subtle` | cool gray | structural dividers |
| `--text-primary` | near-black navy | primary content |
| `--text-secondary` | slate | metadata and explanations |
| `--accent` | restrained indigo | focus, selected objects, primary action |
| `--success` | green | satisfied, healthy, accepted |
| `--warning` | amber | waiting, held, pending, degraded |
| `--danger` | red | failed, rejected, forbidden, disconnected |

Do not use gradients. Reserve saturated color for current status, selection, focus, and actionable feedback.

### Spacing and shape

- Base spacing unit: 4px. Common gaps: 8, 12, 16, 24px.
- Panels use 1px borders and 10-12px corner radii.
- Controls use 8px radii; status chips may be fully rounded.
- Avoid excessive nested cards. Group related content with headings, dividers, and spacing before adding another container.
- Minimum interactive target: 40x40px; icon-only controls require accessible names and tooltips.

## Core components

### Capacity control

Display `desired / healthy / active / waiting` in one compact strip. The minus and plus buttons submit an idempotent request with the last seen capacity revision. While pending, keep the authoritative number unchanged and show an adjacent progress indicator. On conflict, refresh the server snapshot and announce the reason.

### Coordination graph

Task groups contain exactly the documented `plan`, `execute`, and `verify` Phase Endpoints. Nodes show phase, state, run policy, generation, and active/waiting indicator. Edges and blockers use documented kinds; layout metadata is presentation-only and never sent back as graph mutation.

Selected, keyboard-focused, running, held, waiting, satisfied, and failed states must remain distinguishable without relying on color. Selecting a node changes inspection context only.

### Manager conversation

The composer sends natural language plus optional selected `PhaseEndpointRef` and seen graph revision. It never emits a graph patch, transition, or pending subgraph. Messages render from structured Manager interaction events and show pending/accepted/rejected/conflict state. Accepted replies expose `ManagerInputRef`, `DecisionRef`, and resulting graph revision.

### Phase Endpoint inspector

The inspector header identifies Task, phase, generation, and the active or most recent Invocation. Its body has three explicit sections:

1. **Subscription subgraphs:** active subscriptions, origin (`initial`, `retrieval`, `explicit`), subgraph identity, revision, and overlap/union explanation.
2. **Context Slice:** the context actually materialized for that Invocation, including revision, frontier, omitted/redacted markers, and conflicts.
3. **Task Memory Buffer:** candidates created by the inspected Invocation. Candidates remain visually distinct from accepted Context Graph nodes.

Empty, redacted, forbidden, expired, and no-active-Invocation states each need specific copy; never collapse them into a generic empty panel.

## Interaction and motion

- Motion clarifies state transitions; it does not decorate the page.
- Use 120-180ms opacity/transform transitions for panels, selection, and small status changes.
- Use spring motion only for direct manipulation such as opening the inspector sheet; avoid bounce.
- Graph node position changes may animate briefly after a server revision, but state labels and revision text update immediately.
- Respect `prefers-reduced-motion`; remove nonessential transforms and use instant graph layout changes.
- Never animate continuous telemetry indefinitely. Connection activity may use a subtle finite pulse only when reconnecting.

## Responsive behavior

- At 1200px and wider, show graph and a 360-420px context rail.
- Between 760px and 1199px, the rail overlays the graph and may be dismissed without clearing selection.
- Below 760px, use the accessible endpoint list as the default coordination view; graph remains an optional secondary view. Capacity controls wrap without horizontal page scrolling.
- Do not use fixed viewport-height layouts that trap content. Use `min-height: 100dvh` and allow the document or designated panel regions to scroll.

## Accessibility

- Use semantic buttons, forms, headings, lists, dialogs, tabs, and status regions.
- Provide a visible focus style with at least 3:1 contrast.
- Announce capacity updates, connection loss/recovery, Manager results, and graph revision changes through polite live regions.
- Graph nodes must be reachable by keyboard and mirrored in a readable list. Arrow-key navigation may supplement but cannot replace normal tab order.
- Every icon has an accessible name or is hidden when text already supplies the name.
- Ensure text and status contrast meet WCAG AA; candidates and Context Graph nodes must also differ by label and structure.

## Content conventions

- Prefer concise operational language: "Waiting for input", "Held by Manager decision", "Revision conflict — view refreshed".
- Preserve domain casing in technical labels: `DecisionRef`, `BindingRef`, `Context Slice`, `TaskMemoryBuffer`.
- Truncate opaque IDs visually only when the full value is available by focus/copy action.
- Error messages state what remained authoritative and what action is safe next. Never imply that a rejected client request changed the graph.

## Implementation constraints

- Consume only the documented HTTP query/command surfaces and SSE projection.
- Use one reducer keyed by authoritative snapshot revision and event cursor; duplicate events are idempotent.
- No optimistic Coordination Graph mutation. Temporary UI state is limited to selection, panel visibility, form drafts, and pending request indicators.
- Use one icon family through the project icon tooling. Do not hand-author SVG icons or use Unicode symbols as interface icons.
- Prefer accessible component primitives for dialogs, tabs, menus, tooltips, and sheets.
- Keep animations in the chosen motion library or CSS transitions; do not mix competing animation systems.
