---
version: alpha
name: Threadmill Control Room
description: 面向任务操作者的高密度实时协调控制台。
colors:
  bg: "#F2F5F5"
  surface: "#FBFCFC"
  surface-strong: "#EAF0F0"
  ink: "#142127"
  muted: "#5C6A73"
  line: "#D5DEDF"
  primary: "#0F766E"
  primary-strong: "#0B5F59"
  primary-soft: "#DDF0EC"
  danger: "#B42318"
  warning: "#8A5A00"
  success: "#227A47"
  focus: "#1D4ED8"
typography:
  heading:
    fontFamily: "Segoe UI, Microsoft YaHei, PingFang SC, sans-serif"
    fontSize: 24px
    fontWeight: 650
    lineHeight: 1.2
  title:
    fontFamily: "Segoe UI, Microsoft YaHei, PingFang SC, sans-serif"
    fontSize: 16px
    fontWeight: 650
    lineHeight: 1.35
  body:
    fontFamily: "Segoe UI, Microsoft YaHei, PingFang SC, sans-serif"
    fontSize: 14px
    fontWeight: 400
    lineHeight: 1.5
  label:
    fontFamily: "Segoe UI, Microsoft YaHei, PingFang SC, sans-serif"
    fontSize: 12px
    fontWeight: 600
    lineHeight: 1.4
  data:
    fontFamily: "Cascadia Mono, SFMono-Regular, Consolas, monospace"
    fontSize: 12px
    fontWeight: 500
    lineHeight: 1.45
rounded:
  control: 6px
  panel: 10px
  pill: 9999px
spacing:
  1: 4px
  2: 8px
  3: 12px
  4: 16px
  5: 20px
  6: 24px
  8: 32px
components:
  button-primary:
    backgroundColor: "{colors.primary}"
    textColor: "{colors.surface}"
    typography: "{typography.label}"
    rounded: "{rounded.control}"
    padding: 10px
    height: 40px
  button-secondary:
    backgroundColor: "{colors.surface}"
    textColor: "{colors.ink}"
    typography: "{typography.label}"
    rounded: "{rounded.control}"
    padding: 10px
    height: 40px
  panel:
    backgroundColor: "{colors.surface}"
    textColor: "{colors.ink}"
    rounded: "{rounded.panel}"
    padding: 16px
  endpoint-selected:
    backgroundColor: "{colors.primary-soft}"
    textColor: "{colors.ink}"
    rounded: "{rounded.control}"
    padding: 12px
---

# Design

## Overview

### Source of truth

- Status: Active
- Last refreshed: 2026-08-10
- Primary product surface: embedded Threadmill acceptance Demo at `/`
- Evidence reviewed: coordination graph, Task Manager Agent, unified design, context graph, repository implementation plan, current embedded web source, and the supplied six-skill reference image

### Brand

Threadmill is an engineering control surface, not a marketing page. It should feel precise, calm, inspectable, and ready for long operator sessions. The interface uses a cool light theme, one teal interaction accent, dense information, and restrained physical feedback. Avoid glass effects, gradients, oversized headings, decorative animation, and generic card mosaics.

### Product goals

The primary job is to understand and steer one live coordination graph without changing mental context. Success means an operator can adjust concurrency, select a graph endpoint, issue a Manager instruction, and verify subscriptions and context provenance from one screen. The Demo does not attempt production administration, authentication, or destructive operations.

### Personas and jobs

The primary user is an engineer supervising AgentTeams execution. Their work is state-first: detect capacity pressure, locate a phase endpoint, understand why it can or cannot run, change execution through Task Manager, and inspect the exact context assembled for that invocation.

### Information architecture

The coordination graph is the main canvas. Capacity controls occupy the fixed left rail. Manager conversation and recent events form the right operations rail. Selecting an endpoint opens an inspector directly below the graph with three stable views: subscription subgraphs, held project context, and the created task memory buffer.

### Design principles

- State before decoration: every color, icon, and motion must explain status, ownership, selection, or feedback.
- Graph before controls: the coordination graph receives the most space and remains the visual anchor.
- Provenance beside content: refs, generation, lease, subscription source, and creator invocation stay near the value they qualify.
- Dense but legible: use spacing and hairlines instead of nesting data inside repeated cards.
- Design dials: `DESIGN_VARIANCE=4`, `MOTION_INTENSITY=3`, `VISUAL_DENSITY=8`.

### Open questions

- None for the acceptance Demo. Production authorization, persistence, and multi-project navigation remain intentionally outside this design contract.

## Colors

The page uses one cool neutral family. `{colors.primary}` is reserved for interactive emphasis, selection, and the primary action. Success, warning, and danger colors communicate actual runtime semantics only and never act as decorative accents. Text and controls must maintain WCAG AA contrast.

## Typography

The system UI stack prioritizes Chinese and English readability without a font download. `{typography.heading}` is limited to the application title. `{typography.title}` names operational sections and nodes. `{typography.body}` carries instructions and messages. `{typography.data}` is mandatory for revisions, counts, refs, leases, generations, timestamps, and identifiers, using tabular numerals where supported.

## Layout

Desktop uses a three-region control-room layout: a 248px capacity rail, a fluid graph workspace, and a 340px operations rail. The inspector spans the graph workspace and presents three columns. Hairlines and tonal bands group related data. At widths below 1100px the rails move below the graph; below 720px every region becomes one column, controls remain at least 40px high, and horizontal phase lanes become a vertically scrollable node list.

The spacing scale follows 4px steps. Dense metadata uses 4px to 8px gaps, controls use 8px to 12px gaps, and primary regions use 16px to 24px gaps. No component may rely on viewport height for correctness.

## Elevation & Depth

Hierarchy comes from tonal surfaces, 1px borders, and sticky placement. Shadows are limited to the fixed application bar and selected endpoint focus, with no glow or blurred backdrop. Nested inspector data stays flat and uses separators instead of additional elevation.

## Shapes

Containers use `{rounded.panel}`, controls and endpoint nodes use `{rounded.control}`, and compact status tags use `{rounded.pill}`. This category rule is the only allowed radius variation. Status dots are allowed only when they represent a real live connection or execution state.

## Components

### Coordination graph

Each Task is a horizontal execution lane with endpoint nodes grouped by waiting, active or held, and completed states. A node exposes its endpoint ID, phase, generation, lease, checkpoint, and dependency counts. Selection uses the accent border and a tonal accent background, not scale or glow.

### Capacity controller

Show desired, healthy, active, and waiting together. Plus and minus buttons modify desired concurrency by one, display pending state, and keep the natural-drain explanation visible. A capacity change must not appear to mutate graph revision.

### Manager console

Manager is the only graph-writing surface. Quick actions and free text always target the selected endpoint. The current target, expected graph revision, ManagerInputRef, DecisionRef, and result are visible near the conversation. Inline errors remain beside the composer.

### Endpoint inspector

The three top-level views are stable and never hidden behind tabs in the acceptance Demo: current and recent subscription subgraphs, the effective context slice, and TaskMemoryBuffer candidates. Active and historical subscriptions are distinguishable by text and state, not color alone. Created context defaults to the current invocation provenance while retaining same-task evidence in the returned view.

### Interaction states

Loading uses shape-matched skeleton rows. Empty states state what is absent and the next useful action. Errors are inline and announced. Success is visible in changed data and the event log. Disabled controls explain limits through nearby copy. EventSource disconnects retain the last snapshot and announce retry state.

### Accessibility and motion

Use native buttons, labels, lists, and headings. Every icon-only control has an accessible name; decorative SVGs are hidden from assistive technology. Focus rings use `{colors.focus}` and remain visible on every interactive element. Motion is limited to opacity and transform under 200ms for selection, incoming state, and press feedback. `prefers-reduced-motion` disables all non-essential transitions and pulses.

### Iconography

Use one Tabler outline family sourced through Better Icons, with 2px strokes and `currentColor`. Icons support labels and never replace critical text. Emoji and hand-drawn SVG paths are not allowed.

### Content voice

Use concise Chinese operator language with canonical English field names where they improve traceability. Prefer direct verbs such as 调整、暂停、恢复、完成、创建、检查. Do not use marketing claims, metaphors, decorative version strings, or unexplained abbreviations.

### Implementation and acceptance

The Demo remains dependency-free at runtime: semantic HTML, CSS, and vanilla JavaScript are embedded by the Go server. The project-only design and icon CLIs are development tools. Acceptance requires fresh Go tests, JavaScript syntax validation, DESIGN.md lint and export, plus desktop and narrow-width browser screenshots showing all four core jobs on the real HTTP server.

## Do's and Don'ts

- Do keep the coordination graph as the largest region and preserve selected endpoint context across live updates.
- Do show desired, healthy, active, and waiting as separate values with tabular numerals.
- Do expose ManagerInputRef, DecisionRef, generation, lease, checkpoint, and subscription provenance.
- Do make active, historical, held, waiting, and completed states understandable without relying only on color.
- Do use Tabler icons retrieved through Better Icons and keep text labels for critical actions.
- Don't expose direct graph CRUD or direct invocation hold and resume controls outside the Manager surface.
- Don't imply that lowering desired concurrency terminates active invocations.
- Don't use gradients, glass blur, glow, decorative motion, huge headings, nested card stacks, or multiple accent colors.
- Don't animate layout properties or ignore reduced-motion preferences.
