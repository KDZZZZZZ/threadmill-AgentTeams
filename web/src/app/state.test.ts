import { describe, expect, it } from "vitest";
import type { CoordinationSnapshot, UiEvent } from "../api/types";
import { consoleReducer, initialConsoleState } from "./state";

const snapshot: CoordinationSnapshot = {
  project_id: "project-1",
  revision: 4,
  cursor: "cursor-4",
  tasks: [],
  nodes: [],
  edges: [],
  capacity: {
    project_id: "project-1",
    revision: 2,
    desired_concurrency: 3,
    healthy_capacity: 2,
    active_invocations: 1,
    waiting_invocations: 2,
    updated_at: "2026-08-11T00:00:00Z",
  },
};

describe("consoleReducer", () => {
  it("treats duplicate server events as idempotent", () => {
    const loaded = consoleReducer(initialConsoleState, {
      type: "snapshot.loaded",
      snapshot,
    });
    const event: UiEvent = {
      event_id: "event-1",
      cursor: "cursor-5",
      type: "capacity.updated",
      occurred_at: "2026-08-11T00:00:01Z",
      project_id: "project-1",
      payload: { ...snapshot.capacity, revision: 3, desired_concurrency: 5 },
    };
    const once = consoleReducer(loaded, { type: "event.received", event });
    const twice = consoleReducer(once, { type: "event.received", event });

    expect(once.snapshot?.capacity.desired_concurrency).toBe(5);
    expect(twice).toBe(once);
  });

  it("does not apply stale capacity projections", () => {
    const loaded = consoleReducer(initialConsoleState, {
      type: "snapshot.loaded",
      snapshot,
    });
    const event: UiEvent = {
      event_id: "event-stale",
      cursor: "cursor-5",
      type: "capacity.updated",
      occurred_at: "2026-08-11T00:00:01Z",
      project_id: "project-1",
      payload: { ...snapshot.capacity, revision: 1, desired_concurrency: 99 },
    };

    const result = consoleReducer(loaded, { type: "event.received", event });
    expect(result.snapshot?.capacity.desired_concurrency).toBe(3);
    expect(result.snapshot?.cursor).toBe("cursor-5");
  });

  it("does not regress the graph when snapshot refreshes resolve out of order", () => {
    const loaded = consoleReducer(initialConsoleState, {
      type: "snapshot.loaded",
      snapshot,
    });

    const result = consoleReducer(loaded, {
      type: "snapshot.loaded",
      snapshot: { ...snapshot, revision: 3, cursor: "cursor-3" },
    });

    expect(result).toBe(loaded);
  });
});
