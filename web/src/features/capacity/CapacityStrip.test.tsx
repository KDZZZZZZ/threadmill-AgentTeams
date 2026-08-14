import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CapacityStrip } from "./CapacityStrip";

afterEach(() => vi.restoreAllMocks());

describe("CapacityStrip", () => {
  it("keeps the authoritative desired value while an adjustment is pending", async () => {
    vi.stubGlobal("crypto", { randomUUID: () => "request-1" });
    vi.stubGlobal(
      "fetch",
      vi.fn(() => new Promise(() => undefined)),
    );
    render(
      <CapacityStrip
        capacity={{
          project_id: "project-1",
          revision: 3,
          desired_concurrency: 4,
          healthy_capacity: 3,
          active_invocations: 2,
          waiting_invocations: 1,
          updated_at: "2026-08-11T00:00:00Z",
        }}
        onCapacityAccepted={vi.fn()}
        onConflict={vi.fn()}
      />,
    );

    await userEvent.click(
      screen.getByRole("button", { name: "增加一个 Agent 并发目标" }),
    );
    expect(screen.getByText("4")).toBeInTheDocument();
    expect(screen.queryByText("5")).not.toBeInTheDocument();
  });
});
