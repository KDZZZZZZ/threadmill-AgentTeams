import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { RequirementComposer } from "./RequirementComposer";

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("RequirementComposer", () => {
  it("keeps accepted input distinct from an applied graph revision", async () => {
    const fetch = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          requirement_id: "requirement-1",
          manager_input_ref: "manager-input-1",
          invocation_ref: "invocation-1",
          conversation_id: "conversation-1",
          status: "accepted",
        }),
        { status: 202, headers: { "Content-Type": "application/json" } },
      ),
    );
    vi.stubGlobal("fetch", fetch);
    vi.stubGlobal("crypto", { randomUUID: () => "request-1" });
    const onAccepted = vi.fn().mockResolvedValue(undefined);
    const { rerender } = render(
      <RequirementComposer
        projectID="project-1"
        conversationID="conversation-1"
        graphRevision={4}
        hasTasks={false}
        onAccepted={onAccepted}
      />,
    );

    await userEvent.type(
      screen.getByLabelText("任务目标"),
      "真实运行 Task Manager",
    );
    await userEvent.click(
      screen.getByRole("button", { name: "交给 Task Manager" }),
    );

    expect(
      await screen.findByText("Task Manager 已接收，等待权威协调图更新"),
    ).toBeInTheDocument();
    expect(
      screen.queryByText(/检测到新的权威协调图 revision/),
    ).not.toBeInTheDocument();
    expect(onAccepted).toHaveBeenCalledOnce();
    expect(fetch).toHaveBeenCalledWith(
      "/v1/requirements",
      expect.objectContaining({ method: "POST" }),
    );

    rerender(
      <RequirementComposer
        projectID="project-1"
        conversationID="conversation-1"
        graphRevision={5}
        hasTasks
        onAccepted={onAccepted}
      />,
    );
    expect(
      screen.getByText("检测到新的权威协调图 revision 5"),
    ).toBeInTheDocument();
  });
});
