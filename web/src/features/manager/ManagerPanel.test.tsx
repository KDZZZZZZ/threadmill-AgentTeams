import { cleanup, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ManagerPanel } from "./ManagerPanel";

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("ManagerPanel", () => {
  it("sends orchestration as an explicit non-lifecycle intent", async () => {
    vi.stubGlobal("crypto", { randomUUID: () => "request-orchestrate" });
    const fetch = vi.fn(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        if (String(input).includes("/v1/manager/conversations/")) {
          return new Response(
            JSON.stringify({
              conversation_id: "conversation-1",
              project_id: "project-1",
              messages: [],
            }),
            { status: 200, headers: { "Content-Type": "application/json" } },
          );
        }
        return new Response(
          JSON.stringify({
            manager_input_ref: "manager-input-1",
            invocation_ref: "invocation-1",
            status: "accepted",
          }),
          { status: 202, headers: { "Content-Type": "application/json" } },
        );
      },
    );
    vi.stubGlobal("fetch", fetch);

    render(
      <ManagerPanel
        projectID="project-1"
        conversationID="conversation-1"
        graphRevision={12}
      />,
    );
    await userEvent.type(
      screen.getByLabelText("给 Task Manager 的消息"),
      "重新编排剩余任务",
    );
    await userEvent.click(screen.getByRole("button", { name: "Send" }));

    await waitFor(() =>
      expect(
        fetch.mock.calls.some(
          ([url]) => String(url) === "/v1/manager/messages",
        ),
      ).toBe(true),
    );
    const post = fetch.mock.calls.find(
      ([url]) => String(url) === "/v1/manager/messages",
    );
    const payload = JSON.parse(String(post?.[1]?.body));
    expect(payload).toMatchObject({
      intent: "orchestrate",
      observed_graph_revision: 12,
    });
    expect(payload.selected_endpoint).toBeUndefined();
  });

  it("requires a selected Phase before hold or resume can be sent", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response("not found", { status: 404 })),
    );
    render(
      <ManagerPanel
        projectID="project-1"
        conversationID="conversation-1"
        graphRevision={12}
      />,
    );

    expect(screen.getByRole("radio", { name: "暂停 Phase" })).toBeDisabled();
    expect(screen.getByRole("radio", { name: "恢复 Phase" })).toBeDisabled();
  });
});
