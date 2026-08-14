import { render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { EndpointInspectorPanel } from "./EndpointInspectorPanel";

afterEach(() => vi.restoreAllMocks());

describe("EndpointInspectorPanel", () => {
  it("keeps Context Slice nodes and TaskMemoryBuffer candidates distinct", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(
        async () =>
          new Response(
            JSON.stringify({
              endpoint: { task_id: "task-1", endpoint_id: "execute" },
              generation: 2,
              graph_revision: 8,
              invocation: { invocation_id: "inv-2", status: "running" },
              subscriptions: [
                {
                  subscription_id: "sub-1",
                  subgraph_ids: ["project-facts"],
                  active: true,
                  source: "search",
                },
              ],
              context_slice: {
                context_slice_ref: "slice-1",
                revision: "ctx-4",
                nodes: [
                  {
                    node_id: "node-1",
                    kind: "fact",
                    statement: "已注入的项目事实",
                    source_refs: [],
                  },
                ],
                omitted: [],
              },
              task_memory_buffer: {
                task_memory_buffer_ref: "buffer-1",
                candidates: [
                  {
                    candidate_id: "candidate-1",
                    candidate: {
                      kind: "hypothesis",
                      statement: "尚待终审的候选",
                      source_refs: [],
                      subgraph_ids: ["project-facts"],
                    },
                  },
                ],
              },
            }),
            { status: 200, headers: { "Content-Type": "application/json" } },
          ),
      ),
    );

    render(
      <EndpointInspectorPanel
        projectID="project-1"
        endpoint={{ task_id: "task-1", endpoint_id: "execute" }}
        generation={2}
      />,
    );

    expect(await screen.findByText("已注入的项目事实")).toBeInTheDocument();
    expect(screen.getByText("尚待终审的候选")).toBeInTheDocument();
    expect(screen.getByText("candidate")).toBeInTheDocument();
  });

  it("renders a specific never-run state without fabricating Invocation resources", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(
        async () =>
          new Response(
            JSON.stringify({
              endpoint: { task_id: "task-1", endpoint_id: "verify" },
              generation: 1,
              graph_revision: 8,
              subscriptions: [],
            }),
            { status: 200, headers: { "Content-Type": "application/json" } },
          ),
      ),
    );

    render(
      <EndpointInspectorPanel
        projectID="project-1"
        endpoint={{ task_id: "task-1", endpoint_id: "verify" }}
        generation={1}
      />,
    );

    expect(await screen.findByText("not started")).toBeInTheDocument();
    expect(screen.getByText("No Invocation")).toBeInTheDocument();
    expect(
      screen.getByText(
        /该 Phase 尚未开始，因此没有 Invocation 绑定的 TaskMemoryBuffer/,
      ),
    ).toBeInTheDocument();
  });
});
