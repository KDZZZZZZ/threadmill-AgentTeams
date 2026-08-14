import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ContextGraphSnapshot } from "../../api/types";
import { ContextGraphPanel } from "./ContextGraphPanel";

afterEach(() => cleanup());

const snapshot: ContextGraphSnapshot = {
  project_id: "project-1",
  revision: 8,
  nodes: [
    {
      node_id: "node-cold",
      kind: "fact",
      statement: "Cold accepted memory",
      status: "accepted",
      source_refs: ["artifact://cold"],
      subgraph_ids: ["general"],
      usage_count: 0,
    },
    {
      node_id: "node-hot",
      kind: "directive",
      statement: "Hot directive memory",
      status: "accepted",
      source_refs: ["requirement://hot"],
      subgraph_ids: ["task-alpha"],
      usage_count: 6,
      last_used_at: "2026-08-12T10:30:00Z",
    },
  ],
  edges: [
    {
      edge_id: "edge-1",
      from_node_id: "node-hot",
      to_node_id: "node-cold",
      kind: "supports",
      status: "accepted",
    },
  ],
};

describe("ContextGraphPanel", () => {
  it("renders server snapshot metrics and selected node details", async () => {
    render(
      <ContextGraphPanel
        snapshot={snapshot}
        loading={false}
        nodeGrowth={1}
        onRefresh={vi.fn()}
      />,
    );

    expect(
      screen.getByRole("heading", { name: "Context Graph" }),
    ).toBeVisible();
    expect(screen.getByText("节点数").closest("div")).toHaveTextContent("2");
    expect(screen.getByText("边数").closest("div")).toHaveTextContent("1");
    expect(screen.getByText("注入次数").closest("div")).toHaveTextContent("6");
    expect(screen.getByText("记忆节点增长").closest("div")).toHaveTextContent(
      "+1",
    );
    expect(screen.getByText("大小和颜色来自 usage_count")).toBeVisible();

    fireEvent.click(screen.getByText("Cold accepted memory"));
    expect(screen.getByLabelText("Context node detail")).toHaveTextContent(
      "artifact://cold",
    );
  });

  it("does not render fabricated graph data when the API is unavailable", () => {
    render(
      <ContextGraphPanel
        loading={false}
        error="404 page not found"
        nodeGrowth={0}
        onRefresh={vi.fn()}
      />,
    );

    expect(screen.getByText("404 page not found")).toBeVisible();
    expect(screen.queryByText("Hot directive memory")).not.toBeInTheDocument();
  });
});
