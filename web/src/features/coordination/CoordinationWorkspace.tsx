import { useMemo, useState } from "react";
import {
  Background,
  Controls,
  MarkerType,
  ReactFlow,
  type Edge,
  type Node,
  type NodeMouseHandler,
} from "@xyflow/react";
import { ListTree, Network, Workflow } from "lucide-react";
import type {
  CoordinationSnapshot,
  EndpointRef,
  GraphNode,
} from "../../api/types";
import { PhaseNode, type PhaseNodeData } from "./PhaseNode";

interface Props {
  snapshot: CoordinationSnapshot;
  selectedEndpoint?: EndpointRef;
  onSelectEndpoint: (endpoint: EndpointRef, generation: number) => void;
  onRequestRequirement: () => void;
}

const phaseOrder = { plan: 0, execute: 1, verify: 2 } as const;

function refKey(ref: EndpointRef): string {
  return `${ref.task_id}:${ref.endpoint_id}`;
}

function isSelected(node: GraphNode, selected?: EndpointRef): boolean {
  return Boolean(
    selected &&
      node.task_id === selected.task_id &&
      node.endpoint_id === selected.endpoint_id,
  );
}

export function CoordinationWorkspace({
  snapshot,
  selectedEndpoint,
  onSelectEndpoint,
  onRequestRequirement,
}: Props) {
  const [view, setView] = useState<"graph" | "list">(() =>
    window.matchMedia?.("(max-width: 759px)").matches ? "list" : "graph",
  );
  const endpointNodes = useMemo(
    () => snapshot.nodes.filter((node) => node.kind === "endpoint"),
    [snapshot],
  );
  const lookup = useMemo(
    () => new Map(endpointNodes.map((node) => [refKey(node), node.id])),
    [endpointNodes],
  );
  const taskIndex = useMemo(
    () => new Map(snapshot.tasks.map((task, index) => [task.task_id, index])),
    [snapshot.tasks],
  );
  const nodes = useMemo<Node<PhaseNodeData>[]>(
    () =>
      endpointNodes.map((node) => ({
        id: node.id,
        type: "phase",
        selected: isSelected(node, selectedEndpoint),
        draggable: false,
        connectable: false,
        selectable: true,
        position: {
          x: (taskIndex.get(node.task_id) ?? 0) * 330 + 40,
          y: phaseOrder[node.endpoint_id] * 170 + 48,
        },
        data: {
          label: node.label,
          taskID: node.task_id,
          endpointID: node.endpoint_id,
          generation: node.generation,
          state: node.state,
          runPolicy: node.run_policy,
        },
      })),
    [endpointNodes, selectedEndpoint, taskIndex],
  );
  const edges = useMemo<Edge[]>(
    () =>
      snapshot.edges.flatMap((edge) => {
        const source = lookup.get(refKey(edge.from));
        const target = lookup.get(refKey(edge.to));
        if (!source || !target) return [];
        return [
          {
            id: edge.id,
            source,
            target,
            label: edge.required_by,
            className: `graph-edge state-${edge.state}`,
            markerEnd: { type: MarkerType.ArrowClosed },
          },
        ];
      }),
    [lookup, snapshot.edges],
  );

  const handleNodeClick: NodeMouseHandler<Node<PhaseNodeData>> = (
    _event,
    node,
  ) => {
    onSelectEndpoint(
      { task_id: node.data.taskID, endpoint_id: node.data.endpointID },
      node.data.generation,
    );
  };

  return (
    <div className="coordination-workspace">
      <div className="view-switch" role="group" aria-label="协调图显示方式">
        <button
          type="button"
          aria-pressed={view === "graph"}
          onClick={() => setView("graph")}
        >
          <Network size={15} aria-hidden="true" /> Graph
        </button>
        <button
          type="button"
          aria-pressed={view === "list"}
          onClick={() => setView("list")}
        >
          <ListTree size={15} aria-hidden="true" /> List
        </button>
      </div>

      <div
        className={`graph-view ${view === "graph" ? "is-visible" : ""}`}
        aria-hidden={view !== "graph"}
      >
        {endpointNodes.length ? (
          <ReactFlow
            nodes={nodes}
            edges={edges}
            nodeTypes={{ phase: PhaseNode }}
            onNodeClick={handleNodeClick}
            nodesDraggable={false}
            nodesConnectable={false}
            elementsSelectable
            fitView
            minZoom={0.35}
            maxZoom={1.6}
            proOptions={{ hideAttribution: true }}
            aria-label="Coordination Graph 节点和依赖"
          >
            <Background gap={24} size={1} color="var(--graph-dot)" />
            <Controls showInteractive={false} position="bottom-left" />
          </ReactFlow>
        ) : (
          <div className="graph-empty">
            <Workflow size={22} aria-hidden="true" />
            <h3>协调图还没有节点</h3>
            <p>提交需求后，Task Manager 才能通过受控接口创建待执行子图。</p>
            <button
              className="primary-button"
              type="button"
              onClick={onRequestRequirement}
            >
              提交首个需求
            </button>
          </div>
        )}
      </div>

      <div className={`list-view ${view === "list" ? "is-visible" : ""}`}>
        {snapshot.tasks.map((task) => (
          <section
            key={task.task_id}
            className="task-group"
            aria-labelledby={`task-${task.task_id}`}
          >
            <div className="task-group-heading">
              <div>
                <h3 id={`task-${task.task_id}`}>
                  {task.title || task.task_id}
                </h3>
                <code>{task.task_id}</code>
              </div>
              <span className={`status-label state-${task.status}`}>
                {task.status}
              </span>
            </div>
            <ul>
              {endpointNodes
                .filter((node) => node.task_id === task.task_id)
                .sort(
                  (a, b) =>
                    phaseOrder[a.endpoint_id] - phaseOrder[b.endpoint_id],
                )
                .map((node) => (
                  <li key={node.id}>
                    <button
                      type="button"
                      aria-current={
                        isSelected(node, selectedEndpoint) ? "true" : undefined
                      }
                      onClick={() =>
                        onSelectEndpoint(
                          {
                            task_id: node.task_id,
                            endpoint_id: node.endpoint_id,
                          },
                          node.generation,
                        )
                      }
                    >
                      <span className="phase-name">{node.endpoint_id}</span>
                      <span>{node.label}</span>
                      <span className="phase-list-meta">
                        gen {node.generation} ·{" "}
                        {node.run_policy === "held" ? "held" : node.state}
                      </span>
                    </button>
                  </li>
                ))}
            </ul>
          </section>
        ))}
      </div>
    </div>
  );
}
