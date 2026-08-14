import { useMemo, useState, type CSSProperties } from "react";
import {
  Background,
  Controls,
  MarkerType,
  ReactFlow,
  type Edge,
  type Node,
  type NodeMouseHandler,
  type NodeProps,
} from "@xyflow/react";
import {
  BrainCircuit,
  CircleAlert,
  Database,
  Flame,
  GitBranch,
  RefreshCw,
  type LucideIcon,
} from "lucide-react";
import type {
  ContextGraphSnapshot,
  ContextSnapshotNode,
} from "../../api/types";

interface Props {
  snapshot?: ContextGraphSnapshot;
  loading: boolean;
  error?: string;
  nodeGrowth: number;
  onRefresh: () => void;
}

interface ContextNodeData extends Record<string, unknown> {
  item: ContextSnapshotNode;
  heat: number;
}

const kindColumn: Record<string, number> = {
  directive: 0,
  fact: 1,
  hypothesis: 2,
};

function sourceNodeID(edge: { from_node_id?: string; from_ref?: string }) {
  if (edge.from_node_id) return edge.from_node_id;
  if (edge.from_ref?.startsWith("node:")) return edge.from_ref.slice(5);
  return undefined;
}

function heatClass(heat: number) {
  if (heat >= 0.66) return "context-hot";
  if (heat >= 0.33) return "context-warm";
  return "context-cold";
}

function compactTime(value?: string | null) {
  if (!value) return "未注入";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString(undefined, {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });
}

export function ContextGraphPanel({
  snapshot,
  loading,
  error,
  nodeGrowth,
  onRefresh,
}: Props) {
  const [selectedID, setSelectedID] = useState<string>();
  const maxUsage = useMemo(
    () =>
      Math.max(1, ...(snapshot?.nodes.map((node) => node.usage_count) ?? [])),
    [snapshot],
  );
  const sortedNodes = useMemo(
    () =>
      [...(snapshot?.nodes ?? [])].sort((a, b) => {
        const kindDelta = (kindColumn[a.kind] ?? 3) - (kindColumn[b.kind] ?? 3);
        if (kindDelta !== 0) return kindDelta;
        return (
          b.usage_count - a.usage_count || a.node_id.localeCompare(b.node_id)
        );
      }),
    [snapshot],
  );
  const nodesByKind = useMemo(() => {
    const seen = new Map<string, number>();
    return sortedNodes.map((item) => {
      const column = kindColumn[item.kind] ?? 3;
      const row = seen.get(item.kind) ?? 0;
      seen.set(item.kind, row + 1);
      const heat = item.usage_count / maxUsage;
      const size = 118 + Math.round(34 * heat);
      return {
        id: item.node_id,
        type: "context",
        selected: selectedID === item.node_id,
        draggable: false,
        connectable: false,
        selectable: true,
        position: { x: column * 260 + 36, y: row * 142 + 74 },
        data: { item, heat },
        style: { width: size, minHeight: size },
      } satisfies Node<ContextNodeData>;
    });
  }, [maxUsage, selectedID, sortedNodes]);
  const visible = useMemo(
    () => new Set(nodesByKind.map((node) => node.id)),
    [nodesByKind],
  );
  const edges = useMemo<Edge[]>(
    () =>
      (snapshot?.edges ?? []).flatMap((edge, index) => {
        const source = sourceNodeID(edge);
        if (!source || !visible.has(source) || !visible.has(edge.to_node_id)) {
          return [];
        }
        return [
          {
            id:
              edge.edge_id ??
              `${source}:${edge.to_node_id}:${edge.kind}:${index}`,
            source,
            target: edge.to_node_id,
            type: "smoothstep",
            className: `context-edge state-${edge.status ?? "accepted"}`,
            markerEnd: { type: MarkerType.ArrowClosed, width: 13, height: 13 },
          },
        ];
      }),
    [snapshot?.edges, visible],
  );
  const selected =
    sortedNodes.find((node) => node.node_id === selectedID) ?? sortedNodes[0];
  const totalUsage = sortedNodes.reduce(
    (sum, node) => sum + node.usage_count,
    0,
  );

  const handleNodeClick: NodeMouseHandler<Node<ContextNodeData>> = (
    _event,
    node,
  ) => setSelectedID(node.id);

  return (
    <section className="context-graph-region" aria-labelledby="context-heading">
      <div className="section-heading context-heading">
        <div>
          <p className="eyebrow">Memory projection</p>
          <h2 id="context-heading">Context Graph</h2>
        </div>
        <button
          className="secondary-button"
          type="button"
          onClick={onRefresh}
          aria-label="刷新 Context Graph"
        >
          <RefreshCw size={16} aria-hidden="true" />
          Refresh
        </button>
      </div>

      <dl className="context-metrics" aria-label="Context Graph 指标">
        <Metric icon={Database} label="节点数" value={sortedNodes.length} />
        <Metric
          icon={GitBranch}
          label="边数"
          value={snapshot?.edges.length ?? 0}
        />
        <Metric icon={Flame} label="注入次数" value={totalUsage} />
        <Metric
          icon={BrainCircuit}
          label="记忆节点增长"
          value={nodeGrowth > 0 ? `+${nodeGrowth}` : "0"}
        />
      </dl>

      {error ? (
        <div className="context-unavailable" role="status">
          <CircleAlert size={18} aria-hidden="true" />
          <span>{error}</span>
        </div>
      ) : loading && !snapshot ? (
        <div className="context-unavailable" role="status">
          <RefreshCw className="spin-once" size={18} aria-hidden="true" />
          <span>正在读取 Context Graph 快照</span>
        </div>
      ) : sortedNodes.length ? (
        <div className="context-graph-body">
          <div className="context-flow">
            <ReactFlow
              nodes={nodesByKind}
              edges={edges}
              nodeTypes={{ context: ContextMemoryNode }}
              onNodeClick={handleNodeClick}
              nodesDraggable={false}
              nodesConnectable={false}
              elementsSelectable
              fitView
              fitViewOptions={{ padding: 0.18, minZoom: 0.4, maxZoom: 1.18 }}
              minZoom={0.35}
              maxZoom={1.6}
              defaultEdgeOptions={{ interactionWidth: 16 }}
              proOptions={{ hideAttribution: true }}
              aria-label="Context Graph 记忆节点和引用关系"
            >
              <Background gap={22} size={1} color="var(--graph-dot)" />
              <Controls showInteractive={false} position="bottom-left" />
            </ReactFlow>
          </div>
          <ContextNodeDetails node={selected} />
        </div>
      ) : (
        <div className="context-unavailable" role="status">
          <BrainCircuit size={18} aria-hidden="true" />
          <span>后端尚未返回 Context Graph 节点。</span>
        </div>
      )}

      <div className="graph-legend context-heat-legend" aria-label="热度图例">
        <span>
          <i className="legend-heat context-cold" aria-hidden="true" />
          cold
        </span>
        <span>
          <i className="legend-heat context-warm" aria-hidden="true" />
          warm
        </span>
        <span>
          <i className="legend-heat context-hot" aria-hidden="true" />
          hot
        </span>
        <span>大小和颜色来自 usage_count</span>
      </div>
    </section>
  );
}

function Metric({
  icon: Icon,
  label,
  value,
}: {
  icon: LucideIcon;
  label: string;
  value: number | string;
}) {
  return (
    <div>
      <dt>
        <Icon size={14} aria-hidden="true" />
        {label}
      </dt>
      <dd>{value}</dd>
    </div>
  );
}

function ContextMemoryNode({
  data,
  selected,
}: NodeProps<Node<ContextNodeData>>) {
  const item = data.item;
  return (
    <button
      type="button"
      className={`context-memory-node ${heatClass(data.heat)} ${
        selected ? "is-selected" : ""
      }`}
      style={{ "--context-heat": data.heat } as CSSProperties}
    >
      <span className="context-node-kind">{item.kind}</span>
      <strong>{item.statement}</strong>
      <span className="context-node-meta">
        used {item.usage_count} / {compactTime(item.last_used_at)}
      </span>
    </button>
  );
}

function ContextNodeDetails({ node }: { node?: ContextSnapshotNode }) {
  if (!node) return null;
  return (
    <aside className="context-node-detail" aria-label="Context node detail">
      <div>
        <span className={`context-kind state-${node.status}`}>{node.kind}</span>
        <span className="context-detail-status">{node.status}</span>
      </div>
      <h3>{node.statement}</h3>
      <dl>
        <div>
          <dt>Node</dt>
          <dd>
            <code>{node.node_id}</code>
          </dd>
        </div>
        <div>
          <dt>Usage</dt>
          <dd>{node.usage_count}</dd>
        </div>
        <div>
          <dt>Last used</dt>
          <dd>{compactTime(node.last_used_at)}</dd>
        </div>
      </dl>
      <ReferenceList title="Source refs" values={node.source_refs} />
      <ReferenceList title="Subgraphs" values={node.subgraph_ids} />
    </aside>
  );
}

function ReferenceList({ title, values }: { title: string; values: string[] }) {
  return (
    <div className="context-reference-block">
      <h4>{title}</h4>
      {values.length ? (
        <ul>
          {values.map((value) => (
            <li key={value}>
              <code>{value}</code>
            </li>
          ))}
        </ul>
      ) : (
        <p>无</p>
      )}
    </div>
  );
}
