import type { NodeProps } from "@xyflow/react";
import { Handle, Position } from "@xyflow/react";
import {
  CircleCheck,
  CirclePause,
  CircleX,
  Clock3,
  LoaderCircle,
} from "lucide-react";
import type { EndpointID } from "../../api/types";

export interface PhaseNodeData extends Record<string, unknown> {
  label: string;
  taskID: string;
  endpointID: EndpointID;
  generation: number;
  state: string;
  runPolicy?: "enabled" | "held";
}

function StatusIcon({ state, held }: { state: string; held: boolean }) {
  if (held) return <CirclePause size={15} aria-hidden="true" />;
  if (state === "satisfied" || state === "completed")
    return <CircleCheck size={15} aria-hidden="true" />;
  if (state === "failed" || state === "rejected")
    return <CircleX size={15} aria-hidden="true" />;
  if (state === "running" || state === "starting")
    return <LoaderCircle size={15} aria-hidden="true" />;
  return <Clock3 size={15} aria-hidden="true" />;
}

export function PhaseNode({ data, selected }: NodeProps) {
  const phase = data as PhaseNodeData;
  const held = phase.runPolicy === "held";
  return (
    <div
      className={`phase-node state-${phase.state} ${selected ? "is-selected" : ""}`}
    >
      <Handle type="target" position={Position.Left} isConnectable={false} />
      <div className="phase-node-topline">
        <span className="phase-kicker">{phase.endpointID}</span>
        <span className="phase-generation">gen {phase.generation}</span>
      </div>
      <strong>{phase.label}</strong>
      <span className="phase-task" title={phase.taskID}>
        {phase.taskID}
      </span>
      <span className={`status-label ${held ? "status-held" : ""}`}>
        <StatusIcon state={phase.state} held={held} />
        {held ? "held" : phase.state}
      </span>
      <Handle type="source" position={Position.Right} isConnectable={false} />
    </div>
  );
}
