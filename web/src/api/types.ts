export type EndpointID = "plan" | "execute" | "verify";

export interface EndpointRef {
  task_id: string;
  endpoint_id: EndpointID;
}

export interface CapacityState {
  project_id: string;
  revision: number;
  desired_concurrency: number;
  healthy_capacity: number;
  active_invocations: number;
  waiting_invocations: number;
  degraded_reason?: string;
  updated_at: string;
}

export interface TaskSummary {
  task_id: string;
  title?: string;
  status: "pending" | "running" | "blocked" | "done" | "canceled" | "failed";
}

export interface GraphNode {
  id: string;
  kind: "endpoint";
  label: string;
  task_id: string;
  endpoint_id: EndpointID;
  generation: number;
  state: string;
  run_policy?: "enabled" | "held";
  binding_ref?: string;
  latest_invocation_ref?: string;
}

export interface GraphEdge {
  id: string;
  from: EndpointRef;
  to: EndpointRef;
  required_by: "start" | "completion";
  state: "pending" | "satisfied" | "failed" | "obsolete";
  artifact_kinds?: string[];
}

export interface CoordinationSnapshot {
  project_id: string;
  revision: number;
  cursor: string;
  tasks: TaskSummary[];
  nodes: GraphNode[];
  edges: GraphEdge[];
  capacity: CapacityState;
}

export interface RequirementCreateResponse {
  requirement_id: string;
  manager_input_ref: string;
  invocation_ref: string;
  conversation_id?: string;
  status: "accepted";
}

export interface InvocationProjection {
  invocation_id: string;
  provider?: "agentteams_qwenpaw_taskflow";
  status:
    | "pending"
    | "running"
    | "waiting"
    | "stopped"
    | "failed"
    | "completed";
  started_at?: string;
  ended_at?: string;
  input_revision?: string;
  workspace_ref?: string;
  context_slice_ref?: string;
  task_memory_buffer_ref?: string;
}

export interface SubscriptionProjection {
  subscription_id: string;
  subgraph_ids: string[];
  active: boolean;
  source?: "initial_slice" | "search" | "explicit";
}

export interface ArtifactRef {
  artifact_id: string;
  object_key?: string;
  sha256: string;
  media_type: string;
  size_bytes: number;
  acl?: string[];
  source_event_id?: string;
  source_invocation_id?: string;
}

export interface ContextNodeView {
  node_id: string;
  kind: "directive" | "fact" | "hypothesis";
  statement: string;
  status?: string;
  source_refs: string[];
  subgraph_ids?: string[];
}

export interface ContextSliceView {
  context_slice_ref: string;
  revision: string;
  nodes: ContextNodeView[];
  frontier?: string[];
  omitted: Array<{
    reason: "forbidden" | "redacted" | "stale" | "budget_limited";
    count: number;
  }>;
}

export interface MemoryCandidate {
  statement: string;
  kind: "directive" | "fact" | "hypothesis";
  source_refs: string[];
  subgraph_ids: string[];
}

export interface TaskMemoryBufferView {
  task_memory_buffer_ref: string;
  candidates: Array<{ candidate_id: string; candidate: MemoryCandidate }>;
  omitted?: Array<{
    reason: "forbidden" | "redacted" | "stale" | "budget_limited";
    count: number;
  }>;
}

export interface EndpointInspector {
  endpoint: EndpointRef;
  generation: number;
  graph_revision: number;
  invocation?: InvocationProjection;
  subscriptions: SubscriptionProjection[];
  context_slice?: ContextSliceView;
  task_memory_buffer?: TaskMemoryBufferView;
}

export interface ManagerConversationEntry {
  entry_id: string;
  kind: "user_message" | "manager_reply" | "decision" | "mutation_result";
  created_at: string;
  manager_input_ref?: string;
  decision_ref?: string;
  graph_revision?: number;
  body?: string;
  disposition?:
    | "pending"
    | "accepted"
    | "rejected"
    | "conflict"
    | "applied"
    | "no_change";
}

export interface ManagerConversation {
  conversation_id: string;
  project_id: string;
  messages: ManagerConversationEntry[];
  cursor: string;
}

export type UiEventType =
  | "capacity.updated"
  | "graph.revision"
  | "task.updated"
  | "endpoint.updated"
  | "invocation.updated"
  | "subscription.updated"
  | "context.delta"
  | "task_memory_buffer.updated"
  | "manager.interaction";

export interface UiEvent {
  event_id: string;
  cursor: string;
  type: UiEventType;
  occurred_at: string;
  project_id: string;
  task_id?: string;
  endpoint_id?: EndpointID;
  payload: Record<string, unknown>;
}

export interface ApiErrorBody {
  code: string;
  message: string;
  recoverable: boolean;
  correlation_id?: string;
  details?: Record<string, unknown>;
}
