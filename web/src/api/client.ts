import type {
  ApiErrorBody,
  CapacityState,
  CoordinationSnapshot,
  EndpointInspector,
  EndpointRef,
  ManagerConversation,
  RequirementCreateResponse,
} from "./types";

export class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly body: ApiErrorBody,
  ) {
    super(body.message);
    this.name = "ApiError";
  }
}

function csrfToken(): string {
  return (
    document.querySelector<HTMLMetaElement>('meta[name="threadmill-csrf"]')
      ?.content ?? ""
  );
}

async function request<T>(
  input: RequestInfo | URL,
  init?: RequestInit,
): Promise<T> {
  const response = await fetch(input, {
    credentials: "same-origin",
    ...init,
    headers: {
      Accept: "application/json",
      ...(init?.body ? { "Content-Type": "application/json" } : {}),
      ...init?.headers,
    },
  });

  if (!response.ok) {
    const body = (await response.json().catch(() => ({
      code: "internal_error",
      message: response.statusText || "请求失败",
      recoverable: false,
    }))) as ApiErrorBody;
    throw new ApiError(response.status, body);
  }
  return (await response.json()) as T;
}

export function getCoordinationSnapshot(
  projectID: string,
): Promise<CoordinationSnapshot> {
  const params = new URLSearchParams({ project_id: projectID });
  return request(`/v1/coordination/snapshot?${params}`);
}

export function submitRequirement(input: {
  projectID: string;
  conversationID: string;
  body: string;
  motivation?: string;
  constraints?: string[];
  acceptance?: string[];
}): Promise<RequirementCreateResponse> {
  return request("/v1/requirements", {
    method: "POST",
    headers: { "X-Threadmill-CSRF": csrfToken() },
    body: JSON.stringify({
      request_id: crypto.randomUUID(),
      project_id: input.projectID,
      conversation_id: input.conversationID,
      body: input.body,
      motivation: input.motivation,
      constraints: input.constraints,
      acceptance: input.acceptance,
      source: { kind: "browser" },
    }),
  });
}

export function getCapacity(projectID: string): Promise<CapacityState> {
  const params = new URLSearchParams({ project_id: projectID });
  return request(`/v1/capacity?${params}`);
}

export function adjustCapacity(
  projectID: string,
  expectedRevision: number,
  desiredConcurrency: number,
): Promise<{ command_ref: string; capacity: CapacityState }> {
  return request("/v1/capacity-adjustments", {
    method: "POST",
    headers: { "X-Threadmill-CSRF": csrfToken() },
    body: JSON.stringify({
      request_id: crypto.randomUUID(),
      project_id: projectID,
      expected_revision: expectedRevision,
      desired_concurrency: desiredConcurrency,
    }),
  });
}

export function getEndpointInspector(
  projectID: string,
  ref: EndpointRef,
  generation?: number,
): Promise<EndpointInspector> {
  const params = new URLSearchParams({ project_id: projectID });
  if (generation) params.set("generation", String(generation));
  const suffix = params.size ? `?${params}` : "";
  return request(
    `/v1/coordination/endpoints/${encodeURIComponent(ref.task_id)}/${encodeURIComponent(ref.endpoint_id)}/inspector${suffix}`,
  );
}

export function getManagerConversation(
  projectID: string,
  conversationID: string,
): Promise<ManagerConversation> {
  const params = new URLSearchParams({ project_id: projectID });
  return request(
    `/v1/manager/conversations/${encodeURIComponent(conversationID)}?${params}`,
  );
}

export function submitManagerMessage(input: {
  projectID: string;
  conversationID: string;
  body: string;
  selectedEndpoint?: EndpointRef;
  observedGraphRevision: number;
}): Promise<{
  manager_input_ref: string;
  invocation_ref: string;
  conversation_id?: string;
  status: "accepted";
}> {
  return request("/v1/manager/messages", {
    method: "POST",
    headers: { "X-Threadmill-CSRF": csrfToken() },
    body: JSON.stringify({
      request_id: crypto.randomUUID(),
      project_id: input.projectID,
      conversation_id: input.conversationID,
      body: input.body,
      selected_endpoint: input.selectedEndpoint,
      observed_graph_revision: input.observedGraphRevision,
    }),
  });
}
