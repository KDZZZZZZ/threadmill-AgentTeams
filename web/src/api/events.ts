import type { UiEvent } from "./types";

export interface EventStream {
  close(): void;
}

export function openEventStream(
  projectID: string,
  after: string | undefined,
  handlers: {
    onOpen: () => void;
    onEvent: (event: UiEvent) => void;
    onError: () => void;
  },
): EventStream {
  const params = new URLSearchParams({ project_id: projectID });
  if (after) params.set("after", after);
  const source = new EventSource(`/v1/events/stream?${params}`, {
    withCredentials: true,
  });

  source.onopen = handlers.onOpen;
  source.onerror = handlers.onError;
  const types: UiEvent["type"][] = [
    "capacity.updated",
    "graph.revision",
    "task.updated",
    "endpoint.updated",
    "invocation.updated",
    "subscription.updated",
    "context.delta",
    "task_memory_buffer.updated",
    "manager.interaction",
  ];
  types.forEach((type) => {
    source.addEventListener(type, (message) => {
      try {
        handlers.onEvent(
          JSON.parse((message as MessageEvent<string>).data) as UiEvent,
        );
      } catch {
        handlers.onError();
      }
    });
  });
  return source;
}
