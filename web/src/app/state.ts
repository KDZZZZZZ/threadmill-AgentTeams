import type {
  CapacityState,
  CoordinationSnapshot,
  EndpointRef,
  UiEvent,
} from "../api/types";

export type ConnectionState = "connecting" | "live" | "disconnected";
export type RailMode = "manager" | "inspector";

export interface ConsoleState {
  snapshot?: CoordinationSnapshot;
  selectedEndpoint?: EndpointRef;
  selectedGeneration?: number;
  railMode: RailMode;
  connection: ConnectionState;
  seenEventIDs: string[];
  announcement: string;
}

export type ConsoleAction =
  | { type: "snapshot.loaded"; snapshot: CoordinationSnapshot }
  | { type: "endpoint.selected"; endpoint: EndpointRef; generation: number }
  | { type: "rail.changed"; mode: RailMode }
  | { type: "connection.changed"; connection: ConnectionState }
  | { type: "event.received"; event: UiEvent }
  | { type: "capacity.loaded"; capacity: CapacityState };

export const initialConsoleState: ConsoleState = {
  railMode: "manager",
  connection: "connecting",
  seenEventIDs: [],
  announcement: "正在连接 Threadmill",
};

const maximumRememberedEvents = 256;

export function consoleReducer(
  state: ConsoleState,
  action: ConsoleAction,
): ConsoleState {
  switch (action.type) {
    case "snapshot.loaded":
      if (
        state.snapshot &&
        (action.snapshot.project_id !== state.snapshot.project_id ||
          action.snapshot.revision < state.snapshot.revision)
      ) {
        return state;
      }
      return {
        ...state,
        snapshot: action.snapshot,
        announcement: `协调图已更新到 revision ${action.snapshot.revision}`,
      };
    case "endpoint.selected":
      return {
        ...state,
        selectedEndpoint: action.endpoint,
        selectedGeneration: action.generation,
        railMode: "inspector",
      };
    case "rail.changed":
      return { ...state, railMode: action.mode };
    case "connection.changed":
      return {
        ...state,
        connection: action.connection,
        announcement:
          action.connection === "live"
            ? "实时连接已恢复"
            : action.connection === "disconnected"
              ? "实时连接已断开，正在重连"
              : "正在连接实时事件",
      };
    case "capacity.loaded":
      if (!state.snapshot) return state;
      return {
        ...state,
        snapshot: { ...state.snapshot, capacity: action.capacity },
        announcement: `并发目标已更新为 ${action.capacity.desired_concurrency}`,
      };
    case "event.received": {
      if (state.seenEventIDs.includes(action.event.event_id)) return state;
      const seenEventIDs = [...state.seenEventIDs, action.event.event_id].slice(
        -maximumRememberedEvents,
      );
      if (action.event.type === "capacity.updated" && state.snapshot) {
        const capacity = action.event.payload as unknown as CapacityState;
        if (
          capacity.project_id === state.snapshot.project_id &&
          typeof capacity.revision === "number" &&
          capacity.revision >= state.snapshot.capacity.revision
        ) {
          return {
            ...state,
            seenEventIDs,
            snapshot: {
              ...state.snapshot,
              cursor: action.event.cursor,
              capacity,
            },
            announcement: `并发状态已更新，revision ${capacity.revision}`,
          };
        }
      }
      return {
        ...state,
        seenEventIDs,
        snapshot: state.snapshot
          ? { ...state.snapshot, cursor: action.event.cursor }
          : undefined,
      };
    }
  }
}
