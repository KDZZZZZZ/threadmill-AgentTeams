import {
  useCallback,
  useEffect,
  useMemo,
  useReducer,
  useRef,
  useState,
} from "react";
import { MotionConfig } from "motion/react";
import {
  MessageSquareText,
  PanelRightOpen,
  RefreshCw,
  Workflow,
} from "lucide-react";
import {
  getContextGraphSnapshot,
  getCoordinationSnapshot,
} from "../api/client";
import { openEventStream } from "../api/events";
import type { ContextGraphSnapshot, EndpointRef, UiEvent } from "../api/types";
import { CapacityStrip } from "../features/capacity/CapacityStrip";
import { CoordinationWorkspace } from "../features/coordination/CoordinationWorkspace";
import { ContextGraphPanel } from "../features/context-graph/ContextGraphPanel";
import { EndpointInspectorPanel } from "../features/endpoint-inspector/EndpointInspectorPanel";
import { ManagerPanel } from "../features/manager/ManagerPanel";
import { RequirementComposer } from "../features/requirements/RequirementComposer";
import { consoleReducer, initialConsoleState } from "./state";

function queryValue(name: string): string {
  return new URLSearchParams(window.location.search).get(name)?.trim() || "";
}

export function App() {
  const projectID = useMemo(() => queryValue("project_id"), []);
  const conversationID = useMemo(
    () => queryValue("conversation_id") || `${projectID}-manager`,
    [projectID],
  );
  const [state, dispatch] = useReducer(consoleReducer, initialConsoleState);
  const [loadError, setLoadError] = useState<string>();
  const [contextSnapshot, setContextSnapshot] =
    useState<ContextGraphSnapshot>();
  const [contextError, setContextError] = useState<string>();
  const [contextLoading, setContextLoading] = useState(false);
  const [contextNodeGrowth, setContextNodeGrowth] = useState(0);
  const [streamEpoch, setStreamEpoch] = useState(0);
  const initialContextNodeCount = useRef<number | undefined>(undefined);
  const requirementInputRef = useRef<HTMLTextAreaElement>(null);

  const refreshSnapshot = useCallback(async () => {
    if (!projectID) return;
    try {
      const snapshot = await getCoordinationSnapshot(projectID);
      dispatch({ type: "snapshot.loaded", snapshot });
      setLoadError(undefined);
    } catch (error) {
      setLoadError(
        error instanceof Error ? error.message : "无法读取协调图快照",
      );
    }
  }, [projectID]);

  const refreshContextSnapshot = useCallback(async () => {
    if (!projectID) return;
    setContextLoading(true);
    try {
      const snapshot = await getContextGraphSnapshot(projectID);
      if (initialContextNodeCount.current === undefined) {
        initialContextNodeCount.current = snapshot.nodes.length;
      }
      setContextNodeGrowth(
        Math.max(0, snapshot.nodes.length - initialContextNodeCount.current),
      );
      setContextSnapshot(snapshot);
      setContextError(undefined);
    } catch (error) {
      setContextError(
        error instanceof Error ? error.message : "无法读取 Context Graph 快照",
      );
    } finally {
      setContextLoading(false);
    }
  }, [projectID]);

  useEffect(() => {
    void refreshSnapshot();
    void refreshContextSnapshot();
  }, [refreshContextSnapshot, refreshSnapshot]);

  useEffect(() => {
    if (!projectID || !state.snapshot) return;
    let disposed = false;
    let recovering = false;
    let recoveryTimer: number | undefined;
    let stream: ReturnType<typeof openEventStream> | undefined;
    const recoverFromCursor = () => {
      if (disposed || recovering) return;
      recovering = true;
      dispatch({ type: "connection.changed", connection: "disconnected" });
      stream?.close();
      recoveryTimer = window.setTimeout(() => {
        void refreshSnapshot().finally(() => {
          if (!disposed) setStreamEpoch((current) => current + 1);
        });
      }, 250);
    };
    stream = openEventStream(projectID, state.snapshot.cursor, {
      onOpen: () =>
        dispatch({ type: "connection.changed", connection: "live" }),
      onError: recoverFromCursor,
      onEvent: (event: UiEvent) => {
        dispatch({ type: "event.received", event });
        if (
          event.type === "context.delta" ||
          event.type === "task_memory_buffer.updated"
        ) {
          void refreshContextSnapshot();
        }
        if (event.type !== "capacity.updated" && event.type !== "context.delta")
          void refreshSnapshot();
      },
    });
    return () => {
      disposed = true;
      if (recoveryTimer !== undefined) window.clearTimeout(recoveryTimer);
      stream?.close();
    };
  }, [
    projectID,
    refreshContextSnapshot,
    refreshSnapshot,
    state.snapshot?.project_id,
    streamEpoch,
  ]);

  const selectEndpoint = useCallback(
    (endpoint: EndpointRef, generation: number) => {
      dispatch({ type: "endpoint.selected", endpoint, generation });
    },
    [],
  );

  if (!projectID) {
    return (
      <main className="configuration-error" role="alert">
        <Workflow size={24} aria-hidden="true" />
        <h1>缺少项目上下文</h1>
        <p>
          生产控制台要求 URL 显式提供 <code>project_id</code>。界面不会回退到
          demo 项目或展示示例数据。
        </p>
      </main>
    );
  }

  return (
    <MotionConfig reducedMotion="user" transition={{ duration: 0.16 }}>
      <div className="app-shell">
        <header className="command-bar">
          <div className="brand-block">
            <span className="brand-mark" aria-hidden="true" />
            <div>
              <p className="eyebrow">Agent orchestration</p>
              <h1>Threadmill</h1>
            </div>
          </div>
          <div className="command-meta" aria-label="当前项目与事件状态">
            <span className="project-pill">
              <span>Project</span>
              <code title={projectID}>{projectID}</code>
            </span>
            <span
              className={`connection-status connection-${state.connection}`}
              role="status"
            >
              <span className="connection-dot" aria-hidden="true" />
              {state.connection === "live"
                ? "Live"
                : state.connection === "connecting"
                  ? "Connecting"
                  : "Reconnecting"}
            </span>
            <span className="revision-label">
              Graph revision <strong>{state.snapshot?.revision ?? "—"}</strong>
            </span>
            <button
              className="icon-button"
              type="button"
              onClick={() => void refreshSnapshot()}
              aria-label="刷新服务端快照"
              title="刷新服务端快照"
            >
              <RefreshCw size={17} aria-hidden="true" />
            </button>
          </div>
        </header>

        {state.snapshot ? (
          <CapacityStrip
            capacity={state.snapshot.capacity}
            onCapacityAccepted={(capacity) =>
              dispatch({ type: "capacity.loaded", capacity })
            }
            onConflict={refreshSnapshot}
          />
        ) : null}

        {state.snapshot ? (
          <RequirementComposer
            projectID={projectID}
            conversationID={conversationID}
            graphRevision={state.snapshot.revision}
            hasTasks={state.snapshot.tasks.length > 0}
            onAccepted={refreshSnapshot}
            inputRef={requirementInputRef}
          />
        ) : null}

        <main className="console-layout">
          <div className="primary-column">
            <section
              className="coordination-region"
              aria-labelledby="coordination-heading"
            >
              <div className="section-heading">
                <div>
                  <p className="eyebrow">Authoritative projection</p>
                  <h2 id="coordination-heading">Coordination Graph</h2>
                </div>
                {state.selectedEndpoint ? (
                  <button
                    type="button"
                    className="secondary-button rail-shortcut"
                    onClick={() =>
                      dispatch({ type: "rail.changed", mode: "inspector" })
                    }
                  >
                    <PanelRightOpen size={16} aria-hidden="true" />
                    Inspect selected Phase
                  </button>
                ) : null}
              </div>

              {loadError ? (
                <div className="error-notice" role="alert">
                  <strong>快照未更新。</strong> {loadError}
                  <button type="button" onClick={() => void refreshSnapshot()}>
                    重试
                  </button>
                </div>
              ) : null}

              {state.snapshot ? (
                <CoordinationWorkspace
                  snapshot={state.snapshot}
                  selectedEndpoint={state.selectedEndpoint}
                  onSelectEndpoint={selectEndpoint}
                  onRequestRequirement={() =>
                    requirementInputRef.current?.focus()
                  }
                />
              ) : (
                <div className="loading-state" role="status">
                  <RefreshCw
                    className="spin-once"
                    size={18}
                    aria-hidden="true"
                  />
                  正在读取权限过滤后的协调图…
                </div>
              )}
            </section>

            <ContextGraphPanel
              snapshot={contextSnapshot}
              loading={contextLoading}
              error={contextError}
              nodeGrowth={contextNodeGrowth}
              onRefresh={refreshContextSnapshot}
            />
          </div>

          <aside className="context-rail" aria-label="Manager 与 Phase 检查器">
            <div className="rail-tabs" role="tablist" aria-label="Context rail">
              <button
                type="button"
                role="tab"
                aria-selected={state.railMode === "manager"}
                onClick={() =>
                  dispatch({ type: "rail.changed", mode: "manager" })
                }
              >
                <MessageSquareText size={16} aria-hidden="true" />
                Manager
              </button>
              <button
                type="button"
                role="tab"
                aria-selected={state.railMode === "inspector"}
                disabled={!state.selectedEndpoint}
                onClick={() =>
                  dispatch({ type: "rail.changed", mode: "inspector" })
                }
              >
                <PanelRightOpen size={16} aria-hidden="true" />
                Phase inspector
              </button>
            </div>

            {state.railMode === "manager" ? (
              <ManagerPanel
                projectID={projectID}
                conversationID={conversationID}
                graphRevision={state.snapshot?.revision ?? 0}
                selectedEndpoint={state.selectedEndpoint}
                refreshCursor={state.snapshot?.cursor}
              />
            ) : state.selectedEndpoint ? (
              <EndpointInspectorPanel
                projectID={projectID}
                endpoint={state.selectedEndpoint}
                generation={state.selectedGeneration}
                refreshCursor={state.snapshot?.cursor}
              />
            ) : (
              <div className="rail-empty">
                <PanelRightOpen size={20} aria-hidden="true" />
                <p>
                  选择一个 plan、execute 或 verify 节点来检查其 Invocation
                  上下文。
                </p>
              </div>
            )}
          </aside>
        </main>

        <p className="sr-only" aria-live="polite">
          {state.announcement}
        </p>
      </div>
    </MotionConfig>
  );
}
