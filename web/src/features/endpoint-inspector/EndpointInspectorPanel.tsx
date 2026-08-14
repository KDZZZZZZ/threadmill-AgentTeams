import { useEffect, useState } from "react";
import { motion } from "motion/react";
import { Boxes, Database, EyeOff, GitBranch, RefreshCw } from "lucide-react";
import { getEndpointInspector } from "../../api/client";
import type { EndpointInspector, EndpointRef } from "../../api/types";

interface Props {
  projectID: string;
  endpoint: EndpointRef;
  generation?: number;
  refreshCursor?: string;
}

export function EndpointInspectorPanel({
  projectID,
  endpoint,
  generation,
  refreshCursor,
}: Props) {
  const [inspector, setInspector] = useState<EndpointInspector>();
  const [error, setError] = useState<string>();

  useEffect(() => {
    let active = true;
    setInspector(undefined);
    setError(undefined);
    void getEndpointInspector(projectID, endpoint, generation)
      .then((result) => {
        if (active) setInspector(result);
      })
      .catch((reason: unknown) => {
        if (active)
          setError(
            reason instanceof Error ? reason.message : "无法读取 Phase 检查器",
          );
      });
    return () => {
      active = false;
    };
  }, [endpoint, generation, projectID, refreshCursor]);

  if (error) {
    return (
      <div className="rail-empty" role="alert">
        <EyeOff size={20} aria-hidden="true" />
        <p>{error}</p>
        <span>
          该 Invocation 可能不存在、已过期，或正文不在当前项目权限内。
        </span>
      </div>
    );
  }

  if (!inspector) {
    return (
      <div className="rail-empty" role="status">
        <RefreshCw className="spin-once" size={20} aria-hidden="true" />
        <p>正在读取该 Invocation 的有效上下文投影…</p>
      </div>
    );
  }

  return (
    <motion.section
      className="rail-panel inspector-panel"
      initial={{ opacity: 0, x: 8 }}
      animate={{ opacity: 1, x: 0 }}
      aria-labelledby="inspector-heading"
    >
      <header className="rail-panel-header">
        <div>
          <p className="eyebrow">Phase Endpoint</p>
          <h2 id="inspector-heading">{inspector.endpoint.endpoint_id}</h2>
          <code>{inspector.endpoint.task_id}</code>
        </div>
        <span
          className={`status-label state-${inspector.invocation?.status ?? "pending"}`}
        >
          {inspector.invocation?.status ?? "not started"}
        </span>
      </header>

      <dl className="inspector-meta">
        <div>
          <dt>Generation</dt>
          <dd>{inspector.generation}</dd>
        </div>
        <div>
          <dt>Graph revision</dt>
          <dd>{inspector.graph_revision}</dd>
        </div>
        <div className="wide">
          <dt>Invocation</dt>
          <dd title={inspector.invocation?.invocation_id}>
            {inspector.invocation?.invocation_id ?? "No Invocation"}
          </dd>
        </div>
      </dl>

      <section
        className="inspector-section"
        aria-labelledby="subscriptions-heading"
      >
        <div className="inspector-section-heading">
          <GitBranch size={17} aria-hidden="true" />
          <div>
            <h3 id="subscriptions-heading">Subscription subgraphs</h3>
            <p>Runtime 物化所有 active subscription 的子图并集。</p>
          </div>
        </div>
        {inspector.subscriptions.length ? (
          <ul className="subscription-list">
            {inspector.subscriptions.map((subscription) => (
              <li
                key={subscription.subscription_id}
                className={!subscription.active ? "is-inactive" : ""}
              >
                <div>
                  <code title={subscription.subscription_id}>
                    {subscription.subscription_id}
                  </code>
                  <span
                    className={`status-label ${subscription.active ? "state-running" : "state-stopped"}`}
                  >
                    {subscription.active ? "active" : "inactive"}
                  </span>
                </div>
                <span className="subscription-source">
                  {subscription.source ?? "explicit"}
                </span>
                <div className="token-list">
                  {subscription.subgraph_ids.map((id) => (
                    <code key={id}>{id}</code>
                  ))}
                </div>
              </li>
            ))}
          </ul>
        ) : (
          <p className="specific-empty">
            {inspector.invocation
              ? "该 Invocation 没有可见的有效订阅。"
              : "该 Phase 尚未开始，因此没有 Invocation 级订阅。"}
          </p>
        )}
      </section>

      <section className="inspector-section" aria-labelledby="context-heading">
        <div className="inspector-section-heading">
          <Database size={17} aria-hidden="true" />
          <div>
            <h3 id="context-heading">Context Slice</h3>
            <p>
              {inspector.context_slice ? (
                <>
                  <code>{inspector.context_slice.context_slice_ref}</code> · rev{" "}
                  {inspector.context_slice.revision}
                </>
              ) : (
                "尚无物化切片"
              )}
            </p>
          </div>
        </div>
        {inspector.context_slice?.nodes.length ? (
          <ul className="context-node-list">
            {inspector.context_slice.nodes.map((node) => (
              <li key={node.node_id}>
                <span className={`context-kind kind-${node.kind}`}>
                  {node.kind}
                </span>
                <p>{node.statement}</p>
                <code title={node.node_id}>{node.node_id}</code>
              </li>
            ))}
          </ul>
        ) : (
          <p className="specific-empty">
            {inspector.invocation
              ? "该 Invocation 的物化 Context Slice 为空。"
              : "该 Phase 尚未开始，因此没有物化 Context Slice。"}
          </p>
        )}
        {inspector.context_slice?.frontier?.length ? (
          <div className="token-list" aria-label="Context frontier">
            {inspector.context_slice.frontier.map((ref) => (
              <code key={ref}>{ref}</code>
            ))}
          </div>
        ) : null}
        {inspector.context_slice?.omitted.length ? (
          <div className="omitted-list" aria-label="省略或脱敏的上下文">
            {inspector.context_slice.omitted.map((item) => (
              <span key={item.reason}>
                {item.reason}: {item.count}
              </span>
            ))}
          </div>
        ) : null}
      </section>

      <section className="inspector-section" aria-labelledby="memory-heading">
        <div className="inspector-section-heading">
          <Boxes size={17} aria-hidden="true" />
          <div>
            <h3 id="memory-heading">TaskMemoryBuffer</h3>
            <p>
              只显示由当前 Invocation 创建的候选；它们尚不是 Context Graph
              节点。
            </p>
          </div>
        </div>
        {inspector.task_memory_buffer?.candidates.length ? (
          <ul className="candidate-list">
            {inspector.task_memory_buffer.candidates.map(
              ({ candidate_id, candidate }) => (
                <li key={candidate_id}>
                  <div>
                    <span className="candidate-label">candidate</span>
                    <span className={`context-kind kind-${candidate.kind}`}>
                      {candidate.kind}
                    </span>
                  </div>
                  <p>{candidate.statement}</p>
                  <code title={candidate_id}>{candidate_id}</code>
                </li>
              ),
            )}
          </ul>
        ) : (
          <p className="specific-empty">
            {inspector.invocation
              ? "该 Invocation 尚未创建 TaskMemoryBuffer 候选。"
              : "该 Phase 尚未开始，因此没有 Invocation 绑定的 TaskMemoryBuffer 视图。"}
          </p>
        )}
        {inspector.task_memory_buffer?.omitted?.length ? (
          <div className="omitted-list" aria-label="省略或脱敏的候选">
            {inspector.task_memory_buffer.omitted.map((item) => (
              <span key={item.reason}>
                {item.reason}: {item.count}
              </span>
            ))}
          </div>
        ) : null}
      </section>
    </motion.section>
  );
}
