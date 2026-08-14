import { FormEvent, useCallback, useEffect, useState } from "react";
import { motion } from "motion/react";
import { ArrowUp, Braces, MessageSquareText } from "lucide-react";
import {
  ApiError,
  getManagerConversation,
  submitManagerMessage,
} from "../../api/client";
import type { EndpointRef, ManagerConversation } from "../../api/types";

interface Props {
  projectID: string;
  conversationID: string;
  graphRevision: number;
  selectedEndpoint?: EndpointRef;
  refreshCursor?: string;
}

type ManagerIntent = "orchestrate" | "hold" | "resume";

const managerIntents: Array<{ value: ManagerIntent; label: string }> = [
  { value: "orchestrate", label: "调整编排" },
  { value: "hold", label: "暂停 Phase" },
  { value: "resume", label: "恢复 Phase" },
];

export function ManagerPanel({
  projectID,
  conversationID,
  graphRevision,
  selectedEndpoint,
  refreshCursor,
}: Props) {
  const [conversation, setConversation] = useState<ManagerConversation>();
  const [body, setBody] = useState("");
  const [intent, setIntent] = useState<ManagerIntent>("orchestrate");
  const [pending, setPending] = useState(false);
  const [feedback, setFeedback] = useState<string>();

  const refresh = useCallback(async () => {
    try {
      setConversation(await getManagerConversation(projectID, conversationID));
    } catch (error) {
      if (!(error instanceof ApiError && error.status === 404)) {
        setFeedback(
          error instanceof Error ? error.message : "无法读取 Manager 对话",
        );
      }
    }
  }, [conversationID, projectID]);

  useEffect(() => {
    void refresh();
  }, [refresh, refreshCursor]);

  async function submit(event: FormEvent) {
    event.preventDefault();
    const message = body.trim();
    if (!message || pending) return;
    setPending(true);
    setFeedback(
      "消息正在持久化为 ManagerInputRef；协调图仍以当前 revision 为准",
    );
    try {
      const result = await submitManagerMessage({
        projectID,
        conversationID,
        body: message,
        intent,
        selectedEndpoint,
        observedGraphRevision: graphRevision,
      });
      setBody("");
      setFeedback(
        `已接受 ManagerInputRef ${result.manager_input_ref}，等待 Task Manager 决策`,
      );
      await refresh();
    } catch (error) {
      setFeedback(
        error instanceof Error ? error.message : "Manager 消息提交失败",
      );
    } finally {
      setPending(false);
    }
  }

  return (
    <motion.section
      className="rail-panel manager-panel"
      initial={{ opacity: 0, x: 8 }}
      animate={{ opacity: 1, x: 0 }}
      aria-labelledby="manager-heading"
    >
      <header className="rail-panel-header">
        <div>
          <p className="eyebrow">Indirect graph control</p>
          <h2 id="manager-heading">Task Manager</h2>
        </div>
        <span className="revision-label">seen rev {graphRevision}</span>
      </header>

      <div className="manager-context">
        <Braces size={15} aria-hidden="true" />
        {selectedEndpoint ? (
          <span>
            Context: <code>{selectedEndpoint.task_id}</code> /{" "}
            {selectedEndpoint.endpoint_id}
          </span>
        ) : (
          <span>未选择 Phase；消息作用于项目级 Manager 上下文</span>
        )}
      </div>

      <div className="conversation" aria-label="Manager conversation">
        {conversation?.messages.length ? (
          conversation.messages.map((entry) => (
            <article
              key={entry.entry_id}
              className={`conversation-entry entry-${entry.kind}`}
            >
              <div className="conversation-entry-meta">
                <span>{entry.kind.replace("_", " ")}</span>
                <time dateTime={entry.created_at}>
                  {new Date(entry.created_at).toLocaleTimeString()}
                </time>
              </div>
              {entry.body ? <p>{entry.body}</p> : null}
              <div className="reference-row">
                {entry.manager_input_ref ? (
                  <code>Input {entry.manager_input_ref}</code>
                ) : null}
                {entry.decision_ref ? (
                  <code>Decision {entry.decision_ref}</code>
                ) : null}
                {entry.graph_revision !== undefined ? (
                  <code>rev {entry.graph_revision}</code>
                ) : null}
                {entry.disposition ? (
                  <span
                    className={`status-label disposition-${entry.disposition}`}
                  >
                    {entry.disposition}
                  </span>
                ) : null}
              </div>
            </article>
          ))
        ) : (
          <div className="rail-empty compact">
            <MessageSquareText size={20} aria-hidden="true" />
            <p>
              可以描述 hold、resume、重排或增加前置。UI
              不会把文本解析成图写操作。
            </p>
          </div>
        )}
      </div>

      <form className="manager-composer" onSubmit={submit}>
        <fieldset className="manager-intents">
          <legend>操作意图</legend>
          <div role="radiogroup" aria-label="Manager 操作意图">
            {managerIntents.map((option) => {
              const requiresEndpoint = option.value !== "orchestrate";
              return (
                <label
                  key={option.value}
                  className={intent === option.value ? "is-active" : undefined}
                >
                  <input
                    type="radio"
                    name="manager-intent"
                    value={option.value}
                    checked={intent === option.value}
                    disabled={requiresEndpoint && !selectedEndpoint}
                    onChange={() => setIntent(option.value)}
                  />
                  {option.label}
                </label>
              );
            })}
          </div>
          <small>
            暂停与恢复只作用于当前选中的
            Phase；自然语言不会被推断成生命周期控制。
          </small>
        </fieldset>
        <label htmlFor="manager-message">给 Task Manager 的消息</label>
        <textarea
          id="manager-message"
          value={body}
          onChange={(event) => setBody(event.target.value)}
          placeholder="例如：先 hold 当前 execute，等依赖证据到齐后再恢复。"
          maxLength={50000}
          rows={4}
        />
        <div className="composer-footer">
          <span>只发送自然语言、EndpointRef 与所见 revision</span>
          <button
            className="primary-button"
            type="submit"
            disabled={
              pending ||
              !body.trim() ||
              (intent !== "orchestrate" && !selectedEndpoint)
            }
          >
            <ArrowUp size={16} aria-hidden="true" />
            {pending ? "Sending" : "Send"}
          </button>
        </div>
      </form>
      <p className="manager-feedback" aria-live="polite">
        {feedback}
      </p>
    </motion.section>
  );
}
