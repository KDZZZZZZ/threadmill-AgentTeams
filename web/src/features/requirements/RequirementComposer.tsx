import { FormEvent, useState } from "react";
import { motion } from "motion/react";
import { CircleCheck, ListChecks, Send, Workflow } from "lucide-react";
import { submitRequirement } from "../../api/client";
import type { RequirementCreateResponse } from "../../api/types";

interface Props {
  projectID: string;
  conversationID: string;
  graphRevision: number;
  hasTasks: boolean;
  onAccepted: () => Promise<void>;
}

function lines(value: string): string[] | undefined {
  const items = value
    .split("\n")
    .map((item) => item.trim())
    .filter(Boolean);
  return items.length ? items : undefined;
}

export function RequirementComposer({
  projectID,
  conversationID,
  graphRevision,
  hasTasks,
  onAccepted,
}: Props) {
  const [body, setBody] = useState("");
  const [motivation, setMotivation] = useState("");
  const [constraints, setConstraints] = useState("");
  const [acceptance, setAcceptance] = useState("");
  const [pending, setPending] = useState(false);
  const [result, setResult] = useState<RequirementCreateResponse>();
  const [acceptedAtRevision, setAcceptedAtRevision] = useState<number>();
  const [error, setError] = useState<string>();

  const graphAdvanced =
    acceptedAtRevision !== undefined && graphRevision > acceptedAtRevision;

  async function submit(event: FormEvent) {
    event.preventDefault();
    const requirement = body.trim();
    if (!requirement || pending) return;

    setPending(true);
    setResult(undefined);
    setError(undefined);
    try {
      const accepted = await submitRequirement({
        projectID,
        conversationID,
        body: requirement,
        motivation: motivation.trim() || undefined,
        constraints: lines(constraints),
        acceptance: lines(acceptance),
      });
      setResult(accepted);
      setAcceptedAtRevision(graphRevision);
      setBody("");
      setMotivation("");
      setConstraints("");
      setAcceptance("");
      await onAccepted();
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "需求提交失败");
    } finally {
      setPending(false);
    }
  }

  return (
    <section
      className="requirement-region"
      aria-labelledby="requirement-heading"
    >
      <div className="requirement-heading">
        <div>
          <h2 id="requirement-heading">提交需求</h2>
          <p>
            请求先持久化并交给 Task Manager。只有服务端发布新 graph revision
            后，协调图才会变化。
          </p>
        </div>
        <span className="revision-label">
          current graph rev {graphRevision}
        </span>
      </div>

      <form className="requirement-form" onSubmit={submit}>
        <div className="requirement-primary-field">
          <label htmlFor="requirement-body">任务目标</label>
          <textarea
            id="requirement-body"
            value={body}
            onChange={(event) => setBody(event.target.value)}
            placeholder="描述需要 Task Manager 拆分和协调的真实工作。"
            maxLength={50000}
            rows={hasTasks ? 2 : 4}
            required
          />
        </div>

        <details className="requirement-details">
          <summary>
            <ListChecks size={16} aria-hidden="true" />
            补充动机、约束和验收条件
          </summary>
          <div className="requirement-detail-grid">
            <div>
              <label htmlFor="requirement-motivation">动机</label>
              <textarea
                id="requirement-motivation"
                value={motivation}
                onChange={(event) => setMotivation(event.target.value)}
                maxLength={20000}
                rows={3}
              />
            </div>
            <div>
              <label htmlFor="requirement-constraints">约束（每行一项）</label>
              <textarea
                id="requirement-constraints"
                value={constraints}
                onChange={(event) => setConstraints(event.target.value)}
                rows={3}
              />
            </div>
            <div>
              <label htmlFor="requirement-acceptance">
                验收条件（每行一项）
              </label>
              <textarea
                id="requirement-acceptance"
                value={acceptance}
                onChange={(event) => setAcceptance(event.target.value)}
                rows={3}
              />
            </div>
          </div>
        </details>

        <div className="requirement-actions">
          <p>
            {pending
              ? "正在等待生产入口持久化并确认接收。"
              : "接收成功不等于图已应用，界面不会乐观创建节点。"}
          </p>
          <button
            className="primary-button"
            type="submit"
            disabled={pending || !body.trim()}
          >
            <Send size={16} aria-hidden="true" />
            {pending ? "提交中" : "交给 Task Manager"}
          </button>
        </div>
      </form>

      {result ? (
        <motion.div
          className={`requirement-status ${graphAdvanced ? "is-updated" : "is-accepted"}`}
          initial={{ opacity: 0, y: 4 }}
          animate={{ opacity: 1, y: 0 }}
          role="status"
        >
          {graphAdvanced ? (
            <CircleCheck size={18} aria-hidden="true" />
          ) : (
            <Workflow size={18} aria-hidden="true" />
          )}
          <div>
            <strong>
              {graphAdvanced
                ? `检测到新的权威协调图 revision ${graphRevision}`
                : "Task Manager 已接收，等待权威协调图更新"}
            </strong>
            <span>
              Input{" "}
              <code title={result.manager_input_ref}>
                {result.manager_input_ref}
              </code>
              <span aria-hidden="true">/</span> Invocation{" "}
              <code title={result.invocation_ref}>{result.invocation_ref}</code>
            </span>
          </div>
        </motion.div>
      ) : null}

      {error ? (
        <p className="requirement-error" role="alert">
          <strong>需求未被接收。</strong> {error}
        </p>
      ) : null}
    </section>
  );
}
