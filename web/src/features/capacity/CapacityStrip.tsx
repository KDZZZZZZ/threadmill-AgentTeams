import { useState } from "react";
import {
  Activity,
  Minus,
  Plus,
  ServerCog,
  TimerReset,
  UsersRound,
} from "lucide-react";
import { adjustCapacity, ApiError } from "../../api/client";
import type { CapacityState } from "../../api/types";

interface Props {
  capacity: CapacityState;
  onCapacityAccepted: (capacity: CapacityState) => void;
  onConflict: () => Promise<void>;
}

export function CapacityStrip({
  capacity,
  onCapacityAccepted,
  onConflict,
}: Props) {
  const [pending, setPending] = useState(false);
  const [feedback, setFeedback] = useState<string>();

  async function submit(next: number) {
    if (pending || next < 0 || next === capacity.desired_concurrency) return;
    setPending(true);
    setFeedback(
      `正在请求并发目标 ${next}，当前权威值仍为 ${capacity.desired_concurrency}`,
    );
    try {
      const result = await adjustCapacity(
        capacity.project_id,
        capacity.revision,
        next,
      );
      onCapacityAccepted(result.capacity);
      setFeedback(
        `服务端已接受，并发目标为 ${result.capacity.desired_concurrency}`,
      );
    } catch (error) {
      if (error instanceof ApiError && error.status === 409) {
        setFeedback("容量 revision 已变化，正在刷新服务端权威值");
        await onConflict();
      } else {
        setFeedback(error instanceof Error ? error.message : "容量调整失败");
      }
    } finally {
      setPending(false);
    }
  }

  return (
    <section className="capacity-strip" aria-labelledby="capacity-heading">
      <div className="capacity-title">
        <ServerCog size={18} aria-hidden="true" />
        <div>
          <h2 id="capacity-heading">Agent capacity</h2>
          <span>吞吐控制，不改变协调图语义</span>
        </div>
      </div>
      <dl className="capacity-values">
        <div>
          <dt>Desired</dt>
          <dd>{capacity.desired_concurrency}</dd>
        </div>
        <div>
          <dt>Healthy</dt>
          <dd>
            <Activity size={14} aria-hidden="true" />{" "}
            {capacity.healthy_capacity}
          </dd>
        </div>
        <div>
          <dt>Active</dt>
          <dd>
            <UsersRound size={14} aria-hidden="true" />{" "}
            {capacity.active_invocations}
          </dd>
        </div>
        <div>
          <dt>Waiting</dt>
          <dd>
            <TimerReset size={14} aria-hidden="true" />{" "}
            {capacity.waiting_invocations}
          </dd>
        </div>
      </dl>
      <div className="capacity-actions" aria-label="调整 desired concurrency">
        <button
          type="button"
          className="icon-button"
          disabled={pending || capacity.desired_concurrency === 0}
          onClick={() => void submit(capacity.desired_concurrency - 1)}
          aria-label="减少一个 Agent 并发目标"
          title="减少 desired concurrency"
        >
          <Minus size={17} aria-hidden="true" />
        </button>
        <button
          type="button"
          className="icon-button primary-icon-button"
          disabled={pending}
          onClick={() => void submit(capacity.desired_concurrency + 1)}
          aria-label="增加一个 Agent 并发目标"
          title="增加 desired concurrency"
        >
          <Plus size={17} aria-hidden="true" />
        </button>
        <span className="capacity-revision">rev {capacity.revision}</span>
      </div>
      <p className="sr-only" aria-live="polite">
        {feedback}
      </p>
    </section>
  );
}
