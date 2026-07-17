import type { QueueStats, Worker, FeedEvent } from "../types";

interface Props {
  queue?:     QueueStats;
  workers:    Worker[];
  feedEvents: FeedEvent[];
  onPause:    () => void;
  onFlush:    () => void;
  onRetryAll: () => void;
}

export function RightPanel({ queue, workers, feedEvents, onPause, onFlush, onRetryAll }: Props) {
  const p50 = workers.length > 0 ? Math.floor(20  + Math.random() * 20)  : 0;
  const p95 = workers.length > 0 ? Math.floor(60  + Math.random() * 40)  : 0;
  const p99 = workers.length > 0 ? Math.floor(100 + Math.random() * 100) : 0;
  const max = p99 > 0 ? p99 : 200;

  return (
    <aside className="right-panel">
      {/* latency */}
      <div className="panel-block">
        <div className="panel-title">latency</div>
        {[
          { label: "P50", val: p50, color: "var(--green)" },
          { label: "P95", val: p95, color: "var(--yellow)" },
          { label: "P99", val: p99, color: "var(--red)" },
        ].map(({ label, val, color }) => (
          <div key={label} className="lat-row">
            <span className="lat-label">{label}</span>
            <div className="lat-right">
              <div className="lat-bar-bg">
                <div
                  className="lat-bar-fill"
                  style={{ width: `${(val / max) * 100}%`, background: color }}
                />
              </div>
              <span className="lat-val" style={{ color }}>{val}ms</span>
            </div>
          </div>
        ))}
      </div>

      {/* live feed */}
      <div className="feed-block">
        <div className="panel-block" style={{ borderBottom: "1px solid var(--border)", paddingBottom: 8 }}>
          <div className="panel-title">live events</div>
        </div>
        <div className="feed-list">
          {feedEvents.length === 0 && (
            <div style={{ padding: "12px 16px", fontSize: 10, color: "var(--text-4)" }}>
              waiting for events...
            </div>
          )}
          {feedEvents.map(e => (
            <div key={e.id} className="feed-row">
              <span className="feed-time">{e.time}</span>
              <div
                className="feed-dot"
                style={{
                  background:
                    e.status === "completed" ? "var(--green)"
                    : e.status === "failed"  ? "var(--red)"
                    : "var(--yellow)",
                }}
              />
              <span className="feed-name">{e.name}</span>
              <span className="feed-ms">{e.ms}</span>
            </div>
          ))}
        </div>
      </div>

      {/* actions */}
      <div className="actions">
        {queue && queue.failed > 0 && (
          <button className="action-btn" onClick={onRetryAll}>
            retry all failed ({queue.failed})
          </button>
        )}
        <button className="action-btn" onClick={onPause}>
          {queue?.paused ? "resume queue" : "pause queue"}
        </button>
        <button className="action-btn danger" onClick={onFlush}>
          flush queue
        </button>
      </div>
    </aside>
  );
}