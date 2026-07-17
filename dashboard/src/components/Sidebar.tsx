import type { QueueStats, Worker } from "../types";

interface Props {
  queues:   QueueStats[];
  workers:  Worker[];
  selected: string;
  onSelect: (name: string) => void;
}

function queueDot(q: QueueStats) {
  if (q.paused)          return "dot-yellow";
  if (q.failed > 10)     return "dot-red";
  if (q.waiting > 10000) return "dot-yellow";
  return "dot-green";
}

export function Sidebar({ queues, workers, selected, onSelect }: Props) {
  return (
    <aside className="sidebar">
      <div className="sidebar-section">
        <div className="sidebar-label">Queues</div>
        {queues.length === 0 && (
          <div style={{ padding: "8px 16px", fontSize: 11, color: "var(--text-4)" }}>
            no queues found
          </div>
        )}
        {queues.map(q => (
          <div
            key={q.name}
            className={`queue-item ${selected === q.name ? "selected" : ""}`}
            onClick={() => onSelect(q.name)}
          >
            <div className="queue-item-left">
              <div className={`queue-dot ${queueDot(q)}`} />
              <span className="queue-name">{q.name}</span>
            </div>
            <span className="queue-count">
              {q.waiting > 999 ? `${(q.waiting/1000).toFixed(1)}k` : q.waiting}
            </span>
          </div>
        ))}
      </div>

      <div className="sidebar-section">
        <div className="sidebar-label">Workers</div>
        {workers.length === 0 && (
          <div style={{ padding: "8px 16px", fontSize: 11, color: "var(--text-4)" }}>
            no active workers
          </div>
        )}
        {workers.slice(0, 8).map((w, i) => (
          <div key={w.job_id} className="worker-item">
            <div className="worker-avatar">W{i + 1}</div>
            <div className="worker-info">
              <div className="worker-name">worker-{i + 1}</div>
              <div className="worker-job">{w.job_name} #{w.job_id.slice(0, 6)}</div>
            </div>
            <span className="worker-ttl">{w.lock_ttl}s</span>
          </div>
        ))}
      </div>
    </aside>
  );
}