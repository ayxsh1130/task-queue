import type { QueueStats } from "../types";

interface Props {
  stats?: QueueStats;
}

function fmt(n: number) {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000)     return `${(n / 1_000).toFixed(1)}k`;
  return String(n);
}

export function StatsRow({ stats }: Props) {
  if (!stats) {
    return (
      <div className="stats-row">
        {["waiting","active","completed","failed","delayed"].map(s => (
          <div key={s} className="stat-cell">
            <div className="stat-val" style={{ color: "var(--text-4)" }}>—</div>
            <div className="stat-lbl">{s}</div>
          </div>
        ))}
      </div>
    );
  }

  return (
    <div className="stats-row">
      <div className="stat-cell">
        <div className={`stat-val c-waiting`}>{fmt(stats.waiting)}</div>
        <div className="stat-lbl">waiting</div>
      </div>
      <div className="stat-cell">
        <div className={`stat-val c-active`}>{fmt(stats.active)}</div>
        <div className="stat-lbl">active</div>
      </div>
      <div className="stat-cell">
        <div className={`stat-val c-completed`}>{fmt(stats.completed)}</div>
        <div className="stat-lbl">completed</div>
      </div>
      <div className="stat-cell">
        <div className={`stat-val c-failed`}>{fmt(stats.failed)}</div>
        <div className="stat-lbl">failed</div>
      </div>
      <div className="stat-cell">
        <div className={`stat-val c-delayed`}>{fmt(stats.delayed)}</div>
        <div className="stat-lbl">delayed</div>
      </div>
    </div>
  );
}