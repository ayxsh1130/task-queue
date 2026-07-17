import type { Job } from "../types";

interface Props {
  jobs:       Job[];
  onRetry:    (id: string) => void;
  onJobClick: (id: string) => void;
}

function timeAgo(ts: number | null) {
  if (!ts) return "—";
  const diff = Date.now() / 1000 - ts;
  if (diff < 60)   return `${Math.floor(diff)}s ago`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  return `${Math.floor(diff / 3600)}h ago`;
}

export function JobFeed({ jobs, onRetry, onJobClick }: Props) {
  if (jobs.length === 0) {
    return (
      <div className="job-list">
        <div className="job-empty">no jobs found</div>
      </div>
    );
  }

  return (
    <div className="job-list">
      {jobs.map(job => (
        <div key={job.id} className="job-row" onClick={() => onJobClick(job.id)}>
          <span className="job-id">{job.id.slice(0, 8)}</span>
          <span className="job-name">{job.name}</span>

          {job.error && (
            <span className="job-error">{job.error}</span>
          )}

          <span className="job-attempts">
            {job.attempts}/{job.max_attempts}
          </span>

          <span className={`job-badge badge-${job.status}`}>
            {job.status}
          </span>

          <span style={{ fontSize: 10, color: "var(--text-4)", width: 56, textAlign: "right", flexShrink: 0 }}>
            {timeAgo(job.finished_at ?? job.started_at ?? job.created_at)}
          </span>

          {job.status === "failed" && (
            <button
              className="retry-btn"
              onClick={e => { e.stopPropagation(); onRetry(job.id); }}
            >
              retry
            </button>
          )}
        </div>
      ))}
    </div>
  );
}