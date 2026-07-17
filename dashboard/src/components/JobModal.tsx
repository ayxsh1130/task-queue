import type { Job } from "../types";

interface Props {
  job:     Job;
  onClose: () => void;
}

function fmt(ts: number | null) {
  if (!ts) return "—";
  return new Date(ts * 1000).toLocaleString();
}

export function JobModal({ job, onClose }: Props) {
  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal" onClick={e => e.stopPropagation()}>
        <div className="modal-header">
          <span className="modal-title">{job.name}</span>
          <button className="modal-close" onClick={onClose}>×</button>
        </div>

        <div className="modal-body">
          <div className="modal-field">
            <div className="modal-label">Job ID</div>
            <div className="modal-value" style={{ fontFamily: "monospace", color: "var(--text-3)" }}>{job.id}</div>
          </div>

          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, marginBottom: 16 }}>
            <div>
              <div className="modal-label">Status</div>
              <span className={`job-badge badge-${job.status}`} style={{ fontSize: 11 }}>{job.status}</span>
            </div>
            <div>
              <div className="modal-label">Queue</div>
              <div className="modal-value">{job.queue}</div>
            </div>
            <div>
              <div className="modal-label">Attempts</div>
              <div className="modal-value">{job.attempts} / {job.max_attempts}</div>
            </div>
            <div>
              <div className="modal-label">Priority</div>
              <div className="modal-value">{job.priority}</div>
            </div>
            <div>
              <div className="modal-label">Created</div>
              <div className="modal-value" style={{ fontSize: 11 }}>{fmt(job.created_at)}</div>
            </div>
            <div>
              <div className="modal-label">Finished</div>
              <div className="modal-value" style={{ fontSize: 11 }}>{fmt(job.finished_at)}</div>
            </div>
          </div>

          <div className="modal-field">
            <div className="modal-label">Payload</div>
            <pre className="modal-code">{JSON.stringify(job.data, null, 2)}</pre>
          </div>

          {job.result && (
            <div className="modal-field">
              <div className="modal-label">Result</div>
              <pre className="modal-code">{JSON.stringify(job.result, null, 2)}</pre>
            </div>
          )}

          {job.error && (
            <div className="modal-field">
              <div className="modal-label">Error</div>
              <pre className="modal-code" style={{ color: "var(--red)" }}>{job.error}</pre>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}