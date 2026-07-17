import { useState, useEffect, useRef } from "react";
import { useWebSocket } from "./hooks/useWebSocket";
import { useApi } from "./hooks/useApi";
import type { QueueStats, Worker, ThroughputPoint, Job } from "./types";
import { Sidebar } from "./components/Sidebar";
import { StatsRow } from "./components/StatsRow";
import { ThroughputChart } from "./components/ThroughputChart";
import { JobFeed } from "./components/JobFeed";
import { RightPanel } from "./components/RightPanel";
import { JobModal } from "./components/JobModal";
import "./styles.css";

export default function App() {
  const { message, connected }          = useWebSocket();
  const api                             = useApi();

  const [queues, setQueues]             = useState<QueueStats[]>([]);
  const [workers, setWorkers]           = useState<Worker[]>([]);
  const [throughput, setThroughput]     = useState(0);
  const [throughputHistory, setHistory] = useState<ThroughputPoint[]>([]);
  const [selectedQueue, setSelected]    = useState<string>("default");
  const [jobs, setJobs]                 = useState<Job[]>([]);
  const [jobStatus, setJobStatus]       = useState<string>("waiting");
  const [selectedJob, setSelectedJob]   = useState<Job | null>(null);
  const [feedEvents, setFeedEvents]     = useState<{id:string;name:string;status:string;ms:string;time:string}[]>([]);

  const prevCompleted = useRef<Record<string, number>>({});

  // handle websocket messages
  useEffect(() => {
    if (!message) return;

    setQueues(message.queues);
    setWorkers(message.workers);
    setThroughput(message.throughput);

    // build throughput history
    const now = new Date();
    const label = `${now.getHours()}:${String(now.getMinutes()).padStart(2,"0")}:${String(now.getSeconds()).padStart(2,"0")}`;
    setHistory(h => [...h.slice(-59), { time: label, value: message.throughput }]);

    // generate feed events from completed delta
    message.queues.forEach((q: QueueStats) => {
      const prev = prevCompleted.current[q.name] ?? q.completed;
      const delta = q.completed - prev;
      if (delta > 0 && delta < 10) {
        for (let i = 0; i < delta; i++) {
          setFeedEvents(f => [{
            id: Math.random().toString(36).slice(2,8),
            name: "job",
            status: "completed",
            ms: `${Math.floor(Math.random() * 200)}ms`,
            time: label,
          }, ...f.slice(0, 49)]);
        }
      }
      prevCompleted.current[q.name] = q.completed;
    });

    // auto select first queue
    if (!selectedQueue && message.queues.length > 0) {
      setSelected(message.queues[0].name);
    }
  }, [message]);

  // load jobs when queue or status changes
  useEffect(() => {
    if (!selectedQueue) return;
    api.getJobs(selectedQueue, jobStatus).then(setJobs).catch(() => {});
  }, [selectedQueue, jobStatus, queues]);

  const selectedQueueData = queues.find(q => q.name === selectedQueue);

  const handleRetry = async (jobId: string) => {
    await api.retryJob(selectedQueue, jobId);
    api.getJobs(selectedQueue, jobStatus).then(setJobs);
  };

  const handlePause = async () => {
    if (!selectedQueueData) return;
    if (selectedQueueData.paused) {
      await api.resumeQueue(selectedQueue);
    } else {
      await api.pauseQueue(selectedQueue);
    }
  };

  const handleFlush = async () => {
    if (!window.confirm(`Flush all jobs in queue "${selectedQueue}"? This cannot be undone.`)) return;
    await api.flushQueue(selectedQueue);
  };

  const handleJobClick = async (jobId: string) => {
    const job = await api.getJob(selectedQueue, jobId);
    setSelectedJob(job);
  };

  return (
    <div className="app">
      {/* top bar */}
      <header className="topbar">
        <div className="topbar-left">
          <span className="logo">tq-engine</span>
          <span className={`conn-badge ${connected ? "conn-ok" : "conn-err"}`}>
            {connected ? "connected" : "reconnecting..."}
          </span>
        </div>
        <div className="topbar-center">
          <span className="tps-val">{throughput.toLocaleString()}</span>
          <span className="tps-label">jobs/sec</span>
        </div>
        <div className="topbar-right">
          <span className="pill pill-workers">{workers.length} workers</span>
          {selectedQueueData?.paused && <span className="pill pill-paused">paused</span>}
        </div>
      </header>

      {/* main layout */}
      <div className="layout">
        <Sidebar
          queues={queues}
          workers={workers}
          selected={selectedQueue}
          onSelect={setSelected}
        />

        <main className="main">
          <StatsRow stats={selectedQueueData} />

          <div className="chart-section">
            <div className="section-header">
              <span className="section-title">throughput</span>
              <span className="section-sub">jobs/sec — last 60s</span>
            </div>
            <ThroughputChart data={throughputHistory} />
          </div>

          <div className="jobs-section">
            <div className="section-header">
              <span className="section-title">jobs</span>
              <div className="status-tabs">
                {["waiting","active","completed","failed","delayed"].map(s => (
                  <button
                    key={s}
                    className={`status-tab ${jobStatus === s ? "active" : ""} tab-${s}`}
                    onClick={() => setJobStatus(s)}
                  >
                    {s}
                  </button>
                ))}
              </div>
            </div>
            <JobFeed
              jobs={jobs}
              onRetry={handleRetry}
              onJobClick={handleJobClick}
            />
          </div>
        </main>

        <RightPanel
          queue={selectedQueueData}
          workers={workers.filter(w => w.queue === selectedQueue)}
          feedEvents={feedEvents}
          onPause={handlePause}
          onFlush={handleFlush}
          onRetryAll={() => api.retryAllFailed(selectedQueue).then(() => api.getJobs(selectedQueue, jobStatus).then(setJobs))}
        />
      </div>

      {selectedJob && (
        <JobModal job={selectedJob} onClose={() => setSelectedJob(null)} />
      )}
    </div>
  );
}