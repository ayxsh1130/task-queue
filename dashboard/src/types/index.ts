export interface QueueStats {
  name: string;
  waiting: number;
  active: number;
  completed: number;
  failed: number;
  delayed: number;
  total: number;
  paused: boolean;
}

export interface Job {
  id: string;
  name: string;
  queue: string;
  status: "waiting" | "active" | "completed" | "failed" | "delayed";
  data: Record<string, unknown>;
  result: Record<string, unknown> | null;
  error: string | null;
  attempts: number;
  max_attempts: number;
  priority: number;
  created_at: number;
  started_at: number | null;
  finished_at: number | null;
  run_at: number | null;
}

export interface Worker {
  queue: string;
  job_id: string;
  job_name: string;
  lock_ttl: number;
}

export interface WSMessage {
  type: "stats";
  queues: QueueStats[];
  workers: Worker[];
  throughput: number;
  timestamp: number;
}

export interface ThroughputPoint {
  time: string;
  value: number;
}

export interface FeedEvent {
  id: string;
  name: string;
  status: string;
  ms: string;
  time: string;
}