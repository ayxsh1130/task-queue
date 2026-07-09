"""
models.py — Job schema and Redis key helpers

Every other module imports from here. Nothing in this file talks to Redis
directly — it's pure data definitions. This makes it easy to test in isolation.

Redis data layout:
  queue:{name}:jobs:{id}    HASH    — full job data
  queue:{name}:waiting      ZSET    — job IDs scored by priority (lower = first)
  queue:{name}:active       LIST    — job IDs currently being processed
  queue:{name}:completed    ZSET    — job IDs scored by finish timestamp
  queue:{name}:failed       ZSET    — job IDs scored by fail timestamp
  queue:{name}:delayed      ZSET    — job IDs scored by scheduled run timestamp
  queue:{name}:lock:{id}    STRING  — worker lock (TTL = WORKER_LOCK_TTL)
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any


# ---------------------------------------------------------------------------
# Job status
# ---------------------------------------------------------------------------

class JobStatus(str, Enum):
    """All possible states a job can be in.

    Using (str, Enum) means the values serialise to plain strings, so they
    round-trip cleanly through Redis HSET/HGET without extra conversion.
    """
    WAITING   = "waiting"    # sitting in the queue, not yet picked up
    ACTIVE    = "active"     # a worker has it and is processing it right now
    COMPLETED = "completed"  # finished successfully
    FAILED    = "failed"     # ran out of retry attempts
    DELAYED   = "delayed"    # scheduled for a future time


# ---------------------------------------------------------------------------
# Job model
# ---------------------------------------------------------------------------

@dataclass
class Job:
    """A single unit of work in the queue.

    Design notes:
    - `data` is the user payload — anything JSON-serialisable.
    - `priority` is a ZSET score: lower number = higher priority (picked first).
      0 = critical, 10 = normal, 100 = low. Default is 10.
    - `delay` is seconds from now before the job becomes eligible to run.
      0 = run immediately.
    - `max_attempts` controls how many times the job is retried before it moves
      to the failed queue. Set to 1 to disable retries.
    - `attempts` is incremented by the worker each time it tries the job.
    - `result` and `error` are written by the worker on completion/failure.
    """

    # --- identity ---
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""                       # human-readable job type, e.g. "send_email"
    queue: str = "default"              # which queue this job belongs to

    # --- payload ---
    data: dict[str, Any] = field(default_factory=dict)

    # --- scheduling ---
    priority: int = 10                   # lower = picked first
    delay: float = 0                     # seconds from now (0 = immediate)

    # --- retry ---
    attempts: int = 0                    # how many times we've tried so far
    max_attempts: int = 3               # give up after this many total attempts

    # --- status ---
    status: JobStatus = JobStatus.WAITING

    # --- output (written by worker) ---
    result: dict[str, Any] | None = None
    error: str | None = None            # last error message if failed

    # --- timestamps (Unix seconds) ---
    created_at: float = field(default_factory=time.time)
    started_at: float | None = None     # when the worker picked it up
    finished_at: float | None = None    # when it completed or failed
    run_at: float | None = None         # for delayed jobs: scheduled run time

    # ------------------------------------------------------------------
    # Serialisation helpers
    # (Redis stores everything as strings, so we need to/from dict)
    # ------------------------------------------------------------------

    def to_redis_dict(self) -> dict[str, str]:
        """Flatten the job to a dict of strings for Redis HSET."""
        import json
        d = asdict(self)
        d["status"] = self.status.value
        d["data"] = json.dumps(d["data"])
        d["result"] = json.dumps(d["result"]) if d["result"] is not None else ""
        d["error"] = d["error"] or ""
        d["started_at"] = str(d["started_at"] or "")
        d["finished_at"] = str(d["finished_at"] or "")
        d["run_at"] = str(d["run_at"] or "")
        # convert every value to string (Redis HSET requirement)
        return {k: str(v) for k, v in d.items()}

    @classmethod
    def from_redis_dict(cls, d: dict[str, str]) -> "Job":
        """Reconstruct a Job from the strings Redis returns via HGETALL."""
        import json

        def _float_or_none(v: str) -> float | None:
            return float(v) if v else None

        return cls(
            id=d["id"],
            name=d["name"],
            queue=d["queue"],
            data=json.loads(d["data"]),
            priority=int(d["priority"]),
            delay=float(d["delay"]),
            attempts=int(d["attempts"]),
            max_attempts=int(d["max_attempts"]),
            status=JobStatus(d["status"]),
            result=json.loads(d["result"]) if d["result"] else None,
            error=d["error"] or None,
            created_at=float(d["created_at"]),
            started_at=_float_or_none(d["started_at"]),
            finished_at=_float_or_none(d["finished_at"]),
            run_at=_float_or_none(d["run_at"]),
        )


# ---------------------------------------------------------------------------
# Redis key helpers
# ---------------------------------------------------------------------------

class RedisKeys:
    """Central place for all Redis key patterns.

    Always use these methods — never hardcode key strings in other modules.
    Changing the key scheme in one place updates the whole system.
    """

    @staticmethod
    def job(queue: str, job_id: str) -> str:
        """HASH — stores all fields of a job."""
        return f"queue:{queue}:jobs:{job_id}"

    @staticmethod
    def waiting(queue: str) -> str:
        """ZSET — job IDs scored by priority (lower score = dequeued first)."""
        return f"queue:{queue}:waiting"

    @staticmethod
    def active(queue: str) -> str:
        """LIST — job IDs currently being processed by workers."""
        return f"queue:{queue}:active"

    @staticmethod
    def completed(queue: str) -> str:
        """ZSET — job IDs scored by finish timestamp (for TTL trimming)."""
        return f"queue:{queue}:completed"

    @staticmethod
    def failed(queue: str) -> str:
        """ZSET — job IDs scored by fail timestamp."""
        return f"queue:{queue}:failed"

    @staticmethod
    def delayed(queue: str) -> str:
        """ZSET — job IDs scored by scheduled run timestamp."""
        return f"queue:{queue}:delayed"

    @staticmethod
    def lock(queue: str, job_id: str) -> str:
        """STRING with TTL — worker heartbeat lock to prevent double-processing."""
        return f"queue:{queue}:lock:{job_id}"

    @staticmethod
    def all_queues_pattern() -> str:
        """Glob pattern to find all queue waiting sets (used by the API for stats)."""
        return "queue:*:waiting"