"""
reaper.py — Recovers stale jobs from crashed workers

Runs as a background loop every REAPER_POLL_INTERVAL seconds.
Scans the active list for jobs whose worker lock has expired
(meaning the worker crashed or was killed mid-job) and re-queues
them back to waiting so another worker can pick them up.

Without the reaper, a crashed worker would leave jobs stuck in
"active" forever — never completing, never failing, just lost.

Usage:
    reaper = Reaper()
    await reaper.start()  # runs forever until stop() is called
"""

from __future__ import annotations

import asyncio
import logging
import os

import redis.asyncio as aioredis
from dotenv import load_dotenv

from src.models import Job, JobStatus, RedisKeys

load_dotenv()

logger = logging.getLogger(__name__)


class Reaper:
    """Detects and recovers jobs from crashed workers."""

    def __init__(
        self,
        redis_url: str | None = None,
        poll_interval: float | None = None,
    ):
        self._url = redis_url or os.getenv("REDIS_URL", "redis://localhost:6379/0")
        self._poll_interval = poll_interval or float(os.getenv("REAPER_POLL_INTERVAL", 30))
        self._redis: aioredis.Redis | None = None
        self._running = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self):
        self._redis = await aioredis.from_url(self._url, decode_responses=True)
        logger.info(f"Reaper connected — poll interval={self._poll_interval}s")

    async def disconnect(self):
        self._running = False
        if self._redis:
            await self._redis.aclose()

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, *_):
        await self.disconnect()

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def start(self):
        """Start the reaper loop. Runs until stop() is called."""
        self._running = True
        logger.info("Reaper started")

        while self._running:
            try:
                recovered = await self._scan()
                if recovered > 0:
                    logger.info(f"Reaper recovered {recovered} stale job(s)")
            except Exception as e:
                logger.error(f"Reaper error: {e}")

            await asyncio.sleep(self._poll_interval)

    async def stop(self):
        self._running = False
        logger.info("Reaper stopped")

    # ------------------------------------------------------------------
    # Core logic
    # ------------------------------------------------------------------

    async def _scan(self) -> int:
        """Scan all queues for stale active jobs and recover them.

        Returns number of jobs recovered.
        """
        total_recovered = 0

        # find all queues that have active jobs
        queue_names = await self._get_active_queues()

        for queue in queue_names:
            recovered = await self._scan_queue(queue)
            total_recovered += recovered

        return total_recovered

    async def _scan_queue(self, queue: str) -> int:
        """Scan one queue's active list for stale jobs.

        For each job in the active list:
        - Check if its lock key still exists in Redis
        - If lock is GONE → worker crashed → re-queue the job
        - If lock EXISTS → worker is alive → leave it alone

        Returns number of jobs recovered in this queue.
        """
        active_key = RedisKeys.active(queue)

        # get all job IDs currently in the active list
        active_job_ids = await self._redis.lrange(active_key, 0, -1)

        if not active_job_ids:
            return 0

        recovered = 0
        for job_id in active_job_ids:
            is_stale = await self._is_stale(queue, job_id)
            if is_stale:
                success = await self._requeue(queue, job_id)
                if success:
                    recovered += 1
                    logger.warning(
                        f"Reaper recovered stale job {job_id} "
                        f"(queue={queue}) — worker lock expired"
                    )

        return recovered

    async def _is_stale(self, queue: str, job_id: str) -> bool:
        """Check if a job's worker lock has expired.

        Lock key: queue:{name}:lock:{job_id}
        - EXISTS → worker is alive and processing → not stale
        - MISSING → lock TTL expired → worker crashed → stale
        """
        lock_key = RedisKeys.lock(queue, job_id)
        lock_exists = await self._redis.exists(lock_key)
        return lock_exists == 0  # 0 means key does not exist → stale

    async def _requeue(self, queue: str, job_id: str) -> bool:
        """Move a stale job from active back to waiting.

        Returns True if successfully requeued, False if job data is missing.
        """
        job_key = RedisKeys.job(queue, job_id)
        active_key = RedisKeys.active(queue)
        waiting_key = RedisKeys.waiting(queue)

        # fetch job data to get priority
        job_data = await self._redis.hgetall(job_key)
        if not job_data:
            # job data is gone — just remove from active list
            await self._redis.lrem(active_key, 1, job_id)
            logger.error(f"Reaper: job {job_id} data missing — removed from active")
            return False

        job = Job.from_redis_dict(job_data)

        # atomically move from active → waiting
        pipe = self._redis.pipeline()
        pipe.lrem(active_key, 1, job_id)
        pipe.zadd(waiting_key, {job_id: job.priority})
        pipe.hset(job_key, mapping={
            "status": JobStatus.WAITING.value,
            "started_at": "",   # clear started_at
            "finished_at": "",  # clear finished_at
        })
        await pipe.execute()

        return True

    async def _get_active_queues(self) -> list[str]:
        """Find all queue names that have active jobs."""
        pattern = "queue:*:active"
        keys = await self._redis.keys(pattern)

        queues = []
        for key in keys:
            parts = key.split(":")
            if len(parts) == 3:
                queues.append(parts[1])

        return queues