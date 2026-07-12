"""
scheduler.py — Promotes delayed jobs to waiting when their time comes

Runs as a background loop, waking every SCHEDULER_POLL_INTERVAL seconds.
Checks the delayed sorted set for jobs whose score (run_at timestamp)
is <= now, and moves them to the waiting sorted set so workers can pick them up.

Usage:
    scheduler = Scheduler()
    await scheduler.start()  # runs forever until stop() is called
"""

from __future__ import annotations

import asyncio
import logging
import os
import time

import redis.asyncio as aioredis
from dotenv import load_dotenv

from src.models import JobStatus, RedisKeys

load_dotenv()

logger = logging.getLogger(__name__)


class Scheduler:
    """Promotes delayed jobs to waiting queue when their scheduled time arrives."""

    def __init__(
        self,
        redis_url: str | None = None,
        poll_interval: float | None = None,
    ):
        self._url = redis_url or os.getenv("REDIS_URL", "redis://localhost:6379/0")
        self._poll_interval = poll_interval or float(os.getenv("SCHEDULER_POLL_INTERVAL", 1))
        self._redis: aioredis.Redis | None = None
        self._running = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self):
        self._redis = await aioredis.from_url(self._url, decode_responses=True)
        logger.info(f"Scheduler connected — poll interval={self._poll_interval}s")

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
        """Start the scheduler loop. Runs until stop() is called."""
        self._running = True
        logger.info("Scheduler started")

        while self._running:
            try:
                promoted = await self._tick()
                if promoted > 0:
                    logger.info(f"Scheduler promoted {promoted} delayed job(s) to waiting")
            except Exception as e:
                logger.error(f"Scheduler error: {e}")

            await asyncio.sleep(self._poll_interval)

    async def stop(self):
        self._running = False
        logger.info("Scheduler stopped")

    # ------------------------------------------------------------------
    # Core logic
    # ------------------------------------------------------------------

    async def _tick(self) -> int:
        """Check all queues for delayed jobs that are due and promote them.

        Returns number of jobs promoted this tick.
        """
        now = time.time()
        total_promoted = 0

        # find all queue names by scanning delayed keys
        # pattern: queue:*:delayed
        queue_names = await self._get_active_queues()

        for queue in queue_names:
            promoted = await self._promote_due_jobs(queue, now)
            total_promoted += promoted

        return total_promoted

    async def _promote_due_jobs(self, queue: str, now: float) -> int:
        """Move all due delayed jobs for a queue into the waiting sorted set.

        Uses ZRANGEBYSCORE to find jobs whose run_at score <= now,
        then atomically moves each one from delayed → waiting.

        Returns number of jobs promoted.
        """
        delayed_key = RedisKeys.delayed(queue)
        waiting_key = RedisKeys.waiting(queue)

        # get all job IDs whose scheduled time has passed
        # ZRANGEBYSCORE key min max — score range [0, now]
        due_job_ids = await self._redis.zrangebyscore(
            delayed_key,
            min=0,
            max=now,
        )

        if not due_job_ids:
            return 0

        promoted = 0
        for job_id in due_job_ids:
            # atomically move from delayed → waiting
            moved = await self._promote_single(queue, job_id)
            if moved:
                promoted += 1
                logger.debug(f"Promoted delayed job {job_id} → waiting (queue={queue})")

        return promoted

    async def _promote_single(self, queue: str, job_id: str) -> bool:
        """Promote one job from delayed to waiting.

        Uses a pipeline with WATCH for optimistic locking —
        if another scheduler instance already promoted this job,
        the transaction will fail and we skip it safely.

        Returns True if promoted, False if already handled.
        """
        delayed_key = RedisKeys.delayed(queue)
        waiting_key = RedisKeys.waiting(queue)
        job_key = RedisKeys.job(queue, job_id)

        async with self._redis.pipeline(transaction=True) as pipe:
            try:
                # WATCH the delayed key — if it changes before EXEC, retry
                await pipe.watch(delayed_key)

                # check job still exists in delayed (another scheduler may have taken it)
                score = await pipe.zscore(delayed_key, job_id)
                if score is None:
                    await pipe.reset()
                    return False  # already promoted by another instance

                # get job priority for waiting queue score
                priority = await pipe.hget(job_key, "priority")
                priority_score = float(priority) if priority else 10.0

                # atomic block: MULTI → EXEC
                pipe.multi()
                pipe.zrem(delayed_key, job_id)
                pipe.zadd(waiting_key, {job_id: priority_score})
                pipe.hset(job_key, "status", JobStatus.WAITING.value)
                await pipe.execute()
                return True

            except aioredis.WatchError:
                # another scheduler instance promoted this job first — that's fine
                logger.debug(f"WatchError promoting {job_id} — already handled")
                return False

    async def _get_active_queues(self) -> list[str]:
        """Find all queue names that have delayed jobs waiting."""
        pattern = "queue:*:delayed"
        keys = await self._redis.keys(pattern)

        # extract queue name from key pattern "queue:{name}:delayed"
        queues = []
        for key in keys:
            parts = key.split(":")
            if len(parts) == 3:
                queues.append(parts[1])

        return queues