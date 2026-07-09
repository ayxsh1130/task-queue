"""Tests for models.py — no Redis needed, pure unit tests."""

import time
import pytest
from src.models import Job, JobStatus, RedisKeys


class TestJob:
    def test_default_values(self):
        job = Job(name="send_email")
        assert job.status == JobStatus.WAITING
        assert job.attempts == 0
        assert job.priority == 10
        assert job.queue == "default"
        assert job.id is not None

    def test_unique_ids(self):
        ids = {Job().id for _ in range(100)}
        assert len(ids) == 100  # all unique

    def test_redis_roundtrip(self):
        """to_redis_dict → from_redis_dict should produce identical job."""
        original = Job(
            name="send_email",
            queue="emails",
            data={"to": "test@example.com", "subject": "Hello"},
            priority=5,
            attempts=1,
            status=JobStatus.ACTIVE,
            result=None,
            error="timeout",
        )
        redis_dict = original.to_redis_dict()
        restored = Job.from_redis_dict(redis_dict)

        assert restored.id == original.id
        assert restored.name == original.name
        assert restored.data == original.data
        assert restored.status == original.status
        assert restored.error == original.error
        assert restored.attempts == original.attempts

    def test_redis_dict_all_strings(self):
        job = Job(name="test")
        d = job.to_redis_dict()
        for k, v in d.items():
            assert isinstance(v, str), f"Key '{k}' has non-string value: {type(v)}"

    def test_completed_job_roundtrip(self):
        job = Job(name="process_image", status=JobStatus.COMPLETED)
        job.result = {"url": "https://cdn.example.com/img.jpg"}
        job.finished_at = time.time()
        restored = Job.from_redis_dict(job.to_redis_dict())
        assert restored.result == {"url": "https://cdn.example.com/img.jpg"}
        assert restored.finished_at is not None


class TestRedisKeys:
    def test_key_patterns(self):
        assert RedisKeys.job("emails", "abc-123") == "queue:emails:jobs:abc-123"
        assert RedisKeys.waiting("emails") == "queue:emails:waiting"
        assert RedisKeys.active("emails") == "queue:emails:active"
        assert RedisKeys.completed("emails") == "queue:emails:completed"
        assert RedisKeys.failed("emails") == "queue:emails:failed"
        assert RedisKeys.delayed("emails") == "queue:emails:delayed"
        assert RedisKeys.lock("emails", "abc-123") == "queue:emails:lock:abc-123"

    def test_different_queues_different_keys(self):
        assert RedisKeys.waiting("emails") != RedisKeys.waiting("notifications")