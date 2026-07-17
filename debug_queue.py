"""Quick debug script to inspect queue state."""
import asyncio
import redis.asyncio as aioredis
from src.models import RedisKeys

async def main():
    redis = await aioredis.from_url("redis://localhost:6379/0", decode_responses=True)
    
    # check all bench queues
    keys = await redis.keys("queue:bench*:*")
    queues = set()
    for k in keys:
        parts = k.split(":")
        if len(parts) == 3:
            queues.add(parts[1])
    
    for q in sorted(queues):
        waiting   = await redis.zcard(f"queue:{q}:waiting")
        active    = await redis.llen(f"queue:{q}:active")
        completed = await redis.zcard(f"queue:{q}:completed")
        failed    = await redis.zcard(f"queue:{q}:failed")
        print(f"{q}: waiting={waiting} active={active} completed={completed} failed={failed}")
    
    await redis.aclose()

asyncio.run(main())