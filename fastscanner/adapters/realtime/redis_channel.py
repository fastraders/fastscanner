import logging
from typing import Any, cast

import pandas as pd
import redis.asyncio as aioredis

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class RedisChannel:
    def __init__(
        self,
        host="localhost",
        port=6379,
        password=None,
        db=0,
        stream_key="realtime_stream",
    ):
        self.redis = aioredis.Redis(
            host=host, port=port, password=password, db=db, decode_responses=True
        )
        self.stream_key = stream_key

    async def push(self, data: list[dict[str, Any]]):
        if self.redis is None:
            raise RuntimeError(
                "Redis connection not established. Call connect() first."
            )
        pipe = self.redis.pipeline()
        for record in data:
            record_str: dict[Any, Any] = {k: v for k, v in record.items()}
            pipe.xadd(self.stream_key, record_str)
        await pipe.execute()
