import logging
from typing import Any, cast

import pandas as pd
import redis.asyncio as aioredis

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class RedisChannel:
    def __init__(
        self,
        host: str,
        port: int,
        password: str | None,
        db: int,
    ):
        self.redis = aioredis.Redis(
            host=host,
            port=port,
            password=password,
            db=db,
            decode_responses=True,
        )

    async def push(self, channel_id: str, data: list[dict[str, Any]]):
        pipe = self.redis.pipeline()
        for record in data:
            record: dict[Any, Any]
            pipe.xadd(channel_id, record)
        await pipe.execute()
