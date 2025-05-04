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
        self._pipeline = None

    async def push(self, channel_id: str, data: dict[Any, Any], flush: bool = True):
        if self._pipeline is None:
            self._pipeline = self.redis.pipeline()

        self._pipeline.xadd(channel_id, data)
        if flush:
            await self.flush()

    async def flush(self):
        if self._pipeline:
            await self._pipeline.execute()
            self._pipeline = None
