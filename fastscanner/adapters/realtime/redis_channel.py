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
        # self.redis = aioredis.Redis(
        #     host=host,
        #     port=port,
        #     password=password,
        #     db=db,
        #     decode_responses=True,
        # )
        self.redis = aioredis.Redis(
            unix_socket_path="/run/redis/redis-server.sock",
            password=None,
            db=0,
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
            pipeline = self._pipeline
            self._pipeline = None
            await pipeline.execute()
