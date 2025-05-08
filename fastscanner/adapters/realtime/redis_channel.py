import asyncio
import logging
from typing import Any, Optional, cast

import pandas as pd
import redis.asyncio as aioredis

from fastscanner.services.indicators.ports import ChannelHandler

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
        # self.redis = aioredis.Redis(
        #     unix_socket_path="/run/redis/redis-server.sock",
        #     password=None,
        #     db=0,
        #     decode_responses=True,
        # )
        self._pipeline = None
        self._handlers: dict[str, ChannelHandler] = {}
        self._xread_task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self._last_ids: dict[str, str] = {}

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

    async def subscribe(self, channel_id: str, handler: ChannelHandler) -> None:
        self._handlers[channel_id] = handler
        self._last_ids[channel_id] = await self._get_last_id(channel_id)
        await self._restart_xread()

    async def _get_last_id(self, stream_key: str) -> str:
        if stream_key in self._last_ids:
            return self._last_ids[stream_key]

        last_entry = await self.redis.xrevrange(stream_key, count=1)
        return last_entry[0][0] if last_entry else "0-0"

    async def _restart_xread(self) -> None:
        if self._xread_task and not self._xread_task.done():
            self._stop_event.set()
            await self._xread_task

        self._stop_event = asyncio.Event()  
        self._xread_task = asyncio.create_task(self._xread_loop())

    async def _xread_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                if not self._handlers:
                    await asyncio.sleep(1)
                    continue

                last_ids :dict[Any,Any]= {
                    stream: self._last_ids.get(stream, "0-0")
                    for stream in self._handlers
                }

                entries = await self.redis.xread(last_ids, block=1000, count=10000)

                for stream, stream_entries in entries:
                    for entry_id, data in stream_entries:
                        self._last_ids[stream] = entry_id
                        handler = self._handlers.get(stream)
                        if handler:
                            await handler.handle(stream, data)

            except Exception as e:
                logger.error("Redis xread error: %s", e, exc_info=True)
                await asyncio.sleep(1)
