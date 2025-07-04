import asyncio
import logging
from typing import Any, Optional, cast

import pandas as pd
import redis.asyncio as aioredis
from redis import RedisError

from fastscanner.services.indicators.ports import ChannelHandler

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class RedisChannel:
    def __init__(
        self,
        host: str,
        unix_socket_path: str,
        port: int,
        password: str | None,
        db: int,
    ):
        self.redis = aioredis.Redis(
            unix_socket_path=unix_socket_path,
            password=None,
            db=0,
            decode_responses=True,
        )
        self._pipeline = None
        self._handlers: dict[str, list[ChannelHandler]] = {}
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
        if channel_id not in self._handlers:
            self._handlers[channel_id] = []

        if handler not in self._handlers[channel_id]:
            self._handlers[channel_id].append(handler)

        if channel_id not in self._last_ids:
            self._last_ids[channel_id] = await self._get_last_id(channel_id)

        if self._xread_task is None or self._xread_task.done():
            self._xread_task = asyncio.create_task(self._xread_loop())

    async def _get_last_id(self, stream_key: str) -> str:
        last_entry = await self.redis.xrevrange(stream_key, count=1)
        return last_entry[0][0] if last_entry else "0-0"

    async def _xread_loop(self) -> None:
        while True:
            try:
                if not self._handlers:
                    await asyncio.sleep(1)
                    continue

                entries = await self.redis.xread(self._last_ids, block=1000, count=10000)  # type: ignore

                for stream, stream_entries in entries:
                    for entry_id, data in stream_entries:
                        self._last_ids[stream] = entry_id
                        for handler in self._handlers.get(stream, []):
                            await handler.handle(stream, data)

            except RedisError as e:
                logger.error("Redis xread error: %s", e, exc_info=True)
                await asyncio.sleep(1)

    async def unsubscribe(self, channel_id: str, handler_id: str) -> None:
        handlers = self._handlers.get(channel_id, [])
        for h in handlers:
            if h.id() == handler_id:
                handlers.remove(h)
                break
        else:
            return
        if not handlers:
            del self._handlers[channel_id]
            self._last_ids.pop(channel_id, None)
            if not self._handlers and self._xread_task is not None:
                self._xread_task.cancel()
                self._xread_task = None

    async def reset(self) -> None:
        try:

            await self.redis.flushdb(asynchronous=True)
            logger.info("Successfully cleared all Redis data in database 0")

        except RedisError as e:
            logger.error(f"Redis error during cleanup: {e}", exc_info=True)
            raise
        finally:
            if self.redis:
                await self.redis.aclose()
                logger.info("Redis connection closed")
