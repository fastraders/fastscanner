import json
import logging
import time
from typing import Any

import nats
from nats.aio.client import Client as NATS
from nats.aio.msg import Msg
from nats.aio.subscription import Subscription

from fastscanner.pkg.observability import metrics
from fastscanner.services.indicators.ports import ChannelHandler

logger = logging.getLogger(__name__)


def _subject_kind(subject: str) -> str:
    parts = subject.split(".", 2)
    if len(parts) >= 2:
        return f"{parts[0]}_{parts[1]}"
    return subject


class NATSChannel:
    def __init__(
        self,
        servers: list[str],
    ):
        self._servers = servers
        self._nc: NATS | None = None
        self._handlers: dict[str, list[ChannelHandler]] = {}
        self._subscriptions: dict[str, Subscription] = {}
        self._pending_messages: list[tuple[str, dict[Any, Any]]] = []

    @property
    def nc(self) -> NATS:
        if self._nc is None:
            raise RuntimeError(
                "NATS connection not established. Call _ensure_connection() first."
            )
        return self._nc

    async def _on_reconnected(self) -> None:
        metrics.nats_reconnect()
        logger.info("NATS reconnected")

    async def _on_disconnected(self) -> None:
        logger.warning("NATS disconnected")

    async def _on_closed(self) -> None:
        logger.warning("NATS connection closed")

    async def _ensure_connection(self):
        if self._nc is None or self._nc.is_closed:
            logger.info(f"Connecting to NAS Server at {self._servers}")
            self._nc = await nats.connect(
                servers=self._servers,
                reconnected_cb=self._on_reconnected,
                disconnected_cb=self._on_disconnected,
                closed_cb=self._on_closed,
            )
            logger.info("Connection to NAS Succesfull")

    async def push(self, channel_id: str, data: dict[Any, Any], flush: bool = True):
        if flush:
            await self._ensure_connection()
            kind = _subject_kind(channel_id)
            try:
                await self.nc.publish(channel_id, json.dumps(data).encode())
                metrics.nats_publish(kind)
            except Exception:
                metrics.nats_publish_error(kind)
                raise
        else:
            self._pending_messages.append((channel_id, data))

    async def flush(self):
        if not self._pending_messages:
            return

        await self._ensure_connection()
        metrics.nats_pending(len(self._pending_messages))

        start = time.perf_counter()
        for channel_id, data in self._pending_messages:
            kind = _subject_kind(channel_id)
            try:
                await self.nc.publish(channel_id, json.dumps(data).encode())
                metrics.nats_publish(kind)
            except Exception:
                metrics.nats_publish_error(kind)
                raise
        metrics.nats_flush(time.perf_counter() - start)

        self._pending_messages.clear()
        metrics.nats_pending(0)

    def _message_handler(self, channel_id: str):
        async def handler(msg: Msg) -> None:
            try:
                data = json.loads(msg.data.decode())
                handlers = self._handlers.get(channel_id, [])
                for h in handlers:
                    await h.handle(msg.subject, data)
            except Exception:
                logger.exception(f"Message processing error for channel {channel_id}")

        return handler

    async def subscribe(self, channel_id: str, handler: ChannelHandler):
        handlers = self._handlers.setdefault(channel_id, [])
        handler_ids = {h.id() for h in handlers}

        if handler.id() not in handler_ids:
            handlers.append(handler)

        if channel_id not in self._subscriptions:
            await self._ensure_connection()
            subscription = await self.nc.subscribe(
                channel_id, cb=self._message_handler(channel_id)
            )
            self._subscriptions[channel_id] = subscription

    async def unsubscribe(self, channel_id: str, handler_id: str):
        handlers = [
            h for h in self._handlers.get(channel_id, []) if h.id() != handler_id
        ]

        if not handlers and channel_id in self._handlers:
            del self._handlers[channel_id]

            if channel_id in self._subscriptions:
                await self._subscriptions[channel_id].unsubscribe()
                del self._subscriptions[channel_id]
        else:
            self._handlers[channel_id] = handlers

    async def reset(self): ...
