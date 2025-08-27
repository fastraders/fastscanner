import json
import logging
from typing import Any

import nats
from nats.aio.client import Client as NATS
from nats.aio.msg import Msg
from nats.aio.subscription import Subscription

from fastscanner.services.indicators.ports import ChannelHandler

logger = logging.getLogger(__name__)


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
        self._is_stopped = False

    @property
    def nc(self) -> NATS:
        if self._nc is None:
            raise RuntimeError(
                "NATS connection not established. Call _ensure_connection() first."
            )
        return self._nc

    async def _ensure_connection(self):
        if self._nc is None or self._nc.is_closed:
            self._nc = await nats.connect(servers=self._servers)

    async def push(self, channel_id: str, data: dict[Any, Any], flush: bool = True):
        if flush:
            await self._ensure_connection()
            await self.nc.publish(channel_id, json.dumps(data).encode())
        else:
            self._pending_messages.append((channel_id, data))

    async def flush(self):
        if not self._pending_messages:
            return

        await self._ensure_connection()

        for channel_id, data in self._pending_messages:
            await self.nc.publish(channel_id, json.dumps(data).encode())

        self._pending_messages.clear()

    def _message_handler(self, channel_id: str):
        async def handler(msg: Msg) -> None:
            try:
                data = json.loads(msg.data.decode())
                handlers = self._handlers.get(channel_id, [])
                for h in handlers:
                    await h.handle(channel_id, data)
            except Exception:
                logger.exception(f"Message processing error for channel {channel_id}")

        return handler

    async def subscribe(self, channel_id: str, handler: ChannelHandler):
        if channel_id not in self._handlers:
            self._handlers[channel_id] = []

        if handler not in self._handlers[channel_id]:
            self._handlers[channel_id].append(handler)

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

    async def reset(self):
        self._is_stopped = True

        for subscription in self._subscriptions.values():
            await subscription.unsubscribe()
        self._subscriptions.clear()

        if self._nc and not self._nc.is_closed:
            await self._nc.close()
            logger.info("NATS connection closed")

        self._handlers.clear()
        self._pending_messages.clear()
