import json
import logging
from datetime import datetime
from typing import Any, Awaitable, Callable, Protocol

import websockets
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class SubscriptionRequest(BaseModel):
    action: str = "subscribe"
    subscription_id: str
    symbol: str
    freq: str
    indicators: list[dict[str, Any]]


class UnsubscriptionRequest(BaseModel):
    action: str = "unsubscribe"
    subscription_id: str


class CandleMessage(BaseModel):
    subscription_id: str
    symbol: str
    timestamp: datetime
    candle: dict[str, Any]


class CandleSubscriptionClient:
    """WebSocket client for consuming candle data with indicators in real-time."""

    def __init__(self, url: str):
        self.url = url
        self._websocket = None
        self._subscriptions: dict[str, Callable[[CandleMessage], Awaitable[None]]] = {}
        self._running = False

    async def connect(self):
        """Connect to the websocket server."""
        self._websocket = await websockets.connect(self.url)
        self._running = True

    async def disconnect(self):
        """Disconnect from the websocket server."""
        self._running = False
        if self._websocket:
            await self._websocket.close()

    async def subscribe(
        self,
        subscription_id: str,
        symbol: str,
        freq: str,
        indicators: list[dict],
        callback: Callable[[CandleMessage], Awaitable[None]],
    ):
        """Subscribe to indicators for a symbol."""
        if not self._websocket:
            raise RuntimeError("Not connected. Call connect() first.")

        request = SubscriptionRequest(
            subscription_id=subscription_id,
            symbol=symbol,
            freq=freq,
            indicators=indicators,
        )

        await self._websocket.send(request.model_dump_json())
        self._subscriptions[subscription_id] = callback

    async def unsubscribe(self, subscription_id: str):
        """Unsubscribe from a subscription."""
        if not self._websocket:
            raise RuntimeError("Not connected. Call connect() first.")

        request = UnsubscriptionRequest(subscription_id=subscription_id)
        await self._websocket.send(request.model_dump_json())

        if subscription_id in self._subscriptions:
            del self._subscriptions[subscription_id]

    async def listen(self):
        """Listen for incoming messages and dispatch to callbacks."""
        if not self._websocket:
            raise RuntimeError("Not connected. Call connect() first.")

        while self._running:
            message = await self._websocket.recv()
            data = json.loads(message)

            # Check if it's an indicator message
            if "candle" not in data or "subscription_id" not in data:
                continue
            indicator_msg = CandleMessage(**data)
            subscription_id = indicator_msg.subscription_id

            if subscription_id not in self._subscriptions:
                continue
            callback = self._subscriptions[subscription_id]
            try:
                await callback(indicator_msg)
            except Exception as e:
                logger.exception(e)

    async def run(self):
        """Connect and start listening for messages."""
        await self.connect()
        await self.listen()

    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()
