import asyncio
import json
import logging
from datetime import date, datetime
from io import StringIO
from typing import Any, Awaitable, Callable, Protocol
from uuid import uuid4

import httpx
import pandas as pd
import websockets
from pydantic import BaseModel

from fastscanner.pkg.clock import LOCAL_TIMEZONE_STR
from fastscanner.services.indicators.ports import CandleCol

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


class CandleClient:
    """WebSocket client for consuming candle data with indicators in real-time."""

    def __init__(self, host: str, port: int, max_connections: int = 10):
        self._host = host
        self._port = port
        self._socket_ids: list[str] = []
        self._websockets: dict[str, websockets.ClientConnection] = {}
        self._socket_to_handlers: dict[str, list[str]] = {}
        self._handler_to_socket: dict[str, str] = {}
        self._handlers: dict[str, Callable[[CandleMessage], Awaitable[None]]] = {}
        self._subscription_requests: dict[str, SubscriptionRequest] = {}

        self._tasks: list[asyncio.Task] = []
        self._curr_socket_idx = 0
        self._max_connections = max_connections

    async def _websocket(self, handler_id: str) -> websockets.ClientConnection:
        if handler_id not in self._handler_to_socket:
            if len(self._socket_ids) < self._max_connections:
                url = f"ws://{self._host}:{self._port}/api/indicators"
                ws = await websockets.connect(url)
                socket_id = str(uuid4())
                self._websockets[socket_id] = ws
                self._socket_ids.append(socket_id)
                task = asyncio.create_task(self._listen_ws(socket_id))
                self._tasks.append(task)
            else:
                socket_id = self._socket_ids[
                    self._curr_socket_idx % self._max_connections
                ]
                ws = self._websockets[socket_id]
            self._curr_socket_idx = (self._curr_socket_idx + 1) % self._max_connections
            self._handler_to_socket[handler_id] = socket_id
            self._socket_to_handlers.setdefault(socket_id, []).append(handler_id)

        return self._websockets[self._handler_to_socket[handler_id]]

    async def _listen_ws(self, socket_id: str):
        ws = self._websockets[socket_id]
        while True:
            try:
                message = await ws.recv()
            except websockets.ConnectionClosedError:
                logger.warning(f"WebSocket {socket_id} closed. Reconnecting...")
                try:
                    url = f"ws://{self._host}:{self._port}/api/indicators"
                    ws = await websockets.connect(url)
                    self._websockets[socket_id] = ws
                    for handler_id in self._socket_to_handlers.get(socket_id, []):
                        request = self._subscription_requests.get(handler_id)
                        if request is None:
                            continue
                        await ws.send(request.model_dump_json())
                except Exception as e:
                    logger.error(f"Failed to reconnect WebSocket {socket_id}: {e}")
                    await asyncio.sleep(5)
                    continue
                logger.info(f"WebSocket {socket_id} reconnected.")

            data = json.loads(message)

            # Check if it's an indicator message
            if "candle" not in data:
                continue
            indicator_msg = CandleMessage(**data)
            subscription_id = indicator_msg.subscription_id

            if subscription_id not in self._handlers:
                logger.warning(f"No handler for subscription_id {subscription_id}")
                continue
            handler = self._handlers[subscription_id]
            try:
                await handler(indicator_msg)
            except Exception as e:
                logger.error(
                    f"Error in handler for sub {subscription_id}, symbol {indicator_msg.symbol}, candle {indicator_msg.candle}: {e}"
                )

    async def subscribe(
        self,
        subscription_id: str,
        symbol: str,
        freq: str,
        indicators: list[dict],
        handler: Callable[[CandleMessage], Awaitable[None]],
    ):
        """Subscribe to indicators for a symbol."""
        request = SubscriptionRequest(
            subscription_id=subscription_id,
            symbol=symbol,
            freq=freq,
            indicators=indicators,
        )
        self._subscription_requests[subscription_id] = request
        self._handlers[subscription_id] = handler
        ws = await self._websocket(subscription_id)
        await ws.send(request.model_dump_json())

    async def unsubscribe(self, subscription_id: str):
        ws = await self._websocket(subscription_id)
        request = UnsubscriptionRequest(subscription_id=subscription_id)
        await ws.send(request.model_dump_json())

        self._handlers.pop(subscription_id, None)
        self._subscription_requests.pop(subscription_id, None)
        socket_id = self._handler_to_socket.pop(subscription_id, None)
        if socket_id is not None:
            self._socket_to_handlers[socket_id].remove(subscription_id)

    async def stop(self):
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        for ws in self._websockets.values():
            try:
                await ws.close()
            except Exception as e:
                logger.error(f"Error closing WebSocket: {e}")

    async def get(
        self,
        symbol: str,
        start: date,
        end: date,
        freq: str,
        indicators: list[dict[str, Any]],
    ) -> pd.DataFrame:
        async with httpx.AsyncClient() as client:
            params = {
                "symbol": symbol,
                "start": start.isoformat(),
                "end": end.isoformat(),
                "freq": freq,
                "indicators": indicators,
            }
            url = f"http://{self._host}:{self._port}/api/indicators/calculate"
            response = await client.post(url, json=params)
            response.raise_for_status()
            df = (
                pd.read_csv(
                    StringIO(response.json()),
                    index_col=CandleCol.DATETIME,
                    parse_dates=[CandleCol.DATETIME],
                )
                .tz_localize("utc")
                .tz_convert(LOCAL_TIMEZONE_STR)
            )
        return df
