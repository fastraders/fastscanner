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

        self._websocket_available = asyncio.Condition()
        self._sockets_to_connect: asyncio.Queue[tuple[str, bool]] = asyncio.Queue()
        self._connect_ws_task = asyncio.create_task(self._connect_websockets())

        self._tasks: list[asyncio.Task] = []
        self._curr_socket_idx = 0
        self._max_connections = max_connections

    async def _websocket(self, handler_id: str) -> websockets.ClientConnection:
        async with self._websocket_available:
            if handler_id in self._handler_to_socket:
                socket_id = self._handler_to_socket[handler_id]
            elif len(self._socket_ids) < self._max_connections:
                # Schedules the connection of a new websocket and assigns it to the handler
                socket_id = str(uuid4())
                self._socket_ids.append(socket_id)
                self._handler_to_socket[handler_id] = socket_id
                self._socket_to_handlers.setdefault(socket_id, []).append(handler_id)
                await self._sockets_to_connect.put((socket_id, True))
            else:
                socket_id = self._socket_ids[self._curr_socket_idx]
                self._curr_socket_idx = (
                    self._curr_socket_idx + 1
                ) % self._max_connections
                self._handler_to_socket[handler_id] = socket_id
                self._socket_to_handlers.setdefault(socket_id, []).append(handler_id)

            while socket_id not in self._websockets:
                await self._websocket_available.wait()

            return self._websockets[socket_id]

    _DELAY_BASE = 0.1
    _DELAY_MAX = 10.0
    _DELAY_FACTOR = 2.0
    _POISON_PILL = ("STOP", False)

    async def _connect_websockets(self):
        delay = self._DELAY_BASE
        while True:
            socket_id, is_new = await self._sockets_to_connect.get()
            if socket_id == self._POISON_PILL[0]:
                break
            try:
                async with self._websocket_available:
                    url = f"ws://{self._host}:{self._port}/api/indicators"
                    ws = await websockets.connect(url)
                    # Resend all subscriptions for this socket
                    for handler_id in self._socket_to_handlers.get(socket_id, []):
                        request = self._subscription_requests.get(handler_id)
                        if request is None:
                            continue
                        await ws.send(request.model_dump_json())

                    self._websockets[socket_id] = ws
                    if is_new:
                        self._tasks.append(
                            asyncio.create_task(self._listen_ws(socket_id))
                        )
                        is_new = False
                    self._websocket_available.notify_all()
                delay = self._DELAY_BASE
            except Exception as e:
                logger.error(f"Failed to reconnect WebSocket {socket_id}: {e}")
                await asyncio.sleep(delay)
                await self._sockets_to_connect.put((socket_id, is_new))
                delay = min(delay * self._DELAY_FACTOR, self._DELAY_MAX)

    async def _reconnect_websocket(self, socket_id: str) -> websockets.ClientConnection:
        async with self._websocket_available:
            self._websockets.pop(socket_id, None)
            await self._sockets_to_connect.put((socket_id, False))
            while socket_id not in self._websockets:
                await self._websocket_available.wait()
            return self._websockets[socket_id]

    async def _listen_ws(self, socket_id: str):
        ws = self._websockets[socket_id]
        while True:
            try:
                message = await ws.recv()
            except websockets.ConnectionClosedError:
                logger.warning(f"WebSocket {socket_id} closed. Reconnecting...")
                ws = await self._reconnect_websocket(socket_id)
                logger.info(f"WebSocket {socket_id} reconnected.")
                continue

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

    _MAX_SUBSCRIBE_RETRIES = 3

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
        await self._send_ws_message(subscription_id, request.model_dump_json())

    async def unsubscribe(self, subscription_id: str):
        request = UnsubscriptionRequest(subscription_id=subscription_id)
        await self._send_ws_message(subscription_id, request.model_dump_json())

        self._handlers.pop(subscription_id, None)
        self._subscription_requests.pop(subscription_id, None)
        async with self._websocket_available:
            socket_id = self._handler_to_socket.pop(subscription_id, None)
            if socket_id is not None:
                self._socket_to_handlers[socket_id].remove(subscription_id)

    async def _send_ws_message(self, handler_id: str, message: str):
        ws = await self._websocket(handler_id)
        for attempt in range(self._MAX_SUBSCRIBE_RETRIES):
            try:
                await ws.send(message)
                return
            except websockets.ConnectionClosedError:
                if attempt >= self._MAX_SUBSCRIBE_RETRIES - 1:
                    logger.error(
                        f"Failed to send message for {handler_id} after {self._MAX_SUBSCRIBE_RETRIES} attempts"
                    )
                    raise

                logger.warning(
                    f"WebSocket closed when sending message {message} for {handler_id}. Reconnecting..."
                )
                ws = await self._reconnect_websocket(
                    self._handler_to_socket[handler_id]
                )
                logger.info(f"WebSocket reconnected for {handler_id}.")

    async def stop(self):
        await self._sockets_to_connect.put(self._POISON_PILL)
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(
            self._connect_ws_task, *self._tasks, return_exceptions=True
        )
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
