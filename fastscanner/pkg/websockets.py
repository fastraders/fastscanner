import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Any
from uuid import uuid4

import websockets

logger = logging.getLogger(__name__)


class WebSocketSubscriber(ABC):
    _DELAY_BASE = 0.1
    _DELAY_MAX = 10.0
    _DELAY_FACTOR = 2.0
    _POISON_PILL = ("STOP", False)
    _MAX_SEND_RETRIES = 3

    def __init__(
        self, host: str, port: int, endpoint: str, max_connections: int | None = 10
    ):
        self._host = host
        self._port = port
        self._endpoint = endpoint
        self._socket_ids: list[str] = []
        self._websockets: dict[str, websockets.ClientConnection] = {}
        self._socket_to_handlers: dict[str, list[str]] = {}
        self._handler_to_socket: dict[str, str] = {}
        self._subscription_messages: dict[str, str] = {}

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
            elif (
                self._max_connections is None
                or len(self._socket_ids) < self._max_connections
            ):
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

    async def _connect_websockets(self):
        delay = self._DELAY_BASE

        while True:
            socket_id, is_new = await self._sockets_to_connect.get()
            if socket_id == self._POISON_PILL[0]:
                break
            try:
                async with self._websocket_available:
                    url = f"ws://{self._host}:{self._port}{self._endpoint}"
                    ws = await websockets.connect(url)
                    if not is_new:
                        for message in self._subscription_messages.values():
                            await ws.send(message)

                    self._websockets[socket_id] = ws
                    if is_new:
                        self._tasks.append(
                            asyncio.create_task(self._listen_ws(socket_id))
                        )
                        is_new = False
                    self._websocket_available.notify_all()
                delay = self._DELAY_BASE
            except Exception as e:
                logger.error(
                    f"Failed to connect WebSocket {socket_id}: {e}. Retrying in {delay}s"
                )
                await asyncio.sleep(delay)
                delay = min(delay * self._DELAY_FACTOR, self._DELAY_MAX)
                await self._sockets_to_connect.put((socket_id, is_new))

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

            try:
                await self.handle_ws_message(socket_id, message)
            except Exception as e:
                logger.error(
                    f"Error handling websocket message for socket {socket_id}: {e}"
                )

    @abstractmethod
    async def handle_ws_message(self, socket_id: str, message: str | bytes):
        """
        Handle incoming WebSocket messages.
        Subclasses must implement this method to process messages.
        """
        pass

    async def send_subscribe_message(
        self,
        handler_id: str,
        message: str,
    ):
        self._subscription_messages[handler_id] = message
        await self.send_ws_message(handler_id, message)

    async def send_unsubscribe_message(self, handler_id: str, message: str):
        await self.send_ws_message(
            handler_id,
            message,
        )
        self._subscription_messages.pop(handler_id, None)
        socket_id = self._handler_to_socket.pop(handler_id, None)
        if socket_id is not None:
            handlers = self._socket_to_handlers.get(socket_id, [])
            if handler_id in handlers:
                handlers.remove(handler_id)

    async def send_ws_message(self, handler_id: str, message: str):
        """
        Send a message through the websocket.
        Automatically handles reconnection on connection failures.
        """
        ws = await self._websocket(handler_id)
        for attempt in range(self._MAX_SEND_RETRIES):
            try:
                await ws.send(message)
                return
            except websockets.ConnectionClosed:
                if attempt >= self._MAX_SEND_RETRIES - 1:
                    logger.error(
                        f"Failed to send message for {handler_id} after {self._MAX_SEND_RETRIES} attempts"
                    )
                    raise

                logger.warning(
                    f"WebSocket closed when sending message for {handler_id}. Reconnecting..."
                )
                ws = await self._reconnect_websocket(
                    self._handler_to_socket[handler_id]
                )
                logger.info(f"WebSocket reconnected for {handler_id}.")

    async def stop(self):
        """Stop all websocket connections and cleanup."""
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
