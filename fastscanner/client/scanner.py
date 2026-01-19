import json
import logging
from datetime import date
from typing import Any, Awaitable, Callable
from uuid import uuid4

import httpx

from fastscanner.adapters.rest.models import (
    ActionType,
    ScannerMessage,
    ScanRealtimeSubscribeRequest,
    ScanRealtimeSubscribeResponse,
    ScanRequest,
    ScanResponse,
)
from fastscanner.pkg.websockets import WebSocketSubscriber

logger = logging.getLogger(__name__)


class ScanRequestError(Exception):
    pass


class ScannerClient(WebSocketSubscriber):
    """WebSocket client for consuming candle data with indicators in real-time."""

    def __init__(self, host: str, port: int):
        super().__init__(host, port, "/api/scanners", None)
        self._handlers: dict[str, Callable[[ScannerMessage], Awaitable[None]]] = {}

    async def handle_ws_message(self, socket_id: str, message: str | bytes):
        data = json.loads(message)

        if "candle" not in data:
            return

        msg = ScannerMessage.model_validate(data)
        scanner_id = msg.scanner_id

        if scanner_id not in self._handlers:
            logger.warning(f"No handler for scanner_id {scanner_id}")
            return

        handler = self._handlers[scanner_id]
        try:
            await handler(msg)
        except Exception as e:
            logger.error(
                f"Error in handler for sub {scanner_id}, symbol {msg.symbol}, candle {msg.candle}: {e}"
            )

    async def subscribe(
        self,
        type: str,
        params: dict[str, Any],
        freq: str,
        handler: Callable[[ScannerMessage], Awaitable[None]],
    ) -> str:
        """Subscribe to indicators for a symbol."""
        scanner_id = str(uuid4())
        request = ScanRealtimeSubscribeRequest(
            scanner_id=scanner_id,
            type=type,
            params=params,
            freq=freq,
        )
        self._handlers[scanner_id] = handler
        await self.send_subscribe_message(scanner_id, request.model_dump_json())

        return scanner_id

    async def unsubscribe(self, scanner_id: str):
        await self.send_unsubscribe_message(
            scanner_id, json.dumps({"action": ActionType.UNSUBSCRIBE.value})
        )
        self._handlers.pop(scanner_id, None)

    async def scan_day(self, day: date, type: str, params: dict[str, Any]) -> set[str]:
        """Scan candles for a specific day."""
        url = f"http://{self._host}:{self._port}/api/scanners/{type}/scans"
        async with httpx.AsyncClient(timeout=600) as client:
            request = ScanRequest(
                start=day,
                end=day,
                freq="1d",
                type=type,
                params=params,
            )

            response = await client.post(url, json=request.model_dump(mode="json"))
            if response.status_code != 200:
                raise ScanRequestError(
                    f"Failed to scan day {day} with scanner {type}: "
                    f"{response.status_code} {response.text}"
                )
            response = ScanResponse.model_validate_json(response.content)

            return {result["symbol"] for result in response.results}
