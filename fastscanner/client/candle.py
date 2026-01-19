import json
import logging
from datetime import date, datetime
from io import StringIO
from typing import Any, Awaitable, Callable

import httpx
import pandas as pd
from pydantic import BaseModel

from fastscanner.pkg.clock import LOCAL_TIMEZONE_STR
from fastscanner.pkg.websockets import WebSocketSubscriber
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


class CandleClient(WebSocketSubscriber):
    """WebSocket client for consuming candle data with indicators in real-time."""

    def __init__(self, host: str, port: int, max_connections: int = 10):
        super().__init__(host, port, "/api/indicators", max_connections)
        self._handlers: dict[str, Callable[[CandleMessage], Awaitable[None]]] = {}

    async def handle_ws_message(self, socket_id: str, message: str | bytes):
        data = json.loads(message)

        if "candle" not in data:
            return

        indicator_msg = CandleMessage(**data)
        subscription_id = indicator_msg.subscription_id

        if subscription_id not in self._handlers:
            logger.warning(f"No handler for subscription_id {subscription_id}")
            return

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
        self._handlers[subscription_id] = handler
        await self.send_subscribe_message(subscription_id, request.model_dump_json())

    async def unsubscribe(self, subscription_id: str):
        request = UnsubscriptionRequest(subscription_id=subscription_id)
        await self.send_unsubscribe_message(subscription_id, request.model_dump_json())
        self._handlers.pop(subscription_id, None)

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
