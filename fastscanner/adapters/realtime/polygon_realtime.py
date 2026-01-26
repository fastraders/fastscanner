import asyncio
import logging
import traceback
from datetime import date
from typing import Protocol

import uvloop
from massive import WebSocketClient
from massive.websocket.models import (
    EquityAgg,
    EventType,
    Feed,
    Market,
    WebSocketMessage,
)
from websockets import ConnectionClosedError

from fastscanner.adapters.realtime.redis_channel import RedisChannel
from fastscanner.pkg import config
from fastscanner.pkg.clock import ClockRegistry
from fastscanner.services.indicators.ports import Channel

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class ActiveSymbolsProvider(Protocol):
    async def active_symbols(self, exchanges: list[str] | None = None) -> list[str]: ...


class PolygonRealtime:
    def __init__(self, api_key: str, channel: Channel):
        self._api_key = api_key
        self._client: WebSocketClient | None = None
        self._running = False
        self._channel = channel
        self._ws_task: asyncio.Task | None = None

        self._symbols_provider: ActiveSymbolsProvider | None = None
        self._is_filtering_active_symbols = False
        self._active_symbols: set[str] = set()
        self._active_symbols_last_updated: date | None = None

    @property
    def symbols_provider(self) -> ActiveSymbolsProvider:
        if self._symbols_provider is None:
            raise RuntimeError("ActiveSymbolsProvider is not set.")
        return self._symbols_provider

    def is_active_symbols_expired(self) -> bool:
        if self._active_symbols_last_updated is None:
            return True
        return ClockRegistry.clock.today() > self._active_symbols_last_updated

    async def reload_active_symbols(self):
        symbols = await self.symbols_provider.active_symbols()
        self._active_symbols = set(symbols)
        self._active_symbols_last_updated = ClockRegistry.clock.today()
        logger.info(
            f"Reloaded active symbols. Total active symbols: {len(self._active_symbols)}"
        )

    async def start(self):
        if self._running:
            logger.warning("WebSocket already running.")
            return

        self._client = WebSocketClient(
            api_key=self._api_key,
            feed=Feed.RealTime,
            market=Market.Stocks,
            max_reconnects=None,  # Unlimited reconnects.
        )
        self._running = True
        logger.info("Connecting WebSocket")

        self._ws_task = asyncio.create_task(self._client.connect(self.handle_messages))

    async def stop(self):
        if not self._running:
            logger.warning("WebSocket is not running.")
            return
        try:
            if self._client:
                await self._client.close()
            if self._ws_task:
                await self._ws_task
        except ConnectionClosedError as e:
            logger.warning(f"Client was disconnected: {e}")
        finally:
            self._running = False
            logger.info("WebSocket stopped.")

    def subscribe_all(self):
        if not self._running:
            raise RuntimeError("WebSocket is not running")
        if self._client is None:
            raise RuntimeError("WebSocketClient is None during subscribe.")
        self._client.subscribe("AM.*")

    async def subscribe_all_active(self, symbols_provider: ActiveSymbolsProvider):
        if not self._running:
            raise RuntimeError("WebSocket is not running")
        if self._client is None:
            raise RuntimeError("WebSocketClient is None during subscribe.")

        self._symbols_provider = symbols_provider
        self._is_filtering_active_symbols = True
        self._active_symbols_last_updated = ClockRegistry.clock.today()
        symbols = await self._symbols_provider.active_symbols()
        self._active_symbols = set(symbols)

        self._client.subscribe("AM.*")

    def subscribe_min(self, symbols: set[str]):
        if not self._running:
            raise RuntimeError("WebSocket is not running")
        if self._client is None:
            raise RuntimeError("WebSocketClient is None during subscribe.")

        tickers = [f"AM.{symbol}" for symbol in symbols]
        self._client.subscribe(*tickers)

    def subscribe_s(self, symbols: set[str]):
        if not self._running:
            raise RuntimeError("WebSocket is not running")
        if self._client is None:
            raise RuntimeError("WebSocketClient is None during subscribe.")

        tickers = [f"A.{symbol}" for symbol in symbols]
        self._client.subscribe(*tickers)

    def unsubscribe_min(self, symbols: set[str]):
        if not self._running:
            logger.warning("WebSocket is not running.")
            return

        if self._client is None:
            raise RuntimeError("WebSocketClient is None during unsubscribe.")

        if not symbols:
            logger.warning("No symbols to unsubscribe.")
            return

        tickers = [f"AM.{symbol}" for symbol in symbols]
        self._client.unsubscribe(*tickers)

    def unsubscribe_s(self, symbols: set[str]):
        if not self._running:
            logger.warning("WebSocket is not running.")
            return

        if self._client is None:
            raise RuntimeError("WebSocketClient is None during unsubscribe.")

        if not symbols:
            logger.warning("No symbols to unsubscribe.")
            return

        tickers = [f"A.{symbol}" for symbol in symbols]
        self._client.unsubscribe(*tickers)

    def unsubscribe_all(self):
        if not self._running:
            logger.warning("WebSocket is not running.")
            return

        if self._client is None:
            raise RuntimeError("WebSocketClient is None during unsubscribe.")

        self._client.unsubscribe_all()

    _event_type_to_channel = {
        EventType.EquityAgg: "candles.s.",
        EventType.EquityAggMin: "candles.min.",
        EventType.EquityAgg.value: "candles.s.",
        EventType.EquityAggMin.value: "candles.min.",
    }

    async def handle_messages(self, msgs: list[WebSocketMessage]):
        parsed_msgs: list[dict] = []
        if self._is_filtering_active_symbols and self.is_active_symbols_expired():
            await self.reload_active_symbols()
        try:
            for msg in msgs:
                if not isinstance(msg, EquityAgg):
                    logger.warning("Received unexpected message %s", str(msg))
                    continue
                if msg.start_timestamp is None:
                    continue

                if msg.event_type not in self._event_type_to_channel:
                    logger.warning(f"Unknown event type: {msg.event_type}")
                    continue

                if (
                    self._is_filtering_active_symbols
                    and msg.symbol not in self._active_symbols
                ):
                    continue

                record = {
                    "timestamp": msg.start_timestamp,
                    "open": msg.open,
                    "high": msg.high,
                    "low": msg.low,
                    "close": msg.close,
                    "volume": msg.volume,
                }
                parsed_msgs.append(record)
                channel_id = (
                    f"{self._event_type_to_channel[msg.event_type]}{msg.symbol}"
                )
                await self._channel.push(channel_id, record, flush=False)

            await self._channel.flush()
        except Exception as e:
            logger.error(traceback.format_exc())
