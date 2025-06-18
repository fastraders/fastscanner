import asyncio
from datetime import datetime, timedelta
from typing import Any

import pandas as pd

from fastscanner.pkg.candle import CandleBuffer
from fastscanner.pkg.clock import LOCAL_TIMEZONE_STR, ClockRegistry
from fastscanner.services.indicators.ports import CandleCol as C
from fastscanner.services.indicators.ports import CandleStore, Channel, ChannelHandler
from fastscanner.services.indicators.utils import lookback_days
from fastscanner.services.scanners.ports import Scanner


class SubscriptionHandler:
    def handle(self, symbol: str, new_row: pd.Series, passed: bool) -> pd.Series: ...


class ScannerChannelHandler:
    def __init__(
        self,
        symbol: str,
        scanner: Scanner,
        handler: SubscriptionHandler,
        freq: str,
    ):
        self._symbol = symbol
        self._scanner = scanner
        self._handler = handler
        self._freq = freq
        self._buffer = CandleBuffer(symbol, freq, self._handle)

    async def _handle(self, row: pd.Series) -> None:
        new_row, passed = await self._scanner.scan_realtime(
            self._symbol, row, self._freq
        )
        self._handler.handle(self._symbol, new_row, passed)

    async def handle(self, channel_id: str, data: dict[Any, Any]) -> None:
        for field in (
            C.OPEN,
            C.HIGH,
            C.LOW,
            C.CLOSE,
            C.VOLUME,
        ):
            if field in data:
                data[field] = float(data[field])
        ts = pd.to_datetime(int(data["timestamp"]), unit="ms", utc=True).tz_convert(
            LOCAL_TIMEZONE_STR
        )
        row = pd.Series(data, name=ts)

        if self._freq == "1min":
            new_row, passed = await self._scanner.scan_realtime(
                self._symbol, row, self._freq
            )
            self._handler.handle(self._symbol, new_row, passed)
            return
        agg = await self._buffer.add(row)
        if agg is None:
            return
        await self._handle(agg)


class ScannerService:
    def __init__(self, candles: CandleStore, channel: Channel):
        self._candles = candles
        self._channel = channel
        self._handlers: dict[
            tuple[str, str, SubscriptionHandler], ScannerChannelHandler
        ] = {}

    async def subscribe_realtime(
        self,
        symbol: str,
        freq: str,
        scanner: Scanner,
        handler: SubscriptionHandler,
    ):
        stream_key = f"candles_min_{symbol}"
        sch = ScannerChannelHandler(symbol, scanner, handler, freq)
        self._handlers[(symbol, freq, handler)] = sch
        await self._channel.subscribe(stream_key, sch)

    async def unsubscribe_realtime(
        self,
        symbol: str,
        freq: str,
        handler: SubscriptionHandler,
    ):
        stream_key = f"candles_min_{symbol}"
        key = (symbol, freq, handler)
        sch = self._handlers.pop(key, None)
        if sch is not None:
            await self._channel.unsubscribe(stream_key, sch)
