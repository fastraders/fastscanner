import asyncio
from datetime import datetime, timedelta
from typing import Any

import pandas as pd

from fastscanner.pkg.candle import CandleBuffer
from fastscanner.pkg.datetime import LOCAL_TIMEZONE_STR
from fastscanner.services.indicators.clock import ClockRegistry
from fastscanner.services.indicators.ports import CandleCol as C
from fastscanner.services.indicators.ports import CandleStore, Channel, ChannelHandler
from fastscanner.services.indicators.utils import lookback_days
from fastscanner.services.scanners.ports import Scanner


class SubscriptionHandler:
    def handle(self, symbol: str, new_row: pd.Series, passed: bool) -> pd.Series: ...


class ScannerChannelHandler(ChannelHandler):
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
        self._buffer = CandleBuffer(symbol, freq)

    async def handle(self, channel_id: str, data: dict[Any, Any]):
        for field in (C.OPEN, C.HIGH, C.LOW, C.CLOSE, C.VOLUME):
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
            return self._handler.handle(self._symbol, new_row, passed)
        agg = await self._buffer.add(row)
        if agg is None:
            return
        new_row, passed = await self._scanner.scan_realtime(
            self._symbol, agg, self._freq
        )
        self._handler.handle(self._symbol, new_row, passed)


class ScannerService:
    def __init__(self, candles: CandleStore, channel: Channel):
        self._candles = candles
        self._channel = channel

    async def subscribe_realtime(
        self,
        symbol: str,
        freq: str,
        scanner: Scanner,
        handler: SubscriptionHandler,
    ):
        max_days = scanner.lookback_days()
        today = datetime.now().date()

        if max_days > 0:
            lookback_start = lookback_days(today, max_days)
            end_date = today - timedelta(days=1)
            df = await self._candles.get(symbol, lookback_start, end_date, freq)
            for _, row in df.iterrows():
                await scanner.scan_realtime(symbol, row, freq)  # warm up

        stream_key = f"candles_min_{symbol}"
        await self._channel.subscribe(
            stream_key, ScannerChannelHandler(symbol, scanner, handler, freq)
        )
