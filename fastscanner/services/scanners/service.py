import asyncio
from datetime import datetime, timedelta
from typing import Any, Protocol

import pandas as pd

from fastscanner.pkg.candle import CandleBuffer
from fastscanner.pkg.clock import LOCAL_TIMEZONE_STR, ClockRegistry
from fastscanner.services.indicators.ports import CandleCol as C
from fastscanner.services.indicators.ports import CandleStore, Channel, ChannelHandler
from fastscanner.services.indicators.utils import lookback_days
from fastscanner.services.scanners.lib import ScannersLibrary
from fastscanner.services.scanners.ports import Scanner, ScannerParams, SymbolsProvider


class SubscriptionHandler(Protocol):
    async def handle(
        self, symbol: str, new_row: pd.Series, passed: bool
    ) -> pd.Series: ...

    def set_scanner_id(self, scanner_id: str): ...


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

    def id(self) -> str:
        return f"{self._scanner.id()}_{self._symbol}"

    async def _handle(self, row: pd.Series) -> None:
        new_row, passed = await self._scanner.scan_realtime(
            self._symbol, row, self._freq
        )
        await self._handler.handle(self._symbol, new_row, passed)

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
            await self._handler.handle(self._symbol, new_row, passed)
            return
        agg = await self._buffer.add(row)
        if agg is None:
            return
        await self._handle(agg)


class ScannerService:
    def __init__(
        self, candles: CandleStore, channel: Channel, symbols_provider: SymbolsProvider
    ):
        self._candles = candles
        self._channel = channel
        self._symbols_provider = symbols_provider
        self._handlers: dict[str, list[ScannerChannelHandler]] = {}

    async def _subscribe_symbol(
        self, symbol: str, scanner: Scanner, handler: SubscriptionHandler, freq: str
    ) -> ScannerChannelHandler:
        stream_key = f"candles_min_{symbol}"
        sch = ScannerChannelHandler(symbol, scanner, handler, freq)
        await self._channel.subscribe(stream_key, sch)
        return sch

    async def subscribe_realtime(
        self,
        params: ScannerParams,
        handler: SubscriptionHandler,
        freq: str,
    ) -> str:
        scanner = ScannersLibrary.instance().get(params.type_, params.params)
        scanner_id = scanner.id()
        handler.set_scanner_id(scanner_id)
        symbols = await self._symbols_provider.active_symbols()
        tasks = [
            asyncio.create_task(self._subscribe_symbol(symbol, scanner, handler, freq))
            for symbol in symbols
        ]
        handlers = await asyncio.gather(*tasks)

        self._handlers[scanner_id] = handlers

        return scanner_id

    async def unsubscribe_realtime(self, scanner_id: str):

        if scanner_id not in self._handlers:
            return

        handlers = self._handlers[scanner_id]

        for handler in handlers:
            stream_key = f"candles_min_{handler._symbol}"
            await self._channel.unsubscribe(stream_key, handler.id())

        del self._handlers[scanner_id]
