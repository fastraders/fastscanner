import asyncio
import logging
import math
import multiprocessing
from datetime import date, datetime, time, timedelta
from typing import Any, Awaitable, Callable, Protocol
from uuid import uuid4

import pandas as pd

from fastscanner.pkg.candle import CandleBuffer
from fastscanner.pkg.clock import LOCAL_TIMEZONE_STR, ClockRegistry, FixedClock
from fastscanner.services.indicators.lib import Cacheable
from fastscanner.services.indicators.ports import CandleCol as C
from fastscanner.services.indicators.ports import CandleStore, Channel
from fastscanner.services.registry import ApplicationRegistry
from fastscanner.services.scanners.lib import ScannersLibrary
from fastscanner.services.scanners.ports import (
    ScanAllResult,
    Scanner,
    ScannerParams,
    ScannerRealtime,
    SymbolsProvider,
)

from ..exceptions import UnsubscribeSignal

logger = logging.getLogger(__name__)


class SubscriptionHandler(Protocol):
    async def handle(
        self, symbol: str, new_row: pd.Series, passed: bool
    ) -> pd.Series: ...


class ScannerChannelHandler:
    def __init__(
        self,
        id_: str,
        scanner: ScannerRealtime,
        handler: SubscriptionHandler,
        freq: str,
        unsubscribe: Callable[[str], Awaitable[None]],
    ):
        self._id = id_
        self._scanner = scanner
        self._handler = handler
        self._freq = freq
        self._buffers: dict[str, CandleBuffer] = {}
        self._unsubscribe = unsubscribe

    def _new_buffer(self, symbol: str) -> CandleBuffer:
        async def _handle(row: pd.Series) -> None:
            new_row, passed = await self._scanner.scan_realtime(symbol, row)
            try:
                await self._handler.handle(symbol, new_row, passed)
            except UnsubscribeSignal:
                await self._unsubscribe(self._id)
                return

        buffer = CandleBuffer(symbol, self._freq, _handle)
        self._buffers[symbol] = buffer
        return buffer

    async def handle(self, channel_id: str, data: dict[Any, Any]) -> None:
        symbol = channel_id.split(".", 2)[-1]
        buffer = self._buffers.get(symbol)
        if buffer is None:
            buffer = self._new_buffer(symbol)
        ts = pd.to_datetime(int(data["timestamp"]), unit="ms", utc=True).tz_convert(
            LOCAL_TIMEZONE_STR
        )
        row = pd.Series(data, name=ts)

        if self._freq == "1min":
            new_row, passed = await self._scanner.scan_realtime(symbol, row)
            await self._handler.handle(symbol, new_row, passed)
            return
        agg = await buffer.add(row)
        if agg is None:
            return
        new_row, passed = await self._scanner.scan_realtime(symbol, agg)
        await self._handler.handle(symbol, new_row, passed)

    def id(self) -> str:
        return self._id


class ScannerService:
    def __init__(
        self, candles: CandleStore, channel: Channel, symbols_provider: SymbolsProvider
    ):
        self._candles = candles
        self._channel = channel
        self._symbols_provider = symbols_provider
        self._handlers: dict[str, ScannerChannelHandler] = {}
        self._lock = asyncio.Lock()

    async def subscribe_realtime(
        self,
        scanner_id: str,
        params: ScannerParams,
        handler: SubscriptionHandler,
        freq: str,
    ):
        scanner = ScannersLibrary.instance().get_realtime(params.type_, params.params)
        await ScannersLibrary.instance().load_all_cacheable(scanner)
        async with self._lock:
            stream_key = "candles.min.*"
            sch = ScannerChannelHandler(
                scanner_id, scanner, handler, freq, self.unsubscribe_realtime
            )
            await self._channel.subscribe(stream_key, sch)

            self._handlers[scanner_id] = sch

    async def unsubscribe_realtime(self, scanner_id: str):
        async with self._lock:
            if scanner_id not in self._handlers:
                return

            handler = self._handlers.pop(scanner_id)
            stream_key = "candles.min.*"
            await self._channel.unsubscribe(stream_key, handler.id())

    async def scan_all(
        self,
        scanner_type: str,
        params: dict[str, Any],
        start: date,
        end: date,
        freq: str,
    ) -> ScanAllResult:
        scanner = ScannersLibrary.instance().get(scanner_type, params)
        symbols = await self._symbols_provider.active_symbols()
        all_results = await asyncio.to_thread(_scan, scanner, symbols, start, end, freq)
        return ScanAllResult(results=all_results, scanner_type=scanner_type)


def _scan(
    scanner: Scanner,
    symbols: list[str],
    start_date: date,
    end_date: date,
    freq: str,
) -> list[dict]:
    n_workers = multiprocessing.cpu_count() // 2
    batch_size = math.ceil(len(symbols) / n_workers)
    registry_params = ApplicationRegistry.params_for_init()
    batches = [
        (
            scanner,
            symbols[i : i + batch_size],
            start_date,
            end_date,
            freq,
            registry_params,
        )
        for i in range(0, len(symbols), batch_size)
    ]

    with multiprocessing.Pool(n_workers) as pool:
        results = pool.map(_run_scanner_worker, batches)
        results = [item for sublist in results for item in sublist]
        return results


def _run_scanner_worker(
    args: tuple[Scanner, list[str], date, date, str, dict],
) -> list[dict]:
    return asyncio.run(_run_async_scan(*args))


async def _run_async_scan(
    scanner: Scanner,
    symbols: list[str],
    start_date: date,
    end_date: date,
    freq: str,
    registry_params: dict,
) -> list[dict]:
    ApplicationRegistry.init(**registry_params)
    ClockRegistry.set(
        FixedClock(datetime.combine(end_date + timedelta(days=1), time.min))
    )
    all_results: list[dict] = []
    for symbol in symbols:
        df = await scanner.scan(
            symbol=symbol,
            start=start_date,
            end=end_date,
            freq=freq,
        )
        if df.empty:
            continue

        df.loc[:, "date"] = df.index.date  # type: ignore
        df = df.reset_index().groupby("date").first(skipna=False)
        df["symbol"] = symbol
        symbol_results = df.to_dict(orient="records")
        all_results.extend(symbol_results)

    return all_results
