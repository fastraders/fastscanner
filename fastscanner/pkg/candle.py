import asyncio
from typing import Awaitable, Callable

import pandas as pd

from fastscanner.pkg.clock import ClockRegistry
from fastscanner.services.indicators.ports import CandleCol as C

_TimeoutHandler = Callable[[pd.Series], Awaitable[None]]


class CandleBuffer:
    def __init__(
        self,
        symbol: str,
        freq: str,
        timeout_handler: _TimeoutHandler,
        timeout: float = 20,
    ):
        self._symbol = symbol
        self._freq = freq
        self._timeout = timeout
        self._buffer: dict[pd.Timestamp, pd.Series] = {}
        self._lock = asyncio.Lock()
        self._timeout_task: asyncio.Task | None = None
        self._timeout_handler = timeout_handler

    def _expected_ts(self, ts: pd.Timestamp) -> pd.Timestamp:
        return ts.floor(self._freq)

    async def add(self, row: pd.Series):
        if not isinstance(row.name, pd.Timestamp):
            raise ValueError("Expected row.name to be a pd.Timestamp")
        async with self._lock:
            self._buffer[row.name] = row
            ts = row.name.floor(self._freq)
            end_ts = ts + pd.Timedelta(self._freq) - pd.Timedelta("1min")
            if row.name == end_ts:
                return await self.flush()
            if self._timeout_task is None or self._timeout_task.done():
                self._timeout_task = asyncio.create_task(self._timeout_flush(ts))
        return None

    async def _timeout_flush(self, candle_start: pd.Timestamp):
        now = ClockRegistry.clock.now()
        flush_at = (
            candle_start
            + pd.Timedelta(self._freq)
            + pd.Timedelta(seconds=self._timeout)
        )
        await asyncio.sleep((flush_at - now).total_seconds())
        async with self._lock:
            row = await self.flush()
            if row is None:
                return
            await self._timeout_handler(row)

    async def flush(self):
        if not self._buffer:
            return None
        df = pd.DataFrame(self._buffer.values())
        ts = df.index[0].floor(self._freq)
        agg = pd.Series(
            {
                C.OPEN: df[C.OPEN].iloc[0],
                C.HIGH: df[C.HIGH].max(),
                C.LOW: df[C.LOW].min(),
                C.CLOSE: df[C.CLOSE].iloc[-1],
                C.VOLUME: df[C.VOLUME].sum(),
            },
            name=ts,
        )
        self._buffer.clear()
        return agg
