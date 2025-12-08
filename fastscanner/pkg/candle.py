import asyncio
from dataclasses import dataclass
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
        self._lock = asyncio.Lock()
        self._timeout_task: asyncio.Task | None = None
        self._timeout_handler = timeout_handler
        self._buffer: pd.Series | None = None
        self._last_flushed_ts: pd.Timestamp | None = None

    def _get_base_freq(self, freq: str) -> str:
        freq = freq.lower()
        if freq.endswith("s"):
            return "1s"
        if freq.endswith("min"):
            return "1min"
        if freq.endswith("h"):
            return "1H"
        if freq.endswith("d"):
            return "1D"
        raise ValueError(f"Unsupported frequency: {freq}")

    async def add(self, row: pd.Series):
        async with self._lock:
            if not isinstance(row.name, pd.Timestamp):
                raise ValueError("Expected row.name to be a pd.Timestamp")
            buffer_ts = row.name.floor(self._freq)
            if self._last_flushed_ts is not None and buffer_ts <= self._last_flushed_ts:
                return None

            if self._buffer is None:
                self._buffer = row.rename(buffer_ts)
                self._timeout_task = asyncio.create_task(self._timeout_flush(buffer_ts))
            elif buffer_ts < self._buffer.name:  # type: ignore[attr-defined]
                return None
            elif buffer_ts > self._buffer.name:  # type: ignore[attr-defined]
                prev_buffer = self._buffer
                self._buffer = row.rename(buffer_ts)
                self._timeout_task = asyncio.create_task(self._timeout_flush(buffer_ts))
                self._last_flushed_ts = prev_buffer.name  # type: ignore[attr-defined]
                return prev_buffer
            else:
                self._buffer = self._agg_row(self._buffer, row)

            base_freq = self._get_base_freq(self._freq)
            end_ts = buffer_ts + pd.Timedelta(self._freq) - pd.Timedelta(base_freq)
            if row.name == end_ts:
                self._last_flushed_ts = self._buffer.name  # type: ignore[attr-defined]
                buffer = self._buffer
                self._buffer = None
                return buffer
            return None

    async def _timeout_flush(self, buffer_ts: pd.Timestamp):
        now = ClockRegistry.clock.now()
        flush_at = (
            buffer_ts + pd.Timedelta(self._freq) + pd.Timedelta(seconds=self._timeout)
        )
        await asyncio.sleep((flush_at - now).total_seconds())
        async with self._lock:
            if self._buffer is None or self._buffer.name != buffer_ts:
                return

            self._last_flushed_ts = buffer_ts
            await self._timeout_handler(self._buffer)
            self._buffer = None

    def _agg_row(self, base_row: pd.Series, other: pd.Series) -> pd.Series:
        return pd.Series(
            {
                C.OPEN: base_row[C.OPEN],
                C.HIGH: max(base_row[C.HIGH], other[C.HIGH]),
                C.LOW: min(base_row[C.LOW], other[C.LOW]),
                C.CLOSE: other[C.CLOSE],
                C.VOLUME: base_row[C.VOLUME] + other[C.VOLUME],
            },
            name=base_row.name,
        )
