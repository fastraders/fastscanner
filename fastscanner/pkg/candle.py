import asyncio
from typing import Any, Awaitable, Callable

import pandas as pd

from fastscanner.pkg.clock import ClockRegistry
from fastscanner.services.indicators.ports import CandleCol as C

_TimeoutHandler = Callable[[dict[str, Any]], Awaitable[None]]


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
        self._buffer: dict[pd.Timestamp, dict[str, Any]] = {}
        self._lock = asyncio.Lock()
        self._timeout_task: asyncio.Task | None = None
        self._timeout_handler = timeout_handler

    def _expected_ts(self, ts: pd.Timestamp) -> pd.Timestamp:
        return ts.floor(self._freq)

    async def add(self, row_dict: dict[str, Any]):
        if "datetime" not in row_dict or not isinstance(
            row_dict["datetime"], pd.Timestamp
        ):
            raise ValueError(
                "Expected row_dict to have 'datetime' key with pd.Timestamp value"
            )
        async with self._lock:
            ts = row_dict["datetime"]
            self._buffer[ts] = row_dict
            floor_ts = ts.floor(self._freq)
            end_ts = floor_ts + pd.Timedelta(self._freq) - pd.Timedelta("1min")
            if ts == end_ts:
                return await self.flush()
            if self._timeout_task is None or self._timeout_task.done():
                self._timeout_task = asyncio.create_task(self._timeout_flush(floor_ts))
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

        rows = list(self._buffer.values())
        timestamps = [row["datetime"] for row in rows]
        ts = min(timestamps).floor(self._freq)

        agg_dict = {
            "datetime": ts,
            C.OPEN: rows[0][C.OPEN],
            C.HIGH: max(row[C.HIGH] for row in rows),
            C.LOW: min(row[C.LOW] for row in rows),
            C.CLOSE: rows[-1][C.CLOSE],
            C.VOLUME: sum(row[C.VOLUME] for row in rows),
        }

        self._buffer.clear()
        return agg_dict
