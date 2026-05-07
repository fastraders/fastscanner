import asyncio
from datetime import date, datetime, time
from unittest.mock import AsyncMock
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from fastscanner.pkg.candle import Candle
from fastscanner.pkg.clock import LOCAL_TIMEZONE_STR, ClockRegistry, FixedClock
from fastscanner.services.indicators.service import (
    IndicatorsService,
    _prewarm_marker_key,
)
from fastscanner.services.indicators.tests.fixtures import (
    MockCache,
    MockFundamentalDataStore,
    MockPublicHolidaysStore,
)
from fastscanner.services.registry import ApplicationRegistry


EST = ZoneInfo(LOCAL_TIMEZONE_STR)
TODAY = date(2026, 5, 7)


class _FakeSymbols:
    def __init__(self, symbols: list[str]):
        self._symbols = symbols

    async def active_symbols(self) -> list[str]:
        return list(self._symbols)


class _RecordingIndicator:
    """Mimics extend_realtime cold-path: records calls and exposes save_to_cache."""

    def __init__(self, type_: str, *, fail_for: set[str] | None = None) -> None:
        self._type = type_
        self.calls: list[tuple[str, pd.Timestamp]] = []
        self.save_calls = 0
        self._fail_for = fail_for or set()

    @classmethod
    def type(cls) -> str:
        return "recording"

    def column_name(self) -> str:
        return self._type

    async def extend_realtime(self, symbol: str, new_row: Candle) -> Candle:
        self.calls.append((symbol, new_row.timestamp))
        if symbol in self._fail_for:
            raise RuntimeError(f"boom {symbol}")
        return new_row

    async def extend(self, symbol, df):  # pragma: no cover
        return df

    async def save_to_cache(self) -> None:
        self.save_calls += 1

    async def load_from_cache(self, symbol=None):  # pragma: no cover
        return None


class _CountingSemaphoreIndicator(_RecordingIndicator):
    def __init__(self, type_: str, *, hold_seconds: float = 0.01) -> None:
        super().__init__(type_)
        self._inflight = 0
        self.max_inflight = 0
        self._hold_seconds = hold_seconds
        self._lock = asyncio.Lock()

    async def extend_realtime(self, symbol: str, new_row: Candle) -> Candle:
        async with self._lock:
            self._inflight += 1
            if self._inflight > self.max_inflight:
                self.max_inflight = self._inflight
        await asyncio.sleep(self._hold_seconds)
        async with self._lock:
            self._inflight -= 1
        return await super().extend_realtime(symbol, new_row)


class _FailingSaveIndicator(_RecordingIndicator):
    async def save_to_cache(self) -> None:
        self.save_calls += 1
        raise RuntimeError("save broke")


def _make_service(cache: MockCache) -> IndicatorsService:
    return IndicatorsService(
        candles=AsyncMock(),
        fundamentals=AsyncMock(),
        channel=AsyncMock(),
        cache=cache,
        symbols_subscribe_channel="sub",
        symbols_unsubscribe_channel="unsub",
        cache_at_seconds=10,
        symbols_slow_indicators_subscribe_channel="slow_sub",
        symbols_slow_indicators_unsubscribe_channel="slow_unsub",
    )


def _setup(now: datetime) -> tuple[MockCache, IndicatorsService]:
    cache = MockCache()
    ApplicationRegistry.init(
        candles=None,  # type: ignore[arg-type]
        fundamentals=MockFundamentalDataStore(),
        holidays=MockPublicHolidaysStore(),
        cache=cache,
    )
    ClockRegistry.set(FixedClock(now))
    return cache, _make_service(cache)


@pytest.fixture(autouse=True)
def _reset_registry():
    yield
    try:
        ApplicationRegistry.reset()
    except AttributeError:
        pass
    if ClockRegistry.is_set():
        ClockRegistry.unset()


def _seed_prewarm_state(
    service: IndicatorsService,
    symbols: list[str],
    indicators: list,
    concurrency: int = 50,
) -> None:
    """Manually populate prewarm config without spawning the loop task."""
    service._prewarm_symbols_source = _FakeSymbols(symbols)
    service._prewarm_indicators = indicators
    service._prewarm_concurrency = concurrency
    service._prewarm_at = time(3, 0)


@pytest.mark.asyncio
async def test_run_once_all_succeed_marks_completed_and_saves():
    cache, service = _setup(datetime(2026, 5, 7, 3, 0, tzinfo=EST))
    ind_a = _RecordingIndicator("a")
    ind_b = _RecordingIndicator("b")
    _seed_prewarm_state(service, ["AAPL", "MSFT"], [ind_a, ind_b])

    await service._run_prewarm_once()

    assert {s for s, _ in ind_a.calls} == {"AAPL", "MSFT"}
    assert {s for s, _ in ind_b.calls} == {"AAPL", "MSFT"}
    assert ind_a.save_calls == 1
    assert ind_b.save_calls == 1
    assert await cache.get(_prewarm_marker_key(TODAY)) == "1"


@pytest.mark.asyncio
async def test_run_once_per_symbol_failure_isolated():
    cache, service = _setup(datetime(2026, 5, 7, 3, 0, tzinfo=EST))
    ind = _RecordingIndicator("a", fail_for={"BAD"})
    _seed_prewarm_state(service, ["AAPL", "BAD", "MSFT"], [ind])

    stats = await service._run_prewarm_once()

    assert stats.succeeded == 2
    assert stats.failed == 1
    assert ind.save_calls == 1
    assert await cache.get(_prewarm_marker_key(TODAY)) == "1"


@pytest.mark.asyncio
async def test_run_once_save_failure_skips_marker():
    cache, service = _setup(datetime(2026, 5, 7, 3, 0, tzinfo=EST))
    ind = _FailingSaveIndicator("a")
    _seed_prewarm_state(service, ["AAPL"], [ind])

    stats = await service._run_prewarm_once()

    assert stats.succeeded == 1
    assert await cache.get(_prewarm_marker_key(TODAY)) == ""


@pytest.mark.asyncio
async def test_run_once_concurrency_capped_by_semaphore():
    cache, service = _setup(datetime(2026, 5, 7, 3, 0, tzinfo=EST))
    ind = _CountingSemaphoreIndicator("a", hold_seconds=0.02)
    _seed_prewarm_state(
        service,
        [f"S{i}" for i in range(40)],
        [ind],
        concurrency=5,
    )

    await service._run_prewarm_once()

    assert ind.max_inflight <= 5
    assert ind.max_inflight >= 2  # actually parallelized


@pytest.mark.asyncio
async def test_run_once_emits_summary_log(caplog):
    cache, service = _setup(datetime(2026, 5, 7, 3, 0, tzinfo=EST))
    ind = _RecordingIndicator("a", fail_for={"BAD"})
    _seed_prewarm_state(service, ["AAPL", "BAD"], [ind])

    import logging

    caplog.set_level(logging.INFO, logger="fastscanner.services.indicators.service")
    await service._run_prewarm_once()

    summary_lines = [
        r.message for r in caplog.records if "[daily_prewarm] done" in r.message
    ]
    assert len(summary_lines) == 1
    assert "succeeded=1" in summary_lines[0]
    assert "failed=1" in summary_lines[0]
    assert "elapsed=" in summary_lines[0]
    assert "outcome=partial" in summary_lines[0]


@pytest.mark.asyncio
async def test_is_warmed_for_today_states():
    cache, service = _setup(datetime(2026, 5, 7, 8, 0, tzinfo=EST))

    # Marker absent.
    assert await service._is_prewarmed_today() is False

    # Marker for yesterday.
    await cache.save(_prewarm_marker_key(date(2026, 5, 6)), "1")
    assert await service._is_prewarmed_today() is False

    # Marker for today.
    await cache.save(_prewarm_marker_key(date(2026, 5, 7)), "1")
    assert await service._is_prewarmed_today() is True


@pytest.mark.asyncio
async def test_start_runs_immediately_when_not_warmed():
    cache, service = _setup(datetime(2026, 5, 7, 8, 0, tzinfo=EST))
    ind = _RecordingIndicator("a")

    await service.start_daily_prewarm(
        symbols_source=_FakeSymbols(["AAPL"]), indicators=[ind]
    )
    for _ in range(50):
        if ind.save_calls > 0:
            break
        await asyncio.sleep(0)
    await service.stop_daily_prewarm()

    assert ind.save_calls == 1
    assert {s for s, _ in ind.calls} == {"AAPL"}


@pytest.mark.asyncio
async def test_start_skips_run_when_already_warmed():
    cache, service = _setup(datetime(2026, 5, 7, 8, 0, tzinfo=EST))
    await cache.save(_prewarm_marker_key(TODAY), "1")
    ind = _RecordingIndicator("a")

    await service.start_daily_prewarm(
        symbols_source=_FakeSymbols(["AAPL"]), indicators=[ind]
    )
    await asyncio.sleep(0)
    await service.stop_daily_prewarm()

    assert ind.calls == []
    assert ind.save_calls == 0


@pytest.mark.asyncio
async def test_stop_cancels_loop_cleanly():
    cache, service = _setup(datetime(2026, 5, 7, 8, 0, tzinfo=EST))
    await cache.save(_prewarm_marker_key(TODAY), "1")  # skip immediate run

    await service.start_daily_prewarm(symbols_source=_FakeSymbols([]), indicators=[])
    assert service._prewarm_task is not None
    await service.stop_daily_prewarm()
    assert service._prewarm_task is None


# -------- Integration test (load-bearing) --------


class _CountingCandleStore:
    def __init__(self):
        self.calls: list[tuple[str, date, date, str]] = []

    async def get(self, symbol, start, end, freq, adjusted: bool = True):
        self.calls.append((symbol, start, end, freq))
        idx = pd.date_range(start=start, end=start, freq="D")
        return pd.DataFrame({"close": [100.0]}, index=idx)


@pytest.mark.asyncio
async def test_real_candle_after_prewarm_skips_cold_path_read():
    """After pre-warm, a real candle for the same (symbol, today) must NOT trigger
    candles.get inside extend_realtime's cold-path."""
    from fastscanner.services.indicators.lib.daily import PrevDayIndicator
    from fastscanner.services.indicators.ports import CandleCol

    cache = MockCache()
    candle_store = _CountingCandleStore()
    ApplicationRegistry.init(
        candles=candle_store,
        fundamentals=MockFundamentalDataStore(),
        holidays=MockPublicHolidaysStore(),
        cache=cache,
    )
    ClockRegistry.set(FixedClock(datetime(2026, 5, 7, 3, 0, tzinfo=EST)))

    service = _make_service(cache)
    ind = PrevDayIndicator(candle_col=CandleCol.CLOSE)
    _seed_prewarm_state(service, ["AAPL"], [ind])

    await service._run_prewarm_once()
    calls_after_prewarm = len(candle_store.calls)
    assert calls_after_prewarm >= 1  # pre-warm did read

    real_ts = pd.Timestamp(2026, 5, 7, 9, 31, tz=LOCAL_TIMEZONE_STR)
    real_candle = Candle(
        {
            CandleCol.OPEN: 101.0,
            CandleCol.HIGH: 102.0,
            CandleCol.LOW: 100.5,
            CandleCol.CLOSE: 101.5,
            CandleCol.VOLUME: 1000,
        },
        timestamp=real_ts,
    )
    await ind.extend_realtime("AAPL", real_candle)

    # Cold-path was bypassed: no new candles.get call.
    assert len(candle_store.calls) == calls_after_prewarm
    assert real_candle[ind.column_name()] == 100.0
