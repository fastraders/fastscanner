from datetime import datetime
from typing import Dict
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from fastscanner.pkg.candle import Candle
from fastscanner.pkg.clock import LOCAL_TIMEZONE_STR
from fastscanner.services.indicators.lib.fundamental import (
    SharesFloatIndicator,
    _DilutionTrackerDriver,
    _parse_float_value,
)
from fastscanner.services.indicators.ports import Cache, FundamentalDataStore
from fastscanner.services.registry import ApplicationRegistry


class MockCache(Cache):
    def __init__(self) -> None:
        self._data: Dict[str, str] = {}

    async def save(self, key: str, value: str) -> None:
        self._data[key] = value

    async def get(self, key: str) -> str:
        return self._data.get(key, "")


class FundamentalDataStoreTest(FundamentalDataStore):
    async def get(self, symbol: str):  # not used by this indicator
        raise NotImplementedError


@pytest.fixture
def cache():
    mock_cache = MockCache()
    ApplicationRegistry.init(
        candles=None,  # type: ignore
        fundamentals=FundamentalDataStoreTest(),
        holidays=None,  # type: ignore
        cache=mock_cache,
    )
    yield mock_cache
    ApplicationRegistry.reset()


def _make_row(ts: datetime) -> Candle:
    df = pd.DataFrame(
        {"close": [10.0]},
        index=pd.DatetimeIndex([ts]).tz_localize(LOCAL_TIMEZONE_STR),
    )
    row = Candle.from_series(df.iloc[0])
    row.timestamp = df.index[0]
    return row


def test_type_and_column_name():
    assert SharesFloatIndicator.type() == "shares_float"
    assert SharesFloatIndicator().column_name() == "shares_float"


def test_cache_key():
    assert SharesFloatIndicator._cache_key("AAPL") == "indicator:shares_float:AAPL"


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("1.5M", 1_500_000.0),
        ("10.2K", 10_200.0),
        ("1.2B", 1_200_000_000.0),
        ("500", 500.0),
        ("1,234,567", 1_234_567.0),
        ("3.5m", 3_500_000.0),
        ("N/A", None),
        ("", None),
        ("garbage", None),
    ],
)
def test_parse_float_value(raw, expected):
    assert _parse_float_value(raw) == expected


@pytest.mark.asyncio
async def test_extend_fills_with_na():
    indicator = SharesFloatIndicator()
    df = pd.DataFrame({"close": [1.0, 2.0]})
    out = await indicator.extend("AAPL", df)
    assert out[indicator.column_name()].isna().all()


@pytest.mark.asyncio
async def test_extend_realtime_consumer_reads_from_cache(cache):
    cache._data[SharesFloatIndicator._cache_key("AAPL")] = "1500000.0"
    indicator = SharesFloatIndicator(caching=False)

    row = _make_row(datetime(2026, 5, 3, 10, 0))
    extended = await indicator.extend_realtime("AAPL", row)

    assert extended[indicator.column_name()] == 1_500_000.0
    assert indicator._float["AAPL"] == 1_500_000.0


@pytest.mark.asyncio
async def test_extend_realtime_consumer_returns_none_when_cache_empty(cache):
    indicator = SharesFloatIndicator(caching=False)
    row = _make_row(datetime(2026, 5, 3, 10, 0))
    extended = await indicator.extend_realtime("AAPL", row)
    assert extended[indicator.column_name()] is None


@pytest.mark.asyncio
async def test_extend_realtime_uses_in_memory_value_without_hitting_cache(cache):
    indicator = SharesFloatIndicator(caching=False)
    row = _make_row(datetime(2026, 5, 3, 10, 0))
    indicator._last_date["AAPL"] = row.timestamp.date()
    indicator._float["AAPL"] = 999_999.0

    cache.get = AsyncMock(side_effect=AssertionError("should not be called"))
    extended = await indicator.extend_realtime("AAPL", row)
    assert extended[indicator.column_name()] == 999_999.0


@pytest.mark.asyncio
async def test_day_rollover_clears_in_memory_value(cache):
    indicator = SharesFloatIndicator(caching=False)
    indicator._last_date["AAPL"] = datetime(2026, 5, 2).date()
    indicator._float["AAPL"] = 100.0

    row = _make_row(datetime(2026, 5, 3, 10, 0))
    extended = await indicator.extend_realtime("AAPL", row)
    # cleared, fell through to cache (empty), so None
    assert extended[indicator.column_name()] is None
    assert "AAPL" not in indicator._float


@pytest.mark.asyncio
async def test_extend_realtime_caching_spawns_fetch_and_caches(cache):
    indicator = SharesFloatIndicator(caching=True)
    row = _make_row(datetime(2026, 5, 3, 10, 0))

    with patch.object(
        _DilutionTrackerDriver, "fetch_float", new=AsyncMock(return_value=2_500_000.0)
    ) as mocked:
        first = await indicator.extend_realtime("AAPL", row)
        # first call returns None because fetch is async / fire-and-forget
        assert first[indicator.column_name()] is None

        # let the spawned task run
        task = indicator._tasks["AAPL"]
        await task

    mocked.assert_awaited_once_with("AAPL")
    assert indicator._float["AAPL"] == 2_500_000.0
    assert cache._data[SharesFloatIndicator._cache_key("AAPL")] == "2500000.0"

    # second call sees in-memory value
    second = await indicator.extend_realtime("AAPL", _make_row(datetime(2026, 5, 3, 10, 1)))
    assert second[indicator.column_name()] == 2_500_000.0


@pytest.mark.asyncio
async def test_caching_does_not_double_spawn_for_inflight_symbol(cache):
    indicator = SharesFloatIndicator(caching=True)
    row = _make_row(datetime(2026, 5, 3, 10, 0))

    call_count = 0

    async def slow_fetch(symbol: str):
        nonlocal call_count
        call_count += 1
        return 1.0

    with patch.object(_DilutionTrackerDriver, "fetch_float", new=slow_fetch):
        await indicator.extend_realtime("AAPL", row)
        # second call before the first task completes should NOT spawn another
        await indicator.extend_realtime("AAPL", _make_row(datetime(2026, 5, 3, 10, 0, 30)))
        await indicator._tasks["AAPL"]

    assert call_count == 1


@pytest.mark.asyncio
async def test_caching_swallows_fetch_failures(cache):
    indicator = SharesFloatIndicator(caching=True)
    row = _make_row(datetime(2026, 5, 3, 10, 0))

    with patch.object(
        _DilutionTrackerDriver, "fetch_float", new=AsyncMock(side_effect=RuntimeError("boom"))
    ):
        await indicator.extend_realtime("AAPL", row)
        await indicator._tasks["AAPL"]

    assert "AAPL" not in indicator._float
    assert SharesFloatIndicator._cache_key("AAPL") not in cache._data


@pytest.mark.asyncio
async def test_caching_caches_none_when_fetch_returns_none(cache):
    indicator = SharesFloatIndicator(caching=True)
    row = _make_row(datetime(2026, 5, 3, 10, 0))

    with patch.object(
        _DilutionTrackerDriver, "fetch_float", new=AsyncMock(return_value=None)
    ):
        first = await indicator.extend_realtime("AAPL", row)
        assert first[indicator.column_name()] is None
        await indicator._tasks["AAPL"]

    # None is now a known-good value: in memory and persisted in shared cache
    assert indicator._float["AAPL"] is None
    assert (
        cache._data[SharesFloatIndicator._cache_key("AAPL")]
        == SharesFloatIndicator._NULL_CACHE_VALUE
    )

    # Subsequent realtime ticks return None without re-spawning the fetch
    with patch.object(
        _DilutionTrackerDriver, "fetch_float", new=AsyncMock(return_value=999.0)
    ) as should_not_run:
        second = await indicator.extend_realtime(
            "AAPL", _make_row(datetime(2026, 5, 3, 10, 1))
        )
    assert second[indicator.column_name()] is None
    should_not_run.assert_not_awaited()


@pytest.mark.asyncio
async def test_consumer_reads_null_sentinel_from_cache(cache):
    cache._data[SharesFloatIndicator._cache_key("AAPL")] = (
        SharesFloatIndicator._NULL_CACHE_VALUE
    )
    indicator = SharesFloatIndicator(caching=False)

    extended = await indicator.extend_realtime(
        "AAPL", _make_row(datetime(2026, 5, 3, 10, 0))
    )

    assert extended[indicator.column_name()] is None
    # remembered in memory so subsequent ticks don't hit the cache again
    assert "AAPL" in indicator._float
    assert indicator._float["AAPL"] is None

    cache.get = AsyncMock(side_effect=AssertionError("should not be called"))
    second = await indicator.extend_realtime(
        "AAPL", _make_row(datetime(2026, 5, 3, 10, 1))
    )
    assert second[indicator.column_name()] is None
