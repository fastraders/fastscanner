import json
import os
import tempfile
import time
from datetime import date, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from fastscanner.adapters.candle.partitioned_csv import PartitionedCSVCandlesProvider
from fastscanner.pkg.clock import LOCAL_TIMEZONE_STR, ClockRegistry, LocalClock
from fastscanner.services.indicators.ports import CandleCol, CandleStore

SYMBOL = "AAPL"
FREQ = "1min"
FAILFREQ = "0.3xyz"
UNIT = "min"
TEST_KEY = date(2023, 1, 1).strftime("%Y-%m-%d")


@pytest.fixture
def provider(tmp_path):
    mock_store = MagicMock()
    ClockRegistry.set(LocalClock())
    provider = PartitionedCSVCandlesProvider(mock_store)
    provider.CACHE_DIR = tmp_path / "candles"
    return provider


def test_save_and_load_cache(provider):
    df = pd.DataFrame(
        {
            CandleCol.DATETIME: pd.date_range(
                "2023-02-01", periods=5, freq="2min", tz="UTC"
            ),
            CandleCol.OPEN: [10, 20, 30, 40, 50],
            CandleCol.HIGH: [11, 21, 31, 41, 51],
            CandleCol.LOW: [9, 19, 29, 39, 49],
            CandleCol.CLOSE: [10, 20, 30, 40, 50],
            CandleCol.VOLUME: [500, 600, 700, 800, 900],
        }
    ).set_index(CandleCol.DATETIME)

    provider._save_cache(SYMBOL, TEST_KEY, FREQ, df)
    path = provider._partition_path(SYMBOL, TEST_KEY, FREQ)

    assert os.path.exists(path)
    df_loaded = pd.read_csv(path)
    assert len(df_loaded) == 5
    assert CandleCol.OPEN in df_loaded.columns


@pytest.mark.asyncio
async def test_fetch_and_cache_new_data(tmp_path):
    mock_store = AsyncMock()
    provider = PartitionedCSVCandlesProvider(mock_store)
    provider.CACHE_DIR = tmp_path

    df = pd.DataFrame(
        {
            CandleCol.DATETIME: pd.date_range(
                "2023-03-01", periods=3, freq="1min", tz="UTC"
            ),
            CandleCol.OPEN: [5, 6, 7],
            CandleCol.HIGH: [6, 7, 8],
            CandleCol.LOW: [4, 5, 6],
            CandleCol.CLOSE: [5, 6, 7],
            CandleCol.VOLUME: [100, 200, 300],
        }
    ).set_index(CandleCol.DATETIME)

    mock_store.get.return_value = df

    result_df = await provider._cache(SYMBOL, TEST_KEY, UNIT, FREQ)

    assert isinstance(result_df, pd.DataFrame)
    assert len(result_df) == 3


@pytest.mark.asyncio
@patch(
    "fastscanner.adapters.candle.partitioned_csv.PartitionedCSVCandlesProvider._cache"
)
async def test_get_success_path(mock_cache):
    df = pd.DataFrame(
        {
            CandleCol.OPEN: [1, 2, 3, 4],
            CandleCol.HIGH: [2, 3, 4, 5],
            CandleCol.LOW: [0, 1, 2, 3],
            CandleCol.CLOSE: [1, 2, 3, 4],
            CandleCol.VOLUME: [100, 110, 120, 130],
        },
        index=pd.date_range("2023-01-01", periods=4, freq="D", tz="America/New_York"),
    )
    df.index.name = CandleCol.DATETIME

    mock_cache.return_value = df
    provider = PartitionedCSVCandlesProvider(MagicMock())

    with patch.object(provider, "_partition_keys_in_range", return_value=["2023-01"]):
        result = await provider.get(SYMBOL, date(2023, 1, 1), date(2023, 1, 4), "1h")
        assert not result.empty
        assert len(result) == 4


@pytest.mark.asyncio
@patch(
    "fastscanner.adapters.candle.partitioned_csv.PartitionedCSVCandlesProvider._cache"
)
async def test_get_returns_empty_when_no_data(mock_cache):
    mock_cache.return_value = pd.DataFrame()
    provider = PartitionedCSVCandlesProvider(MagicMock())

    with patch.object(provider, "_partition_keys_in_range", return_value=["2023-01"]):
        result = await provider.get(SYMBOL, date(2023, 1, 1), date(2023, 1, 5), "1h")

        assert result.empty
        assert list(result.columns) == list(CandleCol.RESAMPLE_MAP.keys())


@pytest.mark.asyncio
@patch(
    "fastscanner.adapters.candle.partitioned_csv.PartitionedCSVCandlesProvider._cache"
)
async def test_get_trims_data_within_start_end(mock_cache):
    df = pd.DataFrame(
        {
            CandleCol.OPEN: [1, 2, 3],
            CandleCol.HIGH: [1, 2, 3],
            CandleCol.LOW: [1, 2, 3],
            CandleCol.CLOSE: [1, 2, 3],
            CandleCol.VOLUME: [10, 20, 30],
        },
        index=pd.date_range("2023-01-01", periods=3, freq="D", tz="America/New_York"),
    )
    df.index.name = CandleCol.DATETIME

    mock_cache.return_value = df
    provider = PartitionedCSVCandlesProvider(MagicMock())

    with patch.object(provider, "_partition_keys_in_range", return_value=["2023-01"]):
        result = await provider.get(SYMBOL, date(2023, 1, 2), date(2023, 1, 3), "1h")

        assert all(
            (result.index.date >= date(2023, 1, 2))  # type: ignore
            & (result.index.date <= date(2023, 1, 3))  # type: ignore
        )


@pytest.mark.asyncio
async def test_get_invalid_unit():
    provider = PartitionedCSVCandlesProvider((MagicMock()))
    with pytest.raises(ValueError, match="Invalid frequency"):
        await provider.get(SYMBOL, date(2023, 1, 1), date(2023, 1, 10), FAILFREQ)


@pytest.mark.asyncio
async def test_range_from_key_invalid_unit(provider):
    with pytest.raises(ValueError, match="Invalid unit"):
        provider._range_from_key("2023-01", "invalid_unit")


@pytest.mark.asyncio
async def test_cache_fallback_on_corrupt_file(tmp_path):
    mock_store = MagicMock()

    provider = PartitionedCSVCandlesProvider(store=mock_store)
    provider.CACHE_DIR = tmp_path / "candles"
    path = provider._partition_path("AAPL", "2023-04", "1min")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write("bad,data\n1,2,3")

    df = pd.DataFrame(
        {
            CandleCol.DATETIME: pd.date_range(
                "2023-04-01", periods=2, freq="1min", tz="UTC"
            ),
            CandleCol.OPEN: [10, 11],
            CandleCol.HIGH: [11, 12],
            CandleCol.LOW: [9, 10],
            CandleCol.CLOSE: [10, 11],
            CandleCol.VOLUME: [500, 600],
        }
    ).set_index(CandleCol.DATETIME)

    mock_store.get = AsyncMock(return_value=df)

    result = await provider._cache("AAPL", "2023", "d", "1min")

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2


@pytest.mark.asyncio
async def test_partition_keys_invalid_unit(provider):
    idx = pd.date_range("2023-01-01", periods=3, freq="D")
    with pytest.raises(ValueError, match="Invalid unit"):
        provider._partition_keys(idx, "badunit")


def test_partition_keys_in_range(provider):
    keys = provider._partition_keys_in_range(date(2023, 2, 1), date(2023, 2, 5), UNIT)
    assert isinstance(keys, list)
    assert all(isinstance(k, str) for k in keys)
    assert len(set(keys)) == len(keys)


def test_partition_path(provider):
    path = provider._partition_path(SYMBOL, TEST_KEY, FREQ)
    assert path.endswith(f"{TEST_KEY}.csv")
    assert FREQ in path


def test_partition_keys(provider):
    idx = pd.date_range("2023-01-01", "2023-01-03", freq="1h")
    keys = provider._partition_keys(idx, UNIT)
    assert isinstance(keys, pd.Series)
    assert not keys.empty


def test_range_from_key_min(provider):
    start, end = provider._range_from_key("2023-01-01", UNIT)
    assert isinstance(start, date)
    assert (end - start).days == 6


def test_range_from_key_hour(provider):
    start, end = provider._range_from_key("2023-01", "h")
    assert start.month == 1
    assert end.month == 1


@pytest.mark.asyncio
@patch("fastscanner.adapters.candle.partitioned_csv.ClockRegistry")
async def test_collect_expired_data_basic(mock_clock_registry, provider):
    today = datetime(2023, 5, 30, 12, 0, 0)
    mock_clock_registry.clock.now.return_value = today
    mock_clock_registry.clock.today.return_value = today.date()

    symbol = "AAPL"

    yesterday = today.date() - timedelta(days=1)
    provider._expirations = {
        symbol: {
            "2023-05-22_min": yesterday - timedelta(days=5),  # expired
            "2023-05-29_min": yesterday + timedelta(days=2),  # not expired
            "2023-05_h": yesterday - timedelta(days=3),  # expired
            "2023-01_d": yesterday + timedelta(days=10),  # not expired
        }
    }

    dummy_df = pd.DataFrame(
        {"OPEN": [1], "CLOSE": [2]},
        index=pd.date_range("2023-05-22", periods=1, freq="T", tz=LOCAL_TIMEZONE_STR),
    )
    dummy_df["datetime"] = dummy_df.index
    provider._store.get = AsyncMock(return_value=dummy_df)

    await provider.collect_expired_data(symbol)

    calls = provider._store.get.call_args_list

    called_freqs = [call.args[3] for call in calls]
    called_start_dates = [call.args[1] for call in calls]

    assert "1min" in called_freqs
    assert "1h" in called_freqs
    assert "1d" in called_freqs

    assert all(isinstance(sd, date) for sd in called_start_dates)

    expected_freqs = ["1min", "2min", "3min", "5min", "10min", "15min", "1h", "1d"]
    assert set(called_freqs) == set(expected_freqs)

    for call in calls:
        symbol_arg, start_arg, end_arg, freq_arg = call.args
        assert symbol_arg == "AAPL"
        assert isinstance(start_arg, date)
        assert isinstance(end_arg, date)
        assert freq_arg in expected_freqs


class MockClockBeforeMidnight:
    def __init__(self):
        self._start = time.time()

    def now(self):
        elapsed_time = time.time() - self._start  # Time elapsed in seconds

        base_time = datetime(
            2020, 1, 8, 23, 59, 59, 999000, tzinfo=ZoneInfo("UTC")
        )  # January 8, 2020, 23:59:59.999000

        new_time = base_time + timedelta(seconds=elapsed_time)

        return new_time


class MockClockSunday:
    def __init__(self, today: date):
        self._now = datetime.combine(
            today, datetime.min.time(), tzinfo=ZoneInfo(LOCAL_TIMEZONE_STR)
        )

    def now(self):
        return self._now


class MockStoreWithDelay:
    def __init__(self):
        self.calls = []

    async def get(self, symbol, start, end, freq):
        self.calls.append((symbol, start, end, freq))
        time.sleep(0.2)

        df = pd.DataFrame({"datetime": pd.to_datetime([], utc=True)}).set_index(
            "datetime"
        )
        return df

    async def splits(self, start, end):
        return {}


class MockStoreReturningData:
    def __init__(self):
        self.calls = []

    async def get(self, symbol, start, end, freq):
        self.calls.append((start, end, freq))
        rng = pd.date_range(start, end, freq="1D", tz=LOCAL_TIMEZONE_STR)
        df = pd.DataFrame(
            {
                CandleCol.DATETIME: rng,
                "open": 1,
                "high": 1,
                "low": 1,
                "close": 1,
                "volume": 100,
            }
        ).set_index(CandleCol.DATETIME)
        return df

    async def splits(self, start, end):
        return {}


def setup_cache_env(tmp_path):
    cache_dir = tmp_path
    PartitionedCSVCandlesProvider.CACHE_DIR = str(cache_dir)
    os.makedirs(cache_dir / "AAPL", exist_ok=True)
    (cache_dir / "last_checked_splits.txt").write_text("2019-12-31")


@pytest.mark.asyncio
async def test_midnight_expiration_skips_days(tmp_path):
    setup_cache_env(tmp_path)

    clock = MockClockBeforeMidnight()
    ClockRegistry.set(clock)

    now = clock.now()
    today = now.date()
    yesterday = today - timedelta(days=1)
    tomorrow = today + timedelta(days=1)

    store = MockStoreWithDelay()
    provider = PartitionedCSVCandlesProvider(store)

    await provider.collect_expired_data("AAPL", ["1min"])

    assert len(store.calls) > 0, "No calls made to store"

    assert "AAPL" in provider._expirations, "No expirations set for AAPL"

    for exp_date in provider._expirations["AAPL"].values():
        assert exp_date == tomorrow, f"Expiration should be {tomorrow}, got {exp_date}"

    today_str = today.strftime("%Y-%m-%d")
    assert not any(
        today_str in key for key in provider._expirations.get("AAPL", {})
    ), f"Data was unexpectedly collected for {today}"


# @pytest.mark.asyncio
# async def test_sunday_run_misses_week(tmp_path):
#     setup_cache_env(tmp_path)
#     store = MockStoreReturningData()

#     ClockRegistry.set(MockClockSunday(date(2020, 1, 6)))
#     provider = PartitionedCSVCandlesProvider(store)
#     await provider.collect_expired_data("AAPL")

#     ClockRegistry.set(MockClockSunday(date(2020, 1, 20)))
#     await provider.collect_expired_data("AAPL")
#     expected_start = date(2020, 1, 6)
#     expected_end = date(2020, 1, 12)
#     week_collected = any(
#         (start == expected_start and end == expected_end)
#         for start, end, _ in store.calls
#     )
#     assert (
#         week_collected
#     ), f"Expected week {expected_start} to {expected_end} to be collected"
