from datetime import date, datetime
from typing import Dict

import pandas as pd
import pytest

from fastscanner.pkg.candle import Candle
from fastscanner.pkg.clock import ClockRegistry, FixedClock
from fastscanner.services.indicators.lib.fundamental import DaysSinceIPOIndicator
from fastscanner.services.indicators.ports import (
    Cache,
    CandleCol,
    FundamentalData,
    FundamentalDataStore,
)
from fastscanner.services.registry import ApplicationRegistry


class FundamentalDataStoreTest(FundamentalDataStore):
    def __init__(self) -> None:
        self._data: Dict[str, FundamentalData] = {}

    def set(self, symbol: str, data: FundamentalData) -> None:
        self._data[symbol] = data

    async def get(self, symbol: str) -> FundamentalData:
        return self._data[symbol]


class MockCache(Cache):
    def __init__(self):
        self._data = {}

    async def save(self, key: str, value: str) -> None:
        self._data[key] = value

    async def get(self, key: str) -> str:
        return self._data.get(key, "")


def setup_module(module):
    ClockRegistry.set(FixedClock(datetime(2023, 2, 1, 9, 30)))


@pytest.fixture
def fundamentals():
    store = FundamentalDataStoreTest()
    cache = MockCache()
    ApplicationRegistry.init(candles=None, fundamentals=store, holidays=None, cache=cache)  # type: ignore
    yield store
    ApplicationRegistry.reset()


def create_fundamental_data(ipo_date: str | None) -> FundamentalData:
    return FundamentalData(
        type="Common Stock",
        exchange="NYSE",
        country="USA",
        city="New York",
        gic_industry="Technology",
        gic_sector="Information Technology",
        historical_market_cap=pd.Series(dtype=float),
        earnings_dates=pd.DatetimeIndex([]),
        insiders_ownership_perc=10.0,
        institutional_ownership_perc=70.0,
        shares_float=1000000.0,
        beta=1.2,
        ipo_date=ipo_date,
        ceo_name=None,
        cfo_name=None,
    )


def test_type():
    assert DaysSinceIPOIndicator.type() == "days_since_ipo"


def test_column_name():
    indicator = DaysSinceIPOIndicator()
    assert indicator.column_name() == "days_since_ipo"


@pytest.mark.asyncio
async def test_extend_with_ipo_date(fundamentals: FundamentalDataStoreTest):
    fundamentals.set("AAPL", create_fundamental_data("2023-01-01"))

    dates = [
        datetime(2023, 1, 10, 9, 30),
        datetime(2023, 1, 10, 10, 0),
        datetime(2023, 1, 20, 9, 30),
        datetime(2023, 2, 1, 9, 30),
    ]
    df = pd.DataFrame(
        {CandleCol.CLOSE: [100, 101, 102, 103]}, index=pd.DatetimeIndex(dates)
    )

    indicator = DaysSinceIPOIndicator()
    result_df = await indicator.extend("AAPL", df)

    expected_days = [9, 9, 19, 31]
    assert result_df[indicator.column_name()].tolist() == expected_days


@pytest.mark.asyncio
async def test_extend_with_null_ipo_date(fundamentals: FundamentalDataStoreTest):
    fundamentals.set("AAPL", create_fundamental_data(None))

    dates = [
        datetime(2023, 1, 10, 9, 30),
        datetime(2023, 1, 20, 9, 30),
    ]
    df = pd.DataFrame({CandleCol.CLOSE: [100, 101]}, index=pd.DatetimeIndex(dates))

    indicator = DaysSinceIPOIndicator()
    result_df = await indicator.extend("AAPL", df)

    assert pd.isna(result_df[indicator.column_name()]).all()


@pytest.mark.asyncio
async def test_extend_before_ipo_date_returns_nan(
    fundamentals: FundamentalDataStoreTest,
):
    fundamentals.set("AAPL", create_fundamental_data("2023-01-15"))

    dates = [
        datetime(2023, 1, 10, 9, 30),
        datetime(2023, 1, 14, 9, 30),
        datetime(2023, 1, 15, 9, 30),
        datetime(2023, 1, 16, 9, 30),
    ]
    df = pd.DataFrame(
        {CandleCol.CLOSE: [100, 101, 102, 103]}, index=pd.DatetimeIndex(dates)
    )

    indicator = DaysSinceIPOIndicator()
    result_df = await indicator.extend("AAPL", df)

    col = indicator.column_name()
    assert pd.isna(result_df[col].iloc[0])
    assert pd.isna(result_df[col].iloc[1])
    assert result_df[col].iloc[2] == 0
    assert result_df[col].iloc[3] == 1


@pytest.mark.asyncio
async def test_extend_realtime_with_ipo_date(fundamentals: FundamentalDataStoreTest):
    fundamentals.set("AAPL", create_fundamental_data("2023-01-01"))

    indicator = DaysSinceIPOIndicator()
    row = Candle(
        {CandleCol.CLOSE: 100.0},
        timestamp=pd.Timestamp(2023, 1, 11, 10, 0),
    )
    result = await indicator.extend_realtime("AAPL", row)
    assert result[indicator.column_name()] == 10


@pytest.mark.asyncio
async def test_extend_realtime_with_null_ipo_date(
    fundamentals: FundamentalDataStoreTest,
):
    fundamentals.set("AAPL", create_fundamental_data(None))

    indicator = DaysSinceIPOIndicator()
    row = Candle(
        {CandleCol.CLOSE: 100.0},
        timestamp=pd.Timestamp(2023, 1, 11, 10, 0),
    )
    result = await indicator.extend_realtime("AAPL", row)
    assert result[indicator.column_name()] is None


@pytest.mark.asyncio
async def test_extend_realtime_caches_per_day(fundamentals: FundamentalDataStoreTest):
    fundamentals.set("AAPL", create_fundamental_data("2023-01-01"))

    indicator = DaysSinceIPOIndicator()

    row1 = Candle(
        {CandleCol.CLOSE: 100.0},
        timestamp=pd.Timestamp(2023, 1, 11, 9, 30),
    )
    result1 = await indicator.extend_realtime("AAPL", row1)
    assert result1[indicator.column_name()] == 10

    row2 = Candle(
        {CandleCol.CLOSE: 101.0},
        timestamp=pd.Timestamp(2023, 1, 11, 10, 0),
    )
    result2 = await indicator.extend_realtime("AAPL", row2)
    assert result2[indicator.column_name()] == 10

    row3 = Candle(
        {CandleCol.CLOSE: 102.0},
        timestamp=pd.Timestamp(2023, 1, 12, 9, 30),
    )
    result3 = await indicator.extend_realtime("AAPL", row3)
    assert result3[indicator.column_name()] == 11


@pytest.mark.asyncio
async def test_save_and_load_from_cache(fundamentals: FundamentalDataStoreTest):
    fundamentals.set("AAPL", create_fundamental_data("2023-01-01"))

    indicator = DaysSinceIPOIndicator()
    row = Candle(
        {CandleCol.CLOSE: 100.0},
        timestamp=pd.Timestamp(2023, 1, 11, 10, 0),
    )
    await indicator.extend_realtime("AAPL", row)
    await indicator.save_to_cache()

    new_indicator = DaysSinceIPOIndicator()
    await new_indicator.load_from_cache()

    row2 = Candle(
        {CandleCol.CLOSE: 101.0},
        timestamp=pd.Timestamp(2023, 1, 11, 11, 0),
    )
    result = await new_indicator.extend_realtime("AAPL", row2)
    assert result[new_indicator.column_name()] == 10
