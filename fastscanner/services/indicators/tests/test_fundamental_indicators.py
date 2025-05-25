from datetime import date, datetime
from typing import Dict

import pandas as pd
import pytest

from fastscanner.services.indicators.lib.fundamental import (
    DaysFromEarningsIndicator,
    DaysToEarningsIndicator,
)
from fastscanner.services.indicators.ports import (
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


@pytest.fixture
def fundamentals():
    store = FundamentalDataStoreTest()
    ApplicationRegistry.init(candles=None, fundamentals=store, holidays=None)  # type: ignore
    yield store
    ApplicationRegistry.reset()


def create_fundamental_data(earnings_dates: list[date]) -> FundamentalData:
    return FundamentalData(
        type="Common Stock",
        exchange="NYSE",
        country="USA",
        city="New York",
        gic_industry="Technology",
        gic_sector="Information Technology",
        historical_market_cap=pd.Series(dtype=float),
        earnings_dates=pd.DatetimeIndex(earnings_dates),
        insiders_ownership_perc=10.0,
        institutional_ownership_perc=70.0,
        shares_float=1000000.0,
        beta=1.2,
    )


def test_days_to_earnings_type():
    assert DaysToEarningsIndicator.type() == "days_to_earnings"


def test_days_to_earnings_column_name():
    indicator = DaysToEarningsIndicator()
    assert indicator.column_name() == "days_to_earnings"


@pytest.mark.asyncio
async def test_days_to_earnings_extend_with_future_earnings(
    fundamentals: FundamentalDataStoreTest,
):
    earnings_dates = [
        date(2023, 1, 15),
        date(2023, 2, 15),
    ]

    fundamentals.set("AAPL", create_fundamental_data(earnings_dates))

    dates = [
        datetime(2023, 1, 5, 9, 30),
        datetime(2023, 1, 5, 10, 0),
        datetime(2023, 1, 20, 9, 30),
        datetime(2023, 1, 20, 10, 0),
        datetime(2023, 2, 10, 9, 30),
    ]

    df = pd.DataFrame(
        {CandleCol.CLOSE: [100, 101, 102, 103, 104]}, index=pd.DatetimeIndex(dates)
    )

    indicator = DaysToEarningsIndicator()
    result_df = await indicator.extend("AAPL", df)

    expected_days = [10, 10, 26, 26, 5]
    assert result_df[indicator.column_name()].tolist() == expected_days


@pytest.mark.asyncio
async def test_days_to_earnings_extend_with_no_future_earnings(
    fundamentals: FundamentalDataStoreTest,
):
    earnings_dates = [date(2022, 12, 15)]

    fundamentals.set("AAPL", create_fundamental_data(earnings_dates))

    dates = [
        datetime(2023, 1, 5, 9, 30),
        datetime(2023, 1, 5, 10, 0),
    ]

    df = pd.DataFrame({CandleCol.CLOSE: [100, 101]}, index=pd.DatetimeIndex(dates))

    indicator = DaysToEarningsIndicator()
    result_df = await indicator.extend("AAPL", df)

    assert pd.isna(result_df[indicator.column_name()]).all()


def test_days_from_earnings_type():
    assert DaysFromEarningsIndicator.type() == "days_from_earnings"


def test_days_from_earnings_column_name():
    indicator = DaysFromEarningsIndicator()
    assert indicator.column_name() == "days_from_earnings"


@pytest.mark.asyncio
async def test_days_from_earnings_extend_with_past_earnings(
    fundamentals: FundamentalDataStoreTest,
):
    earnings_dates = [
        date(2022, 12, 15),
        date(2023, 1, 15),
        date(2023, 2, 15),
    ]

    fundamentals.set("AAPL", create_fundamental_data(earnings_dates))

    dates = [
        datetime(2023, 1, 5, 9, 30),
        datetime(2023, 1, 5, 10, 0),
        datetime(2023, 1, 20, 9, 30),
        datetime(2023, 1, 20, 10, 0),
        datetime(2023, 2, 20, 9, 30),
    ]

    df = pd.DataFrame(
        {CandleCol.CLOSE: [100, 101, 102, 103, 104]}, index=pd.DatetimeIndex(dates)
    )

    indicator = DaysFromEarningsIndicator()
    result_df = await indicator.extend("AAPL", df)

    expected_days = [21, 21, 5, 5, 5]
    assert result_df[indicator.column_name()].tolist() == expected_days


@pytest.mark.asyncio
async def test_days_from_earnings_extend_with_no_past_earnings(
    fundamentals: FundamentalDataStoreTest,
):
    earnings_dates = [date(2023, 2, 15)]

    fundamentals.set("AAPL", create_fundamental_data(earnings_dates))

    dates = [
        datetime(2023, 1, 5, 9, 30),
        datetime(2023, 1, 5, 10, 0),
    ]

    df = pd.DataFrame({CandleCol.CLOSE: [100, 101]}, index=pd.DatetimeIndex(dates))

    indicator = DaysFromEarningsIndicator()
    result_df = await indicator.extend("AAPL", df)

    assert pd.isna(result_df[indicator.column_name()]).all()


@pytest.mark.asyncio
async def test_days_from_earnings_extend_with_multiple_symbols(
    fundamentals: FundamentalDataStoreTest,
):
    aapl_earnings_dates = [date(2022, 12, 15)]
    msft_earnings_dates = [date(2022, 12, 20)]

    fundamentals.set("AAPL", create_fundamental_data(aapl_earnings_dates))
    fundamentals.set("MSFT", create_fundamental_data(msft_earnings_dates))

    aapl_dates = [datetime(2023, 1, 5, 9, 30)]
    aapl_df = pd.DataFrame({CandleCol.CLOSE: [100]}, index=pd.DatetimeIndex(aapl_dates))

    msft_dates = [datetime(2023, 1, 5, 9, 30)]
    msft_df = pd.DataFrame({CandleCol.CLOSE: [200]}, index=pd.DatetimeIndex(msft_dates))

    indicator = DaysFromEarningsIndicator()
    aapl_result = await indicator.extend("AAPL", aapl_df)
    msft_result = await indicator.extend("MSFT", msft_df)

    assert aapl_result[indicator.column_name()].iloc[0] == 21
    assert msft_result[indicator.column_name()].iloc[0] == 16


@pytest.mark.asyncio
async def test_days_to_earnings_array_reduction(fundamentals: FundamentalDataStoreTest):
    earnings_dates = [
        date(2023, 1, 15),
        date(2023, 2, 15),
        date(2023, 3, 15),
    ]

    fundamentals.set("AAPL", create_fundamental_data(earnings_dates))

    dates = [
        datetime(2023, 1, 5, 9, 30),
        datetime(2023, 1, 20, 9, 30),
        datetime(2023, 2, 20, 9, 30),
    ]

    df = pd.DataFrame({CandleCol.CLOSE: [100, 101, 102]}, index=pd.DatetimeIndex(dates))

    indicator = DaysToEarningsIndicator()
    result_df = await indicator.extend("AAPL", df)

    expected_days = [10, 26, 23]
    assert result_df[indicator.column_name()].tolist() == expected_days


@pytest.mark.asyncio
async def test_days_from_earnings_array_reduction(
    fundamentals: FundamentalDataStoreTest,
):
    earnings_dates = [
        date(2023, 1, 15),
        date(2023, 2, 15),
        date(2023, 3, 15),
    ]

    fundamentals.set("AAPL", create_fundamental_data(earnings_dates))

    dates = [
        datetime(2023, 3, 20, 9, 30),
        datetime(2023, 2, 20, 9, 30),
        datetime(2023, 1, 20, 9, 30),
    ]

    df = pd.DataFrame({CandleCol.CLOSE: [100, 101, 102]}, index=pd.DatetimeIndex(dates))

    indicator = DaysFromEarningsIndicator()
    result_df = await indicator.extend("AAPL", df)

    expected_days = [5, 5, 5]
    assert result_df[indicator.column_name()].tolist() == expected_days
