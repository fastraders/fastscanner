from datetime import date, datetime
from typing import Dict

import numpy as np
import pandas as pd
import pytest

from fastscanner.services.indicators.lib.fundamental import MarketCapIndicator
from fastscanner.services.indicators.ports import FundamentalData, FundamentalDataStore
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


def create_fundamental_data_with_market_caps(market_caps: pd.Series) -> FundamentalData:
    return FundamentalData(
        type="Common Stock",
        exchange="NYSE",
        country="USA",
        city="New York",
        gic_industry="Technology",
        gic_sector="Information Technology",
        historical_market_cap=market_caps,
        earnings_dates=pd.DatetimeIndex([]),  # type: ignore
        insiders_ownership_perc=10.0,
        institutional_ownership_perc=70.0,
        shares_float=1000000.0,
        beta=1.2,
    )


@pytest.mark.asyncio
async def test_market_cap_indicator_with_historical_data(fundamentals):
    dates = pd.to_datetime(
        [
            "2023-01-01",
            "2023-01-05",
            "2023-01-10",
        ]
    ).date
    market_caps = pd.Series([1000.0, 1100.0, 1200.0], index=dates)

    fundamentals.set("AAPL", create_fundamental_data_with_market_caps(market_caps))

    df_dates = [
        pd.Timestamp("2023-01-02"),
        pd.Timestamp("2023-01-05"),
        pd.Timestamp("2023-01-07"),
        pd.Timestamp("2023-01-12"),
    ]
    df = pd.DataFrame({"some_col": [1, 2, 3, 4]}, index=pd.Index(df_dates))

    indicator = MarketCapIndicator()
    extended_df = await indicator.extend("AAPL", df)

    expected_market_caps = [
        1000.0,  # closest <= 2023-01-02 is 2023-01-01
        1100.0,  # exact date 2023-01-05
        1100.0,  # closest <= 2023-01-07 is 2023-01-05
        1200.0,  # closest <= 2023-01-12 is 2023-01-10
    ]

    result = extended_df[indicator.column_name()].tolist()
    assert result == expected_market_caps


@pytest.mark.asyncio
async def test_market_cap_indicator_with_no_market_caps(fundamentals):
    empty_market_caps = pd.Series(dtype=float)
    fundamentals.set(
        "MSFT", create_fundamental_data_with_market_caps(empty_market_caps)
    )

    df_dates = [
        datetime(2023, 2, 1),
        datetime(2023, 2, 2),
    ]
    df = pd.DataFrame({"some_col": [1, 2]}, index=pd.DatetimeIndex(df_dates))

    indicator = MarketCapIndicator()
    extended_df = await indicator.extend("MSFT", df)

    result = extended_df[indicator.column_name()]
    assert result.isna().all()


@pytest.mark.asyncio
async def test_market_cap_indicator_extend_realtime(fundamentals):
    dates = pd.to_datetime(["2023-01-01"]).date
    market_caps = pd.Series([1000.0], index=dates)

    fundamentals.set("GOOG", create_fundamental_data_with_market_caps(market_caps))

    df_dates = [datetime(2023, 1, 1, 10, 0)]
    df = pd.DataFrame({"some_col": [1]}, index=pd.DatetimeIndex(df_dates))

    indicator = MarketCapIndicator()

    new_row = df.iloc[0]
    new_row.name = df.index[0]

    extended_row = await indicator.extend_realtime("GOOG", new_row)

    assert indicator._last_date["GOOG"] == new_row.name.date()
    assert indicator._last_market_cap["GOOG"] == 1000.0
    assert extended_row[indicator.column_name()] == 1000.0
