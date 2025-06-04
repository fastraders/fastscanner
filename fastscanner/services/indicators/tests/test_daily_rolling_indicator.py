from datetime import date, datetime
from typing import Any

import numpy as np
import pandas as pd
import pytest

from fastscanner.services.indicators.lib.candle import DailyRollingIndicator
from fastscanner.services.indicators.ports import CandleCol
from fastscanner.services.indicators.tests.fixtures import CandleStoreTest, candles


def test_daily_rolling_type():
    assert DailyRollingIndicator.type() == "daily_rolling"


def test_daily_rolling_column_name():
    indicator = DailyRollingIndicator(n_days=10, operation="min", candle_col="close")
    assert indicator.column_name() == "min_close_10d"

    indicator = DailyRollingIndicator(n_days=20, operation="max", candle_col="high")
    assert indicator.column_name() == "max_high_20d"

    indicator = DailyRollingIndicator(n_days=5, operation="sum", candle_col="volume")
    assert indicator.column_name() == "sum_volume_5d"


@pytest.mark.asyncio
async def test_daily_rolling_extend_min(candles: "CandleStoreTest"):
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 10))
    daily_closes = [150, 152, 155, 153, 151, 154, 156, 158, 157, 155]
    daily_data = pd.DataFrame({CandleCol.CLOSE: daily_closes}, index=daily_dates)

    candles.set_data("AAPL", daily_data)

    dates = [
        datetime(2023, 1, 11, 9, 30),
        datetime(2023, 1, 11, 10, 0),
        datetime(2023, 1, 11, 10, 30),
    ]
    closes = [135, 140, 145]
    df = pd.DataFrame({CandleCol.CLOSE: closes}, index=pd.DatetimeIndex(dates))

    indicator = DailyRollingIndicator(n_days=5, operation="min", candle_col="close")
    result_df = await indicator.extend("AAPL", df)

    expected_values = [154, 154, 154]

    for actual, expected in zip(
        result_df[indicator.column_name()].to_list(), expected_values
    ):
        assert actual == expected


@pytest.mark.asyncio
async def test_daily_rolling_extend_max(candles: "CandleStoreTest"):
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 10))
    daily_highs = [150, 152, 155, 153, 151, 154, 156, 158, 157, 155]
    daily_data = pd.DataFrame({CandleCol.HIGH: daily_highs}, index=daily_dates)

    candles.set_data("AAPL", daily_data)

    dates = [
        datetime(2023, 1, 11, 9, 30),
        datetime(2023, 1, 11, 10, 0),
        datetime(2023, 1, 11, 10, 30),
    ]
    highs = [135, 140, 145]
    df = pd.DataFrame({CandleCol.HIGH: highs}, index=pd.DatetimeIndex(dates))

    indicator = DailyRollingIndicator(n_days=5, operation="max", candle_col="high")
    result_df = await indicator.extend("AAPL", df)

    expected_values = [158, 158, 158]

    for actual, expected in zip(
        result_df[indicator.column_name()].to_list(), expected_values
    ):
        assert actual == expected


@pytest.mark.asyncio
async def test_daily_rolling_extend_sum(candles: "CandleStoreTest"):
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 10))
    daily_volumes = [1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000]
    daily_data = pd.DataFrame({CandleCol.VOLUME: daily_volumes}, index=daily_dates)

    candles.set_data("AAPL", daily_data)

    dates = [
        datetime(2023, 1, 11, 9, 30),
        datetime(2023, 1, 11, 10, 0),
        datetime(2023, 1, 11, 10, 30),
    ]
    volumes = [500, 600, 700]
    df = pd.DataFrame({CandleCol.VOLUME: volumes}, index=pd.DatetimeIndex(dates))

    indicator = DailyRollingIndicator(n_days=5, operation="sum", candle_col="volume")
    result_df = await indicator.extend("AAPL", df)

    expected_values = [40000, 40000, 40000]

    for actual, expected in zip(
        result_df[indicator.column_name()].to_list(), expected_values
    ):
        assert actual == expected


@pytest.mark.asyncio
async def test_daily_rolling_extend_empty_data(candles: "CandleStoreTest"):
    candles.set_data("AAPL", pd.DataFrame(index=pd.DatetimeIndex([])))

    dates = [datetime(2023, 1, 10, 9, 30)]
    closes = [135]
    df = pd.DataFrame({CandleCol.CLOSE: closes}, index=pd.DatetimeIndex(dates))

    indicator = DailyRollingIndicator(n_days=5, operation="min", candle_col="close")
    result_df = await indicator.extend("AAPL", df)

    assert pd.isna(result_df[indicator.column_name()].iloc[0])


@pytest.mark.asyncio
async def test_daily_rolling_extend_realtime_first_candle(candles: "CandleStoreTest"):
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 10))
    daily_closes = [150, 152, 155, 153, 151, 154, 156, 158, 157, 155]
    daily_data = pd.DataFrame({CandleCol.CLOSE: daily_closes}, index=daily_dates)

    candles.set_data("AAPL", daily_data)

    indicator = DailyRollingIndicator(n_days=5, operation="min", candle_col="close")

    row_time = datetime(2023, 1, 11, 9, 30)
    row = pd.Series({CandleCol.CLOSE: 140}, name=row_time)

    result_row = await indicator.extend_realtime("AAPL", row)

    assert result_row[indicator.column_name()] == 154
    assert indicator._last_date["AAPL"] == row_time.date()
    assert len(indicator._rolling_values["AAPL"]) == 5


@pytest.mark.asyncio
async def test_daily_rolling_extend_realtime_same_day(candles: "CandleStoreTest"):
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 10))
    daily_highs = [150, 152, 155, 153, 151, 154, 156, 158, 157, 155]
    daily_data = pd.DataFrame({CandleCol.HIGH: daily_highs}, index=daily_dates)

    candles.set_data("AAPL", daily_data)

    indicator = DailyRollingIndicator(n_days=5, operation="max", candle_col="high")

    first_time = datetime(2023, 1, 11, 9, 30)
    first_row = pd.Series({CandleCol.HIGH: 140}, name=first_time)
    await indicator.extend_realtime("AAPL", first_row)

    second_time = datetime(2023, 1, 11, 10, 0)
    second_row = pd.Series({CandleCol.HIGH: 145}, name=second_time)
    result_row = await indicator.extend_realtime("AAPL", second_row)

    assert result_row[indicator.column_name()] == 158
    assert indicator._last_date["AAPL"] == first_time.date()


@pytest.mark.asyncio
async def test_daily_rolling_extend_realtime_new_day(candles: "CandleStoreTest"):
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 11))
    daily_volumes = [1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 11000]
    daily_data = pd.DataFrame({CandleCol.VOLUME: daily_volumes}, index=daily_dates)

    candles.set_data("AAPL", daily_data)

    indicator = DailyRollingIndicator(n_days=5, operation="sum", candle_col="volume")

    day1_time = datetime(2023, 1, 10, 9, 30)
    day1_row = pd.Series({CandleCol.VOLUME: 500}, name=day1_time)
    await indicator.extend_realtime("AAPL", day1_row)

    values_day1 = indicator._rolling_values["AAPL"].copy()

    day2_time = datetime(2023, 1, 12, 9, 30)
    day2_row = pd.Series({CandleCol.VOLUME: 600}, name=day2_time)
    result_row = await indicator.extend_realtime("AAPL", day2_row)

    assert indicator._last_date["AAPL"] == day2_time.date()
    assert indicator._rolling_values["AAPL"] != values_day1


@pytest.mark.asyncio
async def test_daily_rolling_extend_realtime_multiple_symbols(
    candles: "CandleStoreTest",
):
    aapl_daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 10))
    aapl_daily_closes = [150, 152, 155, 153, 151, 154, 156, 158, 157, 155]
    aapl_daily_data = pd.DataFrame(
        {CandleCol.CLOSE: aapl_daily_closes}, index=aapl_daily_dates
    )

    msft_daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 10))
    msft_daily_closes = [250, 252, 255, 253, 251, 254, 256, 258, 257, 255]
    msft_daily_data = pd.DataFrame(
        {CandleCol.CLOSE: msft_daily_closes}, index=msft_daily_dates
    )

    candles.set_data("AAPL", aapl_daily_data)
    candles.set_data("MSFT", msft_daily_data)

    indicator = DailyRollingIndicator(n_days=5, operation="min", candle_col="close")

    aapl_time = datetime(2023, 1, 11, 9, 30)
    aapl_row = pd.Series({CandleCol.CLOSE: 140}, name=aapl_time)
    result_row = await indicator.extend_realtime("AAPL", aapl_row)

    assert result_row[indicator.column_name()] == 154

    msft_time = datetime(2023, 1, 11, 9, 30)
    msft_row = pd.Series({CandleCol.CLOSE: 240}, name=msft_time)
    result_row = await indicator.extend_realtime("MSFT", msft_row)

    assert result_row[indicator.column_name()] == 254

    assert "AAPL" in indicator._rolling_values
    assert "MSFT" in indicator._rolling_values
    assert indicator._last_date["AAPL"] == aapl_time.date()
    assert indicator._last_date["MSFT"] == msft_time.date()


@pytest.mark.asyncio
async def test_daily_rolling_edge_cases(candles: "CandleStoreTest"):
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 10))
    daily_closes = [150, 150, 150, 150, 150, 150, 150, 150, 150, 150]
    daily_data = pd.DataFrame({CandleCol.CLOSE: daily_closes}, index=daily_dates)

    candles.set_data("AAPL", daily_data)

    indicator = DailyRollingIndicator(n_days=5, operation="min", candle_col="close")

    row_time = datetime(2023, 1, 11, 9, 30)
    row = pd.Series({CandleCol.CLOSE: 140}, name=row_time)
    result_row = await indicator.extend_realtime("AAPL", row)

    assert result_row[indicator.column_name()] == 150

    indicator = DailyRollingIndicator(n_days=1, operation="max", candle_col="close")
    result_row = await indicator.extend_realtime("AAPL", row)

    assert result_row[indicator.column_name()] == 150
