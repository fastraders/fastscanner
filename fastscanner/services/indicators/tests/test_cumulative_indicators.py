from datetime import datetime

import numpy as np
import pandas as pd
import pytest

from fastscanner.services.indicators.lib.candle import (
    CumulativeDailyVolumeIndicator,
    CumulativeOperation,
    PremarketCumulativeIndicator,
)
from fastscanner.services.indicators.ports import CandleCol


def test_cumulative_daily_volume_type():
    assert CumulativeDailyVolumeIndicator.type() == "cumulative_daily_volume"


def test_cumulative_daily_volume_column_name():
    indicator = CumulativeDailyVolumeIndicator()
    assert indicator.column_name() == "cumulative_daily_volume"


@pytest.mark.asyncio
async def test_cumulative_daily_volume_extend_single_day():
    dates = [
        datetime(2023, 1, 1, 9, 30),
        datetime(2023, 1, 1, 10, 0),
        datetime(2023, 1, 1, 10, 30),
        datetime(2023, 1, 1, 11, 0),
    ]
    volumes = [100, 150, 200, 250]

    df = pd.DataFrame({CandleCol.VOLUME: volumes}, index=pd.DatetimeIndex(dates))

    indicator = CumulativeDailyVolumeIndicator()
    result_df = await indicator.extend("AAPL", df)
    expected_volumes = [100, 250, 450, 700]

    assert result_df[indicator.column_name()].to_list() == expected_volumes


@pytest.mark.asyncio
async def test_cumulative_daily_volume_extend_multiple_days():
    dates = [
        datetime(2023, 1, 1, 9, 30),
        datetime(2023, 1, 1, 10, 0),
        datetime(2023, 1, 2, 9, 30),
        datetime(2023, 1, 2, 10, 0),
        datetime(2023, 1, 3, 9, 30),
        datetime(2023, 1, 3, 10, 0),
    ]
    volumes = [100, 150, 200, 250, 300, 350]

    df = pd.DataFrame({CandleCol.VOLUME: volumes}, index=pd.DatetimeIndex(dates))

    indicator = CumulativeDailyVolumeIndicator()
    result_df = await indicator.extend("AAPL", df)

    expected_volumes = [100, 250, 200, 450, 300, 650]

    assert result_df[indicator.column_name()].to_list() == expected_volumes


@pytest.mark.asyncio
async def test_cumulative_daily_volume_extend_realtime_first_candle():
    indicator = CumulativeDailyVolumeIndicator()

    row_time = datetime(2023, 1, 1, 9, 30)
    row = {"datetime": row_time, CandleCol.VOLUME: 100}

    result_row = await indicator.extend_realtime("AAPL", row)

    assert result_row[indicator.column_name()] == 100

    assert indicator._last_date["AAPL"] == row_time.date()
    assert indicator._last_volume["AAPL"] == 100


@pytest.mark.asyncio
async def test_cumulative_daily_volume_extend_realtime_same_day():
    indicator = CumulativeDailyVolumeIndicator()

    first_time = datetime(2023, 1, 1, 9, 30)
    first_row = {"datetime": first_time, CandleCol.VOLUME: 100}
    await indicator.extend_realtime("AAPL", first_row)

    second_time = datetime(2023, 1, 1, 10, 0)
    second_row = {"datetime": second_time, CandleCol.VOLUME: 150}
    result_row = await indicator.extend_realtime("AAPL", second_row)

    assert result_row[indicator.column_name()] == 250

    third_time = datetime(2023, 1, 1, 10, 30)
    third_row = {"datetime": third_time, CandleCol.VOLUME: 200}
    result_row = await indicator.extend_realtime("AAPL", third_row)
    assert result_row[indicator.column_name()] == 450


@pytest.mark.asyncio
async def test_cumulative_daily_volume_extend_realtime_new_day():
    indicator = CumulativeDailyVolumeIndicator()

    first_time = datetime(2023, 1, 1, 9, 30)
    first_row = {"datetime": first_time, CandleCol.VOLUME: 100}
    await indicator.extend_realtime("AAPL", first_row)

    second_time = datetime(2023, 1, 2, 9, 30)
    second_row = {"datetime": second_time, CandleCol.VOLUME: 200}
    result_row = await indicator.extend_realtime("AAPL", second_row)

    # Volume should reset for new day
    assert result_row[indicator.column_name()] == 200


@pytest.mark.asyncio
async def test_cumulative_daily_volume_extend_realtime_multiple_symbols():
    indicator = CumulativeDailyVolumeIndicator()

    aapl_time = datetime(2023, 1, 1, 9, 30)
    aapl_row = {"datetime": aapl_time, CandleCol.VOLUME: 100}
    result_row = await indicator.extend_realtime("AAPL", aapl_row)
    assert result_row[indicator.column_name()] == 100

    msft_time = datetime(2023, 1, 1, 9, 30)
    msft_row = {"datetime": msft_time, CandleCol.VOLUME: 200}
    result_row = await indicator.extend_realtime("MSFT", msft_row)

    assert result_row[indicator.column_name()] == 200

    aapl_time2 = datetime(2023, 1, 1, 10, 0)
    aapl_row2 = {"datetime": aapl_time2, CandleCol.VOLUME: 150}
    result_row = await indicator.extend_realtime("AAPL", aapl_row2)

    assert result_row[indicator.column_name()] == 250


def test_premarket_cumulative_type():
    assert PremarketCumulativeIndicator.type() == "premarket_cumulative"


def test_premarket_cumulative_column_name():
    # Test with different operations
    indicator_min = PremarketCumulativeIndicator(
        CandleCol.CLOSE, CumulativeOperation.MIN
    )
    assert indicator_min.column_name() == "premarket_lowest_close"

    indicator_max = PremarketCumulativeIndicator(
        CandleCol.HIGH, CumulativeOperation.MAX
    )
    assert indicator_max.column_name() == "premarket_highest_high"

    indicator_sum = PremarketCumulativeIndicator(
        CandleCol.VOLUME, CumulativeOperation.SUM
    )
    assert indicator_sum.column_name() == "premarket_total_volume"


@pytest.mark.asyncio
async def test_premarket_cumulative_extend_single_day():
    # Create test data with premarket and market hours
    dates = [
        datetime(2023, 1, 1, 8, 0),  # Premarket
        datetime(2023, 1, 1, 8, 30),  # Premarket
        datetime(2023, 1, 1, 9, 0),  # Premarket
        datetime(2023, 1, 1, 9, 30),  # Market hours
        datetime(2023, 1, 1, 10, 0),  # Market hours
        datetime(2023, 1, 1, 10, 30),  # Market hours
    ]

    # Test with MAX operation on HIGH column
    highs = [150, 160, 155, 170, 180, 190]
    df = pd.DataFrame({CandleCol.HIGH: highs}, index=pd.DatetimeIndex(dates))

    indicator = PremarketCumulativeIndicator(CandleCol.HIGH, CumulativeOperation.MAX)
    result_df = await indicator.extend("AAPL", df)

    # Maintain highest premarket value during market hours
    expected_values = [150, 160, 160, 160, 160, 160]
    assert result_df[indicator.column_name()].to_list() == expected_values

    # Test with MIN operation on LOW column
    lows = [145, 140, 142, 138, 135, 130]
    df = pd.DataFrame({CandleCol.LOW: lows}, index=pd.DatetimeIndex(dates))

    indicator = PremarketCumulativeIndicator(CandleCol.LOW, CumulativeOperation.MIN)
    result_df = await indicator.extend("AAPL", df)

    # Maintain lowest premarket value during market hours
    expected_values = [145, 140, 140, 140, 140, 140]
    assert result_df[indicator.column_name()].to_list() == expected_values

    # Test with SUM operation on VOLUME column
    volumes = [100, 150, 200, 250, 300, 350]
    df = pd.DataFrame({CandleCol.VOLUME: volumes}, index=pd.DatetimeIndex(dates))

    indicator = PremarketCumulativeIndicator(CandleCol.VOLUME, CumulativeOperation.SUM)
    result_df = await indicator.extend("AAPL", df)

    # Maintain accumulated premarket volume during market hours
    expected_values = [100, 250, 450, 450, 450, 450]
    assert result_df[indicator.column_name()].to_list() == expected_values


@pytest.mark.asyncio
async def test_premarket_cumulative_extend_multiple_days():
    # Create test data with multiple days
    dates = [
        datetime(2023, 1, 1, 8, 0),  # Day 1 Premarket
        datetime(2023, 1, 1, 9, 0),  # Day 1 Premarket
        datetime(2023, 1, 1, 9, 30),  # Day 1 Market
        datetime(2023, 1, 1, 10, 0),  # Day 1 Market
        datetime(2023, 1, 2, 8, 0),  # Day 2 Premarket
        datetime(2023, 1, 2, 9, 0),  # Day 2 Premarket
        datetime(2023, 1, 2, 9, 30),  # Day 2 Market
        datetime(2023, 1, 2, 10, 0),  # Day 2 Market
    ]

    highs = [150, 160, 170, 180, 155, 165, 175, 185]
    df = pd.DataFrame({CandleCol.HIGH: highs}, index=pd.DatetimeIndex(dates))

    indicator = PremarketCumulativeIndicator(CandleCol.HIGH, CumulativeOperation.MAX)
    result_df = await indicator.extend("AAPL", df)

    # Each day tracks its own premarket high
    expected_values = [150, 160, 160, 160, 155, 165, 165, 165]
    assert result_df[indicator.column_name()].to_list() == expected_values

    volumes = [100, 150, 200, 250, 120, 180, 220, 280]
    df = pd.DataFrame({CandleCol.VOLUME: volumes}, index=pd.DatetimeIndex(dates))

    indicator = PremarketCumulativeIndicator(CandleCol.VOLUME, CumulativeOperation.SUM)
    result_df = await indicator.extend("AAPL", df)

    # Each day accumulates its own premarket volume
    expected_values = [100, 250, 250, 250, 120, 300, 300, 300]
    assert result_df[indicator.column_name()].to_list() == expected_values


@pytest.mark.asyncio
async def test_premarket_cumulative_extend_realtime_first_candle():
    indicator = PremarketCumulativeIndicator(CandleCol.HIGH, CumulativeOperation.MAX)

    row_time = datetime(2023, 1, 1, 8, 0)
    row = {"datetime": row_time, CandleCol.HIGH: 150}

    result_row = await indicator.extend_realtime("AAPL", row)

    assert result_row[indicator.column_name()] == 150
    assert indicator._last_date["AAPL"] == row_time.date()
    assert indicator._last_value["AAPL"] == 150


@pytest.mark.asyncio
async def test_premarket_cumulative_extend_realtime_same_day_premarket():
    # Test with MAX operation
    indicator = PremarketCumulativeIndicator(CandleCol.HIGH, CumulativeOperation.MAX)

    first_time = datetime(2023, 1, 1, 8, 0)
    first_row = {"datetime": first_time, CandleCol.HIGH: 150}
    await indicator.extend_realtime("AAPL", first_row)

    second_time = datetime(2023, 1, 1, 8, 30)
    second_row = {"datetime": second_time, CandleCol.HIGH: 160}
    result_row = await indicator.extend_realtime("AAPL", second_row)

    assert result_row[indicator.column_name()] == 160

    third_time = datetime(2023, 1, 1, 9, 0)
    third_row = {"datetime": third_time, CandleCol.HIGH: 155}
    result_row = await indicator.extend_realtime("AAPL", third_row)

    # Maintains highest value
    assert result_row[indicator.column_name()] == 160


@pytest.mark.asyncio
async def test_premarket_cumulative_extend_realtime_market_hours():
    indicator = PremarketCumulativeIndicator(CandleCol.HIGH, CumulativeOperation.MAX)

    premarket_time = datetime(2023, 1, 1, 8, 0)
    premarket_row = {"datetime": premarket_time, CandleCol.HIGH: 150}
    await indicator.extend_realtime("AAPL", premarket_row)

    market_time = datetime(2023, 1, 1, 9, 30)
    market_row = {"datetime": market_time, CandleCol.HIGH: 170}
    result_row = await indicator.extend_realtime("AAPL", market_row)

    # Maintains premarket value during market hours
    assert result_row[indicator.column_name()] == 150

    later_time = datetime(2023, 1, 1, 10, 0)
    later_row = {"datetime": later_time, CandleCol.HIGH: 180}
    result_row = await indicator.extend_realtime("AAPL", later_row)

    # Still maintains premarket value
    assert result_row[indicator.column_name()] == 150


@pytest.mark.asyncio
async def test_premarket_cumulative_extend_realtime_new_day():
    indicator = PremarketCumulativeIndicator(CandleCol.HIGH, CumulativeOperation.MAX)

    day1_time = datetime(2023, 1, 1, 8, 0)
    day1_row = {"datetime": day1_time, CandleCol.HIGH: 150}
    await indicator.extend_realtime("AAPL", day1_row)

    day2_time = datetime(2023, 1, 2, 8, 0)
    day2_row = {"datetime": day2_time, CandleCol.HIGH: 160}
    result_row = await indicator.extend_realtime("AAPL", day2_row)

    # Resets for new day
    assert result_row[indicator.column_name()] == 160


@pytest.mark.asyncio
async def test_premarket_cumulative_extend_realtime_multiple_symbols():
    indicator = PremarketCumulativeIndicator(CandleCol.HIGH, CumulativeOperation.MAX)

    aapl_time = datetime(2023, 1, 1, 8, 0)
    aapl_row = {"datetime": aapl_time, CandleCol.HIGH: 150}
    result_row = await indicator.extend_realtime("AAPL", aapl_row)
    assert result_row[indicator.column_name()] == 150

    msft_time = datetime(2023, 1, 1, 8, 0)
    msft_row = {"datetime": msft_time, CandleCol.HIGH: 250}
    result_row = await indicator.extend_realtime("MSFT", msft_row)
    assert result_row[indicator.column_name()] == 250

    aapl_time2 = datetime(2023, 1, 1, 8, 30)
    aapl_row2 = {"datetime": aapl_time2, CandleCol.HIGH: 160}
    result_row = await indicator.extend_realtime("AAPL", aapl_row2)
    assert result_row[indicator.column_name()] == 160

    # Each symbol maintains separate values
    assert indicator._last_value["MSFT"] == 250


@pytest.mark.asyncio
async def test_premarket_cumulative_min_operation():
    indicator = PremarketCumulativeIndicator(CandleCol.LOW, CumulativeOperation.MIN)

    # First premarket candle
    first_time = datetime(2023, 1, 1, 8, 0)
    first_row = {"datetime": first_time, CandleCol.LOW: 150}
    await indicator.extend_realtime("AAPL", first_row)

    second_time = datetime(2023, 1, 1, 8, 30)
    second_row = {"datetime": second_time, CandleCol.LOW: 140}
    result_row = await indicator.extend_realtime("AAPL", second_row)

    assert result_row[indicator.column_name()] == 140

    third_time = datetime(2023, 1, 1, 9, 0)
    third_row = {"datetime": third_time, CandleCol.LOW: 145}
    result_row = await indicator.extend_realtime("AAPL", third_row)

    # Maintains lowest value
    assert result_row[indicator.column_name()] == 140


@pytest.mark.asyncio
async def test_premarket_cumulative_sum_operation():
    indicator = PremarketCumulativeIndicator(CandleCol.VOLUME, CumulativeOperation.SUM)

    first_time = datetime(2023, 1, 1, 8, 0)
    first_row = {"datetime": first_time, CandleCol.VOLUME: 100}
    await indicator.extend_realtime("AAPL", first_row)

    second_time = datetime(2023, 1, 1, 8, 30)
    second_row = {"datetime": second_time, CandleCol.VOLUME: 150}
    result_row = await indicator.extend_realtime("AAPL", second_row)

    # Sums values correctly
    assert result_row[indicator.column_name()] == 250

    third_time = datetime(2023, 1, 1, 9, 0)
    third_row = {"datetime": third_time, CandleCol.VOLUME: 200}
    result_row = await indicator.extend_realtime("AAPL", third_row)

    # Continues summing correctly
    assert result_row[indicator.column_name()] == 450
