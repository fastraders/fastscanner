from datetime import date, datetime, time

import numpy as np
import pandas as pd
import pytest

from fastscanner.services.indicators.lib.daily import (
    DailyATRGapIndicator,
    DailyATRIndicator,
    DailyGapIndicator,
    PrevDayIndicator,
)
from fastscanner.services.indicators.ports import CandleCol
from fastscanner.services.indicators.registry import ApplicationRegistry
from fastscanner.services.indicators.tests.fixtures import CandleStoreTest, candles


def test_daily_atr_gap_indicator_type():
    indicator = DailyATRGapIndicator(period=3)
    assert indicator.type() == "daily_atr_gap"


def test_daily_atr_gap_indicator_column_name():
    indicator = DailyATRGapIndicator(period=3)
    assert indicator.column_name() == "daily_atr_gap_3"

    indicator = DailyATRGapIndicator(period=5)
    assert indicator.column_name() == "daily_atr_gap_5"


@pytest.mark.asyncio
async def test_daily_atr_gap_indicator_extend(candles: "CandleStoreTest"):
    # Set up test data for daily candles - using only 7 days
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 7))
    daily_data = pd.DataFrame(
        {
            CandleCol.OPEN: [100, 102, 105, 103, 101, 104, 106],
            CandleCol.HIGH: [105, 107, 110, 108, 106, 109, 111],
            CandleCol.LOW: [95, 97, 100, 98, 96, 99, 101],
            CandleCol.CLOSE: [102, 105, 103, 101, 104, 106, 108],
        },
        index=daily_dates,
    )

    candles.set_data("AAPL", daily_data)

    # Create test data for intraday candles
    dates = [
        datetime(2023, 1, 8, 9, 30),
        datetime(2023, 1, 8, 10, 0),
        datetime(2023, 1, 8, 10, 30),
    ]
    df = pd.DataFrame(
        {
            CandleCol.OPEN: [110, 111, 112],
            CandleCol.HIGH: [115, 116, 117],
            CandleCol.LOW: [105, 106, 107],
            CandleCol.CLOSE: [113, 114, 115],
        },
        index=pd.DatetimeIndex(dates),
    )

    # Test with period=3
    indicator = DailyATRGapIndicator(period=3)
    result_df = await indicator.extend("AAPL", df.copy())

    # Verify the column exists and has values for all rows
    assert indicator.column_name() in result_df.columns
    assert not result_df[indicator.column_name()].isna().any()

    # All rows should have the same ATR gap value since they're on the same day
    assert result_df[indicator.column_name()].nunique() == 1


@pytest.mark.asyncio
async def test_daily_atr_gap_indicator_calculation(candles: "CandleStoreTest"):
    # Set up test data with known values for manual calculation
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 5))
    daily_data = pd.DataFrame(
        {
            CandleCol.HIGH: [100, 110, 120, 130, 140],
            CandleCol.LOW: [90, 95, 105, 115, 125],
            CandleCol.CLOSE: [95, 105, 115, 125, 135],
        },
        index=daily_dates,
    )

    candles.set_data("AAPL", daily_data)

    # Create test data for intraday candles
    dates = [datetime(2023, 1, 6, 9, 30)]
    df = pd.DataFrame(
        {
            CandleCol.OPEN: [145],  # Open price for Jan 6
            CandleCol.HIGH: [150],
            CandleCol.LOW: [140],
            CandleCol.CLOSE: [148],
        },
        index=pd.DatetimeIndex(dates),
    )

    # Test with period=5
    atr_indicator = DailyATRIndicator(period=5)
    atr_df = await atr_indicator.extend("AAPL", df.copy())
    atr_value = atr_df[atr_indicator.column_name()].iloc[0]

    # Calculate expected gap ratio
    # Gap = (day_open - prev_day_close) / atr
    # day_open = 145, prev_day_close = 135
    # gap = (145 - 135) / atr_value
    expected_gap_ratio = (145 - 135) / atr_value

    # Now test the ATR Gap indicator
    gap_indicator = DailyATRGapIndicator(period=5)
    result_df = await gap_indicator.extend("AAPL", df.copy())

    # Verify the ATR Gap value matches our calculation
    gap_value = result_df[gap_indicator.column_name()].iloc[0]
    assert abs(gap_value - expected_gap_ratio) < 1e-4


@pytest.mark.asyncio
async def test_daily_atr_gap_indicator_extend_realtime(candles: "CandleStoreTest"):
    # Set up test data for daily candles
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 5))
    daily_data = pd.DataFrame(
        {
            CandleCol.HIGH: [100, 110, 120, 130, 140],
            CandleCol.LOW: [90, 95, 105, 115, 125],
            CandleCol.CLOSE: [95, 105, 115, 125, 135],
        },
        index=daily_dates,
    )

    candles.set_data("AAPL", daily_data)

    # Create a new row for realtime data at market open
    new_row = pd.Series(
        {
            CandleCol.OPEN: 145,  # Open price for Jan 6
            CandleCol.HIGH: 150,
            CandleCol.LOW: 140,
            CandleCol.CLOSE: 148,
        },
        name=datetime(2023, 1, 6, 9, 30),  # Market open time
    )

    # Test with period=5
    indicator = DailyATRGapIndicator(period=5)
    result_row = await indicator.extend_realtime("AAPL", new_row.copy())

    # Verify the ATR Gap value exists
    assert indicator.column_name() in result_row.index
    assert not pd.isna(result_row[indicator.column_name()])

    # Calculate expected value for comparison
    atr_indicator = DailyATRIndicator(period=5)
    atr_df = await atr_indicator.extend("AAPL", new_row.to_frame().T.copy())
    atr_value = atr_df[atr_indicator.column_name()].iloc[0]

    # Gap = (day_open - prev_day_close) / atr
    # day_open = 145, prev_day_close = 135
    expected_gap_ratio = (145 - 135) / atr_value

    # Verify the ATR Gap value matches our calculation
    gap_value = result_row[indicator.column_name()]
    assert abs(gap_value - expected_gap_ratio) < 1e-4


@pytest.mark.asyncio
async def test_daily_atr_gap_indicator_extend_realtime_multiple_calls_same_day(
    candles: "CandleStoreTest",
):
    # Set up test data for daily candles
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 5))
    daily_data = pd.DataFrame(
        {
            CandleCol.HIGH: [100, 110, 120, 130, 140],
            CandleCol.LOW: [90, 95, 105, 115, 125],
            CandleCol.CLOSE: [95, 105, 115, 125, 135],
        },
        index=daily_dates,
    )

    candles.set_data("AAPL", daily_data)

    indicator = DailyATRGapIndicator(period=5)

    # First call for Jan 6 at market open
    row1 = pd.Series(
        {
            CandleCol.OPEN: 145,
            CandleCol.HIGH: 150,
            CandleCol.LOW: 140,
            CandleCol.CLOSE: 148,
        },
        name=datetime(2023, 1, 6, 9, 30),
    )
    result1 = await indicator.extend_realtime("AAPL", row1.copy())

    # Second call for Jan 6 (same day, later time)
    row2 = pd.Series(
        {
            CandleCol.OPEN: 146,  # Different open, but should use the first one
            CandleCol.HIGH: 152,
            CandleCol.LOW: 142,
            CandleCol.CLOSE: 150,
        },
        name=datetime(2023, 1, 6, 10, 0),
    )
    result2 = await indicator.extend_realtime("AAPL", row2.copy())

    # Both should have the same ATR Gap value
    assert result1[indicator.column_name()] == result2[indicator.column_name()]

    # The indicator should use the first market open price for both calculations
    # and should not update with the second row's open price


@pytest.mark.asyncio
async def test_daily_atr_gap_indicator_extend_realtime_different_days(
    candles: "CandleStoreTest",
):
    # Set up test data for daily candles
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 6))
    daily_data = pd.DataFrame(
        {
            CandleCol.HIGH: [100, 110, 120, 130, 140, 150],
            CandleCol.LOW: [90, 95, 105, 115, 125, 135],
            CandleCol.CLOSE: [95, 105, 115, 125, 135, 145],
        },
        index=daily_dates,
    )

    candles.set_data("AAPL", daily_data)

    indicator = DailyATRGapIndicator(period=5)

    # Call for Jan 6
    row1 = pd.Series(
        {
            CandleCol.OPEN: 150,
            CandleCol.HIGH: 155,
            CandleCol.LOW: 145,
            CandleCol.CLOSE: 152,
        },
        name=datetime(2023, 1, 6, 9, 30),
    )
    result1 = await indicator.extend_realtime("AAPL", row1.copy())

    # Call for Jan 7 (different day)
    row2 = pd.Series(
        {
            CandleCol.OPEN: 155,
            CandleCol.HIGH: 160,
            CandleCol.LOW: 150,
            CandleCol.CLOSE: 158,
        },
        name=datetime(2023, 1, 7, 9, 30),
    )
    result2 = await indicator.extend_realtime("AAPL", row2.copy())

    # The values should be different for different days
    assert result1[indicator.column_name()] != result2[indicator.column_name()]

    # Calculate expected values for comparison
    # For Jan 6: day_open = 150, prev_day_close = 135
    # For Jan 7: day_open = 155, prev_day_close = 145

    # We can't easily calculate the exact expected values here without reimplementing
    # the ATR calculation, but we can verify they're different and not NaN
    assert not pd.isna(result1[indicator.column_name()])
    assert not pd.isna(result2[indicator.column_name()])


@pytest.mark.asyncio
async def test_daily_atr_gap_indicator_extend_realtime_premarket(
    candles: "CandleStoreTest",
):
    # Set up test data for daily candles
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 5))
    daily_data = pd.DataFrame(
        {
            CandleCol.HIGH: [100, 110, 120, 130, 140],
            CandleCol.LOW: [90, 95, 105, 115, 125],
            CandleCol.CLOSE: [95, 105, 115, 125, 135],
        },
        index=daily_dates,
    )

    candles.set_data("AAPL", daily_data)

    indicator = DailyATRGapIndicator(period=5)

    # Pre-market call (before 9:30)
    premarket_row = pd.Series(
        {
            CandleCol.OPEN: 140,
            CandleCol.HIGH: 142,
            CandleCol.LOW: 138,
            CandleCol.CLOSE: 141,
        },
        name=datetime(2023, 1, 6, 9, 0),
    )
    premarket_result = await indicator.extend_realtime("AAPL", premarket_row.copy())

    # Should be NaN because we don't have a market open price yet
    assert pd.isna(premarket_result[indicator.column_name()])

    # Market open call (at 9:30)
    market_open_row = pd.Series(
        {
            CandleCol.OPEN: 145,
            CandleCol.HIGH: 150,
            CandleCol.LOW: 140,
            CandleCol.CLOSE: 148,
        },
        name=datetime(2023, 1, 6, 9, 30),
    )
    market_open_result = await indicator.extend_realtime("AAPL", market_open_row.copy())

    # Now we should have a value
    assert not pd.isna(market_open_result[indicator.column_name()])

    # Another pre-market call for the next day
    next_premarket_row = pd.Series(
        {
            CandleCol.OPEN: 150,
            CandleCol.HIGH: 152,
            CandleCol.LOW: 148,
            CandleCol.CLOSE: 151,
        },
        name=datetime(2023, 1, 7, 9, 0),
    )
    next_premarket_result = await indicator.extend_realtime(
        "AAPL", next_premarket_row.copy()
    )

    # Should be NaN because we don't have a market open price for the new day yet
    assert pd.isna(next_premarket_result[indicator.column_name()])
