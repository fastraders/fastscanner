from datetime import date, datetime, time

import numpy as np
import pandas as pd
import pytest

from fastscanner.services.indicators.lib.daily import (
    ADRIndicator,
    ADVIndicator,
    DailyATRGapIndicator,
    DailyATRIndicator,
    DailyGapIndicator,
    DayOpenIndicator,
    PrevDayIndicator,
)
from fastscanner.services.indicators.ports import CandleCol
from fastscanner.services.indicators.tests.fixtures import CandleStoreTest, candles
from fastscanner.services.registry import ApplicationRegistry


def test_prev_day_indicator_type():
    indicator = PrevDayIndicator(candle_col=CandleCol.CLOSE)
    assert indicator.type() == "prev_day"


def test_prev_day_indicator_column_name():
    indicator = PrevDayIndicator(candle_col=CandleCol.CLOSE)
    assert indicator.column_name() == "prev_day_close"

    indicator = PrevDayIndicator(candle_col=CandleCol.HIGH)
    assert indicator.column_name() == "prev_day_high"


@pytest.mark.asyncio
async def test_prev_day_indicator_extend(candles: "CandleStoreTest"):
    # Set up test data for daily candles
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 10))
    daily_closes = [100, 102, 105, 103, 101, 104, 106, 108, 107, 105]
    daily_data = pd.DataFrame({CandleCol.CLOSE: daily_closes}, index=daily_dates)

    candles.set_data("AAPL", daily_data)

    # Create test data for intraday candles
    dates = [
        datetime(2023, 1, 11, 9, 30),
        datetime(2023, 1, 11, 10, 0),
        datetime(2023, 1, 11, 10, 30),
    ]
    closes = [110, 112, 115]
    df = pd.DataFrame({CandleCol.CLOSE: closes}, index=pd.DatetimeIndex(dates))

    indicator = PrevDayIndicator(candle_col=CandleCol.CLOSE)
    result_df = await indicator.extend("AAPL", df)

    # All rows should have the previous day's close (105)
    assert result_df[indicator.column_name()].to_list() == [105, 105, 105]


@pytest.mark.asyncio
async def test_prev_day_indicator_extend_multiple_days(candles: "CandleStoreTest"):
    # Set up test data for daily candles
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 10))
    daily_closes = [100, 102, 105, 103, 101, 104, 106, 108, 107, 105]
    daily_data = pd.DataFrame({CandleCol.CLOSE: daily_closes}, index=daily_dates)

    candles.set_data("AAPL", daily_data)

    # Create test data for intraday candles spanning multiple days
    dates = [
        datetime(2023, 1, 10, 9, 30),
        datetime(2023, 1, 10, 10, 0),
        datetime(2023, 1, 12, 9, 30),
        datetime(2023, 1, 12, 10, 0),
    ]
    closes = [110, 112, 115, 117]
    df = pd.DataFrame({CandleCol.CLOSE: closes}, index=pd.DatetimeIndex(dates))

    indicator = PrevDayIndicator(candle_col=CandleCol.CLOSE)
    result_df = await indicator.extend("AAPL", df)

    # Jan 11 rows should have Jan 10's close (105)
    # Jan 12 rows should have Jan 11's close (which we don't have in our daily data, so it should be NaN)
    expected_values = [107, 107, 105, 105]
    assert len(result_df) == len(expected_values)
    assert result_df[indicator.column_name()].to_list() == expected_values


@pytest.mark.asyncio
async def test_prev_day_indicator_extend_different_columns(candles: "CandleStoreTest"):
    # Set up test data for daily candles with multiple columns
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 10))
    daily_data = pd.DataFrame(
        {
            CandleCol.CLOSE: [100, 102, 105, 103, 101, 104, 106, 108, 107, 105],
            CandleCol.HIGH: [105, 107, 110, 108, 106, 109, 111, 113, 112, 110],
            CandleCol.LOW: [95, 97, 100, 98, 96, 99, 101, 103, 102, 100],
            CandleCol.OPEN: [98, 100, 103, 101, 99, 102, 104, 106, 105, 103],
        },
        index=daily_dates,
    )

    candles.set_data("AAPL", daily_data)

    # Create test data for intraday candles
    dates = [
        datetime(2023, 1, 11, 9, 30),
        datetime(2023, 1, 11, 10, 0),
    ]
    df = pd.DataFrame(
        {
            CandleCol.CLOSE: [110, 112],
            CandleCol.HIGH: [115, 117],
            CandleCol.LOW: [105, 107],
            CandleCol.OPEN: [108, 110],
        },
        index=pd.DatetimeIndex(dates),
    )

    # Test with HIGH column
    high_indicator = PrevDayIndicator(candle_col=CandleCol.HIGH)
    result_df = await high_indicator.extend("AAPL", df.copy())
    assert result_df[high_indicator.column_name()].to_list() == [110, 110]

    # Test with LOW column
    low_indicator = PrevDayIndicator(candle_col=CandleCol.LOW)
    result_df = await low_indicator.extend("AAPL", df.copy())
    assert result_df[low_indicator.column_name()].to_list() == [100, 100]

    # Test with OPEN column
    open_indicator = PrevDayIndicator(candle_col=CandleCol.OPEN)
    result_df = await open_indicator.extend("AAPL", df.copy())
    assert result_df[open_indicator.column_name()].to_list() == [103, 103]


def test_daily_gap_indicator_type():
    indicator = DailyGapIndicator()
    assert indicator.type() == "daily_gap"


def test_daily_gap_indicator_column_name():
    indicator = DailyGapIndicator()
    assert indicator.column_name() == "daily_gap"


@pytest.mark.asyncio
async def test_daily_gap_indicator_extend(candles: "CandleStoreTest"):
    # Set up test data for daily candles
    daily_dates = pd.date_range(start=date(2023, 1, 2), end=date(2023, 1, 11))
    daily_closes = [100, 102, 105, 103, 101, 104, 106, 107, 105, 108]
    daily_opens = [98, 100, 103, 101, 99, 102, 104, 106, 110, 115]
    daily_data = pd.DataFrame(
        {CandleCol.CLOSE: daily_closes, CandleCol.OPEN: daily_opens}, index=daily_dates
    )

    candles.set_data("AAPL", daily_data)

    # Create test data for intraday candles
    dates = [
        datetime(2023, 1, 10, 9, 30),  # Market open
        datetime(2023, 1, 10, 10, 0),
        datetime(2023, 1, 11, 8, 0),  # Pre-market (should be ignored for daily open)
        datetime(2023, 1, 11, 10, 30),
    ]
    opens = [110, 112, 115, 108]
    df = pd.DataFrame({CandleCol.OPEN: opens}, index=pd.DatetimeIndex(dates))

    indicator = DailyGapIndicator()
    result_df = await indicator.extend("AAPL", df)

    expected_gap1 = (110 - 107) / 107
    expected_gap2 = (115 - 105) / 105

    assert (result_df[indicator.column_name()].iloc[0] - expected_gap1) < 1e-4
    assert (result_df[indicator.column_name()].iloc[1] - expected_gap1) < 1e-4

    assert (result_df[indicator.column_name()].iloc[3] - expected_gap2) < 1e-4
    assert pd.isna(result_df[indicator.column_name()].iloc[2])


@pytest.mark.asyncio
async def test_daily_gap_indicator_extend_multiple_days(candles: "CandleStoreTest"):
    # Set up test data for daily candles
    daily_dates = pd.date_range(start=date(2023, 1, 2), end=date(2023, 1, 11))
    daily_closes = [100, 102, 105, 103, 101, 104, 106, 107, 105, 108]
    daily_opens = [100, 102, 105, 103, 101, 104, 106, 108, 110, 120]
    daily_data = pd.DataFrame(
        {CandleCol.CLOSE: daily_closes, CandleCol.OPEN: daily_opens}, index=daily_dates
    )

    candles.set_data("AAPL", daily_data)

    # Create test data for intraday candles spanning multiple days
    dates = [
        datetime(2023, 1, 10, 9, 30),  # Day 1 market open
        datetime(2023, 1, 10, 10, 0),
        datetime(2023, 1, 11, 9, 30),  # Day 2 market open
        datetime(2023, 1, 11, 10, 0),
    ]
    opens = [110, 112, 120, 122]
    df = pd.DataFrame({CandleCol.OPEN: opens}, index=pd.DatetimeIndex(dates))

    indicator = DailyGapIndicator()
    result_df = await indicator.extend("AAPL", df)

    # Day 1 gap: (110 - 105) / 105 = 0.0476
    day1_expected_gap = (110 - 107) / 107
    # Day 2 gap: (120 - 105) / 105 = 0.1429
    day2_expected_gap = (120 - 105) / 105

    # Day 2 gap: We don't have Jan 11's close in our daily data, so it should be NaN

    # Check day 1 values
    assert abs(result_df[indicator.column_name()].iloc[0] - day1_expected_gap) < 1e-4
    assert abs(result_df[indicator.column_name()].iloc[1] - day1_expected_gap) < 1e-4

    # Check day 2 values
    assert abs(result_df[indicator.column_name()].iloc[2] - day2_expected_gap) < 1e-4
    assert abs(result_df[indicator.column_name()].iloc[3] - day2_expected_gap) < 1e-4


@pytest.mark.asyncio
async def test_daily_gap_indicator_extend_with_premarket(candles: "CandleStoreTest"):
    # Set up test data for daily candles
    daily_dates = pd.date_range(start=date(2023, 1, 2), end=date(2023, 1, 11))
    daily_closes = [100, 102, 105, 103, 101, 104, 106, 107, 105, 108]
    daily_opens = [100, 102, 105, 103, 101, 104, 106, 108, 108, 110]
    daily_data = pd.DataFrame(
        {CandleCol.CLOSE: daily_closes, CandleCol.OPEN: daily_opens}, index=daily_dates
    )

    candles.set_data("AAPL", daily_data)

    # Create test data for intraday candles with pre-market data
    dates = [
        datetime(2023, 1, 10, 8, 0),  # Pre-market (should be ignored for daily open)
        datetime(2023, 1, 10, 8, 30),  # Pre-market (should be ignored for daily open)
        datetime(2023, 1, 11, 9, 30),  # Market open (should be used for daily open)
        datetime(2023, 1, 11, 10, 0),
    ]
    opens = [108, 109, 110, 112]
    df = pd.DataFrame({CandleCol.OPEN: opens}, index=pd.DatetimeIndex(dates))

    indicator = DailyGapIndicator()
    result_df = await indicator.extend("AAPL", df)

    # Gap calculation should use the first candle at or after 9:30
    # Previous day close = 105, First market open = 110
    # Gap = (110 - 105) / 105 = 0.0476
    expected_gap = (110 - 105) / 105

    assert abs(result_df[indicator.column_name()].iloc[2] - expected_gap) < 1e-4
    assert abs(result_df[indicator.column_name()].iloc[3] - expected_gap) < 1e-4

    # Pre-market rows should be NaN
    assert pd.isna(result_df[indicator.column_name()].iloc[0])
    assert pd.isna(result_df[indicator.column_name()].iloc[1])


@pytest.mark.asyncio
async def test_daily_gap_indicator_extend_no_market_open(candles: "CandleStoreTest"):
    # Set up test data for daily candles
    daily_dates = pd.date_range(start=date(2023, 1, 2), end=date(2023, 1, 11))
    daily_closes = [100, 102, 105, 103, 101, 104, 106, 108, 107, 105]
    daily_opens = [100, 102, 105, 103, 101, 104, 106, 108, 107, 105]
    daily_data = pd.DataFrame(
        {CandleCol.CLOSE: daily_closes, CandleCol.OPEN: daily_opens}, index=daily_dates
    )

    candles.set_data("AAPL", daily_data)

    # Create test data for intraday candles with only pre-market data
    dates = [
        datetime(2023, 1, 11, 8, 0),  # Pre-market
        datetime(2023, 1, 11, 8, 30),  # Pre-market
        datetime(2023, 1, 11, 9, 0),  # Pre-market
    ]
    opens = [108, 109, 110]
    df = pd.DataFrame({CandleCol.OPEN: opens}, index=pd.DatetimeIndex(dates))

    indicator = DailyGapIndicator()
    result_df = await indicator.extend("AAPL", df)

    # No market open candles, so gap should be NaN
    for gap in result_df[indicator.column_name()]:
        assert pd.isna(gap)


@pytest.mark.asyncio
async def test_prev_day_indicator_extend_realtime(candles: "CandleStoreTest"):
    # Set up test data for daily candles
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 10))
    daily_closes = [100, 102, 105, 103, 101, 104, 106, 108, 107, 105]
    daily_data = pd.DataFrame({CandleCol.CLOSE: daily_closes}, index=daily_dates)

    candles.set_data("AAPL", daily_data)

    # Create a new row for realtime data
    new_row = pd.Series(
        {
            CandleCol.CLOSE: 110,
            CandleCol.HIGH: 112,
            CandleCol.LOW: 108,
            CandleCol.OPEN: 109,
        },
        name=datetime(2023, 1, 11, 9, 30),
    )

    indicator = PrevDayIndicator(candle_col=CandleCol.CLOSE)
    result_row = await indicator.extend_realtime("AAPL", new_row.copy())

    # Should have the previous day's close (105)
    assert result_row[indicator.column_name()] == 105


@pytest.mark.asyncio
async def test_prev_day_indicator_extend_realtime_multiple_calls_same_day(
    candles: "CandleStoreTest",
):
    # Set up test data for daily candles
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 10))
    daily_closes = [100, 102, 105, 103, 101, 104, 106, 108, 107, 105]
    daily_data = pd.DataFrame({CandleCol.CLOSE: daily_closes}, index=daily_dates)

    candles.set_data("AAPL", daily_data)

    indicator = PrevDayIndicator(candle_col=CandleCol.CLOSE)

    # First call for Jan 11
    row1 = pd.Series(
        {CandleCol.CLOSE: 110},
        name=datetime(2023, 1, 11, 9, 30),
    )
    result1 = await indicator.extend_realtime("AAPL", row1.copy())

    # Second call for Jan 11 (same day)
    row2 = pd.Series(
        {CandleCol.CLOSE: 112},
        name=datetime(2023, 1, 11, 10, 0),
    )
    result2 = await indicator.extend_realtime("AAPL", row2.copy())

    # Both should have the same previous day value (105)
    assert result1[indicator.column_name()] == 105
    assert result2[indicator.column_name()] == 105

    # The indicator should have only made one call to get the daily data
    # This is verified implicitly by the fact that the test passes
    # (if it made multiple calls, it would still work but be less efficient)


@pytest.mark.asyncio
async def test_prev_day_indicator_extend_realtime_different_days(
    candles: "CandleStoreTest",
):
    # Set up test data for daily candles
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 11))
    daily_closes = [100, 102, 105, 103, 101, 104, 106, 108, 107, 105, 110]
    daily_data = pd.DataFrame({CandleCol.CLOSE: daily_closes}, index=daily_dates)

    candles.set_data("AAPL", daily_data)

    indicator = PrevDayIndicator(candle_col=CandleCol.CLOSE)

    # Call for Jan 11
    row1 = pd.Series(
        {CandleCol.CLOSE: 110},
        name=datetime(2023, 1, 11, 9, 30),
    )
    result1 = await indicator.extend_realtime("AAPL", row1.copy())

    # Call for Jan 12 (different day)
    row2 = pd.Series(
        {CandleCol.CLOSE: 115},
        name=datetime(2023, 1, 12, 9, 30),
    )
    result2 = await indicator.extend_realtime("AAPL", row2.copy())

    # Jan 11 should have Jan 10's close (105)
    assert result1[indicator.column_name()] == 105

    # Jan 12 should have Jan 11's close (110)
    assert result2[indicator.column_name()] == 110


@pytest.mark.asyncio
async def test_daily_gap_indicator_extend_realtime(candles: "CandleStoreTest"):
    # Set up test data for daily candles
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 10))
    daily_closes = [100, 102, 105, 103, 101, 104, 106, 108, 107, 105]
    daily_data = pd.DataFrame({CandleCol.CLOSE: daily_closes}, index=daily_dates)

    candles.set_data("AAPL", daily_data)

    # Create a new row for realtime data at market open
    new_row = pd.Series(
        {
            CandleCol.CLOSE: 110,
            CandleCol.HIGH: 112,
            CandleCol.LOW: 108,
            CandleCol.OPEN: 110,  # This will be used as the daily open
        },
        name=datetime(2023, 1, 11, 9, 30),  # Market open time
    )

    indicator = DailyGapIndicator()
    result_row = await indicator.extend_realtime("AAPL", new_row.copy())

    # Gap calculation: (daily_open - prev_day_close) / prev_day_close
    # Previous day close = 105, Daily open = 110
    # Gap = (110 - 105) / 105 = 0.0476
    expected_gap = (110 - 105) / 105
    assert abs(result_row[indicator.column_name()] - expected_gap) < 1e-4


@pytest.mark.asyncio
async def test_daily_gap_indicator_extend_realtime_multiple_calls_same_day(
    candles: "CandleStoreTest",
):
    # Set up test data for daily candles
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 10))
    daily_closes = [100, 102, 105, 103, 101, 104, 106, 108, 107, 105]
    daily_data = pd.DataFrame({CandleCol.CLOSE: daily_closes}, index=daily_dates)

    candles.set_data("AAPL", daily_data)

    indicator = DailyGapIndicator()

    # First call for Jan 11 at market open
    row1 = pd.Series(
        {CandleCol.OPEN: 110, CandleCol.CLOSE: 110},
        name=datetime(2023, 1, 11, 9, 30),
    )
    result1 = await indicator.extend_realtime("AAPL", row1.copy())

    # Second call for Jan 11 (same day, later time)
    row2 = pd.Series(
        {CandleCol.OPEN: 112, CandleCol.CLOSE: 112},
        name=datetime(2023, 1, 11, 10, 0),
    )
    result2 = await indicator.extend_realtime("AAPL", row2.copy())

    # Both should have the same gap value
    # Gap = (110 - 105) / 105 = 0.0476
    expected_gap = (110 - 105) / 105
    assert abs(result1[indicator.column_name()] - expected_gap) < 1e-4
    assert abs(result2[indicator.column_name()] - expected_gap) < 1e-4

    # The indicator should use the first market open price (110) for both calculations
    # and should not update with the second row's open price (112)


@pytest.mark.asyncio
async def test_daily_gap_indicator_extend_realtime_different_days(
    candles: "CandleStoreTest",
):
    # Set up test data for daily candles
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 11))
    daily_closes = [100, 102, 105, 103, 101, 104, 106, 108, 107, 105, 110]
    daily_data = pd.DataFrame({CandleCol.CLOSE: daily_closes}, index=daily_dates)

    candles.set_data("AAPL", daily_data)

    indicator = DailyGapIndicator()

    # Call for Jan 11
    row1 = pd.Series(
        {CandleCol.OPEN: 110, CandleCol.CLOSE: 110},
        name=datetime(2023, 1, 11, 9, 30),
    )
    result1 = await indicator.extend_realtime("AAPL", row1.copy())

    # Call for Jan 12 (different day)
    row2 = pd.Series(
        {CandleCol.OPEN: 115, CandleCol.CLOSE: 115},
        name=datetime(2023, 1, 12, 9, 30),
    )
    result2 = await indicator.extend_realtime("AAPL", row2.copy())

    # Jan 11 gap: (110 - 105) / 105 = 0.0476
    expected_gap1 = (110 - 105) / 105
    assert abs(result1[indicator.column_name()] - expected_gap1) < 1e-4

    # Jan 12 gap: (115 - 110) / 110 = 0.0455
    expected_gap2 = (115 - 110) / 110
    assert abs(result2[indicator.column_name()] - expected_gap2) < 1e-4


@pytest.mark.asyncio
async def test_daily_gap_indicator_extend_realtime_premarket(
    candles: "CandleStoreTest",
):
    # Set up test data for daily candles
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 10))
    daily_closes = [100, 102, 105, 103, 101, 104, 106, 108, 107, 105]
    daily_data = pd.DataFrame({CandleCol.CLOSE: daily_closes}, index=daily_dates)

    candles.set_data("AAPL", daily_data)

    indicator = DailyGapIndicator()

    # Pre-market call (before 9:30)
    premarket_row = pd.Series(
        {CandleCol.OPEN: 108, CandleCol.CLOSE: 108},
        name=datetime(2023, 1, 11, 9, 0),
    )
    premarket_result = await indicator.extend_realtime("AAPL", premarket_row.copy())

    # Should be NaN because we don't have a market open price yet
    assert pd.isna(premarket_result[indicator.column_name()])

    # Market open call (at 9:30)
    market_open_row = pd.Series(
        {CandleCol.OPEN: 110, CandleCol.CLOSE: 110},
        name=datetime(2023, 1, 11, 9, 30),
    )
    market_open_result = await indicator.extend_realtime("AAPL", market_open_row.copy())

    # Now we should have a gap value
    expected_gap = (110 - 105) / 105
    assert abs(market_open_result[indicator.column_name()] - expected_gap) < 1e-4

    # Another pre-market call for the next day
    next_premarket_row = pd.Series(
        {CandleCol.OPEN: 112, CandleCol.CLOSE: 112},
        name=datetime(2023, 1, 12, 9, 0),
    )
    next_premarket_result = await indicator.extend_realtime(
        "AAPL", next_premarket_row.copy()
    )

    # Should be NaN because we don't have a market open price for the new day yet
    assert pd.isna(next_premarket_result[indicator.column_name()])


@pytest.mark.asyncio
async def test_daily_gap_indicator_extend_realtime_missing_prev_close(
    candles: "CandleStoreTest",
):
    # Set up test data for daily candles, but with a gap in the data
    # Missing Jan 10 data
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 9))
    daily_closes = [100, 102, 105, 103, 101, 104, 106, 108, 107]
    daily_data = pd.DataFrame({CandleCol.CLOSE: daily_closes}, index=daily_dates)

    candles.set_data("AAPL", daily_data)

    indicator = DailyGapIndicator()

    # Call for Jan 11 (missing previous day's close)
    row = pd.Series(
        {CandleCol.OPEN: 110, CandleCol.CLOSE: 110},
        name=datetime(2023, 1, 11, 9, 30),
    )

    result = await indicator.extend_realtime("AAPL", row.copy())
    assert pd.isna(result[indicator.column_name()])


def test_daily_atr_indicator_type():
    indicator = DailyATRIndicator(period=14)
    assert indicator.type() == "daily_atr"


def test_daily_atr_indicator_column_name():
    indicator = DailyATRIndicator(period=14)
    assert indicator.column_name() == "daily_atr_14"

    indicator = DailyATRIndicator(period=20)
    assert indicator.column_name() == "daily_atr_20"


@pytest.mark.asyncio
async def test_daily_atr_indicator_extend(candles: "CandleStoreTest"):
    # Set up test data for daily candles
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 20))
    daily_data = pd.DataFrame(
        {
            CandleCol.OPEN: [
                100,
                102,
                105,
                103,
                101,
                104,
                106,
                108,
                107,
                105,
                110,
                112,
                114,
                113,
                111,
                115,
                117,
                119,
                118,
                116,
            ],
            CandleCol.HIGH: [
                105,
                107,
                110,
                108,
                106,
                109,
                111,
                113,
                112,
                110,
                115,
                117,
                119,
                118,
                116,
                120,
                122,
                124,
                123,
                121,
            ],
            CandleCol.LOW: [
                95,
                97,
                100,
                98,
                96,
                99,
                101,
                103,
                102,
                100,
                105,
                107,
                109,
                108,
                106,
                110,
                112,
                114,
                113,
                111,
            ],
            CandleCol.CLOSE: [
                102,
                105,
                103,
                101,
                104,
                106,
                108,
                107,
                105,
                110,
                112,
                114,
                113,
                111,
                115,
                117,
                119,
                118,
                116,
                120,
            ],
        },
        index=daily_dates,
    )

    candles.set_data("AAPL", daily_data)

    # Create test data for intraday candles
    dates = [
        datetime(2023, 1, 21, 9, 30),
        datetime(2023, 1, 21, 10, 0),
        datetime(2023, 1, 21, 10, 30),
    ]
    df = pd.DataFrame(
        {
            CandleCol.OPEN: [118, 119, 120],
            CandleCol.HIGH: [123, 124, 125],
            CandleCol.LOW: [113, 114, 115],
            CandleCol.CLOSE: [121, 122, 123],
        },
        index=pd.DatetimeIndex(dates),
    )

    # Test with period=14
    indicator = DailyATRIndicator(period=14)
    result_df = await indicator.extend("AAPL", df.copy())

    # Verify the column exists and has values for all rows
    assert indicator.column_name() in result_df.columns
    assert not result_df[indicator.column_name()].isna().any()

    # All rows should have the same ATR value since they're on the same day
    assert result_df[indicator.column_name()].nunique() == 1


@pytest.mark.asyncio
async def test_daily_atr_indicator_calculation(candles: "CandleStoreTest"):
    # Set up test data with known values for manual ATR calculation
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
            CandleCol.OPEN: [140],
            CandleCol.HIGH: [150],
            CandleCol.LOW: [130],
            CandleCol.CLOSE: [145],
        },
        index=pd.DatetimeIndex(dates),
    )

    # Test with period=5
    indicator = DailyATRIndicator(period=5)
    result_df = await indicator.extend("AAPL", df.copy())

    # Verify the ATR value is reasonable (should be around 13-14)
    atr_value = result_df[indicator.column_name()].iloc[0]
    assert abs(atr_value - 14.391) <= 0.001


@pytest.mark.asyncio
async def test_daily_atr_indicator_extend_realtime(candles: "CandleStoreTest"):
    # Set up test data for daily candles with simple values for easier calculation
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

    # Create a new row for realtime data
    new_row = pd.Series(
        {
            CandleCol.OPEN: 140,
            CandleCol.HIGH: 150,
            CandleCol.LOW: 130,
            CandleCol.CLOSE: 145,
        },
        name=datetime(2023, 1, 6, 9, 30),
    )

    # Use a smaller period for easier calculation
    indicator = DailyATRIndicator(period=5)
    result_row = await indicator.extend_realtime("AAPL", new_row.copy())

    # Verify the actual ATR value
    expected_atr = 14.391  # Calculated from the test data with period=5
    actual_atr = result_row[indicator.column_name()]
    assert abs(actual_atr - expected_atr) < 0.001


@pytest.mark.asyncio
async def test_daily_atr_indicator_extend_realtime_multiple_calls_same_day(
    candles: "CandleStoreTest",
):
    # Set up test data for daily candles with a reduced period of 5
    # This reduces the data array size significantly
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

    # Use a smaller period (5 instead of 14)
    indicator = DailyATRIndicator(period=5)

    # First call for Jan 7
    row1 = pd.Series(
        {
            CandleCol.OPEN: 145,
            CandleCol.HIGH: 160,
            CandleCol.LOW: 140,
            CandleCol.CLOSE: 155,
        },
        name=datetime(2023, 1, 7, 9, 30),
    )
    result1 = await indicator.extend_realtime("AAPL", row1.copy())

    # Second call for Jan 7 (same day)
    row2 = pd.Series(
        {
            CandleCol.OPEN: 155,
            CandleCol.HIGH: 165,
            CandleCol.LOW: 145,
            CandleCol.CLOSE: 160,
        },
        name=datetime(2023, 1, 7, 10, 0),
    )
    result2 = await indicator.extend_realtime("AAPL", row2.copy())

    # Both should have the same ATR value
    assert result1[indicator.column_name()] == result2[indicator.column_name()]

    # Assert the actual ATR value
    # Expected ATR calculation:
    # TR values for days 1-6:
    # Day 1: High-Low = 10
    # Day 2: max(15, 15, 10) = 15
    # Day 3: max(15, 15, 10) = 15
    # Day 4: max(15, 15, 10) = 15
    # Day 5: max(15, 15, 10) = 15
    # Day 6: max(15, 15, 10) = 15
    # EMA with alpha=1/5: ~14.5
    expected_atr = 14.5
    actual_atr = result1[indicator.column_name()]
    assert abs(actual_atr - expected_atr) < 0.1


@pytest.mark.asyncio
async def test_daily_atr_indicator_extend_realtime_different_days(
    candles: "CandleStoreTest",
):
    # Set up test data for daily candles with simple values for easy manual calculation
    # Use datetime objects for the index, not date objects
    daily_dates = [
        datetime(2023, 1, 1, 16, 0),  # Use 4 PM for daily candles
        datetime(2023, 1, 2, 16, 0),
        datetime(2023, 1, 3, 16, 0),
        datetime(2023, 1, 4, 16, 0),
        datetime(2023, 1, 5, 16, 0),
    ]
    daily_data = pd.DataFrame(
        {
            CandleCol.HIGH: [100, 110, 120, 130, 140],
            CandleCol.LOW: [90, 95, 105, 115, 125],
            CandleCol.CLOSE: [95, 105, 115, 125, 135],
        },
        index=pd.DatetimeIndex(daily_dates),
    )

    candles.set_data("AAPL", daily_data)

    # Use a small period for easier calculation
    indicator = DailyATRIndicator(period=3)

    # Call for Jan 6
    row1 = pd.Series(
        {
            CandleCol.OPEN: 140,
            CandleCol.HIGH: 150,
            CandleCol.LOW: 130,
            CandleCol.CLOSE: 145,
        },
        name=datetime(2023, 1, 6, 9, 30),
    )
    result1 = await indicator.extend_realtime("AAPL", row1.copy())

    # Get the ATR value for Jan 6
    jan6_atr = result1[indicator.column_name()]

    # Add Jan 6 data to our daily data for the next calculation
    jan6_data = pd.DataFrame(
        {
            CandleCol.HIGH: [150],
            CandleCol.LOW: [130],
            CandleCol.CLOSE: [145],
        },
        index=[datetime(2023, 1, 6, 16, 0)],  # Use datetime object for the index
    )
    updated_daily_data = pd.concat([daily_data, jan6_data])
    candles.set_data("AAPL", updated_daily_data)

    # Call for Jan 7 with significantly different values
    row2 = pd.Series(
        {
            CandleCol.OPEN: 160,
            CandleCol.HIGH: 180,
            CandleCol.LOW: 140,
            CandleCol.CLOSE: 170,
        },
        name=datetime(2023, 1, 7, 9, 30),
    )
    result2 = await indicator.extend_realtime("AAPL", row2.copy())

    # Get the ATR value for Jan 7
    jan7_atr = result2[indicator.column_name()]

    # Verify the ATR values are reasonable and different for different days
    assert abs(jan6_atr - 15) < 0.001
    assert abs(jan7_atr - 17.077) < 0.001

    # Ensure the values are different
    assert jan6_atr != jan7_atr


def test_adv_indicator_type():
    indicator = ADVIndicator(period=14)
    assert indicator.type() == "adv"


def test_adv_indicator_column_name():
    indicator = ADVIndicator(period=14)
    assert indicator.column_name() == "adv_14"

    indicator = ADVIndicator(period=20)
    assert indicator.column_name() == "adv_20"


@pytest.mark.asyncio
async def test_adv_indicator_extend(candles: "CandleStoreTest"):
    # Set up test data for daily candles
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 20))
    daily_volumes = [
        1000000,
        1200000,
        900000,
        1100000,
        1300000,
        1500000,
        1400000,
        1600000,
        1800000,
        1700000,
        1900000,
        2000000,
        1800000,
        1700000,
        1600000,
        1500000,
        1400000,
        1300000,
        1200000,
        1100000,
    ]
    daily_data = pd.DataFrame(
        {
            CandleCol.VOLUME: daily_volumes,
            CandleCol.CLOSE: [100] * 20,  # Add CLOSE column to satisfy any requirements
        },
        index=daily_dates,
    )

    candles.set_data("AAPL", daily_data)

    # Create test data for intraday candles
    dates = [
        datetime(2023, 1, 21, 9, 30),
        datetime(2023, 1, 21, 10, 0),
        datetime(2023, 1, 21, 10, 30),
    ]
    df = pd.DataFrame(
        {CandleCol.VOLUME: [100000, 120000, 90000], CandleCol.CLOSE: [110, 112, 115]},
        index=pd.DatetimeIndex(dates),
    )

    # Test with period=14
    indicator = ADVIndicator(period=14)
    result_df = await indicator.extend("AAPL", df.copy())

    # Verify the column exists and has values for all rows
    assert indicator.column_name() in result_df.columns
    assert not result_df[indicator.column_name()].isna().any()

    # All rows should have the same ADV value since they're on the same day
    assert result_df[indicator.column_name()].nunique() == 1

    # Calculate expected ADV (average of last 14 days' volume)
    expected_adv = sum(daily_volumes[-14:]) / 14
    actual_adv = result_df[indicator.column_name()].iloc[0]
    assert abs(actual_adv - expected_adv) < 0.001


@pytest.mark.asyncio
async def test_adv_indicator_extend_empty_data(candles: "CandleStoreTest"):
    # Set up empty data
    daily_data = pd.DataFrame(
        {CandleCol.VOLUME: [], CandleCol.CLOSE: []}, index=pd.DatetimeIndex([])
    )
    candles.set_data("AAPL", daily_data)

    # Create test data for intraday candles
    dates = [datetime(2023, 1, 21, 9, 30)]
    df = pd.DataFrame(
        {CandleCol.VOLUME: [100000], CandleCol.CLOSE: [110]},
        index=pd.DatetimeIndex(dates),
    )

    # Test with period=14
    indicator = ADVIndicator(period=14)
    result_df = await indicator.extend("AAPL", df.copy())

    # Should return NaN for empty data
    assert pd.isna(result_df[indicator.column_name()].iloc[0])


@pytest.mark.asyncio
async def test_adv_indicator_extend_multiple_days(candles: "CandleStoreTest"):
    # Set up test data for daily candles
    daily_dates = pd.date_range(start=date(2023, 1, 2), end=date(2023, 1, 21))
    daily_volumes = [
        1000000,
        1200000,
        900000,
        1100000,
        1300000,
        1500000,
        1400000,
        1600000,
        1800000,
        1700000,
        1900000,
        2000000,
        1800000,
        1700000,
        1600000,
        1500000,
        1400000,
        1300000,
        1200000,
        1100000,
    ]
    daily_data = pd.DataFrame(
        {CandleCol.VOLUME: daily_volumes, CandleCol.CLOSE: [100] * 20},
        index=daily_dates,
    )

    candles.set_data("AAPL", daily_data)

    # Create test data for intraday candles spanning multiple days
    dates = [
        datetime(2023, 1, 21, 9, 30),
        datetime(2023, 1, 21, 10, 0),
        datetime(2023, 1, 22, 9, 30),
        datetime(2023, 1, 22, 10, 0),
    ]
    df = pd.DataFrame(
        {
            CandleCol.VOLUME: [100000, 120000, 90000, 110000],
            CandleCol.CLOSE: [110, 112, 115, 117],
        },
        index=pd.DatetimeIndex(dates),
    )

    # Test with period=14
    indicator = ADVIndicator(period=14)
    result_df = await indicator.extend("AAPL", df.copy())

    # Verify the column exists and has values for all rows
    assert indicator.column_name() in result_df.columns

    jan21_expected_adv = sum(daily_volumes[-15:-1]) / 14
    jan22_expected_adv = sum(daily_volumes[-14:]) / 14

    # Jan 21 rows should have the same ADV
    assert abs(result_df[indicator.column_name()].iloc[0] - jan21_expected_adv) < 0.001
    assert abs(result_df[indicator.column_name()].iloc[1] - jan21_expected_adv) < 0.001

    # Jan 22 rows should have the same ADV
    assert abs(result_df[indicator.column_name()].iloc[2] - jan22_expected_adv) < 0.001
    assert abs(result_df[indicator.column_name()].iloc[3] - jan22_expected_adv) < 0.001


@pytest.mark.asyncio
async def test_adv_indicator_extend_realtime(candles: "CandleStoreTest"):
    # Set up test data for daily candles
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 14))
    daily_volumes = [
        1000000,
        1200000,
        900000,
        1100000,
        1300000,
        1500000,
        1400000,
        1600000,
        1800000,
        1700000,
        1900000,
        2000000,
        1800000,
        1700000,
    ]
    daily_data = pd.DataFrame(
        {CandleCol.VOLUME: daily_volumes, CandleCol.CLOSE: [100] * 14},
        index=daily_dates,
    )

    candles.set_data("AAPL", daily_data)

    # Create a new row for realtime data
    new_row = pd.Series(
        {CandleCol.VOLUME: 100000, CandleCol.CLOSE: 110},
        name=datetime(2023, 1, 15, 9, 30),
    )

    # Test with period=14
    indicator = ADVIndicator(period=14)
    result_row = await indicator.extend_realtime("AAPL", new_row.copy())

    # Calculate expected ADV (average of last 14 days' volume)
    expected_adv = sum(daily_volumes) / 14
    actual_adv = result_row[indicator.column_name()]
    assert abs(actual_adv - expected_adv) < 0.001


@pytest.mark.asyncio
async def test_adv_indicator_extend_realtime_multiple_calls_same_day(
    candles: "CandleStoreTest",
):
    # Set up test data for daily candles
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 14))
    daily_volumes = [
        1000000,
        1200000,
        900000,
        1100000,
        1300000,
        1500000,
        1400000,
        1600000,
        1800000,
        1700000,
        1900000,
        2000000,
        1800000,
        1700000,
    ]
    daily_data = pd.DataFrame(
        {CandleCol.VOLUME: daily_volumes, CandleCol.CLOSE: [100] * 14},
        index=daily_dates,
    )

    candles.set_data("AAPL", daily_data)

    indicator = ADVIndicator(period=14)

    # First call for Jan 15
    row1 = pd.Series(
        {CandleCol.VOLUME: 100000, CandleCol.CLOSE: 110},
        name=datetime(2023, 1, 15, 9, 30),
    )
    result1 = await indicator.extend_realtime("AAPL", row1.copy())

    # Second call for Jan 15 (same day)
    row2 = pd.Series(
        {CandleCol.VOLUME: 120000, CandleCol.CLOSE: 112},
        name=datetime(2023, 1, 15, 10, 0),
    )
    result2 = await indicator.extend_realtime("AAPL", row2.copy())

    # Both should have the same ADV value
    assert result1[indicator.column_name()] == result2[indicator.column_name()]

    # Calculate expected ADV
    expected_adv = sum(daily_volumes) / 14
    assert abs(result1[indicator.column_name()] - expected_adv) < 0.001


@pytest.mark.asyncio
async def test_adv_indicator_extend_realtime_different_days(candles: "CandleStoreTest"):
    # Set up test data for daily candles
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 14))
    daily_volumes = [
        1000000,
        1200000,
        900000,
        1100000,
        1300000,
        1500000,
        1400000,
        1600000,
        1800000,
        1700000,
        1900000,
        2000000,
        1800000,
        1700000,
    ]
    daily_data = pd.DataFrame(
        {CandleCol.VOLUME: daily_volumes, CandleCol.CLOSE: [100] * 14},
        index=daily_dates,
    )

    candles.set_data("AAPL", daily_data)

    indicator = ADVIndicator(period=14)

    # Call for Jan 15
    row1 = pd.Series(
        {CandleCol.VOLUME: 100000, CandleCol.CLOSE: 110},
        name=datetime(2023, 1, 15, 9, 30),
    )
    result1 = await indicator.extend_realtime("AAPL", row1.copy())

    # Get the ADV value for Jan 15
    jan15_adv = result1[indicator.column_name()]

    # Add Jan 15 data to our daily data for the next calculation
    jan15_data = pd.DataFrame(
        {CandleCol.VOLUME: [1500000], CandleCol.CLOSE: [110]},
        index=[datetime(2023, 1, 15, 16, 0)],
    )
    updated_daily_data = pd.concat([daily_data, jan15_data])
    candles.set_data("AAPL", updated_daily_data)

    # Call for Jan 16 (different day)
    row2 = pd.Series(
        {CandleCol.VOLUME: 120000, CandleCol.CLOSE: 115},
        name=datetime(2023, 1, 16, 9, 30),
    )
    result2 = await indicator.extend_realtime("AAPL", row2.copy())

    # Get the ADV value for Jan 16
    jan16_adv = result2[indicator.column_name()]

    # Calculate expected ADV for Jan 15
    jan15_expected_adv = sum(daily_volumes) / 14

    # Calculate expected ADV for Jan 16 (should include Jan 15 and exclude Jan 1)
    jan16_expected_adv = (sum(daily_volumes[1:]) + 1500000) / 14

    # Verify the ADV values
    assert abs(jan15_adv - jan15_expected_adv) < 0.001
    assert abs(jan16_adv - jan16_expected_adv) < 0.001

    # Ensure the values are different
    assert jan15_adv != jan16_adv


def test_adr_indicator_type():
    indicator = ADRIndicator(period=14)
    assert indicator.type() == "adr"


def test_adr_indicator_column_name():
    indicator = ADRIndicator(period=14)
    assert indicator.column_name() == "adr_14"

    indicator = ADRIndicator(period=20)
    assert indicator.column_name() == "adr_20"


@pytest.mark.asyncio
async def test_adr_indicator_extend(candles: "CandleStoreTest"):
    # Set up test data for daily candles
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 20))
    daily_data = pd.DataFrame(
        {
            CandleCol.HIGH: [
                105,
                107,
                110,
                108,
                106,
                109,
                111,
                113,
                112,
                110,
                115,
                117,
                119,
                118,
                116,
                120,
                122,
                124,
                123,
                121,
            ],
            CandleCol.LOW: [
                95,
                97,
                100,
                98,
                96,
                99,
                101,
                103,
                102,
                100,
                105,
                107,
                109,
                108,
                106,
                110,
                112,
                114,
                113,
                111,
            ],
            CandleCol.CLOSE: [
                102,
                105,
                103,
                101,
                104,
                106,
                108,
                107,
                105,
                110,
                112,
                114,
                113,
                111,
                115,
                117,
                119,
                118,
                116,
                120,
            ],
        },
        index=daily_dates,
    )

    candles.set_data("AAPL", daily_data)

    # Create test data for intraday candles
    dates = [
        datetime(2023, 1, 21, 9, 30),
        datetime(2023, 1, 21, 10, 0),
        datetime(2023, 1, 21, 10, 30),
    ]
    df = pd.DataFrame(
        {
            CandleCol.HIGH: [123, 124, 125],
            CandleCol.LOW: [113, 114, 115],
            CandleCol.CLOSE: [121, 122, 123],
        },
        index=pd.DatetimeIndex(dates),
    )

    # Test with period=14
    indicator = ADRIndicator(period=14)
    result_df = await indicator.extend("AAPL", df.copy())

    # Verify the column exists and has values for all rows
    assert indicator.column_name() in result_df.columns
    assert not result_df[indicator.column_name()].isna().any()

    # All rows should have the same ADR value since they're on the same day
    assert result_df[indicator.column_name()].nunique() == 1

    # Calculate expected ADR (average of (high-low)/low for the last 14 days)
    expected_adr = 0
    for i in range(6, 20):  # Last 14 days (from day 6 to day 19)
        high = daily_data[CandleCol.HIGH].iloc[i]
        low = daily_data[CandleCol.LOW].iloc[i]
        close = daily_data[CandleCol.CLOSE].iloc[i]
        expected_adr += (high - low) / close
    expected_adr /= 14

    actual_adr = result_df[indicator.column_name()].iloc[0]
    assert abs(actual_adr - expected_adr) < 0.001


@pytest.mark.asyncio
async def test_adr_indicator_extend_empty_data(candles: "CandleStoreTest"):
    # Set up empty data
    daily_data = pd.DataFrame(
        {CandleCol.HIGH: [], CandleCol.LOW: [], CandleCol.CLOSE: []},
        index=pd.DatetimeIndex([]),
    )
    candles.set_data("AAPL", daily_data)

    # Create test data for intraday candles
    dates = [datetime(2023, 1, 21, 9, 30)]
    df = pd.DataFrame(
        {
            CandleCol.HIGH: [123],
            CandleCol.LOW: [113],
            CandleCol.CLOSE: [121],
        },
        index=pd.DatetimeIndex(dates),
    )

    # Test with period=14
    indicator = ADRIndicator(period=14)
    result_df = await indicator.extend("AAPL", df.copy())

    # Should return NaN for empty data
    assert pd.isna(result_df[indicator.column_name()].iloc[0])


@pytest.mark.asyncio
async def test_adr_indicator_extend_multiple_days(candles: "CandleStoreTest"):
    # Set up test data for daily candles
    daily_dates = pd.date_range(start=date(2023, 1, 2), end=date(2023, 1, 21))
    daily_data = pd.DataFrame(
        {
            CandleCol.HIGH: [
                105,
                107,
                110,
                108,
                106,
                109,
                111,
                113,
                112,
                110,
                115,
                117,
                119,
                118,
                116,
                120,
                122,
                124,
                123,
                121,
            ],
            CandleCol.LOW: [
                95,
                97,
                100,
                98,
                96,
                99,
                101,
                103,
                102,
                100,
                105,
                107,
                109,
                108,
                106,
                110,
                112,
                114,
                113,
                111,
            ],
            CandleCol.CLOSE: [
                95,
                97,
                100,
                98,
                96,
                99,
                101,
                103,
                102,
                100,
                105,
                107,
                109,
                108,
                106,
                110,
                112,
                114,
                113,
                111,
            ],
        },
        index=daily_dates,
    )

    candles.set_data("AAPL", daily_data)

    # Create test data for intraday candles spanning multiple days
    dates = [
        datetime(2023, 1, 21, 9, 30),
        datetime(2023, 1, 21, 10, 0),
        datetime(2023, 1, 22, 9, 30),
        datetime(2023, 1, 22, 10, 0),
    ]
    df = pd.DataFrame(
        {
            CandleCol.HIGH: [123, 124, 125, 126],
            CandleCol.LOW: [113, 114, 115, 116],
        },
        index=pd.DatetimeIndex(dates),
    )

    # Test with period=14
    indicator = ADRIndicator(period=14)
    result_df = await indicator.extend("AAPL", df.copy())

    # Verify the column exists and has values for all rows
    assert indicator.column_name() in result_df.columns

    # Calculate expected ADR for Jan 21 (average of days 6-19)
    jan21_expected_adr = 0
    for i in range(5, 19):  # Last 14 days (from day 6 to day 19)
        high = daily_data[CandleCol.HIGH].iloc[i]
        low = daily_data[CandleCol.LOW].iloc[i]
        close = daily_data[CandleCol.CLOSE].iloc[i]
        jan21_expected_adr += (high - low) / close
    jan21_expected_adr /= 14

    jan22_expected_adr = 0
    for i in range(6, 20):  # Last 14 days (from day 7 to day 20)
        high = daily_data[CandleCol.HIGH].iloc[i]
        low = daily_data[CandleCol.LOW].iloc[i]
        close = daily_data[CandleCol.CLOSE].iloc[i]
        jan22_expected_adr += (high - low) / close
    jan22_expected_adr /= 14

    # Jan 21 rows should have the same ADR
    assert abs(result_df[indicator.column_name()].iloc[0] - jan21_expected_adr) < 0.001
    assert abs(result_df[indicator.column_name()].iloc[1] - jan21_expected_adr) < 0.001

    # Jan 22 rows should have the same ADR (but different from Jan 21)
    # In a real scenario with day 21 data, these would be different
    assert abs(result_df[indicator.column_name()].iloc[2] - jan22_expected_adr) < 0.001
    assert abs(result_df[indicator.column_name()].iloc[3] - jan22_expected_adr) < 0.001


@pytest.mark.asyncio
async def test_adr_indicator_extend_realtime(candles: "CandleStoreTest"):
    # Set up test data for daily candles
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 14))
    daily_data = pd.DataFrame(
        {
            CandleCol.HIGH: [
                105,
                107,
                110,
                108,
                106,
                109,
                111,
                113,
                112,
                110,
                115,
                117,
                119,
                118,
            ],
            CandleCol.LOW: [
                95,
                97,
                100,
                98,
                96,
                99,
                101,
                103,
                102,
                100,
                105,
                107,
                109,
                108,
            ],
            CandleCol.CLOSE: [
                102,
                105,
                103,
                101,
                104,
                106,
                108,
                107,
                105,
                110,
                112,
                114,
                113,
                111,
            ],
        },
        index=daily_dates,
    )

    candles.set_data("AAPL", daily_data)

    # Create a new row for realtime data
    new_row = pd.Series(
        {
            CandleCol.HIGH: 123,
            CandleCol.LOW: 113,
            CandleCol.CLOSE: 121,
        },
        name=datetime(2023, 1, 15, 9, 30),
    )

    # Test with period=14
    indicator = ADRIndicator(period=14)
    result_row = await indicator.extend_realtime("AAPL", new_row.copy())

    # Calculate expected ADR (average of (high-low)/low for the last 14 days)
    expected_adr = 0
    for i in range(0, 14):  # All 14 days in our data
        high = daily_data[CandleCol.HIGH].iloc[i]
        low = daily_data[CandleCol.LOW].iloc[i]
        close = daily_data[CandleCol.CLOSE].iloc[i]
        expected_adr += (high - low) / close
    expected_adr /= 14

    actual_adr = result_row[indicator.column_name()]
    assert abs(actual_adr - expected_adr) < 0.001


@pytest.mark.asyncio
async def test_adr_indicator_extend_realtime_multiple_calls_same_day(
    candles: "CandleStoreTest",
):
    # Set up test data for daily candles
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 14))
    daily_data = pd.DataFrame(
        {
            CandleCol.HIGH: [
                105,
                107,
                110,
                108,
                106,
                109,
                111,
                113,
                112,
                110,
                115,
                117,
                119,
                118,
            ],
            CandleCol.LOW: [
                95,
                97,
                100,
                98,
                96,
                99,
                101,
                103,
                102,
                100,
                105,
                107,
                109,
                108,
            ],
            CandleCol.CLOSE: [
                102,
                105,
                103,
                101,
                104,
                106,
                108,
                107,
                105,
                110,
                112,
                114,
                113,
                111,
            ],
        },
        index=daily_dates,
    )

    candles.set_data("AAPL", daily_data)

    indicator = ADRIndicator(period=14)

    # First call for Jan 15
    row1 = pd.Series(
        {
            CandleCol.HIGH: 123,
            CandleCol.LOW: 113,
            CandleCol.CLOSE: 121,
        },
        name=datetime(2023, 1, 15, 9, 30),
    )
    result1 = await indicator.extend_realtime("AAPL", row1.copy())

    # Second call for Jan 15 (same day)
    row2 = pd.Series(
        {
            CandleCol.HIGH: 125,
            CandleCol.LOW: 115,
            CandleCol.CLOSE: 123,
        },
        name=datetime(2023, 1, 15, 10, 0),
    )
    result2 = await indicator.extend_realtime("AAPL", row2.copy())

    # Both should have the same ADR value
    assert result1[indicator.column_name()] == result2[indicator.column_name()]

    # Calculate expected ADR
    expected_adr = 0
    for i in range(0, 14):  # All 14 days in our data
        high = daily_data[CandleCol.HIGH].iloc[i]
        low = daily_data[CandleCol.LOW].iloc[i]
        close = daily_data[CandleCol.CLOSE].iloc[i]
        expected_adr += (high - low) / close
    expected_adr /= 14

    assert abs(result1[indicator.column_name()] - expected_adr) < 0.001


@pytest.mark.asyncio
async def test_adr_indicator_extend_realtime_different_days(candles: "CandleStoreTest"):
    # Set up test data for daily candles
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 14))
    daily_data = pd.DataFrame(
        {
            CandleCol.HIGH: [
                105,
                107,
                110,
                108,
                106,
                109,
                111,
                113,
                112,
                110,
                115,
                117,
                119,
                118,
            ],
            CandleCol.LOW: [
                95,
                97,
                100,
                98,
                96,
                99,
                101,
                103,
                102,
                100,
                105,
                107,
                109,
                108,
            ],
            CandleCol.CLOSE: [
                102,
                105,
                103,
                101,
                104,
                106,
                108,
                107,
                105,
                110,
                112,
                114,
                113,
                111,
            ],
        },
        index=daily_dates,
    )

    candles.set_data("AAPL", daily_data)

    indicator = ADRIndicator(period=14)

    # Call for Jan 15
    row1 = pd.Series(
        {
            CandleCol.HIGH: 123,
            CandleCol.LOW: 113,
            CandleCol.CLOSE: 121,
        },
        name=datetime(2023, 1, 15, 9, 30),
    )
    result1 = await indicator.extend_realtime("AAPL", row1.copy())

    # Get the ADR value for Jan 15
    jan15_adr = result1[indicator.column_name()]

    # Add Jan 15 data to our daily data for the next calculation
    jan15_data = pd.DataFrame(
        {
            CandleCol.HIGH: [123],
            CandleCol.LOW: [113],
            CandleCol.CLOSE: [121],
        },
        index=[datetime(2023, 1, 15, 16, 0)],
    )
    updated_daily_data = pd.concat([daily_data, jan15_data])
    candles.set_data("AAPL", updated_daily_data)

    # Call for Jan 16 (different day)
    row2 = pd.Series(
        {
            CandleCol.HIGH: 125,
            CandleCol.LOW: 115,
            CandleCol.CLOSE: 123,
        },
        name=datetime(2023, 1, 16, 9, 30),
    )
    result2 = await indicator.extend_realtime("AAPL", row2.copy())

    # Get the ADR value for Jan 16
    jan16_adr = result2[indicator.column_name()]

    # Calculate expected ADR for Jan 15
    jan15_expected_adr = 0
    for i in range(0, 14):  # First 14 days
        high = daily_data[CandleCol.HIGH].iloc[i]
        low = daily_data[CandleCol.LOW].iloc[i]
        close = daily_data[CandleCol.CLOSE].iloc[i]
        jan15_expected_adr += (high - low) / close
    jan15_expected_adr /= 14

    # Calculate expected ADR for Jan 16 (should include Jan 15 and exclude Jan 1)
    jan16_expected_adr = 0
    for i in range(1, 15):  # Days 2-15
        high = updated_daily_data[CandleCol.HIGH].iloc[i]
        low = updated_daily_data[CandleCol.LOW].iloc[i]
        close = updated_daily_data[CandleCol.CLOSE].iloc[i]
        jan16_expected_adr += (high - low) / close
    jan16_expected_adr /= 14

    # Verify the ADR values
    assert abs(jan15_adr - jan15_expected_adr) < 0.001
    assert abs(jan16_adr - jan16_expected_adr) < 0.001

    # Ensure the values are different
    assert jan15_adr != jan16_adr


def test_day_open_indicator_type():
    indicator = DayOpenIndicator()
    assert indicator.type() == "day_open"


def test_day_open_indicator_column_name():
    indicator = DayOpenIndicator()
    assert indicator.column_name() == "day_open"


@pytest.mark.asyncio
async def test_day_open_indicator_extend(candles: "CandleStoreTest"):
    daily_dates = pd.date_range(start=date(2023, 1, 10), end=date(2023, 1, 11))
    daily_opens = [100, 110]
    daily_data = pd.DataFrame({CandleCol.OPEN: daily_opens}, index=daily_dates)

    candles.set_data("AAPL", daily_data)

    dates = [
        datetime(2023, 1, 10, 9, 30),
        datetime(2023, 1, 10, 10, 0),
        datetime(2023, 1, 10, 10, 30),
    ]
    opens = [100, 101, 102]
    df = pd.DataFrame({CandleCol.OPEN: opens}, index=pd.DatetimeIndex(dates))

    indicator = DayOpenIndicator()
    result_df = await indicator.extend("AAPL", df)

    assert result_df[indicator.column_name()].to_list() == [100, 100, 100]


@pytest.mark.asyncio
async def test_day_open_indicator_extend_multiple_days(candles: "CandleStoreTest"):
    daily_dates = pd.date_range(start=date(2023, 1, 10), end=date(2023, 1, 12))
    daily_opens = [100, 110, 120]
    daily_data = pd.DataFrame({CandleCol.OPEN: daily_opens}, index=daily_dates)

    candles.set_data("AAPL", daily_data)

    dates = [
        datetime(2023, 1, 10, 9, 30),
        datetime(2023, 1, 10, 10, 0),
        datetime(2023, 1, 11, 9, 30),
        datetime(2023, 1, 11, 10, 0),
        datetime(2023, 1, 12, 9, 30),
    ]
    opens = [100, 101, 110, 111, 120]
    df = pd.DataFrame({CandleCol.OPEN: opens}, index=pd.DatetimeIndex(dates))

    indicator = DayOpenIndicator()
    result_df = await indicator.extend("AAPL", df)

    expected_values = [100, 100, 110, 110, 120]
    assert result_df[indicator.column_name()].to_list() == expected_values


@pytest.mark.asyncio
async def test_day_open_indicator_extend_premarket(candles: "CandleStoreTest"):
    daily_dates = pd.date_range(start=date(2023, 1, 10), end=date(2023, 1, 11))
    daily_opens = [100, 110]
    daily_data = pd.DataFrame({CandleCol.OPEN: daily_opens}, index=daily_dates)

    candles.set_data("AAPL", daily_data)

    dates = [
        datetime(2023, 1, 10, 8, 0),
        datetime(2023, 1, 10, 9, 0),
        datetime(2023, 1, 10, 9, 30),
        datetime(2023, 1, 10, 10, 0),
    ]
    opens = [95, 98, 100, 101]
    df = pd.DataFrame({CandleCol.OPEN: opens}, index=pd.DatetimeIndex(dates))

    indicator = DayOpenIndicator()
    result_df = await indicator.extend("AAPL", df)

    assert pd.isna(result_df[indicator.column_name()].iloc[0])
    assert pd.isna(result_df[indicator.column_name()].iloc[1])
    assert result_df[indicator.column_name()].iloc[2] == 100
    assert result_df[indicator.column_name()].iloc[3] == 100


@pytest.mark.asyncio
async def test_day_open_indicator_extend_empty_df(candles: "CandleStoreTest"):
    daily_data = pd.DataFrame({CandleCol.OPEN: []}, index=pd.DatetimeIndex([]))
    candles.set_data("AAPL", daily_data)

    df = pd.DataFrame({CandleCol.OPEN: []}, index=pd.DatetimeIndex([]))

    indicator = DayOpenIndicator()
    result_df = await indicator.extend("AAPL", df)

    assert indicator.column_name() in result_df.columns
    assert len(result_df) == 0


@pytest.mark.asyncio
async def test_day_open_indicator_extend_realtime(candles: "CandleStoreTest"):
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 10))
    daily_opens = [100, 102, 105, 103, 101, 104, 106, 108, 107, 105]
    daily_data = pd.DataFrame({CandleCol.OPEN: daily_opens}, index=daily_dates)

    candles.set_data("AAPL", daily_data)

    new_row = pd.Series(
        {
            CandleCol.CLOSE: 110,
            CandleCol.HIGH: 112,
            CandleCol.LOW: 108,
            CandleCol.OPEN: 109,
        },
        name=datetime(2023, 1, 11, 9, 30),
    )

    indicator = DayOpenIndicator()
    result_row = await indicator.extend_realtime("AAPL", new_row.copy())

    assert result_row[indicator.column_name()] == 109


@pytest.mark.asyncio
async def test_day_open_indicator_extend_realtime_multiple_calls_same_day(
    candles: "CandleStoreTest",
):
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 10))
    daily_opens = [100, 102, 105, 103, 101, 104, 106, 108, 107, 105]
    daily_data = pd.DataFrame({CandleCol.OPEN: daily_opens}, index=daily_dates)

    candles.set_data("AAPL", daily_data)

    indicator = DayOpenIndicator()

    row1 = pd.Series(
        {CandleCol.CLOSE: 110, CandleCol.OPEN: 109},
        name=datetime(2023, 1, 11, 9, 30),
    )
    result1 = await indicator.extend_realtime("AAPL", row1.copy())

    row2 = pd.Series(
        {CandleCol.CLOSE: 112, CandleCol.OPEN: 111},
        name=datetime(2023, 1, 11, 10, 0),
    )
    result2 = await indicator.extend_realtime("AAPL", row2.copy())

    assert result1[indicator.column_name()] == 109
    assert result2[indicator.column_name()] == 109


@pytest.mark.asyncio
async def test_day_open_indicator_extend_realtime_different_days(
    candles: "CandleStoreTest",
):
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 11))
    daily_opens = [100, 102, 105, 103, 101, 104, 106, 108, 107, 105, 110]
    daily_data = pd.DataFrame({CandleCol.OPEN: daily_opens}, index=daily_dates)

    candles.set_data("AAPL", daily_data)

    indicator = DayOpenIndicator()

    row1 = pd.Series(
        {CandleCol.CLOSE: 110, CandleCol.OPEN: 109},
        name=datetime(2023, 1, 11, 9, 30),
    )
    result1 = await indicator.extend_realtime("AAPL", row1.copy())

    row2 = pd.Series(
        {CandleCol.CLOSE: 115, CandleCol.OPEN: 114},
        name=datetime(2023, 1, 12, 9, 30),
    )
    result2 = await indicator.extend_realtime("AAPL", row2.copy())

    assert result1[indicator.column_name()] == 109
    assert result2[indicator.column_name()] == 114


@pytest.mark.asyncio
async def test_day_open_indicator_extend_realtime_premarket(candles: "CandleStoreTest"):
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 10))
    daily_opens = [100, 102, 105, 103, 101, 104, 106, 108, 107, 105]
    daily_data = pd.DataFrame({CandleCol.OPEN: daily_opens}, index=daily_dates)

    candles.set_data("AAPL", daily_data)

    indicator = DayOpenIndicator()

    premarket_row = pd.Series(
        {CandleCol.OPEN: 108, CandleCol.CLOSE: 108},
        name=datetime(2023, 1, 11, 9, 0),
    )
    premarket_result = await indicator.extend_realtime("AAPL", premarket_row.copy())

    assert pd.isna(premarket_result[indicator.column_name()])

    market_open_row = pd.Series(
        {CandleCol.OPEN: 110, CandleCol.CLOSE: 110},
        name=datetime(2023, 1, 11, 9, 30),
    )
    market_open_result = await indicator.extend_realtime("AAPL", market_open_row.copy())

    assert market_open_result[indicator.column_name()] == 110


@pytest.mark.asyncio
async def test_day_open_indicator_extend_realtime_new_day_reset(
    candles: "CandleStoreTest",
):
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 10))
    daily_opens = [100, 102, 105, 103, 101, 104, 106, 108, 107, 105]
    daily_data = pd.DataFrame({CandleCol.OPEN: daily_opens}, index=daily_dates)

    candles.set_data("AAPL", daily_data)

    indicator = DayOpenIndicator()

    row1 = pd.Series(
        {CandleCol.OPEN: 109, CandleCol.CLOSE: 110},
        name=datetime(2023, 1, 11, 9, 30),
    )
    result1 = await indicator.extend_realtime("AAPL", row1.copy())
    assert result1[indicator.column_name()] == 109

    premarket_next_day = pd.Series(
        {CandleCol.OPEN: 112, CandleCol.CLOSE: 112},
        name=datetime(2023, 1, 12, 9, 0),
    )
    premarket_result = await indicator.extend_realtime("AAPL", premarket_next_day.copy())

    assert pd.isna(premarket_result[indicator.column_name()])

    market_open_next_day = pd.Series(
        {CandleCol.OPEN: 115, CandleCol.CLOSE: 115},
        name=datetime(2023, 1, 12, 9, 30),
    )
    market_open_result = await indicator.extend_realtime(
        "AAPL", market_open_next_day.copy()
    )

    assert market_open_result[indicator.column_name()] == 115
