from datetime import date, datetime

import pandas as pd
import pytest

from fastscanner.pkg.candle import Candle
from fastscanner.services.indicators.lib.candle import GapIndicator
from fastscanner.services.indicators.ports import CandleCol
from fastscanner.services.indicators.tests.fixtures import CandleStoreTest, candles


def test_gap_type():
    assert GapIndicator.type() == "gap"


def test_gap_column_name():
    indicator = GapIndicator(candle_col=CandleCol.OPEN)
    assert indicator.column_name() == "gap_open"

    indicator = GapIndicator(candle_col=CandleCol.CLOSE)
    assert indicator.column_name() == "gap_close"

    indicator = GapIndicator(candle_col=CandleCol.HIGH)
    assert indicator.column_name() == "gap_high"


@pytest.mark.asyncio
async def test_gap_extend(candles: "CandleStoreTest"):
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 10))
    daily_closes = [100, 102, 105, 103, 101, 104, 106, 108, 107, 105]
    daily_data = pd.DataFrame({CandleCol.CLOSE: daily_closes}, index=daily_dates)

    candles.set_data("AAPL", daily_data)

    dates = [
        datetime(2023, 1, 11, 9, 30),
        datetime(2023, 1, 11, 10, 0),
        datetime(2023, 1, 11, 10, 30),
    ]
    opens = [110.0, 112.0, 115.0]
    df = pd.DataFrame(
        {CandleCol.OPEN: opens, CandleCol.CLOSE: [109, 111, 114]},
        index=pd.DatetimeIndex(dates),
    )

    indicator = GapIndicator(candle_col=CandleCol.OPEN)
    result_df = await indicator.extend("AAPL", df)

    assert indicator.column_name() in result_df.columns
    # gap_open = (open - prev_day_close) / prev_day_close
    # prev_day_close = 105
    expected = [(110 - 105) / 105, (112 - 105) / 105, (115 - 105) / 105]
    for actual, exp in zip(result_df[indicator.column_name()].tolist(), expected):
        assert abs(actual - exp) < 1e-6

    assert "prev_day_close" not in result_df.columns


@pytest.mark.asyncio
async def test_gap_extend_does_not_drop_existing_prev_day(candles: "CandleStoreTest"):
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 10))
    daily_closes = [100, 102, 105, 103, 101, 104, 106, 108, 107, 105]
    daily_data = pd.DataFrame({CandleCol.CLOSE: daily_closes}, index=daily_dates)

    candles.set_data("AAPL", daily_data)

    dates = [datetime(2023, 1, 11, 9, 30)]
    df = pd.DataFrame(
        {CandleCol.OPEN: [110.0], CandleCol.CLOSE: [109.0], "prev_day_close": [105.0]},
        index=pd.DatetimeIndex(dates),
    )

    indicator = GapIndicator(candle_col=CandleCol.OPEN)
    result_df = await indicator.extend("AAPL", df)

    assert "prev_day_close" in result_df.columns


@pytest.mark.asyncio
async def test_gap_extend_multiple_days(candles: "CandleStoreTest"):
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 11))
    daily_closes = [100, 102, 105, 103, 101, 104, 106, 108, 107, 105, 110]
    daily_data = pd.DataFrame({CandleCol.CLOSE: daily_closes}, index=daily_dates)

    candles.set_data("AAPL", daily_data)

    dates = [
        datetime(2023, 1, 11, 9, 30),
        datetime(2023, 1, 11, 10, 0),
        datetime(2023, 1, 12, 9, 30),
        datetime(2023, 1, 12, 10, 0),
    ]
    opens = [110, 112, 115, 117]
    df = pd.DataFrame(
        {CandleCol.OPEN: opens, CandleCol.CLOSE: [109, 111, 114, 116]},
        index=pd.DatetimeIndex(dates),
    )

    indicator = GapIndicator(candle_col=CandleCol.OPEN)
    result_df = await indicator.extend("AAPL", df)

    # Jan 11: prev_day_close = 105
    assert abs(result_df[indicator.column_name()].iloc[0] - (110 - 105) / 105) < 1e-6
    assert abs(result_df[indicator.column_name()].iloc[1] - (112 - 105) / 105) < 1e-6
    # Jan 12: prev_day_close = 110
    assert abs(result_df[indicator.column_name()].iloc[2] - (115 - 110) / 110) < 1e-6
    assert abs(result_df[indicator.column_name()].iloc[3] - (117 - 110) / 110) < 1e-6


@pytest.mark.asyncio
async def test_gap_extend_realtime(candles: "CandleStoreTest"):
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 10))
    daily_closes = [100, 102, 105, 103, 101, 104, 106, 108, 107, 105]
    daily_data = pd.DataFrame({CandleCol.CLOSE: daily_closes}, index=daily_dates)

    candles.set_data("AAPL", daily_data)

    indicator = GapIndicator(candle_col=CandleCol.OPEN)

    row = Candle(
        {CandleCol.OPEN: 110.0, CandleCol.CLOSE: 109.0},
        timestamp=pd.Timestamp(2023, 1, 11, 9, 30),
    )
    result = await indicator.extend_realtime("AAPL", row)

    expected_gap = (110 - 105) / 105
    assert abs(result[indicator.column_name()] - expected_gap) < 1e-6
    assert "prev_day_close" not in result


@pytest.mark.asyncio
async def test_gap_extend_realtime_same_day(candles: "CandleStoreTest"):
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 10))
    daily_closes = [100, 102, 105, 103, 101, 104, 106, 108, 107, 105]
    daily_data = pd.DataFrame({CandleCol.CLOSE: daily_closes}, index=daily_dates)

    candles.set_data("AAPL", daily_data)

    indicator = GapIndicator(candle_col=CandleCol.CLOSE)

    row1 = Candle(
        {CandleCol.CLOSE: 110.0},
        timestamp=pd.Timestamp(2023, 1, 11, 9, 30),
    )
    result1 = await indicator.extend_realtime("AAPL", row1)

    row2 = Candle(
        {CandleCol.CLOSE: 115.0},
        timestamp=pd.Timestamp(2023, 1, 11, 10, 0),
    )
    result2 = await indicator.extend_realtime("AAPL", row2)

    # Both should reference prev_day_close = 105
    assert abs(result1[indicator.column_name()] - (110 - 105) / 105) < 1e-6
    assert abs(result2[indicator.column_name()] - (115 - 105) / 105) < 1e-6


@pytest.mark.asyncio
async def test_gap_extend_realtime_different_days(candles: "CandleStoreTest"):
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 11))
    daily_closes = [100, 102, 105, 103, 101, 104, 106, 108, 107, 105, 110]
    daily_data = pd.DataFrame({CandleCol.CLOSE: daily_closes}, index=daily_dates)

    candles.set_data("AAPL", daily_data)

    indicator = GapIndicator(candle_col=CandleCol.OPEN)

    row1 = Candle(
        {CandleCol.OPEN: 110.0, CandleCol.CLOSE: 109.0},
        timestamp=pd.Timestamp(2023, 1, 11, 9, 30),
    )
    result1 = await indicator.extend_realtime("AAPL", row1)

    row2 = Candle(
        {CandleCol.OPEN: 115.0, CandleCol.CLOSE: 114.0},
        timestamp=pd.Timestamp(2023, 1, 12, 9, 30),
    )
    result2 = await indicator.extend_realtime("AAPL", row2)

    assert abs(result1[indicator.column_name()] - (110 - 105) / 105) < 1e-6
    assert abs(result2[indicator.column_name()] - (115 - 110) / 110) < 1e-6


@pytest.mark.asyncio
async def test_gap_extend_realtime_preserves_existing_prev_day(candles: "CandleStoreTest"):
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 10))
    daily_closes = [100, 102, 105, 103, 101, 104, 106, 108, 107, 105]
    daily_data = pd.DataFrame({CandleCol.CLOSE: daily_closes}, index=daily_dates)

    candles.set_data("AAPL", daily_data)

    indicator = GapIndicator(candle_col=CandleCol.OPEN)

    row = Candle(
        {CandleCol.OPEN: 110.0, CandleCol.CLOSE: 109.0, "prev_day_close": 105.0},
        timestamp=pd.Timestamp(2023, 1, 11, 9, 30),
    )
    result = await indicator.extend_realtime("AAPL", row)

    assert "prev_day_close" in result


@pytest.mark.asyncio
async def test_gap_extend_realtime_multiple_symbols(candles: "CandleStoreTest"):
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 10))
    aapl_data = pd.DataFrame(
        {CandleCol.CLOSE: [100, 102, 105, 103, 101, 104, 106, 108, 107, 105]},
        index=daily_dates,
    )
    msft_data = pd.DataFrame(
        {CandleCol.CLOSE: [200, 202, 205, 203, 201, 204, 206, 208, 207, 210]},
        index=daily_dates,
    )

    candles.set_data("AAPL", aapl_data)
    candles.set_data("MSFT", msft_data)

    indicator = GapIndicator(candle_col=CandleCol.OPEN)

    aapl_row = Candle(
        {CandleCol.OPEN: 110.0, CandleCol.CLOSE: 109.0},
        timestamp=pd.Timestamp(2023, 1, 11, 9, 30),
    )
    aapl_result = await indicator.extend_realtime("AAPL", aapl_row)

    msft_row = Candle(
        {CandleCol.OPEN: 220.0, CandleCol.CLOSE: 219.0},
        timestamp=pd.Timestamp(2023, 1, 11, 9, 30),
    )
    msft_result = await indicator.extend_realtime("MSFT", msft_row)

    assert abs(aapl_result[indicator.column_name()] - (110 - 105) / 105) < 1e-6
    assert abs(msft_result[indicator.column_name()] - (220 - 210) / 210) < 1e-6
