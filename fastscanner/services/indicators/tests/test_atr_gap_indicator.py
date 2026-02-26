from datetime import date, datetime

import pandas as pd
import pytest

from fastscanner.pkg.candle import Candle
from fastscanner.services.indicators.lib.candle import ATRGapIndicator
from fastscanner.services.indicators.ports import CandleCol
from fastscanner.services.indicators.tests.fixtures import CandleStoreTest, candles


def test_atr_gap_type():
    assert ATRGapIndicator.type() == "atr_gap"


def test_atr_gap_column_name():
    indicator = ATRGapIndicator(period=14, candle_col=CandleCol.OPEN)
    assert indicator.column_name() == "atr_gap_open_14"

    indicator = ATRGapIndicator(period=5, candle_col=CandleCol.CLOSE)
    assert indicator.column_name() == "atr_gap_close_5"


@pytest.mark.asyncio
async def test_atr_gap_extend(candles: "CandleStoreTest"):
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 10))
    daily_data = pd.DataFrame(
        {
            CandleCol.HIGH: [105, 107, 110, 108, 106, 109, 111, 113, 112, 110],
            CandleCol.LOW: [95, 97, 100, 98, 96, 99, 101, 103, 102, 100],
            CandleCol.CLOSE: [102, 105, 103, 101, 104, 106, 108, 107, 105, 108],
        },
        index=daily_dates,
    )

    candles.set_data("AAPL", daily_data)

    dates = [
        datetime(2023, 1, 11, 9, 30),
        datetime(2023, 1, 11, 10, 0),
        datetime(2023, 1, 11, 10, 30),
    ]
    df = pd.DataFrame(
        {
            CandleCol.OPEN: [115, 116, 117],
            CandleCol.HIGH: [120, 121, 122],
            CandleCol.LOW: [110, 111, 112],
            CandleCol.CLOSE: [118, 119, 120],
        },
        index=pd.DatetimeIndex(dates),
    )

    indicator = ATRGapIndicator(period=5, candle_col=CandleCol.OPEN)
    result_df = await indicator.extend("AAPL", df)

    assert indicator.column_name() in result_df.columns
    assert not result_df[indicator.column_name()].isna().any()

    assert "daily_atr_5" not in result_df.columns
    assert "gap_open" not in result_df.columns
    assert "prev_day_close" not in result_df.columns


@pytest.mark.asyncio
async def test_atr_gap_extend_preserves_existing_columns(candles: "CandleStoreTest"):
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 10))
    daily_data = pd.DataFrame(
        {
            CandleCol.HIGH: [105, 107, 110, 108, 106, 109, 111, 113, 112, 110],
            CandleCol.LOW: [95, 97, 100, 98, 96, 99, 101, 103, 102, 100],
            CandleCol.CLOSE: [102, 105, 103, 101, 104, 106, 108, 107, 105, 108],
        },
        index=daily_dates,
    )

    candles.set_data("AAPL", daily_data)

    dates = [datetime(2023, 1, 11, 9, 30)]
    df = pd.DataFrame(
        {
            CandleCol.OPEN: [115],
            CandleCol.HIGH: [120],
            CandleCol.LOW: [110],
            CandleCol.CLOSE: [118],
            "daily_atr_5": [10.0],
        },
        index=pd.DatetimeIndex(dates),
    )

    indicator = ATRGapIndicator(period=5, candle_col=CandleCol.OPEN)
    result_df = await indicator.extend("AAPL", df)

    assert "daily_atr_5" in result_df.columns


@pytest.mark.asyncio
async def test_atr_gap_extend_calculation(candles: "CandleStoreTest"):
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

    dates = [datetime(2023, 1, 6, 9, 30)]
    df = pd.DataFrame(
        {
            CandleCol.OPEN: [145],
            CandleCol.HIGH: [150],
            CandleCol.LOW: [140],
            CandleCol.CLOSE: [148],
        },
        index=pd.DatetimeIndex(dates),
    )

    from fastscanner.services.indicators.lib.daily import DailyATRIndicator

    atr_ind = DailyATRIndicator(period=5)
    atr_df = await atr_ind.extend("AAPL", df.copy())
    atr_value = atr_df[atr_ind.column_name()].iloc[0]

    indicator = ATRGapIndicator(period=5, candle_col=CandleCol.OPEN)
    result_df = await indicator.extend("AAPL", df.copy())

    # atr_gap = gap * prev_day_close / atr = ((open - prev_close) / prev_close) * prev_close / atr
    # = (open - prev_close) / atr = (145 - 135) / atr_value
    expected = (145 - 135) / atr_value
    assert abs(result_df[indicator.column_name()].iloc[0] - expected) < 1e-4


@pytest.mark.asyncio
async def test_atr_gap_extend_realtime(candles: "CandleStoreTest"):
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

    indicator = ATRGapIndicator(period=5, candle_col=CandleCol.OPEN)

    row = Candle(
        {
            CandleCol.OPEN: 145,
            CandleCol.HIGH: 150,
            CandleCol.LOW: 140,
            CandleCol.CLOSE: 148,
        },
        timestamp=pd.Timestamp(2023, 1, 6, 9, 30),
    )
    result = await indicator.extend_realtime("AAPL", row)

    assert indicator.column_name() in result
    assert not pd.isna(result[indicator.column_name()])
    assert "daily_atr_5" not in result
    assert "gap_open" not in result
    assert "prev_day_close" not in result


@pytest.mark.asyncio
async def test_atr_gap_extend_realtime_same_day(candles: "CandleStoreTest"):
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

    indicator = ATRGapIndicator(period=5, candle_col=CandleCol.OPEN)

    row1 = Candle(
        {
            CandleCol.OPEN: 145,
            CandleCol.HIGH: 150,
            CandleCol.LOW: 140,
            CandleCol.CLOSE: 148,
        },
        timestamp=pd.Timestamp(2023, 1, 6, 9, 30),
    )
    result1 = await indicator.extend_realtime("AAPL", row1)

    row2 = Candle(
        {
            CandleCol.OPEN: 150,
            CandleCol.HIGH: 155,
            CandleCol.LOW: 145,
            CandleCol.CLOSE: 152,
        },
        timestamp=pd.Timestamp(2023, 1, 6, 10, 0),
    )
    result2 = await indicator.extend_realtime("AAPL", row2)

    assert not pd.isna(result1[indicator.column_name()])
    assert not pd.isna(result2[indicator.column_name()])


@pytest.mark.asyncio
async def test_atr_gap_extend_realtime_different_days(candles: "CandleStoreTest"):
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

    indicator = ATRGapIndicator(period=5, candle_col=CandleCol.OPEN)

    row1 = Candle(
        {
            CandleCol.OPEN: 150,
            CandleCol.HIGH: 155,
            CandleCol.LOW: 145,
            CandleCol.CLOSE: 152,
        },
        timestamp=pd.Timestamp(2023, 1, 6, 9, 30),
    )
    result1 = await indicator.extend_realtime("AAPL", row1)

    row2 = Candle(
        {
            CandleCol.OPEN: 155,
            CandleCol.HIGH: 160,
            CandleCol.LOW: 150,
            CandleCol.CLOSE: 158,
        },
        timestamp=pd.Timestamp(2023, 1, 7, 9, 30),
    )
    result2 = await indicator.extend_realtime("AAPL", row2)

    assert not pd.isna(result1[indicator.column_name()])
    assert not pd.isna(result2[indicator.column_name()])
    assert result1[indicator.column_name()] != result2[indicator.column_name()]


@pytest.mark.asyncio
async def test_atr_gap_extend_realtime_zero_atr(candles: "CandleStoreTest"):
    daily_dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 5))
    daily_data = pd.DataFrame(
        {
            CandleCol.HIGH: [100, 100, 100, 100, 100],
            CandleCol.LOW: [100, 100, 100, 100, 100],
            CandleCol.CLOSE: [100, 100, 100, 100, 100],
        },
        index=daily_dates,
    )

    candles.set_data("AAPL", daily_data)

    indicator = ATRGapIndicator(period=5, candle_col=CandleCol.OPEN)

    row = Candle(
        {
            CandleCol.OPEN: 105,
            CandleCol.HIGH: 105,
            CandleCol.LOW: 105,
            CandleCol.CLOSE: 105,
        },
        timestamp=pd.Timestamp(2023, 1, 6, 9, 30),
    )
    result = await indicator.extend_realtime("AAPL", row)

    assert pd.isna(result[indicator.column_name()])
