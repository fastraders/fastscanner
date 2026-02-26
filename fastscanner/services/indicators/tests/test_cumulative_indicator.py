from datetime import datetime

import pandas as pd
import pytest

from fastscanner.pkg.candle import Candle
from fastscanner.services.indicators.lib.candle import CumulativeIndicator
from fastscanner.services.indicators.ports import CandleCol


def test_cumulative_type():
    assert CumulativeIndicator.type() == "cumulative"


def test_cumulative_column_name():
    indicator = CumulativeIndicator(candle_col=CandleCol.HIGH, op="max")
    assert indicator.column_name() == "highest_high"

    indicator = CumulativeIndicator(candle_col=CandleCol.LOW, op="min")
    assert indicator.column_name() == "lowest_low"

    indicator = CumulativeIndicator(candle_col=CandleCol.VOLUME, op="sum")
    assert indicator.column_name() == "total_volume"


@pytest.mark.asyncio
async def test_cumulative_extend_max_single_day():
    dates = [
        datetime(2023, 1, 1, 9, 30),
        datetime(2023, 1, 1, 10, 0),
        datetime(2023, 1, 1, 10, 30),
        datetime(2023, 1, 1, 11, 0),
    ]
    highs = [100, 105, 103, 110]
    df = pd.DataFrame({CandleCol.HIGH: highs}, index=pd.DatetimeIndex(dates))

    indicator = CumulativeIndicator(candle_col=CandleCol.HIGH, op="max")
    result_df = await indicator.extend("AAPL", df)

    expected = [100, 105, 105, 110]
    assert result_df[indicator.column_name()].tolist() == expected


@pytest.mark.asyncio
async def test_cumulative_extend_min_single_day():
    dates = [
        datetime(2023, 1, 1, 9, 30),
        datetime(2023, 1, 1, 10, 0),
        datetime(2023, 1, 1, 10, 30),
        datetime(2023, 1, 1, 11, 0),
    ]
    lows = [100, 95, 97, 90]
    df = pd.DataFrame({CandleCol.LOW: lows}, index=pd.DatetimeIndex(dates))

    indicator = CumulativeIndicator(candle_col=CandleCol.LOW, op="min")
    result_df = await indicator.extend("AAPL", df)

    expected = [100, 95, 95, 90]
    assert result_df[indicator.column_name()].tolist() == expected


@pytest.mark.asyncio
async def test_cumulative_extend_sum_single_day():
    dates = [
        datetime(2023, 1, 1, 9, 30),
        datetime(2023, 1, 1, 10, 0),
        datetime(2023, 1, 1, 10, 30),
        datetime(2023, 1, 1, 11, 0),
    ]
    volumes = [100, 150, 200, 250]
    df = pd.DataFrame({CandleCol.VOLUME: volumes}, index=pd.DatetimeIndex(dates))

    indicator = CumulativeIndicator(candle_col=CandleCol.VOLUME, op="sum")
    result_df = await indicator.extend("AAPL", df)

    expected = [100, 250, 450, 700]
    assert result_df[indicator.column_name()].tolist() == expected


@pytest.mark.asyncio
async def test_cumulative_extend_multiple_days():
    dates = [
        datetime(2023, 1, 1, 9, 30),
        datetime(2023, 1, 1, 10, 0),
        datetime(2023, 1, 2, 9, 30),
        datetime(2023, 1, 2, 10, 0),
    ]
    highs = [100, 110, 105, 115]
    df = pd.DataFrame({CandleCol.HIGH: highs}, index=pd.DatetimeIndex(dates))

    indicator = CumulativeIndicator(candle_col=CandleCol.HIGH, op="max")
    result_df = await indicator.extend("AAPL", df)

    # Resets per day
    expected = [100, 110, 105, 115]
    assert result_df[indicator.column_name()].tolist() == expected


@pytest.mark.asyncio
async def test_cumulative_extend_realtime_max():
    indicator = CumulativeIndicator(candle_col=CandleCol.HIGH, op="max")

    row1 = Candle({CandleCol.HIGH: 100}, timestamp=pd.Timestamp(2023, 1, 1, 9, 30))
    result1 = await indicator.extend_realtime("AAPL", row1)
    assert result1[indicator.column_name()] == 100

    row2 = Candle({CandleCol.HIGH: 105}, timestamp=pd.Timestamp(2023, 1, 1, 10, 0))
    result2 = await indicator.extend_realtime("AAPL", row2)
    assert result2[indicator.column_name()] == 105

    row3 = Candle({CandleCol.HIGH: 102}, timestamp=pd.Timestamp(2023, 1, 1, 10, 30))
    result3 = await indicator.extend_realtime("AAPL", row3)
    assert result3[indicator.column_name()] == 105


@pytest.mark.asyncio
async def test_cumulative_extend_realtime_min():
    indicator = CumulativeIndicator(candle_col=CandleCol.LOW, op="min")

    row1 = Candle({CandleCol.LOW: 100}, timestamp=pd.Timestamp(2023, 1, 1, 9, 30))
    result1 = await indicator.extend_realtime("AAPL", row1)
    assert result1[indicator.column_name()] == 100

    row2 = Candle({CandleCol.LOW: 95}, timestamp=pd.Timestamp(2023, 1, 1, 10, 0))
    result2 = await indicator.extend_realtime("AAPL", row2)
    assert result2[indicator.column_name()] == 95

    row3 = Candle({CandleCol.LOW: 98}, timestamp=pd.Timestamp(2023, 1, 1, 10, 30))
    result3 = await indicator.extend_realtime("AAPL", row3)
    assert result3[indicator.column_name()] == 95


@pytest.mark.asyncio
async def test_cumulative_extend_realtime_sum():
    indicator = CumulativeIndicator(candle_col=CandleCol.VOLUME, op="sum")

    row1 = Candle({CandleCol.VOLUME: 100}, timestamp=pd.Timestamp(2023, 1, 1, 9, 30))
    result1 = await indicator.extend_realtime("AAPL", row1)
    assert result1[indicator.column_name()] == 100

    row2 = Candle({CandleCol.VOLUME: 150}, timestamp=pd.Timestamp(2023, 1, 1, 10, 0))
    result2 = await indicator.extend_realtime("AAPL", row2)
    assert result2[indicator.column_name()] == 250

    row3 = Candle({CandleCol.VOLUME: 200}, timestamp=pd.Timestamp(2023, 1, 1, 10, 30))
    result3 = await indicator.extend_realtime("AAPL", row3)
    assert result3[indicator.column_name()] == 450


@pytest.mark.asyncio
async def test_cumulative_extend_realtime_multiple_symbols():
    indicator = CumulativeIndicator(candle_col=CandleCol.HIGH, op="max")

    aapl_row = Candle({CandleCol.HIGH: 100}, timestamp=pd.Timestamp(2023, 1, 1, 9, 30))
    aapl_result = await indicator.extend_realtime("AAPL", aapl_row)
    assert aapl_result[indicator.column_name()] == 100

    msft_row = Candle({CandleCol.HIGH: 200}, timestamp=pd.Timestamp(2023, 1, 1, 9, 30))
    msft_result = await indicator.extend_realtime("MSFT", msft_row)
    assert msft_result[indicator.column_name()] == 200

    aapl_row2 = Candle({CandleCol.HIGH: 110}, timestamp=pd.Timestamp(2023, 1, 1, 10, 0))
    aapl_result2 = await indicator.extend_realtime("AAPL", aapl_row2)
    assert aapl_result2[indicator.column_name()] == 110

    assert indicator._last_value["MSFT"] == 200


@pytest.mark.asyncio
async def test_cumulative_extend_realtime_first_candle_no_prior():
    indicator = CumulativeIndicator(candle_col=CandleCol.HIGH, op="max")

    row = Candle({CandleCol.HIGH: 50}, timestamp=pd.Timestamp(2023, 1, 1, 9, 30))
    result = await indicator.extend_realtime("AAPL", row)

    assert result[indicator.column_name()] == 50
