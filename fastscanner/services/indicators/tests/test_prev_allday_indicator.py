from datetime import datetime

import pandas as pd
import pytest

from fastscanner.services.indicators.lib.daily import PrevAllDayIndicator
from fastscanner.services.indicators.ports import CandleCol
from fastscanner.services.indicators.tests.fixtures import CandleStoreTest, candles


def test_prev_allday_type():
    assert PrevAllDayIndicator.type() == "prev_allday"


def test_prev_allday_column_name():
    indicator = PrevAllDayIndicator(candle_col=CandleCol.HIGH)
    assert indicator.column_name() == "prev_allday_high"

    indicator = PrevAllDayIndicator(candle_col=CandleCol.VOLUME)
    assert indicator.column_name() == "prev_allday_volume"


@pytest.mark.asyncio
async def test_prev_allday_extend(candles: "CandleStoreTest"):
    # Minute candles for Jan 9 and Jan 10
    minute_dates = [
        datetime(2023, 1, 9, 9, 30),
        datetime(2023, 1, 9, 10, 0),
        datetime(2023, 1, 9, 10, 30),
        datetime(2023, 1, 10, 9, 30),
        datetime(2023, 1, 10, 10, 0),
        datetime(2023, 1, 10, 10, 30),
    ]
    minute_data = pd.DataFrame(
        {
            CandleCol.OPEN: [100, 102, 104, 106, 108, 110],
            CandleCol.HIGH: [105, 107, 109, 111, 113, 115],
            CandleCol.LOW: [95, 97, 99, 101, 103, 105],
            CandleCol.CLOSE: [102, 104, 106, 108, 110, 112],
            CandleCol.VOLUME: [1000, 1100, 1200, 1300, 1400, 1500],
        },
        index=pd.DatetimeIndex(minute_dates),
    )

    candles.set_data("AAPL", minute_data)

    # Intraday candles for Jan 11
    dates = [
        datetime(2023, 1, 11, 9, 30),
        datetime(2023, 1, 11, 10, 0),
    ]
    df = pd.DataFrame(
        {
            CandleCol.OPEN: [114, 116],
            CandleCol.HIGH: [119, 121],
            CandleCol.LOW: [109, 111],
            CandleCol.CLOSE: [116, 118],
            CandleCol.VOLUME: [1600, 1700],
        },
        index=pd.DatetimeIndex(dates),
    )

    indicator = PrevAllDayIndicator(candle_col=CandleCol.HIGH)
    result_df = await indicator.extend("AAPL", df)

    assert indicator.column_name() in result_df.columns
    # Jan 10 high across all minute candles: max(111, 113, 115) = 115
    assert result_df[indicator.column_name()].iloc[0] == 115
    assert result_df[indicator.column_name()].iloc[1] == 115


@pytest.mark.asyncio
async def test_prev_allday_extend_volume(candles: "CandleStoreTest"):
    minute_dates = [
        datetime(2023, 1, 10, 9, 30),
        datetime(2023, 1, 10, 10, 0),
        datetime(2023, 1, 10, 10, 30),
    ]
    minute_data = pd.DataFrame(
        {
            CandleCol.OPEN: [100, 102, 104],
            CandleCol.HIGH: [105, 107, 109],
            CandleCol.LOW: [95, 97, 99],
            CandleCol.CLOSE: [102, 104, 106],
            CandleCol.VOLUME: [1000, 1100, 1200],
        },
        index=pd.DatetimeIndex(minute_dates),
    )

    candles.set_data("AAPL", minute_data)

    dates = [datetime(2023, 1, 11, 9, 30)]
    df = pd.DataFrame(
        {
            CandleCol.OPEN: [108],
            CandleCol.HIGH: [113],
            CandleCol.LOW: [103],
            CandleCol.CLOSE: [110],
            CandleCol.VOLUME: [1300],
        },
        index=pd.DatetimeIndex(dates),
    )

    indicator = PrevAllDayIndicator(candle_col=CandleCol.VOLUME)
    result_df = await indicator.extend("AAPL", df)

    # Jan 10 volume sum: 1000 + 1100 + 1200 = 3300
    assert result_df[indicator.column_name()].iloc[0] == 3300


@pytest.mark.asyncio
async def test_prev_allday_extend_empty_data(candles: "CandleStoreTest"):
    candles.set_data(
        "AAPL",
        pd.DataFrame(
            index=pd.DatetimeIndex([]),
            columns=[CandleCol.OPEN, CandleCol.HIGH, CandleCol.LOW, CandleCol.CLOSE, CandleCol.VOLUME],
        ),
    )

    df = pd.DataFrame(
        {
            CandleCol.OPEN: [],
            CandleCol.HIGH: [],
            CandleCol.LOW: [],
            CandleCol.CLOSE: [],
            CandleCol.VOLUME: [],
        },
        index=pd.DatetimeIndex([]),
    )

    indicator = PrevAllDayIndicator(candle_col=CandleCol.HIGH)
    result_df = await indicator.extend("AAPL", df)

    assert indicator.column_name() in result_df.columns
    assert len(result_df) == 0


@pytest.mark.asyncio
async def test_prev_allday_extend_multiple_days(candles: "CandleStoreTest"):
    minute_dates = [
        datetime(2023, 1, 9, 9, 30),
        datetime(2023, 1, 9, 10, 0),
        datetime(2023, 1, 10, 9, 30),
        datetime(2023, 1, 10, 10, 0),
        datetime(2023, 1, 11, 9, 30),
        datetime(2023, 1, 11, 10, 0),
    ]
    minute_data = pd.DataFrame(
        {
            CandleCol.OPEN: [100, 102, 110, 112, 120, 122],
            CandleCol.HIGH: [105, 107, 115, 117, 125, 127],
            CandleCol.LOW: [95, 97, 105, 107, 115, 117],
            CandleCol.CLOSE: [102, 104, 112, 114, 122, 124],
            CandleCol.VOLUME: [1000, 1100, 1200, 1300, 1400, 1500],
        },
        index=pd.DatetimeIndex(minute_dates),
    )

    candles.set_data("AAPL", minute_data)

    dates = [
        datetime(2023, 1, 11, 9, 30),
        datetime(2023, 1, 12, 9, 30),
    ]
    df = pd.DataFrame(
        {
            CandleCol.OPEN: [120, 130],
            CandleCol.HIGH: [125, 135],
            CandleCol.LOW: [115, 125],
            CandleCol.CLOSE: [122, 132],
            CandleCol.VOLUME: [1400, 1600],
        },
        index=pd.DatetimeIndex(dates),
    )

    indicator = PrevAllDayIndicator(candle_col=CandleCol.HIGH)
    result_df = await indicator.extend("AAPL", df)

    # Jan 11: prev allday high = max of Jan 10 highs = max(115, 117) = 117
    assert result_df[indicator.column_name()].iloc[0] == 117
    # Jan 12: prev allday high = max of Jan 11 highs = max(125, 127) = 127
    assert result_df[indicator.column_name()].iloc[1] == 127


@pytest.mark.asyncio
async def test_prev_allday_extend_low(candles: "CandleStoreTest"):
    minute_dates = [
        datetime(2023, 1, 10, 9, 30),
        datetime(2023, 1, 10, 10, 0),
        datetime(2023, 1, 10, 10, 30),
    ]
    minute_data = pd.DataFrame(
        {
            CandleCol.OPEN: [100, 102, 104],
            CandleCol.HIGH: [105, 107, 109],
            CandleCol.LOW: [95, 97, 99],
            CandleCol.CLOSE: [102, 104, 106],
            CandleCol.VOLUME: [1000, 1100, 1200],
        },
        index=pd.DatetimeIndex(minute_dates),
    )

    candles.set_data("AAPL", minute_data)

    dates = [datetime(2023, 1, 11, 9, 30)]
    df = pd.DataFrame(
        {
            CandleCol.OPEN: [108],
            CandleCol.HIGH: [113],
            CandleCol.LOW: [103],
            CandleCol.CLOSE: [110],
            CandleCol.VOLUME: [1300],
        },
        index=pd.DatetimeIndex(dates),
    )

    indicator = PrevAllDayIndicator(candle_col=CandleCol.LOW)
    result_df = await indicator.extend("AAPL", df)

    # Jan 10 low across all minute candles: min(95, 97, 99) = 95
    assert result_df[indicator.column_name()].iloc[0] == 95
