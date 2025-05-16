import asyncio
from datetime import date, datetime, time, timedelta

import pandas as pd
import pytest
import pytz

from fastscanner.pkg.datetime import LOCAL_TIMEZONE_STR
from fastscanner.services.indicators.lib.candle import (
    ATRIndicator,
    CumulativeDailyVolumeIndicator,
    PositionInRangeIndicator,
    PremarketCumulativeIndicator,
)
from fastscanner.services.indicators.lib.daily import (
    DailyATRGapIndicator,
    DailyATRIndicator,
    DailyGapIndicator,
    PrevDayIndicator,
)
from fastscanner.services.indicators.ports import CandleCol
from fastscanner.services.indicators.registry import ApplicationRegistry
from fastscanner.services.indicators.service import (
    CandleChannelHandler,
    SubscriptionHandler,
)
from fastscanner.services.indicators.tests.fixtures import (
    CandleStoreTest,
    MockFundamentalDataStore,
    MockPublicHolidaysStore,
)


class FakeCandleStore:
    def __init__(self):
        self._data = {}

    def set_data(self, symbol: str, df: pd.DataFrame):
        self._data[symbol] = df

    def get(self, symbol: str, start, end, freq: str):
        df = self._data.get(symbol, pd.DataFrame())
        return df[(df.index.date >= start) & (df.index.date <= end)]


class TestHandler(SubscriptionHandler):
    def __init__(self):
        self.received = []

    def handle(self, symbol: str, new_row: pd.Series):
        self.received.append((symbol, new_row))


def create_test_data(symbol: str, days: int = 15):
    index = pd.date_range("2023-01-01", periods=days, freq="D", tz=LOCAL_TIMEZONE_STR)
    df = pd.DataFrame(
        {
            CandleCol.OPEN: range(100, 100 + days),
            CandleCol.HIGH: range(110, 110 + days),
            CandleCol.LOW: range(90, 90 + days),
            CandleCol.CLOSE: range(105, 105 + days),
            CandleCol.VOLUME: [1000 + i * 10 for i in range(days)],
        },
        index=index,
    )
    store = FakeCandleStore()
    store.set_data(symbol, df)
    return store


def create_stream_message(
    ts: datetime, open=110, high=112, low=108, close=111, volume=1500
):
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=pytz.timezone(LOCAL_TIMEZONE_STR))
    return {
        "timestamp": str(int(ts.timestamp() * 1000)),
        CandleCol.OPEN: str(open),
        CandleCol.HIGH: str(high),
        CandleCol.LOW: str(low),
        CandleCol.CLOSE: str(close),
        CandleCol.VOLUME: str(volume),
    }


@pytest.fixture
def candles():
    candle_store = CandleStoreTest()
    fundamental_store = MockFundamentalDataStore()
    holiday_store = MockPublicHolidaysStore()

    ApplicationRegistry.init(
        candles=candle_store,
        fundamentals=fundamental_store,
        holidays=holiday_store,
    )
    yield candle_store
    ApplicationRegistry.reset()


@pytest.fixture
def setup(candles):
    symbol = "AAPL"
    ts = datetime(2023, 1, 11, 13, 0, tzinfo=pytz.timezone(LOCAL_TIMEZONE_STR))
    df = pd.DataFrame(
        {
            CandleCol.OPEN: range(100, 115),
            CandleCol.HIGH: range(110, 125),
            CandleCol.LOW: range(90, 105),
            CandleCol.CLOSE: range(105, 120),
            CandleCol.VOLUME: [1000 + i * 10 for i in range(15)],
        },
        index=pd.date_range("2023-01-01", periods=15, freq="D", tz=LOCAL_TIMEZONE_STR),
    )
    candles.set_data(symbol, df)
    handler = TestHandler()
    return symbol, ts, handler


@pytest.mark.asyncio
async def test_prev_day_indicator(setup):
    symbol, ts, handler = setup
    indicator = PrevDayIndicator(candle_col=CandleCol.CLOSE)

    df = await ApplicationRegistry.candles.get(
        symbol, date(2023, 1, 1), date(2023, 1, 10), "1D"
    )
    await indicator.extend(symbol, df)

    channel_handler = CandleChannelHandler(symbol, [indicator], handler)

    await channel_handler.handle(f"candles_min_{symbol}", create_stream_message(ts))

    assert len(handler.received) == 1
    _, row = handler.received[0]
    assert "prev_day_close" in row
    assert row["prev_day_close"] == 114


@pytest.mark.asyncio
async def test_daily_gap_indicator(setup):
    symbol, ts, handler = setup
    indicator = DailyGapIndicator()

    pre_market_ts = ts.replace(hour=9, minute=15)
    channel_handler = CandleChannelHandler(symbol, [indicator], handler)
    await channel_handler.handle(
        f"candles_min_{symbol}", create_stream_message(pre_market_ts)
    )
    assert len(handler.received) == 1
    _, row = handler.received[0]
    assert pd.isna(row["daily_gap"])

    handler.received = []

    await channel_handler.handle(f"candles_min_{symbol}", create_stream_message(ts))

    assert len(handler.received) == 1
    _, row = handler.received[0]
    assert "daily_gap" in row
    assert round(row["daily_gap"], 4) == round((110 - 114) / 114, 4)


@pytest.mark.asyncio
async def test_daily_atr_indicator(setup):
    symbol, ts, handler = setup
    indicator = DailyATRIndicator(period=3)

    df = await ApplicationRegistry.candles.get(
        symbol, date(2023, 1, 1), date(2023, 1, 10), "1D"
    )
    await indicator.extend(symbol, df)

    channel_handler = CandleChannelHandler(symbol, [indicator], handler)
    await channel_handler.handle(f"candles_min_{symbol}", create_stream_message(ts))

    assert len(handler.received) == 1
    _, row = handler.received[0]
    assert "daily_atr_3" in row
    assert row["daily_atr_3"] > 0


@pytest.mark.asyncio
async def test_daily_atr_gap_indicator(setup):
    symbol, ts, handler = setup
    indicator = DailyATRGapIndicator(period=3)

    channel_handler = CandleChannelHandler(symbol, [indicator], handler)
    await channel_handler.handle(f"candles_min_{symbol}", create_stream_message(ts))

    assert len(handler.received) == 1
    _, row = handler.received[0]
    assert "daily_atr_gap_3" in row
    assert not pd.isna(row["daily_atr_gap_3"])


def test_cumulative_daily_volume(setup):
    symbol, ts, handler = setup
    indicator = CumulativeDailyVolumeIndicator()

    channel_handler = CandleChannelHandler(symbol, [indicator], handler)
    asyncio.run(
        channel_handler.handle(
            f"candles_min_{symbol}", create_stream_message(ts, volume=1000)
        )
    )

    assert len(handler.received) == 1
    _, row = handler.received[0]
    assert row["cumulative_daily_volume"] == 1000

    handler.received = []
    asyncio.run(
        channel_handler.handle(
            f"candles_min_{symbol}",
            create_stream_message(ts + timedelta(minutes=1), volume=500),
        )
    )
    assert len(handler.received) == 1
    _, row = handler.received[0]
    assert row["cumulative_daily_volume"] == 1500


def test_premarket_cumulative(setup):
    symbol, ts, handler = setup
    indicator = PremarketCumulativeIndicator(CandleCol.CLOSE, op="sum")

    pre_market_ts = ts.replace(hour=9, minute=15)
    channel_handler = CandleChannelHandler(symbol, [indicator], handler)
    asyncio.run(
        channel_handler.handle(
            f"candles_min_{symbol}", create_stream_message(pre_market_ts, close=100)
        )
    )

    assert len(handler.received) == 1
    _, row = handler.received[0]
    assert row["premarket_total_close"] == 100

    handler.received = []
    asyncio.run(
        channel_handler.handle(
            f"candles_min_{symbol}",
            create_stream_message(pre_market_ts + timedelta(minutes=1), close=50),
        )
    )
    assert len(handler.received) == 1
    _, row = handler.received[0]
    assert row["premarket_total_close"] == 150

    handler.received = []
    asyncio.run(
        channel_handler.handle(
            f"candles_min_{symbol}", create_stream_message(ts, close=200)
        )
    )
    assert len(handler.received) == 1
    _, row = handler.received[0]
    assert row["premarket_total_close"] == 150


@pytest.mark.asyncio
async def test_atr_indicator(setup):
    symbol, _, handler = setup
    indicator = ATRIndicator(period=3, freq="1min")

    index = pd.date_range(
        "2023-01-11 12:56", periods=4, freq="T", tz=LOCAL_TIMEZONE_STR
    )
    historical_rows = [
        create_stream_message(index[0], open=100, high=110, low=90, close=105),
        create_stream_message(index[1], open=101, high=111, low=91, close=106),
        create_stream_message(index[2], open=102, high=112, low=92, close=107),
    ]

    for msg in historical_rows:
        row = pd.Series(
            {k: float(v) for k, v in msg.items() if k != "timestamp"},
            name=pd.to_datetime(int(msg["timestamp"]), unit="ms", utc=True).tz_convert(
                LOCAL_TIMEZONE_STR
            ),
        )
        await indicator.extend_realtime(symbol, row)

    ts = index[3]
    channel_handler = CandleChannelHandler(symbol, [indicator], handler)
    await channel_handler.handle(
        f"candles_min_{symbol}",
        create_stream_message(ts, open=103, high=113, low=93, close=108),
    )

    assert len(handler.received) == 1
    _, row = handler.received[0]
    assert "atr_3" in row
    assert not pd.isna(row["atr_3"]), f"ATR is NA: {row}"
    assert row["atr_3"] > 0, f"ATR is not > 0: {row}"


@pytest.mark.asyncio
async def test_position_in_range(setup):
    symbol, ts, handler = setup
    indicator = PositionInRangeIndicator(n_days=3)

    df = await ApplicationRegistry.candles.get(
        symbol, date(2023, 1, 1), date(2023, 1, 10), "1D"
    )
    await indicator.extend(symbol, df)

    channel_handler = CandleChannelHandler(symbol, [indicator], handler)

    await channel_handler.handle(
        f"candles_min_{symbol}", create_stream_message(ts, close=112)
    )

    assert len(handler.received) == 1
    _, row = handler.received[0]
    assert "position_in_range_3" in row
    assert 0 <= row["position_in_range_3"] <= 1


def test_channel_handler_missing_timestamp(setup):
    symbol, _, handler = setup
    channel_handler = CandleChannelHandler(symbol, [], handler)

    asyncio.run(channel_handler.handle(f"candles_min_{symbol}", {}))
    assert len(handler.received) == 0


def test_channel_handler_invalid_data(setup):
    symbol, _, handler = setup
    channel_handler = CandleChannelHandler(symbol, [], handler)

    asyncio.run(
        channel_handler.handle(
            f"candles_min_{symbol}", {"timestamp": "invalid", "open": "not_a_number"}
        )
    )
    assert len(handler.received) == 0


@pytest.mark.asyncio
async def test_channel_handler_multiple_indicators(setup):
    symbol, ts, handler = setup
    indicators = [
        PrevDayIndicator(candle_col=CandleCol.CLOSE),
        DailyGapIndicator(),
        CumulativeDailyVolumeIndicator(),
    ]

    df = await ApplicationRegistry.candles.get(
        symbol, date(2023, 1, 1), date(2023, 1, 10), "1D"
    )
    for indicator in indicators:
        if hasattr(indicator, "extend"):
            await indicator.extend(symbol, df)

    channel_handler = CandleChannelHandler(symbol, indicators, handler)

    await channel_handler.handle(f"candles_min_{symbol}", create_stream_message(ts))

    assert len(handler.received) == 1
    _, row = handler.received[0]
    assert "prev_day_close" in row
    assert "daily_gap" in row
    assert "cumulative_daily_volume" in row
