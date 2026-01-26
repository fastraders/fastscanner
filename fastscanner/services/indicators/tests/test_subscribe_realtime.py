import asyncio
from datetime import date, datetime, timedelta
from unittest.mock import MagicMock

import pandas as pd
import pytest
import pytz
from httpx import patch

from fastscanner.pkg.clock import LOCAL_TIMEZONE_STR, ClockRegistry, FixedClock
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
from fastscanner.services.indicators.service import (
    CandleBuffer,
    CandleChannelHandler,
    SubscriptionHandler,
)
from fastscanner.services.indicators.tests.fixtures import (
    CandleStoreTest,
    MockCache,
    MockFundamentalDataStore,
    MockPublicHolidaysStore,
)
from fastscanner.services.registry import ApplicationRegistry


class FakeCandleStore:
    def __init__(self):
        self._data = {}

    def set_data(self, symbol: str, df: pd.DataFrame):
        self._data[symbol] = df

    def get(self, symbol: str, start, end, freq: str):
        df = self._data.get(symbol, pd.DataFrame())
        return df[(df.index.date >= start) & (df.index.date <= end)]


class HandlerTest(SubscriptionHandler):
    def __init__(self):
        self.received = []

    async def handle(self, symbol: str, new_row: pd.Series):
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
        CandleCol.OPEN: open,
        CandleCol.HIGH: high,
        CandleCol.LOW: low,
        CandleCol.CLOSE: close,
        CandleCol.VOLUME: volume,
    }


@pytest.fixture
def candles():
    candle_store = CandleStoreTest()
    fundamental_store = MockFundamentalDataStore()
    holiday_store = MockPublicHolidaysStore()
    cache = MockCache()

    ApplicationRegistry.init(
        candles=candle_store,
        fundamentals=fundamental_store,
        holidays=holiday_store,
        cache=cache,
    )
    yield candle_store
    ApplicationRegistry.reset()


@pytest.fixture
def setup(candles):
    symbol = "AAPL"
    ts = datetime(2023, 1, 11, 13, 0, tzinfo=pytz.timezone(LOCAL_TIMEZONE_STR))
    ClockRegistry.set(FixedClock(ts))
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
    handler = HandlerTest()
    return symbol, ts, handler


@pytest.mark.asyncio
async def test_prev_day_indicator(setup):
    symbol, ts, handler = setup
    indicator = PrevDayIndicator(candle_col=CandleCol.CLOSE)

    df = await ApplicationRegistry.candles.get(
        symbol, date(2023, 1, 1), date(2023, 1, 10), "1D"
    )
    await indicator.extend(symbol, df)

    ts_pd = pd.Timestamp(ts)

    mock_clock = MagicMock()
    mock_clock.now.return_value = ts_pd.floor("1min") + pd.Timedelta(
        seconds=59, milliseconds=990
    )
    ClockRegistry.set(mock_clock)
    unsubscribe = MagicMock()

    channel_handler = CandleChannelHandler([indicator], handler, "1min", unsubscribe)

    await channel_handler.handle(f"candles.min.{symbol}", create_stream_message(ts))

    ClockRegistry.unset()

    assert len(handler.received) == 1
    _, row = handler.received[0]
    assert "prev_day_close" in row
    assert row["prev_day_close"] == 114


@pytest.mark.asyncio
async def test_daily_gap_indicator(setup):
    symbol, ts, handler = setup
    indicator = DailyGapIndicator()

    pre_market_ts = ts.replace(hour=9, minute=15)
    channel_handler = CandleChannelHandler([indicator], handler, "1min", MagicMock())
    await channel_handler.handle(
        f"candles.min.{symbol}", create_stream_message(pre_market_ts)
    )
    assert len(handler.received) == 1
    _, row = handler.received[0]
    assert pd.isna(row["daily_gap"])

    handler.received = []

    await channel_handler.handle(f"candles.min.{symbol}", create_stream_message(ts))
    await asyncio.sleep(0.2)
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

    channel_handler = CandleChannelHandler([indicator], handler, "1min", MagicMock())
    await channel_handler.handle(f"candles.min.{symbol}", create_stream_message(ts))

    assert len(handler.received) == 1
    _, row = handler.received[0]
    assert "daily_atr_3" in row
    assert row["daily_atr_3"] > 0


@pytest.mark.asyncio
async def test_daily_atr_gap_indicator(setup):
    symbol, ts, handler = setup
    indicator = DailyATRGapIndicator(period=3)

    channel_handler = CandleChannelHandler([indicator], handler, "1min", MagicMock())
    await channel_handler.handle(f"candles.min.{symbol}", create_stream_message(ts))

    assert len(handler.received) == 1
    _, row = handler.received[0]
    assert "daily_atr_gap_3" in row
    assert not pd.isna(row["daily_atr_gap_3"])


@pytest.mark.asyncio
async def test_cumulative_daily_volume(setup):
    symbol, ts, handler = setup
    indicator = CumulativeDailyVolumeIndicator()

    channel_handler = CandleChannelHandler([indicator], handler, "1min", MagicMock())
    await channel_handler.handle(
        f"candles.min.{symbol}", create_stream_message(ts, volume=1000)
    )

    assert len(handler.received) == 1
    _, row = handler.received[0]
    assert row["cumulative_daily_volume"] == 1000

    handler.received = []
    await channel_handler.handle(
        f"candles.min.{symbol}",
        create_stream_message(ts + timedelta(minutes=1), volume=500),
    )
    await asyncio.sleep(0.2)
    assert len(handler.received) == 1
    _, row = handler.received[0]
    assert row["cumulative_daily_volume"] == 1500


@pytest.mark.asyncio
async def test_premarket_cumulative(setup):
    symbol, ts, handler = setup
    indicator = PremarketCumulativeIndicator(CandleCol.CLOSE, op="sum")

    pre_market_ts = ts.replace(hour=9, minute=15)
    channel_handler = CandleChannelHandler([indicator], handler, "1min", MagicMock())
    await channel_handler.handle(
        f"candles.min.{symbol}", create_stream_message(pre_market_ts, close=100)
    )

    assert len(handler.received) == 1
    _, row = handler.received[0]
    assert row["premarket_total_close"] == 100

    handler.received = []
    await channel_handler.handle(
        f"candles.min.{symbol}",
        create_stream_message(pre_market_ts + timedelta(minutes=1), close=50),
    )
    await asyncio.sleep(0.2)
    assert len(handler.received) == 1
    _, row = handler.received[0]
    assert row["premarket_total_close"] == 150

    handler.received = []
    await channel_handler.handle(
        f"candles.min.{symbol}", create_stream_message(ts, close=200)
    )
    await asyncio.sleep(0.2)
    assert len(handler.received) == 1
    _, row = handler.received[0]
    assert row["premarket_total_close"] == 150


@pytest.mark.asyncio
async def test_atr_indicator(setup):
    symbol, _, handler = setup
    indicator = ATRIndicator(period=3, freq="1min")

    index = pd.date_range(
        "2023-01-11 12:56", periods=4, freq="1min", tz=LOCAL_TIMEZONE_STR
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
    channel_handler = CandleChannelHandler([indicator], handler, "1min", MagicMock())
    await channel_handler.handle(
        f"candles.min.{symbol}",
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

    channel_handler = CandleChannelHandler([indicator], handler, "1min", MagicMock())

    await channel_handler.handle(
        f"candles.min.{symbol}", create_stream_message(ts, close=112)
    )

    assert len(handler.received) == 1
    _, row = handler.received[0]
    assert "position_in_range_3" in row
    assert 0 <= row["position_in_range_3"] <= 1


@pytest.mark.asyncio
async def test_channel_handler_missing_timestamp(setup):
    symbol, _, handler = setup
    channel_handler = CandleChannelHandler([], handler, "1min", MagicMock())

    try:
        await channel_handler.handle(f"candles.min.{symbol}", {})
        assert False, "Expected KeyError"
    except KeyError:
        ...


@pytest.mark.asyncio
async def test_channel_handler_invalid_data(setup):
    symbol, _, handler = setup
    channel_handler = CandleChannelHandler([], handler, "1min", MagicMock())

    try:
        await channel_handler.handle(
            f"candles.min.{symbol}", {"timestamp": "invalid", "open": "not_a_number"}
        )
        assert False, "Expected ValueError"
    except ValueError:
        ...


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

    channel_handler = CandleChannelHandler(indicators, handler, "1min", MagicMock())

    await channel_handler.handle(f"candles.min.{symbol}", create_stream_message(ts))

    assert len(handler.received) == 1
    _, row = handler.received[0]
    assert "prev_day_close" in row
    assert "daily_gap" in row
    assert "cumulative_daily_volume" in row


@pytest.mark.asyncio
async def test_multiple_ticks_aggregation():
    handler = HandlerTest()
    symbol = "AAPL"
    base_ts = pd.Timestamp("2023-01-01 10:00:00", tz=LOCAL_TIMEZONE_STR)

    # 3min frequency - should buffer until 3 ticks arrive
    channel_handler = CandleChannelHandler([], handler, "3min", MagicMock())

    # Send 3 ticks at 1 minute intervals
    for i in range(3):
        ts = base_ts + pd.Timedelta(minutes=i)
        data = {
            "timestamp": ts.value // 10**6,
            "open": 100.0 + i,
            "high": 101.0 + i,
            "low": 99.0 - i,
            "close": 100.5 + i,
            "volume": 1000 * (i + 1),
        }
        await channel_handler.handle(f"candles.min.{symbol}", data)

    # Should have one aggregated candle
    assert len(handler.received) == 1
    _, candle = handler.received[0]
    assert candle["open"] == 100.0  # first tick's open
    assert candle["close"] == 102.5  # last tick's close
    assert candle["high"] == 103.0  # max high
    assert candle["low"] == 97.0  # min low
    assert candle["volume"] == 6000  # sum of volumes (1000+2000+3000)


@pytest.mark.asyncio
async def test_candle_aggregation(setup):
    symbol, ts, handler = setup

    channel_handler = CandleChannelHandler([], handler, "3min", MagicMock())

    ts_start = pd.Timestamp(ts).floor("3min")
    ts1 = ts_start
    ts2 = ts_start + timedelta(minutes=1)
    ts3 = ts_start + timedelta(minutes=2)

    msg1 = create_stream_message(ts1, open=100, high=105, low=98, close=102, volume=500)
    msg2 = create_stream_message(
        ts2, open=102, high=108, low=101, close=107, volume=700
    )
    msg3 = create_stream_message(
        ts3, open=107, high=110, low=105, close=108, volume=300
    )

    mock_clock = MagicMock()
    ClockRegistry.set(mock_clock)
    mock_clock.now.return_value = ts_start

    try:
        await channel_handler.handle(f"candles.min.{symbol}", msg1)
        await channel_handler.handle(f"candles.min.{symbol}", msg2)
        await channel_handler.handle(f"candles.min.{symbol}", msg3)

        await asyncio.sleep(0.05)

        assert len(handler.received) == 1
        _, row = handler.received[0]

        assert row[CandleCol.OPEN] == 100
        assert row[CandleCol.HIGH] == 110
        assert row[CandleCol.LOW] == 98
        assert row[CandleCol.CLOSE] == 108
        assert row[CandleCol.VOLUME] == 1500

        assert row.name == ts_start
    finally:
        ClockRegistry.unset()


@pytest.mark.asyncio
async def test_flush_on_timeout_with_partial_buffer(setup):
    symbol, ts, handler = setup
    channel_handler = CandleChannelHandler([], handler, "3min", MagicMock())
    buffer = channel_handler._new_buffer(symbol)
    buffer._timeout = 0.1
    channel_handler._buffers = {symbol: buffer}
    ts_start = pd.Timestamp(ts).floor("3min")

    msg1 = create_stream_message(
        ts_start, open=100, high=101, low=99, close=100, volume=100
    )
    msg2 = create_stream_message(
        ts_start + timedelta(minutes=1),
        open=100,
        high=102,
        low=98,
        close=101,
        volume=200,
    )

    mock_clock = MagicMock()

    mock_clock.now.return_value = ts_start + timedelta(
        minutes=2, seconds=59, microseconds=990000
    )
    ClockRegistry.set(mock_clock)

    try:
        await channel_handler.handle(f"candles.min.{symbol}", msg1)
        await channel_handler.handle(f"candles.min.{symbol}", msg2)

        await asyncio.sleep(1)

        assert len(handler.received) == 1
        _, row = handler.received[0]
        assert row[CandleCol.OPEN] == 100
    finally:
        ClockRegistry.unset()
