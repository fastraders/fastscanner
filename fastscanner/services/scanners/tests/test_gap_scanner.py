from datetime import date, time

import pandas as pd
import pytest

from fastscanner.pkg.candle import Candle
from fastscanner.services.indicators.ports import CandleCol as C, FundamentalData
from fastscanner.services.indicators.tests.fixtures import (
    MockCache,
    MockPublicHolidaysStore,
)
from fastscanner.services.registry import ApplicationRegistry
from fastscanner.services.scanners.lib.gap import ATRGapDownScanner, ATRGapUpScanner


class MultiFreqCandleStore:
    def __init__(self):
        self._data: dict[tuple[str, str], pd.DataFrame] = {}

    def set_data(self, symbol: str, freq: str, data: pd.DataFrame):
        self._data[(symbol, freq)] = data

    async def get(self, symbol, start_date, end_date, freq, adjusted=True):
        key = (symbol, freq)
        if key not in self._data:
            return pd.DataFrame(
                index=pd.DatetimeIndex([]), columns=C.COLUMNS
            )
        df = self._data[key]
        return df[
            (df.index.date >= start_date) & (df.index.date <= end_date)
        ]


class MockFundamentalDataStore:
    def __init__(self, market_cap: float = 1e9):
        self._market_cap = market_cap

    async def get(self, symbol) -> FundamentalData:
        date_index = pd.date_range(start="2023-01-01", periods=30, freq="D").date
        return FundamentalData(
            "",
            "",
            "",
            "",
            "",
            "",
            pd.Series([self._market_cap] * 30, index=date_index),
            pd.DatetimeIndex([]),
            None,
            None,
            None,
            None,
        )


@pytest.fixture
def store():
    candle_store = MultiFreqCandleStore()
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


def _make_daily_data(n_days: int = 22) -> pd.DataFrame:
    dates = pd.date_range(start=date(2023, 1, 1), periods=n_days, freq="B")
    base = 100.0
    highs, lows, opens, closes, volumes = [], [], [], [], []
    for i in range(n_days):
        o = base + i * 2
        h = o + 5
        low = o - 3
        c = o + 1
        highs.append(h)
        lows.append(low)
        opens.append(o)
        closes.append(c)
        volumes.append(1_000_000 + i * 100_000)
    return pd.DataFrame(
        {
            C.OPEN: opens,
            C.HIGH: highs,
            C.LOW: lows,
            C.CLOSE: closes,
            C.VOLUME: volumes,
        },
        index=dates,
    )


# --- ATRGapDownScanner tests ---


@pytest.mark.asyncio
async def test_gap_down_scan_realtime_passes(store: MultiFreqCandleStore):
    daily = _make_daily_data()
    store.set_data("AAPL", "1d", daily)

    scanner = ATRGapDownScanner(
        min_adv=0,
        min_adr=0,
        atr_multiplier=0.1,
        min_volume=0,
        start_time=time(9, 30),
        end_time=time(16, 0),
    )

    last_close = daily[C.CLOSE].iloc[-1]
    gap_down_low = last_close - 20

    row = Candle(
        {
            C.OPEN: gap_down_low + 1,
            C.HIGH: gap_down_low + 3,
            C.LOW: gap_down_low,
            C.CLOSE: gap_down_low - 2,
            C.VOLUME: 500000,
        },
        timestamp=pd.Timestamp(2023, 2, 1, 10, 0),
    )
    result, passed = await scanner.scan_realtime("AAPL", row)

    assert passed is True


@pytest.mark.asyncio
async def test_gap_down_scan_realtime_outside_time_window(store: MultiFreqCandleStore):
    daily = _make_daily_data()
    store.set_data("AAPL", "1d", daily)

    scanner = ATRGapDownScanner(
        min_adv=0,
        min_adr=0,
        atr_multiplier=0.1,
        min_volume=0,
        start_time=time(9, 30),
        end_time=time(10, 0),
    )

    row = Candle(
        {
            C.OPEN: 80.0,
            C.HIGH: 82.0,
            C.LOW: 75.0,
            C.CLOSE: 76.0,
            C.VOLUME: 500000,
        },
        timestamp=pd.Timestamp(2023, 2, 1, 14, 0),
    )
    result, passed = await scanner.scan_realtime("AAPL", row)

    assert passed is False
    assert pd.isna(result["signal"])


@pytest.mark.asyncio
async def test_gap_down_scan_realtime_fails_min_adv(store: MultiFreqCandleStore):
    daily = _make_daily_data()
    store.set_data("AAPL", "1d", daily)

    scanner = ATRGapDownScanner(
        min_adv=999_999_999,
        min_adr=0,
        atr_multiplier=0.1,
        min_volume=0,
        start_time=time(9, 30),
        end_time=time(16, 0),
    )

    row = Candle(
        {
            C.OPEN: 80.0,
            C.HIGH: 82.0,
            C.LOW: 75.0,
            C.CLOSE: 76.0,
            C.VOLUME: 500000,
        },
        timestamp=pd.Timestamp(2023, 2, 1, 10, 0),
    )
    result, passed = await scanner.scan_realtime("AAPL", row)

    assert passed is False


@pytest.mark.asyncio
async def test_gap_down_scan_realtime_days_of_week_filter(store: MultiFreqCandleStore):
    daily = _make_daily_data()
    store.set_data("AAPL", "1d", daily)

    scanner = ATRGapDownScanner(
        min_adv=0,
        min_adr=0,
        atr_multiplier=0.1,
        min_volume=0,
        start_time=time(9, 30),
        end_time=time(16, 0),
        days_of_week=[0],  # Monday only
    )

    row = Candle(
        {
            C.OPEN: 80.0,
            C.HIGH: 82.0,
            C.LOW: 75.0,
            C.CLOSE: 76.0,
            C.VOLUME: 500000,
        },
        timestamp=pd.Timestamp(2023, 2, 1, 10, 0),  # Wednesday
    )
    result, passed = await scanner.scan_realtime("AAPL", row)

    assert passed is False
    assert pd.isna(result["signal"])


@pytest.mark.asyncio
async def test_gap_down_scan_empty_daily(store: MultiFreqCandleStore):
    scanner = ATRGapDownScanner(
        min_adv=0,
        min_adr=0,
        atr_multiplier=0.1,
        min_volume=0,
        start_time=time(9, 30),
        end_time=time(16, 0),
    )

    result_df = await scanner.scan(
        symbol="AAPL",
        start=date(2023, 1, 1),
        end=date(2023, 1, 31),
        freq="5min",
    )

    assert result_df.empty


@pytest.mark.asyncio
async def test_gap_down_scan_returns_dataframe(store: MultiFreqCandleStore):
    daily = _make_daily_data()
    store.set_data("AAPL", "1d", daily)

    from datetime import datetime, timedelta

    last_daily_date = daily.index[-1].date()
    scan_day = last_daily_date + timedelta(days=1)
    while scan_day.weekday() >= 5:
        scan_day += timedelta(days=1)

    last_close = daily[C.CLOSE].iloc[-1]
    gap_down = last_close - 20
    times = [
        datetime(scan_day.year, scan_day.month, scan_day.day, 9, 30),
        datetime(scan_day.year, scan_day.month, scan_day.day, 9, 31),
    ]
    minute = pd.DataFrame(
        {
            C.OPEN: [gap_down + 1, gap_down],
            C.HIGH: [gap_down + 3, gap_down + 2],
            C.LOW: [gap_down - 1, gap_down - 2],
            C.CLOSE: [gap_down - 2, gap_down - 3],
            C.VOLUME: [500000, 600000],
        },
        index=pd.DatetimeIndex(times),
    )
    store.set_data("AAPL", "5min", minute)

    scanner = ATRGapDownScanner(
        min_adv=0,
        min_adr=0,
        atr_multiplier=0.1,
        min_volume=0,
        start_time=time(9, 30),
        end_time=time(16, 0),
    )

    result_df = await scanner.scan(
        symbol="AAPL",
        start=date(2023, 1, 1),
        end=scan_day,
        freq="5min",
    )

    assert isinstance(result_df, pd.DataFrame)


# --- ATRGapUpScanner tests ---


@pytest.mark.asyncio
async def test_gap_up_scan_realtime_passes(store: MultiFreqCandleStore):
    daily = _make_daily_data()
    store.set_data("AAPL", "1d", daily)

    scanner = ATRGapUpScanner(
        min_adv=0,
        min_adr=0,
        atr_multiplier=0.1,
        min_volume=0,
        start_time=time(9, 30),
        end_time=time(16, 0),
    )

    last_close = daily[C.CLOSE].iloc[-1]
    gap_up_high = last_close + 20

    row = Candle(
        {
            C.OPEN: gap_up_high - 2,
            C.HIGH: gap_up_high,
            C.LOW: gap_up_high - 5,
            C.CLOSE: gap_up_high + 2,
            C.VOLUME: 500000,
        },
        timestamp=pd.Timestamp(2023, 2, 1, 10, 0),
    )
    result, passed = await scanner.scan_realtime("AAPL", row)

    assert passed is True


@pytest.mark.asyncio
async def test_gap_up_scan_realtime_outside_time_window(store: MultiFreqCandleStore):
    daily = _make_daily_data()
    store.set_data("AAPL", "1d", daily)

    scanner = ATRGapUpScanner(
        min_adv=0,
        min_adr=0,
        atr_multiplier=0.1,
        min_volume=0,
        start_time=time(9, 30),
        end_time=time(10, 0),
    )

    row = Candle(
        {
            C.OPEN: 160.0,
            C.HIGH: 165.0,
            C.LOW: 155.0,
            C.CLOSE: 163.0,
            C.VOLUME: 500000,
        },
        timestamp=pd.Timestamp(2023, 2, 1, 14, 0),
    )
    result, passed = await scanner.scan_realtime("AAPL", row)

    assert passed is False
    assert pd.isna(result["signal"])


@pytest.mark.asyncio
async def test_gap_up_scan_realtime_fails_market_cap(store: MultiFreqCandleStore):
    daily = _make_daily_data()
    store.set_data("AAPL", "1d", daily)

    scanner = ATRGapUpScanner(
        min_adv=0,
        min_adr=0,
        atr_multiplier=0.1,
        min_volume=0,
        start_time=time(9, 30),
        end_time=time(16, 0),
        min_market_cap=5e9,
        max_market_cap=10e9,
    )

    last_close = daily[C.CLOSE].iloc[-1]
    gap_up_high = last_close + 20

    row = Candle(
        {
            C.OPEN: gap_up_high - 2,
            C.HIGH: gap_up_high,
            C.LOW: gap_up_high - 5,
            C.CLOSE: gap_up_high + 2,
            C.VOLUME: 500000,
        },
        timestamp=pd.Timestamp(2023, 2, 1, 10, 0),
    )
    result, passed = await scanner.scan_realtime("AAPL", row)

    assert passed is False


@pytest.mark.asyncio
async def test_gap_up_scan_empty_daily(store: MultiFreqCandleStore):
    scanner = ATRGapUpScanner(
        min_adv=0,
        min_adr=0,
        atr_multiplier=0.1,
        min_volume=0,
        start_time=time(9, 30),
        end_time=time(16, 0),
    )

    result_df = await scanner.scan(
        symbol="AAPL",
        start=date(2023, 1, 1),
        end=date(2023, 1, 31),
        freq="5min",
    )

    assert result_df.empty


@pytest.mark.asyncio
async def test_gap_up_scan_returns_dataframe(store: MultiFreqCandleStore):
    daily = _make_daily_data()
    store.set_data("AAPL", "1d", daily)

    from datetime import datetime, timedelta

    last_daily_date = daily.index[-1].date()
    scan_day = last_daily_date + timedelta(days=1)
    while scan_day.weekday() >= 5:
        scan_day += timedelta(days=1)

    last_close = daily[C.CLOSE].iloc[-1]
    gap_up = last_close + 20
    times = [
        datetime(scan_day.year, scan_day.month, scan_day.day, 9, 30),
        datetime(scan_day.year, scan_day.month, scan_day.day, 9, 31),
    ]
    minute = pd.DataFrame(
        {
            C.OPEN: [gap_up - 2, gap_up - 1],
            C.HIGH: [gap_up, gap_up + 1],
            C.LOW: [gap_up - 5, gap_up - 4],
            C.CLOSE: [gap_up + 2, gap_up + 3],
            C.VOLUME: [500000, 600000],
        },
        index=pd.DatetimeIndex(times),
    )
    store.set_data("AAPL", "5min", minute)

    scanner = ATRGapUpScanner(
        min_adv=0,
        min_adr=0,
        atr_multiplier=0.1,
        min_volume=0,
        start_time=time(9, 30),
        end_time=time(16, 0),
    )

    result_df = await scanner.scan(
        symbol="AAPL",
        start=date(2023, 1, 1),
        end=scan_day,
        freq="5min",
    )

    assert isinstance(result_df, pd.DataFrame)


@pytest.mark.asyncio
async def test_gap_up_scan_realtime_days_of_week_filter(store: MultiFreqCandleStore):
    daily = _make_daily_data()
    store.set_data("AAPL", "1d", daily)

    scanner = ATRGapUpScanner(
        min_adv=0,
        min_adr=0,
        atr_multiplier=0.1,
        min_volume=0,
        start_time=time(9, 30),
        end_time=time(16, 0),
        days_of_week=[4],  # Friday only
    )

    row = Candle(
        {
            C.OPEN: 160.0,
            C.HIGH: 165.0,
            C.LOW: 155.0,
            C.CLOSE: 163.0,
            C.VOLUME: 500000,
        },
        timestamp=pd.Timestamp(2023, 2, 1, 10, 0),  # Wednesday
    )
    result, passed = await scanner.scan_realtime("AAPL", row)

    assert passed is False
    assert pd.isna(result["signal"])
