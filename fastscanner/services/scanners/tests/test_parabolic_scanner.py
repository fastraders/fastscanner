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
from fastscanner.services.scanners.lib.parabolic import (
    ATRParabolicDownScanner,
    ATRParabolicUpScanner,
)


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


def _make_daily_data(n_days: int = 20) -> pd.DataFrame:
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


def _make_minute_data(
    day: date, n_bars: int = 5, base_open: float = 100.0
) -> pd.DataFrame:
    from datetime import datetime, timedelta

    times = [datetime(day.year, day.month, day.day, 9, 30) + timedelta(minutes=i) for i in range(n_bars)]
    rows = []
    for i, t in enumerate(times):
        o = base_open + i
        rows.append({
            C.OPEN: o,
            C.HIGH: o + 2,
            C.LOW: o - 1,
            C.CLOSE: o + 0.5,
            C.VOLUME: 50000 + i * 10000,
        })
    return pd.DataFrame(rows, index=pd.DatetimeIndex(times))


# --- ATRParabolicDownScanner tests ---


@pytest.mark.asyncio
async def test_parabolic_down_scan_realtime_passes(store: MultiFreqCandleStore):
    daily = _make_daily_data()
    store.set_data("AAPL", "1d", daily)

    scanner = ATRParabolicDownScanner(
        min_adv=0,
        min_adr=0,
        atr_multiplier=0.5,
        min_volume=0,
        start_time=time(9, 30),
        end_time=time(16, 0),
    )

    row = Candle(
        {
            C.OPEN: 200.0,
            C.HIGH: 205.0,
            C.LOW: 140.0,
            C.CLOSE: 145.0,
            C.VOLUME: 500000,
        },
        timestamp=pd.Timestamp(2023, 2, 1, 10, 0),
    )
    result, passed = await scanner.scan_realtime("AAPL", row)

    assert "signal" in result
    assert not pd.isna(result["signal"])
    assert passed is True


@pytest.mark.asyncio
async def test_parabolic_down_scan_realtime_fails_low_signal(store: MultiFreqCandleStore):
    daily = _make_daily_data()
    store.set_data("AAPL", "1d", daily)

    scanner = ATRParabolicDownScanner(
        min_adv=0,
        min_adr=0,
        atr_multiplier=10.0,
        min_volume=0,
        start_time=time(9, 30),
        end_time=time(16, 0),
    )

    row = Candle(
        {
            C.OPEN: 140.0,
            C.HIGH: 142.0,
            C.LOW: 138.0,
            C.CLOSE: 139.5,
            C.VOLUME: 500000,
        },
        timestamp=pd.Timestamp(2023, 2, 1, 10, 0),
    )
    result, passed = await scanner.scan_realtime("AAPL", row)

    assert passed is False


@pytest.mark.asyncio
async def test_parabolic_down_scan_realtime_outside_time_window(store: MultiFreqCandleStore):
    daily = _make_daily_data()
    store.set_data("AAPL", "1d", daily)

    scanner = ATRParabolicDownScanner(
        min_adv=0,
        min_adr=0,
        atr_multiplier=0.5,
        min_volume=0,
        start_time=time(9, 30),
        end_time=time(10, 0),
    )

    row = Candle(
        {
            C.OPEN: 200.0,
            C.HIGH: 205.0,
            C.LOW: 140.0,
            C.CLOSE: 145.0,
            C.VOLUME: 500000,
        },
        timestamp=pd.Timestamp(2023, 2, 1, 12, 0),
    )
    result, passed = await scanner.scan_realtime("AAPL", row)

    assert passed is False
    assert pd.isna(result["signal"])


@pytest.mark.asyncio
async def test_parabolic_down_scan_realtime_fails_min_adv(store: MultiFreqCandleStore):
    daily = _make_daily_data()
    store.set_data("AAPL", "1d", daily)

    scanner = ATRParabolicDownScanner(
        min_adv=999_999_999,
        min_adr=0,
        atr_multiplier=0.5,
        min_volume=0,
        start_time=time(9, 30),
        end_time=time(16, 0),
    )

    row = Candle(
        {
            C.OPEN: 200.0,
            C.HIGH: 205.0,
            C.LOW: 140.0,
            C.CLOSE: 145.0,
            C.VOLUME: 500000,
        },
        timestamp=pd.Timestamp(2023, 2, 1, 10, 0),
    )
    result, passed = await scanner.scan_realtime("AAPL", row)

    assert passed is False


@pytest.mark.asyncio
async def test_parabolic_down_scan_realtime_market_cap_filter(store: MultiFreqCandleStore):
    daily = _make_daily_data()
    store.set_data("AAPL", "1d", daily)

    scanner = ATRParabolicDownScanner(
        min_adv=0,
        min_adr=0,
        atr_multiplier=0.5,
        min_volume=0,
        start_time=time(9, 30),
        end_time=time(16, 0),
        min_market_cap=5e9,
        max_market_cap=10e9,
    )

    row = Candle(
        {
            C.OPEN: 200.0,
            C.HIGH: 205.0,
            C.LOW: 140.0,
            C.CLOSE: 145.0,
            C.VOLUME: 500000,
        },
        timestamp=pd.Timestamp(2023, 2, 1, 10, 0),
    )
    result, passed = await scanner.scan_realtime("AAPL", row)

    assert passed is False


@pytest.mark.asyncio
async def test_parabolic_down_scan(store: MultiFreqCandleStore):
    daily = _make_daily_data()
    store.set_data("AAPL", "1d", daily)

    last_daily_date = daily.index[-1].date()
    from datetime import timedelta

    scan_day = last_daily_date + timedelta(days=1)
    while scan_day.weekday() >= 5:
        scan_day += timedelta(days=1)

    minute = _make_minute_data(scan_day, n_bars=3, base_open=200.0)
    minute[C.CLOSE] = [145.0, 144.0, 143.0]
    store.set_data("AAPL", "5min", minute)

    scanner = ATRParabolicDownScanner(
        min_adv=0,
        min_adr=0,
        atr_multiplier=0.5,
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
async def test_parabolic_down_scan_empty_daily(store: MultiFreqCandleStore):
    scanner = ATRParabolicDownScanner(
        min_adv=0,
        min_adr=0,
        atr_multiplier=0.5,
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


# --- ATRParabolicUpScanner tests ---


@pytest.mark.asyncio
async def test_parabolic_up_scan_realtime_passes(store: MultiFreqCandleStore):
    daily = _make_daily_data()
    store.set_data("AAPL", "1d", daily)

    scanner = ATRParabolicUpScanner(
        min_adv=0,
        min_adr=0,
        atr_multiplier=0.5,
        min_volume=0,
        start_time=time(9, 30),
        end_time=time(16, 0),
    )

    row = Candle(
        {
            C.OPEN: 145.0,
            C.HIGH: 205.0,
            C.LOW: 140.0,
            C.CLOSE: 200.0,
            C.VOLUME: 500000,
        },
        timestamp=pd.Timestamp(2023, 2, 1, 10, 0),
    )
    result, passed = await scanner.scan_realtime("AAPL", row)

    assert "signal" in result
    assert not pd.isna(result["signal"])
    assert passed is True


@pytest.mark.asyncio
async def test_parabolic_up_scan_realtime_fails_low_signal(store: MultiFreqCandleStore):
    daily = _make_daily_data()
    store.set_data("AAPL", "1d", daily)

    scanner = ATRParabolicUpScanner(
        min_adv=0,
        min_adr=0,
        atr_multiplier=10.0,
        min_volume=0,
        start_time=time(9, 30),
        end_time=time(16, 0),
    )

    row = Candle(
        {
            C.OPEN: 139.0,
            C.HIGH: 142.0,
            C.LOW: 138.0,
            C.CLOSE: 140.0,
            C.VOLUME: 500000,
        },
        timestamp=pd.Timestamp(2023, 2, 1, 10, 0),
    )
    result, passed = await scanner.scan_realtime("AAPL", row)

    assert passed is False


@pytest.mark.asyncio
async def test_parabolic_up_scan_realtime_outside_time_window(store: MultiFreqCandleStore):
    daily = _make_daily_data()
    store.set_data("AAPL", "1d", daily)

    scanner = ATRParabolicUpScanner(
        min_adv=0,
        min_adr=0,
        atr_multiplier=0.5,
        min_volume=0,
        start_time=time(9, 30),
        end_time=time(10, 0),
    )

    row = Candle(
        {
            C.OPEN: 145.0,
            C.HIGH: 205.0,
            C.LOW: 140.0,
            C.CLOSE: 200.0,
            C.VOLUME: 500000,
        },
        timestamp=pd.Timestamp(2023, 2, 1, 12, 0),
    )
    result, passed = await scanner.scan_realtime("AAPL", row)

    assert passed is False
    assert pd.isna(result["signal"])


@pytest.mark.asyncio
async def test_parabolic_up_scan_empty_daily(store: MultiFreqCandleStore):
    scanner = ATRParabolicUpScanner(
        min_adv=0,
        min_adr=0,
        atr_multiplier=0.5,
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
async def test_parabolic_up_scan(store: MultiFreqCandleStore):
    daily = _make_daily_data()
    store.set_data("AAPL", "1d", daily)

    last_daily_date = daily.index[-1].date()
    from datetime import timedelta

    scan_day = last_daily_date + timedelta(days=1)
    while scan_day.weekday() >= 5:
        scan_day += timedelta(days=1)

    minute = _make_minute_data(scan_day, n_bars=3, base_open=145.0)
    minute[C.CLOSE] = [200.0, 202.0, 205.0]
    store.set_data("AAPL", "5min", minute)

    scanner = ATRParabolicUpScanner(
        min_adv=0,
        min_adr=0,
        atr_multiplier=0.5,
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
async def test_parabolic_up_scan_realtime_min_volume_filter(store: MultiFreqCandleStore):
    daily = _make_daily_data()
    store.set_data("AAPL", "1d", daily)

    scanner = ATRParabolicUpScanner(
        min_adv=0,
        min_adr=0,
        atr_multiplier=0.5,
        min_volume=999_999_999,
        start_time=time(9, 30),
        end_time=time(16, 0),
    )

    row = Candle(
        {
            C.OPEN: 145.0,
            C.HIGH: 205.0,
            C.LOW: 140.0,
            C.CLOSE: 200.0,
            C.VOLUME: 500000,
        },
        timestamp=pd.Timestamp(2023, 2, 1, 10, 0),
    )
    result, passed = await scanner.scan_realtime("AAPL", row)

    assert passed is False
