import uuid
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from fastscanner.adapters.rest.main import app
from fastscanner.adapters.rest.scanner import get_scanner_service
from fastscanner.pkg.clock import ClockRegistry, FixedClock
from fastscanner.services.indicators.ports import (
    CandleCol,
    ChannelHandler,
    FundamentalData,
)
from fastscanner.services.registry import ApplicationRegistry
from fastscanner.services.scanners.lib import ScannersLibrary
from fastscanner.services.scanners.ports import ScannerParams
from fastscanner.services.scanners.service import ScannerService, SubscriptionHandler


class DummyScanner:
    def __init__(self, min_value: float = 0.0, **kwargs):
        self._id = str(uuid.uuid4())
        self._min_value = min_value

    def id(self) -> str:
        return self._id

    @classmethod
    def type(cls) -> str:
        return "dummy_scanner"

    async def scan(self, symbol: str, start, end, freq: str) -> pd.DataFrame:
        return pd.DataFrame()

    async def scan_realtime(
        self, symbol: str, new_row: pd.Series
    ) -> tuple[pd.Series, bool]:
        # Add a test indicator to the row to verify processing
        enhanced_row = new_row.copy()
        enhanced_row["test_indicator"] = new_row.get("close", 0) * 2
        passed = new_row.get("close", 0) > self._min_value
        return enhanced_row, passed


class MockSymbolsProvider:
    async def active_symbols(self, exchanges=None):
        return ["AAPL", "GOOGL", "MSFT"]


class MockChannel:
    def __init__(self):
        self.subscriptions = {}
        self.handlers: dict[str, ChannelHandler] = {}

    async def subscribe(self, channel_id: str, handler: ChannelHandler):
        if channel_id not in self.subscriptions:
            self.subscriptions[channel_id] = []
        self.handlers[channel_id] = handler

    async def unsubscribe(self, channel_id: str, handler_id: str):
        # Actually remove the handler when unsubscribing (needed for websocket tests)
        if channel_id in self.handlers:
            del self.handlers[channel_id]

    async def push_data(self, channel_id: str, data: dict[Any, Any]):
        """Helper method to push data to a specific channel for testing"""
        for cid, h in self.handlers.items():
            _, unit, symbol = cid.split(".")
            if symbol == "*" or cid == channel_id:
                await h.handle(channel_id, data)

    async def push(self, channel_id: str, data: dict[Any, Any], flush: bool = True): ...

    async def flush(self): ...

    async def reset(self): ...


class MockCandleStore:
    async def get(
        self, symbol, start, end, freq, adjusted: bool = True
    ) -> pd.DataFrame:
        return pd.DataFrame()


class MockSubscriptionHandler(SubscriptionHandler):
    def __init__(self):
        self.handled_symbols = []
        self.handled_rows = []
        self.handled_passed = []

    async def handle(self, symbol: str, new_row: pd.Series, passed: bool) -> pd.Series:
        self.handled_symbols.append(symbol)
        self.handled_rows.append(new_row)
        self.handled_passed.append(passed)
        return new_row


@pytest.fixture
def scanner_service(candles):
    library = ScannersLibrary()
    library.register(DummyScanner)
    library.register_realtime(DummyScanner)

    original_instance = ScannersLibrary.instance
    ScannersLibrary.instance = lambda: library

    ClockRegistry.set(
        FixedClock(datetime(2022, 1, 1, 9, 30, tzinfo=ZoneInfo("America/New_York")))
    )
    channel = MockChannel()
    symbols_provider = MockSymbolsProvider()
    service = ScannerService(candles, channel, symbols_provider)

    yield service, channel

    ScannersLibrary.instance = original_instance


@pytest.fixture
def scanner_params():
    return ScannerParams(type_="dummy_scanner", params={"min_value": 50.0})


@pytest.fixture
def subscription_handler():
    return MockSubscriptionHandler()


@pytest.fixture
def client(scanner_service):
    ClockRegistry.set(
        FixedClock(datetime(2022, 1, 1, 9, 30, tzinfo=ZoneInfo("America/New_York")))
    )
    service, channel = scanner_service
    app.dependency_overrides[get_scanner_service] = lambda: service
    client = TestClient(app)
    yield client
    app.dependency_overrides.clear()


class CandleStoreTest:
    def __init__(self):
        self._data = {}

    def set_data(self, symbol, data):
        self._data[symbol] = data

    async def get(self, symbol, start, end, freq, adjusted: bool = True):
        if symbol not in self._data:
            return pd.DataFrame(index=pd.DatetimeIndex([]), columns=[CandleCol.COLUMNS])
        df = self._data[symbol]
        return df[
            (df.index.date >= start_date) & (df.index.date <= end_date)  # type: ignore
        ]


class MockFundamentalDataStore:
    async def get(self, symbol):
        date_index = pd.date_range(start="2023-01-01", periods=3, freq="D").date
        return FundamentalData(
            "",
            "",
            "",
            "",
            "",
            "",
            pd.Series([1000000000.0, 1000000000.0, 1000000000.0], index=date_index),
            pd.DatetimeIndex([]),
            None,
            None,
            None,
            None,
        )


class MockPublicHolidaysStore:
    def get(self):
        return set()


@pytest.fixture
def candles():
    candle_store = CandleStoreTest()
    fundamental_store = MockFundamentalDataStore()
    holiday_store = MockPublicHolidaysStore()

    ApplicationRegistry.init(
        candles=candle_store, fundamentals=fundamental_store, holidays=holiday_store
    )

    yield candle_store
    ApplicationRegistry.reset()
