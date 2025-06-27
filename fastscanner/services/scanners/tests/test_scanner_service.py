import uuid
from datetime import date, time
from typing import Any

import pandas as pd
import pytest

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

    async def scan(
        self, symbol: str, start: date, end: date, freq: str
    ) -> pd.DataFrame:
        return pd.DataFrame()

    async def scan_realtime(
        self, symbol: str, new_row: dict[str, Any], freq: str
    ) -> tuple[dict[str, Any], bool]:
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
        self.unsubscriptions = {}
        self.handlers = {}

    async def subscribe(self, channel_id: str, handler):
        if channel_id not in self.subscriptions:
            self.subscriptions[channel_id] = []
        self.subscriptions[channel_id].append(handler)
        self.handlers[channel_id] = handler

    async def unsubscribe(self, channel_id: str, handler_id: str):
        if channel_id not in self.unsubscriptions:
            self.unsubscriptions[channel_id] = []
        self.unsubscriptions[channel_id].append(handler_id)

    async def push_data(self, channel_id: str, data: dict[Any, Any]):
        """Helper method to push data to a specific channel for testing"""
        if channel_id in self.handlers:
            await self.handlers[channel_id].handle(channel_id, data)

    async def push(self, channel_id: str, data: dict[Any, Any], flush: bool = True): ...

    async def flush(self): ...


class MockCandleStore:
    async def get(self, symbol, start, end, freq):
        return pd.DataFrame()


class MockSubscriptionHandler(SubscriptionHandler):
    def __init__(self):
        self.handled_symbols = []
        self.handled_rows = []
        self.handled_passed = []

    async def handle(
        self, symbol: str, new_row: dict[str, Any], passed: bool
    ) -> dict[str, Any]:
        self.handled_symbols.append(symbol)
        self.handled_rows.append(new_row)
        self.handled_passed.append(passed)
        return new_row

    def set_scanner_id(self, scanner_id: str): ...


@pytest.fixture
def scanner_service():
    library = ScannersLibrary()
    library.register(DummyScanner)

    original_instance = ScannersLibrary.instance
    ScannersLibrary.instance = lambda: library

    candles = MockCandleStore()
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


@pytest.mark.asyncio
async def test_realtime_data_flow_scanner_passes(
    scanner_service, scanner_params, subscription_handler
):
    service, channel = scanner_service

    scanner_id = await service.subscribe_realtime(
        params=scanner_params, handler=subscription_handler, freq="1min"
    )

    test_data = {
        "open": 100.0,
        "high": 105.0,
        "low": 95.0,
        "close": 60.0,  # Above min_value of 50.0
        "volume": 1000,
        "timestamp": 1640995200000,  # 2022-01-01 00:00:00 UTC
    }

    await channel.push_data("candles_min_AAPL", test_data)

    assert len(subscription_handler.handled_symbols) == 1
    assert subscription_handler.handled_symbols[0] == "AAPL"
    assert bool(subscription_handler.handled_passed[0]) is True

    handled_row = subscription_handler.handled_rows[0]
    assert "test_indicator" in handled_row
    assert handled_row["test_indicator"] == 120.0  # close * 2


@pytest.mark.asyncio
async def test_realtime_data_flow_scanner_fails(
    scanner_service, scanner_params, subscription_handler
):
    service, channel = scanner_service

    scanner_id = await service.subscribe_realtime(
        params=scanner_params, handler=subscription_handler, freq="1min"
    )

    test_data = {
        "open": 40.0,
        "high": 45.0,
        "low": 35.0,
        "close": 30.0,  # Below min_value of 50.0
        "volume": 1000,
        "timestamp": 1640995200000,
    }

    await channel.push_data("candles_min_AAPL", test_data)

    assert len(subscription_handler.handled_symbols) == 1
    assert subscription_handler.handled_symbols[0] == "AAPL"
    assert bool(subscription_handler.handled_passed[0]) is False

    handled_row = subscription_handler.handled_rows[0]
    assert "test_indicator" in handled_row
    assert handled_row["test_indicator"] == 60.0  # close * 2


@pytest.mark.asyncio
async def test_realtime_data_flow_multiple_symbols(
    scanner_service, scanner_params, subscription_handler
):
    service, channel = scanner_service

    scanner_id = await service.subscribe_realtime(
        params=scanner_params, handler=subscription_handler, freq="1min"
    )

    test_data_aapl = {
        "close": 60.0,  # Should pass
        "volume": 1000,
        "timestamp": 1640995200000,
    }

    test_data_googl = {
        "close": 40.0,  # Should fail
        "volume": 2000,
        "timestamp": 1640995200000,
    }

    await channel.push_data("candles_min_AAPL", test_data_aapl)
    await channel.push_data("candles_min_GOOGL", test_data_googl)

    assert len(subscription_handler.handled_symbols) == 2
    assert "AAPL" in subscription_handler.handled_symbols
    assert "GOOGL" in subscription_handler.handled_symbols

    aapl_idx = subscription_handler.handled_symbols.index("AAPL")
    googl_idx = subscription_handler.handled_symbols.index("GOOGL")

    assert bool(subscription_handler.handled_passed[aapl_idx]) is True
    assert bool(subscription_handler.handled_passed[googl_idx]) is False


@pytest.mark.asyncio
async def test_realtime_data_flow_different_scanner_params(
    scanner_service, subscription_handler
):
    service, channel = scanner_service

    params = ScannerParams(type_="dummy_scanner", params={"min_value": 100.0})
    scanner_id = await service.subscribe_realtime(
        params=params, handler=subscription_handler, freq="1min"
    )

    test_data = {
        "close": 75.0,  # Between 50 and 100
        "volume": 1000,
        "timestamp": 1640995200000,
    }

    await channel.push_data("candles_min_AAPL", test_data)

    assert bool(subscription_handler.handled_passed[0]) is False


@pytest.mark.asyncio
async def test_scanner_service_subscription_behavior():
    library = ScannersLibrary()
    library.register(DummyScanner)
    ScannersLibrary.instance = lambda: library

    candles = MockCandleStore()
    channel = MockChannel()
    symbols_provider = MockSymbolsProvider()
    service = ScannerService(candles, channel, symbols_provider)
    handler = MockSubscriptionHandler()

    params = ScannerParams(type_="dummy_scanner", params={"min_value": 50.0})

    scanner_id = await service.subscribe_realtime(params, handler, "1min")
    assert scanner_id is not None

    await service.unsubscribe_realtime(scanner_id)

    await service.unsubscribe_realtime("non-existent")


@pytest.mark.asyncio
async def test_data_type_conversion():
    library = ScannersLibrary()
    library.register(DummyScanner)
    ScannersLibrary.instance = lambda: library

    candles = MockCandleStore()
    channel = MockChannel()
    symbols_provider = MockSymbolsProvider()
    service = ScannerService(candles, channel, symbols_provider)
    handler = MockSubscriptionHandler()

    params = ScannerParams(type_="dummy_scanner", params={"min_value": 50.0})
    scanner_id = await service.subscribe_realtime(params, handler, "1min")

    test_data = {
        "open": "100.5",
        "high": "105.5",
        "low": "95.5",
        "close": "60.5",
        "volume": "1000",
        "timestamp": 1640995200000,
    }

    await channel.push_data("candles_min_AAPL", test_data)

    handled_row = handler.handled_rows[0]
    assert isinstance(handled_row["open"], float)
    assert isinstance(handled_row["high"], float)
    assert isinstance(handled_row["low"], float)
    assert isinstance(handled_row["close"], float)
    assert isinstance(handled_row["volume"], float)
