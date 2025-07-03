import uuid
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

    async def scan(self, symbol: str, start, end, freq: str) -> pd.DataFrame:
        return pd.DataFrame()

    async def scan_realtime(
        self, symbol: str, new_row: pd.Series, freq: str
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

        # Actually remove the handler when unsubscribing (needed for websocket tests)
        if channel_id in self.handlers:
            del self.handlers[channel_id]

        # Remove from subscriptions as well
        if channel_id in self.subscriptions:
            del self.subscriptions[channel_id]

    async def push_data(self, channel_id: str, data: dict[Any, Any]):
        """Helper method to push data to a specific channel for testing"""
        if channel_id in self.handlers:
            await self.handlers[channel_id].handle(channel_id, data)

    async def push(self, channel_id: str, data: dict[Any, Any], flush: bool = True): ...

    async def flush(self): ...

    async def reset(self): ...


class MockCandleStore:
    async def get(self, symbol, start, end, freq):
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
