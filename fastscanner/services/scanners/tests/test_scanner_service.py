from typing import Any
import pytest
from unittest.mock import AsyncMock, Mock
from datetime import time, date
import pandas as pd
import uuid

from fastscanner.services.scanners.service import ScannerService, SubscriptionHandler
from fastscanner.services.scanners.ports import ScannerParams
from fastscanner.services.scanners.lib import ScannersLibrary


class DummyScanner:
    def __init__(self, min_value: float = 0.0, **kwargs):
        self._id = str(uuid.uuid4())
        self._min_value = min_value

    def id(self) -> str:
        return self._id

    @classmethod
    def type(cls) -> str:
        return "dummy_scanner"

    async def scan(self, symbol: str, start: date, end: date, freq: str) -> pd.DataFrame:
        return pd.DataFrame()

    async def scan_realtime(self, symbol: str, new_row: pd.Series, freq: str) -> tuple[pd.Series, bool]:
        passed = new_row.get("close", 0) > self._min_value
        return new_row, passed


class AnotherDummyScanner:
    def __init__(self, threshold: int = 100, **kwargs):
        self._id = str(uuid.uuid4())
        self._threshold = threshold

    def id(self) -> str:
        return self._id

    @classmethod
    def type(cls) -> str:
        return "another_dummy"

    async def scan(self, symbol: str, start: date, end: date, freq: str) -> pd.DataFrame:
        return pd.DataFrame()

    async def scan_realtime(self, symbol: str, new_row: pd.Series, freq: str) -> tuple[pd.Series, bool]:
        passed = new_row.get("volume", 0) > self._threshold
        return new_row, passed


class MockSymbolsProvider:
    async def active_symbols(self, exchanges=None):
        return ["AAPL", "GOOGL", "MSFT"]


class MockChannel:
    def __init__(self):
        self.subscriptions = {}
        self.unsubscriptions = {}

    async def subscribe(self, channel_id: str, handler):
        if channel_id not in self.subscriptions:
            self.subscriptions[channel_id] = []
        self.subscriptions[channel_id].append(handler)

    async def unsubscribe(self, channel_id: str, handler_id: str):
        if channel_id not in self.unsubscriptions:
            self.unsubscriptions[channel_id] = []
        self.unsubscriptions[channel_id].append(handler_id)


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

    def handle(self, symbol: str, new_row: pd.Series, passed: bool) -> pd.Series:
        self.handled_symbols.append(symbol)
        self.handled_rows.append(new_row)
        self.handled_passed.append(passed)
        return new_row


@pytest.fixture
def scanner_service():
    library = ScannersLibrary()
    library.register(DummyScanner)
    library.register(AnotherDummyScanner)
    
    original_instance = ScannersLibrary.instance
    ScannersLibrary.instance = lambda: library
    
    candles = MockCandleStore()
    channel = MockChannel()
    symbols_provider = MockSymbolsProvider()
    service = ScannerService(candles, channel, symbols_provider)
    
    yield service
    
    ScannersLibrary.instance = original_instance


@pytest.fixture
def scanner_params():
    return ScannerParams(
        type_="dummy_scanner",
        params={"min_value": 50.0}
    )


@pytest.fixture
def subscription_handler():
    return MockSubscriptionHandler()


@pytest.mark.asyncio
async def test_subscribe_realtime_creates_subscriptions_for_all_symbols(scanner_service, scanner_params, subscription_handler):
    scanner_id = await scanner_service.subscribe_realtime(
        params=scanner_params,
        handler=subscription_handler,
        freq="1min"
    )
    
    assert scanner_id is not None
    assert scanner_id in scanner_service._handlers
    assert len(scanner_service._handlers[scanner_id]) == 3  # AAPL, GOOGL, MSFT
    
    expected_streams = ["candles_min_AAPL", "candles_min_GOOGL", "candles_min_MSFT"]
    for channel_id in expected_streams:
        assert channel_id in scanner_service._channel.subscriptions
        assert len(scanner_service._channel.subscriptions[channel_id]) == 1


@pytest.mark.asyncio
async def test_subscribe_realtime_uses_scanner_library(scanner_service, subscription_handler):
    params = ScannerParams(
        type_="another_dummy",
        params={"threshold": 1000}
    )
    
    scanner_id = await scanner_service.subscribe_realtime(
        params=params,
        handler=subscription_handler,
        freq="1min"
    )
    
    assert scanner_id is not None
    handlers = scanner_service._handlers[scanner_id]
    
    handler = handlers[0]
    assert handler._scanner.type() == "another_dummy"


@pytest.mark.asyncio
async def test_subscribe_realtime_returns_unique_scanner_ids(scanner_service, scanner_params, subscription_handler):
    scanner_id_1 = await scanner_service.subscribe_realtime(
        params=scanner_params,
        handler=subscription_handler,
        freq="1min"
    )
    scanner_id_2 = await scanner_service.subscribe_realtime(
        params=scanner_params,
        handler=subscription_handler,
        freq="1min"
    )
    
    assert scanner_id_1 != scanner_id_2
    assert scanner_id_1 in scanner_service._handlers
    assert scanner_id_2 in scanner_service._handlers


@pytest.mark.asyncio
async def test_unsubscribe_realtime_removes_all_subscriptions(scanner_service, scanner_params, subscription_handler):
    scanner_id = await scanner_service.subscribe_realtime(
        params=scanner_params,
        handler=subscription_handler,
        freq="1min"
    )
    
    await scanner_service.unsubscribe_realtime(scanner_id)
    
    # Assert
    assert scanner_id not in scanner_service._handlers
    
    expected_streams = ["candles_min_AAPL", "candles_min_GOOGL", "candles_min_MSFT"]
    for channel_id in expected_streams:
        assert channel_id in scanner_service._channel.unsubscriptions
        assert len(scanner_service._channel.unsubscriptions[channel_id]) == 1


@pytest.mark.asyncio
async def test_unsubscribe_realtime_handles_nonexistent_scanner(scanner_service):
    await scanner_service.unsubscribe_realtime("non-existent-scanner-id")


@pytest.mark.asyncio
async def test_subscribe_realtime_uses_symbols_provider(scanner_service, scanner_params, subscription_handler):
    async def mock_active_symbols(exchanges=None):
        return ["TEST1", "TEST2"]
    
    scanner_service._symbols_provider.active_symbols = mock_active_symbols
    
    scanner_id = await scanner_service.subscribe_realtime(
        params=scanner_params,
        handler=subscription_handler,
        freq="1min"
    )
    
    assert len(scanner_service._handlers[scanner_id]) == 2  # TEST1, TEST2
    assert "candles_min_TEST1" in scanner_service._channel.subscriptions
    assert "candles_min_TEST2" in scanner_service._channel.subscriptions


@pytest.mark.asyncio
async def test_scanner_params_with_different_frequencies(scanner_service, scanner_params, subscription_handler):
    scanner_id = await scanner_service.subscribe_realtime(
        params=scanner_params,
        handler=subscription_handler,
        freq="5min"
    )
    
    handlers = scanner_service._handlers[scanner_id]
    for handler in handlers:
        assert handler._freq == "5min"


@pytest.mark.asyncio
async def test_scanner_concurrent_subscription_execution(scanner_service, scanner_params, subscription_handler):
    
    scanner_id = await scanner_service.subscribe_realtime(
        params=scanner_params,
        handler=subscription_handler,
        freq="1min"
    )
    
    assert len(scanner_service._handlers[scanner_id]) == 3
    
    for handler in scanner_service._handlers[scanner_id]:
        assert handler._scanner.type() == "dummy_scanner"
        assert handler._freq == "1min"


@pytest.mark.asyncio
async def test_scanner_service_multiple_concurrent_subscriptions(scanner_service, subscription_handler):
    params1 = ScannerParams(type_="dummy_scanner", params={"min_value": 10.0})
    params2 = ScannerParams(type_="another_dummy", params={"threshold": 500})
    
    scanner_id_1 = await scanner_service.subscribe_realtime(params1, subscription_handler, "1min")
    scanner_id_2 = await scanner_service.subscribe_realtime(params2, subscription_handler, "1min")
    
    assert len(scanner_service._handlers) == 2
    assert scanner_id_1 in scanner_service._handlers
    assert scanner_id_2 in scanner_service._handlers
    
    assert len(scanner_service._handlers[scanner_id_1]) == 3
    assert len(scanner_service._handlers[scanner_id_2]) == 3


@pytest.mark.asyncio
async def test_scanner_service_limits_symbol_count(scanner_service, scanner_params, subscription_handler):
    large_symbol_list = [f"SYMBOL{i}" for i in range(1500)]
    
    async def mock_active_symbols(exchanges=None):
        return large_symbol_list
    
    scanner_service._symbols_provider.active_symbols = mock_active_symbols
    
    scanner_id = await scanner_service.subscribe_realtime(
        params=scanner_params,
        handler=subscription_handler,
        freq="1min"
    )
    
    assert len(scanner_service._handlers[scanner_id]) == 1000


def test_dummy_scanner_type_registration():
    library = ScannersLibrary()
    library.register(DummyScanner)
    library.register(AnotherDummyScanner)
    
    dummy = library.get("dummy_scanner", {"min_value": 10.0})
    assert isinstance(dummy, DummyScanner)
    assert dummy.type() == "dummy_scanner"
    
    another = library.get("another_dummy", {"threshold": 500})
    assert isinstance(another, AnotherDummyScanner)
    assert another.type() == "another_dummy"


def test_dummy_scanner_invalid_type():
    library = ScannersLibrary()
    library.register(DummyScanner)
    
    with pytest.raises(ValueError, match="Scanner invalid_type not found"):
        library.get("invalid_type", {})


@pytest.mark.asyncio
async def test_subscription_handler_interface():
    handler = MockSubscriptionHandler()
    
    test_row = pd.Series({"close": 100.0, "volume": 1000})
    
    result = handler.handle("AAPL", test_row, True)
    
    assert len(handler.handled_symbols) == 1
    assert handler.handled_symbols[0] == "AAPL"
    assert handler.handled_passed[0] is True
    assert result.equals(test_row) 