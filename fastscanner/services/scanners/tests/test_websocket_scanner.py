import asyncio
import json
from typing import Dict, List

import pandas as pd
import pytest

from fastscanner.adapters.rest.scanner import WebSocketScannerHandler
from fastscanner.services.scanners.ports import ScannerParams


class MockWebSocket:
    def __init__(self):
        self.sent_messages: List[str] = []
        self.received_messages: List[str] = []
        self.is_connected = True

    async def send_text(self, message: str):
        if not self.is_connected:
            raise Exception("WebSocket is not connected")
        self.sent_messages.append(message)

    async def receive_text(self) -> str:
        if self.received_messages:
            return self.received_messages.pop(0)
        while True:
            await asyncio.sleep(0.1)

    async def accept(self):
        self.is_connected = True

    def disconnect(self):
        self.is_connected = False

    def add_received_message(self, message: str):
        self.received_messages.append(message)

    def get_sent_messages_as_objects(self) -> List[Dict]:
        """Helper to parse sent JSON messages"""
        return [json.loads(msg) for msg in self.sent_messages]


@pytest.fixture
def mock_websocket():
    return MockWebSocket()


@pytest.mark.asyncio
async def test_end_to_end_websocket_scanner_flow(scanner_service, mock_websocket):
    """End-to-end test: create scanner, push data, verify websocket message"""
    service, channel = scanner_service

    handler = WebSocketScannerHandler(mock_websocket)

    scanner_params = ScannerParams(type_="dummy_scanner", params={"min_value": 50.0})

    scanner_id = await service.subscribe_realtime(
        params=scanner_params, handler=handler, freq="1min"
    )

    handler.set_scanner_id(scanner_id)

    test_data = {
        "open": 100.0,
        "high": 105.0,
        "low": 95.0,
        "close": 75.0,
        "volume": 1000,
        "timestamp": 1640995200000,
    }

    await channel.push_data("candles_min_AAPL", test_data)

    assert len(mock_websocket.sent_messages) == 1

    sent_message = json.loads(mock_websocket.sent_messages[0])
    assert sent_message["symbol"] == "AAPL"
    assert sent_message["scanner_id"] == scanner_id
    assert "scan_time" in sent_message
    assert sent_message["candle"]["close"] == 75.0
    assert sent_message["candle"]["test_indicator"] == 150.0


@pytest.mark.asyncio
async def test_end_to_end_websocket_scanner_flow_scanner_fails(
    scanner_service, mock_websocket
):
    """End-to-end test: data that fails scanner should not send websocket message"""
    service, channel = scanner_service

    handler = WebSocketScannerHandler(mock_websocket)
    scanner_params = ScannerParams(type_="dummy_scanner", params={"min_value": 50.0})

    scanner_id = await service.subscribe_realtime(
        params=scanner_params, handler=handler, freq="1min"
    )
    handler.set_scanner_id(scanner_id)

    test_data = {
        "open": 40.0,
        "high": 45.0,
        "low": 35.0,
        "close": 30.0,  # Below min_value of 50.0
        "volume": 1000,
        "timestamp": 1640995200000,
    }

    await channel.push_data("candles_min_AAPL", test_data)

    assert len(mock_websocket.sent_messages) == 0


@pytest.mark.asyncio
async def test_multiple_symbols_websocket_flow(scanner_service, mock_websocket):
    """Test websocket messages for multiple symbols"""
    service, channel = scanner_service

    handler = WebSocketScannerHandler(mock_websocket)
    scanner_params = ScannerParams(type_="dummy_scanner", params={"min_value": 50.0})

    scanner_id = await service.subscribe_realtime(
        params=scanner_params, handler=handler, freq="1min"
    )
    handler.set_scanner_id(scanner_id)

    test_data_aapl = {
        "close": 75.0,
        "volume": 1000,
        "timestamp": 1640995200000,
    }

    test_data_googl = {
        "close": 30.0,  # Below threshold
        "volume": 2000,
        "timestamp": 1640995200000,
    }

    test_data_msft = {
        "close": 60.0,
        "volume": 1500,
        "timestamp": 1640995200000,
    }

    await channel.push_data("candles_min_AAPL", test_data_aapl)
    await channel.push_data("candles_min_GOOGL", test_data_googl)
    await channel.push_data("candles_min_MSFT", test_data_msft)

    assert len(mock_websocket.sent_messages) == 2

    sent_messages = mock_websocket.get_sent_messages_as_objects()
    symbols = [msg["symbol"] for msg in sent_messages]

    assert "AAPL" in symbols
    assert "MSFT" in symbols
    assert "GOOGL" not in symbols


@pytest.mark.asyncio
async def test_websocket_unsubscribe_flow(scanner_service, mock_websocket):
    """Test that unsubscribe properly cleans up the scanner"""
    service, channel = scanner_service

    handler = WebSocketScannerHandler(mock_websocket)
    scanner_params = ScannerParams(type_="dummy_scanner", params={"min_value": 50.0})

    scanner_id = await service.subscribe_realtime(
        params=scanner_params, handler=handler, freq="1min"
    )

    assert len(channel.subscriptions) > 0

    await service.unsubscribe_realtime(scanner_id)

    test_data = {
        "close": 75.0,
        "volume": 1000,
        "timestamp": 1640995200000,
    }

    await channel.push_data("candles_min_AAPL", test_data)

    assert len(mock_websocket.sent_messages) == 0


@pytest.mark.asyncio
async def test_websocket_continues_processing_despite_connection_issues(
    scanner_service, mock_websocket
):
    """Test that system continues processing even when websocket has connection issues"""
    service, channel = scanner_service

    handler = WebSocketScannerHandler(mock_websocket)
    scanner_params = ScannerParams(type_="dummy_scanner", params={"min_value": 50.0})

    scanner_id = await service.subscribe_realtime(
        params=scanner_params, handler=handler, freq="1min"
    )
    handler.set_scanner_id(scanner_id)

    mock_websocket.disconnect()

    test_data = {
        "close": 75.0,  # Above threshold
        "volume": 1000,
        "timestamp": 1640995200000,
    }

    await channel.push_data("candles_min_AAPL", test_data)

    assert len(mock_websocket.sent_messages) == 0

    mock_websocket.is_connected = True

    test_data_2 = {
        "close": 80.0,
        "volume": 1500,
        "timestamp": 1640995260000,
    }

    await channel.push_data("candles_min_AAPL", test_data_2)

    assert len(mock_websocket.sent_messages) == 1
    sent_message = json.loads(mock_websocket.sent_messages[0])
    assert sent_message["symbol"] == "AAPL"
    assert sent_message["candle"]["close"] == 80.0
