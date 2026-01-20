import asyncio
import json

import pytest
from fastapi import WebSocketDisconnect
from fastapi.testclient import TestClient
from pydantic import ValidationError

from fastscanner.adapters.rest.main import app
from fastscanner.adapters.rest.scanner import get_scanner_service


@pytest.mark.asyncio
async def test_websocket_realtime_scanner_end_to_end(scanner_service):
    service, channel = scanner_service
    app.dependency_overrides[get_scanner_service] = lambda: service

    client = TestClient(app)

    scanner_request = {
        "scanner_id": "test_scanner_id",
        "freq": "1min",
        "type": "dummy_scanner",
        "params": {
            "min_value": 50.0,
            "freq": "1min",
            "start_time": "09:00:00",
            "end_time": "16:00:00",
        },
    }

    test_data = {
        "open": 100.0,
        "high": 105.0,
        "low": 95.0,
        "close": 60.0,
        "volume": 1000,
        "timestamp": 1640995200000,
    }

    with client.websocket_connect("/api/scanners") as websocket:
        websocket.send_text(json.dumps(scanner_request))

        scanner_response = websocket.receive_text()
        scanner_id = json.loads(scanner_response)["scanner_id"]
        assert isinstance(scanner_id, str)

        await channel.push_data("candles_min_AAPL", test_data)

        message = websocket.receive_text()
        data = json.loads(message)

        assert data["scanner_id"] == scanner_id
        assert data["symbol"] == "AAPL"
        assert data["candle"]["close"] == 60.0
        assert data["candle"]["volume"] == 1000
        assert data["candle"]["test_indicator"] == 120.0


@pytest.mark.asyncio
async def test_websocket_malformed_subscription(scanner_service):
    """
    Test: When invalid JSON is sent to /api/scanners, the server raises ValidationError.
    """
    service, _ = scanner_service
    app.dependency_overrides[get_scanner_service] = lambda: service

    client = TestClient(app)

    with pytest.raises(ValidationError):
        with client.websocket_connect("/api/scanners") as websocket:
            websocket.send_text("this is not valid json")
            websocket.receive_text()

    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_websocket_scanner_failing_conditions(scanner_service):
    """
    Test: When scanner conditions are not met, no message should be sent via WebSocket.
    """
    service, channel = scanner_service
    app.dependency_overrides[get_scanner_service] = lambda: service

    client = TestClient(app)

    scanner_request = {
        "scanner_id": "test_scanner_id",
        "freq": "1min",
        "type": "dummy_scanner",
        "params": {
            "min_value": 100.0,  # Higher threshold
            "freq": "1min",
            "start_time": "09:00:00",
            "end_time": "16:00:00",
        },
    }

    test_data = {
        "open": 80.0,
        "high": 85.0,
        "low": 75.0,
        "close": 80.0,  # Below min_value of 100.0
        "volume": 1000,
        "timestamp": 1640995200000,
    }

    with client.websocket_connect("/api/scanners") as websocket:
        websocket.send_text(json.dumps(scanner_request))

        scanner_response = websocket.receive_text()
        scanner_id = json.loads(scanner_response)["scanner_id"]
        assert isinstance(scanner_id, str)

        await channel.push_data("candles_min_AAPL", test_data)

        await asyncio.sleep(0.1)

    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_websocket_disconnect_and_unsubscribe(scanner_service):
    """
    Test: When WebSocket disconnects, the scanner should be properly unsubscribed.
    """
    service, channel = scanner_service
    app.dependency_overrides[get_scanner_service] = lambda: service

    client = TestClient(app)

    scanner_request = {
        "scanner_id": "test_scanner_id",
        "freq": "1min",
        "type": "dummy_scanner",
        "params": {
            "min_value": 50.0,
            "freq": "1min",
            "start_time": "09:00:00",
            "end_time": "16:00:00",
        },
    }

    with client.websocket_connect("/api/scanners") as websocket:
        websocket.send_text(json.dumps(scanner_request))

        scanner_response = websocket.receive_text()
        scanner_id = json.loads(scanner_response)["scanner_id"]
        assert isinstance(scanner_id, str)

    test_data = {
        "open": 100.0,
        "high": 105.0,
        "low": 95.0,
        "close": 60.0,
        "volume": 1000,
        "timestamp": 1640995200000,
    }

    with client.websocket_connect("/api/scanners") as websocket:
        websocket.send_text(json.dumps(scanner_request))

        new_scanner_response = websocket.receive_text()
        new_scanner_id = json.loads(new_scanner_response)["scanner_id"]
        assert isinstance(new_scanner_id, str)
        assert new_scanner_id != scanner_id  # Should be a different ID

        await channel.push_data("candles_min_AAPL", test_data)

        message = websocket.receive_text()
        data = json.loads(message)

        assert data["scanner_id"] == new_scanner_id
        assert data["symbol"] == "AAPL"
        assert data["candle"]["close"] == 60.0

    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_websocket_invalid_scanner_type(scanner_service):
    """
    Test: When an invalid scanner type is provided, the system should handle it gracefully.
    """
    service, channel = scanner_service
    app.dependency_overrides[get_scanner_service] = lambda: service

    client = TestClient(app)

    scanner_request = {
        "scanner_id": "test_scanner_id",
        "freq": "1min",
        "type": "non_existent_scanner",  # Invalid scanner type
        "params": {
            "min_value": 50.0,
            "freq": "1min",
            "start_time": "09:00:00",
            "end_time": "16:00:00",
        },
    }

    with pytest.raises(Exception):
        with client.websocket_connect("/api/scanners") as websocket:
            websocket.send_text(json.dumps(scanner_request))
            websocket.receive_text()

    app.dependency_overrides.clear()
