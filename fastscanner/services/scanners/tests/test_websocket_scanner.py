import json
from fastapi import WebSocketDisconnect
from pydantic import ValidationError
import pytest
from fastapi.testclient import TestClient
from fastscanner.adapters.rest.main import app
from fastscanner.adapters.rest.scanner import get_scanner_service


@pytest.mark.asyncio
async def test_websocket_realtime_scanner_end_to_end(scanner_service):
    service, channel = scanner_service
    app.dependency_overrides[get_scanner_service] = lambda: service

    client = TestClient(app)

    scanner_request = {
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
