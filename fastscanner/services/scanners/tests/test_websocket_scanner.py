import asyncio
import json
from unittest.mock import AsyncMock, patch, MagicMock

import pytest
from fastapi.testclient import TestClient

from fastscanner.adapters.rest.main import app
from fastscanner.adapters.rest.scanner import WebSocketScannerHandler
import pandas as pd
from unittest.mock import AsyncMock


@pytest.mark.asyncio
async def test_websocket_scanner_connection_and_subscription():
    client = TestClient(app)

    with patch("fastscanner.adapters.rest.scanner.RedisChannel"), patch(
        "fastscanner.adapters.rest.scanner.PartitionedCSVCandlesProvider"
    ), patch("fastscanner.adapters.rest.scanner.PolygonCandlesProvider"), patch(
        "fastscanner.adapters.rest.scanner.ScannerService"
    ) as mock_scanner_service:

        mock_service_instance = AsyncMock()
        mock_service_instance.subscribe_realtime.return_value = "test-scanner-123"
        mock_scanner_service.return_value = mock_service_instance

        with client.websocket_connect("/api/scanners") as websocket:
            scanner_request = {
                "type": "atr_gap_up",
                "params": {
                    "min_adv": 1000000,
                    "min_adr": 1.0,
                    "atr_multiplier": 2.0,
                    "min_volume": 100000,
                    "start_time": "09:30",
                    "end_time": "16:00",
                    "freq": "1min",
                },
            }
            websocket.send_text(json.dumps(scanner_request))

            response_data = websocket.receive_text()
            response = json.loads(response_data)

            assert "scanner_id" in response
            assert response["scanner_id"] == "test-scanner-123"


@pytest.mark.asyncio
async def test_websocket_scanner_with_time_params():
    client = TestClient(app)

    with patch("fastscanner.adapters.rest.scanner.RedisChannel"), patch(
        "fastscanner.adapters.rest.scanner.PartitionedCSVCandlesProvider"
    ), patch("fastscanner.adapters.rest.scanner.PolygonCandlesProvider"), patch(
        "fastscanner.adapters.rest.scanner.ScannerService"
    ) as mock_scanner_service:

        mock_service_instance = AsyncMock()
        mock_service_instance.subscribe_realtime.return_value = "test-scanner-789"
        mock_scanner_service.return_value = mock_service_instance

        with client.websocket_connect("/api/scanners") as websocket:
            scanner_request = {
                "type": "atr_gap_up",
                "params": {
                    "min_adv": 1000000,
                    "min_adr": 1.0,
                    "atr_multiplier": 2.0,
                    "min_volume": 100000,
                    "start_time": "09:30",
                    "end_time": "16:00",
                    "freq": "1min",
                },
            }
            websocket.send_text(json.dumps(scanner_request))

            response_data = websocket.receive_text()
            response = json.loads(response_data)

            assert "scanner_id" in response
            assert response["scanner_id"] == "test-scanner-789"


@pytest.mark.asyncio
async def test_websocket_scanner_invalid_request():
    client = TestClient(app)

    with patch("fastscanner.adapters.rest.scanner.RedisChannel"), patch(
        "fastscanner.adapters.rest.scanner.PartitionedCSVCandlesProvider"
    ), patch("fastscanner.adapters.rest.scanner.PolygonCandlesProvider"), patch(
        "fastscanner.adapters.rest.scanner.ScannerService"
    ):

        try:
            with client.websocket_connect("/api/scanners") as websocket:
                invalid_request = {"type": "atr_gap_up"}

                websocket.send_text(json.dumps(invalid_request))

        except Exception:
            pass


@pytest.mark.asyncio
async def test_websocket_scanner_different_scanner_types():
    client = TestClient(app)

    scanner_types = [
        {
            "type": "atr_gap_up",
            "params": {
                "min_adv": 1000000,
                "min_adr": 1.0,
                "atr_multiplier": 2.0,
                "min_volume": 100000,
                "start_time": "09:30",
                "end_time": "16:00",
                "freq": "1min",
            },
        }
    ]

    for i, scanner_config in enumerate(scanner_types):
        with patch("fastscanner.adapters.rest.scanner.RedisChannel"), patch(
            "fastscanner.adapters.rest.scanner.PartitionedCSVCandlesProvider"
        ), patch("fastscanner.adapters.rest.scanner.PolygonCandlesProvider"), patch(
            "fastscanner.adapters.rest.scanner.ScannerService"
        ) as mock_scanner_service:

            mock_service_instance = AsyncMock()
            mock_service_instance.subscribe_realtime.return_value = f"test-scanner-{i}"
            mock_scanner_service.return_value = mock_service_instance

            with client.websocket_connect("/api/scanners") as websocket:
                websocket.send_text(json.dumps(scanner_config))

                response_data = websocket.receive_text()
                response = json.loads(response_data)

                assert "scanner_id" in response
                assert response["scanner_id"] == f"test-scanner-{i}"


@pytest.mark.asyncio
async def test_websocket_scanner_handler_unit():
    mock_websocket = AsyncMock()

    handler = WebSocketScannerHandler(mock_websocket)
    handler.set_scanner_id("test-scanner-123")

    test_data = pd.Series(
        {
            "open": 100.0,
            "high": 105.0,
            "low": 95.0,
            "close": 102.0,
            "volume": 1000,
            "test_indicator": 150.0,
        },
        name=pd.Timestamp("2024-01-01 09:30:00"),
    )

    result = await handler.handle("AAPL", test_data, passed=True)

    mock_websocket.send_text.assert_called_once()
    sent_data = mock_websocket.send_text.call_args[0][0]
    message = json.loads(sent_data)

    assert message["symbol"] == "AAPL"
    assert message["scanner_id"] == "test-scanner-123"
    assert message["scan_time"] == "09:30"
    assert message["candle"]["close"] == 102.0
    assert message["candle"]["test_indicator"] == 150.0

    mock_websocket.reset_mock()
    result = await handler.handle("GOOGL", test_data, passed=False)

    mock_websocket.send_text.assert_not_called()

    pd.testing.assert_series_equal(result, test_data)


@pytest.mark.asyncio
async def test_websocket_scanner_handler_connection_error():

    mock_websocket = AsyncMock()
    mock_websocket.send_text.side_effect = Exception("Connection lost")

    # Create handler
    handler = WebSocketScannerHandler(mock_websocket)
    handler.set_scanner_id("test-scanner-123")

    # Create test data
    test_data = pd.Series(
        {
            "close": 102.0,
            "volume": 1000,
        },
        name=pd.Timestamp("2024-01-01 09:30:00"),
    )

    result = await handler.handle("AAPL", test_data, passed=True)

    pd.testing.assert_series_equal(result, test_data)
