import pytest
from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi.testclient import TestClient
from fastapi import status

from fastscanner.adapters.rest.main import app
from fastscanner.adapters.rest.scanner import get_scanner_service
from fastscanner.services.scanners.lib import ScannersLibrary


@pytest.fixture
def client(scanner_service):
    service, channel = scanner_service
    app.dependency_overrides[get_scanner_service] = lambda: service
    client = TestClient(app)
    yield client
    app.dependency_overrides.clear()


def test_scan_endpoint_success(client, scanner_service):
    service, channel = scanner_service

    request_data = {
        "start": "2023-01-01",
        "end": "2023-01-10",
        "freq": "1D",
        "type": "dummy_scanner",
        "params": {"min_value": 50.0, "max_value": 200.0},
    }

    response = client.post("/api/scanners/scan", json=request_data)

    assert response.status_code == status.HTTP_200_OK

    response_data = response.json()
    assert "results" in response_data
    assert "total_symbols" in response_data
    assert "scanner_type" in response_data
    assert response_data["scanner_type"] == "dummy_scanner"
    assert response_data["total_symbols"] == 3
    assert isinstance(response_data["results"], list)


def test_scan_endpoint_with_results(client, scanner_service):
    service, channel = scanner_service

    request_data = {
        "start": "2023-01-01",
        "end": "2023-01-10",
        "freq": "1D",
        "type": "dummy_scanner",
        "params": {},
    }

    response = client.post("/api/scanners/scan", json=request_data)

    assert response.status_code == status.HTTP_200_OK

    response_data = response.json()
    results = response_data["results"]

    assert isinstance(results, list)


def test_scan_endpoint_invalid_date_format(client, scanner_service):
    service, channel = scanner_service

    request_data = {
        "start": "invalid-date",
        "end": "2023-01-10",
        "freq": "1D",
        "type": "dummy_scanner",
        "params": {},
    }

    response = client.post("/api/scanners/scan", json=request_data)

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


def test_scan_endpoint_missing_required_fields(client, scanner_service):
    service, channel = scanner_service

    request_data = {
        "start": "2023-01-01",
    }

    response = client.post("/api/scanners/scan", json=request_data)

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


def test_scan_endpoint_different_frequencies(client, scanner_service):
    service, channel = scanner_service

    frequencies = ["1min", "5min", "15min", "1H", "1D"]

    for freq in frequencies:
        request_data = {
            "start": "2023-01-01",
            "end": "2023-01-10",
            "freq": freq,
            "type": "dummy_scanner",
            "params": {},
        }

        response = client.post("/api/scanners/scan", json=request_data)

        assert (
            response.status_code == status.HTTP_200_OK
        ), f"Failed for frequency {freq}"
