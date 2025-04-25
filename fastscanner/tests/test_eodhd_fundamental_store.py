import json
import os
import tempfile
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from fastscanner.adapters.fundamental.eodhd import EODHDFundamentalStore
from fastscanner.services.indicators.ports import FundamentalData


@pytest.fixture
def sample_fundamental_data():
    return {
        "General": {
            "Exchange": "NASDAQ",
            "CountryName": "USA",
            "AddressData": {"City": "Cupertino"},
            "GicIndustry": "Tech Hardware",
            "GicSector": "Information Technology",
        },
        "Earnings": {"History": {"2023-12-31": {}, "2023-09-30": {}}},
        "SharesStats": {
            "PercentInsiders": 1.23,
            "PercentInstitutions": 65.4,
            "SharesFloat": 123456789,
        },
    }


@pytest.fixture
def sample_market_cap():
    return {
        "0": {"date": "2023-12-31", "value": 1000000000},
        "1": {"date": "2023-09-30", "value": 900000000},
    }


@pytest.fixture
def store(tmp_path):
    BASE_URL = "api/demo"
    API_KEY = "demo"
    store = EODHDFundamentalStore(BASE_URL, API_KEY)
    store.CACHE_DIR = tmp_path
    return store


def test_get_from_cache(store):
    expected = FundamentalData(
        exchange="NASDAQ",
        country="USA",
        city="Cupertino",
        gic_industry="Tech Hardware",
        gic_sector="Information Technology",
        historical_market_cap=pd.Series(
            {"2023-12-31": 1e9, "2023-09-30": 9e8}, dtype="float"
        ),
        earnings_dates=pd.to_datetime(["2023-09-30", "2023-12-31"]),
        insiders_ownership_perc=1.23,
        institutional_ownership_perc=65.4,
        shares_float=123456789,
    )

    cache_path = os.path.join(store.CACHE_DIR, "AAPL.json")
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    with open(cache_path, "w") as f:
        json.dump(
            {
                "exchange": expected.exchange,
                "country": expected.country,
                "city": expected.city,
                "gic_industry": expected.gic_industry,
                "gic_sector": expected.gic_sector,
                "historical_market_cap": expected.historical_market_cap.to_dict(),
                "earnings_dates": expected.earnings_dates.strftime("%Y-%m-%d").tolist(),
                "insiders_ownership_perc": expected.insiders_ownership_perc,
                "institutional_ownership_perc": expected.institutional_ownership_perc,
                "shares_float": expected.shares_float,
            },
            f,
        )

    result = store.get("AAPL")

    assert result.exchange == expected.exchange
    assert result.country == expected.country
    assert result.city == expected.city
    assert result.gic_industry == expected.gic_industry
    assert result.gic_sector == expected.gic_sector
    assert result.insiders_ownership_perc == expected.insiders_ownership_perc
    assert result.institutional_ownership_perc == expected.institutional_ownership_perc
    assert result.shares_float == expected.shares_float


@patch("fastscanner.adapters.fundamental.eodhd.retry_request")
def test_get_fetch_and_store(
    mock_retry_request, store, sample_fundamental_data, sample_market_cap
):
    mock_fundamentals = MagicMock()
    mock_fundamentals.status_code = 200
    mock_fundamentals.json.return_value = sample_fundamental_data

    mock_marketcap = MagicMock()
    mock_marketcap.status_code = 200
    mock_marketcap.json.return_value = sample_market_cap

    mock_retry_request.side_effect = [mock_fundamentals, mock_marketcap]

    result = store.get("AAPL")

    assert result.exchange == "NASDAQ"
    assert result.city == "Cupertino"
    assert result.historical_market_cap[pd.Timestamp("2023-12-31").date()] == 1e9

    path = store._get_cache_path("AAPL")
    assert os.path.exists(path)


@patch("httpx.Client.get")
def test_get_handles_missing_data_gracefully(mock_httpx_get, store):
    empty_fundamentals = MagicMock()
    empty_fundamentals.status_code = 200
    empty_fundamentals.json.return_value = {}

    empty_market_cap = MagicMock()
    empty_market_cap.status_code = 200
    empty_market_cap.json.return_value = {}

    mock_httpx_get.side_effect = [empty_fundamentals, empty_market_cap]

    result = store.get("AAPL")

    assert result is not None
    assert result.exchange == ""
    assert isinstance(result.historical_market_cap, pd.Series)
    assert result.historical_market_cap.empty
    assert isinstance(result.earnings_dates, pd.DatetimeIndex)
    assert len(result.earnings_dates) == 0


def test_load_cached_bad_json(store):
    path = store._get_cache_path("AAPL")
    os.makedirs(os.path.dirname(path), exist_ok=True)

    with open(path, "w") as f:
        f.write("bad-json")

    result = store._load_cached("AAPL")

    assert result is None


def test_store_and_load_roundtrip(store, sample_fundamental_data, sample_market_cap):
    data = store._parse_data(sample_fundamental_data, sample_market_cap)
    store._store("AAPL", data)

    loaded = store._load_cached("AAPL")

    assert loaded.exchange == data.exchange
    assert loaded.country == data.country
    assert loaded.city == data.city
    assert loaded.gic_industry == data.gic_industry
    assert loaded.gic_sector == data.gic_sector
    assert loaded.insiders_ownership_perc == data.insiders_ownership_perc
    assert loaded.institutional_ownership_perc == data.institutional_ownership_perc
    assert loaded.shares_float == data.shares_float

    pd.testing.assert_series_equal(
        loaded.historical_market_cap.sort_index(),
        data.historical_market_cap.sort_index(),
    )
    pd.testing.assert_index_equal(
        loaded.earnings_dates.sort_values(), data.earnings_dates.sort_values()
    )


@patch("fastscanner.adapters.fundamental.eodhd.retry_request")
def test_reload_stores_and_returns_fresh_data(
    mock_retry_request, store, sample_fundamental_data, sample_market_cap
):
    mock_fundamentals = MagicMock()
    mock_fundamentals.status_code = 200
    mock_fundamentals.json.return_value = sample_fundamental_data

    mock_marketcap = MagicMock()
    mock_marketcap.status_code = 200
    mock_marketcap.json.return_value = sample_market_cap

    mock_retry_request.side_effect = [mock_fundamentals, mock_marketcap]

    result = store.reload("AAPL")

    assert isinstance(result, FundamentalData)
    assert result.exchange == "NASDAQ"
    assert result.city == "Cupertino"
    assert result.historical_market_cap[pd.Timestamp("2023-12-31").date()] == 1e9

    cache_path = store._get_cache_path("AAPL")
    assert os.path.exists(cache_path)

    with open(cache_path) as f:
        saved_data = json.load(f)
        assert saved_data["exchange"] == "NASDAQ"
