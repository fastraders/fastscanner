import json
import os
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from fastscanner.adapters.fundamental.eodhd import EODHDFundamentalStore
from fastscanner.services.indicators.ports import FundamentalData


@pytest.fixture
def sample_fundamental_data():
    return {
        "General": {
            "Type": "Common Stock",
            "Exchange": "NASDAQ",
            "CountryName": "USA",
            "AddressData": {"City": "Cupertino"},
            "GicIndustry": "Tech Hardware",
            "GicSector": "Information Technology",
        },
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
def sample_earnings_calendar_response():
    return {
        "earnings": [
            {"code": "AAPL.US", "report_date": "2023-10-26", "date": "2023-09-30"},
            {"code": "AAPL.US", "report_date": "2024-01-25", "date": "2023-12-31"},
            {"code": "MSFT.US", "report_date": "2023-10-24", "date": "2023-09-30"},
        ]
    }


@pytest.fixture
def store(tmp_path):
    BASE_URL = "api/demo"
    API_KEY = "demo"
    store = EODHDFundamentalStore(BASE_URL, API_KEY)
    store.CACHE_DIR = tmp_path / "fundamentals"
    store.RAW_CACHE_DIR = tmp_path / "fundamentals_raw"
    os.makedirs(store.CACHE_DIR, exist_ok=True)
    os.makedirs(store.RAW_CACHE_DIR, exist_ok=True)
    return store


@pytest.mark.asyncio
async def test_get_from_cache(store):
    expected = FundamentalData(
        type="Common Stock",
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
        beta=1,
        ipo_date=None,
        ceo_name=None,
        cfo_name=None,
    )

    cache_path = os.path.join(store.CACHE_DIR, "AAPL.json")
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    with open(cache_path, "w") as f:
        json.dump(
            {
                "type": expected.type,
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
                "beta": expected.beta,
                "ipo_date": expected.ipo_date,
                "ceo_name": expected.ceo_name,
                "cfo_name": expected.cfo_name,
            },
            f,
        )

    result = await store.get("AAPL")

    assert result.exchange == expected.exchange
    assert result.country == expected.country
    assert result.city == expected.city
    assert result.gic_industry == expected.gic_industry
    assert result.gic_sector == expected.gic_sector
    assert result.insiders_ownership_perc == expected.insiders_ownership_perc
    assert result.institutional_ownership_perc == expected.institutional_ownership_perc
    assert result.shares_float == expected.shares_float


@pytest.mark.asyncio
@patch("fastscanner.adapters.fundamental.eodhd.async_retry_request")
async def test_get_fetch_and_store(
    mock_retry_request, store, sample_fundamental_data, sample_market_cap
):
    mock_fundamentals = MagicMock()
    mock_fundamentals.status_code = 200
    mock_fundamentals.json.return_value = sample_fundamental_data

    mock_marketcap = MagicMock()
    mock_marketcap.status_code = 200
    mock_marketcap.json.return_value = sample_market_cap

    mock_earnings = MagicMock()
    mock_earnings.status_code = 200
    mock_earnings.json.return_value = {
        "earnings": [
            {"code": "AAPL.US", "report_date": "2023-10-26"},
            {"code": "AAPL.US", "report_date": "2024-01-25"},
        ]
    }

    mock_retry_request.side_effect = [mock_fundamentals, mock_marketcap, mock_earnings]

    result = await store.get("AAPL")

    assert result.exchange == "NASDAQ"
    assert result.city == "Cupertino"
    assert result.historical_market_cap[pd.Timestamp("2023-12-31").date()] == 1e9
    assert len(result.earnings_dates) == 2

    path = store._get_cache_path("AAPL")
    assert os.path.exists(path)


def test_load_cached_bad_json(store):
    path = store._get_cache_path("AAPL")
    os.makedirs(os.path.dirname(path), exist_ok=True)

    with open(path, "w") as f:
        f.write("bad-json")

    result = store._load_cached("AAPL")

    assert result is None


def test_store_and_load_roundtrip(store, sample_fundamental_data, sample_market_cap):
    earnings_dates = pd.DatetimeIndex(
        pd.to_datetime(["2023-10-26", "2024-01-25"]), name="report_date"
    )
    data = store._parse_data(sample_fundamental_data, sample_market_cap, earnings_dates)
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


@pytest.mark.asyncio
@patch("fastscanner.adapters.fundamental.eodhd.async_retry_request")
async def test_reload_stores_and_returns_fresh_data(
    mock_retry_request, store, sample_fundamental_data, sample_market_cap
):
    mock_fundamentals = MagicMock()
    mock_fundamentals.status_code = 200
    mock_fundamentals.json.return_value = sample_fundamental_data

    mock_marketcap = MagicMock()
    mock_marketcap.status_code = 200
    mock_marketcap.json.return_value = sample_market_cap

    mock_earnings = MagicMock()
    mock_earnings.status_code = 200
    mock_earnings.json.return_value = {
        "earnings": [
            {"code": "AAPL.US", "report_date": "2023-10-26"},
        ]
    }

    mock_retry_request.side_effect = [mock_fundamentals, mock_marketcap, mock_earnings]

    result = await store.reload("AAPL")

    assert isinstance(result, FundamentalData)
    assert result.exchange == "NASDAQ"
    assert result.city == "Cupertino"
    assert result.historical_market_cap[pd.Timestamp("2023-12-31").date()] == 1e9
    assert len(result.earnings_dates) == 1

    cache_path = store._get_cache_path("AAPL")
    assert os.path.exists(cache_path)

    with open(cache_path) as f:
        saved_data = json.load(f)
        assert saved_data["exchange"] == "NASDAQ"


def test_parse_earnings_records(store):
    records = [
        {"code": "AAPL.US", "report_date": "2023-10-26"},
        {"code": "AAPL.US", "report_date": "2024-01-25"},
        {"code": "AAPL.US", "report_date": "2023-10-26"},
    ]
    result = store._parse_earnings_records(records)

    assert len(result) == 2
    assert result[0] == pd.Timestamp("2023-10-26")
    assert result[1] == pd.Timestamp("2024-01-25")


def test_parse_earnings_records_empty(store):
    result = store._parse_earnings_records([])
    assert len(result) == 0


def test_group_earnings_by_symbol(store):
    records = [
        {"code": "AAPL.US", "report_date": "2023-10-26"},
        {"code": "MSFT.US", "report_date": "2023-10-24"},
        {"code": "AAPL.US", "report_date": "2024-01-25"},
    ]
    result = store._group_earnings_by_symbol(records)

    assert len(result) == 2
    assert len(result["AAPL"]) == 2
    assert len(result["MSFT"]) == 1


def test_update_cached_earnings(store, sample_fundamental_data, sample_market_cap):
    earnings_dates = pd.DatetimeIndex(
        pd.to_datetime(["2023-10-26"]), name="report_date"
    )
    data = store._parse_data(sample_fundamental_data, sample_market_cap, earnings_dates)
    store._store("AAPL", data)

    new_dates = pd.DatetimeIndex(
        pd.to_datetime(["2023-10-26", "2024-01-25"]), name="report_date"
    )
    store._update_cached_earnings("AAPL", new_dates)

    loaded = store._load_cached("AAPL")
    assert len(loaded.earnings_dates) == 2


@pytest.mark.asyncio
@patch("fastscanner.adapters.fundamental.eodhd.async_retry_request")
async def test_reload_earnings_no_cache(
    mock_retry_request, store, sample_fundamental_data, sample_market_cap
):
    earnings_dates = pd.DatetimeIndex(
        pd.to_datetime(["2023-10-26"]), name="report_date"
    )
    fd = store._parse_data(sample_fundamental_data, sample_market_cap, earnings_dates)
    store._store("AAPL", fd)
    store._store("MSFT", fd)

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "earnings": [
            {"code": "GOOG.US", "report_date": "2024-04-25"},
            {"code": "GOOG.US", "report_date": "2024-07-23"},
        ]
    }
    mock_retry_request.return_value = mock_response

    await store.reload_earnings(["GOOG"])

    raw_path = os.path.join(store.RAW_CACHE_DIR, "GOOG_earnings.json")
    assert os.path.exists(raw_path)


@pytest.mark.asyncio
@patch("fastscanner.adapters.fundamental.eodhd.async_retry_request")
async def test_reload_earnings_with_existing_cache_merges(
    mock_retry_request, store, sample_fundamental_data, sample_market_cap
):
    earnings_dates = pd.DatetimeIndex(
        pd.to_datetime(["2023-10-26"]), name="report_date"
    )
    fd = store._parse_data(sample_fundamental_data, sample_market_cap, earnings_dates)
    store._store("AAPL", fd)

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "earnings": [
            {"code": "AAPL.US", "report_date": "2024-01-25"},
            {"code": "AAPL.US", "report_date": "2024-04-25"},
        ]
    }
    mock_retry_request.return_value = mock_response

    await store.reload_earnings(["AAPL"])

    loaded = store._load_cached("AAPL")
    assert len(loaded.earnings_dates) == 3
    expected_dates = pd.to_datetime(["2023-10-26", "2024-01-25", "2024-04-25"])
    for expected in expected_dates:
        assert expected in loaded.earnings_dates
