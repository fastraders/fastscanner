import os
import json
import tempfile
import pytest
from unittest.mock import patch, MagicMock

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
            "GicSector": "Information Technology"
        },
        "Earnings": {
            "History": {
                "2023-12-31": {},
                "2023-09-30": {}
            }
        },
        "SharesStats": {
            "PercentInsiders": 1.23,
            "PercentInstitutions": 65.4,
            "SharesFloat": 123456789
        }
    }


@pytest.fixture
def sample_market_cap():
    return {
        "0": {"date": "2023-12-31", "value": 1000000000},
        "1": {"date": "2023-09-30", "value": 900000000}
    }


@pytest.fixture
def store(tmp_path):
    store = EODHDFundamentalStore()
    store.CACHE_DIR = tmp_path
    return store


def test_get_from_cache(store, sample_fundamental_data, sample_market_cap):
    expected = FundamentalData(
        exchange="NASDAQ",
        country="USA",
        city="Cupertino",
        gic_industry="Tech Hardware",
        gic_sector="Information Technology",
        historical_market_cap={"2023-12-31": 1e9, "2023-09-30": 9e8},
        earnings_dates=["2023-09-30", "2023-12-31"],
        insiders_ownership_perc=1.23,
        institutional_ownership_perc=65.4,
        shares_float=123456789
    )
    cache_path = store._get_cache_path("AAPL.US")
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    with open(cache_path, "w") as f:
        json.dump(expected.__dict__, f)

    result = store.get("AAPL.US")
    assert result == expected


@patch.object(EODHDFundamentalStore, "_fetch_fundamentals")
@patch.object(EODHDFundamentalStore, "_fetch_market_cap")
def test_get_fetch_and_store(mock_market_cap, mock_fundamentals, store, sample_fundamental_data, sample_market_cap):
    mock_fundamentals.return_value = sample_fundamental_data
    mock_market_cap.return_value = sample_market_cap

    result = store.get("AAPL.US")

    assert result.exchange == "NASDAQ"
    assert result.city == "Cupertino"
    assert result.historical_market_cap["2023-12-31"] == 1e9

    path = store._get_cache_path("AAPL.US")
    assert os.path.exists(path)


@patch.object(EODHDFundamentalStore, "_fetch_fundamentals", return_value={})
@patch.object(EODHDFundamentalStore, "_fetch_market_cap", return_value={})
def test_get_fails_on_missing_data(mock_fundamentals, mock_market_cap, store):
    with pytest.raises(ValueError):
        store.get("AAPL.US")


@patch("fastscanner.adapters.fundamental.eodhd.json.load", side_effect=json.JSONDecodeError("msg", doc="", pos=0))
def test_load_cached_bad_json(mock_json, store):
    path = store._get_cache_path("AAPL.US")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write("bad-json")

    result = store._load_cached("AAPL.US")
    assert result is None



def test_store_and_load_roundtrip(store, sample_fundamental_data, sample_market_cap):
    data = store._parse_data(sample_fundamental_data, sample_market_cap)
    store._store("AAPL.US", data)

    loaded = store._load_cached("AAPL.US")
    assert loaded == data


def _get_cache_path(self, symbol: str) -> str:
    return os.path.join(self.CACHE_DIR, f"{symbol}.json")

EODHDFundamentalStore._get_cache_path = _get_cache_path
