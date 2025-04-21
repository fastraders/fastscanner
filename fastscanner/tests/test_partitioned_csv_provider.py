import os
import json
from datetime import date, datetime, time, timedelta 
import pandas as pd
import pytest
from unittest.mock import MagicMock, patch
from fastscanner.pkg.localize import LOCAL_TIMEZONE_STR
import pytz

tz = pytz.timezone(LOCAL_TIMEZONE_STR)
from fastscanner.adapters.candle.partitioned_csv import PartitionedCSVBarsProvider
from fastscanner.services.indicators.ports import CandleCol

SYMBOL = "AAPL"
FREQ = "1min"
UNIT = "min"
TEST_KEY = datetime.now().date().isoformat()
SAMPLE_DF = pd.DataFrame({
    CandleCol.DATETIME: pd.date_range("2023-01-01 00:00", periods=5, freq="2min", tz=tz)
}).set_index(CandleCol.DATETIME)


SAMPLE_DF[CandleCol.OPEN] = [1, 2, 3, 4, 5]
SAMPLE_DF[CandleCol.HIGH] = [1, 2, 3, 4, 5]
SAMPLE_DF[CandleCol.LOW] = [1, 2, 3, 4, 5]
SAMPLE_DF[CandleCol.CLOSE] = [1, 2, 3, 4, 5]
SAMPLE_DF[CandleCol.VOLUME] = [100, 200, 300, 400, 500]


@pytest.fixture
def provider(tmp_path):
    provider = PartitionedCSVBarsProvider()
    provider.CACHE_DIR = str(tmp_path / "candles")
    return provider


def test_save_and_load_cache(provider):
    provider._save_cache(SYMBOL, UNIT, TEST_KEY, FREQ, SAMPLE_DF)
    path = provider._partition_path(SYMBOL, TEST_KEY, UNIT, FREQ)
    assert os.path.exists(path)

    df_loaded = pd.read_csv(path)
    assert len(df_loaded) == 5
    assert CandleCol.OPEN in df_loaded.columns


@patch("fastscanner.adapters.candle.partitioned_csv.httpx.Client")
def test_fetch_and_cache_new_data(mock_client, provider):
    mock_fetch = MagicMock(return_value=SAMPLE_DF) 
    provider._fetch = mock_fetch

    df = provider._cache(SYMBOL, TEST_KEY, UNIT, FREQ)

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 5
    mock_fetch.assert_called_once()


def test_partition_path(provider):
    path = provider._partition_path(SYMBOL, TEST_KEY, UNIT, FREQ)
    assert f"{FREQ}_{TEST_KEY}.csv" in path


def test_partition_keys(provider):
    idx = pd.date_range("2023-01-01", "2023-01-10", freq="1h")
    keys = provider._partition_keys(idx, UNIT)
    assert isinstance(keys, pd.Series)
    assert not keys.empty


def test_range_from_key_min(provider):
    start, end = provider._range_from_key("2023-01-01", UNIT)
    assert isinstance(start, date)
    assert (end - start).days == 6


def test_range_from_key_hour(provider):
    start, end = provider._range_from_key("2023-01", "h")
    assert start.month == 1
    assert end.month == 1
