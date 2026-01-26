import pandas as pd
import pytest

from fastscanner.services.indicators.ports import (
    Cache,
    CandleCol,
    CandleStore,
    FundamentalData,
    FundamentalDataStore,
    PublicHolidaysStore,
)
from fastscanner.services.registry import ApplicationRegistry


class CandleStoreTest(CandleStore):
    def __init__(self):
        self._data = {}

    def set_data(self, symbol, data):
        self._data[symbol] = data

    async def get(self, symbol, start_date, end_date, freq="1m"):
        if symbol not in self._data:
            return pd.DataFrame(index=pd.DatetimeIndex([]), columns=[CandleCol.COLUMNS])
        df = self._data[symbol]
        return df[
            (df.index.date >= start_date) & (df.index.date <= end_date)  # type: ignore
        ]


class MockFundamentalDataStore:
    async def get(self, symbol) -> FundamentalData:
        date_index = pd.date_range(start="2023-01-01", periods=3, freq="D").date
        return FundamentalData(
            "",
            "",
            "",
            "",
            "",
            "",
            pd.Series([1000000000.0, 1000000000.0, 1000000000.0], index=date_index),
            pd.DatetimeIndex([]),
            None,
            None,
            None,
            None,
        )


class MockPublicHolidaysStore:
    def get(self):
        return set()


class MockCache(Cache):
    def __init__(self):
        self._data = {}

    async def save(self, key: str, value: str) -> None:
        self._data[key] = value

    async def get(self, key: str) -> str:
        return self._data.get(key, "")


@pytest.fixture
def candles():
    candle_store = CandleStoreTest()
    fundamental_store = MockFundamentalDataStore()
    holiday_store = MockPublicHolidaysStore()
    cache = MockCache()

    ApplicationRegistry.init(
        candles=candle_store,
        fundamentals=fundamental_store,
        holidays=holiday_store,
        cache=cache,
    )

    yield candle_store
    ApplicationRegistry.reset()
