import pandas as pd
import pytest

from fastscanner.services.indicators.ports import (
    CandleStore,
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
        df = self._data[symbol]
        return df[
            (df.index.date >= start_date) & (df.index.date <= end_date)  # type: ignore
        ]


class MockFundamentalDataStore(FundamentalDataStore):
    async def get(self, symbol):
        return None


class MockPublicHolidaysStore(PublicHolidaysStore):
    def get(self):
        return set()


@pytest.fixture
def candles():
    candle_store = CandleStoreTest()
    fundamental_store = MockFundamentalDataStore()
    holiday_store = MockPublicHolidaysStore()

    ApplicationRegistry.init(
        candles=candle_store, fundamentals=fundamental_store, holidays=holiday_store
    )

    yield candle_store
    ApplicationRegistry.reset()
