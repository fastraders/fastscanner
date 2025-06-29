import pandas as pd
import pytest

from fastscanner.services.indicators.ports import (
    CandleCol,
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
        if symbol not in self._data:
            return pd.DataFrame(index=pd.DatetimeIndex([]), columns=[CandleCol.COLUMNS])
        df = self._data[symbol]
        return df[
            (df.index.date >= start_date) & (df.index.date <= end_date)  # type: ignore
        ]


class MockFundamentalData:
    def __init__(self):
        date_index = pd.date_range(start="2023-01-01", periods=3, freq="D").date
        self.historical_market_cap = pd.Series(
            [1000000000, 1000000000, 1000000000], index=date_index
        )


class MockFundamentalDataStore(FundamentalDataStore):
    async def get(self, symbol):
        return MockFundamentalData()


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
