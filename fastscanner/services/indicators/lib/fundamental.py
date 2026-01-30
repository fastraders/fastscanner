import json
from datetime import date

import numpy as np
import pandas as pd

from ...registry import ApplicationRegistry


class DaysToEarningsIndicator:
    def __init__(self) -> None:
        self._last_date: dict[str, date] = {}
        self._last_days: dict[str, int] = {}

    @classmethod
    def type(cls):
        return "days_to_earnings"

    def column_name(self):
        return self.type()

    async def save_to_cache(self):
        await ApplicationRegistry.cache.save(
            f"indicator:{self.column_name()}",
            json.dumps(
                {
                    "last_date": {k: v.isoformat() for k, v in self._last_date.items()},
                    "last_days": self._last_days,
                }
            ),
        )

    async def load_from_cache(self, symbol: str | None = None):
        indicator_data = await ApplicationRegistry.cache.get(
            f"indicator:{self.column_name()}"
        )
        indicator_data = json.loads(indicator_data)
        self._last_date = {
            k: date.fromisoformat(v)
            for k, v in indicator_data["last_date"].items()
            if symbol is None or k == symbol
        }
        self._last_days = {
            k: v
            for k, v in indicator_data["last_days"].items()
            if symbol is None or k == symbol
        }

    async def extend(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        fundamentals = await ApplicationRegistry.fundamentals.get(symbol)
        assert isinstance(df.index, pd.DatetimeIndex)

        earnings_dates = fundamentals.earnings_dates.date
        unique_dates = np.unique(df.index.date)

        date_to_earnings = pd.Series(
            np.nan, index=unique_dates, dtype=float, name=self.column_name()
        )
        for date in unique_dates:
            earnings_dates = earnings_dates[earnings_dates >= date]

            if len(earnings_dates) > 0:
                next_earnings = earnings_dates[0]
                days = (next_earnings - date).days
            else:
                break

            date_to_earnings[date] = days

        df.loc[:, "date"] = df.index.date
        df = df.join(date_to_earnings, on="date")
        return df.drop(columns=["date"])

    async def extend_realtime(self, symbol: str, new_row: pd.Series) -> pd.Series:
        assert isinstance(new_row.name, pd.Timestamp)
        new_date = new_row.name.date()
        if (last_date := self._last_date.get(symbol)) is None or last_date != new_date:
            new_row = (await self.extend(symbol, new_row.to_frame().T)).iloc[0]
            self._last_date[symbol] = new_date
            self._last_days.pop(symbol, None)
            if pd.notna(value := new_row[self.column_name()]):
                self._last_days[symbol] = int(value)

        new_row[self.column_name()] = self._last_days.get(symbol, pd.NA)
        return new_row


class DaysFromEarningsIndicator:
    def __init__(self) -> None:
        self._last_date: dict[str, date] = {}
        self._last_days: dict[str, int] = {}

    @classmethod
    def type(cls):
        return "days_from_earnings"

    def column_name(self):
        return self.type()

    async def save_to_cache(self):
        await ApplicationRegistry.cache.save(
            f"indicator:{self.column_name()}",
            json.dumps(
                {
                    "last_date": {k: v.isoformat() for k, v in self._last_date.items()},
                    "last_days": self._last_days,
                }
            ),
        )

    async def load_from_cache(self, symbol: str | None = None):
        indicator_data = await ApplicationRegistry.cache.get(
            f"indicator:{self.column_name()}"
        )
        indicator_data = json.loads(indicator_data)
        self._last_date = {
            k: date.fromisoformat(v)
            for k, v in indicator_data["last_date"].items()
            if symbol is None or k == symbol
        }
        self._last_days = {
            k: v
            for k, v in indicator_data["last_days"].items()
            if symbol is None or k == symbol
        }

    async def extend(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        fundamentals = await ApplicationRegistry.fundamentals.get(symbol)
        assert isinstance(df.index, pd.DatetimeIndex)

        earnings_dates = fundamentals.earnings_dates.date
        unique_dates = np.unique(df.index.date)

        date_from_earnings = pd.Series(
            np.nan, index=unique_dates, dtype=float, name=self.column_name()
        )
        for date in unique_dates[::-1]:
            earning_dates = earnings_dates[earnings_dates <= date]

            if len(earning_dates) > 0:
                last_earnings = earning_dates[-1]
                days = (date - last_earnings).days
            else:
                break

            date_from_earnings[date] = days
        df.loc[:, "date"] = df.index.date
        df = df.join(date_from_earnings, on="date")
        return df.drop(columns=["date"])

    async def extend_realtime(self, symbol: str, new_row: pd.Series) -> pd.Series:
        assert isinstance(new_row.name, pd.Timestamp)
        new_date = new_row.name.date()
        if (last_date := self._last_date.get(symbol)) is None or last_date != new_date:
            new_row = (await self.extend(symbol, new_row.to_frame().T)).iloc[0]
            self._last_date[symbol] = new_date
            self._last_days.pop(symbol, None)
            if pd.notna(value := new_row[self.column_name()]):
                self._last_days[symbol] = int(value)

        new_row[self.column_name()] = self._last_days.get(symbol, pd.NA)
        return new_row


class MarketCapIndicator:
    def __init__(self) -> None:
        self._last_date: dict[str, date] = {}
        self._last_market_cap: dict[str, float] = {}

    @classmethod
    def type(cls):
        return "market_cap"

    def column_name(self):
        return self.type()

    async def save_to_cache(self):
        await ApplicationRegistry.cache.save(
            f"indicator:{self.column_name()}",
            json.dumps(
                {
                    "last_date": {k: v.isoformat() for k, v in self._last_date.items()},
                    "last_market_cap": self._last_market_cap,
                }
            ),
        )

    async def load_from_cache(self, symbol: str | None = None):
        indicator_data = await ApplicationRegistry.cache.get(
            f"indicator:{self.column_name()}"
        )
        indicator_data = json.loads(indicator_data)
        self._last_date = {
            k: date.fromisoformat(v)
            for k, v in indicator_data["last_date"].items()
            if symbol is None or k == symbol
        }
        self._last_market_cap = {
            k: v
            for k, v in indicator_data["last_market_cap"].items()
            if symbol is None or k == symbol
        }

    async def extend(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        fundamentals = await ApplicationRegistry.fundamentals.get(symbol)
        assert isinstance(df.index, pd.DatetimeIndex)

        historical_market_cap = fundamentals.historical_market_cap

        unique_dates = np.unique(df.index.date)

        date_to_market_cap = pd.Series(
            np.nan, index=unique_dates, dtype=float, name=self.column_name()
        )

        for date in unique_dates:
            filtered_series = historical_market_cap[historical_market_cap.index <= date]

            if not filtered_series.empty:
                market_cap = filtered_series.iloc[-1]
                date_to_market_cap[date] = market_cap

        df.loc[:, "date"] = df.index.date
        df = df.join(date_to_market_cap, on="date")
        return df.drop(columns=["date"])

    async def extend_realtime(self, symbol: str, new_row: pd.Series) -> pd.Series:
        assert isinstance(new_row.name, pd.Timestamp)
        new_date = new_row.name.date()

        if (last_date := self._last_date.get(symbol)) is None or last_date != new_date:
            new_row = (await self.extend(symbol, new_row.to_frame().T)).iloc[0]
            self._last_date[symbol] = new_date
            self._last_market_cap.pop(symbol, None)
            if pd.notna(value := new_row[self.column_name()]):
                self._last_market_cap[symbol] = float(value)

        new_row[self.column_name()] = self._last_market_cap.get(symbol, pd.NA)
        return new_row
