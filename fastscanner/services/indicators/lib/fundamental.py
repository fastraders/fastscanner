from datetime import date, datetime
from typing import Any, Optional

import numpy as np
import pandas as pd

from ...registry import ApplicationRegistry


class DaysToEarningsIndicator:
    def __init__(self) -> None:
        self._last_date: dict[str, date] = {}
        self._last_days: dict[str, Optional[int]] = {}

    @classmethod
    def type(cls):
        return "days_to_earnings"

    def column_name(self):
        return self.type()

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

    async def extend_realtime(
        self, symbol: str, new_row: dict[str, Any]
    ) -> dict[str, Any]:
        timestamp = new_row["datetime"]
        assert isinstance(timestamp, datetime)
        new_date = timestamp.date()

        if (last_date := self._last_date.get(symbol)) is None or last_date != new_date:
            fundamentals = await ApplicationRegistry.fundamentals.get(symbol)
            earnings_dates = fundamentals.earnings_dates.date
            future_earnings = earnings_dates[earnings_dates >= new_date]
            if len(future_earnings) > 0:
                next_earnings = future_earnings[0]
                days = (next_earnings - new_date).days
            else:
                days = None
            self._last_days[symbol] = days
            self._last_date[symbol] = new_date

        value = self._last_days.get(symbol, None)
        if value is None:
            new_row[self.column_name()] = pd.NA
        else:
            new_row[self.column_name()] = int(value)

        return new_row


class DaysFromEarningsIndicator:
    def __init__(self) -> None:
        self._last_date: dict[str, date] = {}
        self._last_days: dict[str, Optional[int]] = {}

    @classmethod
    def type(cls):
        return "days_from_earnings"

    def column_name(self):
        return self.type()

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

    async def extend_realtime(
        self, symbol: str, new_row: dict[str, Any]
    ) -> dict[str, Any]:
        timestamp = new_row["datetime"]
        assert isinstance(timestamp, datetime)
        new_date = timestamp.date()

        if (last_date := self._last_date.get(symbol)) is None or last_date != new_date:
            fundamentals = await ApplicationRegistry.fundamentals.get(symbol)
            earnings_dates = fundamentals.earnings_dates.date
            past_earnings = earnings_dates[earnings_dates <= new_date]
            if len(past_earnings) > 0:
                last_earnings = past_earnings[-1]
                days = (new_date - last_earnings).days
            else:
                days = None
            self._last_days[symbol] = days
            self._last_date[symbol] = new_date

        value = self._last_days.get(symbol, None)
        if value is None:
            new_row[self.column_name()] = None
        else:
            new_row[self.column_name()] = int(value)
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

    async def extend_realtime(
        self, symbol: str, new_row: dict[str, Any]
    ) -> dict[str, Any]:
        timestamp = new_row["datetime"]
        assert isinstance(timestamp, datetime)
        new_date = timestamp.date()

        if (last_date := self._last_date.get(symbol)) is None or last_date != new_date:
            fundamentals = await ApplicationRegistry.fundamentals.get(symbol)
            historical_market_cap = fundamentals.historical_market_cap
            filtered_series = historical_market_cap[
                historical_market_cap.index <= new_date
            ]
            if not filtered_series.empty:
                market_cap = filtered_series.iloc[-1]
            else:
                market_cap = float("nan")
            self._last_market_cap[symbol] = market_cap
            self._last_date[symbol] = new_date

        value = self._last_market_cap.get(symbol)
        if value is not None and np.isfinite(value):
            new_row[self.column_name()] = value
        else:
            new_row[self.column_name()] = None

        return new_row
