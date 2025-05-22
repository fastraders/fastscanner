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

    def lookback_days(self) -> int:
        return 0

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

        df["date"] = df.index.date
        df = df.join(date_to_earnings, on="date")
        return df.drop(columns=["date"])

    async def extend_realtime(self, symbol: str, new_row: pd.Series) -> pd.Series:
        assert isinstance(new_row.name, pd.Timestamp)
        new_date = new_row.name.date()
        if (last_date := self._last_date.get(symbol)) is None or last_date != new_date:
            new_row = (await self.extend(symbol, new_row.to_frame().T)).iloc[0]
            self._last_days[symbol] = new_row[self.column_name()]
            self._last_date[symbol] = new_date

        new_row[self.column_name()] = self._last_days[symbol]
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

    def lookback_days(self) -> int:
        return 0

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
        df["date"] = df.index.date
        df = df.join(date_from_earnings, on="date")
        return df.drop(columns=["date"])

    async def extend_realtime(self, symbol: str, new_row: pd.Series) -> pd.Series:
        assert isinstance(new_row.name, pd.Timestamp)
        new_date = new_row.name.date()
        if (last_date := self._last_date.get(symbol)) is None or last_date != new_date:
            new_row = (await self.extend(symbol, new_row.to_frame().T)).iloc[0]
            self._last_days[symbol] = new_row[self.column_name()]
            self._last_date[symbol] = new_date

        new_row[self.column_name()] = self._last_days[symbol]
        return new_row
