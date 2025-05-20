import logging
import math
from datetime import date, datetime, time, timedelta
from enum import StrEnum
from operator import add
from typing import Any, Callable

import numpy as np
import pandas as pd

from fastscanner.pkg.datetime import split_freq

from ...registry import ApplicationRegistry
from ..ports import CandleCol
from ..utils import lookback_days

logger = logging.getLogger(__name__)


class CumulativeDailyVolumeIndicator:
    def __init__(self):
        self._last_date: dict[str, date] = {}
        self._last_volume: dict[str, float] = {}

    def lookback_days(self) -> int:
        return 0

    @classmethod
    def type(cls):
        return "cumulative_daily_volume"

    def column_name(self):
        return self.type()

    async def extend(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        volume = df[CandleCol.VOLUME]
        assert isinstance(volume.index, pd.DatetimeIndex)
        cum_volume = volume.groupby(volume.index.date).cumsum()
        df[self.column_name()] = cum_volume
        return df

    async def extend_realtime(self, symbol: str, new_row: pd.Series) -> pd.Series:
        volume = new_row[CandleCol.VOLUME]
        assert isinstance(new_row.name, datetime)
        last_date = self._last_date.get(symbol)
        if last_date is not None and last_date == new_row.name.date():
            volume += self._last_volume.get(symbol, 0)
        new_row[self.column_name()] = volume
        self._last_date[symbol] = new_row.name.date()
        self._last_volume[symbol] = volume
        return new_row


class CumulativeOperation(StrEnum):
    MIN = "min"
    MAX = "max"
    SUM = "sum"

    def label(self) -> str:
        return {
            self.MIN: "lowest",
            self.MAX: "highest",
            self.SUM: "total",
        }[self]

    def pandas_func(self) -> str:
        return {
            self.MIN: "cummin",
            self.MAX: "cummax",
            self.SUM: "cumsum",
        }[self]

    def func(self) -> Callable[[float, float], float]:
        return {
            self.MIN: min,
            self.MAX: max,
            self.SUM: add,  # Use addition instead of sum() which expects an iterable
        }[self]

    def initial_value(self) -> float:
        return {
            self.MIN: np.inf,
            self.MAX: -np.inf,
            self.SUM: 0,
        }[self]


class PremarketCumulativeIndicator:
    def __init__(self, candle_col: str, op: str):
        self._candle_col = candle_col
        self._op = CumulativeOperation(op)
        self._last_date: dict[str, date] = {}
        self._last_value: dict[str, float] = {}

    def lookback_days(self) -> int:
        return 0

    @classmethod
    def type(cls):
        return "premarket_cumulative"

    def column_name(self):
        return f"premarket_{self._op.label()}_{self._candle_col}"

    async def extend(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        values = df[self._candle_col]
        assert isinstance(values.index, pd.DatetimeIndex)
        cum_values = values.groupby(values.index.date).agg(self._op.pandas_func())
        cum_values[cum_values.index.time >= time(9, 30)] = pd.NA  # type: ignore
        cum_values = cum_values.ffill()
        df[self.column_name()] = cum_values
        return df

    async def extend_realtime(self, symbol: str, new_row: pd.Series) -> pd.Series:
        assert isinstance(new_row.name, datetime)
        last_date = self._last_date.get(symbol)
        last_value = self._last_value.get(symbol)
        if new_row.name.time() >= time(9, 30):
            if last_date is None or last_date != new_row.name.date():
                new_row[self.column_name()] = pd.NA
            else:
                new_row[self.column_name()] = last_value
            return new_row

        value = new_row[self._candle_col]
        if last_value is not None and last_date == new_row.name.date():
            value = self._op.func()(value, last_value)

        new_row[self.column_name()] = value
        self._last_date[symbol] = new_row.name.date()
        self._last_value[symbol] = value
        return new_row


class ATRIndicator:
    def __init__(self, period: int, freq: str):
        self._period = period
        self._last_atr: dict[str, float] = {}
        self._last_close: dict[str, float] = {}
        self._freq = freq

    def lookback_days(self) -> int:
        n, unit = split_freq(self._freq)
        if unit == "d":
            n *= 390
        elif unit == "h":
            n *= 390 / 6.5

        # 390 minute candles in a day and a security factor of 2
        return math.ceil(2 * self._period * n / 390)

    @classmethod
    def type(cls):
        return "atr"

    def column_name(self):
        return f"atr_{self._period}"

    async def extend(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        assert isinstance(df.index, pd.DatetimeIndex)
        tr0 = (df[CandleCol.HIGH] - df[CandleCol.LOW]).abs()
        tr1 = (df[CandleCol.HIGH] - df[CandleCol.CLOSE].shift(1)).abs()
        tr2 = (df[CandleCol.LOW] - df[CandleCol.CLOSE].shift(1)).abs()
        col_name = self.column_name()
        df[col_name] = (
            pd.concat([tr0, tr1, tr2], axis=1)
            .max(axis=1)
            .ewm(alpha=1 / self._period)
            .mean()
            .round(3)
        )
        return df

    async def extend_realtime(self, symbol: str, new_row: pd.Series) -> pd.Series:
        assert isinstance(new_row.name, datetime)
        last_close = self._last_close.get(symbol)
        if last_close is None:
            new_row[self.column_name()] = pd.NA
            self._last_close[symbol] = new_row[CandleCol.CLOSE]
            return new_row

        tr0: float = abs(new_row[CandleCol.HIGH] - new_row[CandleCol.LOW])
        tr1: float = abs(new_row[CandleCol.HIGH] - last_close)
        tr2: float = abs(new_row[CandleCol.LOW] - last_close)
        tr: float = max(tr0, tr1, tr2)

        last_atr = self._last_atr.get(symbol)
        if last_atr is None:
            new_row[self.column_name()] = tr
            self._last_close[symbol] = new_row[CandleCol.CLOSE]
            self._last_atr[symbol] = tr
            return new_row

        alpha = 1 / self._period
        atr = last_atr * (1 - alpha) + tr * alpha
        new_row[self.column_name()] = round(atr, 3)
        self._last_atr[symbol] = atr
        self._last_close[symbol] = new_row[CandleCol.CLOSE]
        return new_row


class PositionInRangeIndicator:
    """
    Normalizes the current close between the highest and lowest values of the last n days.
    """

    def __init__(self, n_days: int):
        self._n_days = n_days
        self._high_n_days: dict[str, list[float]] = {}
        self._low_n_days: dict[str, list[float]] = {}
        self._last_date: dict[str, date] = {}

    @classmethod
    def type(cls):
        return "position_in_range"

    def column_name(self):
        return f"position_in_range_{self._n_days}"

    def lookback_days(self) -> int:
        return 0

    async def _get_high_low_n_days(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        start_date = lookback_days(df.index[0].date(), self._n_days)
        end_date = df.index[-1].date() - timedelta(days=1)
        daily_df = await ApplicationRegistry.candles.get(
            symbol, start_date, end_date, "1d"
        )
        if daily_df.shape[0] < self._n_days:
            logger.warning(
                f"Requested {self._n_days} days of data for {symbol} on {start_date}-{end_date}, got {daily_df.shape[0]} days."
            )
        return daily_df

    async def extend(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        daily_df = await self._get_high_low_n_days(symbol, df)
        if daily_df.empty:
            df[self.column_name()] = pd.NA
            return df

        daily_df = (
            daily_df[[CandleCol.HIGH, CandleCol.LOW]]
            .rolling(self._n_days)
            .agg(
                {
                    CandleCol.HIGH: "max",
                    CandleCol.LOW: "min",
                }
            )
            .rename(
                columns={
                    CandleCol.HIGH: "_highest",
                    CandleCol.LOW: "_lowest",
                }
            )
            .set_index(daily_df.index.date)  # type: ignore
        )

        daily_df.loc[df.index[-1].date(), ["_highest", "_lowest"]] = pd.NA
        daily_df = daily_df.shift(1)

        df["date"] = df.index.date  # type: ignore
        df = df.join(daily_df, on="date")
        df[self.column_name()] = (df[CandleCol.CLOSE] - df["_lowest"]) / (
            df["_highest"] - df["_lowest"]
        )

        return df.drop(columns=["date", "_lowest", "_highest"])

    async def extend_realtime(self, symbol: str, new_row: pd.Series) -> pd.Series:
        assert isinstance(new_row.name, datetime)
        last_date = self._last_date.get(symbol)
        if last_date is None or last_date != new_row.name.date():
            daily_df = await self._get_high_low_n_days(symbol, new_row.to_frame().T)
            self._last_date[symbol] = new_row.name.date()  # type: ignore
            self._low_n_days[symbol] = daily_df[CandleCol.LOW].to_list()[
                -self._n_days :
            ]
            self._high_n_days[symbol] = daily_df[CandleCol.HIGH].to_list()[
                -self._n_days :
            ]

        last_high = max(self._high_n_days[symbol])
        last_low = min(self._low_n_days[symbol])
        last_close = new_row[CandleCol.CLOSE]
        range_val = last_high - last_low
        if range_val == 0:
            new_row[self.column_name()] = pd.NA
        else:
            new_row[self.column_name()] = (last_close - last_low) / range_val

        return new_row
