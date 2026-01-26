import json
import logging
import math
from datetime import date, datetime, time, timedelta
from enum import StrEnum
from operator import add
from typing import TYPE_CHECKING, Any, Callable, Literal

import numpy as np
import pandas as pd

from fastscanner.pkg.clock import ClockRegistry, split_freq
from fastscanner.services.indicators.lib.daily import (
    DailyATRIndicator,
    PrevDayIndicator,
)

from ...registry import ApplicationRegistry
from ..ports import Cache, CandleCol
from ..utils import lookback_days

if TYPE_CHECKING:
    from fastscanner.services.indicators.lib import Indicator

logger = logging.getLogger(__name__)


class CumulativeDailyVolumeIndicator:
    def __init__(self):
        self._last_date: dict[str, date] = {}
        self._last_volume: dict[str, float] = {}

    @classmethod
    def type(cls):
        return "cumulative_daily_volume"

    def column_name(self):
        return self.type()

    async def save_to_cache(self):
        await ApplicationRegistry.cache.save(
            f"indicator:{self.column_name()}",
            json.dumps(
                {
                    "last_volume": self._last_volume,
                    "last_date": {k: v.isoformat() for k, v in self._last_date.items()},
                }
            ),
        )

    async def load_from_cache(self):
        indicator_data = await ApplicationRegistry.cache.get(
            f"indicator:{self.column_name()}"
        )
        indicator_data = json.loads(indicator_data)
        self._last_date = {
            k: date.fromisoformat(v) for k, v in indicator_data["last_date"].items()
        }
        self._last_volume = indicator_data["last_volume"]

    async def extend(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        volume = df[CandleCol.VOLUME]
        assert isinstance(volume.index, pd.DatetimeIndex)
        cum_volume = volume.groupby(volume.index.date).cumsum()
        df.loc[:, self.column_name()] = cum_volume
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

    @classmethod
    def type(cls):
        return "premarket_cumulative"

    def column_name(self):
        return f"premarket_{self._op.label()}_{self._candle_col}"

    async def save_to_cache(self):
        await ApplicationRegistry.cache.save(
            f"indicator:{self.column_name()}",
            json.dumps(
                {
                    "last_value": self._last_value,
                    "last_date": {k: v.isoformat() for k, v in self._last_date.items()},
                }
            ),
        )

    async def load_from_cache(self):
        indicator_data = await ApplicationRegistry.cache.get(
            f"indicator:{self.column_name()}"
        )
        indicator_data = json.loads(indicator_data)
        self._last_date = {
            k: date.fromisoformat(v) for k, v in indicator_data["last_date"].items()
        }
        self._last_value = indicator_data["last_value"]

    async def extend(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            df.loc[:, self.column_name()] = 0.0
            return df

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


class CumulativeIndicator:
    def __init__(self, candle_col: str, op: str):
        self._candle_col = candle_col
        self._op = CumulativeOperation(op)
        self._last_value: dict[str, float] = {}

    @classmethod
    def type(cls):
        return "cumulative"

    def column_name(self):
        return f"{self._op.label()}_{self._candle_col}"

    async def save_to_cache(self):
        await ApplicationRegistry.cache.save(
            f"indicator:{self.column_name()}",
            json.dumps({"last_value": self._last_value}),
        )

    async def load_from_cache(self):
        indicator_data = await ApplicationRegistry.cache.get(
            f"indicator:{self.column_name()}"
        )
        indicator_data = json.loads(indicator_data)
        self._last_value = indicator_data["last_value"]

    async def extend(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        values = df[self._candle_col]
        assert isinstance(values.index, pd.DatetimeIndex)
        df[self.column_name()] = values.groupby(values.index.date).agg(
            self._op.pandas_func()
        )
        return df

    async def extend_realtime(self, symbol: str, new_row: pd.Series) -> pd.Series:
        assert isinstance(new_row.name, datetime)
        last_value = self._last_value.get(symbol)
        value = new_row[self._candle_col]
        if last_value is not None:
            value = self._op.func()(value, last_value)

        new_row[self.column_name()] = value
        self._last_value[symbol] = value
        return new_row


class ATRIndicator:
    def __init__(self, period: int, freq: str):
        self._period = period
        self._last_atr: dict[str, float] = {}
        self._last_close: dict[str, float] = {}
        self._freq = freq

    def _lookback_days(self, start_date: date) -> date:
        n, unit = split_freq(self._freq)
        if unit == "d":
            n *= 390
        elif unit == "h":
            n *= 390 / 6.5

        # 390 minute candles in a day and a security factor of 2
        n_days = math.ceil(2 * self._period * n / 390)
        return lookback_days(start_date, n_days)

    @classmethod
    def type(cls):
        return "atr"

    def column_name(self):
        return f"atr_{self._period}_{self._freq}"

    async def save_to_cache(self):
        await ApplicationRegistry.cache.save(
            f"indicator:{self.column_name()}",
            json.dumps({"last_atr": self._last_atr, "last_close": self._last_close}),
        )

    async def load_from_cache(self):
        indicator_data = await ApplicationRegistry.cache.get(
            f"indicator:{self.column_name()}"
        )
        indicator_data = json.loads(indicator_data)
        self._last_atr = indicator_data["last_atr"]
        self._last_close = indicator_data["last_close"]

    async def extend(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        col_name = self.column_name()
        if df.empty:
            return df

        df_start = df.index[0]
        end_date = df.index[0].date()
        start_date = self._lookback_days(end_date)
        prev_df = await ApplicationRegistry.candles.get(
            symbol, start_date, end_date, self._freq
        )

        if not prev_df.empty:
            df = pd.concat([prev_df, df])

        assert isinstance(df.index, pd.DatetimeIndex)
        tr0 = (df[CandleCol.HIGH] - df[CandleCol.LOW]).abs()
        tr1 = (df[CandleCol.HIGH] - df[CandleCol.CLOSE].shift(1)).abs()
        tr2 = (df[CandleCol.LOW] - df[CandleCol.CLOSE].shift(1)).abs()
        df[col_name] = (
            pd.concat([tr0, tr1, tr2], axis=1)
            .max(axis=1)
            .ewm(alpha=1 / self._period)
            .mean()
        )
        return df.loc[df_start:]

    async def extend_realtime(self, symbol: str, new_row: pd.Series) -> pd.Series:
        assert isinstance(new_row.name, datetime)
        last_close = self._last_close.get(symbol)
        if last_close is None:
            new_row = (await self.extend(symbol, new_row.to_frame().T)).iloc[0]
            self._last_atr[symbol] = new_row[self.column_name()]
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
        self._high_n_days = DailyRollingIndicator(n_days, "max", CandleCol.HIGH)
        self._low_n_days = DailyRollingIndicator(n_days, "min", CandleCol.LOW)
        self._last_date: dict[str, date] = {}

    @classmethod
    def type(cls):
        return "position_in_range"

    def column_name(self):
        return f"position_in_range_{self._n_days}"

    async def save_to_cache(self):
        await ApplicationRegistry.cache.save(
            f"indicator:{self.column_name()}",
            json.dumps(
                {"last_date": {k: v.isoformat() for k, v in self._last_date.items()}}
            ),
        )

    async def load_from_cache(self):
        indicator_data = await ApplicationRegistry.cache.get(
            f"indicator:{self.column_name()}"
        )
        indicator_data = json.loads(indicator_data)
        self._last_date = {
            k: date.fromisoformat(v) for k, v in indicator_data["last_date"].items()
        }

    async def extend(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        original_columns = set(df.columns)
        df = await self._high_n_days.extend(symbol, df)
        df = await self._low_n_days.extend(symbol, df)

        high_col = self._high_n_days.column_name()
        low_col = self._low_n_days.column_name()

        df[self.column_name()] = (df[CandleCol.CLOSE] - df[low_col]) / (
            df[high_col] - df[low_col]
        )

        columns_to_drop = [
            col
            for col in [high_col, low_col]
            if col not in original_columns and col in df.columns
        ]
        if columns_to_drop:
            df = df.drop(columns=columns_to_drop)

        return df

    async def extend_realtime(self, symbol: str, new_row: pd.Series) -> pd.Series:
        original_columns = set(new_row.index)
        new_row = await self._high_n_days.extend_realtime(symbol, new_row)
        new_row = await self._low_n_days.extend_realtime(symbol, new_row)

        high_col = self._high_n_days.column_name()
        low_col = self._low_n_days.column_name()

        high = new_row[high_col]
        low = new_row[low_col]
        close = new_row[CandleCol.CLOSE]
        if pd.isna(high) or pd.isna(low) or high == low:
            new_row[self.column_name()] = pd.NA
        else:
            new_row[self.column_name()] = (close - low) / (high - low)

        columns_to_drop = [
            col for col in [high_col, low_col] if col not in original_columns
        ]
        new_row = new_row.drop(columns_to_drop)

        return new_row


class DailyRollingIndicator:
    """
    Computes a rolling aggregation (min, max, sum) on a specified candle column over the last n_days.
    """

    def __init__(self, n_days: int, operation: str, candle_col: str):
        assert operation in {
            "min",
            "max",
            "sum",
        }, "Operation must be one of min, max, sum"
        self._n_days = n_days
        self._operation = operation
        self._candle_col = candle_col

        self._rolling_values: dict[str, list[float]] = {}
        self._last_date: dict[str, date] = {}

    @classmethod
    def type(cls):
        return "daily_rolling"

    def column_name(self) -> str:
        return f"{self._operation}_{self._candle_col}_{self._n_days}d"

    async def save_to_cache(self):
        await ApplicationRegistry.cache.save(
            f"indicator:{self.column_name()}",
            json.dumps(
                {
                    "rolling_values": self._rolling_values,
                    "last_date": {k: v.isoformat() for k, v in self._last_date.items()},
                }
            ),
        )

    async def load_from_cache(self):
        indicator_data = await ApplicationRegistry.cache.get(
            f"indicator:{self.column_name()}"
        )
        indicator_data = json.loads(indicator_data)
        self._rolling_values = indicator_data["rolling_values"]
        self._last_date = {
            k: date.fromisoformat(v) for k, v in indicator_data["last_date"].items()
        }

    async def _get_data_for_n_days(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        start_date = lookback_days(df.index[0].date(), self._n_days)
        end_date = df.index[-1].date() - timedelta(days=1)
        daily_df = await ApplicationRegistry.candles.get(
            symbol, start_date, end_date, "1d"
        )
        # if daily_df.shape[0] < self._n_days:
        #     logger.warning(
        #         f"Requested {self._n_days} days of data for {symbol} on {start_date}-{end_date}, got {daily_df.shape[0]} days."
        #     )
        return daily_df

    async def extend(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        if self.column_name() in df.columns:
            return df

        daily_df = await self._get_data_for_n_days(symbol, df)
        if daily_df.empty:
            df[self.column_name()] = pd.NA
            return df

        rolling_df = (
            daily_df[[self._candle_col]]
            .rolling(self._n_days, min_periods=1)
            .agg(self._operation)
            .rename(columns={self._candle_col: self.column_name()})
            .set_index(daily_df.index.date)  # type: ignore
        )

        rolling_df.loc[df.index[-1].date(), self.column_name()] = pd.NA
        rolling_df = rolling_df.shift(1)

        df.loc[:, "date"] = df.index.date  # type: ignore
        df = df.join(rolling_df, on="date")

        return df.drop(columns=["date"])

    async def extend_realtime(self, symbol: str, new_row: pd.Series) -> pd.Series:
        assert isinstance(new_row.name, datetime)
        last_date = self._last_date.get(symbol)

        if last_date is None or last_date != new_row.name.date():
            daily_df = await self._get_data_for_n_days(symbol, new_row.to_frame().T)
            self._last_date[symbol] = new_row.name.date()

            self._rolling_values[symbol] = daily_df[self._candle_col].to_list()[
                -self._n_days :
            ]

        values = self._rolling_values.get(symbol, [])
        if not values:
            new_row[self.column_name()] = pd.NA
            return new_row

        if self._operation == "min":
            agg_val = min(values)
        elif self._operation == "max":
            agg_val = max(values)
        elif self._operation == "sum":
            agg_val = sum(values)
        else:
            agg_val = pd.NA

        new_row[self.column_name()] = agg_val
        return new_row


class GapIndicator:
    def __init__(self, candle_col: str):
        self._candle_col = candle_col
        self._prev_day = PrevDayIndicator(CandleCol.CLOSE)

    @classmethod
    def type(cls):
        return "gap"

    def column_name(self) -> str:
        return f"gap_{self._candle_col}"

    async def save_to_cache(self): ...

    async def load_from_cache(self):
        await self._prev_day.load_from_cache()

    async def extend(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        cols_to_drop: list[str] = []
        if self._prev_day.column_name() not in df.columns:
            cols_to_drop.append(self._prev_day.column_name())
            df = await self._prev_day.extend(symbol, df)

        prev_col = self._prev_day.column_name()
        df.loc[:, self.column_name()] = (df[self._candle_col] - df[prev_col]) / df[
            prev_col
        ]

        return df.drop(columns=cols_to_drop)

    async def extend_realtime(self, symbol: str, new_row: pd.Series) -> pd.Series:
        assert isinstance(new_row.name, datetime)
        cols_to_drop: list[str] = []
        if self._prev_day.column_name() not in new_row.index:
            new_row = await self._prev_day.extend_realtime(symbol, new_row)
            cols_to_drop.append(self._prev_day.column_name())

        prev_col = self._prev_day.column_name()
        new_row.at[self.column_name()] = (
            new_row[self._candle_col] - new_row[prev_col]
        ) / new_row[prev_col]

        return new_row.drop(cols_to_drop)


class ATRGapIndicator:
    def __init__(self, period: int, candle_col: str):
        self._period = period
        self._candle_col = candle_col
        self._atr = DailyATRIndicator(period)
        self._gap = GapIndicator(candle_col)
        self._prev_day = PrevDayIndicator(CandleCol.CLOSE)

    @classmethod
    def type(cls):
        return "atr_gap"

    def column_name(self) -> str:
        return f"atr_gap_{self._candle_col}_{self._period}"

    async def save_to_cache(self): ...

    async def load_from_cache(self):
        await self._atr.load_from_cache()
        await self._gap.load_from_cache()
        await self._prev_day.load_from_cache()

    async def extend(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        aux_indicators = [self._atr, self._gap, self._prev_day]
        cols_to_drop: list[str] = []
        for ind in aux_indicators:
            if ind.column_name() in df.columns:
                continue
            df = await ind.extend(symbol, df)
            cols_to_drop.append(ind.column_name())

        df.loc[:, self.column_name()] = (
            df[self._gap.column_name()]
            * df[self._prev_day.column_name()]
            / df[self._atr.column_name()]
        )
        return df.drop(columns=cols_to_drop)

    async def extend_realtime(self, symbol: str, new_row: pd.Series) -> pd.Series:
        assert isinstance(new_row.name, datetime)
        aux_indicators: "list[Indicator]" = [self._atr, self._gap, self._prev_day]
        cols_to_drop: list[str] = []
        for ind in aux_indicators:
            if ind.column_name() not in new_row.index:
                new_row = await ind.extend_realtime(symbol, new_row)
                cols_to_drop.append(ind.column_name())

        if new_row[self._atr.column_name()] > 0:
            new_row.at[self.column_name()] = (
                new_row[self._gap.column_name()]
                * new_row[self._prev_day.column_name()]
                / new_row[self._atr.column_name()]
            )
        else:
            new_row.at[self.column_name()] = pd.NA

        return new_row.drop(cols_to_drop)


class ShiftIndicator:
    def __init__(self, candle_col: str, shift: int):
        self._candle_col = candle_col
        self._shift = shift
        self._last_date: dict[str, date] = {}
        self._last_values: dict[str, list[float]] = {}

    @classmethod
    def type(cls):
        return "shift"

    def column_name(self) -> str:
        return f"{self._candle_col}_shift_{self._shift}"

    async def save_to_cache(self):
        await ApplicationRegistry.cache.save(
            f"indicator:{self.column_name()}",
            json.dumps(
                {
                    "last_date": {k: v.isoformat() for k, v in self._last_date.items()},
                    "last_values": self._last_values,
                }
            ),
        )

    async def load_from_cache(self):
        indicator_data = await ApplicationRegistry.cache.get(
            f"indicator:{self.column_name()}"
        )
        indicator_data = json.loads(indicator_data)
        self._last_date = {
            k: date.fromisoformat(v) for k, v in indicator_data["last_date"].items()
        }
        self._last_values = indicator_data["last_values"]

    async def extend(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            df[self.column_name()] = pd.NA
            return df
        df[self.column_name()] = df[self._candle_col].groupby(df.index.date).shift(self._shift)  # type: ignore
        return df

    async def extend_realtime(self, symbol: str, new_row: pd.Series) -> pd.Series:
        assert isinstance(new_row.name, datetime)
        last_date = self._last_date.get(symbol)

        if last_date is None or last_date != new_row.name.date():
            self._last_date[symbol] = new_row.name.date()
            self._last_values[symbol] = []

        values = self._last_values.setdefault(symbol, [])
        values.append(new_row[self._candle_col])
        if len(values) < self._shift + 1:
            new_row[self.column_name()] = pd.NA
            return new_row

        new_row[self.column_name()] = values.pop(0)
        return new_row
