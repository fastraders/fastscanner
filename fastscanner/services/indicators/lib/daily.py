from datetime import date, datetime, time, timedelta
from typing import TYPE_CHECKING, Any

import pandas as pd

from ...registry import ApplicationRegistry
from ..ports import CandleCol as C
from ..utils import lookback_days

if TYPE_CHECKING:
    from . import Indicator


class PrevDayIndicator:
    def __init__(self, candle_col: str):
        self._candle_col = candle_col
        self._prev_day: dict[str, float] = {}
        self._last_date: dict[str, date] = {}

    @classmethod
    def type(cls):
        return "prev_day"

    def column_name(self):
        return f"prev_day_{self._candle_col}"

    async def extend(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        start_date = lookback_days(df.index[0].date(), 1)
        end_date = df.index[-1].date() - timedelta(days=1)

        daily = await ApplicationRegistry.candles.get(
            symbol, start_date, end_date, "1d"
        )
        daily = daily.set_index(daily.index.date)  # type: ignore
        daily.loc[df.index[-1].date(), self._candle_col] = pd.NA
        daily = daily.shift(1).rename(columns={self._candle_col: self.column_name()})

        df.loc[:, "date"] = df.index.date  # type: ignore
        df = df.join(daily[[self.column_name()]], on="date")
        return df.drop(columns=["date"])

    async def extend_realtime(
        self, symbol: str, new_row: dict[str, Any]
    ) -> dict[str, Any]:
        timestamp = new_row["datetime"]
        assert isinstance(timestamp, datetime)
        new_date = timestamp.date()
        if (last_date := self._last_date.get(symbol)) is None or last_date != new_date:
            yday = lookback_days(new_date, 1)
            close = await ApplicationRegistry.candles.get(symbol, yday, yday, "1d")
            if not close.empty:
                self._prev_day[symbol] = close[self._candle_col].values[0]
            self._last_date[symbol] = new_date

        new_row[self.column_name()] = self._prev_day.get(symbol, None)
        return new_row


class DailyGapIndicator:
    def __init__(self):
        self._daily_open: dict[str, float] = {}
        self._daily_close: dict[str, float] = {}
        self._last_date: dict[str, date] = {}

    @classmethod
    def type(cls):
        return "daily_gap"

    def column_name(self):
        return "daily_gap"

    async def extend(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        start_date = lookback_days(df.index[0].date(), 1)
        end_date = df.index[-1].date()

        df.loc[:, "date"] = df.index.date  # type: ignore

        daily_df = await ApplicationRegistry.candles.get(
            symbol, start_date, end_date, "1d"
        )
        daily_df = daily_df.set_index(daily_df.index.date)[[C.CLOSE, C.OPEN]]  # type: ignore
        daily_df[C.CLOSE] = daily_df[C.CLOSE].shift(1)
        daily_df[self.column_name()] = (
            daily_df[C.OPEN] - daily_df[C.CLOSE]
        ) / daily_df[C.CLOSE]

        df = df.join(daily_df[self.column_name()], on="date")
        # We use this to account for cases where the freq is daily.
        if not df.index.empty and df.index[0].time() > time(0, 0):
            df.loc[df.index.time < time(9, 30), self.column_name()] = pd.NA  # type: ignore

        return df.drop(columns=["date"])

    async def extend_realtime(
        self, symbol: str, new_row: dict[str, Any]
    ) -> dict[str, Any]:
        timestamp = new_row["datetime"]
        assert isinstance(timestamp, datetime)
        new_date = timestamp.date()
        if (last_date := self._last_date.get(symbol)) is None or last_date != new_date:
            yday = lookback_days(new_date, 1)
            close = await ApplicationRegistry.candles.get(symbol, yday, yday, "1d")
            if not close.empty:
                self._daily_close[symbol] = close[C.CLOSE].values[0]
            self._daily_open.pop(symbol, None)
            self._last_date[symbol] = new_date

        day_open = self._daily_open.get(symbol)
        if day_open is None and timestamp.time() >= time(9, 30):
            day_open = new_row[C.OPEN]
            self._daily_open[symbol] = day_open

        day_close = self._daily_close.get(symbol)
        if day_open is not None and day_close is not None:
            new_row[self.column_name()] = (day_open - day_close) / day_close
        else:
            new_row[self.column_name()] = None

        return new_row


class DailyATRIndicator:
    def __init__(self, period: int):
        self._period = period
        self._daily_atr: dict[str, float] = {}
        self._last_date: dict[str, date] = {}

    @classmethod
    def type(cls):
        return "daily_atr"

    def column_name(self):
        return f"daily_atr_{self._period}"

    async def extend(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        start_date = lookback_days(df.index[0].date(), self._period + 1)
        end_date = df.index[-1].date() - timedelta(days=1)

        daily = await ApplicationRegistry.candles.get(
            symbol, start_date, end_date, "1d"
        )
        daily = daily.set_index(daily.index.date)  # type: ignore
        daily.loc[df.index[-1].date(), [C.HIGH, C.LOW, C.CLOSE]] = pd.NA
        daily = daily.shift(1)

        tr0 = (daily[C.HIGH] - daily[C.LOW]).abs()
        tr1 = (daily[C.HIGH] - daily[C.CLOSE].shift(1)).abs()
        tr2 = (daily[C.LOW] - daily[C.CLOSE].shift(1)).abs()

        atr = (
            pd.concat([tr0, tr1, tr2], axis=1)
            .max(axis=1)
            .ewm(alpha=1 / self._period)
            .mean()
        )
        df.loc[:, "date"] = df.index.date  # type: ignore
        df = df.join(atr.rename(self.column_name()), on="date")
        return df.drop(columns=["date"])

    async def extend_realtime(
        self, symbol: str, new_row: dict[str, Any]
    ) -> dict[str, Any]:
        timestamp = new_row["datetime"]
        assert isinstance(timestamp, datetime)
        new_date = timestamp.date()

        if (last_date := self._last_date.get(symbol)) is None or last_date != new_date:
            start_date = lookback_days(new_date, self._period + 1)
            end_date = new_date - timedelta(days=1)
            daily = await ApplicationRegistry.candles.get(
                symbol, start_date, end_date, "1d"
            )
            daily = daily.set_index(daily.index.date)  # type: ignore
            daily.loc[new_date, [C.HIGH, C.LOW, C.CLOSE]] = pd.NA
            daily = daily.shift(1)

            tr0 = (daily[C.HIGH] - daily[C.LOW]).abs()
            tr1 = (daily[C.HIGH] - daily[C.CLOSE].shift(1)).abs()
            tr2 = (daily[C.LOW] - daily[C.CLOSE].shift(1)).abs()

            atr = (
                pd.concat([tr0, tr1, tr2], axis=1)
                .max(axis=1)
                .ewm(alpha=1 / self._period)
                .mean()
            )
            atr_val = atr.get(new_date, pd.NA)

            self._daily_atr[symbol] = atr_val
            self._last_date[symbol] = new_date

        val = self._daily_atr.get(symbol, pd.NA)
        new_row[self.column_name()] = val
        return new_row


class DailyATRGapIndicator:
    def __init__(self, period: int):
        self._period = period
        self._last_date: dict[str, date] = {}
        self._daily_gap: dict[str, float] = {}
        self._atr = DailyATRIndicator(self._period)
        self._gap = DailyGapIndicator()
        self._prev_day = PrevDayIndicator(C.CLOSE)
        self._aux_indicators = [self._atr, self._gap, self._prev_day]

    @classmethod
    def type(cls):
        return "daily_atr_gap"

    def column_name(self):
        return f"daily_atr_gap_{self._period}"

    async def extend(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        # Gets the ratio (day_open - prev_day_close) / atr
        atr_indicator = DailyATRIndicator(self._period)
        daily_gap = DailyGapIndicator()
        prev_day = PrevDayIndicator(C.CLOSE)
        aux_indicators = [atr_indicator, daily_gap, prev_day]
        cols_to_drop: list[str] = []
        for ind in aux_indicators:
            if ind.column_name() in df.columns:
                continue
            df = await ind.extend(symbol, df)
            cols_to_drop.append(ind.column_name())

        df.loc[:, self.column_name()] = (
            df[daily_gap.column_name()]
            * df[prev_day.column_name()]
            / df[atr_indicator.column_name()]
        )
        return df.drop(columns=cols_to_drop)

    async def extend_realtime(
        self, symbol: str, new_row: dict[str, Any]
    ) -> dict[str, Any]:
        timestamp = new_row["datetime"]
        assert isinstance(timestamp, datetime)
        new_date = timestamp.date()
        if (last_date := self._last_date.get(symbol)) is None or last_date != new_date:
            self._daily_gap.pop(symbol, None)
            self._last_date[symbol] = new_date

        if timestamp.time() >= time(9, 30) and symbol not in self._daily_gap:
            cols_to_drop = []
            for ind in self._aux_indicators:
                if ind.column_name() in new_row:
                    continue
                new_row = await ind.extend_realtime(symbol, new_row)
                cols_to_drop.append(ind.column_name())

            new_row[self.column_name()] = (
                new_row[self._gap.column_name()]
                * new_row[self._prev_day.column_name()]
                / new_row[self._atr.column_name()]
            )
            new_row = {
                key: value for key, value in new_row.items() if key not in cols_to_drop
            }
            self._daily_gap[symbol] = new_row[self.column_name()]
            self._last_date[symbol] = new_date

        new_row[self.column_name()] = self._daily_gap.get(symbol, None)
        return new_row


class ADRIndicator:
    """
    Average Daily Range (ADR) indicator.
    """

    def __init__(self, period: int):
        self._period = period
        self._last_date: dict[str, date] = {}
        self._last_value: dict[str, float] = {}

    @classmethod
    def type(cls):
        return "adr"

    def column_name(self):
        return f"adr_{self._period}"

    async def extend(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        assert isinstance(df.index, pd.DatetimeIndex)
        start = lookback_days(df.index[0].date(), self._period + 1)
        end = df.index[-1].date() - timedelta(days=1)
        daily_df = await ApplicationRegistry.candles.get(symbol, start, end, "1d")
        if daily_df.empty:
            df[self.column_name()] = pd.NA
            return df

        adr = (
            ((daily_df[C.HIGH] - daily_df[C.LOW]) / daily_df[C.CLOSE])
            .rolling(self._period, min_periods=1)
            .mean()
            .set_axis(daily_df.index.date)  # type: ignore
            .rename(self.column_name())
        )
        adr[df.index[-1].date()] = pd.NA
        adr = adr.shift(1)
        df.loc[:, "date"] = df.index.date  # type: ignore
        df = df.join(adr, on="date")
        return df.drop(columns=["date"])

    async def extend_realtime(
        self, symbol: str, new_row: dict[str, Any]
    ) -> dict[str, Any]:
        timestamp = new_row["datetime"]
        assert isinstance(timestamp, datetime)
        new_date = timestamp.date()

        if (last_date := self._last_date.get(symbol)) is None or last_date != new_date:
            start = lookback_days(new_date, self._period + 1)
            end = new_date - timedelta(days=1)
            daily_df = await ApplicationRegistry.candles.get(symbol, start, end, "1d")
            if daily_df.empty:
                adr_val = float("nan")
            else:
                adr = (
                    ((daily_df[C.HIGH] - daily_df[C.LOW]) / daily_df[C.CLOSE])
                    .rolling(self._period, min_periods=1)
                    .mean()
                    .set_axis(daily_df.index.date)  # type: ignore
                    .rename(self.column_name())
                )
                adr[new_date] = pd.NA
                adr = adr.shift(1)
                adr_val = adr.get(new_date, pd.NA)
            self._last_value[symbol] = adr_val
            self._last_date[symbol] = new_date

        val = self._last_value.get(symbol, pd.NA)
        new_row[self.column_name()] = val
        return new_row


class ADVIndicator:
    def __init__(self, period: int):
        self._period = period
        self._last_date: dict[str, date] = {}
        self._last_value: dict[str, float] = {}

    @classmethod
    def type(cls):
        return "adv"

    def column_name(self):
        return f"adv_{self._period}"

    async def extend(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        assert isinstance(df.index, pd.DatetimeIndex)
        start = lookback_days(df.index[0].date(), self._period + 1)
        end = df.index[-1].date() - timedelta(days=1)
        df_daily = await ApplicationRegistry.candles.get(symbol, start, end, "1d")
        daily_vol = df_daily[C.VOLUME]
        if daily_vol.empty:
            df[self.column_name()] = pd.NA
            return df

        daily_vol = (
            daily_vol.set_axis(daily_vol.index.date)  # type: ignore
            .rolling(self._period, min_periods=1)
            .mean()
            .rename(self.column_name())
        )
        daily_vol[df.index[-1].date()] = pd.NA
        daily_vol = daily_vol.shift(1)
        df.loc[:, "date"] = df.index.date  # type: ignore
        df = df.join(daily_vol, on="date")
        return df.drop(columns=["date"])

    async def extend_realtime(
        self, symbol: str, new_row: dict[str, Any]
    ) -> dict[str, Any]:
        timestamp = new_row["datetime"]
        assert isinstance(timestamp, datetime)
        new_date = timestamp.date()

        if (last_date := self._last_date.get(symbol)) is None or last_date != new_date:
            start = lookback_days(new_date, self._period + 1)
            end = new_date - timedelta(days=1)
            df_daily = await ApplicationRegistry.candles.get(symbol, start, end, "1d")
            daily_vol = df_daily[C.VOLUME]
            if daily_vol.empty:
                adv_val = float("nan")
            else:
                daily_vol = (
                    daily_vol.set_axis(daily_vol.index.date)  # type: ignore
                    .rolling(self._period, min_periods=1)
                    .mean()
                    .rename(self.column_name())
                )
                daily_vol[new_date] = pd.NA
                daily_vol = daily_vol.shift(1)
                adv_val = daily_vol.get(new_date, pd.NA)
            self._last_value[symbol] = adv_val
            self._last_date[symbol] = new_date

        val = self._last_value.get(symbol, pd.NA)

        new_row[self.column_name()] = val
        return new_row
