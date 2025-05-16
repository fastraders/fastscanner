from datetime import date, datetime, time, timedelta

import pandas as pd

from ..ports import CandleCol
from ..registry import ApplicationRegistry
from ..utils import lookback_days


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

    def lookback_days(self) -> int:
        return 0

    async def extend(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        start_date = lookback_days(df.index[0].date(), 1)
        end_date = df.index[-1].date() - timedelta(days=1)

        daily = await ApplicationRegistry.candles.get(
            symbol, start_date, end_date, "1d"
        )
        daily = daily.set_index(daily.index.date)  # type: ignore
        daily.loc[df.index[-1].date(), self._candle_col] = pd.NA
        daily = daily.shift(1).rename(columns={self._candle_col: self.column_name()})

        df["date"] = df.index.date  # type: ignore
        df = df.join(daily[[self.column_name()]], on="date")
        return df.drop(columns=["date"])

    async def extend_realtime(self, symbol: str, new_row: pd.Series) -> pd.Series:
        assert isinstance(new_row.name, datetime)
        new_date = new_row.name.date()
        if (last_date := self._last_date.get(symbol)) is None or last_date != new_date:
            yday = lookback_days(new_date, 1)
            close = await ApplicationRegistry.candles.get(symbol, yday, yday, "1d")
            if not close.empty:
                self._prev_day[symbol] = close[self._candle_col].values[0]
            self._last_date[symbol] = new_date

        new_row[self.column_name()] = self._prev_day.get(symbol, pd.NA)
        return new_row


class DailyGapIndicator:
    def __init__(self):
        self._daily_open: dict[str, float] = {}
        self._daily_close: dict[str, float] = {}
        self._last_date: dict[str, date] = {}

    @classmethod
    def type(cls):
        return "daily_gap"

    def lookback_days(self) -> int:
        return 0

    def column_name(self):
        return "daily_gap"

    async def extend(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        start_date = lookback_days(df.index[0].date(), 1)
        end_date = df.index[-1].date() - timedelta(days=1)

        df["date"] = df.index.date  # type: ignore

        daily_close = await ApplicationRegistry.candles.get(
            symbol, start_date, end_date, "1d"
        )
        daily_close = daily_close.set_index(daily_close.index.date)[[CandleCol.CLOSE]]  # type: ignore
        daily_open = (
            df.loc[df.index.time >= time(9, 30)].groupby("date")[CandleCol.OPEN].first()  # type: ignore
        )
        daily_close.loc[df.index[-1].date(), CandleCol.CLOSE] = pd.NA
        daily_close = daily_close.shift(1)
        daily_close = daily_close.join(daily_open)
        daily_close[self.column_name()] = (
            daily_close[CandleCol.OPEN] - daily_close[CandleCol.CLOSE]
        ) / daily_close[CandleCol.CLOSE]

        df = df.join(daily_close[self.column_name()], on="date")
        df.loc[df.index.time < time(9, 30), self.column_name()] = pd.NA  # type: ignore

        return df.drop(columns=["date"])

    async def extend_realtime(self, symbol: str, new_row: pd.Series) -> pd.Series:
        assert isinstance(new_row.name, datetime)
        new_date = new_row.name.date()
        if (last_date := self._last_date.get(symbol)) is None or last_date != new_date:
            yday = lookback_days(new_date, 1)
            close = await ApplicationRegistry.candles.get(symbol, yday, yday, "1d")
            if not close.empty:
                self._daily_close[symbol] = close[CandleCol.CLOSE].values[0]
            self._daily_open.pop(symbol, None)
            self._last_date[symbol] = new_date

        day_open = self._daily_open.get(symbol)
        if day_open is None and new_row.name.time() >= time(9, 30):
            day_open = new_row[CandleCol.OPEN]
            self._daily_open[symbol] = day_open

        day_close = self._daily_close.get(symbol)
        if day_open is not None and day_close is not None:
            new_row[self.column_name()] = (day_open - day_close) / day_close
        else:
            new_row[self.column_name()] = pd.NA

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

    def lookback_days(self) -> int:
        return 0

    async def extend(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        start_date = lookback_days(df.index[0].date(), self._period + 1)
        end_date = df.index[-1].date() - timedelta(days=1)

        daily = await ApplicationRegistry.candles.get(
            symbol, start_date, end_date, "1d"
        )
        daily = daily.set_index(daily.index.date)  # type: ignore
        daily.loc[
            df.index[-1].date(), [CandleCol.HIGH, CandleCol.LOW, CandleCol.CLOSE]
        ] = pd.NA
        daily = daily.shift(1)

        tr0 = (daily[CandleCol.HIGH] - daily[CandleCol.LOW]).abs()
        tr1 = (daily[CandleCol.HIGH] - daily[CandleCol.CLOSE].shift(1)).abs()
        tr2 = (daily[CandleCol.LOW] - daily[CandleCol.CLOSE].shift(1)).abs()

        atr = (
            pd.concat([tr0, tr1, tr2], axis=1)
            .max(axis=1)
            .ewm(alpha=1 / self._period)
            .mean()
            .round(3)
        )
        df["date"] = df.index.date  # type: ignore
        df = df.join(atr.rename(self.column_name()), on="date")
        return df.drop(columns=["date"])

    async def extend_realtime(self, symbol: str, new_row: pd.Series) -> pd.Series:
        assert isinstance(new_row.name, datetime)
        new_date = new_row.name.date()
        if (last_date := self._last_date.get(symbol)) is None or last_date != new_date:
            new_row = (await self.extend(symbol, new_row.to_frame().T)).iloc[0]
            self._daily_atr[symbol] = new_row[self.column_name()]
            self._last_date[symbol] = new_date

        new_row[self.column_name()] = self._daily_atr[symbol]
        return new_row


class DailyATRGapIndicator:
    def __init__(self, period: int):
        self._period = period
        self._last_date: dict[str, date] = {}
        self._daily_gap: dict[str, float] = {}

    @classmethod
    def type(cls):
        return "daily_atr_gap"

    def column_name(self):
        return f"daily_atr_gap_{self._period}"

    def lookback_days(self) -> int:
        return 0

    async def extend(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        # Gets the ratio (day_open - prev_day_close) / atr
        atr_indicator = DailyATRIndicator(self._period)
        daily_gap = DailyGapIndicator()
        prev_day = PrevDayIndicator(CandleCol.CLOSE)
        aux_indicators = [atr_indicator, daily_gap, prev_day]
        cols_to_drop: list[str] = []
        for ind in aux_indicators:
            if ind.column_name() in df.columns:
                continue
            df = await ind.extend(symbol, df)
            cols_to_drop.append(ind.column_name())

        df[self.column_name()] = (
            df[daily_gap.column_name()]
            * df[prev_day.column_name()]
            / df[atr_indicator.column_name()]
        )
        return df.drop(columns=cols_to_drop)

    async def extend_realtime(self, symbol: str, new_row: pd.Series) -> pd.Series:
        assert isinstance(new_row.name, datetime)
        new_date = new_row.name.date()
        if (last_date := self._last_date.get(symbol)) is None or last_date != new_date:
            self._daily_gap.pop(symbol, None)
            self._last_date[symbol] = new_date

        if new_row.name.time() >= time(9, 30) and symbol not in self._daily_gap:
            new_row = (await self.extend(symbol, new_row.to_frame().T)).iloc[0]
            self._daily_gap[symbol] = new_row[self.column_name()]
            self._last_date[symbol] = new_date

        new_row[self.column_name()] = self._daily_gap.get(symbol, pd.NA)
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

    def lookback_days(self) -> int:
        return 0

    async def extend(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        assert isinstance(df.index, pd.DatetimeIndex)
        start = lookback_days(df.index[0].date(), self._period + 1)
        end = df.index[-1].date() - timedelta(days=1)
        daily_df = await ApplicationRegistry.candles.get(symbol, start, end, "1d")
        if daily_df.empty:
            df[self.column_name()] = pd.NA
            return df

        adv = (
            (
                (daily_df[CandleCol.HIGH] - daily_df[CandleCol.LOW])
                / daily_df[CandleCol.LOW]
            )
            .rolling(self._period, min_periods=1)
            .mean()
            .set_axis(daily_df.index.date)  # type: ignore
            .rename(self.column_name())
        )
        adv[df.index[-1].date()] = pd.NA
        adv = adv.shift(1)
        df["date"] = df.index.date  # type: ignore
        df = df.join(adv, on="date")
        return df.drop(columns=["date"])

    async def extend_realtime(self, symbol: str, new_row: pd.Series) -> pd.Series:
        assert isinstance(new_row.name, datetime)
        last_date = self._last_date.get(symbol)
        if last_date is None or last_date != new_row.name.date():
            new_row = (await self.extend(symbol, new_row.to_frame().T)).iloc[0]
            self._last_date[symbol] = new_row.name.date()  # type: ignore
            self._last_value[symbol] = new_row[self.column_name()]

        new_row[self.column_name()] = self._last_value[symbol]
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

    def lookback_days(self) -> int:
        return 0

    async def extend(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        assert isinstance(df.index, pd.DatetimeIndex)
        start = lookback_days(df.index[0].date(), self._period + 1)
        end = df.index[-1].date() - timedelta(days=1)
        df_daily = await ApplicationRegistry.candles.get(symbol, start, end, "1d")
        daily_vol = df_daily[CandleCol.VOLUME]
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
        df["date"] = df.index.date  # type: ignore
        df = df.join(daily_vol, on="date")
        return df.drop(columns=["date"])

    async def extend_realtime(self, symbol: str, new_row: pd.Series) -> pd.Series:
        assert isinstance(new_row.name, datetime)
        last_date = self._last_date.get(symbol)
        if last_date is None or last_date != new_row.name.date():
            new_row = (await self.extend(symbol, new_row.to_frame().T)).iloc[0]
            self._last_date[symbol] = new_row.name.date()  # type: ignore
            self._last_value[symbol] = new_row[self.column_name()]
        new_row[self.column_name()] = self._last_value[symbol]
        return new_row
