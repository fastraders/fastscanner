from datetime import datetime
from typing import TypeVar

import pandas as pd

from fastscanner.pkg.localize import LOCAL_TIMEZONE

T = TypeVar("T", int, float, str, bool, datetime, pd.Timestamp, bytes, complex)


class TimeSeries(pd.Series[T]):
    @property
    def index(self) -> pd.DatetimeIndex:
        return super().index  # type: ignore


class CandlesDataFrame(pd.DataFrame):
    @classmethod
    def from_df(cls, df: pd.DataFrame) -> "CandlesDataFrame":
        col_types = {
            "open": float,
            "high": float,
            "low": float,
            "close": float,
            "volume": int,
        }
        missing_cols = set(col_types.keys()) - set(df.columns)
        if len(missing_cols) > 0:
            raise ValueError(f"Missing columns: {missing_cols}")

        for name, type_ in col_types.items():
            if df[name].dtype != type_:
                df[name] = df[name].astype(type_)

        df.index.rename("datetime", inplace=True)
        index = df.index
        changed = False
        if not isinstance(index, pd.DatetimeIndex):
            index = pd.to_datetime(index)
            changed = True
        if index.tz is None:
            index = index.tz_localize("UTC")
            changed = True
        if index.tz != LOCAL_TIMEZONE:
            index = index.tz_convert(LOCAL_TIMEZONE)
            changed = True

        if changed:
            df.set_index(index, inplace=True)

        return cls(df)

    @property
    def open_(self) -> TimeSeries[float]:
        return self["open"]  # type: ignore

    @property
    def high(self) -> TimeSeries[float]:
        return self["high"]  # type: ignore

    @property
    def low(self) -> TimeSeries[float]:
        return self["low"]  # type: ignore

    @property
    def close(self) -> TimeSeries[float]:
        return self["close"]  # type: ignore

    @property
    def volume(self) -> TimeSeries[int]:
        return self["volume"]  # type: ignore
