from calendar import monthrange
import os
import json
import logging
from datetime import datetime, timedelta, date, time
import pytz
import httpx
import pandas as pd
import numpy as np
import zoneinfo

from fastscanner.pkg.localize import LOCAL_TIMEZONE_STR
from . import config
from .polygon import CandleCol, PolygonBarsProvider, split_freq

logger = logging.getLogger(__name__)

class PartitionedMemmapBarsProvider(PolygonBarsProvider):
    CACHE_DIR = os.path.join("data", "candles")
    tz: str = LOCAL_TIMEZONE_STR

    def get(self, symbol: str, start: date, end: date, freq: str) -> pd.DataFrame:
        _, unit = split_freq(freq)
        keys = self._partition_keys_in_range(start, end, unit)

        dfs: list[pd.DataFrame] = []
        for key in keys:
            df = self._cache(symbol, key, unit)
            if df.empty:
                continue
            dfs.append(df)

        if len(dfs) == 0:
            logger.warning(
                f"No data fetched for {symbol} in the entire date range {start} to {end}."
            )
            return pd.DataFrame(
                columns=list(CandleCol.RESAMPLE_MAP.keys()),
                index=pd.DatetimeIndex([], name=CandleCol.DATETIME),
            ).tz_localize(self.tz)

        df = pd.concat(dfs)
        start_dt = pytz.timezone(self.tz).localize(datetime.combine(start, time(0, 0)))
        end_dt = pytz.timezone(self.tz).localize(datetime.combine(end, time(23, 59, 59)))
        df = df.loc[start_dt:end_dt]

        if freq in ("1min", "1h", "1d"):
            return df

        return df.resample(freq).aggregate(CandleCol.RESAMPLE_MAP).dropna()  # type: ignore

    def _cache(self, symbol: str, key: str, unit: str) -> pd.DataFrame:
        data_path = self._partition_path(symbol, key, unit, ext="dat")
        meta_path = self._partition_path(symbol, key, unit, ext="meta.json")

        if os.path.exists(data_path) and os.path.exists(meta_path) and not self._is_expired(symbol, key, unit):
            try:
                with open(meta_path) as f:
                    meta = json.load(f)

                shape = tuple(meta["shape"])
                dtype = meta["dtype"]
                columns = meta["columns"]

                mm = np.memmap(data_path, dtype=dtype, mode="r", shape=shape)
                df = pd.DataFrame(mm, columns=columns)
                df[CandleCol.DATETIME] = pd.to_datetime(df[CandleCol.DATETIME], utc=True)
                return df.set_index(CandleCol.DATETIME).tz_convert(self.tz)

            except Exception as e:
                logger.error(f"Failed to load memmap cache for {symbol} ({unit}): {e}. Resetting cache.")

        start, end = self._range_from_key(key, unit)
        with httpx.Client() as client:
            df = self._fetch(client, symbol, start, end, f"1{unit}").dropna()
            self._save_cache(symbol, unit, key, df)
            self._mark_expiration(symbol, key, unit)
            return df

    def _save_cache(self, symbol: str, unit: str, key: str, df: pd.DataFrame):
        data_path = self._partition_path(symbol, key, unit, ext="dat")
        meta_path = self._partition_path(symbol, key, unit, ext="meta.json")

        os.makedirs(os.path.dirname(data_path), exist_ok=True)

        df = df.tz_convert("utc").tz_convert(None)
        arr = df.reset_index().to_numpy()
        dtype = arr.dtype.name
        shape = arr.shape

        mm = np.memmap(data_path, dtype=arr.dtype, mode="w+", shape=shape)
        mm[:] = arr[:]
        mm.flush()

        meta = {
            "columns": df.reset_index().columns.tolist(),
            "dtype": dtype,
            "shape": shape
        }

        with open(meta_path, "w") as f:
            json.dump(meta, f)

    def _partition(self, df: pd.DataFrame, unit: str) -> list[pd.DataFrame]:
        keys = self._partition_keys(df.index, unit)  # type: ignore
        df = df.join(keys)
        return [group for _, group in df.groupby("partition_key")]

    def _partition_key(self, dt: datetime, unit: str) -> str:
        return self._partition_keys(pd.DatetimeIndex([dt]), unit)[0]

    def _partition_path(self, symbol: str, key: str, unit: str, ext: str = "dat") -> str:
        return os.path.join(self.CACHE_DIR, symbol, f"{unit}_{key}.{ext}")

    def _partition_keys(self, index: pd.DatetimeIndex, unit: str) -> "pd.Series[str]":
        if unit.lower() in ("min", "t"):
            dt = pd.to_timedelta(index.dayofweek, unit="d")
            return pd.Series((index - dt).strftime("%Y-%m-%d"), index=index, name="partition_key")
        if unit.lower() in ("h", "d"):
            return pd.Series(index.strftime("%Y-%m"), index=index, name="partition_key")
        raise ValueError(f"Invalid unit: {unit}")

    def _partition_keys_in_range(self, start: date, end: date, unit: str) -> list[str]:
        keys = self._partition_keys(pd.date_range(start, end, freq="1d"), unit)
        return keys.drop_duplicates().tolist()

    def _range_from_key(self, key: str, unit: str) -> tuple[date, date]:
        if unit.lower() in ("min", "t"):
            return date.fromisoformat(key), date.fromisoformat(key) + timedelta(days=6)
        if unit.lower() in ("h", "d"):
            year, month = key.split("-")
            year, month = int(year), int(month)
            _, days = monthrange(year, month)
            return date(year, month, 1), date(year, month, days)
        raise ValueError(f"Invalid unit: {unit}")

    _expirations: dict[str, dict[str, date]]

    def _is_expired(self, symbol: str, key: str, unit: str) -> bool:
        self._load_expirations(symbol)

        expiration_key = self._expiration_key(key, unit)
        expirations = self._expirations.get(symbol, {})
        if expiration_key not in expirations:
            return False

        today = datetime.now(zoneinfo.ZoneInfo(self.tz)).date()
        return expirations[expiration_key] <= today

    def _mark_expiration(self, symbol: str, key: str, unit: str) -> None:
        self._load_expirations(symbol)

        _, end = self._range_from_key(key, unit)
        today = datetime.now(zoneinfo.ZoneInfo(self.tz)).date()
        expiration_key = self._expiration_key(key, unit)
        expirations = self._expirations.get(symbol, {})
        if today > end and expiration_key not in expirations:
            return

        if today > end:
            expirations.pop(expiration_key, None)
        else:
            expirations[expiration_key] = today + timedelta(days=1)

        with open(os.path.join(self.CACHE_DIR, symbol, "expirations.json"), "w") as f:
            json.dump({key: value.isoformat() for key, value in expirations.items()}, f)

    def _expiration_key(self, key: str, unit: str) -> str:
        return f"{key}_{unit}"

    def _load_expirations(self, symbol):
        if not hasattr(self, "_expirations"):
            self._expirations = {}

        if symbol in self._expirations:
            return

        try:
            with open(os.path.join(self.CACHE_DIR, symbol, "expirations.json")) as f:
                self._expirations[symbol] = {
                    key: date.fromisoformat(value)
                    for key, value in json.load(f).items()
                }
        except FileNotFoundError:
            self._expirations[symbol] = {}
