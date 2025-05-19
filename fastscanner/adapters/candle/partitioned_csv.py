import asyncio
import json
import logging
import os
import zoneinfo
from calendar import monthrange
from datetime import date, datetime, time, timedelta

import pandas as pd

from fastscanner.pkg import config
from fastscanner.pkg.datetime import LOCAL_TIMEZONE_STR, split_freq
from fastscanner.services.indicators.ports import CandleCol, CandleStore

logger = logging.getLogger(__name__)


class PartitionedCSVCandlesProvider:
    CACHE_DIR = os.path.join(config.DATA_BASE_DIR, "data", "candles")
    tz: str = LOCAL_TIMEZONE_STR

    def __init__(self, store: CandleStore):
        self._store = store

    async def get(self, symbol: str, start: date, end: date, freq: str) -> pd.DataFrame:
        _, unit = split_freq(freq)
        keys = self._partition_keys_in_range(start, end, unit)

        dfs: list[pd.DataFrame] = []
        for key in keys:
            df = await self._cache(symbol, key, unit, freq)
            if df.empty:
                continue
            dfs.append(df)

        if len(dfs) == 0:
            return pd.DataFrame(
                columns=list(CandleCol.RESAMPLE_MAP.keys()),
                index=pd.DatetimeIndex([], name=CandleCol.DATETIME),
            ).tz_localize(self.tz)

        df = pd.concat(dfs)
        start_dt = pd.Timestamp(datetime.combine(start, time(0, 0)), tz=self.tz)
        end_dt = pd.Timestamp(datetime.combine(end, time(23, 59, 59)), tz=self.tz)

        return df.loc[start_dt:end_dt]

    _cache_freqs = ["1min", "2min", "3min", "5min", "10min", "15min", "1h", "1d"]

    async def cache_all_freqs(
        self, symbol: str, start: date, end: date
    ) -> pd.DataFrame:
        minute_df = await self._store.get(symbol, start, end, "1min")
        daily_df = await self._store.get(symbol, start, end, "1d")
        for freq in self._cache_freqs:
            if freq == "1min":
                df = minute_df
            elif freq == "1d":
                df = daily_df
            else:
                df = minute_df.resample(freq).agg(CandleCol.RESAMPLE_MAP).dropna()  # type: ignore

            _, unit = split_freq(freq)
            partition_keys = self._partition_keys_in_range(start, end, unit)
            for key in partition_keys:
                start, end = self._range_from_key(key, unit)
                start_dt = pd.Timestamp(datetime.combine(start, time(0, 0)), tz=self.tz)
                end_dt = pd.Timestamp(
                    datetime.combine(end, time(23, 59, 59)), tz=self.tz
                )
                sub_df = df.loc[start_dt:end_dt]
                self._save_cache(symbol, key, freq, sub_df)
                self._mark_expiration(symbol, key, unit)

        return df.loc[start_dt:end_dt].dropna()

    async def _cache(self, symbol: str, key: str, unit: str, freq: str) -> pd.DataFrame:
        partition_path = self._partition_path(symbol, key, freq)
        if os.path.exists(partition_path) and not self._is_expired(symbol, key, unit):
            try:
                df = pd.read_csv(partition_path)
                if unit.lower() != "d":
                    df[CandleCol.DATETIME] = pd.to_datetime(
                        df[CandleCol.DATETIME], utc=True
                    )
                    return df.set_index(CandleCol.DATETIME).tz_convert(self.tz)
                df[CandleCol.DATETIME] = pd.to_datetime(df[CandleCol.DATETIME])
                return df.set_index(CandleCol.DATETIME).tz_localize(self.tz)
            except Exception as e:
                logger.exception(e)
                logger.error(
                    f"Failed to load cached data for {symbol} ({unit}): {e}. Resetting cache."
                )

        start, end = self._range_from_key(key, unit)
        df = (await self._store.get(symbol, start, end, freq)).dropna()
        self._save_cache(symbol, key, freq, df)
        self._mark_expiration(symbol, key, unit)
        return df

    def _save_cache(self, symbol: str, key: str, freq: str, df: pd.DataFrame):
        partition_path = self._partition_path(symbol, key, freq)
        partition_dir = os.path.dirname(partition_path)
        os.makedirs(partition_dir, exist_ok=True)

        unit = split_freq(freq)[1]
        if unit.lower() != "d":
            df = df.tz_convert("utc").tz_convert(None).reset_index()
        else:
            df = df.reset_index()
            df[CandleCol.DATETIME] = df[CandleCol.DATETIME].dt.strftime("%Y-%m-%d")

        df.to_csv(partition_path, index=False)

    def _partition_path(self, symbol: str, key: str, freq: str) -> str:
        return os.path.join(self.CACHE_DIR, symbol, freq, f"{key}.csv")

    def _partition_key(self, dt: datetime, unit: str) -> str:
        return self._partition_keys(pd.DatetimeIndex([dt]), unit)[0]

    def _partition_keys(self, index: pd.DatetimeIndex, unit: str) -> "pd.Series[str]":
        if unit.lower() in ("min", "t"):
            dt = pd.to_timedelta(index.dayofweek, unit="d")
            return pd.Series(
                (index - dt).strftime("%Y-%m-%d"), index=index, name="partition_key"
            )
        if unit.lower() in ("h",):
            return pd.Series(index.strftime("%Y-%m"), index=index, name="partition_key")
        if unit.lower() in ("d",):
            return pd.Series(index.strftime("%Y"), index=index, name="partition_key")
        raise ValueError(f"Invalid unit: {unit}")

    def _partition_keys_in_range(self, start: date, end: date, unit: str) -> list[str]:
        keys = self._partition_keys(pd.date_range(start, end, freq="1d"), unit)
        return keys.drop_duplicates().tolist()

    def _range_from_key(self, key: str, unit: str) -> tuple[date, date]:
        if unit.lower() in ("min", "t"):
            return date.fromisoformat(key), date.fromisoformat(key) + timedelta(days=6)
        if unit.lower() in ("h",):
            year, month = key.split("-")
            year, month = int(year), int(month)
            _, days = monthrange(year, month)
            return date(year, month, 1), date(year, month, days)
        if unit.lower() in ("d",):
            return date(int(key), 1, 1), date(int(key), 12, 31)
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
