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
        if not df.index.is_monotonic_increasing:
            logger.warning(
                f"Data for {symbol} ({freq}) is not sorted between {start} and {end}"
            )
            df = df.sort_index()

        start_dt = pd.Timestamp(datetime.combine(start, time(0, 0)), tz=self.tz)
        end_dt = pd.Timestamp(datetime.combine(end, time(23, 59, 59)), tz=self.tz)

        return df.loc[start_dt:end_dt]

    _cache_freqs = ["1min", "2min", "3min", "5min", "10min", "15min", "1h", "1d"]

    async def cache_all_freqs(
        self, symbol: str, start: date, end: date, force: bool = False
    ) -> bool:
        pending_freqs: list[str] = []
        for freq in self._cache_freqs:
            if self._is_cached(symbol, start, end, freq) and not force:
                continue
            pending_freqs.append(freq)

        if not pending_freqs:
            return False

        minute_df = await self._store.get(symbol, start, end, "1min")
        daily_df = await self._store.get(symbol, start, end, "1d")
        for freq in pending_freqs:
            if freq == "1min":
                df = minute_df
            elif freq == "1d":
                df = daily_df
            else:
                df = minute_df.resample(freq).agg(CandleCol.RESAMPLE_MAP).dropna()  # type: ignore

            _, unit = split_freq(freq)
            partition_keys = self._partition_keys_in_range(start, end, unit)
            for key in partition_keys:
                curr_start, curr_end = self._range_from_key(key, unit)
                start_dt = pd.Timestamp(
                    datetime.combine(curr_start, time(0, 0)), tz=self.tz
                )
                end_dt = pd.Timestamp(
                    datetime.combine(curr_end, time(23, 59, 59)), tz=self.tz
                )
                sub_df = df.loc[start_dt:end_dt]
                self._save_cache(symbol, key, freq, sub_df)
                self._mark_expiration(symbol, key, unit)

        return minute_df.empty

    def _is_cached(self, symbol: str, start: date, end: date, freq: str) -> bool:
        _, unit = split_freq(freq)
        keys = self._partition_keys_in_range(start, end, unit)
        for key in keys:
            partition_path = self._partition_path(symbol, key, freq)
            if not os.path.exists(partition_path) or self._is_expired(
                symbol, key, unit
            ):
                return False
        return True

    async def cache_all_freqs_empty(self, symbol: str, start: date, end: date):
        empty_df = pd.DataFrame(
            columns=list(CandleCol.RESAMPLE_MAP.keys()),
            index=pd.DatetimeIndex([], name=CandleCol.DATETIME),
        ).tz_localize(self.tz)
        for freq in self._cache_freqs:
            _, unit = split_freq(freq)
            partition_keys = self._partition_keys_in_range(start, end, unit)
            for key in partition_keys:
                self._save_cache(symbol, key, freq, empty_df)
                self._mark_expiration(symbol, key, unit)

    async def _cache(self, symbol: str, key: str, unit: str, freq: str) -> pd.DataFrame:
        partition_path = self._partition_path(symbol, key, freq)
        if not self._is_expired(symbol, key, unit):
            try:
                df = pd.read_csv(partition_path)
                if unit.lower() != "d":
                    df[CandleCol.DATETIME] = pd.to_datetime(
                        df[CandleCol.DATETIME],
                        utc=True,
                        format="%Y-%m-%d %H:%M:%S",
                    )
                    return df.set_index(CandleCol.DATETIME).tz_convert(self.tz)
                df[CandleCol.DATETIME] = pd.to_datetime(
                    df[CandleCol.DATETIME], format="%Y-%m-%d"
                )
                return df.set_index(CandleCol.DATETIME).tz_localize(self.tz)
            except FileNotFoundError:
                logger.info(
                    f"Cache miss for {symbol} ({unit}) with key {key}. Fetching from store."
                )
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
        expirations = self._expirations.setdefault(symbol, {})
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

    async def collect_expired_data(self, symbol: str, today: date) -> bool:
        self._load_expirations(symbol)
        expirations = self._expirations.get(symbol, {})
        grouped_by_unit: dict[str, list[str]] = {}

        for exp_key, exp_date in expirations.items():
            if exp_date >= today:
                continue
            try:
                partition_key, unit = exp_key.rsplit("_", 1)
            except ValueError:
                continue
            grouped_by_unit.setdefault(unit, []).append(partition_key)

        any_data_collected = False

        for unit, partition_keys in grouped_by_unit.items():
            updated_keys: set[str] = set()

            for freq in [f for f in self._cache_freqs if split_freq(f)[1] == unit]:
                for partition_key in partition_keys:
                    try:
                        start_date, _ = self._range_from_key(partition_key, unit)
                        if start_date >= today:
                            continue

                        df = await self._store.get(symbol, start_date, today, freq)
                        if df.empty:
                            continue

                        self._save_cache(symbol, partition_key, freq, df)
                        updated_keys.add(f"{partition_key}_{unit}")
                    except Exception as e:
                        logger.error(
                            f"Error collecting {symbol} {freq} {partition_key}: {e}"
                        )

            if updated_keys:
                for key in updated_keys:
                    expirations[key] = today
                expiration_path = os.path.join(
                    self.CACHE_DIR, symbol, "expirations.json"
                )
                with open(expiration_path, "w") as f:
                    json.dump({k: v.isoformat() for k, v in expirations.items()}, f)
                any_data_collected = True

        return any_data_collected
