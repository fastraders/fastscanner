import asyncio
import json
import logging
import os
import shutil
import zoneinfo
from calendar import monthrange
from datetime import date, datetime, time, timedelta
from typing import Protocol

import pandas as pd

from fastscanner.pkg import config
from fastscanner.pkg.clock import LOCAL_TIMEZONE_STR, ClockRegistry, split_freq
from fastscanner.services.indicators.ports import CandleCol, CandleStore

logger = logging.getLogger(__name__)


class CandleStoreWithSplits(CandleStore, Protocol):
    async def splits(self, start: date, end: date) -> dict[str, date]: ...


class PartitionedCSVCandlesProvider:
    CACHE_DIR = os.path.join(config.DATA_BASE_DIR, "data", "candles")
    tz: str = LOCAL_TIMEZONE_STR

    def __init__(self, store: CandleStoreWithSplits):
        self._store = store

    async def get(
        self,
        symbol: str,
        start: date,
        end: date,
        freq: str,
        _today: date | None = None,
        _log_cache_miss: bool = True,
    ) -> pd.DataFrame:
        today = _today or ClockRegistry.clock.today()
        max_date = today - timedelta(days=1)
        if end > max_date:
            raise ValueError(
                f"End date {end} cannot be in the future. Max date is {max_date}."
            )

        _, unit = split_freq(freq)
        keys = self._partition_keys_in_range(start, end, unit)

        dfs: list[pd.DataFrame] = []
        for key in keys:
            df = await self._cache(
                symbol, key, unit, freq, _today=today, _log_cache_miss=_log_cache_miss
            )
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

    _cache_freqs = ["5s", "1min", "1d"]

    async def cache_all_freqs(self, symbol: str, year: int) -> None:
        today = ClockRegistry.clock.today()
        yday = today - timedelta(days=1)
        start = date(year, 1, 1)
        end = min(date(year, 12, 31), yday)
        second_range = self._covering_range(start, end, "s")
        minute_range = self._covering_range(start, end, "min")
        hourly_range = self._covering_range(start, end, "h")
        daily_range = self._covering_range(start, end, "d")
        if self._is_all_freqs_cached(symbol, year, today):
            # logger.info(
            #     f"Cache for {symbol} ({year}) already exists. Skipping cache creation."
            # )
            return

        second_start = second_range[0]
        second_end = min(second_range[1], yday)
        minute_start = min(minute_range[0], hourly_range[0])
        minute_end = min(max(minute_range[1], hourly_range[1]), yday)
        day_start = daily_range[0]
        day_end = min(daily_range[1], yday)

        seconds_df = await self._store.get(symbol, second_start, second_end, "1s")
        minute_df = await self._store.get(symbol, minute_start, minute_end, "1min")
        daily_df = await self._store.get(symbol, day_start, day_end, "1d")

        for freq in self._cache_freqs:
            if freq == "1s":
                df = seconds_df
            elif freq == "1min":
                df = minute_df
            elif freq == "1d":
                df = daily_df
            elif freq.endswith("s"):
                df = seconds_df.resample(freq).agg(CandleCol.RESAMPLE_MAP).dropna()  # type: ignore
            elif freq.endswith("min") or freq.endswith("h"):
                df = minute_df.resample(freq).agg(CandleCol.RESAMPLE_MAP).dropna()  # type: ignore
            elif freq.endswith("d"):
                df = daily_df.resample(freq).agg(CandleCol.RESAMPLE_MAP).dropna()  # type: ignore
            else:
                raise ValueError(f"Unsupported frequency: {freq}")

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
                self._mark_expiration(symbol, key, unit, today)

    async def collect_expired_data(self, symbol: str) -> None:
        today = ClockRegistry.clock.today()
        yday = today - timedelta(days=1)
        self._load_expirations(symbol)
        expirations = self._expirations.get(symbol, {})
        grouped_by_unit: dict[str, str] = {}
        # if unit not in expiration.json and we should retreive partition_key of yesterday _partition_key(yesterday,unit)
        unit_to_freqs: dict[str, list[str]] = {}
        for freq in self._cache_freqs:
            _, unit = split_freq(freq)
            unit_to_freqs.setdefault(unit, []).append(freq)
        for exp_key, exp_date in expirations.items():
            if exp_date > yday:
                continue
            partition_key, unit = exp_key.rsplit("_", 1)
            grouped_by_unit[unit] = partition_key

        for unit in unit_to_freqs.keys():
            if unit not in grouped_by_unit:
                partition_key = self._partition_key(yday, unit)
                grouped_by_unit[unit] = partition_key

        for unit, partition_key in grouped_by_unit.items():
            freqs = unit_to_freqs[unit]
            for freq in freqs:
                start_date, _ = self._range_from_key(partition_key, unit)
                await self.get(
                    symbol, start_date, yday, freq, today, _log_cache_miss=False
                )

    async def collect_splits(self) -> None:
        today = ClockRegistry.clock.today()
        last_checked_splits_path = os.path.join(
            self.CACHE_DIR, "last_checked_splits.txt"
        )

        if os.path.exists(last_checked_splits_path):
            with open(last_checked_splits_path, "r") as f:
                last_checked_str = f.read().strip()
                splits_last_checked = date.fromisoformat(last_checked_str)
        else:
            splits_last_checked = today - timedelta(days=1)

        if splits_last_checked >= today:
            return

        splits = await self._store.splits(
            splits_last_checked + timedelta(days=1), today
        )

        for symbol, _ in splits.items():
            logger.info(
                f"Found splits for {symbol} since {splits_last_checked}. Expiring cache."
            )
            path = os.path.join(self.CACHE_DIR, symbol)
            shutil.rmtree(path, ignore_errors=True)

        symbol_to_year: dict[str, int] = {}
        for symbol in splits:
            keys = self._partition_keys_in_range(date(2010, 1, 1), today, "d")
            for key in keys:
                partition_path = self._partition_path(symbol, key, "1d")
                if os.path.exists(partition_path):
                    df = await self._cache(
                        symbol, key, "d", "1d", _log_cache_miss=False
                    )
                    if not df.empty:
                        break
            else:
                continue
            symbol_to_year[symbol] = int(df.index.year.min())  # type: ignore

        splits = {key: value for key, value in splits.items() if key in symbol_to_year}
        tasks = [
            asyncio.create_task(self.cache_all_freqs(symbol, year))
            for symbol in splits
            for year in range(symbol_to_year[symbol], today.year + 1)
        ]

        await asyncio.gather(*tasks)
        self.mark_splits_checked(today)

    def mark_splits_checked(self, date_: date):
        with open(os.path.join(self.CACHE_DIR, "last_checked_splits.txt"), "w") as f:
            f.write(date_.isoformat())

    def _is_all_freqs_cached(self, symbol: str, year: int, today: date) -> bool:
        key = f"{year}"
        return os.path.exists(
            self._partition_path(symbol, key, "1d")
        ) and not self._is_expired(symbol, key, "d", today)

    async def _cache(
        self,
        symbol: str,
        key: str,
        unit: str,
        freq: str,
        _today: date | None = None,
        _log_cache_miss: bool = True,
    ) -> pd.DataFrame:
        today = _today or ClockRegistry.clock.today()
        partition_path = self._partition_path(symbol, key, freq)
        if not self._is_expired(symbol, key, unit, today):
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
                if _log_cache_miss:
                    logger.info(
                        f"Cache miss for {symbol} ({unit}) with key {key}. Fetching from store."
                    )
            except Exception as e:
                logger.exception(e)
                logger.error(
                    f"Failed to load cached data for {symbol} ({unit}): {e}. Resetting cache."
                )

        start, end = self._range_from_key(key, unit)
        yday = today - timedelta(days=1)
        end = min(end, yday)
        df = (await self._store.get(symbol, start, end, freq)).dropna()
        self._save_cache(symbol, key, freq, df)
        self._mark_expiration(symbol, key, unit, today)
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
            df[CandleCol.DATETIME] = df[CandleCol.DATETIME].dt.strftime("%Y-%m-%d")  # type: ignore

        df.to_csv(partition_path, index=False)

    def _partition_path(self, symbol: str, key: str, freq: str) -> str:
        return os.path.join(self.CACHE_DIR, symbol, freq, f"{key}.csv")

    def _partition_key(self, dt: date, unit: str) -> str:
        return self._partition_keys(pd.DatetimeIndex([dt]), unit).iat[0]

    def _partition_keys(self, index: pd.DatetimeIndex, unit: str) -> "pd.Series[str]":
        if unit.lower() in ("s", "min", "t"):
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

    def _covering_range(self, start: date, end: date, unit: str) -> tuple[date, date]:
        start_key = self._partition_key(start, unit)
        end_key = self._partition_key(end, unit)
        return (
            self._range_from_key(start_key, unit)[0],
            self._range_from_key(end_key, unit)[1],
        )

    def _range_from_key(self, key: str, unit: str) -> tuple[date, date]:
        if unit.lower() in ("s", "min", "t"):
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

    def _is_expired(self, symbol: str, key: str, unit: str, today: date) -> bool:
        self._load_expirations(symbol)

        expiration_key = self._expiration_key(key, unit)
        expirations = self._expirations.get(symbol, {})
        if expiration_key not in expirations:
            return False

        return expirations[expiration_key] <= today

    def _mark_expiration(self, symbol: str, key: str, unit: str, today: date) -> None:
        self._load_expirations(symbol)

        _, end = self._range_from_key(key, unit)
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
