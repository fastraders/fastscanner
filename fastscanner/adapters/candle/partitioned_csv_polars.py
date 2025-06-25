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
import polars as pl

from fastscanner.pkg import config
from fastscanner.pkg.clock import LOCAL_TIMEZONE_STR, ClockRegistry, split_freq
from fastscanner.services.indicators.ports import CandleCol, CandleStore

logger = logging.getLogger(__name__)


class CandleStoreWithSplits(CandleStore, Protocol):
    async def splits(self, start: date, end: date) -> dict[str, date]: ...


class PartitionedCSVCandlesProviderPolars:
    CACHE_DIR = os.path.join(config.DATA_BASE_DIR, "data", "candles_polars")
    tz: str = LOCAL_TIMEZONE_STR

    def __init__(self, store: CandleStoreWithSplits):
        self._store = store

    async def get(self, symbol: str, start: date, end: date, freq: str) -> pl.DataFrame:
        max_date = ClockRegistry.clock.today() - timedelta(days=1)
        if end > max_date:
            raise ValueError(
                f"End date {end} cannot be in the future. Max date is {max_date}."
            )

        _, unit = split_freq(freq)
        keys = self._partition_keys_in_range(start, end, unit)

        dfs: list[pl.DataFrame] = []
        for key in keys:
            df = await self._cache(symbol, key, unit, freq)
            if df.is_empty():
                continue
            dfs.append(df)

        if len(dfs) == 0:
            # Return empty polars DataFrame with proper schema
            return pl.DataFrame(
                {
                    CandleCol.DATETIME: [],
                    CandleCol.OPEN: [],
                    CandleCol.HIGH: [],
                    CandleCol.LOW: [],
                    CandleCol.CLOSE: [],
                    CandleCol.VOLUME: [],
                },
                schema={
                    CandleCol.DATETIME: pl.Datetime(time_zone=self.tz, time_unit="ms"),
                    CandleCol.OPEN: pl.Float64,
                    CandleCol.HIGH: pl.Float64,
                    CandleCol.LOW: pl.Float64,
                    CandleCol.CLOSE: pl.Float64,
                    CandleCol.VOLUME: pl.Float64,
                },
            )

        df = pl.concat(dfs)

        # Sort the data to ensure proper ordering

        # Filter by date range
        start_dt = datetime.combine(start, time(0, 0))
        end_dt = datetime.combine(end, time(23, 59, 59))

        # Create timezone-aware timestamps for filtering
        start_ts = pl.datetime(
            start_dt.year,
            start_dt.month,
            start_dt.day,
            start_dt.hour,
            start_dt.minute,
            start_dt.second,
            time_zone=self.tz,
        )
        end_ts = pl.datetime(
            end_dt.year,
            end_dt.month,
            end_dt.day,
            end_dt.hour,
            end_dt.minute,
            end_dt.second,
            time_zone=self.tz,
        )

        df = df.filter(
            (pl.col(CandleCol.DATETIME) >= start_ts)
            & (pl.col(CandleCol.DATETIME) <= end_ts)
        )

        return df

    def _pandas_to_polars(self, df: pd.DataFrame) -> pl.DataFrame:
        """Convert pandas DataFrame to polars"""
        if df.empty:
            return pl.DataFrame(
                {
                    CandleCol.DATETIME: [],
                    CandleCol.OPEN: [],
                    CandleCol.HIGH: [],
                    CandleCol.LOW: [],
                    CandleCol.CLOSE: [],
                    CandleCol.VOLUME: [],
                },
                schema={
                    CandleCol.DATETIME: pl.Datetime(time_zone=self.tz, time_unit="ms"),
                    CandleCol.OPEN: pl.Float64,
                    CandleCol.HIGH: pl.Float64,
                    CandleCol.LOW: pl.Float64,
                    CandleCol.CLOSE: pl.Float64,
                    CandleCol.VOLUME: pl.Float64,
                },
            )

        # Reset index to make datetime a column
        df_reset = df.reset_index()
        polars_df = pl.from_pandas(df_reset)
        return polars_df

    _cache_freqs = ["1min", "2min", "3min", "5min", "10min", "15min", "1h", "1d"]

    async def cache_all_freqs(self, symbol: str, year: int) -> None:
        yday = datetime.now(zoneinfo.ZoneInfo(self.tz)).date() - timedelta(days=1)
        start = date(year, 1, 1)
        end = min(date(year, 12, 31), yday)
        minute_range = self._covering_range(start, end, "min")
        hourly_range = self._covering_range(start, end, "h")
        daily_range = self._covering_range(start, end, "d")
        if self._is_all_freqs_cached(symbol, year):
            return

        minute_start = min(minute_range[0], hourly_range[0])
        minute_end = min(max(minute_range[1], hourly_range[1]), yday)
        day_start = daily_range[0]
        day_end = min(daily_range[1], yday)

        # Get data from store (returns pandas) and convert to polars
        minute_df_pandas = await self._store.get(
            symbol, minute_start, minute_end, "1min"
        )
        daily_df_pandas = await self._store.get(symbol, day_start, day_end, "1d")

        minute_df = self._pandas_to_polars(minute_df_pandas)
        daily_df = self._pandas_to_polars(daily_df_pandas)

        for freq in self._cache_freqs:
            if freq == "1min":
                df = minute_df
            elif freq == "1d":
                df = daily_df
            elif freq.endswith("min") or freq.endswith("h"):
                df = self._resample_polars(minute_df, freq)
            elif freq.endswith("d"):
                df = self._resample_polars(daily_df, freq)
            else:
                raise ValueError(f"Unsupported frequency: {freq}")

            _, unit = split_freq(freq)
            partition_keys = self._partition_keys_in_range(start, end, unit)
            for key in partition_keys:
                curr_start, curr_end = self._range_from_key(key, unit)
                start_dt = datetime.combine(curr_start, time(0, 0))
                end_dt = datetime.combine(curr_end, time(23, 59, 59))

                # Filter using polars
                start_ts = pl.datetime(
                    start_dt.year,
                    start_dt.month,
                    start_dt.day,
                    start_dt.hour,
                    start_dt.minute,
                    start_dt.second,
                    time_zone=self.tz,
                )
                end_ts = pl.datetime(
                    end_dt.year,
                    end_dt.month,
                    end_dt.day,
                    end_dt.hour,
                    end_dt.minute,
                    end_dt.second,
                    time_zone=self.tz,
                )

                sub_df = df.filter(
                    (pl.col(CandleCol.DATETIME) >= start_ts)
                    & (pl.col(CandleCol.DATETIME) <= end_ts)
                )

                self._save_cache(symbol, key, freq, sub_df)
                self._mark_expiration(symbol, key, unit)

    def _resample_polars(self, df: pl.DataFrame, freq: str) -> pl.DataFrame:
        """Resample polars DataFrame to specified frequency"""
        if df.is_empty():
            return df

        # Convert frequency to polars format
        freq_map = {
            "2min": "2m",
            "3min": "3m",
            "5min": "5m",
            "10min": "10m",
            "15min": "15m",
            "1h": "1h",
            "1d": "1d",
        }
        polars_freq = freq_map.get(freq, freq)

        # Group by time period and aggregate
        resampled = (
            df.group_by_dynamic(CandleCol.DATETIME, every=polars_freq, closed="left")
            .agg(
                [
                    pl.col(CandleCol.OPEN).first(),
                    pl.col(CandleCol.HIGH).max(),
                    pl.col(CandleCol.LOW).min(),
                    pl.col(CandleCol.CLOSE).last(),
                    pl.col(CandleCol.VOLUME).sum(),
                ]
            )
            .drop_nulls()
        )

        return resampled

    def _is_all_freqs_cached(self, symbol: str, year: int) -> bool:
        key = f"{year}"
        return os.path.exists(
            self._partition_path(symbol, key, "1d")
        ) and not self._is_expired(symbol, key, "d")

    async def _cache(self, symbol: str, key: str, unit: str, freq: str) -> pl.DataFrame:
        partition_path = self._partition_path(symbol, key, freq)
        if not self._is_expired(symbol, key, unit):
            try:
                df = pl.read_csv(partition_path)
                if unit.lower() != "d":
                    df = df.with_columns(
                        [
                            pl.col(CandleCol.DATETIME)
                            .str.to_datetime(
                                format="%Y-%m-%d %H:%M:%S",
                                time_zone="UTC",
                                time_unit="ms",
                            )
                            .dt.convert_time_zone(self.tz)
                        ]
                    )
                else:
                    df = df.with_columns(
                        [
                            pl.col(CandleCol.DATETIME)
                            .str.to_datetime(format="%Y-%m-%d", time_unit="ms")
                            .dt.replace_time_zone(self.tz)
                        ]
                    )
                return df
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
        yday = ClockRegistry.clock.today() - timedelta(days=1)
        end = min(end, yday)

        # Get from store (pandas) and convert to polars
        pandas_df = (await self._store.get(symbol, start, end, freq)).dropna()
        df = self._pandas_to_polars(pandas_df)

        self._save_cache(symbol, key, freq, df)
        self._mark_expiration(symbol, key, unit)
        return df

    def _save_cache(self, symbol: str, key: str, freq: str, df: pl.DataFrame):
        partition_path = self._partition_path(symbol, key, freq)
        partition_dir = os.path.dirname(partition_path)
        os.makedirs(partition_dir, exist_ok=True)

        unit = split_freq(freq)[1]
        if unit.lower() != "d":
            # Convert to UTC and remove timezone info for storage
            df_to_save = df.with_columns(
                [
                    pl.col(CandleCol.DATETIME)
                    .dt.convert_time_zone("UTC")
                    .dt.replace_time_zone(None)
                    .dt.strftime("%Y-%m-%d %H:%M:%S")
                ]
            )
        else:
            # Format date as string for daily data
            df_to_save = df.with_columns(
                [pl.col(CandleCol.DATETIME).dt.strftime("%Y-%m-%d")]
            )

        df_to_save.write_csv(partition_path)

    def _partition_path(self, symbol: str, key: str, freq: str) -> str:
        return os.path.join(self.CACHE_DIR, symbol, freq, f"{key}.csv")

    def _partition_key(self, dt: date, unit: str) -> str:
        date_series = pl.Series([dt])
        return self._partition_keys(date_series, unit)[0]

    def _partition_keys(self, dates: pl.Series, unit: str) -> list[str]:
        if unit.lower() in ("min", "t"):
            return dates.dt.truncate("1w").dt.strftime("%Y-%m-%d").to_list()
        if unit.lower() in ("h",):
            return dates.dt.strftime("%Y-%m").to_list()
        if unit.lower() in ("d",):
            return dates.dt.strftime("%Y").to_list()
        raise ValueError(f"Invalid unit: {unit}")

    def _partition_keys_in_range(self, start: date, end: date, unit: str) -> list[str]:
        date_range = pl.date_range(start, end, "1d", eager=True)
        keys = self._partition_keys(date_range, unit)
        return sorted(list(set(keys)))

    def _covering_range(self, start: date, end: date, unit: str) -> tuple[date, date]:
        start_key = self._partition_key(start, unit)
        end_key = self._partition_key(end, unit)
        return (
            self._range_from_key(start_key, unit)[0],
            self._range_from_key(end_key, unit)[1],
        )

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

        today = ClockRegistry.clock.today()
        return expirations[expiration_key] <= today

    def _mark_expiration(self, symbol: str, key: str, unit: str) -> None:
        self._load_expirations(symbol)

        _, end = self._range_from_key(key, unit)
        today = ClockRegistry.clock.today()
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

    async def collect_expired_data(self, symbol: str) -> None:
        yesterday = ClockRegistry.clock.now().date() - timedelta(days=1)
        self._load_expirations(symbol)
        expirations = self._expirations.get(symbol, {})
        grouped_by_unit: dict[str, str] = {}
        unit_to_freqs: dict[str, list[str]] = {}
        for freq in self._cache_freqs:
            _, unit = split_freq(freq)
            unit_to_freqs.setdefault(unit, []).append(freq)
        for exp_key, exp_date in expirations.items():
            if exp_date > yesterday:
                continue
            partition_key, unit = exp_key.rsplit("_", 1)
            grouped_by_unit[unit] = partition_key

        for unit in unit_to_freqs.keys():
            if unit not in grouped_by_unit:
                partition_key = self._partition_key(yesterday, unit)
                grouped_by_unit[unit] = partition_key

        for unit, partition_key in grouped_by_unit.items():
            freqs = unit_to_freqs[unit]
            for freq in freqs:
                start_date, _ = self._range_from_key(partition_key, unit)
                await self.get(symbol, start_date, yesterday, freq)

    async def collect_splits(self) -> None:
        last_checked_splits_path = os.path.join(
            self.CACHE_DIR, "last_checked_splits.txt"
        )

        with open(last_checked_splits_path, "r") as f:
            last_checked_str = f.read().strip()
            splits_last_checked = date.fromisoformat(last_checked_str)

        if splits_last_checked >= ClockRegistry.clock.today():
            return

        today = ClockRegistry.clock.today()
        splits = await self._store.splits(
            splits_last_checked + timedelta(days=1), today
        )

        for symbol, _ in splits.items():
            logger.info(
                f"Found splits for {symbol} since {splits_last_checked}. Expiring cache."
            )
            path = os.path.join(self.CACHE_DIR, symbol)
            shutil.rmtree(path, ignore_errors=True)

        tasks = [
            asyncio.create_task(self.cache_all_freqs(symbol, year))
            for symbol in splits
            for year in range(2010, today.year + 1)
        ]

        await asyncio.gather(*tasks)
        self.mark_splits_checked()

    def mark_splits_checked(self):
        today = ClockRegistry.clock.today()
        with open(os.path.join(self.CACHE_DIR, "last_checked_splits.txt"), "w") as f:
            f.write(today.isoformat())
