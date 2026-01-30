import asyncio
import json
import logging
import os
from calendar import monthrange
from datetime import date, datetime, time, timedelta

import pandas as pd

from fastscanner.pkg import config
from fastscanner.pkg.clock import LOCAL_TIMEZONE_STR, ClockRegistry, split_freq
from fastscanner.services.indicators.ports import CandleCol, CandleStore

from .massive_adjusted import MassiveAdjustedMixin

logger = logging.getLogger(__name__)


class PartitionedCSVCandlesProvider(MassiveAdjustedMixin):
    CACHE_DIR = os.path.join(config.DATA_BASE_DIR, "data", "candles")
    tz: str = LOCAL_TIMEZONE_STR

    def __init__(self, store: CandleStore):
        self._store = store
        self._base_dir = os.path.join(config.DATA_BASE_DIR, "data")

    async def get(
        self,
        symbol: str,
        start: date,
        end: date,
        freq: str,
        adjusted: bool = True,
        _today: date | None = None,
        _log_cache_miss: bool = True,
    ) -> pd.DataFrame:
        today = _today or ClockRegistry.clock.today()
        max_date = today - timedelta(days=1)
        if end > max_date:
            raise ValueError(
                f"End date {end} cannot be in the future. Max date is {max_date}."
            )

        keys = self._partition_keys_in_range(start, end, freq)

        dfs: list[pd.DataFrame] = []
        for key in keys:
            df = await self._cache(
                symbol, key, freq, _today=today, _log_cache_miss=_log_cache_miss
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
        start_dt = pd.Timestamp(datetime.combine(start, time(0, 0)), tz=self.tz)
        end_dt = pd.Timestamp(datetime.combine(end, time(23, 59, 59)), tz=self.tz)
        df = df.loc[start_dt:end_dt]

        if adjusted:
            df = self.adjust(symbol, df, to=ClockRegistry.clock.today())
        return df

    async def collect(self, symbol: str, year: int, freqs: list[str]) -> None:
        today = ClockRegistry.clock.today()
        yday = today - timedelta(days=1)
        start = date(year, 1, 1)
        end = min(date(year, 12, 31), yday)
        minute_range = self._covering_range(start, end, "1min")
        hourly_range = self._covering_range(start, end, "1h")
        daily_range = self._covering_range(start, end, "1d")

        minute_start = min(minute_range[0], hourly_range[0])
        minute_end = min(max(minute_range[1], hourly_range[1]), yday)
        day_start = daily_range[0]
        day_end = min(daily_range[1], yday)

        minute_df = await self._store.get(
            symbol, minute_start, minute_end, "1min", adjusted=False
        )
        daily_df = await self._store.get(
            symbol, day_start, day_end, "1d", adjusted=False
        )

        for freq in freqs:
            if freq == "1min":
                df = minute_df
            elif freq == "1d":
                df = daily_df
            elif freq.endswith("min") or freq.endswith("h"):
                df = minute_df.resample(freq).agg(CandleCol.RESAMPLE_MAP).dropna()  # type: ignore
            elif freq.endswith("d"):
                df = daily_df.resample(freq).agg(CandleCol.RESAMPLE_MAP).dropna()  # type: ignore
            else:
                raise ValueError(f"Unsupported frequency: {freq}")

            partition_keys = self._partition_keys_in_range(start, end, freq)
            for key in partition_keys:
                curr_start, curr_end = self._range_from_key(key, freq)
                start_dt = pd.Timestamp(
                    datetime.combine(curr_start, time(0, 0)), tz=self.tz
                )
                end_dt = pd.Timestamp(
                    datetime.combine(curr_end, time(23, 59, 59)), tz=self.tz
                )
                sub_df = df.loc[start_dt:end_dt]
                self._save_cache(symbol, key, freq, sub_df)
                self._mark_expiration(symbol, key, freq, today)

    async def collect_expired_data(self, symbol: str, freqs: list[str]) -> None:
        today = ClockRegistry.clock.today()
        yday = today - timedelta(days=1)
        self._load_expirations(symbol)
        expirations = self._expirations.get(symbol, {})

        # Delete after some days
        if any(
            exp_key.split("_", 1)[1] in ("d", "min", "h") for exp_key in expirations
        ):
            expirations = {
                "2026_1d": today,
                "2026-01-26_1min": today,
                "2026-01-26_2min": today,
            }
            self._expirations[symbol] = expirations
            with open(
                os.path.join(self.CACHE_DIR, symbol, "expirations.json"), "w"
            ) as f:
                json.dump(
                    {key: value.isoformat() for key, value in expirations.items()}, f
                )

        freq_to_partition_key: dict[str, str] = {}
        for exp_key, exp_date in expirations.items():
            partition_key, freq = exp_key.rsplit("_", 1)

            if exp_date > today:
                continue
            freq_to_partition_key[freq] = partition_key

        for freq, partition_key in freq_to_partition_key.items():
            start_date, _ = self._range_from_key(partition_key, freq)
            await self.get(
                symbol,
                start_date,
                yday,
                freq,
                adjusted=False,
                _today=today,
                _log_cache_miss=False,
            )

    async def _cache(
        self,
        symbol: str,
        key: str,
        freq: str,
        _today: date | None = None,
        _log_cache_miss: bool = True,
    ) -> pd.DataFrame:
        today = _today or ClockRegistry.clock.today()
        partition_path = self._partition_path(symbol, key, freq)
        if not self._is_expired(symbol, key, freq, today):
            try:
                _, unit = split_freq(freq)
                df = await asyncio.to_thread(pd.read_csv, partition_path)
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
            except pd.errors.EmptyDataError:
                return pd.DataFrame(
                    columns=list(CandleCol.RESAMPLE_MAP.keys()),
                    index=pd.DatetimeIndex([], name=CandleCol.DATETIME),
                ).tz_localize(self.tz)
            except FileNotFoundError:
                if _log_cache_miss:
                    logger.info(
                        f"Cache miss for {symbol} ({freq}) with key {key}. Fetching from store."
                    )
            except Exception as e:
                logger.exception(e)
                logger.error(
                    f"Failed to load cached data for {symbol} ({freq}): {e}. Resetting cache."
                )

        start, end = self._range_from_key(key, freq)
        yday = today - timedelta(days=1)
        end = min(end, yday)
        df = (
            (await self._store.get(symbol, start, end, freq, adjusted=False))
            .dropna()
            .sort_index()
        )
        self._save_cache(symbol, key, freq, df)
        self._mark_expiration(symbol, key, freq, today)
        return df

    def _save_cache(self, symbol: str, key: str, freq: str, df: pd.DataFrame):
        partition_path = self._partition_path(symbol, key, freq)
        partition_dir = os.path.dirname(partition_path)
        os.makedirs(partition_dir, exist_ok=True)

        _, unit = split_freq(freq)
        if unit.lower() != "d":
            df = df.tz_convert("utc").tz_convert(None).reset_index()
        else:
            df = df.reset_index()
            df[CandleCol.DATETIME] = df[CandleCol.DATETIME].dt.strftime("%Y-%m-%d")  # type: ignore

        df.to_csv(partition_path, index=False)

    def _partition_path(self, symbol: str, key: str, freq: str) -> str:
        return os.path.join(self.CACHE_DIR, symbol, freq, f"{key}.csv")

    def _partition_key(self, dt: date, freq: str) -> str:
        return self._partition_keys(pd.DatetimeIndex([dt]), freq).iat[0]

    def _partition_keys(self, index: pd.DatetimeIndex, freq: str) -> "pd.Series[str]":
        _, unit = split_freq(freq)
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

    def _partition_keys_in_range(self, start: date, end: date, freq: str) -> list[str]:
        keys = self._partition_keys(pd.date_range(start, end, freq="1d"), freq)
        return keys.drop_duplicates().tolist()

    def _covering_range(self, start: date, end: date, freq: str) -> tuple[date, date]:
        start_key = self._partition_key(start, freq)
        end_key = self._partition_key(end, freq)
        return (
            self._range_from_key(start_key, freq)[0],
            self._range_from_key(end_key, freq)[1],
        )

    def _range_from_key(self, key: str, freq: str) -> tuple[date, date]:
        _, unit = split_freq(freq)
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

    def _is_expired(self, symbol: str, key: str, freq: str, today: date) -> bool:
        self._load_expirations(symbol)

        expiration_key = self._expiration_key(key, freq)
        expirations = self._expirations.get(symbol, {})
        if expiration_key not in expirations:
            return False

        return expirations[expiration_key] <= today

    def _mark_expiration(self, symbol: str, key: str, freq: str, today: date) -> None:
        self._load_expirations(symbol)

        _, end = self._range_from_key(key, freq)
        expiration_key = self._expiration_key(key, freq)
        expirations = self._expirations.setdefault(symbol, {})
        if today > end and expiration_key not in expirations:
            return

        if today > end:
            expirations.pop(expiration_key, None)
            key = self._partition_key(today, freq)
            expiration_key = self._expiration_key(key, freq)
        expirations[expiration_key] = today + timedelta(days=1)

        with open(os.path.join(self.CACHE_DIR, symbol, "expirations.json"), "w") as f:
            json.dump({key: value.isoformat() for key, value in expirations.items()}, f)

    def _expiration_key(self, key: str, freq: str) -> str:
        return f"{key}_{freq}"

    def _load_expirations(self, symbol: str):
        if not hasattr(self, "_expirations"):
            self._expirations = {}

        if symbol in self._expirations:
            return

        try:
            with open(os.path.join(self.CACHE_DIR, symbol, "expirations.json")) as f:
                # self._expirations[symbol] = {
                #     "2026_1d": date(2026, 1, 28),
                #     "2026-01-26_1min": date(2026, 1, 28),
                #     "2026-01-26_2min": date(2026, 1, 28),
                # }

                self._expirations[symbol] = {
                    key: date.fromisoformat(value)
                    for key, value in json.load(f).items()
                }
        except FileNotFoundError:
            self._expirations[symbol] = {}
