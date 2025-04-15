import io
import json
import logging
import os
import re
import zoneinfo
from abc import ABC, abstractmethod
from calendar import monthrange
from datetime import date, datetime, time, timedelta
from urllib.parse import urljoin

import httpx
import pandas as pd
import pytz

from fastscanner.pkg.http import MaxRetryError, retry_request

from . import config

logger = logging.getLogger(__name__)


class BarCol:
    DATETIME = "datetime"
    OPEN = "open"
    HIGH = "high"
    LOW = "low"
    CLOSE = "close"
    VOLUME = "volume"

    RESAMPLE_MAP = {
        OPEN: "first",
        HIGH: "max",
        LOW: "min",
        CLOSE: "last",
        VOLUME: "sum",
    }


class BarsProvider(ABC):
    INDEX = BarCol.DATETIME
    COLUMNS = [BarCol.OPEN, BarCol.HIGH, BarCol.LOW, BarCol.CLOSE, BarCol.VOLUME]

    def __init__(self, tz: str = "US/Eastern") -> None:
        self.tz = tz

    @abstractmethod
    def _get(self, symbol: str, start: date, end: date, freq: str) -> pd.DataFrame:
        pass

    def _check(self, df: pd.DataFrame) -> None:
        if set(df.columns) != set(self.COLUMNS):
            raise ValueError(f"Columns mismatch: {df.columns} != {self.COLUMNS}")

    def get(self, symbol: str, start: date, end: date, freq: str) -> pd.DataFrame:
        if type(start) != type(end):
            raise ValueError("start and end must be of the same type")
        df = self._get(symbol, start, end, freq)
        self._check(df)
        return df


class BarsCache:
    CACHE_DIR: str
    tz: str

    def _get_from_cache(
        self, symbol: str, start: date, end: date, freq: str
    ) -> pd.DataFrame:
        _, unit = _split_freq(freq)
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
                columns=list(BarCol.RESAMPLE_MAP.keys()),
                index=pd.DatetimeIndex([], name=BarCol.DATETIME),
            ).tz_localize(self.tz)

        df = pd.concat(dfs)
        start_dt = pytz.timezone(self.tz).localize(datetime.combine(start, time(4, 0)))
        end_dt = pytz.timezone(self.tz).localize(datetime.combine(end, time(20, 0)))

        return df.loc[(df.index >= start_dt) & (df.index <= end_dt)].resample(freq).aggregate(BarCol.RESAMPLE_MAP).dropna()  # type: ignore

    def _cache(self, symbol: str, key: str, unit: str) -> pd.DataFrame:
        partition_path = self._partition_path(symbol, key, unit)
        if os.path.exists(partition_path) and not self._is_expired(symbol, key, unit):
            try:
                df = pd.read_csv(partition_path)
                df[BarCol.DATETIME] = pd.to_datetime(df[BarCol.DATETIME], utc=True)
                return df.set_index(BarCol.DATETIME).tz_convert(self.tz)
            except Exception as e:
                logger.error(
                    f"Failed to load cached data for {symbol} ({unit}): {e}. Resetting cache."
                )

        # Fetches if not exists in cache
        start, end = self._range_from_key(key, unit)
        with httpx.Client() as client:
            df = self._fetch(client, symbol, start, end, f"1{unit}").dropna()
            self._save_cache(symbol, unit, key, df)
            self._mark_expiration(symbol, key, unit)
            return df

    def _save_cache(self, symbol: str, unit: str, key: str, df: pd.DataFrame):
        partition_path = self._partition_path(symbol, key, unit)
        partition_dir = os.path.dirname(partition_path)
        os.makedirs(partition_dir, exist_ok=True)

        df.tz_convert("utc").tz_convert(None).reset_index().to_csv(
            partition_path, index=False
        )

    def _partition(self, df: pd.DataFrame, unit: str) -> list[pd.DataFrame]:
        keys = self._partition_keys(df.index, unit)  # type: ignore
        df = df.join(keys)
        return [group for _, group in df.groupby("partition_key")]

    def _partition_key(self, dt: datetime, unit: str) -> str:
        return self._partition_keys(pd.DatetimeIndex([dt]), unit)[0]

    def _partition_path(self, symbol: str, key: str, unit: str) -> str:
        return os.path.join(self.CACHE_DIR, symbol, f"{unit}_{key}.csv")

    def _partition_keys(self, index: pd.DatetimeIndex, unit: str) -> "pd.Series[str]":
        if unit.lower() in ("min", "t"):
            dt = pd.to_timedelta(index.dayofweek, unit="d")
            return pd.Series(
                (index - dt).strftime("%Y-%m-%d"), index=index, name="partition_key"
            )
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

    @abstractmethod
    def _fetch(
        self,
        client: httpx.Client,
        symbol: str,
        start: date,
        end: date,
        freq: str,
    ) -> pd.DataFrame:
        pass

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


class PolygonBarsProvider(BarsProvider):
    def __init__(self, tz: str = "US/Eastern"):
        super().__init__(tz)
        self._base_url = config.POLYGON_BASE_URL
        self._api_key = config.POLYGON_API_KEY

    def _fetch(
        self,
        client: httpx.Client,
        symbol: str,
        start: date,
        end: date,
        freq: str,
    ) -> pd.DataFrame:
        mult, unit = _split_freq(freq)
        unit_mappers = {
            "min": "minute",
            "h": "hour",
            "t": "minute",
            "d": "day",
        }
        max_days_per_unit = {
            "min": 60,
            "t": 60,
            "h": 2000,
            "d": 50000,
        }
        max_days = max_days_per_unit[unit]
        curr_start = start
        curr_end = min(end, start + timedelta(days=max_days))
        dfs: list[pd.DataFrame] = []
        while curr_start <= end:
            url = urljoin(
                self._base_url,
                f"v2/aggs/ticker/{symbol}/range/{mult}/{unit_mappers[unit]}/{curr_start.isoformat()}/{curr_end.isoformat()}",
            )
            try:
                response = retry_request(
                    client,
                    "GET",
                    url,
                    params={"apiKey": self._api_key, "limit": 50000},
                    headers={"Accept": "text/csv"},
                )
                response.raise_for_status()
            except (MaxRetryError, httpx.HTTPStatusError) as exc:
                logger.error(f"Failed to get ticker details for {symbol}")
                raise exc

            try:
                df = pd.read_csv(io.BytesIO(response.content))
            except pd.errors.EmptyDataError:
                logger.warning(
                    f"No data returned for {symbol} between {curr_start} and {curr_end}. Skipping this interval."
                )
                curr_start = curr_end + timedelta(days=1)
                curr_end = min(end, curr_start + timedelta(days=max_days))
                continue

            df[BarCol.DATETIME] = pd.to_datetime(df["t"], unit="ms")
            df = df.set_index(BarCol.DATETIME)
            df = df.tz_localize("utc").tz_convert(self.tz)
            df = (
                df.resample(freq)
                .first()
                .rename(
                    columns={
                        "v": BarCol.VOLUME,
                        "o": BarCol.OPEN,
                        "h": BarCol.HIGH,
                        "c": BarCol.CLOSE,
                        "l": BarCol.LOW,
                    }
                )[self.COLUMNS]
            )
            if unit in ["min", "t", "h"]:
                df = df[
                    (df.index.time >= pd.Timestamp("04:00").time()) & (df.index.time <= pd.Timestamp("20:00").time())  # type: ignore
                ]
            dfs.append(df)

            curr_start = curr_end + timedelta(days=1)
            curr_end = min(end, curr_start + timedelta(days=max_days))

        if not dfs:
            logger.warning(
                f"No data fetched for {symbol} in the entire date range {start} to {end}."
            )
            return pd.DataFrame(
                columns=self.COLUMNS,
                index=pd.DatetimeIndex([], name=BarCol.DATETIME),
            ).tz_localize(self.tz)

        df = pd.concat(dfs)
        assert isinstance(df.index, pd.DatetimeIndex)
        if df.index.tz is None:
            df = df.tz_localize("utc").tz_convert(self.tz)
        return df

    def _get(self, symbol: str, start: date, end: date, freq: str) -> pd.DataFrame:
        with httpx.Client() as client:
            return self._fetch(client, symbol, start, end, freq)


class PartitionedCSVBarsProvider(PolygonBarsProvider, BarsCache):
    CACHE_DIR = os.path.join("data", "candles")

    def _get(self, symbol: str, start: date, end: date, freq: str) -> pd.DataFrame:
        return self._get_from_cache(symbol, start, end, freq)


def _split_freq(freq: str) -> tuple[int, str]:
    match = re.match(r"(\d+)(\w+)", freq)
    if match is None:
        raise ValueError(f"Invalid frequency: {freq}")
    mult = int(match.groups()[0])
    unit = match.groups()[1].lower()
    return mult, unit
