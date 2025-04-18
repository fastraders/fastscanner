import os
import json
import logging
from calendar import monthrange
from datetime import date, datetime, time, timedelta
from typing import Dict, List
from zoneinfo import ZoneInfo

import httpx
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from fastscanner.pkg.localize import LOCAL_TIMEZONE_STR
from . import config
from .polygon import CandleCol, PolygonBarsProvider, split_freq

logger = logging.getLogger(__name__)

class ParquetBarsProvider(PolygonBarsProvider):
    CACHE_DIR = os.path.join("data", "parquet_dataset")
    tz: str = LOCAL_TIMEZONE_STR

    def get(self, symbol: str, start: date, end: date, freq: str) -> pd.DataFrame:
        _, unit = split_freq(freq)
        keys = self._partition_keys_in_range(start, end, unit)

        dataset_path = os.path.join(self.CACHE_DIR, symbol)
        if not os.path.exists(dataset_path):
            return pd.DataFrame(columns=list(CandleCol.RESAMPLE_MAP.keys()),
                                index=pd.DatetimeIndex([], name=CandleCol.DATETIME)).tz_localize(self.tz)

        filter_expr = (
            ds.field("unit") == unit
        ) & (
            ds.field("partition_key").isin(keys)
        )


        try:
            dataset = ds.dataset(dataset_path, format="parquet", partitioning="hive")
            table = dataset.to_table(filter=filter_expr)
            df = table.to_pandas()
        except Exception as e:
            logger.error(f"Failed to read from Parquet dataset: {e}")
            return pd.DataFrame(columns=list(CandleCol.RESAMPLE_MAP.keys()),
                                index=pd.DatetimeIndex([], name=CandleCol.DATETIME)).tz_localize(self.tz)

        if df.empty:
            return df

        df[CandleCol.DATETIME] = pd.to_datetime(df[CandleCol.DATETIME], utc=True)
        df = df.set_index(CandleCol.DATETIME).tz_convert(self.tz)

        start_dt = datetime.combine(start, time.min).replace(tzinfo=ZoneInfo(self.tz))
        end_dt = datetime.combine(end, time.max).replace(tzinfo=ZoneInfo(self.tz))
        df = df.loc[start_dt:end_dt]

        if freq in ("1min", "1h", "1d"):
            return df

        return df.resample(freq).aggregate(CandleCol.RESAMPLE_MAP).dropna()

    def _cache(self, symbol: str, key: str, unit: str) -> pd.DataFrame:
        dataset_path = os.path.join(self.CACHE_DIR, symbol)

        if os.path.exists(dataset_path) and not self._is_expired(symbol, key, unit):
            try:
                dataset = ds.dataset(dataset_path, format="parquet", partitioning="hive")
                table = dataset.to_table(filter=ds.field("unit") == unit & ds.field("partition_key") == key)
                df = table.to_pandas()
                if not df.empty:
                    df[CandleCol.DATETIME] = pd.to_datetime(df[CandleCol.DATETIME], utc=True)
                    return df.set_index(CandleCol.DATETIME).tz_convert(self.tz)
            except Exception as e:
                logger.error(f"Failed to load parquet data: {e}")

        start, end = self._range_from_key(key, unit)
        with httpx.Client() as client:
            df = self._fetch(client, symbol, start, end, f"1{unit}").dropna()
            self._save_cache(symbol, unit, key, df)
            self._mark_expiration(symbol, key, unit)
            return df

    def _save_cache(self, symbol: str, unit: str, key: str, df: pd.DataFrame):
        dataset_path = os.path.join(self.CACHE_DIR, symbol)
        os.makedirs(dataset_path, exist_ok=True)

        df = df.reset_index()
        df[CandleCol.DATETIME] = df[CandleCol.DATETIME].dt.tz_convert("UTC").dt.tz_localize(None)
        df["partition_key"] = key
        df["unit"] = unit

        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_to_dataset(table, root_path=dataset_path, partition_cols=["unit", "partition_key"])

    def _partition_keys(self, index: pd.DatetimeIndex, unit: str) -> pd.Series:
        if unit.lower() in ("min", "t"):
            dt = pd.to_timedelta(index.dayofweek, unit="d")
            return pd.Series((index - dt).strftime("%Y-%m-%d"), index=index, name="partition_key")
        elif unit.lower() in ("h", "d"):
            return pd.Series(index.strftime("%Y-%m"), index=index, name="partition_key")
        raise ValueError(f"Invalid unit: {unit}")

    def _partition_keys_in_range(self, start: date, end: date, unit: str) -> List[str]:
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

    _expirations: Dict[str, Dict[str, date]]

    def _is_expired(self, symbol: str, key: str, unit: str) -> bool:
        self._load_expirations(symbol)
        expiration_key = self._expiration_key(key, unit)
        expirations = self._expirations.get(symbol, {})
        if expiration_key not in expirations:
            return False
        today = datetime.now(ZoneInfo(self.tz)).date()
        return expirations[expiration_key] <= today

    def _mark_expiration(self, symbol: str, key: str, unit: str) -> None:
        self._load_expirations(symbol)
        _, end = self._range_from_key(key, unit)
        today = datetime.now(ZoneInfo(self.tz)).date()
        expiration_key = self._expiration_key(key, unit)
        expirations = self._expirations.get(symbol, {})

        if today > end and expiration_key not in expirations:
            return
        if today > end:
            expirations.pop(expiration_key, None)
        else:
            expirations[expiration_key] = today + timedelta(days=1)

        with open(os.path.join(self.CACHE_DIR, symbol, "expirations.json"), "w") as f:
            json.dump({k: v.isoformat() for k, v in expirations.items()}, f)

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
