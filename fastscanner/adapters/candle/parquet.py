import os
import json
import logging
from calendar import monthrange
from datetime import date, datetime, time, timedelta

import httpx
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytz
import zoneinfo

from fastscanner.pkg.localize import LOCAL_TIMEZONE_STR
from .polygon import CandleCol, PolygonBarsProvider, split_freq

logger = logging.getLogger(__name__)


class ParquetBarsProvider(PolygonBarsProvider):
    CACHE_DIR = os.path.join("data", "parquet_cache")
    tz: str = LOCAL_TIMEZONE_STR

    def get(self, symbol: str, start: date, end: date, freq: str) -> pd.DataFrame:
        path = self._cache_path(symbol, freq)
        _, unit = split_freq(freq)
        keys = self._partition_keys_in_range(start, end, unit)

        all_dfs: list[pd.DataFrame] = []

        existing_df = self._load_cache(path) if os.path.exists(path) else pd.DataFrame(columns=self.columns, index=pd.DatetimeIndex([], name=CandleCol.DATETIME))

        for key in keys:
            part_start, part_end = self._range_from_key(key, unit)
            local_start = self._to_local(part_start)
            local_end = self._to_local(part_end, end_of_day=True)

            cached_slice = existing_df.loc[local_start:local_end] if not existing_df.empty else pd.DataFrame()

            if not cached_slice.empty and len(cached_slice) > 0:
                all_dfs.append(cached_slice)
                continue

            with httpx.Client() as client:
                df = self._fetch(client, symbol, part_start, part_end, f"1{unit}").dropna()
                all_dfs.append(df)
                existing_df = pd.concat([existing_df, df]).drop_duplicates().sort_index()
                self._save_cache(path, existing_df)

        if not all_dfs:
            logger.warning(f"No data found for {symbol} between {start} and {end}.")
            return pd.DataFrame(columns=self.columns, index=pd.DatetimeIndex([], name=CandleCol.DATETIME)).tz_localize(self.tz)

        final_df = pd.concat(all_dfs).drop_duplicates().sort_index()

        start_dt = self._to_local(start)
        end_dt = self._to_local(end, end_of_day=True)
        final_df = final_df.loc[start_dt:end_dt]

        if freq in ("1min", "1h", "1d"):
            return final_df

        return final_df.resample(freq).aggregate(CandleCol.RESAMPLE_MAP).dropna()  # type: ignore

    def _to_local(self, dt: date, end_of_day: bool = False) -> pd.Timestamp:
        return pytz.timezone(self.tz).localize(
            datetime.combine(dt, time(23, 59, 59) if end_of_day else time(0, 0))
        )

    def _cache_path(self, symbol: str, freq: str) -> str:
        return os.path.join(self.CACHE_DIR, f"{symbol}_{freq}.parquet")

    def _load_cache(self, path: str) -> pd.DataFrame:
        try:
            table = pq.read_table(path)
            df = table.to_pandas()
            df[CandleCol.DATETIME] = pd.to_datetime(df[CandleCol.DATETIME], utc=True)
            return df.set_index(CandleCol.DATETIME).tz_convert(self.tz)
        except Exception as e:
            logger.warning(f"Failed to load Parquet file {path}: {e}")
            return pd.DataFrame(columns=self.columns, index=pd.DatetimeIndex([], name=CandleCol.DATETIME))

    def _save_cache(self, path: str, df: pd.DataFrame):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df_out = df.tz_convert("utc").tz_convert(None).reset_index()
        table = pa.Table.from_pandas(df_out)
        pq.write_table(table, path, row_group_size=10000)

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

    def _partition_keys(self, index: pd.DatetimeIndex, unit: str) -> "pd.Series[str]":
        if unit.lower() in ("min", "t"):
            dt = pd.to_timedelta(index.dayofweek, unit="d")
            return pd.Series(
                (index - dt).strftime("%Y-%m-%d"), index=index, name="partition_key"
            )
        if unit.lower() in ("h", "d"):
            return pd.Series(index.strftime("%Y-%m"), index=index, name="partition_key")
        raise ValueError(f"Invalid unit: {unit}")
