import os
import json
import logging
from datetime import date, datetime, timedelta, time
from typing import List, Tuple
from zoneinfo import ZoneInfo

import httpx
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds

from fastscanner.pkg.localize import LOCAL_TIMEZONE_STR
from . import config
from .polygon import CandleCol, PolygonBarsProvider

logger = logging.getLogger(__name__)

class ParquetBarsProvider(PolygonBarsProvider):
    CACHE_DIR = os.path.join("data", "parquet_dataset")
    tz: str = LOCAL_TIMEZONE_STR

    def get(self, symbol: str, start: date, end: date, freq: str) -> pd.DataFrame:
        path = self._dataset_path(symbol, freq)

        if not os.path.exists(path):
            missing_ranges = [(start, end)]
        else:
            missing_ranges = self._missing_ranges(symbol, start, end, freq)

        if missing_ranges:
            with httpx.Client() as client:
                for rng_start, rng_end in missing_ranges:
                    df = self.Partition_fetch(client, symbol, rng_start, rng_end, freq)
                    self._save_cache(symbol, freq, df)
                self._save_current_range(symbol, freq, missing_ranges)

        try:
            dataset = ds.dataset(path, format="parquet")

            start_dt = datetime.combine(start, time.min).replace(tzinfo=ZoneInfo(self.tz))
            end_dt = datetime.combine(end, time.max).replace(tzinfo=ZoneInfo(self.tz))
            if "min" in freq:
                start_key = start.strftime("%Y-%m-%d")
                end_key = end.strftime("%Y-%m-%d")
            else:
                start_key = start.strftime("%Y-%m")
                end_key = end.strftime("%Y-%m")
            
            filter_expr = (
                (ds.field("date") >= pa.scalar(start_key)) &
                (ds.field("date") <= pa.scalar(end_key))
            )

            table = dataset.to_table(filter=filter_expr, columns=self.columns + [CandleCol.DATETIME])
            df = table.to_pandas()
        except Exception as e:
            logger.error(f"Failed to read Parquet file: {e}")
            return pd.DataFrame(columns=list(CandleCol.RESAMPLE_MAP.keys()),
                                index=pd.DatetimeIndex([], name=CandleCol.DATETIME)).tz_localize(self.tz)

        if df.empty:
            return df

        df[CandleCol.DATETIME] = pd.to_datetime(df[CandleCol.DATETIME], utc=True)
        df = df.set_index(CandleCol.DATETIME).tz_convert(self.tz)

        return df


    def _dataset_path(self, symbol: str, freq: str) -> str:
        return os.path.join(self.CACHE_DIR, f"{symbol}_{freq}.parquet")

    def _save_cache(self, symbol: str, freq: str, df: pd.DataFrame):
        if df.empty:
            return

        path = self._dataset_path(symbol, freq)
        os.makedirs(os.path.dirname(path), exist_ok=True)

        df = df.reset_index()

        df["date"] = df[CandleCol.DATETIME].dt.strftime(
            "%Y-%m-%d" if "min" in freq else "%Y-%m"
        )

        df[CandleCol.DATETIME] = df[CandleCol.DATETIME].dt.tz_convert("UTC").dt.tz_localize(None)

        if os.path.exists(path):
            existing_df = pq.read_table(path).to_pandas()
            combined_df = pd.concat([existing_df, df]).drop_duplicates(subset=[CandleCol.DATETIME], keep="last")
        else:
            combined_df = df

        table = pa.Table.from_pandas(combined_df, preserve_index=False)
        pq.write_table(table, path)

    def _current_range_path(self, symbol: str) -> str:
        return os.path.join(self.CACHE_DIR, f"{symbol}_current_ranges.json")

    def _load_current_range(self, symbol: str, freq: str) -> List[Tuple[date, date]]:
        try:
            with open(self._current_range_path(symbol)) as f:
                ranges = json.load(f)
                return [
                    (date.fromisoformat(start), date.fromisoformat(end))
                    for start, end in ranges.get(freq, [])
                ]
        except (FileNotFoundError, KeyError):
            return []

    def _save_current_range(self, symbol: str, freq: str, new_ranges: List[Tuple[date, date]]):
        path = self._current_range_path(symbol)
        os.makedirs(os.path.dirname(path), exist_ok=True)

        try:
            with open(path) as f:
                ranges = json.load(f)
        except FileNotFoundError:
            ranges = {}

        existing = self._load_current_range(symbol, freq)
        all_ranges = existing + new_ranges
        all_ranges = sorted(all_ranges)

        merged = []
        for rng in all_ranges:
            if not merged:
                merged.append(rng)
            else:
                last_start, last_end = merged[-1]
                curr_start, curr_end = rng
                if curr_start <= last_end + timedelta(days=1):
                    merged[-1] = (last_start, max(last_end, curr_end))
                else:
                    merged.append(rng)

        ranges[freq] = [(s.isoformat(), e.isoformat()) for s, e in merged]
        with open(path, "w") as f:
            json.dump(ranges, f)

    def _missing_ranges(self, symbol: str, start: date, end: date, freq: str) -> List[Tuple[date, date]]:
        existing_ranges = self._load_current_range(symbol, freq)
        if not existing_ranges:
            return [(start, end)]

        missing = []
        current = start
        for s, e in sorted(existing_ranges):
            if current < s:
                missing.append((current, min(end, s)))
            current = max(current, e + timedelta(days=1))
            if current > end:
                break

        if current <= end:
            missing.append((current, end))

        return [rng for rng in missing if rng[0] < rng[1]]

    def Partition_fetch(self, client: httpx.Client, symbol: str, start: date, end: date, freq: str) -> pd.DataFrame:
        if "min" in freq:
            delta = pd.Timedelta(days=7)
        elif "h" in freq:
            delta = pd.Timedelta(days=30)
        elif "d" in freq:
            delta = pd.Timedelta(days=3650)
        else:
            raise ValueError(f"Unsupported frequency: {freq}")

        curr_start = pd.Timestamp(start)
        df_all = []

        while curr_start.date() <= end:
            curr_end = min(curr_start + delta, pd.Timestamp(end) + pd.Timedelta(days=1))
            try:
                df = self._fetch(client, symbol, curr_start.date(), curr_end.date(), freq)
                if not df.empty:
                    df_all.append(df)
            except Exception as e:
                logger.error(f"Error fetching Polygon data from {curr_start.date()} to {curr_end.date()}: {e}")
            curr_start = curr_end

        return pd.concat(df_all).dropna() if df_all else pd.DataFrame()
