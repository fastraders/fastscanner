import os
import logging
from calendar import monthrange
from datetime import date, datetime, time, timedelta

import httpx
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytz

from fastscanner.pkg.localize import LOCAL_TIMEZONE_STR
from .polygon import CandleCol, PolygonBarsProvider, split_freq

logger = logging.getLogger(__name__)


class PartitionedParquetBarsProvider(PolygonBarsProvider):
    CACHE_DIR = os.path.join("data", "parquet_cache")
    tz: str = LOCAL_TIMEZONE_STR

    def get(self, symbol: str, start: date, end: date, freq: str) -> pd.DataFrame:
        _, unit = split_freq(freq)
        keys = self._partition_keys_in_range(start, end, unit)

        dfs: list[pd.DataFrame] = []
        for key in keys:
            df = self._load_or_fetch(symbol, key, unit, freq, start, end)
            if not df.empty:
                dfs.append(df)

        if not dfs:
            logger.warning(f"No data found for {symbol} between {start} and {end}.")
            return pd.DataFrame(columns=self.columns, index=pd.DatetimeIndex([], name=CandleCol.DATETIME)).tz_localize(self.tz)

        df = pd.concat(dfs)
        df = df.loc[self._to_local(start):self._to_local(end, end_of_day=True)]

        if freq in ("1min", "1h", "1d"):
            return df

        return df.resample(freq).aggregate(CandleCol.RESAMPLE_MAP).dropna()

    def _load_or_fetch(self, symbol: str, key: str, unit: str, freq: str, start: date, end: date) -> pd.DataFrame:
        path = self._partition_path(symbol, key, unit)
        if os.path.exists(path):
            try:
                filters = [
                    (CandleCol.DATETIME, ">=", datetime.combine(start, time.min)),
                    (CandleCol.DATETIME, "<=", datetime.combine(end, time.max))
                ]

                table = pq.read_table(path, filters=filters)
                df = table.to_pandas()
                df[CandleCol.DATETIME] = pd.to_datetime(df[CandleCol.DATETIME], utc=True)
                return df.set_index(CandleCol.DATETIME).tz_convert(self.tz)
            except Exception as e:
                logger.error(f"Failed to read Parquet file {path}: {e}")

        start_range, end_range = self._range_from_key(key, unit)
        with httpx.Client() as client:
            df = self._fetch(client, symbol, start_range, end_range, f"1{unit}").dropna()
        self._save_partition(symbol, key, unit, df)
        return df

    def _save_partition(self, symbol: str, key: str, unit: str, df: pd.DataFrame):
        path = self._partition_path(symbol, key, unit)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df_out = df.tz_convert("utc").tz_convert(None).reset_index()
        table = pa.Table.from_pandas(df_out)
        pq.write_table(table, path, row_group_size=10000)

    def _partition_path(self, symbol: str, key: str, unit: str) -> str:
        return os.path.join(self.CACHE_DIR, symbol, f"{unit}_{key}.parquet")

    def _to_local(self, dt: date, end_of_day: bool = False) -> pd.Timestamp:
        return pytz.timezone(self.tz).localize(
            datetime.combine(dt, time(23, 59, 59) if end_of_day else time(0, 0))
        )

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
