import os
import numpy as np
import pandas as pd
import httpx
from datetime import datetime, time
import pytz
import logging

from fastscanner.pkg.localize import LOCAL_TIMEZONE_STR
from .polygon import CandleCol, PolygonBarsProvider

logger = logging.getLogger(__name__)

MEMMAP_DTYPE = np.dtype([
    ("timestamp", "int64"),
    ("open", "float32"),
    ("high", "float32"),
    ("low", "float32"),
    ("close", "float32"),
    ("volume", "float32"),
])

class MemmapBarsProvider(PolygonBarsProvider):
    CACHE_DIR = os.path.join("data", "candles_memmap")
    tz: str = LOCAL_TIMEZONE_STR

    def get(self, symbol: str, start, end, freq: str) -> pd.DataFrame:
        file_path = self._file_path(symbol, freq)

        if not os.path.exists(file_path):
            self._fetch_and_save(symbol, freq, file_path)

        df = self._load_memmap(file_path)
        df.index = df.index.tz_convert(self.tz)

        start_dt = pytz.timezone(self.tz).localize(datetime.combine(start, time.min))
        end_dt = pytz.timezone(self.tz).localize(datetime.combine(end, time.max))
        df = df.loc[start_dt:end_dt]

        if freq in ("1min", "1h", "1d"):
            return df

        return df.resample(freq).aggregate(CandleCol.RESAMPLE_MAP).dropna()

    def _fetch_and_save(self, symbol, freq, file_path):
        start = datetime(2023, 1, 1).date()
        end = datetime.now().date()
        with httpx.Client() as client:
            df = self._fetch(client, symbol, start, end, freq).dropna()

        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        df = df.tz_convert("utc")

        memmap_data = np.empty(len(df), dtype=MEMMAP_DTYPE)
        memmap_data["timestamp"] = df.index.view("int64")
        memmap_data["open"] = df[CandleCol.OPEN]
        memmap_data["high"] = df[CandleCol.HIGH]
        memmap_data["low"] = df[CandleCol.LOW]
        memmap_data["close"] = df[CandleCol.CLOSE]
        memmap_data["volume"] = df[CandleCol.VOLUME]

        with open(file_path, "wb") as f:
            f.write(memmap_data.tobytes())

    def _load_memmap(self, file_path):
        mm = np.memmap(file_path, dtype=MEMMAP_DTYPE, mode="r")
        df = pd.DataFrame(mm)
        df.index = pd.to_datetime(df["timestamp"], utc=True)
        df = df.drop(columns=["timestamp"])
        return df

    def _file_path(self, symbol: str, freq: str) -> str:
        return os.path.join(self.CACHE_DIR, f"{symbol}_{freq}.dat")
