import os
import numpy as np
import pandas as pd
import logging
from datetime import datetime, time
import pytz
import httpx

from .polygon import CandleCol, PolygonBarsProvider
from fastscanner.pkg.localize import LOCAL_TIMEZONE_STR

logger = logging.getLogger(__name__)

class MemmapBarsProvider(PolygonBarsProvider):
    CACHE_DIR = os.path.join("data", "candles_memmap")
    tz: str = LOCAL_TIMEZONE_STR

    def get(self, symbol: str, start, end, freq: str) -> pd.DataFrame:
        file_path = self._file_path(symbol, freq)

        if not os.path.exists(file_path):
            self._fetch_and_save(symbol, freq, file_path)

        df = self._load_memmap(file_path)

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
        df = df.tz_convert("utc").tz_convert(None).reset_index()
        df.to_csv(file_path, index=False)

    def _load_memmap(self, file_path):
        df = pd.read_csv(file_path)
        df[CandleCol.DATETIME] = pd.to_datetime(df[CandleCol.DATETIME], utc=True)
        df = df.set_index(CandleCol.DATETIME).tz_convert(self.tz)
        return df

    def _file_path(self, symbol: str, freq: str) -> str:
        return os.path.join(self.CACHE_DIR, f"{symbol}_{freq}.dat")
