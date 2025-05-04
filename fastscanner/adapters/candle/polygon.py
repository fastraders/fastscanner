import io
import logging
import re
from datetime import date, timedelta
from urllib.parse import urljoin

import httpx
import pandas as pd

from fastscanner.pkg.datetime import LOCAL_TIMEZONE_STR
from fastscanner.pkg.http import MaxRetryError, retry_request
from fastscanner.services.indicators.ports import CandleCol

logger = logging.getLogger(__name__)


class PolygonCandlesProvider:
    tz: str = LOCAL_TIMEZONE_STR
    columns = list(CandleCol.RESAMPLE_MAP.keys())

    def __init__(self, base_url: str, api_key: str):
        self._base_url = base_url
        self._api_key = api_key

    def _fetch(
        self,
        client: httpx.Client,
        symbol: str,
        start: date,
        end: date,
        freq: str,
    ) -> pd.DataFrame:
        mult, unit = split_freq(freq)
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

            df[CandleCol.DATETIME] = pd.to_datetime(df["t"], unit="ms")
            df = df.set_index(CandleCol.DATETIME)
            df = df.tz_localize("utc").tz_convert(self.tz)
            df = (
                df.resample(freq)
                .first()
                .rename(
                    columns={
                        "v": CandleCol.VOLUME,
                        "o": CandleCol.OPEN,
                        "h": CandleCol.HIGH,
                        "c": CandleCol.CLOSE,
                        "l": CandleCol.LOW,
                    }
                )[self.columns]
            )
            if unit in ("min", "h", "t"):
                df = df[
                    (df.index.time >= pd.Timestamp("04:00").time()) & (df.index.time <= pd.Timestamp("20:00").time())  # type: ignore
                ]
            dfs.append(df)

            curr_start = curr_end + timedelta(days=1)
            curr_end = min(end, curr_start + timedelta(days=max_days))

        if not dfs:
            return pd.DataFrame(
                columns=self.columns,
                index=pd.DatetimeIndex([], name=CandleCol.DATETIME),
            ).tz_localize(self.tz)

        df = pd.concat(dfs)
        assert isinstance(df.index, pd.DatetimeIndex)
        if df.index.tz is None:
            df = df.tz_localize("utc").tz_convert(self.tz)
        return df

    def get(self, symbol: str, start: date, end: date, freq: str) -> pd.DataFrame:
        with httpx.Client() as client:
            return self._fetch(client, symbol, start, end, freq)


def split_freq(freq: str) -> tuple[int, str]:
    match = re.match(r"(\d+)(\w+)", freq)
    if match is None:
        raise ValueError(f"Invalid frequency: {freq}")
    mult = int(match.groups()[0])
    unit = match.groups()[1].lower()
    return mult, unit
