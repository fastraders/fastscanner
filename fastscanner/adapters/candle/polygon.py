import asyncio
import io
import json
import logging
import os
from datetime import date, datetime, time, timedelta
from urllib.parse import urljoin

import httpx
import pandas as pd

from fastscanner.pkg import config
from fastscanner.pkg.clock import LOCAL_TIMEZONE_STR, ClockRegistry, split_freq
from fastscanner.pkg.http import MaxRetryError, async_retry_request
from fastscanner.pkg.ratelimit import RateLimiter
from fastscanner.services.indicators.ports import CandleCol

logger = logging.getLogger(__name__)


class PolygonCandlesProvider:
    tz: str = LOCAL_TIMEZONE_STR
    columns = list(CandleCol.RESAMPLE_MAP.keys())

    def __init__(
        self,
        base_url: str,
        api_key: str,
        max_requests_per_sec: int = 100,
        max_concurrent_requests: int = 50,
    ):
        self._base_url = base_url
        self._api_key = api_key
        self._rate_limit = RateLimiter(max_requests_per_sec)
        self._semaphore = asyncio.Semaphore(max_concurrent_requests)

    async def _fetch(
        self,
        client: httpx.AsyncClient,
        symbol: str,
        start: date,
        end: date,
        freq: str,
    ) -> pd.DataFrame:
        mult, unit = split_freq(freq)
        unit_mappers = {
            "s": "second",
            "min": "minute",
            "h": "hour",
            "d": "day",
        }
        max_days_per_unit: dict[str, float] = {
            "s": 0.5,
            "min": 30,
            "h": 30,
            "d": 50000,
        }
        max_days = max_days_per_unit[unit]
        max_end = ClockRegistry.clock.today() - timedelta(days=1)
        if end > max_end:
            raise ValueError(
                f"End date {end} is in the future. Please provide a valid end date."
            )

        inc = round(max_days * 24 * 60 * 60 * 1000) - 1
        curr_start = round(pd.Timestamp(start, tz=self.tz).timestamp() * 1000)
        curr_end = curr_start + inc
        end_ts = round(
            pd.Timestamp(end + timedelta(days=1), tz=self.tz).timestamp() * 1000 - 1
        )
        dfs: list[pd.DataFrame] = []

        while curr_start <= end_ts:
            url = urljoin(
                self._base_url,
                f"v2/aggs/ticker/{symbol}/range/{mult}/{unit_mappers[unit]}/{curr_start}/{curr_end}",
            )
            try:
                response = await async_retry_request(
                    client,
                    "GET",
                    url,
                    params={
                        "apiKey": self._api_key,
                        "limit": 50000,
                        # "adjusted": "true",
                    },
                    headers={"Accept": "text/csv"},
                )
                if response.status_code == 404:
                    curr_start = curr_end + 1
                    curr_end = min(end_ts, curr_start + inc)
                    continue

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
                curr_start = curr_end + 1
                curr_end = min(end_ts, curr_start + inc)
                continue

            df[CandleCol.DATETIME] = pd.to_datetime(df.loc[:, "t"], unit="ms")
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
                )
                .dropna(subset=(CandleCol.OPEN,))
                .sort_index()[self.columns]
            )
            if not df.empty:
                dfs.append(df)

            curr_start = curr_end + 1
            curr_end = min(end_ts, curr_start + inc)

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

    async def get(self, symbol: str, start: date, end: date, freq: str) -> pd.DataFrame:
        async with self._semaphore:
            async with self._rate_limit:
                async with httpx.AsyncClient() as client:
                    return await self._fetch(client, symbol, start, end, freq)

    async def splits(self, start: date, end: date) -> dict[str, date]:
        async with httpx.AsyncClient() as client:
            url = urljoin(self._base_url, "v3/reference/splits")
            params = {
                "apiKey": self._api_key,
                "limit": 1000,
                "order": "asc",
                "sort": "execution_date",
                "execution_date.gte": start.isoformat(),
                "execution_date.lte": end.isoformat(),
            }
            splits: dict[str, date] = {}
            while True:
                response = await async_retry_request(
                    client,
                    "GET",
                    url,
                    params=params,
                )
                response.raise_for_status()
                data = response.json()
                if len(data.get("results", [])) == 0:
                    break
                for item in data["results"]:
                    ticker = item["ticker"]
                    execution_date = date.fromisoformat(item["execution_date"])
                    splits[ticker] = execution_date

                if data.get("next_url") is None:
                    break
                url = f'{data["next_url"]}&apiKey={self._api_key}'
                params = None

        return splits

    async def _all_symbols(self, **filter) -> list[str]:
        symbols: list[str] = []

        async with httpx.AsyncClient() as client:
            url = urljoin(self._base_url, "v3/reference/tickers")
            params = {
                "apiKey": self._api_key,
                "type": "CS",
                "limit": 1000,
                **filter,
            }
            while True:
                response = await async_retry_request(
                    client,
                    "GET",
                    url,
                    params=params,
                )
                response.raise_for_status()
                data = response.json()
                if data.get("count", 0) == 0:
                    break

                for item in data["results"]:
                    symbols.append(item["ticker"])

                if data.get("next_url") is None:
                    break
                url = f'{data["next_url"]}&apiKey={self._api_key}'
                params = None
        return symbols

    async def all_symbols(self) -> list[str]:
        symbols: list[str] = []
        symbols_path = os.path.join(
            config.DATA_BASE_DIR, "data", "polygon_symbols.json"
        )

        if os.path.exists(symbols_path):
            with open(symbols_path, "r") as f:
                return json.load(f)

        symbols = await self._all_symbols(active=True)
        symbols.extend(await self._all_symbols(active=False))
        symbols = sorted(set(symbols))

        os.makedirs(os.path.dirname(symbols_path), exist_ok=True)
        with open(symbols_path, "w") as f:
            json.dump(list(symbols), f)
        return symbols

    async def active_symbols(self, exchanges: list[str] | None = None) -> list[str]:
        if exchanges is None:
            exchanges = ["XNAS", "XNYS", "XASE"]

        all_symbols: list[str] = []

        for exchange in exchanges:
            try:
                symbols = await self._all_symbols(active=True, exchange=exchange)
                all_symbols.extend(symbols)
            except Exception as e:
                logger.warning(f"Failed to fetch symbols from exchange {exchange}: {e}")
                continue

        return sorted(list(set(all_symbols)))
