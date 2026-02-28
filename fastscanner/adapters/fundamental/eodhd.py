import asyncio
import json
import logging
import os
from dataclasses import asdict
from datetime import date, timedelta
from typing import Dict

import httpx
import pandas as pd

from fastscanner.pkg import config
from fastscanner.pkg.http import MaxRetryError, async_retry_request
from fastscanner.pkg.ratelimit import RateLimiter
from fastscanner.services.exceptions import NotFound
from fastscanner.services.indicators.ports import FundamentalData

logger = logging.getLogger(__name__)

EARNINGS_BATCH_SIZE = 50


class EODHDFundamentalStore:
    CACHE_DIR = os.path.join(config.DATA_BASE_DIR, "data", "fundamentals")
    RAW_CACHE_DIR = os.path.join(config.DATA_BASE_DIR, "data", "fundamentals_raw")

    def __init__(
        self,
        base_url: str,
        api_key: str,
        max_concurrent_requests: int = 20,
        max_requests_per_min: int = 900,
    ):
        self._base_url = base_url
        self._api_key = api_key
        self._semaphore = asyncio.Semaphore(max_concurrent_requests)
        self._rate_limiter = RateLimiter(
            max_requests=max_requests_per_min,
            time_window=60,
        )

    async def get(self, symbol: str) -> FundamentalData:
        cached = self._load_cached(symbol)
        if cached:
            return cached

        return await self._fetch(symbol)

    async def _fetch(self, symbol: str) -> FundamentalData:
        logger.info(f"Fetching fundamentals for {symbol} from EODHD")
        fundamentals = {}
        market_cap = {}
        earnings_records = []
        try:
            fundamentals, market_cap, earnings_records = await asyncio.gather(
                self._fetch_fundamentals(symbol),
                self._fetch_market_cap(symbol),
                self._fetch_earnings_calendar(
                    [symbol],
                    from_date=date(2005, 1, 1),
                    to_date=date.today() + timedelta(days=365),
                ),
            )
            earnings_dates = self._parse_earnings_records(earnings_records)
            fd = self._parse_data(fundamentals, market_cap, earnings_dates)
        except httpx.HTTPStatusError as e:
            if e.response.status_code != 404:
                logger.error(
                    f"Failed to fetch fundamentals for {symbol}: {e.response.text}"
                )
                raise e
            logger.warning(f"Symbol {symbol} not found in EODHD")
            fd = self._empty_data()

        self._store(symbol, fd)
        self._store_raw(symbol, fundamentals, market_cap)
        self._store_earnings_raw(symbol, earnings_records)
        logger.info(f"Stored fundamental data for {symbol}")
        return fd

    async def _fetch_fundamentals(self, symbol: str) -> Dict:
        url = f"{self._base_url}/fundamentals/{symbol}"
        params = {
            "filter": "General::Code,General,SharesStats,Technicals",
            "api_token": self._api_key,
            "fmt": "json",
        }
        return await self._fetch_json(url, params=params)

    async def _fetch_market_cap(self, symbol: str) -> Dict:
        url = f"{self._base_url}/historical-market-cap/{symbol}"
        params = {"api_token": self._api_key, "fmt": "json"}
        return await self._fetch_json(url, params=params)

    async def _fetch_earnings_calendar(
        self,
        symbols: list[str],
        from_date: date,
        to_date: date,
    ) -> list[dict]:
        url = f"{self._base_url}/calendar/earnings"
        params = {
            "api_token": self._api_key,
            "symbols": ",".join(symbols),
            "from": from_date.isoformat(),
            "to": to_date.isoformat(),
            "fmt": "json",
        }
        data = await self._fetch_json(url, params=params)
        return data.get("earnings", [])

    async def _fetch_json(self, url: str, params: Dict) -> dict:
        async with self._semaphore, self._rate_limiter:
            async with httpx.AsyncClient() as client:
                response = await async_retry_request(client, "GET", url, params=params)
                response.raise_for_status()
                return response.json()

    async def reload(self, symbol: str) -> FundamentalData:
        return await self._fetch(symbol)

    async def reload_earnings(self, symbols: list[str]) -> None:
        today = date.today()
        has_cache = [s for s in symbols if os.path.exists(self._get_cache_path(s))]
        no_cache = [s for s in symbols if not os.path.exists(self._get_cache_path(s))]

        if no_cache:
            await self._batch_fetch_earnings(
                no_cache,
                from_date=date(2005, 1, 1),
                to_date=today + timedelta(days=365),
            )

        if has_cache:
            await self._batch_fetch_earnings(
                has_cache,
                from_date=today - timedelta(days=7),
                to_date=today + timedelta(days=365),
            )

    async def _batch_fetch_earnings(
        self,
        symbols: list[str],
        from_date: date,
        to_date: date,
    ) -> None:
        batches = [
            symbols[i : i + EARNINGS_BATCH_SIZE]
            for i in range(0, len(symbols), EARNINGS_BATCH_SIZE)
        ]
        tasks = [
            asyncio.create_task(
                self._fetch_and_store_earnings_batch(batch, from_date, to_date)
            )
            for batch in batches
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _fetch_and_store_earnings_batch(
        self,
        batch: list[str],
        from_date: date,
        to_date: date,
    ) -> None:
        try:
            records = await self._fetch_earnings_calendar(batch, from_date, to_date)
        except (httpx.HTTPStatusError, MaxRetryError) as e:
            logger.error(f"Failed to fetch earnings calendar for batch: {e}")
            return

        by_symbol = self._group_earnings_by_symbol(records)

        for symbol in batch:
            symbol_records = by_symbol.get(symbol, [])
            new_dates = self._parse_earnings_records(symbol_records)
            cached = self._load_cached(symbol)
            if cached is not None:
                new_dates = cached.earnings_dates.union(new_dates).sort_values()

            self._store_earnings_raw(symbol, symbol_records)
            self._update_cached_earnings(symbol, new_dates)

    def _group_earnings_by_symbol(self, records: list[dict]) -> dict[str, list[dict]]:
        by_symbol: dict[str, list[dict]] = {}
        for record in records:
            code = record.get("code", "")
            symbol = code.split(".")[0] if "." in code else code
            by_symbol.setdefault(symbol, []).append(record)
        return by_symbol

    def _parse_earnings_records(self, records: list[dict]) -> pd.DatetimeIndex:
        dates = []
        for r in records:
            report_date = r.get("report_date")
            if report_date:
                dates.append(report_date)
        if not dates:
            return pd.DatetimeIndex([], dtype="datetime64[ns]", name="report_date")
        return (
            pd.DatetimeIndex(pd.to_datetime(dates), name="report_date")
            .drop_duplicates()
            .sort_values()
        )

    def _update_cached_earnings(
        self, symbol: str, earnings_dates: pd.DatetimeIndex
    ) -> None:
        path = self._get_cache_path(symbol)
        if not os.path.exists(path):
            return
        try:
            with open(path, "r") as f:
                data = json.load(f)
        except json.JSONDecodeError:
            return
        data["earnings_dates"] = [dt.date().isoformat() for dt in earnings_dates]
        with open(path, "w") as f:
            json.dump(data, f, indent=2)

    def _store_earnings_raw(self, symbol: str, records: list[dict]) -> None:
        os.makedirs(self.RAW_CACHE_DIR, exist_ok=True)
        path = os.path.join(self.RAW_CACHE_DIR, f"{symbol}_earnings.json")

        if os.path.exists(path):
            try:
                with open(path, "r") as f:
                    existing = json.load(f)
            except json.JSONDecodeError:
                existing = []
            seen = {r.get("report_date") for r in existing}
            for r in records:
                if r.get("report_date") not in seen:
                    existing.append(r)
                    seen.add(r.get("report_date"))
            records = existing

        with open(path, "w") as f:
            json.dump(records, f)

    def _load_cached(self, symbol: str) -> FundamentalData | None:
        path = self._get_cache_path(symbol)
        if not os.path.exists(path):
            raw_data = self._load_raw_cached(symbol)
            if raw_data is None:
                return None

            fundamentals = raw_data.get("fundamentals", {})
            market_cap = raw_data.get("market_cap", {})
            earnings_dates = self._load_raw_earnings_cached(symbol)
            fd = self._parse_data(fundamentals, market_cap, earnings_dates)
            return fd

        try:
            with open(path, "r") as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            logger.warning(f"Cache for {symbol} is corrupted (JSON error): {e}")
            return None

        market_cap_data = data.get("historical_market_cap", {})
        market_cap = pd.Series(
            data=[float(v) for v in market_cap_data.values()],
            index=pd.to_datetime(list(market_cap_data.keys())).date,
            dtype=float,
        )
        earnings_dates_data = data.get("earnings_dates", [])
        earnings_dates = pd.DatetimeIndex(
            pd.to_datetime(earnings_dates_data), name="report_date"
        )
        return FundamentalData(
            type=data["type"],
            exchange=data["exchange"],
            country=data["country"],
            city=data["city"],
            gic_industry=data["gic_industry"],
            gic_sector=data["gic_sector"],
            historical_market_cap=market_cap,
            earnings_dates=earnings_dates,
            insiders_ownership_perc=data["insiders_ownership_perc"],
            institutional_ownership_perc=data["institutional_ownership_perc"],
            shares_float=data["shares_float"],
            beta=data["beta"],
            ipo_date=data.get("ipo_date"),
            ceo_name=data.get("ceo_name"),
            cfo_name=data.get("cfo_name"),
        )

    def _load_raw_cached(self, symbol: str) -> dict | None:
        path = self._get_raw_cache_path(symbol)
        if not os.path.exists(path):
            return None

        try:
            with open(path, "r") as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            logger.warning(f"Raw cache for {symbol} is corrupted (JSON error): {e}")
            return None

    def _load_raw_earnings_cached(self, symbol: str) -> pd.DatetimeIndex | None:
        path = os.path.join(self.RAW_CACHE_DIR, f"{symbol}_earnings.json")
        if not os.path.exists(path):
            return None
        try:
            with open(path, "r") as f:
                records = json.load(f)
        except json.JSONDecodeError:
            return None
        return self._parse_earnings_records(records)

    def _parse_data(
        self,
        fundamentals: dict,
        market_cap: dict,
        earnings_dates: pd.DatetimeIndex | None = None,
    ) -> FundamentalData:
        fundamentals = {
            k: v for k, v in fundamentals.items() if v is not None and v != "NA"
        }
        general = fundamentals.get("General") or {}
        shares_stats = fundamentals.get("SharesStats") or {}
        technicals = fundamentals.get("Technicals", {}) or {}

        market_cap_data = {
            v["date"]: float(v["value"])
            for v in market_cap.values()
            if isinstance(v, dict) and "date" in v and "value" in v
        }
        historical_market_cap = pd.Series(
            data=list(market_cap_data.values()),
            index=pd.to_datetime(list(market_cap_data.keys())).date,
            dtype=float,
        ).sort_index()

        if earnings_dates is None:
            earnings_dates = pd.DatetimeIndex(
                [], dtype="datetime64[ns]", name="report_date"
            )

        address_data = general.get("AddressData", {}) or {}
        insiders_ownership_perc = None
        institutional_ownership_perc = None
        shares_float = None
        beta = None
        if shares_stats.get("PercentInsiders") is not None:
            insiders_ownership_perc = float(shares_stats["PercentInsiders"])
        if shares_stats.get("PercentInstitutions") is not None:
            institutional_ownership_perc = float(shares_stats["PercentInstitutions"])
        if shares_stats.get("SharesFloat") is not None:
            shares_float = float(shares_stats["SharesFloat"])
        if technicals.get("Beta") is not None:
            beta = float(technicals["Beta"])

        ipo_date = general.get("IPODate")

        officers = general.get("Officers", {})
        ceo_name = None
        cfo_name = None
        for officer in officers.values() if isinstance(officers, dict) else []:
            title = officer.get("Title", "") or ""
            name = officer.get("Name")
            if "CEO" in title.upper() and ceo_name is None:
                ceo_name = name
            if "CFO" in title.upper() and cfo_name is None:
                cfo_name = name

        return FundamentalData(
            type=general.get("Type", ""),
            exchange=general.get("Exchange", ""),
            country=address_data.get("Country", ""),
            city=address_data.get("City", ""),
            gic_industry=general.get("GicIndustry", ""),
            gic_sector=general.get("GicSector", ""),
            historical_market_cap=historical_market_cap,
            earnings_dates=earnings_dates,
            insiders_ownership_perc=insiders_ownership_perc,
            institutional_ownership_perc=institutional_ownership_perc,
            shares_float=shares_float,
            beta=beta,
            ipo_date=ipo_date,
            ceo_name=ceo_name,
            cfo_name=cfo_name,
        )

    def _get_cache_path(self, symbol: str) -> str:
        return os.path.join(self.CACHE_DIR, f"{symbol}.json")

    def _get_raw_cache_path(self, symbol: str) -> str:
        return os.path.join(self.RAW_CACHE_DIR, f"{symbol}.json")

    def _store(self, symbol: str, data: FundamentalData) -> None:
        os.makedirs(self.CACHE_DIR, exist_ok=True)
        path = self._get_cache_path(symbol)

        data_dict = asdict(data)
        data_dict["historical_market_cap"] = {
            str(idx): float(val) for idx, val in data.historical_market_cap.items()
        }
        data_dict["earnings_dates"] = [
            date_.date().isoformat() for date_ in data.earnings_dates
        ]

        with open(path, "w") as f:
            json.dump(data_dict, f, indent=2)

    def _store_raw(self, symbol: str, fundamental: dict, market_cap: dict) -> None:
        data = {
            "fundamentals": fundamental,
            "market_cap": market_cap,
        }
        os.makedirs(self.RAW_CACHE_DIR, exist_ok=True)
        path = self._get_raw_cache_path(symbol)
        with open(path, "w") as f:
            json.dump(data, f)

    def _store_empty(self, symbol: str) -> None:
        os.makedirs(self.CACHE_DIR, exist_ok=True)
        path = self._get_cache_path(symbol)
        with open(path, "w") as f:
            json.dump({}, f, indent=2)

    def _empty_data(self) -> FundamentalData:
        return FundamentalData(
            type="",
            exchange="",
            country="",
            city="",
            gic_industry="",
            gic_sector="",
            historical_market_cap=pd.Series(dtype=float),
            earnings_dates=pd.DatetimeIndex(
                [], dtype="datetime64[ns]", name="report_date"
            ),
            insiders_ownership_perc=None,
            institutional_ownership_perc=None,
            shares_float=None,
            beta=None,
            ipo_date=None,
            ceo_name=None,
            cfo_name=None,
        )
