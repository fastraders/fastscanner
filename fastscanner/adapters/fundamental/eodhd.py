import asyncio
import json
import logging
import os
from dataclasses import asdict
from typing import Dict

import httpx
import pandas as pd

from fastscanner.pkg import config
from fastscanner.pkg.http import MaxRetryError, async_retry_request
from fastscanner.services.exceptions import NotFound
from fastscanner.services.indicators.ports import FundamentalData

logger = logging.getLogger(__name__)


class EODHDFundamentalStore:
    CACHE_DIR = os.path.join(config.DATA_BASE_DIR, "data", "fundamentals")

    def __init__(self, base_url: str, api_key: str, max_concurrent_requests: int = 50):
        self._base_url = base_url
        self._api_key = api_key
        self._semaphore = asyncio.Semaphore(max_concurrent_requests)

    async def get(self, symbol: str) -> FundamentalData:
        cached = self._load_cached(symbol)
        if cached:
            return cached

        logger.info(f"Fetching fundamentals for {symbol} from EODHD")
        async with self._semaphore:
            try:
                fundamentals = await self._fetch_fundamentals(symbol)
                market_cap = await self._fetch_market_cap(symbol)
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    logger.warning(f"Symbol {symbol} not found in EODHD")
                    raise NotFound(f"Symbol {symbol} not found in EODHD") from e

        fd = self._parse_data(fundamentals, market_cap)
        self._store(symbol, fd)
        logger.info(f"Stored fundamental data for {symbol}")
        return fd

    async def _fetch_fundamentals(self, symbol: str) -> Dict:
        url = f"{self._base_url}/fundamentals/{symbol}"
        params = {
            "filter": "General::Code,General,Earnings,SharesStats",
            "api_token": self._api_key,
            "fmt": "json",
        }
        return await self._fetch_json(url, params=params)

    async def _fetch_market_cap(self, symbol: str) -> Dict:
        url = f"{self._base_url}/historical-market-cap/{symbol}"
        params = {"api_token": self._api_key, "fmt": "json"}
        return await self._fetch_json(url, params=params)

    async def _fetch_json(self, url: str, params: Dict) -> dict:
        async with httpx.AsyncClient() as client:
            response = await async_retry_request(client, "GET", url, params=params)
            response.raise_for_status()
            return response.json()
        return {}

    def _load_cached(self, symbol: str) -> FundamentalData | None:
        path = self._get_cache_path(symbol)
        if not os.path.exists(path):
            return None
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
        )

    async def reload(self, symbol: str) -> FundamentalData:
        logger.info(f"Forcing reload of fundamentals for: {symbol}")

        fundamentals = await self._fetch_fundamentals(symbol)
        market_cap = await self._fetch_market_cap(symbol)

        fd = self._parse_data(fundamentals, market_cap)
        try:
            self._store(symbol, fd)
            logger.info(f"Reloaded and stored fundamental data for {symbol}")
        except Exception as e:
            logger.error(f"Failed to reload and store {symbol}: {e}")
            raise
        return fd

    def _parse_data(self, fundamentals: dict, market_cap: dict) -> FundamentalData:
        fundamentals = {
            k: v for k, v in fundamentals.items() if v is not None and v != "NA"
        }
        general = fundamentals.get("General") or {}
        shares_stats = fundamentals.get("SharesStats") or {}
        earnings = fundamentals.get("Earnings") or {}
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

        earnings_dates = pd.DatetimeIndex(
            pd.to_datetime(list(earnings.get("History", {}).keys())), name="report_date"
        ).sort_values()

        address_data = general.get("AddressData", {}) or {}
        insiders_ownership_perc = None
        institutional_ownership_perc = None
        shares_float = None
        beta = None
        if shares_stats.get("PercentInstitutions") is not None:
            insiders_ownership_perc = float(shares_stats["PercentInsiders"])
        if shares_stats.get("PercentInsiders") is not None:
            institutional_ownership_perc = float(shares_stats["PercentInstitutions"])
        if shares_stats.get("SharesFloat") is not None:
            shares_float = float(shares_stats["SharesFloat"])
        if technicals.get("Beta") is not None:
            beta = float(technicals["Beta"])

        return FundamentalData(
            type=general.get("Type", ""),
            exchange=general.get("Exchange", ""),
            country=general.get("CountryName", ""),
            city=address_data.get("City", ""),
            gic_industry=general.get("GicIndustry", ""),
            gic_sector=general.get("GicSector", ""),
            historical_market_cap=historical_market_cap,
            earnings_dates=earnings_dates,
            insiders_ownership_perc=insiders_ownership_perc,
            institutional_ownership_perc=institutional_ownership_perc,
            shares_float=shares_float,
            beta=beta,
        )

    def _get_cache_path(self, symbol: str) -> str:
        return os.path.join(self.CACHE_DIR, f"{symbol}.json")

    def _store(self, symbol: str, data: FundamentalData) -> None:
        os.makedirs(self.CACHE_DIR, exist_ok=True)
        path = self._get_cache_path(symbol)

        data_dict = asdict(data)
        data_dict["historical_market_cap"] = {
            str(idx): float(val) for idx, val in data.historical_market_cap.items()
        }
        data_dict["earnings_dates"] = [
            date.date().isoformat() for date in data.earnings_dates
        ]

        with open(path, "w") as f:
            json.dump(data_dict, f, indent=2)

    def _store_empty(self, symbol: str) -> None:
        os.makedirs(self.CACHE_DIR, exist_ok=True)
        path = self._get_cache_path(symbol)
        with open(path, "w") as f:
            json.dump({}, f, indent=2)
