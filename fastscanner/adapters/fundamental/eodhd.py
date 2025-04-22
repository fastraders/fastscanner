import os
import json
import logging
from typing import Dict
from dataclasses import asdict

import requests
import pandas as pd

from fastscanner.adapters.fundamental import config
from fastscanner.services.indicators.ports import FundamentalData

logger = logging.getLogger(__name__)


class EODHDFundamentalStore:
    CACHE_DIR = os.path.join("data", "fundamentals")

    def __init__(self):
        self._base_url = config.EOD_HD_BASE_URL
        self._api_key = config.EOD_HD_API_KEY

    def get(self, symbol: str) -> FundamentalData:
        logger.info(f"Retrieving fundamentals for: {symbol}")

        cached = self._load_cached(symbol)
        if cached:
            logger.info(f"Loaded {symbol} fundamentals from cache.")
            return cached

        fundamentals = self._fetch_fundamentals(symbol)
        market_cap = self._fetch_market_cap(symbol)

        if not fundamentals or not market_cap:
            logger.warning(f"Missing data for symbol: {symbol}")
            raise ValueError(f"Incomplete data for {symbol}")

        fd= self._parse_data(fundamentals, market_cap)
        try:
            self._store(symbol, fd)
            logger.info(f"Stored fundamental data for {symbol}")
        except Exception as e:
            logger.error(f"Failed to process {symbol}: {e}")
            raise
        return fd


    def _fetch_fundamentals(self, symbol: str) -> Dict:
        filters = "General::Code,General,Earnings,SharesStats"
        url = f"{self._base_url}/fundamentals/{symbol}?filter={filters}&api_token={self._api_key}&fmt=json"
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.exception(f"Error fetching fundamentals for {symbol}")
            return {}

    def _fetch_market_cap(self, symbol: str) -> Dict:
        url = f"{self._base_url}/historical-market-cap/{symbol}?api_token={self._api_key}&fmt=json"
        try:
            response = requests.get(url)
            response.raise_for_status()
            return json.loads(response.text)
        except Exception as e:
            logger.exception(f"Error fetching market cap for {symbol}")
            return {}
        
    def _load_cached(self, symbol: str) -> FundamentalData | None:
        path = os.path.join(self.CACHE_DIR, f"{symbol}.json")
        if os.path.exists(path):
            try:
                with open(path, "r") as f:
                    data = json.load(f)
                    return FundamentalData(**data)
            except (json.JSONDecodeError, TypeError) as e:
                logger.warning(f"Cache for {symbol} is corrupted: {e}")
                return None
        return None


    def _parse_data(self, fundamentals: dict, market_cap: dict) -> FundamentalData:
        general = fundamentals.get("General", {})
        shares_stats = fundamentals.get("SharesStats", {})
        earnings = fundamentals.get("Earnings", {})

        historical_market_cap = {
            v["date"]: float(v["value"])
            for v in market_cap.values()
            if isinstance(v, dict) and "date" in v and "value" in v
        }

        earnings_dates = sorted(earnings.get("History", {}).keys())

        return FundamentalData(
            exchange=general.get("Exchange", ""),
            country=general.get("CountryName", ""),
            city=general.get("AddressData", {}).get("City", ""),
            gic_industry=general.get("GicIndustry", ""),
            gic_sector=general.get("GicSector", ""),
            historical_market_cap=historical_market_cap,
            earnings_dates=earnings_dates,
            insiders_ownership_perc=float(shares_stats.get("PercentInsiders", 0.0)),
            institutional_ownership_perc=float(shares_stats.get("PercentInstitutions", 0.0)),
            shares_float=float(shares_stats.get("SharesFloat", 0.0)),
        )

    def _store(self, symbol: str, data: FundamentalData) -> None:
        os.makedirs(self.CACHE_DIR, exist_ok=True)
        path = os.path.join(self.CACHE_DIR, f"{symbol}.json")
        with open(path, "w") as f:
            json.dump(asdict(data), f, indent=2)


