import json
import logging
import os
from dataclasses import asdict
from typing import Dict

import httpx
import pandas as pd

from fastscanner.pkg import config
from fastscanner.pkg.http import MaxRetryError, retry_request
from fastscanner.services.indicators.ports import FundamentalData

logger = logging.getLogger(__name__)


class EODHDFundamentalStore:
    CACHE_DIR = os.path.join("data", "fundamentals")

    def __init__(self, base_url: str, api_key: str):
        self._base_url = base_url
        self._api_key = api_key

    def get(self, symbol: str) -> FundamentalData:
        logger.info(f"Retrieving fundamentals for: {symbol}")

        cached = self._load_cached(symbol)
        if cached:
            logger.info(f"Loaded {symbol} fundamentals from cache.")
            return cached

        fundamentals = self._fetch_fundamentals(symbol)
        market_cap = self._fetch_market_cap(symbol)

        fd = self._parse_data(fundamentals, market_cap)
        try:
            self._store(symbol, fd)
            logger.info(f"Stored fundamental data for {symbol}")
        except Exception as e:
            logger.error(f"Failed to process {symbol}: {e}")
            raise
        return fd

    def _fetch_fundamentals(self, symbol: str) -> Dict:
        url = f"{self._base_url}/fundamentals/{symbol}"
        params = {
            "filter": "General::Code,General,Earnings,SharesStats",
            "api_token": self._api_key,
            "fmt": "json"
        }
        try:
            fundamentals = self._fetch_json(url, params=params)
            return fundamentals
        except Exception as e:
            logger.exception(f"Error fetching fundamentals for {symbol}")
            return {}

    def _fetch_market_cap(self, symbol: str) -> Dict:
        url = f"{self._base_url}/historical-market-cap/{symbol}"
        params = {
            "api_token": self._api_key,
            "fmt": "json"
        }
        try:
            market_cap = self._fetch_json(url, params=params)
            return market_cap
        except Exception as e:
            logger.exception(f"Error fetching market cap for {symbol}")
            return {}


    def _fetch_json(self, url: str, params: Dict) -> dict:
        try:
            with httpx.Client() as client:
                response = retry_request(
                    client,
                    "GET",
                    url,
                    params=params
                )
                response.raise_for_status()
                return response.json()
        except (MaxRetryError, httpx.HTTPStatusError) as exc:
            logger.exception(f"HTTP error for {url}: {exc}")
        except Exception as exc:
            logger.exception(f"Unexpected error for {url}: {exc}")
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
            exchange=data.get("exchange", ""),
            country=data.get("country", ""),
            city=data.get("city", ""),
            gic_industry=data.get("gic_industry", ""),
            gic_sector=data.get("gic_sector", ""),
            historical_market_cap=market_cap,
            earnings_dates=earnings_dates,
            insiders_ownership_perc=float(data.get("insiders_ownership_perc", 0.0)),
            institutional_ownership_perc=float(
                data.get("institutional_ownership_perc", 0.0)
            ),
            shares_float=float(data.get("shares_float", 0.0)),
        )

    def reload(self, symbol: str) -> FundamentalData:
        logger.info(f"Forcing reload of fundamentals for: {symbol}")

        fundamentals = self._fetch_fundamentals(symbol)
        market_cap = self._fetch_market_cap(symbol)

        fd = self._parse_data(fundamentals, market_cap)
        try:
            self._store(symbol, fd)
            logger.info(f"Reloaded and stored fundamental data for {symbol}")
        except Exception as e:
            logger.error(f"Failed to reload and store {symbol}: {e}")
            raise
        return fd

    def _parse_data(self, fundamentals: dict, market_cap: dict) -> FundamentalData:
        general = fundamentals.get("General", {})
        shares_stats = fundamentals.get("SharesStats", {})
        earnings = fundamentals.get("Earnings", {})

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

        return FundamentalData(
            exchange=general.get("Exchange", ""),
            country=general.get("CountryName", ""),
            city=general.get("AddressData", {}).get("City", ""),
            gic_industry=general.get("GicIndustry", ""),
            gic_sector=general.get("GicSector", ""),
            historical_market_cap=historical_market_cap,
            earnings_dates=earnings_dates,
            insiders_ownership_perc=float(shares_stats.get("PercentInsiders", 0.0)),
            institutional_ownership_perc=float(
                shares_stats.get("PercentInstitutions", 0.0)
            ),
            shares_float=float(shares_stats.get("SharesFloat", 0.0)),
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


if __name__ == "__main__":
    fetcher = EODHDFundamentalStore(config.EOD_HD_BASE_URL, config.EOD_HD_API_KEY)
    symbol = "AAPL"

    logger.info(f"Fetching fundamental data for {symbol}")

    try:
        data = fetcher.get(symbol)
        logger.info("Successfully retrieved fundamental data.")
        logger.info(data)
    except Exception as e:
        logger.error(f"Failed to fetch fundamental data: {e}")
        raise