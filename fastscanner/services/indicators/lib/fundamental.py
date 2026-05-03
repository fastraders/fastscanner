import asyncio
import json
import logging
import time as time_mod
from datetime import date, time
from typing import Any

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup

from fastscanner.pkg import config
from fastscanner.pkg.candle import Candle
from fastscanner.pkg.clock import ClockRegistry

from ...registry import ApplicationRegistry

logger = logging.getLogger(__name__)


class DaysToEarningsIndicator:
    def __init__(self) -> None:
        self._last_date: dict[str, date] = {}
        self._last_days: dict[str, int] = {}

    @classmethod
    def type(cls):
        return "days_to_earnings"

    def column_name(self):
        return self.type()

    async def save_to_cache(self):
        await ApplicationRegistry.cache.save(
            f"indicator:{self.column_name()}",
            json.dumps(
                {
                    "last_date": {k: v.isoformat() for k, v in self._last_date.items()},
                    "last_days": self._last_days,
                }
            ),
        )

    async def load_from_cache(self, symbol: str | None = None):
        indicator_data = await ApplicationRegistry.cache.get(
            f"indicator:{self.column_name()}"
        )
        indicator_data = json.loads(indicator_data)
        self._last_date = {
            k: date.fromisoformat(v)
            for k, v in indicator_data["last_date"].items()
            if symbol is None or k == symbol
        }
        self._last_days = {
            k: v
            for k, v in indicator_data["last_days"].items()
            if symbol is None or k == symbol
        }

    async def extend(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        fundamentals = await ApplicationRegistry.fundamentals.get(symbol)
        assert isinstance(df.index, pd.DatetimeIndex)

        earnings_dates = fundamentals.earnings_dates.date
        unique_dates = np.unique(df.index.date)

        date_to_earnings = pd.Series(
            np.nan, index=unique_dates, dtype=float, name=self.column_name()
        )
        for date in unique_dates:
            earnings_dates = earnings_dates[earnings_dates >= date]

            if len(earnings_dates) > 0:
                next_earnings = earnings_dates[0]
                days = (next_earnings - date).days
            else:
                break

            date_to_earnings[date] = days

        df.loc[:, "date"] = df.index.date
        df = df.join(date_to_earnings, on="date")
        return df.drop(columns=["date"])

    async def extend_realtime(self, symbol: str, new_row: Candle) -> Candle:
        new_date = new_row.timestamp.date()
        if (last_date := self._last_date.get(symbol)) is None or last_date != new_date:
            result = (await self.extend(symbol, new_row.to_dataframe())).iloc[0]
            self._last_date[symbol] = new_date
            self._last_days.pop(symbol, None)
            if pd.notna(value := result[self.column_name()]):
                self._last_days[symbol] = int(value)

        new_row[self.column_name()] = self._last_days.get(symbol)
        return new_row


class DaysFromEarningsIndicator:
    def __init__(self) -> None:
        self._last_date: dict[str, date] = {}
        self._last_days: dict[str, int] = {}

    @classmethod
    def type(cls):
        return "days_from_earnings"

    def column_name(self):
        return self.type()

    async def save_to_cache(self):
        await ApplicationRegistry.cache.save(
            f"indicator:{self.column_name()}",
            json.dumps(
                {
                    "last_date": {k: v.isoformat() for k, v in self._last_date.items()},
                    "last_days": self._last_days,
                }
            ),
        )

    async def load_from_cache(self, symbol: str | None = None):
        indicator_data = await ApplicationRegistry.cache.get(
            f"indicator:{self.column_name()}"
        )
        indicator_data = json.loads(indicator_data)
        self._last_date = {
            k: date.fromisoformat(v)
            for k, v in indicator_data["last_date"].items()
            if symbol is None or k == symbol
        }
        self._last_days = {
            k: v
            for k, v in indicator_data["last_days"].items()
            if symbol is None or k == symbol
        }

    async def extend(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        fundamentals = await ApplicationRegistry.fundamentals.get(symbol)
        assert isinstance(df.index, pd.DatetimeIndex)

        earnings_dates = fundamentals.earnings_dates.date
        unique_dates = np.unique(df.index.date)

        date_from_earnings = pd.Series(
            np.nan, index=unique_dates, dtype=float, name=self.column_name()
        )
        for date in unique_dates[::-1]:
            earning_dates = earnings_dates[earnings_dates <= date]

            if len(earning_dates) > 0:
                last_earnings = earning_dates[-1]
                days = (date - last_earnings).days
            else:
                break

            date_from_earnings[date] = days
        df.loc[:, "date"] = df.index.date
        df = df.join(date_from_earnings, on="date")
        return df.drop(columns=["date"])

    async def extend_realtime(self, symbol: str, new_row: Candle) -> Candle:
        new_date = new_row.timestamp.date()
        if (last_date := self._last_date.get(symbol)) is None or last_date != new_date:
            result = (await self.extend(symbol, new_row.to_dataframe())).iloc[0]
            self._last_date[symbol] = new_date
            self._last_days.pop(symbol, None)
            if pd.notna(value := result[self.column_name()]):
                self._last_days[symbol] = int(value)

        new_row[self.column_name()] = self._last_days.get(symbol)
        return new_row


def _parse_float_value(val: str) -> float | None:
    if not val:
        return None
    cleaned = val.strip().replace(",", "").upper()
    if not cleaned or cleaned == "N/A":
        return None
    multipliers = {"K": 1e3, "M": 1e6, "B": 1e9, "T": 1e12}
    suffix = cleaned[-1]
    try:
        if suffix in multipliers:
            return float(cleaned[:-1]) * multipliers[suffix]
        return float(cleaned)
    except (ValueError, IndexError):
        return None


class _DilutionTrackerDriver:
    _driver: Any = None
    _logged_in: bool = False
    _lock: asyncio.Lock | None = None

    @classmethod
    def _get_lock(cls) -> asyncio.Lock:
        if cls._lock is None:
            cls._lock = asyncio.Lock()
        return cls._lock

    @classmethod
    async def fetch_float(cls, symbol: str) -> float | None:
        async with cls._get_lock():
            if cls._driver is None:
                await asyncio.to_thread(cls._init_and_login)
            if not cls._logged_in:
                return None
            return await asyncio.to_thread(cls._scrape_float_sync, symbol)

    @classmethod
    def _init_and_login(cls) -> None:
        email = config.DILUTION_EMAIL
        password = config.DILUTION_PASSWORD
        if not email or not password:
            logger.warning(
                "DILUTION_EMAIL or DILUTION_PASSWORD not set; "
                "shares_float will be unavailable"
            )
            return

        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.common.by import By
        from selenium.webdriver.common.keys import Keys
        from webdriver_manager.chrome import ChromeDriverManager

        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1400,900")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)
        options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

        try:
            driver = webdriver.Chrome(
                service=Service(ChromeDriverManager().install()), options=options
            )
            driver.execute_script(
                "Object.defineProperty(navigator, 'webdriver', "
                "{get: () => undefined})"
            )
            cls._driver = driver

            driver.get("https://dilutiontracker.com/login")
            time_mod.sleep(3)

            email_field = driver.find_element(By.ID, "email")
            email_field.click()
            for char in email:
                email_field.send_keys(char)
                time_mod.sleep(0.05)

            pw_field = driver.find_element(By.ID, "password")
            pw_field.click()
            for char in password:
                pw_field.send_keys(char)
                time_mod.sleep(0.05)

            time_mod.sleep(1)
            pw_field.send_keys(Keys.RETURN)
            time_mod.sleep(8)
            cls._logged_in = True
            logger.info("Dilutiontracker logged in")
        except Exception:
            logger.exception("Dilutiontracker login failed")

    @classmethod
    def _scrape_float_sync(cls, symbol: str) -> float | None:
        from selenium.webdriver.support.ui import WebDriverWait

        try:
            cls._driver.get(
                f"https://dilutiontracker.com/app/search/{symbol}?a=qs5555"
            )
            try:
                WebDriverWait(cls._driver, 8).until(
                    lambda d: any(
                        span.get_text(strip=True) == "Float"
                        for span in BeautifulSoup(
                            d.page_source, "html.parser"
                        ).find_all("span")
                    )
                )
                time_mod.sleep(2)
            except Exception:
                logger.warning(
                    "[shares_float %s] Float field did not render in time", symbol
                )
                return None

            soup = BeautifulSoup(cls._driver.page_source, "html.parser")
            for span in soup.find_all("span"):
                if span.get_text(strip=True) != "Float":
                    continue
                parent = span.find_parent("span")
                if parent is None:
                    continue
                next_sib = parent.find_next_sibling("span")
                if next_sib is None:
                    continue
                raw = next_sib.get_text(strip=True).split("/")[0].strip()
                return _parse_float_value(raw)
            return None
        except Exception:
            logger.exception("[shares_float %s] scrape failed", symbol)
            return None


class SharesFloatIndicator:
    _NULL_CACHE_VALUE = "null"

    def __init__(self, caching: bool = False) -> None:
        self._caching = caching
        self._float: dict[str, float | None] = {}
        self._last_date: dict[str, date] = {}
        self._tasks: dict[str, asyncio.Task] = {}

    @classmethod
    def type(cls) -> str:
        return "shares_float"

    def column_name(self) -> str:
        return self.type()

    @staticmethod
    def _cache_key(symbol: str) -> str:
        return f"indicator:shares_float:{symbol}"

    async def extend(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        df[self.column_name()] = pd.NA
        return df

    async def extend_realtime(self, symbol: str, new_row: Candle) -> Candle:
        today = new_row.timestamp.date()
        if self._last_date.get(symbol) != today:
            self._last_date[symbol] = today
            self._float.pop(symbol, None)

        if symbol in self._float:
            new_row[self.column_name()] = self._float[symbol]
            return new_row

        if self._caching:
            self._spawn_fetch(symbol)
            new_row[self.column_name()] = self._float.get(symbol)
            return new_row

        try:
            cached = await ApplicationRegistry.cache.get(self._cache_key(symbol))
        except KeyError:
            cached = ""

        if cached == self._NULL_CACHE_VALUE:
            self._float[symbol] = None
            new_row[self.column_name()] = None
            return new_row
        if cached:
            try:
                value = float(cached)
                self._float[symbol] = value
                new_row[self.column_name()] = value
                return new_row
            except ValueError:
                pass

        new_row[self.column_name()] = None
        return new_row

    def _spawn_fetch(self, symbol: str) -> None:
        existing = self._tasks.get(symbol)
        if existing is not None and not existing.done():
            return
        self._tasks[symbol] = asyncio.create_task(self._inline_fetch(symbol))

    async def _inline_fetch(self, symbol: str) -> None:
        try:
            value = await _DilutionTrackerDriver.fetch_float(symbol)
            self._float[symbol] = value
            await ApplicationRegistry.cache.save(
                self._cache_key(symbol),
                self._NULL_CACHE_VALUE if value is None else str(value),
            )
            logger.info("[shares_float %s] fetched float=%s", symbol, value)
        except Exception:
            logger.exception("[shares_float %s] inline fetch failed", symbol)


class MarketCapIndicator:
    def __init__(self) -> None:
        self._last_date: dict[str, date] = {}
        self._last_market_cap: dict[str, float] = {}

    @classmethod
    def type(cls):
        return "market_cap"

    def column_name(self):
        return self.type()

    async def save_to_cache(self):
        await ApplicationRegistry.cache.save(
            f"indicator:{self.column_name()}",
            json.dumps(
                {
                    "last_date": {k: v.isoformat() for k, v in self._last_date.items()},
                    "last_market_cap": self._last_market_cap,
                }
            ),
        )

    async def load_from_cache(self, symbol: str | None = None):
        indicator_data = await ApplicationRegistry.cache.get(
            f"indicator:{self.column_name()}"
        )
        indicator_data = json.loads(indicator_data)
        self._last_date = {
            k: date.fromisoformat(v)
            for k, v in indicator_data["last_date"].items()
            if symbol is None or k == symbol
        }
        self._last_market_cap = {
            k: v
            for k, v in indicator_data["last_market_cap"].items()
            if symbol is None or k == symbol
        }

    async def extend(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        fundamentals = await ApplicationRegistry.fundamentals.get(symbol)
        assert isinstance(df.index, pd.DatetimeIndex)

        historical_market_cap = fundamentals.historical_market_cap

        unique_dates = np.unique(df.index.date)

        date_to_market_cap = pd.Series(
            np.nan, index=unique_dates, dtype=float, name=self.column_name()
        )

        for date in unique_dates:
            filtered_series = historical_market_cap[historical_market_cap.index <= date]

            if not filtered_series.empty:
                market_cap = filtered_series.iloc[-1]
                date_to_market_cap[date] = market_cap

        df.loc[:, "date"] = df.index.date
        df = df.join(date_to_market_cap, on="date")
        return df.drop(columns=["date"])

    async def extend_realtime(self, symbol: str, new_row: Candle) -> Candle:
        new_date = new_row.timestamp.date()

        if (last_date := self._last_date.get(symbol)) is None or last_date != new_date:
            result = (await self.extend(symbol, new_row.to_dataframe())).iloc[0]
            self._last_date[symbol] = new_date
            self._last_market_cap.pop(symbol, None)
            if pd.notna(value := result[self.column_name()]):
                self._last_market_cap[symbol] = float(value)

        new_row[self.column_name()] = self._last_market_cap.get(symbol)
        return new_row


class DaysSinceIPOIndicator:
    def __init__(self) -> None:
        self._last_date: dict[str, date] = {}
        self._last_days: dict[str, int] = {}

    @classmethod
    def type(cls):
        return "days_since_ipo"

    def column_name(self):
        return self.type()

    async def save_to_cache(self):
        await ApplicationRegistry.cache.save(
            f"indicator:{self.column_name()}",
            json.dumps(
                {
                    "last_date": {k: v.isoformat() for k, v in self._last_date.items()},
                    "last_days": self._last_days,
                }
            ),
        )

    async def load_from_cache(self, symbol: str | None = None):
        indicator_data = await ApplicationRegistry.cache.get(
            f"indicator:{self.column_name()}"
        )
        indicator_data = json.loads(indicator_data)
        self._last_date = {
            k: date.fromisoformat(v)
            for k, v in indicator_data["last_date"].items()
            if symbol is None or k == symbol
        }
        self._last_days = {
            k: v
            for k, v in indicator_data["last_days"].items()
            if symbol is None or k == symbol
        }

    async def extend(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        fundamentals = await ApplicationRegistry.fundamentals.get(symbol)
        assert isinstance(df.index, pd.DatetimeIndex)

        ipo_date_str = fundamentals.ipo_date
        col = self.column_name()

        if not ipo_date_str:
            df[col] = np.nan
            return df

        ipo_date = ClockRegistry.clock.date_at(
            date.fromisoformat(ipo_date_str), time(0, 0)
        )
        df[col] = (df.index - ipo_date).days
        df.loc[df[col] < 0, col] = np.nan

        return df

    async def extend_realtime(self, symbol: str, new_row: Candle) -> Candle:
        new_date = new_row.timestamp.date()
        if (last_date := self._last_date.get(symbol)) is None or last_date != new_date:
            result = (await self.extend(symbol, new_row.to_dataframe())).iloc[0]
            self._last_date[symbol] = new_date
            self._last_days.pop(symbol, None)
            if pd.notna(value := result[self.column_name()]):
                self._last_days[symbol] = int(value)

        new_row[self.column_name()] = self._last_days.get(symbol)
        return new_row
