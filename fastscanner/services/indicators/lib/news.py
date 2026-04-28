import asyncio
import email.utils
import logging
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

import feedparser
import httpx
import pandas as pd
from bs4 import BeautifulSoup

from fastscanner.pkg import config
from fastscanner.pkg.candle import Candle
from fastscanner.pkg.http import async_retry_request

logger = logging.getLogger(__name__)

EST = ZoneInfo("America/New_York")
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
HTTP_TIMEOUT = 10.0


class InNewsIndicator:
    def __init__(self) -> None:
        self._cached: dict[str, bool] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    @classmethod
    def type(cls) -> str:
        return "in_news"

    def column_name(self) -> str:
        return self.type()

    async def extend(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        df[self.column_name()] = pd.NA
        return df

    async def extend_realtime(self, symbol: str, new_row: Candle) -> Candle:
        if symbol not in self._cached:
            lock = self._locks.setdefault(symbol, asyncio.Lock())
            async with lock:
                if symbol not in self._cached:
                    self._cached[symbol] = await self._has_news_today(symbol)
        new_row[self.column_name()] = self._cached[symbol]
        return new_row

    async def _has_news_today(self, symbol: str) -> bool:
        today_str = datetime.now(EST).strftime("%Y-%m-%d")
        checks = (
            self._finviz_has_news,
            self._finnhub_has_news,
            self._yahoo_has_news,
            self._seeking_alpha_has_news,
            self._marketwatch_has_news,
        )
        for check in checks:
            try:
                if await check(symbol, today_str):
                    return True
            except Exception as e:
                logger.warning(
                    "in_news source %s failed for %s: %s", check.__name__, symbol, e
                )
        return False

    async def _finviz_has_news(self, symbol: str, today_str: str) -> bool:
        url = f"https://finviz.com/quote.ashx?t={symbol.upper()}&p=d"
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            response = await async_retry_request(
                client, "GET", url, headers={"User-Agent": USER_AGENT}
            )
        if response.status_code != 200:
            return False
        return self._finviz_news_table_has_date(response.text, today_str)

    @staticmethod
    def _finviz_news_table_has_date(html: str, date_str: str) -> bool:
        target = datetime.strptime(date_str, "%Y-%m-%d").strftime("%b-%d-%y")
        soup = BeautifulSoup(html, "html.parser")
        news_table = soup.find("table", id="news-table")
        if not news_table:
            return False
        current_date: str | None = None
        for row in news_table.find_all("tr"):
            date_td = row.find("td", attrs={"width": "130"})
            title_a = row.find("a", class_="tab-link-news")
            if date_td:
                cell_text = date_td.get_text(strip=True)
                if cell_text and cell_text[0].isalpha():
                    current_date = cell_text[:9].strip()
            if title_a and current_date == target:
                return True
        return False

    async def _finnhub_has_news(self, symbol: str, today_str: str) -> bool:
        token = config.FINNHUB_TOKEN
        if not token:
            return False
        url = (
            "https://finnhub.io/api/v1/company-news"
            f"?symbol={symbol.upper()}&from={today_str}&to={today_str}&token={token}"
        )
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            response = await async_retry_request(client, "GET", url)
        if response.status_code != 200:
            return False
        items = response.json()
        if not isinstance(items, list):
            return False
        for item in items:
            ts = item.get("datetime")
            if ts and self._dt_is_on_date(
                datetime.fromtimestamp(ts, tz=timezone.utc), today_str
            ):
                return True
        return False

    async def _yahoo_has_news(self, symbol: str, today_str: str) -> bool:
        url = (
            "https://feeds.finance.yahoo.com/rss/2.0/headline"
            f"?s={symbol.upper()}&region=US&lang=en-US"
        )
        return await self._rss_feed_has_today(url, today_str, match_symbol=None)

    async def _seeking_alpha_has_news(self, symbol: str, today_str: str) -> bool:
        url = f"https://seekingalpha.com/api/sa/combined/{symbol.lower()}.xml"
        return await self._rss_feed_has_today(url, today_str, match_symbol=None)

    async def _marketwatch_has_news(self, symbol: str, today_str: str) -> bool:
        url = "https://feeds.content.dowjones.io/public/rss/mw_realtimeheadlines"
        return await self._rss_feed_has_today(url, today_str, match_symbol=symbol)

    async def _rss_feed_has_today(
        self, url: str, today_str: str, match_symbol: str | None
    ) -> bool:
        feed = await asyncio.to_thread(feedparser.parse, url)
        for entry in feed.entries:
            if match_symbol is not None:
                blob = (entry.get("title", "") + " " + entry.get("summary", "")).upper()
                if match_symbol.upper() not in blob:
                    continue
            dt = self._parse_rss_date(entry)
            if dt is not None and self._dt_is_on_date(dt, today_str):
                return True
        return False

    @staticmethod
    def _parse_rss_date(entry: Any) -> datetime | None:
        published = entry.get("published", "")
        if published:
            try:
                return email.utils.parsedate_to_datetime(published)
            except (TypeError, ValueError):
                pass
        parsed = entry.get("published_parsed")
        if parsed:
            try:
                return datetime(*parsed[:6], tzinfo=timezone.utc)
            except (TypeError, ValueError):
                pass
        return None

    @staticmethod
    def _dt_is_on_date(dt: datetime, date_str: str) -> bool:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(EST).strftime("%Y-%m-%d") == date_str
