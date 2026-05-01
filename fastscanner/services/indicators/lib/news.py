import asyncio
import dataclasses
import email.utils
import json
import logging
import random
import re
import shutil
from datetime import date, datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

import feedparser
import httpx
import pandas as pd
from bs4 import BeautifulSoup

from fastscanner.pkg import config
from fastscanner.pkg.candle import Candle
from fastscanner.pkg.http import async_retry_request
from fastscanner.services.registry import ApplicationRegistry

logger = logging.getLogger(__name__)

EST = ZoneInfo("America/New_York")
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
HTTP_TIMEOUT = 10.0
CODEX_TIMEOUT = 90.0
MAX_HEADLINES_PER_SYMBOL = 50
MIN_SUBSTRING_SYMBOL_LEN = 3
CONFIDENCE_THRESHOLD = 50  # strict greater-than per spec


@dataclasses.dataclass(frozen=True)
class Headline:
    title: str
    source: str


def _entry_str(entry: Any, key: str) -> str:
    value = entry.get(key, "")
    return value if isinstance(value, str) else ""


class InNewsIndicator:
    def __init__(self, caching: bool = False) -> None:
        self._caching = caching
        self._in_news_today: dict[str, bool] = {}  # sticky-True for the day
        self._date: dict[str, date] = {}            # per-symbol day boundary
        self._tasks: dict[str, asyncio.Task] = {}   # in-flight fire-and-forget (caching=True)

    @classmethod
    def type(cls) -> str:
        return "in_news"

    def column_name(self) -> str:
        return self.type()

    @staticmethod
    def _cache_key(symbol: str) -> str:
        return f"indicator:in_news:{symbol}"

    async def extend(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        df[self.column_name()] = pd.NA
        return df

    async def extend_realtime(self, symbol: str, new_row: Candle) -> Candle:
        today = new_row.timestamp.date()
        if self._date.get(symbol) != today:
            self._date[symbol] = today
            self._in_news_today.pop(symbol, None)

        if self._in_news_today.get(symbol) is True:
            new_row[self.column_name()] = True
            return new_row

        if self._caching:
            self._spawn_fetch(symbol)
            new_row[self.column_name()] = self._in_news_today.get(symbol, False)
            return new_row

        # consumer: read from shared cache
        try:
            value = await ApplicationRegistry.cache.get(self._cache_key(symbol))
            cached = value == "true"
        except KeyError:
            cached = False

        if cached:
            self._in_news_today[symbol] = True
        new_row[self.column_name()] = cached
        return new_row

    def _spawn_fetch(self, symbol: str) -> None:
        existing = self._tasks.get(symbol)
        if existing is not None and not existing.done():
            return
        self._tasks[symbol] = asyncio.create_task(self._inline_fetch(symbol))

    async def _inline_fetch(self, symbol: str) -> None:
        try:
            await asyncio.sleep(random.uniform(0, 45))
            in_news = await self._has_news_today(symbol)
            self._in_news_today[symbol] = in_news
            await ApplicationRegistry.cache.save(
                self._cache_key(symbol), "true" if in_news else "false"
            )
        except Exception:
            logger.exception("[in_news %s] inline fetch failed", symbol)

    async def _has_news_today(self, symbol: str) -> bool:
        today_str = datetime.now(EST).strftime("%Y-%m-%d")
        sources: tuple[tuple[str, Any], ...] = (
            ("finviz", self._finviz_headlines),
            ("finnhub", self._finnhub_headlines),
            ("yahoo", self._yahoo_headlines),
            ("seeking_alpha", self._seeking_alpha_headlines),
            ("marketwatch", self._marketwatch_headlines),
        )
        results = await asyncio.gather(
            *(fn(symbol, today_str) for _, fn in sources),
            return_exceptions=True,
        )

        headlines: list[Headline] = []
        for (name, _), result in zip(sources, results):
            if isinstance(result, BaseException):
                logger.warning(
                    "[in_news %s] source %s failed: %s", symbol, name, result
                )
                continue
            for title in result:
                headlines.append(Headline(title=title, source=name))

        if not headlines:
            logger.info("[in_news %s] no headlines retrieved for %s", symbol, today_str)
            return False

        if len(headlines) > MAX_HEADLINES_PER_SYMBOL:
            logger.info(
                "[in_news %s] truncating %d headlines to %d",
                symbol,
                len(headlines),
                MAX_HEADLINES_PER_SYMBOL,
            )
            headlines = headlines[:MAX_HEADLINES_PER_SYMBOL]

        logger.info(
            "[in_news %s] retrieved %d headlines for %s, filtering...",
            symbol,
            len(headlines),
            today_str,
        )

        # Pass 1: substring (only when symbol is long enough to be unambiguous)
        if len(symbol) >= MIN_SUBSTRING_SYMBOL_LEN:
            substring_hits = self._substring_match(symbol, headlines)
            if substring_hits:
                logger.info(
                    "[in_news %s] %d/%d substring hits -> in_news=True (skipped Codex)",
                    symbol,
                    len(substring_hits),
                    len(headlines),
                )
                return True

        # Pass 2: Codex. If Codex itself is unavailable we fail open: we already
        # have N>0 headlines for today, and without the classifier we can't
        # confidently drop them, so report in_news=True rather than miss real news.
        try:
            codex_kept = await self._filter_with_codex(symbol, headlines)
        except Exception as e:
            logger.warning(
                "[in_news %s] Codex unavailable (%s); %d headlines retrieved -> in_news=True (fail-open)",
                symbol,
                e,
                len(headlines),
            )
            return True

        logger.info(
            "[in_news %s] %d/%d headlines kept by Codex -> in_news=%s",
            symbol,
            len(codex_kept),
            len(headlines),
            bool(codex_kept),
        )
        return bool(codex_kept)

    def _substring_match(
        self, symbol: str, headlines: list[Headline]
    ) -> list[Headline]:
        needle = symbol.upper()
        kept: list[Headline] = []
        for h in headlines:
            if needle in h.title.upper():
                logger.info(
                    "[in_news %s][KEEP %s substring] %s",
                    symbol,
                    h.source,
                    h.title,
                )
                kept.append(h)
            else:
                logger.info(
                    "[in_news %s][DROP %s substring] %s",
                    symbol,
                    h.source,
                    h.title,
                )
        return kept

    async def _filter_with_codex(
        self, symbol: str, headlines: list[Headline]
    ) -> list[Headline]:
        if shutil.which("codex") is None:
            raise RuntimeError("codex CLI not found in PATH")

        items = [{"idx": i, "title": h.title} for i, h in enumerate(headlines)]
        prompt = (
            "You are a strict JSON-only classifier. Use only your training "
            "knowledge.\n\n"
            f"For ticker symbol {symbol.upper()}, output the confidence (0-100) "
            f"that the company name corresponding to {symbol.upper()} appears in "
            "each headline.\n\n"
            f"Output ONLY a JSON array of {len(items)} objects, each with keys "
            '"idx" (int) and "confidence" (int 0-100). No prose, no markdown '
            "fences.\n"
            'Format: [{"idx":0,"confidence":87}, ...]\n\n'
            f"Headlines:\n{json.dumps(items, ensure_ascii=False)}\n"
        )

        # codex exec is implicitly non-interactive (no --ask-for-approval needed
        # — that flag only exists on the top-level codex command).
        proc = await asyncio.create_subprocess_exec(
            "codex",
            "exec",
            "--sandbox",
            "read-only",
            "-c",
            'web_search="disabled"',
            "-c",
            "features.shell_tool=false",
            prompt,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout_b, stderr_b = await asyncio.wait_for(
                proc.communicate(), timeout=CODEX_TIMEOUT
            )
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            raise RuntimeError(f"codex timed out after {CODEX_TIMEOUT}s")

        if proc.returncode != 0:
            err = stderr_b.decode(errors="replace")[:200]
            raise RuntimeError(f"codex exited {proc.returncode}: {err}")

        text = stdout_b.decode(errors="replace")
        scores = self._parse_codex_response(text, n_headlines=len(headlines))

        kept: list[Headline] = []
        for entry in scores:
            idx = entry["idx"]
            conf = entry["confidence"]
            h = headlines[idx]
            if conf > CONFIDENCE_THRESHOLD:
                logger.info(
                    "[in_news %s][KEEP %s codex=%d%%] %s",
                    symbol,
                    h.source,
                    conf,
                    h.title,
                )
                kept.append(h)
            else:
                logger.info(
                    "[in_news %s][DROP %s codex=%d%%] %s",
                    symbol,
                    h.source,
                    conf,
                    h.title,
                )
        return kept

    @staticmethod
    def _parse_codex_response(text: str, n_headlines: int) -> list[dict]:
        match = re.search(r"\[\s*(?:\{.*?\}\s*,?\s*)*\]", text, re.DOTALL)
        if not match:
            raise RuntimeError(f"no JSON array in codex output: {text[:200]!r}")
        raw = json.loads(match.group(0))
        if not isinstance(raw, list):
            raise RuntimeError(f"codex output is not a JSON array: {raw!r}")

        valid: list[dict] = []
        for entry in raw:
            if not isinstance(entry, dict):
                continue
            idx = entry.get("idx")
            conf = entry.get("confidence")
            if not isinstance(idx, int) or not isinstance(conf, (int, float)):
                continue
            if not (0 <= idx < n_headlines):
                continue
            if not (0 <= conf <= 100):
                continue
            valid.append({"idx": idx, "confidence": int(conf)})

        if not valid:
            raise RuntimeError(f"codex output had no valid entries: {raw!r}")
        return valid

    # --- per-source helpers: each returns list[str] of titles dated today_str ---

    async def _finviz_headlines(self, symbol: str, today_str: str) -> list[str]:
        url = f"https://finviz.com/quote.ashx?t={symbol.upper()}&p=d"
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            response = await async_retry_request(
                client, "GET", url, headers={"User-Agent": USER_AGENT}
            )
        if response.status_code != 200:
            return []
        return self._finviz_news_table_extract(response.text, today_str)

    @staticmethod
    def _finviz_news_table_extract(html: str, date_str: str) -> list[str]:
        target = datetime.strptime(date_str, "%Y-%m-%d").strftime("%b-%d-%y")
        soup = BeautifulSoup(html, "html.parser")
        news_table = soup.find("table", id="news-table")
        if not news_table:
            return []
        titles: list[str] = []
        current_date: str | None = None
        for row in news_table.find_all("tr"):
            date_td = row.find("td", attrs={"width": "130"})
            title_a = row.find("a", class_="tab-link-news")
            if date_td:
                cell_text = date_td.get_text(strip=True)
                if cell_text and cell_text[0].isalpha():
                    current_date = cell_text[:9].strip()
            if title_a and current_date == target:
                title = title_a.get_text(strip=True)
                if title:
                    titles.append(title)
        return titles

    async def _finnhub_headlines(self, symbol: str, today_str: str) -> list[str]:
        token = config.FINNHUB_TOKEN
        if not token:
            return []
        url = (
            "https://finnhub.io/api/v1/company-news"
            f"?symbol={symbol.upper()}&from={today_str}&to={today_str}&token={token}"
        )
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            response = await async_retry_request(client, "GET", url)
        if response.status_code != 200:
            return []
        items = response.json()
        if not isinstance(items, list):
            return []
        titles: list[str] = []
        for item in items:
            ts = item.get("datetime")
            title = item.get("headline", "")
            if not ts or not title:
                continue
            if self._dt_is_on_date(
                datetime.fromtimestamp(ts, tz=timezone.utc), today_str
            ):
                titles.append(title)
        return titles

    async def _yahoo_headlines(self, symbol: str, today_str: str) -> list[str]:
        url = (
            "https://feeds.finance.yahoo.com/rss/2.0/headline"
            f"?s={symbol.upper()}&region=US&lang=en-US"
        )
        return await self._rss_titles_for_today(url, today_str, match_symbol=None)

    async def _seeking_alpha_headlines(self, symbol: str, today_str: str) -> list[str]:
        url = f"https://seekingalpha.com/api/sa/combined/{symbol.lower()}.xml"
        return await self._rss_titles_for_today(url, today_str, match_symbol=None)

    async def _marketwatch_headlines(self, symbol: str, today_str: str) -> list[str]:
        url = "https://feeds.content.dowjones.io/public/rss/mw_realtimeheadlines"
        return await self._rss_titles_for_today(url, today_str, match_symbol=symbol)

    async def _rss_titles_for_today(
        self, url: str, today_str: str, match_symbol: str | None
    ) -> list[str]:
        feed = await asyncio.to_thread(feedparser.parse, url)
        titles: list[str] = []
        for entry in feed.entries:
            entry_title = _entry_str(entry, "title")
            entry_summary = _entry_str(entry, "summary")
            if match_symbol is not None:
                blob = (entry_title + " " + entry_summary).upper()
                if match_symbol.upper() not in blob:
                    continue
            dt = self._parse_rss_date(entry)
            if dt is not None and self._dt_is_on_date(dt, today_str):
                title = entry_title.strip()
                if title:
                    titles.append(title)
        return titles

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
