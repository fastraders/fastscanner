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
        self._date: dict[str, date] = {}  # per-symbol day boundary
        self._tasks: dict[str, asyncio.Task] = (
            {}
        )  # in-flight fire-and-forget (caching=True)
        self._seen_headlines: dict[str, set[str]] = (
            {}
        )  # symbol → titles already sent to Codex

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
            self._seen_headlines.pop(symbol, None)

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

        # Codex — only unseen headlines.
        seen = self._seen_headlines.setdefault(symbol, set())
        new_headlines = [h for h in headlines if h.title not in seen]
        if not new_headlines:
            return False

        try:
            codex_kept = await self._filter_with_codex(symbol, new_headlines)
        except Exception as e:
            logger.warning(
                "[in_news %s] Codex unavailable (%s); %d headlines retrieved -> in_news=False (fail-open)",
                symbol,
                e,
                len(new_headlines),
            )
            return False

        seen.update(h.title for h in new_headlines)

        logger.info(
            "[in_news %s] %d/%d new headlines kept by Codex -> in_news=%s",
            symbol,
            len(codex_kept),
            len(new_headlines),
            bool(codex_kept),
        )
        return bool(codex_kept)

    async def _filter_with_codex(
        self, symbol: str, headlines: list[Headline]
    ) -> list[Headline]:
        if shutil.which("codex") is None:
            raise RuntimeError("codex CLI not found in PATH")

        items = [{"idx": i, "title": h.title} for i, h in enumerate(headlines)]
        prompt = PROMPT.format(
            symbol=symbol.upper(), headlines=json.dumps(items, ensure_ascii=False)
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


PROMPT = (
    "You are a strict JSON-only classifier. Use only your training "
    "knowledge.\n\n"
    "For ticker symbol {symbol}, output the confidence (0-100) "
    "that each headline represents POSITIVE, ACTIONABLE news for the "
    "company corresponding to {symbol}.\n\n"
    "Confidence >= 50 means the headline should trigger action.\n"
    "Confidence < 50 means the headline should be skipped.\n\n"
    "SCORING RUBRIC\n"
    "==============\n"
    "Score HIGH confidence (75-100) when the headline clearly indicates:\n"
    "  MONEY IN — REQUIRES an explicit dollar amount in the headline of\n"
    "  at least $1 million (e.g., '$5M', '$250 million', '$1.2 billion').\n"
    "  Words implying money without a specific number do NOT qualify\n"
    "  for HIGH. The dollar amount must be stated in the headline text\n"
    "  itself.\n"
    "    - Revenue, partnership, agreement, loan, or investment received\n"
    "    - Lawsuit won or favorable settlement received\n"
    "    - Government contract or grant awarded (DoD, NASA, DOE, etc.)\n"
    "    - Dividend initiation or increase\n"
    "    - Debt paydown or refinancing at better terms\n"
    "    - Insurance settlement received\n"
    "    - Royalty or licensing deal\n"
    "    - Milestone payment received\n"
    "    - Major contract win (named customer, multi-year)\n"
    "    - Earnings beat WITH raised guidance\n"
    "    - Patent litigation won\n"
    "    - Inclusion in major index (S&P 500, Russell 1000, Nasdaq-100)\n"
    "  CORPORATE ACTIONS:\n"
    "    - Acquisition or merger involving the company, in any direction:\n"
    "        * Company acquires another company\n"
    "        * Company is being acquired by another company\n"
    "        * Company merges with another company\n"
    "    - Company is a SPAC or involved in a SPAC deal\n"
    "    - Company filing for bankruptcy\n"
    "    - Reverse stock split\n"
    "  MARKET / COVERAGE:\n"
    "    - Analyst upgrade or price target raise\n"
    "  CLINICAL / BIOTECH (positive RESULTS only, not applications):\n"
    "    - Positive Phase 2, Phase 2b, Phase 3, or Phase 3b results\n"
    "    - Positive cancer-related trial results\n"
    "    - Major breakthrough indications with positive results,\n"
    "      including:\n"
    "        * Cancer cures\n"
    "        * Alzheimer's disease treatments or cures\n"
    "        * Anti-aging / de-aging\n"
    "        * ALS, Parkinson's, Huntington's cures\n"
    "        * Cures for genetic diseases (sickle cell, cystic fibrosis,\n"
    "          muscular dystrophy)\n"
    "        * HIV cures (not just treatments)\n"
    "        * Type 1 diabetes cures (not just management)\n"
    "        * Spinal cord injury reversal / paralysis treatments\n"
    "        * Blindness or deafness reversal\n"
    "        * Universal vaccines (cancer vaccine, universal flu vaccine)\n"
    "        * Any treatment described as a 'cure' for a previously\n"
    "          incurable major disease\n"
    "  AI / STRATEGIC:\n"
    "    - Launches an AI PLATFORM (a standalone product offering, not\n"
    "      a feature added to an existing product)\n"
    "    - Partnership with a named AI company (e.g., OpenAI, Anthropic,\n"
    "      Nvidia, Microsoft AI, Google DeepMind, etc.)\n"
    "    - AI strategy announcement that represents a PIVOT from the\n"
    "      company's current core business (not an extension of\n"
    "      existing operations)\n"
    "    - Major pivot to AI, backed by concrete evidence:\n"
    "        * Renaming the company or core product to reflect AI focus\n"
    "        * Acquisition of a named AI company\n"
    "    - Large AI investment with a specific dollar amount that is\n"
    "      material relative to company size (hundreds of millions or\n"
    "      billions, OR a multi-year capital commitment with a dollar\n"
    "      figure)\n"
    "\n"
    "Score MODERATE confidence (50-74) when the headline indicates:\n"
    "    - Positive Phase 2a results\n"
    "    - Fast Track or Orphan Drug designation\n"
    "    - Breakthrough Therapy designation for major indications\n"
    "      (cancer, Alzheimer's, anti-aging, rare/unbelievable conditions)\n"
    "\n"
    "Score LOW confidence (0-49) when the headline indicates:\n"
    "  CLINICAL / BIOTECH:\n"
    "    - Phase 1 results (positive or otherwise) - always LOW\n"
    "    - Applications, filings, or preparations to file for clinical\n"
    "      trials at any phase\n"
    "    - Trial initiation, enrollment, or design announcements\n"
    "    - PDUFA date set\n"
    "    - Trial mention without explicit positive language\n"
    "    - Breakthrough Therapy designation for non-major indications\n"
    "  AI / STRATEGIC:\n"
    "    - 'Launches AI tool' or 'launches AI feature' (a tool or\n"
    "      feature is an addition to existing products, not a platform)\n"
    "    - 'Integrates AI into [existing product]'\n"
    "    - 'Adds AI capabilities to [existing product]'\n"
    "    - 'AI-powered' / 'AI-driven' / 'AI-enabled' feature launches\n"
    "    - AI strategy that EXTENDS the current business rather than\n"
    "      pivoting away from it\n"
    "    - Generic 'exploring AI' or 'evaluating AI use cases'\n"
    "  UNQUANTIFIED FINANCIALS:\n"
    "    - Money-in events WITHOUT an explicit dollar amount in the\n"
    "      headline (e.g., 'Company signs partnership' with no number)\n"
    "    - Money-in events with dollar amounts under $1 million\n"
    "    - Reverse mergers where the operating company is questionable\n"
    "  HEDGING OR NEGATIVE LANGUAGE:\n"
    "    - Vague language: 'exploring', 'considering', 'in talks',\n"
    "      'evaluating', 'may', 'could', 'potentially'\n"
    "    - Lawsuits FILED AGAINST the company\n"
    "    - Headlines where peers get good news and this company is\n"
    "      mentioned only as a beneficiary by association\n"
    "    - Headlines where the company is only listed among peers\n"
    "  PR LANGUAGE TO IGNORE:\n"
    "    - Words like 'transformative', 'game-changing', 'revolutionary',\n"
    "      'groundbreaking', 'industry-leading' do not raise confidence\n"
    "      on their own. Require concrete evidence (dollar amount, named\n"
    "      counterparty, structural change, or positive trial results).\n"
    "\n"
    "MONEY-IN DOLLAR AMOUNT REQUIREMENT\n"
    "==================================\n"
    "For any money-in event (revenue, partnership, contract, grant,\n"
    "loan, investment, settlement, royalty, milestone, PIPE, direct\n"
    "offering, 'up to $X' deal, etc.) the headline MUST contain an\n"
    "explicit dollar figure of at least $1 million to qualify for\n"
    "HIGH confidence.\n"
    "  - Headline contains '$X million' or '$X billion' (X >= 1) -> HIGH\n"
    "  - Headline contains 'up to $X' where X >= $1 million -> HIGH\n"
    "  - Headline contains a dollar amount under $1 million -> LOW\n"
    "  - Headline implies money but states no number -> LOW\n"
    "  - Headline says 'undisclosed terms' or 'undisclosed amount' -> LOW\n"
    "\n"
    "CLINICAL TRIAL DISAMBIGUATION\n"
    "=============================\n"
    "Distinguish RESULTS from APPLICATIONS:\n"
    "  - Positive Phase 2, 2b, 3, or 3b RESULTS -> HIGH (75-100)\n"
    "  - Positive Phase 2a RESULTS -> MODERATE (50-74)\n"
    "  - Phase 1 RESULTS (any indication, any outcome) -> LOW (0-49)\n"
    "  - APPLICATIONS, FILINGS, or PREPARATIONS to file -> LOW (0-49)\n"
    "  - Trial initiation, enrollment, or design -> LOW (0-49)\n"
    "  - Trial without explicit positive language -> LOW (0-49)\n"
    "\n"
    "AI ANNOUNCEMENT DISAMBIGUATION\n"
    "==============================\n"
    "  - AI PLATFORM launch (standalone product) -> HIGH (75-100)\n"
    "  - AI partnership with NAMED AI company -> HIGH (75-100)\n"
    "  - AI strategy that is a PIVOT from current business -> HIGH\n"
    "  - AI tool or feature added to existing products -> LOW (0-49)\n"
    "  - AI integration into existing products -> LOW (0-49)\n"
    "  - AI strategy that extends existing business -> LOW (0-49)\n"
    "\n"
    "OUTPUT FORMAT\n"
    "=============\n"
    f"Output ONLY a JSON array of objects, each with keys "
    '"idx" (int) and "confidence" (int 0-100). No prose, no markdown '
    "fences, no explanations.\n"
    'Format: [{{"idx":0,"confidence":87}}, ...]\n\n'
    "Headlines:\n{headlines}\n"
)
