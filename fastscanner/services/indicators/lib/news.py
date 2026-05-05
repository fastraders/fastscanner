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


PROMPT = """
You are a strict JSON-only classifier. Use only your training knowledge.

For ticker symbol {symbol}, output the confidence (0-100) that each headline represents POSITIVE, ACTIONABLE news for the company corresponding to {symbol}.

Confidence >= 50 means the headline should trigger action.
Confidence < 50 means the headline should be skipped.

CONFIDENCE TIERS
================
   0-49   LOW           — skip
  50-74   MODERATE      — borderline actionable
  75-89   HIGH          — actionable
  90-100  TIER-DEFINING — high-conviction, rare

SCORING RUBRIC
==============
Score HIGH (75-89), or TIER-DEFINING (90-100) where indicated, when
the headline clearly indicates:

  MONEY IN — REQUIRES an explicit dollar amount in the headline of
  at least $1 million (e.g., '$5M', '$250 million', '$1.2 billion').
  Words implying money without a specific number do NOT qualify
  for HIGH. The dollar amount must be stated in the headline text
  itself.
    Apply the dollar threshold:
      * Stated amount >= $10M -> 90-100 (TIER-DEFINING)
      * Stated amount >= $1M and < $10M -> 75-89 (HIGH)
    Event types covered:
    - Revenue, partnership, agreement, loan, or investment received
    - Lawsuit won or favorable settlement received
    - Government contract or grant awarded (DoD, NASA, DOE, etc.)
    - Dividend initiation or increase
    - Debt paydown or refinancing at better terms
    - Insurance settlement received
    - Royalty or licensing deal
    - Milestone payment received
    - Major contract win (named customer, multi-year)
    - Earnings beat WITH raised guidance
    - Patent litigation won
    - Inclusion in major index (S&P 500, Russell 1000, Nasdaq-100)
  CORPORATE ACTIONS:
    M&A is scored based on what {symbol} is doing in the deal and
    what {symbol} is gaining or giving up.

    {symbol} RECEIVES VALUE (HIGH-eligible):
      - {symbol} is being acquired by another company
      - Another company / fund / investor takes a stake in {symbol}
        (any percentage). Stakes >= 30% are particularly strong
        signals and should always score TIER-DEFINING (90-100)
        when there is a named acquirer, regardless of stated price.
      - {symbol} divests / sells assets / sells a subsidiary and
        receives stated payments
      - {symbol} merges with another company in a deal where
        {symbol} shareholders receive consideration
      Apply the dollar threshold for stated prices / payments to
      {symbol}:
        * Stated price/payment >= $10M -> 90-100 (TIER-DEFINING)
        * Stated price/payment >= $1M and < $10M -> 75-89 (HIGH)
        * No stated price -> 75-89 (HIGH) only if definitive with
          named counterparty
        * Named acquirer takes >= 30% stake -> 90-100 always
      For 'up to $X' payment language, use X as the size.

    {symbol} ACQUIRES AN OPERATING BUSINESS (HIGH-eligible):
      When {symbol} is the acquirer and is buying an operating
      business, operating assets, intellectual property, a
      subsidiary, a product line, or another company, score based
      on stated price:
        * Stated price >= $10M -> 90-100 (TIER-DEFINING)
        * Stated price >= $1M and < $10M -> 75-89 (HIGH)
        * No stated price -> 75-89 (HIGH) only if definitive with
          a named target
      Operating assets means assets that produce revenue or form
      a business (e.g., a drone fleet to operate a drone service,
      patents, a software platform, manufacturing equipment, a
      brand, a customer base, an entire company).

    {symbol} BUYS FINANCIAL ASSETS (LOW):
      When {symbol} is the buyer and is acquiring financial
      assets rather than an operating business, score LOW (0-49)
      even with a stated price. Financial assets include:
      - Cryptocurrency, tokens, stablecoins, digital assets
      - Stocks, bonds, securities, treasuries
      - Commodities (gold, silver, oil, etc.)
      - Real estate held as investment (not as the operating
        business)
      - ETFs, mutual funds, investment portfolios
      This is a treasury / investment move, not a transformative
      M&A event. The company is converting cash into another
      asset class.

    {symbol} TAKES A MINORITY STAKE IN ANOTHER COMPANY (LOW):
      When {symbol} or a {symbol} subsidiary takes a stake in
      another company without receiving payment back, score LOW
      (0-49) regardless of stake size or stated price. This is
      money-out without offsetting value flowing to {symbol}.
      EXCEPTION: If the headline explicitly states {symbol}
      receives cash, payments, or other value back as part of
      the deal (e.g., "acquires X and receives $Y in milestone
      payments"), apply the RECEIVES VALUE rule above instead.

    OTHER CORPORATE ACTIONS:
    - Company is a SPAC or involved in a SPAC deal
    - Company filing for bankruptcy
    - Reverse stock split
  MARKET / COVERAGE:
    - Analyst upgrade or price target raise
  CLINICAL / BIOTECH (positive RESULTS only, not applications):
    - Positive Phase 2, Phase 2b, Phase 3, or Phase 3b results
    - Positive cancer-related trial results
    - Major breakthrough indications with positive results,
      including:
        * Cancer cures
        * Alzheimer's disease treatments or cures
        * Anti-aging / de-aging
        * ALS, Parkinson's, Huntington's cures
        * Cures for genetic diseases (sickle cell, cystic fibrosis,
          muscular dystrophy)
        * HIV cures (not just treatments)
        * Type 1 diabetes cures (not just management)
        * Spinal cord injury reversal / paralysis treatments
        * Blindness or deafness reversal
        * Universal vaccines (cancer vaccine, universal flu vaccine)
        * Any treatment described as a 'cure' for a previously
          incurable major disease
  AI / STRATEGIC:
    - Launches an AI PLATFORM (a standalone product offering, not
      a feature added to an existing product)
    - Partnership where {symbol} is the RECIPIENT of AI capability
      from a named tier-1 AI company (e.g., OpenAI, Anthropic,
      Nvidia, Microsoft AI, Google DeepMind). If {symbol} is itself
      an AI company providing AI to the counterparty, the standard
      money-in dollar threshold applies instead.
    - Large AI investment with a specific dollar amount that is
      material relative to company size (hundreds of millions or
      billions, OR a multi-year capital commitment with a dollar
      figure)

Score MODERATE confidence (50-74) when the headline indicates:
    - Positive Phase 2a results
    - Fast Track designation
    - Breakthrough Therapy designation for major indications
      (cancer, Alzheimer's, anti-aging, rare/unbelievable conditions)
    - AI strategy announcement that represents a PIVOT from the
      company's current core business (not an extension of
      existing operations)
    - Major pivot to AI, backed by concrete evidence:
        * Renaming the company or core product to reflect AI focus
        * Acquisition of a named AI company

Score LOW confidence (0-49) when the headline indicates:
  CLINICAL / BIOTECH:
    - Phase 1 results (positive or otherwise) - always LOW
    - Applications, filings, or preparations to file for clinical
      trials at any phase
    - Trial initiation, enrollment, or design announcements
    - PDUFA date set
    - Trial mention without explicit positive language
    - Breakthrough Therapy designation for non-major indications
  AI / STRATEGIC:
    - 'Launches AI tool' or 'launches AI feature' (a tool or
      feature is an addition to existing products, not a platform)
    - 'Integrates AI into [existing product]'
    - 'Adds AI capabilities to [existing product]'
    - 'AI-powered' / 'AI-driven' / 'AI-enabled' feature launches
    - AI strategy that EXTENDS the current business rather than
      pivoting away from it
    - Generic 'exploring AI' or 'evaluating AI use cases'
    - {symbol} is itself an AI company announcing a partnership
      where it provides AI to the counterparty, with no dollar
      amount stated
  UNQUANTIFIED FINANCIALS:
    - Money-in events WITHOUT an explicit dollar amount in the
      headline AND without a named well-known counterparty
    - Money-in events with dollar amounts under $1 million
    - Reverse mergers where the operating company is questionable
  HEDGING OR NEGATIVE LANGUAGE:
    - Vague language: 'exploring', 'considering', 'in talks',
      'evaluating', 'may', 'could', 'potentially'
    - Lawsuits FILED AGAINST the company
    - Headlines where peers get good news and this company is
      mentioned only as a beneficiary by association
    - Headlines where the company is only listed among peers
  PR LANGUAGE TO IGNORE:
    - Words like 'transformative', 'game-changing', 'revolutionary',
      'groundbreaking', 'industry-leading' do not raise confidence
      on their own. Require concrete evidence (dollar amount, named
      counterparty, structural change, or positive trial results).

MONEY-IN DOLLAR AMOUNT REQUIREMENT
==================================
For any money-in event (revenue, partnership, contract, grant,
loan, investment, settlement, royalty, milestone, PIPE, direct
offering, 'up to $X' deal, etc.) the headline MUST contain an
explicit dollar figure of at least $1 million to qualify for
HIGH confidence.
  - Headline contains '$X million' or '$X billion' where X >= 10
    -> 90-100 (TIER-DEFINING)
  - Headline contains '$X million' where 1 <= X < 10
    -> 75-89 (HIGH)
  - Headline contains 'up to $X' where X >= $10M -> 90-100
  - Headline contains 'up to $X' where $1M <= X < $10M -> 75-89
  - Headline contains a dollar amount under $1 million -> LOW
  - Headline implies money but states no number -> LOW
  - Headline says 'undisclosed terms' or 'undisclosed amount' -> LOW

HYPE CARVE-OUT — LARGE OPPORTUNITY / MARKET SIZE
================================================
If the headline contains a stated dollar figure describing the
size of an opportunity, market, addressable market (TAM), trading
volume, industry, or similar scope/scale measure, and the figure
is in the hundreds of millions or higher, score 90-100
(TIER-DEFINING).

This is a HYPE carve-out — it is not a money-in event. The dollar
figure does not have to represent money flowing to the company.
It can describe:
  - The size of the market the company is entering
  - Trading volume of a partner platform
  - Estimated TAM or annual industry volume
  - Scope of an opportunity the company is positioned to address

Thresholds:
  - Stated opportunity / market size / volume >= $100 million
    -> 90-100 (TIER-DEFINING)
  - Stated opportunity / market size / volume >= $1 billion
    -> 90-100 (TIER-DEFINING)
  - Stated opportunity / market size / volume >= $1 trillion
    -> 90-100 (TIER-DEFINING)
  - Stated opportunity / market size / volume < $100 million
    -> follow normal rules for the headline's primary event type

This rule OVERRIDES the partnership rule and any other rule that
would otherwise score the headline lower. If the headline mentions
a hype figure in the hundreds of millions or higher, the score is
90+ regardless of other classification.

PRIVATE PLACEMENT / PIPE / SECURITIES PURCHASE AGREEMENT
========================================================
Private placements, PIPEs, securities purchase agreements,
registered direct offerings, and similar structures where the
company sells shares, ADS, units, or warrants to specific
investors. Score them based on size:

  - Headline states size < $1M -> LOW (0-49)
  - Headline states size >= $1M and < $15M -> 75-89 (HIGH)
  - Headline states size >= $15M -> 90-100 (TIER-DEFINING)
  - Headline states no size or 'up to $X' where X < $1M -> LOW

This rule OVERRIDES the general money-in dollar threshold for
private placements.

For 'up to $X' language, use X as the size for tier purposes.

EXCEPTION — NAMED STRATEGIC INVESTOR (overrides the size tier):
If the private placement is led by, or includes a named strategic
investor (a named pharmaceutical company, named fund, named
institutional investor, or other identifiable strategic partner)
with explicit dollar amount >= $1M, score 90-100 (TIER-DEFINING)
regardless of size.

Generic descriptions like 'institutional investors', 'accredited
investors', or unnamed financing firms do NOT count as a named
strategic investor. The investor must be specifically named in
the headline (e.g., 'Pfizer', 'BlackRock', 'ARCH Venture
Partners').

PARTNERSHIP / AGREEMENT WITHOUT DOLLAR AMOUNT
=============================================
A 'partnership', 'collaboration', 'strategic alliance', 'MOU', or
similar structure WITHOUT an explicit dollar amount in the headline
is scored as follows:

  - Partnership with a NAMED, WELL-KNOWN counterparty (a
    recognizable major company, exchange, platform, or institution
    — e.g., Microsoft, Apple, Amazon, Walmart, JPMorgan, Visa,
    Mastercard, Coinbase, Crypto.com, Binance, Shopify, Salesforce,
    OpenAI, Anthropic, Nvidia, Google, Meta, or any company of
    similar tier and recognition) -> 75-89 (HIGH)
  - Partnership with no named counterparty, or with an obscure /
    unrecognizable counterparty -> LOW (0-49)
  - Partnership described with hedging language ('exploring',
    'in talks', 'considering', 'evaluating', 'MOU to discuss')
    -> LOW (0-49) regardless of counterparty

Use judgment for "well-known": if the counterparty is a Fortune
500 company, a major exchange, a top-tier financial institution,
a major tech platform, or a household name, treat it as
well-known. If you have to look the company up to know what they
do, they are not well-known.

This rule does NOT apply to:
  - Acquisition agreements / asset purchase agreements / merger
    agreements (these are corporate actions — see the M&A rule)
  - Definitive agreements with stated dollar amounts
  - Government contracts with stated dollar amounts
  - Licensing or royalty agreements with stated dollar amounts

The word 'agreement' alone does NOT trigger this rule. The rule
fires only when the structure is a partnership, collaboration,
MOU, or alliance with no dollar amount stated.

CLINICAL TRIAL DISAMBIGUATION
=============================
Distinguish RESULTS from APPLICATIONS:
  - Positive Phase 2, 2b, 3, or 3b RESULTS -> 75-100 (HIGH); for
    biotechs, see carve-out below
  - Positive Phase 2a RESULTS -> 50-74 (MODERATE)
  - Phase 1 RESULTS (any indication, any outcome) -> LOW (0-49)
  - APPLICATIONS, FILINGS, or PREPARATIONS to file -> LOW (0-49)
  - Trial initiation, enrollment, or design -> LOW (0-49)
  - Trial without explicit positive language -> LOW (0-49)

AI ANNOUNCEMENT DISAMBIGUATION
==============================
  - AI PLATFORM launch (standalone product) -> 75-89 (HIGH)
  - {symbol} receives AI capability from named tier-1 AI company
    (OpenAI, Anthropic, Nvidia, Microsoft AI, Google DeepMind)
    -> 75-89 (HIGH), no dollar amount required
  - {symbol} is itself an AI company providing AI to the
    counterparty: standard money-in threshold applies
  - AI strategy that is a PIVOT from current business -> 50-74
    (MODERATE)
  - Major pivot to AI (renaming, AI company acquisition) -> 50-74
    (MODERATE)
  - AI tool or feature added to existing products -> LOW (0-49)
  - AI integration into existing products -> LOW (0-49)
  - AI strategy that extends existing business -> LOW (0-49)

BIOTECH / HEALTHCARE CARVE-OUTS
===============================
These rules OVERRIDE the general rules above when the company is
a biotech, pharmaceutical, or medical/healthcare company.

REGULATORY DESIGNATIONS:
  - Orphan Drug designation (any indication) -> 90-100 (TIER-DEFINING)

OUTSIDE INVESTMENT / OWNERSHIP:
  Stakes taken in {symbol} by another party are governed by the
  general M&A directionality rule above (RECEIVES VALUE). The
  rules below cover biotech-specific equity investment patterns
  that are not covered there.

  - Outside equity investment from a NAMED strategic third party
    (named pharma company, named fund, named institutional
    investor) with explicit dollar amount of at least $1 million
    -> 90-100 (TIER-DEFINING)
  - Private placement, PIPE, direct offering without a NAMED
    strategic investor -> follow the PRIVATE PLACEMENT tier rule
    above (size-based: < $15M is HIGH, >= $15M is TIER-DEFINING)
  - PIPE, direct offering, or registered offering led by a named
    strategic or institutional investor with explicit dollar
    amount >= $1 million -> 90-100 (TIER-DEFINING)

CLINICAL / BIOTECH RESULTS (BIOTECH-SPECIFIC):
  - Positive Phase 2, Phase 2b, Phase 3, or Phase 3b results
    -> 90-100 (TIER-DEFINING)
  - Positive cancer-related trial results -> 90-100 (TIER-DEFINING)
  - Major breakthrough indications with positive results
    -> 75-89 (HIGH). Do NOT bump to 90+. Includes:
        * Cancer cures
        * Alzheimer's disease treatments or cures
        * Anti-aging / de-aging
        * ALS, Parkinson's, Huntington's cures
        * Cures for genetic diseases (sickle cell, cystic fibrosis,
          muscular dystrophy)
        * HIV cures (not just treatments)
        * Type 1 diabetes cures (not just management)
        * Spinal cord injury reversal / paralysis treatments
        * Blindness or deafness reversal
        * Universal vaccines (cancer vaccine, universal flu vaccine)
        * Any treatment described as a 'cure' for a previously
          incurable major disease

MONEY-IN RESTRICTION (BIOTECH-SPECIFIC):
For biotech and healthcare companies, money-in events qualify for
HIGH or TIER-DEFINING confidence ONLY if the funds come from
outside equity investment. The following do NOT qualify and should
be scored LOW (0-49):
  - Loans, credit facilities, or debt financing of any size
  - Convertible notes (treat as debt unless explicitly described
    as equity-linked strategic investment)
  - Standby equity purchase agreements (SEPAs) / equity lines of
    credit
  - At-the-market (ATM) offerings
  - Refinancing or debt restructuring
  - Royalty financing or revenue-based financing

This restriction applies ONLY to biotech and healthcare. For all
other sectors, the general money-in rules above still apply.

OUTPUT FORMAT
=============
Output ONLY a JSON array of objects, each with keys "idx" (int) and "confidence" (int 0-100). No prose, no markdown fences, no explanations.
Format: [{{"idx":0,"confidence":87}}, ...]

Headlines:
{headlines}
"""
