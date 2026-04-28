from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from fastscanner.pkg.candle import Candle
from fastscanner.services.indicators.lib.news import (
    EST,
    Headline,
    InNewsIndicator,
)


def _make_candle(ts: datetime | None = None) -> Candle:
    ts = ts or datetime(2026, 4, 29, 10, 30, tzinfo=EST)
    return Candle(
        {"open": 1.0, "high": 1.1, "low": 0.9, "close": 1.05, "volume": 1000},
        timestamp=pd.Timestamp(ts),
    )


# --- public API: extend / extend_realtime ---


@pytest.mark.asyncio
async def test_extend_returns_na_column():
    indicator = InNewsIndicator()
    df = pd.DataFrame(
        {"close": [1.0, 2.0]},
        index=pd.to_datetime(["2026-04-29 10:00", "2026-04-29 10:01"]),
    )
    out = await indicator.extend("AAPL", df)
    assert out[indicator.column_name()].isna().all()


@pytest.mark.asyncio
async def test_first_call_scrapes_and_caches_true():
    indicator = InNewsIndicator()
    with patch.object(
        indicator, "_has_news_today", new=AsyncMock(return_value=True)
    ) as scrape:
        candle = await indicator.extend_realtime("AAPL", _make_candle())
        assert candle[indicator.column_name()] is True
        assert indicator._cached["AAPL"] is True
        scrape.assert_awaited_once_with("AAPL")


@pytest.mark.asyncio
async def test_subsequent_calls_use_cache_no_rescrape():
    indicator = InNewsIndicator()
    scrape = AsyncMock(return_value=False)
    with patch.object(indicator, "_has_news_today", new=scrape):
        for _ in range(3):
            candle = await indicator.extend_realtime("AAPL", _make_candle())
            assert candle[indicator.column_name()] is False
        assert scrape.await_count == 1


@pytest.mark.asyncio
async def test_per_symbol_cache_isolation():
    indicator = InNewsIndicator()
    side = {"AAPL": True, "MSFT": False}
    scrape = AsyncMock(side_effect=lambda symbol: side[symbol])
    with patch.object(indicator, "_has_news_today", new=scrape):
        a = await indicator.extend_realtime("AAPL", _make_candle())
        m = await indicator.extend_realtime("MSFT", _make_candle())
        assert a[indicator.column_name()] is True
        assert m[indicator.column_name()] is False
        assert scrape.await_count == 2


# --- Finviz HTML parser ---


def test_finviz_extract_returns_titles_for_target_date():
    today = datetime.now(EST).strftime("%Y-%m-%d")
    target = datetime.strptime(today, "%Y-%m-%d").strftime("%b-%d-%y")
    html = f"""
    <html><body>
      <table id="news-table">
        <tr>
          <td width="130">{target} 09:30AM&nbsp;</td>
          <td><a class="tab-link-news" href="/news/x">Today headline 1</a></td>
        </tr>
        <tr>
          <td width="130">10:30AM&nbsp;</td>
          <td><a class="tab-link-news" href="/news/y">Today headline 2</a></td>
        </tr>
      </table>
    </body></html>
    """
    titles = InNewsIndicator._finviz_news_table_extract(html, today)
    assert titles == ["Today headline 1", "Today headline 2"]


def test_finviz_extract_skips_other_dates():
    html = """
    <html><body>
      <table id="news-table">
        <tr>
          <td width="130">Apr-27-26 09:30AM&nbsp;</td>
          <td><a class="tab-link-news" href="/x">Yesterday headline</a></td>
        </tr>
      </table>
    </body></html>
    """
    assert InNewsIndicator._finviz_news_table_extract(html, "2026-04-29") == []


def test_finviz_extract_returns_empty_when_table_missing():
    assert InNewsIndicator._finviz_news_table_extract("<html></html>", "2026-04-29") == []


# --- date helpers ---


def test_dt_is_on_date_handles_naive_utc():
    naive = datetime(2026, 4, 29, 14, 30)  # 10:30 EDT
    assert InNewsIndicator._dt_is_on_date(naive, "2026-04-29") is True


def test_dt_is_on_date_handles_timezone_boundary():
    # 03:00 UTC on Apr 29 == 23:00 EDT on Apr 28
    dt = datetime(2026, 4, 29, 3, 0, tzinfo=timezone.utc)
    assert InNewsIndicator._dt_is_on_date(dt, "2026-04-28") is True
    assert InNewsIndicator._dt_is_on_date(dt, "2026-04-29") is False


# --- substring filter ---


def test_substring_match_keeps_only_matching_titles():
    indicator = InNewsIndicator()
    headlines = [
        Headline(title="AAPL beats earnings", source="finviz"),
        Headline(title="Tech sector rallies on Fed news", source="yahoo"),
        Headline(title="why aapl is undervalued", source="seeking_alpha"),
    ]
    kept = indicator._substring_match("AAPL", headlines)
    assert [h.title for h in kept] == [
        "AAPL beats earnings",
        "why aapl is undervalued",
    ]


# --- _has_news_today orchestration ---


@pytest.mark.asyncio
async def test_no_headlines_returns_false():
    indicator = InNewsIndicator()
    with patch.object(indicator, "_finviz_headlines", new=AsyncMock(return_value=[])), \
         patch.object(indicator, "_finnhub_headlines", new=AsyncMock(return_value=[])), \
         patch.object(indicator, "_yahoo_headlines", new=AsyncMock(return_value=[])), \
         patch.object(indicator, "_seeking_alpha_headlines", new=AsyncMock(return_value=[])), \
         patch.object(indicator, "_marketwatch_headlines", new=AsyncMock(return_value=[])):
        assert await indicator._has_news_today("AAPL") is False


@pytest.mark.asyncio
async def test_substring_hit_short_circuits_codex():
    indicator = InNewsIndicator()
    codex = AsyncMock()
    with patch.object(
        indicator,
        "_finviz_headlines",
        new=AsyncMock(return_value=["AAPL surges on guidance raise"]),
    ), patch.object(indicator, "_finnhub_headlines", new=AsyncMock(return_value=[])), \
         patch.object(indicator, "_yahoo_headlines", new=AsyncMock(return_value=[])), \
         patch.object(indicator, "_seeking_alpha_headlines", new=AsyncMock(return_value=[])), \
         patch.object(indicator, "_marketwatch_headlines", new=AsyncMock(return_value=[])), \
         patch.object(indicator, "_filter_with_codex", new=codex):
        result = await indicator._has_news_today("AAPL")
        assert result is True
        codex.assert_not_called()


@pytest.mark.asyncio
async def test_codex_runs_when_no_substring_hit():
    indicator = InNewsIndicator()
    codex_kept = [Headline(title="Acme reports earnings", source="finviz")]
    codex = AsyncMock(return_value=codex_kept)
    with patch.object(
        indicator,
        "_finviz_headlines",
        new=AsyncMock(return_value=["Acme reports earnings"]),
    ), patch.object(indicator, "_finnhub_headlines", new=AsyncMock(return_value=[])), \
         patch.object(indicator, "_yahoo_headlines", new=AsyncMock(return_value=[])), \
         patch.object(indicator, "_seeking_alpha_headlines", new=AsyncMock(return_value=[])), \
         patch.object(indicator, "_marketwatch_headlines", new=AsyncMock(return_value=[])), \
         patch.object(indicator, "_filter_with_codex", new=codex):
        # ticker XYZ does not appear in title → substring miss → Codex runs
        result = await indicator._has_news_today("XYZ")
        assert result is True
        codex.assert_awaited_once()


@pytest.mark.asyncio
async def test_codex_failure_returns_false_when_no_substring_hit():
    indicator = InNewsIndicator()
    codex = AsyncMock(side_effect=RuntimeError("codex blew up"))
    with patch.object(
        indicator,
        "_finviz_headlines",
        new=AsyncMock(return_value=["Sector ETF rebalance"]),
    ), patch.object(indicator, "_finnhub_headlines", new=AsyncMock(return_value=[])), \
         patch.object(indicator, "_yahoo_headlines", new=AsyncMock(return_value=[])), \
         patch.object(indicator, "_seeking_alpha_headlines", new=AsyncMock(return_value=[])), \
         patch.object(indicator, "_marketwatch_headlines", new=AsyncMock(return_value=[])), \
         patch.object(indicator, "_filter_with_codex", new=codex):
        assert await indicator._has_news_today("XYZ") is False
        codex.assert_awaited_once()


@pytest.mark.asyncio
async def test_codex_missing_returns_false_when_no_substring_hit():
    indicator = InNewsIndicator()
    with patch.object(
        indicator,
        "_finviz_headlines",
        new=AsyncMock(return_value=["Some unrelated headline"]),
    ), patch.object(indicator, "_finnhub_headlines", new=AsyncMock(return_value=[])), \
         patch.object(indicator, "_yahoo_headlines", new=AsyncMock(return_value=[])), \
         patch.object(indicator, "_seeking_alpha_headlines", new=AsyncMock(return_value=[])), \
         patch.object(indicator, "_marketwatch_headlines", new=AsyncMock(return_value=[])), \
         patch("fastscanner.services.indicators.lib.news.shutil.which", return_value=None):
        assert await indicator._has_news_today("XYZ") is False


@pytest.mark.asyncio
async def test_short_symbol_skips_substring_pass_goes_straight_to_codex():
    indicator = InNewsIndicator()
    # "GE" (2 chars) appears in "GENERAL" but we must skip the substring pass.
    codex = AsyncMock(return_value=[])
    with patch.object(
        indicator,
        "_finviz_headlines",
        new=AsyncMock(return_value=["GENERAL market update; banks rally"]),
    ), patch.object(indicator, "_finnhub_headlines", new=AsyncMock(return_value=[])), \
         patch.object(indicator, "_yahoo_headlines", new=AsyncMock(return_value=[])), \
         patch.object(indicator, "_seeking_alpha_headlines", new=AsyncMock(return_value=[])), \
         patch.object(indicator, "_marketwatch_headlines", new=AsyncMock(return_value=[])), \
         patch.object(indicator, "_substring_match") as substring_mock, \
         patch.object(indicator, "_filter_with_codex", new=codex):
        result = await indicator._has_news_today("GE")
        assert result is False  # codex returned no keepers
        substring_mock.assert_not_called()  # substring pass skipped entirely
        codex.assert_awaited_once()


@pytest.mark.asyncio
async def test_source_exception_does_not_break_other_sources():
    indicator = InNewsIndicator()
    codex = AsyncMock(return_value=[])
    with patch.object(
        indicator, "_finviz_headlines", new=AsyncMock(side_effect=RuntimeError("network"))
    ), patch.object(
        indicator, "_finnhub_headlines", new=AsyncMock(return_value=["Acme rises 10%"])
    ), patch.object(indicator, "_yahoo_headlines", new=AsyncMock(return_value=[])), \
         patch.object(indicator, "_seeking_alpha_headlines", new=AsyncMock(return_value=[])), \
         patch.object(indicator, "_marketwatch_headlines", new=AsyncMock(return_value=[])), \
         patch.object(indicator, "_filter_with_codex", new=codex):
        # finviz raises but finnhub still produces a headline; substring miss → codex
        result = await indicator._has_news_today("XYZ")
        assert result is False
        codex.assert_awaited_once()


# --- Codex response parser ---


def test_parse_codex_response_handles_prose_around_json():
    text = (
        "Here is the analysis you asked for:\n\n"
        '[{"idx":0,"confidence":87},{"idx":1,"confidence":12}]\n\n'
        "Hope that helps!"
    )
    out = InNewsIndicator._parse_codex_response(text, n_headlines=2)
    assert out == [
        {"idx": 0, "confidence": 87},
        {"idx": 1, "confidence": 12},
    ]


def test_parse_codex_response_raises_on_no_array():
    with pytest.raises(RuntimeError, match="no JSON array"):
        InNewsIndicator._parse_codex_response("just prose, no JSON here", n_headlines=2)


def test_parse_codex_response_drops_invalid_entries():
    text = (
        '[{"idx":0,"confidence":99},'
        '{"idx":"bad","confidence":50},'
        '{"idx":1,"confidence":150},'
        '{"idx":99,"confidence":50},'
        '{"idx":2,"confidence":-5},'
        '{"idx":3,"confidence":50}]'
    )
    out = InNewsIndicator._parse_codex_response(text, n_headlines=4)
    assert out == [
        {"idx": 0, "confidence": 99},
        {"idx": 3, "confidence": 50},
    ]


def test_parse_codex_response_raises_when_all_entries_invalid():
    text = '[{"idx":99,"confidence":50},{"idx":"bad","confidence":50}]'
    with pytest.raises(RuntimeError, match="no valid entries"):
        InNewsIndicator._parse_codex_response(text, n_headlines=2)


# --- registry ---


def test_indicator_is_registered_in_library():
    from fastscanner.services.indicators.lib import IndicatorsLibrary

    library = IndicatorsLibrary.instance()
    indicator = library.get("in_news", {})
    assert isinstance(indicator, InNewsIndicator)
    assert indicator.column_name() == "in_news"
