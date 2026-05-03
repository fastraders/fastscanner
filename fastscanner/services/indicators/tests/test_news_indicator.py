from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from fastscanner.pkg.candle import Candle
from fastscanner.services.indicators.lib.news import (
    EST,
    Headline,
    InNewsIndicator,
)
from fastscanner.services.registry import ApplicationRegistry


@pytest.fixture(autouse=True)
def _clean_registry():
    had = "cache" in ApplicationRegistry.__dict__
    orig = ApplicationRegistry.__dict__.get("cache")
    yield
    if had and orig is not None:
        ApplicationRegistry.cache = orig
    elif "cache" in ApplicationRegistry.__dict__:
        try:
            del ApplicationRegistry.cache
        except AttributeError:
            pass


def _make_candle(ts: datetime | None = None) -> Candle:
    ts = ts or datetime(2026, 4, 29, 10, 30)
    return Candle(
        {"open": 1.0, "high": 1.1, "low": 0.9, "close": 1.05, "volume": 1000},
        timestamp=pd.Timestamp(ts),
    )


def _make_candle_on_date(year: int, month: int, day: int) -> Candle:
    ts = datetime(year, month, day, 10, 30)
    return Candle(
        {"open": 1.0, "high": 1.1, "low": 0.9, "close": 1.05, "volume": 1000},
        timestamp=pd.Timestamp(ts),
    )


# --- extend ---


@pytest.mark.asyncio
async def test_extend_returns_na_column():
    indicator = InNewsIndicator()
    df = pd.DataFrame(
        {"close": [1.0, 2.0]},
        index=pd.to_datetime(["2026-04-29 10:00", "2026-04-29 10:01"]),
    )
    out = await indicator.extend("AAPL", df)
    assert out[indicator.column_name()].isna().all()


# --- consumer mode (caching=False, default) ---


@pytest.mark.asyncio
async def test_consumer_reads_cache_true():
    indicator = InNewsIndicator()
    mock_cache = AsyncMock()
    mock_cache.get = AsyncMock(return_value="true")
    ApplicationRegistry.cache = mock_cache
    candle = await indicator.extend_realtime("AAPL", _make_candle())
    assert candle[indicator.column_name()] is True
    assert indicator._in_news_today["AAPL"] is True


@pytest.mark.asyncio
async def test_consumer_cache_miss_returns_false():
    indicator = InNewsIndicator()
    mock_cache = AsyncMock()
    mock_cache.get = AsyncMock(side_effect=KeyError("miss"))
    ApplicationRegistry.cache = mock_cache
    candle = await indicator.extend_realtime("AAPL", _make_candle())
    assert candle[indicator.column_name()] is False
    assert "AAPL" not in indicator._in_news_today


@pytest.mark.asyncio
async def test_consumer_short_circuits_after_true():
    indicator = InNewsIndicator()
    mock_cache = AsyncMock()
    mock_cache.get = AsyncMock(return_value="true")
    ApplicationRegistry.cache = mock_cache
    await indicator.extend_realtime("AAPL", _make_candle())
    # second call should not hit cache (sticky-True in memory)
    mock_cache.get.reset_mock()
    candle = await indicator.extend_realtime("AAPL", _make_candle())
    assert candle[indicator.column_name()] is True
    mock_cache.get.assert_not_called()


@pytest.mark.asyncio
async def test_consumer_does_not_call_has_news_today():
    indicator = InNewsIndicator()
    mock_cache = AsyncMock()
    mock_cache.get = AsyncMock(return_value="false")
    scrape = AsyncMock(return_value=True)
    ApplicationRegistry.cache = mock_cache
    with patch.object(indicator, "_has_news_today", new=scrape):
        await indicator.extend_realtime("AAPL", _make_candle())
    scrape.assert_not_called()


@pytest.mark.asyncio
async def test_per_symbol_cache_isolation():
    indicator = InNewsIndicator()
    mock_cache = AsyncMock()
    mock_cache.get = AsyncMock(
        side_effect=lambda k: (
            "true" if "AAPL" in k else (_ for _ in ()).throw(KeyError("miss"))
        )
    )
    ApplicationRegistry.cache = mock_cache
    a = await indicator.extend_realtime("AAPL", _make_candle())
    m = await indicator.extend_realtime("MSFT", _make_candle())
    assert a[indicator.column_name()] is True
    assert m[indicator.column_name()] is False


# --- day boundary reset ---


@pytest.mark.asyncio
async def test_day_boundary_resets_state():
    indicator = InNewsIndicator()
    mock_cache = AsyncMock()
    call_count = 0

    async def _get(key):
        nonlocal call_count
        call_count += 1
        return "true"

    mock_cache.get = _get
    ApplicationRegistry.cache = mock_cache
    # Day 1: cache returns true → sticky
    await indicator.extend_realtime("AAPL", _make_candle_on_date(2026, 4, 29))
    assert indicator._in_news_today.get("AAPL") is True
    # Day 2: new date → state reset, cache re-read
    await indicator.extend_realtime("AAPL", _make_candle_on_date(2026, 4, 30))
    assert call_count == 2  # cache queried on both days


# --- inline producer mode (caching=True) ---


@pytest.mark.asyncio
async def test_producer_fire_and_forget_does_not_block():
    indicator = InNewsIndicator(caching=True)
    mock_cache = AsyncMock()
    mock_cache.save = AsyncMock()

    async def _slow_fetch(_symbol):
        import asyncio

        await asyncio.sleep(0.05)
        return True

    ApplicationRegistry.cache = mock_cache
    with patch.object(
        indicator, "_has_news_today", new=AsyncMock(side_effect=_slow_fetch)
    ), patch("fastscanner.services.indicators.lib.news.random.uniform", return_value=0):
        candle = await indicator.extend_realtime("AAPL", _make_candle())
        # Returns immediately (False until background task finishes)
        assert candle[indicator.column_name()] is False
        # Let background task finish
        import asyncio

        await asyncio.sleep(0.1)
    assert indicator._in_news_today.get("AAPL") is True
    mock_cache.save.assert_called_once_with(InNewsIndicator._cache_key("AAPL"), "true")


@pytest.mark.asyncio
async def test_producer_dedup_spawns_one_task_per_symbol():
    indicator = InNewsIndicator(caching=True)
    mock_cache = AsyncMock()
    mock_cache.save = AsyncMock()
    scrape = AsyncMock(return_value=False)

    ApplicationRegistry.cache = mock_cache
    with patch.object(indicator, "_has_news_today", new=scrape), patch(
        "fastscanner.services.indicators.lib.news.random.uniform", return_value=0
    ):
        import asyncio

        for _ in range(5):
            await indicator.extend_realtime("AAPL", _make_candle())
        await asyncio.sleep(0.05)

    assert scrape.await_count == 1


@pytest.mark.asyncio
async def test_producer_sticky_true_prevents_refetch():
    indicator = InNewsIndicator(caching=True)
    indicator._in_news_today["AAPL"] = True
    indicator._date["AAPL"] = _make_candle().timestamp.date()
    scrape = AsyncMock(return_value=True)

    with patch.object(indicator, "_has_news_today", new=scrape):
        candle = await indicator.extend_realtime("AAPL", _make_candle())

    assert candle[indicator.column_name()] is True
    scrape.assert_not_called()


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
    assert (
        InNewsIndicator._finviz_news_table_extract("<html></html>", "2026-04-29") == []
    )


# --- date helpers ---


def test_dt_is_on_date_handles_naive_utc():
    naive = datetime(2026, 4, 29, 14, 30)  # 10:30 EDT
    assert InNewsIndicator._dt_is_on_date(naive, "2026-04-29") is True


def test_dt_is_on_date_handles_timezone_boundary():
    # 03:00 UTC on Apr 29 == 23:00 EDT on Apr 28
    dt = datetime(2026, 4, 29, 3, 0, tzinfo=timezone.utc)
    assert InNewsIndicator._dt_is_on_date(dt, "2026-04-28") is True
    assert InNewsIndicator._dt_is_on_date(dt, "2026-04-29") is False


@pytest.mark.asyncio
async def test_no_headlines_returns_false():
    indicator = InNewsIndicator()
    with patch.object(
        indicator, "_finviz_headlines", new=AsyncMock(return_value=[])
    ), patch.object(
        indicator, "_finnhub_headlines", new=AsyncMock(return_value=[])
    ), patch.object(
        indicator, "_yahoo_headlines", new=AsyncMock(return_value=[])
    ), patch.object(
        indicator, "_seeking_alpha_headlines", new=AsyncMock(return_value=[])
    ), patch.object(
        indicator, "_marketwatch_headlines", new=AsyncMock(return_value=[])
    ):
        assert await indicator._has_news_today("AAPL") is False


@pytest.mark.asyncio
async def test_codex_runs_for_headlines():
    indicator = InNewsIndicator()
    codex_kept = [Headline(title="Acme reports earnings", source="finviz")]
    codex = AsyncMock(return_value=codex_kept)
    with patch.object(
        indicator,
        "_finviz_headlines",
        new=AsyncMock(return_value=["Acme reports earnings"]),
    ), patch.object(
        indicator, "_finnhub_headlines", new=AsyncMock(return_value=[])
    ), patch.object(
        indicator, "_yahoo_headlines", new=AsyncMock(return_value=[])
    ), patch.object(
        indicator, "_seeking_alpha_headlines", new=AsyncMock(return_value=[])
    ), patch.object(
        indicator, "_marketwatch_headlines", new=AsyncMock(return_value=[])
    ), patch.object(
        indicator, "_filter_with_codex", new=codex
    ):
        result = await indicator._has_news_today("XYZ")
        assert result is True
        codex.assert_awaited_once()


@pytest.mark.asyncio
async def test_codex_failure_returns_false_when_headlines_exist():
    indicator = InNewsIndicator()
    codex = AsyncMock(side_effect=RuntimeError("codex blew up"))
    with patch.object(
        indicator,
        "_finviz_headlines",
        new=AsyncMock(return_value=["Sector ETF rebalance"]),
    ), patch.object(
        indicator, "_finnhub_headlines", new=AsyncMock(return_value=[])
    ), patch.object(
        indicator, "_yahoo_headlines", new=AsyncMock(return_value=[])
    ), patch.object(
        indicator, "_seeking_alpha_headlines", new=AsyncMock(return_value=[])
    ), patch.object(
        indicator, "_marketwatch_headlines", new=AsyncMock(return_value=[])
    ), patch.object(
        indicator, "_filter_with_codex", new=codex
    ):
        assert await indicator._has_news_today("XYZ") is False
        codex.assert_awaited_once()


@pytest.mark.asyncio
async def test_codex_missing_returns_false_when_headlines_exist():
    indicator = InNewsIndicator()
    with patch.object(
        indicator,
        "_finviz_headlines",
        new=AsyncMock(return_value=["Some unrelated headline"]),
    ), patch.object(
        indicator, "_finnhub_headlines", new=AsyncMock(return_value=[])
    ), patch.object(
        indicator, "_yahoo_headlines", new=AsyncMock(return_value=[])
    ), patch.object(
        indicator, "_seeking_alpha_headlines", new=AsyncMock(return_value=[])
    ), patch.object(
        indicator, "_marketwatch_headlines", new=AsyncMock(return_value=[])
    ), patch(
        "fastscanner.services.indicators.lib.news.shutil.which", return_value=None
    ):
        assert await indicator._has_news_today("XYZ") is False


@pytest.mark.asyncio
async def test_source_exception_does_not_break_other_sources():
    indicator = InNewsIndicator()
    codex = AsyncMock(return_value=[])
    with patch.object(
        indicator,
        "_finviz_headlines",
        new=AsyncMock(side_effect=RuntimeError("network")),
    ), patch.object(
        indicator, "_finnhub_headlines", new=AsyncMock(return_value=["Acme rises 10%"])
    ), patch.object(
        indicator, "_yahoo_headlines", new=AsyncMock(return_value=[])
    ), patch.object(
        indicator, "_seeking_alpha_headlines", new=AsyncMock(return_value=[])
    ), patch.object(
        indicator, "_marketwatch_headlines", new=AsyncMock(return_value=[])
    ), patch.object(
        indicator, "_filter_with_codex", new=codex
    ):
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


def test_parse_codex_response_handles_non_ascii_surrounding_prose():
    text = (
        "分析結果:\n"
        '[{"idx":0,"confidence":91},{"idx":1,"confidence":8}]\n'
        "Résultat: terminé."
    )
    out = InNewsIndicator._parse_codex_response(text, n_headlines=2)
    assert out == [{"idx": 0, "confidence": 91}, {"idx": 1, "confidence": 8}]


def test_parse_codex_response_raises_when_all_entries_invalid():
    text = '[{"idx":99,"confidence":50},{"idx":"bad","confidence":50}]'
    with pytest.raises(RuntimeError, match="no valid entries"):
        InNewsIndicator._parse_codex_response(text, n_headlines=2)


# --- non-ASCII headlines ---


@pytest.mark.asyncio
async def test_non_ascii_headlines_flow_through_has_news_today():
    indicator = InNewsIndicator()
    non_ascii = [
        "Société Générale raises €50M price target on XYZ",
        "日本企業XYZがAI戦略を発表",
        "Ação da XYZ sobe após acordo de R$2bi",
    ]
    received: list[list[Headline]] = []

    async def _capture(symbol, headlines):
        received.append(list(headlines))
        return headlines[:1]

    with patch.object(
        indicator, "_finviz_headlines", new=AsyncMock(return_value=non_ascii)
    ), patch.object(
        indicator, "_finnhub_headlines", new=AsyncMock(return_value=[])
    ), patch.object(
        indicator, "_yahoo_headlines", new=AsyncMock(return_value=[])
    ), patch.object(
        indicator, "_seeking_alpha_headlines", new=AsyncMock(return_value=[])
    ), patch.object(
        indicator, "_marketwatch_headlines", new=AsyncMock(return_value=[])
    ), patch.object(
        indicator, "_filter_with_codex", new=AsyncMock(side_effect=_capture)
    ):
        result = await indicator._has_news_today("XYZ")

    assert result is True
    assert [h.title for h in received[0]] == non_ascii


# --- cache key ---


def test_cache_key_format():
    assert InNewsIndicator._cache_key("AAPL") == "indicator:in_news:AAPL"


# --- registry ---


def test_indicator_is_registered_in_library():
    from fastscanner.services.indicators.lib import IndicatorsLibrary

    library = IndicatorsLibrary.instance()
    indicator = library.get("in_news", {})
    assert isinstance(indicator, InNewsIndicator)
    assert indicator.column_name() == "in_news"


def test_indicator_registered_with_caching_true():
    from fastscanner.services.indicators.lib import IndicatorsLibrary

    library = IndicatorsLibrary.instance()
    indicator = library.get("in_news", {"caching": True})
    assert isinstance(indicator, InNewsIndicator)
    assert indicator._caching is True
