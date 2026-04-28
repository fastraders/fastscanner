from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from fastscanner.pkg.candle import Candle
from fastscanner.services.indicators.lib.news import EST, InNewsIndicator


def _make_candle(ts: datetime | None = None) -> Candle:
    ts = ts or datetime(2026, 4, 28, 10, 30, tzinfo=EST)
    return Candle(
        {"open": 1.0, "high": 1.1, "low": 0.9, "close": 1.05, "volume": 1000},
        timestamp=pd.Timestamp(ts),
    )


@pytest.mark.asyncio
async def test_extend_returns_na_column():
    indicator = InNewsIndicator()
    df = pd.DataFrame(
        {"close": [1.0, 2.0]},
        index=pd.to_datetime(["2026-04-28 10:00", "2026-04-28 10:01"]),
    )
    out = await indicator.extend("AAPL", df)
    col = out[indicator.column_name()]
    assert col.isna().all()


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


def test_finviz_news_table_has_date_match():
    today = datetime.now(EST).strftime("%Y-%m-%d")
    target = datetime.strptime(today, "%Y-%m-%d").strftime("%b-%d-%y")
    html = f"""
    <html><body>
      <table id="news-table">
        <tr>
          <td width="130">{target} 09:30AM&nbsp;</td>
          <td><a class="tab-link-news" href="/news/x">A headline</a></td>
        </tr>
      </table>
    </body></html>
    """
    assert InNewsIndicator._finviz_news_table_has_date(html, today) is True


def test_finviz_news_table_has_date_no_match():
    today = "2026-04-28"
    html = """
    <html><body>
      <table id="news-table">
        <tr>
          <td width="130">Apr-27-26 09:30AM&nbsp;</td>
          <td><a class="tab-link-news" href="/news/x">Yesterday headline</a></td>
        </tr>
      </table>
    </body></html>
    """
    assert InNewsIndicator._finviz_news_table_has_date(html, today) is False


def test_finviz_news_table_missing_table_returns_false():
    assert (
        InNewsIndicator._finviz_news_table_has_date("<html></html>", "2026-04-28")
        is False
    )


def test_dt_is_on_date_handles_naive_utc():
    naive = datetime(2026, 4, 28, 14, 30)  # 10:30 EDT
    assert InNewsIndicator._dt_is_on_date(naive, "2026-04-28") is True


def test_dt_is_on_date_handles_timezone_boundary():
    # 03:00 UTC on Apr 28 == 23:00 EDT on Apr 27
    dt = datetime(2026, 4, 28, 3, 0, tzinfo=timezone.utc)
    assert InNewsIndicator._dt_is_on_date(dt, "2026-04-27") is True
    assert InNewsIndicator._dt_is_on_date(dt, "2026-04-28") is False


@pytest.mark.asyncio
async def test_has_news_today_returns_true_on_first_source_hit():
    indicator = InNewsIndicator()
    with patch.object(
        indicator, "_finviz_has_news", new=AsyncMock(return_value=True)
    ) as finviz, patch.object(
        indicator, "_finnhub_has_news", new=AsyncMock(return_value=False)
    ) as finnhub:
        result = await indicator._has_news_today("AAPL")
        assert result is True
        finviz.assert_awaited_once()
        finnhub.assert_not_awaited()


@pytest.mark.asyncio
async def test_has_news_today_swallows_source_errors():
    indicator = InNewsIndicator()
    with patch.object(
        indicator, "_finviz_has_news", new=AsyncMock(side_effect=RuntimeError("boom"))
    ), patch.object(
        indicator, "_finnhub_has_news", new=AsyncMock(return_value=True)
    ):
        assert await indicator._has_news_today("AAPL") is True


@pytest.mark.asyncio
async def test_has_news_today_returns_false_when_all_sources_fail():
    indicator = InNewsIndicator()
    with patch.object(
        indicator, "_finviz_has_news", new=AsyncMock(side_effect=RuntimeError("a"))
    ), patch.object(
        indicator, "_finnhub_has_news", new=AsyncMock(side_effect=RuntimeError("b"))
    ), patch.object(
        indicator, "_yahoo_has_news", new=AsyncMock(return_value=False)
    ), patch.object(
        indicator, "_seeking_alpha_has_news", new=AsyncMock(return_value=False)
    ), patch.object(
        indicator, "_marketwatch_has_news", new=AsyncMock(return_value=False)
    ):
        assert await indicator._has_news_today("AAPL") is False


def test_indicator_is_registered_in_library():
    from fastscanner.services.indicators.lib import IndicatorsLibrary

    library = IndicatorsLibrary.instance()
    indicator = library.get("in_news", {})
    assert isinstance(indicator, InNewsIndicator)
    assert indicator.column_name() == "in_news"
