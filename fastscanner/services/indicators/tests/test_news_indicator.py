import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from fastscanner.pkg.candle import Candle
from fastscanner.pkg.clock import ClockRegistry, FixedClock
from fastscanner.services.indicators.lib.news import (
    EST,
    Headline,
    NewsConfidenceIndicator,
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


@pytest.fixture(autouse=True)
def _fixed_clock():
    """Pin ClockRegistry to 2026-04-29 EST so producer paths that read
    today's date (cache encoding, source date string) are deterministic."""
    prev = ClockRegistry.clock if ClockRegistry.is_set() else None
    ClockRegistry.set(FixedClock(datetime(2026, 4, 29, 10, 30, tzinfo=EST)))
    yield
    if prev is None:
        ClockRegistry.unset()
    else:
        ClockRegistry.clock = prev


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


def _payload(date_str: str, value: int | None) -> str:
    return json.dumps({"date": date_str, "value": value})


# --- extend ---


@pytest.mark.asyncio
async def test_extend_returns_na_column():
    indicator = NewsConfidenceIndicator()
    df = pd.DataFrame(
        {"close": [1.0, 2.0]},
        index=pd.to_datetime(["2026-04-29 10:00", "2026-04-29 10:01"]),
    )
    out = await indicator.extend("AAPL", df)
    assert out[indicator.column_name()].isna().all()


# --- cache encode / decode ---


def test_encode_cache_format():
    raw = NewsConfidenceIndicator._encode_cache(
        datetime(2026, 4, 29).date(), 73
    )
    assert json.loads(raw) == {"date": "2026-04-29", "value": 73}


def test_encode_cache_with_none_value():
    raw = NewsConfidenceIndicator._encode_cache(
        datetime(2026, 4, 29).date(), None
    )
    assert json.loads(raw) == {"date": "2026-04-29", "value": None}


def test_decode_cache_returns_value_for_today():
    today = datetime(2026, 4, 29).date()
    raw = _payload("2026-04-29", 87)
    assert NewsConfidenceIndicator._decode_cache(raw, today) == 87


def test_decode_cache_returns_none_for_stale_date():
    today = datetime(2026, 4, 30).date()
    raw = _payload("2026-04-29", 87)
    assert NewsConfidenceIndicator._decode_cache(raw, today) is None


def test_decode_cache_returns_none_for_null_value():
    today = datetime(2026, 4, 29).date()
    raw = _payload("2026-04-29", None)
    assert NewsConfidenceIndicator._decode_cache(raw, today) is None


def test_decode_cache_returns_none_for_malformed_json():
    today = datetime(2026, 4, 29).date()
    assert NewsConfidenceIndicator._decode_cache("not json", today) is None
    assert NewsConfidenceIndicator._decode_cache("[1,2,3]", today) is None


def test_decode_cache_returns_none_for_out_of_range():
    today = datetime(2026, 4, 29).date()
    assert NewsConfidenceIndicator._decode_cache(_payload("2026-04-29", -1), today) is None
    assert NewsConfidenceIndicator._decode_cache(_payload("2026-04-29", 101), today) is None


def test_decode_cache_zero_is_real_signal():
    today = datetime(2026, 4, 29).date()
    assert NewsConfidenceIndicator._decode_cache(_payload("2026-04-29", 0), today) == 0


# --- max_merge helper ---


def test_max_merge_both_none():
    assert NewsConfidenceIndicator._max_merge(None, None) is None


def test_max_merge_one_none():
    assert NewsConfidenceIndicator._max_merge(None, 42) == 42
    assert NewsConfidenceIndicator._max_merge(42, None) == 42


def test_max_merge_keeps_higher():
    assert NewsConfidenceIndicator._max_merge(80, 60) == 80
    assert NewsConfidenceIndicator._max_merge(60, 80) == 80


# --- consumer mode (caching=False) ---


@pytest.mark.asyncio
async def test_consumer_reads_cached_score():
    indicator = NewsConfidenceIndicator()
    mock_cache = AsyncMock()
    mock_cache.get = AsyncMock(return_value=_payload("2026-04-29", 85))
    ApplicationRegistry.cache = mock_cache
    candle = await indicator.extend_realtime("AAPL", _make_candle())
    assert candle[indicator.column_name()] == 85
    assert indicator._confidence_today["AAPL"] == 85


@pytest.mark.asyncio
async def test_consumer_cache_miss_returns_none():
    indicator = NewsConfidenceIndicator()
    mock_cache = AsyncMock()
    mock_cache.get = AsyncMock(side_effect=KeyError("miss"))
    ApplicationRegistry.cache = mock_cache
    candle = await indicator.extend_realtime("AAPL", _make_candle())
    assert candle[indicator.column_name()] is None
    assert indicator._confidence_today.get("AAPL") is None


@pytest.mark.asyncio
async def test_consumer_stale_date_returns_none():
    indicator = NewsConfidenceIndicator()
    mock_cache = AsyncMock()
    # Yesterday's payload still in cache; consumer should ignore it.
    mock_cache.get = AsyncMock(return_value=_payload("2026-04-28", 90))
    ApplicationRegistry.cache = mock_cache
    candle = await indicator.extend_realtime("AAPL", _make_candle())
    assert candle[indicator.column_name()] is None


@pytest.mark.asyncio
async def test_consumer_zero_is_returned_as_zero():
    indicator = NewsConfidenceIndicator()
    mock_cache = AsyncMock()
    mock_cache.get = AsyncMock(return_value=_payload("2026-04-29", 0))
    ApplicationRegistry.cache = mock_cache
    candle = await indicator.extend_realtime("AAPL", _make_candle())
    assert candle[indicator.column_name()] == 0
    assert indicator._confidence_today["AAPL"] == 0


@pytest.mark.asyncio
async def test_consumer_max_merges_with_cache_each_tick():
    """Consumer must NOT freeze at first non-None value; it tracks producer's
    running max as the producer ratchets up over the day."""
    indicator = NewsConfidenceIndicator()
    mock_cache = AsyncMock()
    seq = iter([
        _payload("2026-04-29", 20),  # tick 1: producer at 20
        _payload("2026-04-29", 80),  # tick 2: producer ratcheted to 80
    ])
    mock_cache.get = AsyncMock(side_effect=lambda _k: next(seq))
    ApplicationRegistry.cache = mock_cache

    c1 = await indicator.extend_realtime("AAPL", _make_candle())
    c2 = await indicator.extend_realtime("AAPL", _make_candle())
    assert c1[indicator.column_name()] == 20
    assert c2[indicator.column_name()] == 80
    assert indicator._confidence_today["AAPL"] == 80


@pytest.mark.asyncio
async def test_consumer_does_not_downgrade_when_cache_drops():
    """If cache somehow returns a lower value than in-memory has, consumer
    keeps the higher value (max-merge)."""
    indicator = NewsConfidenceIndicator()
    mock_cache = AsyncMock()
    seq = iter([
        _payload("2026-04-29", 80),
        _payload("2026-04-29", 30),  # would be a regression — must be ignored
    ])
    mock_cache.get = AsyncMock(side_effect=lambda _k: next(seq))
    ApplicationRegistry.cache = mock_cache

    await indicator.extend_realtime("AAPL", _make_candle())
    c2 = await indicator.extend_realtime("AAPL", _make_candle())
    assert c2[indicator.column_name()] == 80


@pytest.mark.asyncio
async def test_consumer_does_not_call_score_new_headlines_today():
    indicator = NewsConfidenceIndicator()
    mock_cache = AsyncMock()
    mock_cache.get = AsyncMock(side_effect=KeyError("miss"))
    scrape = AsyncMock(return_value=42)
    ApplicationRegistry.cache = mock_cache
    with patch.object(indicator, "_score_new_headlines_today", new=scrape):
        await indicator.extend_realtime("AAPL", _make_candle())
    scrape.assert_not_called()


@pytest.mark.asyncio
async def test_per_symbol_cache_isolation():
    indicator = NewsConfidenceIndicator()
    mock_cache = AsyncMock()

    async def _get(key: str) -> str:
        if "AAPL" in key:
            return _payload("2026-04-29", 75)
        raise KeyError("miss")

    mock_cache.get = _get
    ApplicationRegistry.cache = mock_cache
    a = await indicator.extend_realtime("AAPL", _make_candle())
    m = await indicator.extend_realtime("MSFT", _make_candle())
    assert a[indicator.column_name()] == 75
    assert m[indicator.column_name()] is None


# --- day boundary reset ---


@pytest.mark.asyncio
async def test_day_boundary_resets_state():
    indicator = NewsConfidenceIndicator()
    mock_cache = AsyncMock()

    async def _get(_key: str) -> str:
        # Producer always wrote the matching day's payload before consumer ran.
        # On day 1 candle date is 2026-04-29; on day 2 it's 2026-04-30.
        if indicator._date.get("AAPL") == datetime(2026, 4, 29).date():
            return _payload("2026-04-29", 70)
        return _payload("2026-04-30", 40)

    mock_cache.get = _get
    ApplicationRegistry.cache = mock_cache

    c1 = await indicator.extend_realtime("AAPL", _make_candle_on_date(2026, 4, 29))
    assert c1[indicator.column_name()] == 70
    assert indicator._confidence_today["AAPL"] == 70

    # Day 2: in-memory state and seen-headlines reset; reads fresh value.
    c2 = await indicator.extend_realtime("AAPL", _make_candle_on_date(2026, 4, 30))
    assert c2[indicator.column_name()] == 40
    assert indicator._confidence_today["AAPL"] == 40


@pytest.mark.asyncio
async def test_day_boundary_invalidates_stale_cache():
    """If on day N+1 morning the cache still holds yesterday's payload (producer
    hasn't re-fetched yet), consumer must NOT serve yesterday's score."""
    indicator = NewsConfidenceIndicator()
    mock_cache = AsyncMock()
    mock_cache.get = AsyncMock(return_value=_payload("2026-04-29", 95))
    ApplicationRegistry.cache = mock_cache

    # Candle is on 2026-04-30; cache still has 2026-04-29 payload.
    c = await indicator.extend_realtime("AAPL", _make_candle_on_date(2026, 4, 30))
    assert c[indicator.column_name()] is None


# --- producer mode (caching=True) ---


@pytest.mark.asyncio
async def test_producer_fire_and_forget_does_not_block():
    indicator = NewsConfidenceIndicator(caching=True)
    mock_cache = AsyncMock()
    mock_cache.save = AsyncMock()

    async def _slow_score(_symbol: str) -> int:
        import asyncio
        await asyncio.sleep(0.05)
        return 87

    ApplicationRegistry.cache = mock_cache
    with patch.object(
        indicator,
        "_score_new_headlines_today",
        new=AsyncMock(side_effect=_slow_score),
    ), patch(
        "fastscanner.services.indicators.lib.news.random.uniform", return_value=0
    ):
        candle = await indicator.extend_realtime("AAPL", _make_candle())
        # Returns immediately (None until background task finishes).
        assert candle[indicator.column_name()] is None

        import asyncio
        await asyncio.sleep(0.1)

    assert indicator._confidence_today.get("AAPL") == 87
    mock_cache.save.assert_called_once()
    saved_key, saved_value = mock_cache.save.call_args.args
    assert saved_key == NewsConfidenceIndicator._cache_key("AAPL")
    assert json.loads(saved_value)["value"] == 87


@pytest.mark.asyncio
async def test_producer_dedup_spawns_one_task_per_symbol():
    indicator = NewsConfidenceIndicator(caching=True)
    mock_cache = AsyncMock()
    mock_cache.save = AsyncMock()
    score = AsyncMock(return_value=0)

    ApplicationRegistry.cache = mock_cache
    with patch.object(
        indicator, "_score_new_headlines_today", new=score
    ), patch(
        "fastscanner.services.indicators.lib.news.random.uniform", return_value=0
    ):
        import asyncio
        for _ in range(5):
            await indicator.extend_realtime("AAPL", _make_candle())
        await asyncio.sleep(0.05)

    assert score.await_count == 1


@pytest.mark.asyncio
async def test_producer_running_max_across_multi_tick():
    """Producer must keep the highest score seen today even if subsequent
    Codex calls return lower scores (because _seen_headlines dedupes earlier
    headlines from being re-scored)."""
    indicator = NewsConfidenceIndicator(caching=True)
    mock_cache = AsyncMock()
    mock_cache.save = AsyncMock()

    # Three consecutive runs return: 60 (initial), 30 (lower new headlines), None (codex blip).
    seq = iter([60, 30, None])
    score = AsyncMock(side_effect=lambda _s: next(seq))

    ApplicationRegistry.cache = mock_cache
    with patch.object(
        indicator, "_score_new_headlines_today", new=score
    ), patch(
        "fastscanner.services.indicators.lib.news.random.uniform", return_value=0
    ):
        await indicator._inline_fetch("AAPL")
        await indicator._inline_fetch("AAPL")
        await indicator._inline_fetch("AAPL")

    assert indicator._confidence_today["AAPL"] == 60
    # Three writes; final one must encode 60.
    final_value = mock_cache.save.call_args_list[-1].args[1]
    assert json.loads(final_value)["value"] == 60


@pytest.mark.asyncio
async def test_producer_writes_null_when_codex_fails_with_no_prior():
    """On the very first Codex failure with no prior in-memory value, producer
    writes today's payload with value=null (so day-N+1 reads correctly stale)."""
    indicator = NewsConfidenceIndicator(caching=True)
    mock_cache = AsyncMock()
    mock_cache.save = AsyncMock()
    score = AsyncMock(return_value=None)

    ApplicationRegistry.cache = mock_cache
    with patch.object(
        indicator, "_score_new_headlines_today", new=score
    ), patch(
        "fastscanner.services.indicators.lib.news.random.uniform", return_value=0
    ):
        await indicator._inline_fetch("AAPL")

    assert indicator._confidence_today.get("AAPL") is None
    saved_value = mock_cache.save.call_args.args[1]
    assert json.loads(saved_value)["value"] is None


# --- _score_new_headlines_today ---


@pytest.mark.asyncio
async def test_score_returns_zero_when_no_headlines():
    indicator = NewsConfidenceIndicator()
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
        assert await indicator._score_new_headlines_today("AAPL") == 0


@pytest.mark.asyncio
async def test_score_returns_max_across_kept_and_dropped():
    """The _score_new_headlines_today contract is 'max across all today's
    headlines,' not just those above CONFIDENCE_THRESHOLD."""
    indicator = NewsConfidenceIndicator()
    scored = [
        (Headline(title="a", source="finviz"), 10),
        (Headline(title="b", source="finviz"), 30),
        (Headline(title="c", source="finviz"), 60),
    ]
    with patch.object(
        indicator,
        "_finviz_headlines",
        new=AsyncMock(return_value=["a", "b", "c"]),
    ), patch.object(
        indicator, "_finnhub_headlines", new=AsyncMock(return_value=[])
    ), patch.object(
        indicator, "_yahoo_headlines", new=AsyncMock(return_value=[])
    ), patch.object(
        indicator, "_seeking_alpha_headlines", new=AsyncMock(return_value=[])
    ), patch.object(
        indicator, "_marketwatch_headlines", new=AsyncMock(return_value=[])
    ), patch.object(
        indicator, "_score_with_codex", new=AsyncMock(return_value=scored)
    ):
        result = await indicator._score_new_headlines_today("XYZ")
    assert result == 60


@pytest.mark.asyncio
async def test_score_returns_none_on_codex_failure():
    indicator = NewsConfidenceIndicator()
    fail = AsyncMock(side_effect=RuntimeError("codex blew up"))
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
        indicator, "_score_with_codex", new=fail
    ):
        assert await indicator._score_new_headlines_today("XYZ") is None
        fail.assert_awaited_once()


@pytest.mark.asyncio
async def test_score_returns_none_when_codex_cli_missing():
    indicator = NewsConfidenceIndicator()
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
        assert await indicator._score_new_headlines_today("XYZ") is None


@pytest.mark.asyncio
async def test_source_exception_does_not_break_other_sources():
    indicator = NewsConfidenceIndicator()
    scored = [(Headline(title="Acme rises 10%", source="finnhub"), 72)]
    with patch.object(
        indicator,
        "_finviz_headlines",
        new=AsyncMock(side_effect=RuntimeError("network")),
    ), patch.object(
        indicator,
        "_finnhub_headlines",
        new=AsyncMock(return_value=["Acme rises 10%"]),
    ), patch.object(
        indicator, "_yahoo_headlines", new=AsyncMock(return_value=[])
    ), patch.object(
        indicator, "_seeking_alpha_headlines", new=AsyncMock(return_value=[])
    ), patch.object(
        indicator, "_marketwatch_headlines", new=AsyncMock(return_value=[])
    ), patch.object(
        indicator, "_score_with_codex", new=AsyncMock(return_value=scored)
    ):
        result = await indicator._score_new_headlines_today("XYZ")
    assert result == 72


# --- round-robin truncation (source-fair sampling) ---


def test_round_robin_evenly_distributes_under_cap():
    per_source = [
        ("finviz", ["a1", "a2"]),
        ("finnhub", ["b1", "b2"]),
        ("yahoo", ["c1"]),
    ]
    out = NewsConfidenceIndicator._round_robin_truncate(per_source, max_total=10)
    titles = [h.title for h in out]
    assert titles == ["a1", "b1", "c1", "a2", "b2"]


def test_round_robin_caps_at_max_total():
    per_source = [
        ("finviz", ["a1", "a2", "a3", "a4"]),
        ("finnhub", ["b1", "b2", "b3", "b4"]),
    ]
    out = NewsConfidenceIndicator._round_robin_truncate(per_source, max_total=3)
    assert [h.title for h in out] == ["a1", "b1", "a2"]


def test_round_robin_does_not_starve_when_one_source_dominates():
    """If finviz returns 60 and finnhub returns 5, finnhub still gets sampled
    (this is the bug Codex flagged in the original [:50] truncation)."""
    per_source = [
        ("finviz", [f"f{i}" for i in range(60)]),
        ("finnhub", [f"n{i}" for i in range(5)]),
    ]
    out = NewsConfidenceIndicator._round_robin_truncate(per_source, max_total=50)
    sources = [h.source for h in out]
    assert sources.count("finnhub") == 5
    assert sources.count("finviz") == 45


def test_round_robin_handles_empty():
    assert NewsConfidenceIndicator._round_robin_truncate([], max_total=10) == []
    assert NewsConfidenceIndicator._round_robin_truncate(
        [("finviz", [])], max_total=10
    ) == []


def test_round_robin_handles_zero_cap():
    per_source = [("finviz", ["a1"])]
    assert NewsConfidenceIndicator._round_robin_truncate(per_source, max_total=0) == []


# --- _score_with_codex returns ALL scored, not just kept ---


@pytest.mark.asyncio
async def test_score_with_codex_returns_every_headline():
    """The helper now returns all (Headline, confidence) pairs; threshold-based
    filtering is the caller's responsibility (max accumulation in this codebase)."""
    indicator = NewsConfidenceIndicator()
    headlines = [
        Headline(title="x", source="finviz"),
        Headline(title="y", source="finviz"),
        Headline(title="z", source="finviz"),
    ]

    async def fake_proc_communicate():
        text = '[{"idx":0,"confidence":10},{"idx":1,"confidence":80},{"idx":2,"confidence":40}]'
        return text.encode(), b""

    class FakeProc:
        returncode = 0

        async def communicate(self):
            return await fake_proc_communicate()

    with patch(
        "fastscanner.services.indicators.lib.news.shutil.which",
        return_value="/usr/bin/codex",
    ), patch(
        "fastscanner.services.indicators.lib.news.asyncio.create_subprocess_exec",
        new=AsyncMock(return_value=FakeProc()),
    ):
        out = await indicator._score_with_codex("XYZ", headlines)

    assert len(out) == 3
    assert [conf for _h, conf in out] == [10, 80, 40]
    # All three returned, regardless of CONFIDENCE_THRESHOLD = 50.


# --- non-ASCII headlines ---


@pytest.mark.asyncio
async def test_non_ascii_headlines_flow_through():
    indicator = NewsConfidenceIndicator()
    non_ascii = [
        "Société Générale raises €50M price target on XYZ",
        "日本企業XYZがAI戦略を発表",
        "Ação da XYZ sobe após acordo de R$2bi",
    ]
    received: list[list[Headline]] = []

    async def _capture(symbol, headlines):
        received.append(list(headlines))
        return [(h, 50 + i * 10) for i, h in enumerate(headlines)]

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
        indicator, "_score_with_codex", new=AsyncMock(side_effect=_capture)
    ):
        result = await indicator._score_new_headlines_today("XYZ")

    assert result == 70  # max of [50, 60, 70]
    assert [h.title for h in received[0]] == non_ascii


# --- Finviz HTML parser (unchanged) ---


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
    titles = NewsConfidenceIndicator._finviz_news_table_extract(html, today)
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
    assert NewsConfidenceIndicator._finviz_news_table_extract(html, "2026-04-29") == []


def test_finviz_extract_returns_empty_when_table_missing():
    assert (
        NewsConfidenceIndicator._finviz_news_table_extract("<html></html>", "2026-04-29")
        == []
    )


# --- date helpers (unchanged) ---


def test_dt_is_on_date_handles_naive_utc():
    naive = datetime(2026, 4, 29, 14, 30)  # 10:30 EDT
    assert NewsConfidenceIndicator._dt_is_on_date(naive, "2026-04-29") is True


def test_dt_is_on_date_handles_timezone_boundary():
    # 03:00 UTC on Apr 29 == 23:00 EDT on Apr 28
    dt = datetime(2026, 4, 29, 3, 0, tzinfo=timezone.utc)
    assert NewsConfidenceIndicator._dt_is_on_date(dt, "2026-04-28") is True
    assert NewsConfidenceIndicator._dt_is_on_date(dt, "2026-04-29") is False


# --- Codex response parser (unchanged) ---


def test_parse_codex_response_handles_prose_around_json():
    text = (
        "Here is the analysis you asked for:\n\n"
        '[{"idx":0,"confidence":87},{"idx":1,"confidence":12}]\n\n'
        "Hope that helps!"
    )
    out = NewsConfidenceIndicator._parse_codex_response(text, n_headlines=2)
    assert out == [
        {"idx": 0, "confidence": 87},
        {"idx": 1, "confidence": 12},
    ]


def test_parse_codex_response_raises_on_no_array():
    with pytest.raises(RuntimeError, match="no JSON array"):
        NewsConfidenceIndicator._parse_codex_response(
            "just prose, no JSON here", n_headlines=2
        )


def test_parse_codex_response_drops_invalid_entries():
    text = (
        '[{"idx":0,"confidence":99},'
        '{"idx":"bad","confidence":50},'
        '{"idx":1,"confidence":150},'
        '{"idx":99,"confidence":50},'
        '{"idx":2,"confidence":-5},'
        '{"idx":3,"confidence":50}]'
    )
    out = NewsConfidenceIndicator._parse_codex_response(text, n_headlines=4)
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
    out = NewsConfidenceIndicator._parse_codex_response(text, n_headlines=2)
    assert out == [{"idx": 0, "confidence": 91}, {"idx": 1, "confidence": 8}]


def test_parse_codex_response_raises_when_all_entries_invalid():
    text = '[{"idx":99,"confidence":50},{"idx":"bad","confidence":50}]'
    with pytest.raises(RuntimeError, match="no valid entries"):
        NewsConfidenceIndicator._parse_codex_response(text, n_headlines=2)


# --- cache key + registry ---


def test_cache_key_format():
    assert (
        NewsConfidenceIndicator._cache_key("AAPL")
        == "indicator:news_confidence:AAPL"
    )


def test_indicator_is_registered_in_library():
    from fastscanner.services.indicators.lib import IndicatorsLibrary

    library = IndicatorsLibrary.instance()
    indicator = library.get("news_confidence", {})
    assert isinstance(indicator, NewsConfidenceIndicator)
    assert indicator.column_name() == "news_confidence"


def test_indicator_registered_with_caching_true():
    from fastscanner.services.indicators.lib import IndicatorsLibrary

    library = IndicatorsLibrary.instance()
    indicator = library.get("news_confidence", {"caching": True})
    assert isinstance(indicator, NewsConfidenceIndicator)
    assert indicator._caching is True
