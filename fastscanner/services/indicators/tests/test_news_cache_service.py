import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from fastscanner.services.indicators.news_cache_service import NewsCacheService


class _MockChannel:
    def __init__(self):
        self._handlers: dict[str, object] = {}

    async def subscribe(self, channel_id: str, handler) -> None:
        self._handlers[channel_id] = handler

    async def unsubscribe(self, channel_id: str, handler_id: str) -> None:
        self._handlers.pop(channel_id, None)

    async def push(self, channel_id: str, data: dict, flush: bool = True) -> None:
        pass

    async def flush(self) -> None:
        pass

    async def reset(self) -> None:
        pass

    def get_handler(self, channel_id: str):
        return self._handlers.get(channel_id)


class _MockCache:
    def __init__(self):
        self.saved: dict[str, str] = {}

    async def save(self, key: str, value: str) -> None:
        self.saved[key] = value

    async def get(self, key: str) -> str:
        if key not in self.saved:
            raise KeyError(key)
        return self.saved[key]


def _make_service(channel=None, cache=None) -> NewsCacheService:
    return NewsCacheService(
        channel=channel or _MockChannel(),
        cache=cache or _MockCache(),
        news_subscribe_channel="test_news_sub",
        news_unsubscribe_channel="test_news_unsub",
        fetch_interval_seconds=60,
        jitter_max_seconds=0.0,
    )


# --- symbol tracking (idempotent via subscriber_id) ---


def test_add_symbol_tracks_subscriber():
    svc = _make_service()
    svc._add_symbol("sub-1", "AAPL")
    assert "AAPL" in svc._symbols
    assert svc._subscribers["sub-1"] == "AAPL"


def test_add_symbol_idempotent_same_id():
    svc = _make_service()
    svc._add_symbol("sub-1", "AAPL")
    svc._add_symbol("sub-1", "AAPL")
    assert list(svc._subscribers.values()).count("AAPL") == 1


def test_remove_symbol_clears_when_no_remaining_subscribers():
    svc = _make_service()
    svc._add_symbol("sub-1", "AAPL")
    svc._remove_symbol("sub-1")
    assert "AAPL" not in svc._symbols


def test_remove_symbol_keeps_active_while_other_subscriber_exists():
    svc = _make_service()
    svc._add_symbol("sub-1", "AAPL")
    svc._add_symbol("sub-2", "AAPL")
    svc._remove_symbol("sub-1")
    assert "AAPL" in svc._symbols


def test_remove_unknown_subscriber_is_noop():
    svc = _make_service()
    svc._remove_symbol("ghost-id")  # must not raise


# --- _fetch_one ---


@pytest.mark.asyncio
async def test_fetch_one_writes_true_when_news_found():
    cache = _MockCache()
    svc = _make_service(cache=cache)
    with patch.object(svc._indicator, "_has_news_today", new=AsyncMock(return_value=True)):
        await svc._fetch_one("AAPL")
    assert cache.saved.get("indicator:in_news:AAPL") == "true"


@pytest.mark.asyncio
async def test_fetch_one_writes_false_when_no_news():
    cache = _MockCache()
    svc = _make_service(cache=cache)
    with patch.object(svc._indicator, "_has_news_today", new=AsyncMock(return_value=False)):
        await svc._fetch_one("MSFT")
    assert cache.saved.get("indicator:in_news:MSFT") == "false"


@pytest.mark.asyncio
async def test_fetch_one_exception_does_not_propagate():
    svc = _make_service()
    with patch.object(
        svc._indicator, "_has_news_today", new=AsyncMock(side_effect=RuntimeError("boom"))
    ):
        await svc._fetch_one("AAPL")  # must not raise


# --- full tick: multiple symbols ---


@pytest.mark.asyncio
async def test_tick_fetches_all_symbols():
    cache = _MockCache()
    svc = _make_service(cache=cache)
    svc._add_symbol("sub-1", "AAPL")
    svc._add_symbol("sub-2", "MSFT")

    results = {"AAPL": True, "MSFT": False}

    async def _fetch(symbol):
        return results[symbol]

    with patch.object(svc._indicator, "_has_news_today", new=AsyncMock(side_effect=_fetch)):
        await asyncio.gather(
            *(svc._fetch_one(sym) for sym in sorted(svc._symbols)),
            return_exceptions=True,
        )

    assert cache.saved["indicator:in_news:AAPL"] == "true"
    assert cache.saved["indicator:in_news:MSFT"] == "false"


# --- NATS subscribe / unsubscribe handlers ---


@pytest.mark.asyncio
async def test_subscribe_handler_adds_symbol_for_in_news():
    channel = _MockChannel()
    svc = _make_service(channel=channel)
    await svc.start()
    svc._loop_task.cancel()

    handler = channel.get_handler("test_news_sub")
    assert handler is not None
    await handler.handle("test_news_sub", {
        "symbol": "AAPL",
        "subscriber_id": "sub-1",
        "unit": "min",
        "indicator_types": ["in_news"],
    })
    assert "AAPL" in svc._symbols


@pytest.mark.asyncio
async def test_subscribe_handler_ignores_non_news_types():
    channel = _MockChannel()
    svc = _make_service(channel=channel)
    await svc.start()
    svc._loop_task.cancel()

    handler = channel.get_handler("test_news_sub")
    await handler.handle("test_news_sub", {
        "symbol": "AAPL",
        "subscriber_id": "sub-1",
        "unit": "min",
        "indicator_types": ["day_open"],
    })
    assert "AAPL" not in svc._symbols


@pytest.mark.asyncio
async def test_unsubscribe_handler_removes_symbol():
    channel = _MockChannel()
    svc = _make_service(channel=channel)
    await svc.start()
    svc._loop_task.cancel()

    svc._add_symbol("sub-1", "AAPL")
    handler = channel.get_handler("test_news_unsub")
    await handler.handle("test_news_unsub", {
        "symbol": "AAPL",
        "subscriber_id": "sub-1",
        "unit": "min",
    })
    assert "AAPL" not in svc._symbols


@pytest.mark.asyncio
async def test_subscribe_same_subscriber_id_is_idempotent():
    channel = _MockChannel()
    svc = _make_service(channel=channel)
    await svc.start()
    svc._loop_task.cancel()

    handler = channel.get_handler("test_news_sub")
    msg = {"symbol": "AAPL", "subscriber_id": "sub-1", "unit": "min", "indicator_types": ["in_news"]}
    await handler.handle("test_news_sub", msg)
    await handler.handle("test_news_sub", msg)
    assert list(svc._subscribers.values()).count("AAPL") == 1
