import pytest

from fastscanner.services.indicators.slow_indicators_service import (
    SlowIndicatorsService,
)


class _RecordingIndicator:
    """Minimal Indicator stub. Records every (symbol, timestamp_ms) it sees."""

    def __init__(self, type_: str, *, raises: bool = False) -> None:
        self._type = type_
        self.calls: list[tuple[str, int]] = []
        self._raises = raises

    @classmethod
    def type(cls) -> str:  # pragma: no cover (overridden by instance for tests)
        return "recording"

    def column_name(self) -> str:
        return self._type

    async def extend(self, symbol, df):  # pragma: no cover - not used
        return df

    async def extend_realtime(self, symbol, new_row):
        self.calls.append((symbol, int(new_row["timestamp"])))
        if self._raises:
            raise RuntimeError(f"{self._type} boom")
        return new_row


def _make_indicator(type_: str, *, raises: bool = False) -> _RecordingIndicator:
    ind = _RecordingIndicator(type_, raises=raises)
    # Bind type() to return this specific type — required by service to key by type.
    ind.type = lambda: type_  # type: ignore[method-assign]
    return ind


class _MockChannel:
    def __init__(self):
        self.subscriptions: dict[str, str] = {}  # channel_id → handler_id
        self.handlers: dict[str, object] = {}  # channel_id → handler
        self.subscribe_calls: list[str] = []
        self.unsubscribe_calls: list[tuple[str, str]] = []

    async def subscribe(self, channel_id: str, handler) -> None:
        self.subscriptions[channel_id] = handler.id()
        self.handlers[channel_id] = handler
        self.subscribe_calls.append(channel_id)

    async def unsubscribe(self, channel_id: str, handler_id: str) -> None:
        self.subscriptions.pop(channel_id, None)
        self.handlers.pop(channel_id, None)
        self.unsubscribe_calls.append((channel_id, handler_id))

    async def push(self, channel_id, data, flush=True): pass
    async def flush(self): pass
    async def reset(self): pass


def _make_service(channel=None, indicators=None):
    indicators = indicators or [_make_indicator("in_news"), _make_indicator("shares_float")]
    return (
        SlowIndicatorsService(
            channel=channel or _MockChannel(),
            indicators=indicators,
            subscribe_channel="slow_sub",
            unsubscribe_channel="slow_unsub",
        ),
        indicators,
    )


@pytest.mark.asyncio
async def test_start_subscribes_to_control_channels():
    channel = _MockChannel()
    svc, _ = _make_service(channel=channel)
    await svc.start()
    assert "slow_sub" in channel.handlers
    assert "slow_unsub" in channel.handlers


@pytest.mark.asyncio
async def test_subscribe_creates_candle_subscription_for_symbol():
    channel = _MockChannel()
    svc, _ = _make_service(channel=channel)
    await svc.start()

    sub = channel.handlers["slow_sub"]
    await sub.handle("slow_sub", {
        "subscriber_id": "s1",
        "symbol": "AAPL",
        "indicator_types": ["in_news"],
    })

    assert "candles.min.AAPL" in channel.handlers


@pytest.mark.asyncio
async def test_subscribe_filters_unknown_indicator_types():
    channel = _MockChannel()
    svc, _ = _make_service(channel=channel)
    await svc.start()

    sub = channel.handlers["slow_sub"]
    await sub.handle("slow_sub", {
        "subscriber_id": "s1",
        "symbol": "AAPL",
        "indicator_types": ["day_open", "atr"],  # neither is a slow indicator
    })

    assert "candles.min.AAPL" not in channel.handlers


@pytest.mark.asyncio
async def test_payload_with_mixed_known_and_unknown_types_uses_only_known():
    channel = _MockChannel()
    svc, _ = _make_service(channel=channel)
    await svc.start()

    sub = channel.handlers["slow_sub"]
    await sub.handle("slow_sub", {
        "subscriber_id": "s1",
        "symbol": "AAPL",
        "indicator_types": ["in_news", "atr"],
    })

    handler = channel.handlers["candles.min.AAPL"]
    assert [ind.type() for ind in handler._indicators] == ["in_news"]


@pytest.mark.asyncio
async def test_second_subscriber_same_type_does_not_recreate_handler():
    channel = _MockChannel()
    svc, _ = _make_service(channel=channel)
    await svc.start()

    sub = channel.handlers["slow_sub"]
    await sub.handle("slow_sub", {
        "subscriber_id": "s1", "symbol": "AAPL", "indicator_types": ["in_news"],
    })
    handler_v1 = channel.handlers["candles.min.AAPL"]

    await sub.handle("slow_sub", {
        "subscriber_id": "s2", "symbol": "AAPL", "indicator_types": ["in_news"],
    })
    handler_v2 = channel.handlers["candles.min.AAPL"]

    assert handler_v1 is handler_v2
    # Only one subscribe call for the candle stream
    assert channel.subscribe_calls.count("candles.min.AAPL") == 1


@pytest.mark.asyncio
async def test_adding_new_indicator_type_for_same_symbol_rebuilds_handler():
    channel = _MockChannel()
    svc, _ = _make_service(channel=channel)
    await svc.start()

    sub = channel.handlers["slow_sub"]
    await sub.handle("slow_sub", {
        "subscriber_id": "s1", "symbol": "AAPL", "indicator_types": ["in_news"],
    })
    await sub.handle("slow_sub", {
        "subscriber_id": "s2", "symbol": "AAPL", "indicator_types": ["shares_float"],
    })

    handler = channel.handlers["candles.min.AAPL"]
    types = sorted(ind.type() for ind in handler._indicators)
    assert types == ["in_news", "shares_float"]


@pytest.mark.asyncio
async def test_unsubscribe_keeps_handler_while_other_subscriber_remains():
    channel = _MockChannel()
    svc, _ = _make_service(channel=channel)
    await svc.start()

    sub = channel.handlers["slow_sub"]
    await sub.handle("slow_sub", {
        "subscriber_id": "s1", "symbol": "AAPL", "indicator_types": ["in_news"],
    })
    await sub.handle("slow_sub", {
        "subscriber_id": "s2", "symbol": "AAPL", "indicator_types": ["in_news"],
    })

    unsub = channel.handlers["slow_unsub"]
    await unsub.handle("slow_unsub", {"subscriber_id": "s1"})

    assert "candles.min.AAPL" in channel.handlers


@pytest.mark.asyncio
async def test_last_unsubscribe_drops_candle_subscription():
    channel = _MockChannel()
    svc, _ = _make_service(channel=channel)
    await svc.start()

    sub = channel.handlers["slow_sub"]
    await sub.handle("slow_sub", {
        "subscriber_id": "s1", "symbol": "AAPL", "indicator_types": ["in_news"],
    })

    unsub = channel.handlers["slow_unsub"]
    await unsub.handle("slow_unsub", {"subscriber_id": "s1"})

    assert "candles.min.AAPL" not in channel.handlers


@pytest.mark.asyncio
async def test_unsubscribe_removing_only_one_of_two_types_keeps_other_active():
    channel = _MockChannel()
    svc, _ = _make_service(channel=channel)
    await svc.start()

    sub = channel.handlers["slow_sub"]
    await sub.handle("slow_sub", {
        "subscriber_id": "s1", "symbol": "AAPL", "indicator_types": ["in_news"],
    })
    await sub.handle("slow_sub", {
        "subscriber_id": "s2", "symbol": "AAPL", "indicator_types": ["shares_float"],
    })

    unsub = channel.handlers["slow_unsub"]
    await unsub.handle("slow_unsub", {"subscriber_id": "s1"})

    handler = channel.handlers["candles.min.AAPL"]
    assert [ind.type() for ind in handler._indicators] == ["shares_float"]


@pytest.mark.asyncio
async def test_candle_arrival_invokes_extend_realtime_on_each_indicator():
    in_news = _make_indicator("in_news")
    shares_float = _make_indicator("shares_float")
    channel = _MockChannel()
    svc, _ = _make_service(channel=channel, indicators=[in_news, shares_float])
    await svc.start()

    sub = channel.handlers["slow_sub"]
    await sub.handle("slow_sub", {
        "subscriber_id": "s1", "symbol": "AAPL",
        "indicator_types": ["in_news", "shares_float"],
    })

    candle_handler = channel.handlers["candles.min.AAPL"]
    await candle_handler.handle("candles.min.AAPL", {
        "timestamp": 1_700_000_000_000,
        "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1.0,
    })

    assert in_news.calls == [("AAPL", 1_700_000_000_000)]
    assert shares_float.calls == [("AAPL", 1_700_000_000_000)]


@pytest.mark.asyncio
async def test_exception_in_one_indicator_does_not_block_others():
    bad = _make_indicator("in_news", raises=True)
    good = _make_indicator("shares_float")
    channel = _MockChannel()
    svc, _ = _make_service(channel=channel, indicators=[bad, good])
    await svc.start()

    sub = channel.handlers["slow_sub"]
    await sub.handle("slow_sub", {
        "subscriber_id": "s1", "symbol": "AAPL",
        "indicator_types": ["in_news", "shares_float"],
    })

    candle_handler = channel.handlers["candles.min.AAPL"]
    await candle_handler.handle("candles.min.AAPL", {
        "timestamp": 1_700_000_000_000,
        "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1.0,
    })

    assert bad.calls == [("AAPL", 1_700_000_000_000)]
    assert good.calls == [("AAPL", 1_700_000_000_000)]


@pytest.mark.asyncio
async def test_subscribe_payload_missing_required_fields_is_noop():
    channel = _MockChannel()
    svc, _ = _make_service(channel=channel)
    await svc.start()
    sub = channel.handlers["slow_sub"]

    await sub.handle("slow_sub", {"symbol": "AAPL", "indicator_types": ["in_news"]})  # no subscriber_id
    await sub.handle("slow_sub", {"subscriber_id": "s1", "indicator_types": ["in_news"]})  # no symbol
    await sub.handle("slow_sub", {"subscriber_id": "s1", "symbol": "AAPL"})  # no types

    assert not any(c.startswith("candles.min.") for c in channel.subscribe_calls)


@pytest.mark.asyncio
async def test_stop_unsubscribes_all_candle_streams():
    channel = _MockChannel()
    svc, _ = _make_service(channel=channel)
    await svc.start()

    sub = channel.handlers["slow_sub"]
    await sub.handle("slow_sub", {
        "subscriber_id": "s1", "symbol": "AAPL", "indicator_types": ["in_news"],
    })
    await sub.handle("slow_sub", {
        "subscriber_id": "s2", "symbol": "MSFT", "indicator_types": ["shares_float"],
    })

    await svc.stop()

    assert "candles.min.AAPL" not in channel.handlers
    assert "candles.min.MSFT" not in channel.handlers
