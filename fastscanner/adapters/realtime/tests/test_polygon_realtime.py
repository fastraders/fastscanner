from unittest.mock import AsyncMock

import pytest
from massive.websocket.models import EquityAgg, EventType

from fastscanner.adapters.realtime.polygon_realtime import PolygonRealtime
from fastscanner.pkg.clock import ClockRegistry, LocalClock


def _read_sample(reg, sample_name: str, **labels) -> float:
    for metric in reg.collect():
        for sample in metric.samples:
            if sample.name != sample_name:
                continue
            if all(sample.labels.get(k) == v for k, v in labels.items()):
                return sample.value
    raise AssertionError(f"sample {sample_name}{labels} not found")


def _make_msg(symbol: str, event_type: EventType, ts: int = 1_700_000_000) -> EquityAgg:
    msg: EquityAgg = object.__new__(EquityAgg)
    msg.event_type = event_type
    msg.symbol = symbol
    msg.open = 1.0
    msg.high = 2.0
    msg.low = 0.5
    msg.close = 1.5
    msg.volume = 100
    msg.start_timestamp = ts
    return msg


@pytest.fixture
def realtime():
    channel = AsyncMock()
    return PolygonRealtime(api_key="test-key", channel=channel)


@pytest.mark.asyncio
async def test_handle_messages_increments_candle_counter(
    realtime, _isolate_metrics
):
    msgs = [
        _make_msg("AAPL", EventType.EquityAggMin),
        _make_msg("MSFT", EventType.EquityAggMin),
        _make_msg("GOOG", EventType.EquityAgg),
    ]

    await realtime.handle_messages(msgs)

    assert (
        _read_sample(
            _isolate_metrics, "fs_candles_received_total", symbol_class="Stocks"
        )
        == 3
    )


@pytest.mark.asyncio
async def test_handle_messages_skips_filtered_symbols(realtime, _isolate_metrics):
    ClockRegistry.set(LocalClock())
    realtime._is_filtering_active_symbols = True
    realtime._active_symbols = {"AAPL"}
    realtime._active_symbols_last_updated = ClockRegistry.clock.today()

    msgs = [
        _make_msg("AAPL", EventType.EquityAggMin),
        _make_msg("FILTERED_OUT", EventType.EquityAggMin),
    ]

    await realtime.handle_messages(msgs)

    assert (
        _read_sample(
            _isolate_metrics, "fs_candles_received_total", symbol_class="Stocks"
        )
        == 1
    )


@pytest.mark.asyncio
async def test_handle_messages_calls_channel_push_per_candle(realtime):
    msgs = [
        _make_msg("AAPL", EventType.EquityAggMin),
        _make_msg("MSFT", EventType.EquityAgg),
    ]
    await realtime.handle_messages(msgs)

    assert realtime._channel.push.await_count == 2
    realtime._channel.flush.assert_awaited_once()


@pytest.mark.asyncio
async def test_handle_messages_skips_unknown_event_type(
    realtime, _isolate_metrics
):
    bad = _make_msg("AAPL", EventType.EquityAggMin)
    bad.event_type = "fake.unknown"  # type: ignore[assignment]

    await realtime.handle_messages([bad])

    with pytest.raises(AssertionError):
        _read_sample(
            _isolate_metrics, "fs_candles_received_total", symbol_class="Stocks"
        )
