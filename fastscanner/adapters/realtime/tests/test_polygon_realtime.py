from unittest.mock import AsyncMock, patch

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


@pytest.mark.asyncio
async def test_handle_messages_records_first_candle_delay(
    realtime, _isolate_metrics
):
    bar_minute_ms = 1_700_000_000_000
    bar_end_seconds = (bar_minute_ms + 60_000) / 1000
    wall_now = bar_end_seconds + 1.4

    msgs = [_make_msg("AAPL", EventType.EquityAggMin, ts=bar_minute_ms)]

    with patch("fastscanner.adapters.realtime.polygon_realtime.time.time", return_value=wall_now):
        await realtime.handle_messages(msgs)

    assert _read_sample(_isolate_metrics, "fs_first_candle_delay_seconds") == pytest.approx(1.4)
    assert _read_sample(_isolate_metrics, "fs_candle_arrival_spread_seconds") == 0.0


@pytest.mark.asyncio
async def test_minute_rollover_records_spread(realtime, _isolate_metrics):
    minute_a_ms = 1_700_000_000_000
    minute_b_ms = minute_a_ms + 60_000

    minute_a_end_s = (minute_a_ms + 60_000) / 1000
    minute_b_end_s = (minute_b_ms + 60_000) / 1000

    wall_a_first = minute_a_end_s + 1.2
    wall_a_last = minute_a_end_s + 3.4
    wall_b_first = minute_b_end_s + 1.5

    with patch(
        "fastscanner.adapters.realtime.polygon_realtime.time.time",
        return_value=wall_a_first,
    ):
        await realtime.handle_messages([_make_msg("AAPL", EventType.EquityAggMin, ts=minute_a_ms)])

    with patch(
        "fastscanner.adapters.realtime.polygon_realtime.time.time",
        return_value=wall_a_last,
    ):
        await realtime.handle_messages([_make_msg("MSFT", EventType.EquityAggMin, ts=minute_a_ms)])

    with patch(
        "fastscanner.adapters.realtime.polygon_realtime.time.time",
        return_value=wall_b_first,
    ):
        await realtime.handle_messages([_make_msg("AAPL", EventType.EquityAggMin, ts=minute_b_ms)])

    assert _read_sample(
        _isolate_metrics, "fs_first_candle_delay_seconds"
    ) == pytest.approx(1.5)
    assert _read_sample(
        _isolate_metrics, "fs_candle_arrival_spread_seconds"
    ) == pytest.approx(2.2)


@pytest.mark.asyncio
async def test_seconds_event_type_not_tracked_for_minute_delay(
    realtime, _isolate_metrics
):
    bar_seconds_ms = 1_700_000_000_000
    msgs = [_make_msg("AAPL", EventType.EquityAgg, ts=bar_seconds_ms)]
    with patch("fastscanner.adapters.realtime.polygon_realtime.time.time", return_value=bar_seconds_ms / 1000 + 1.4):
        await realtime.handle_messages(msgs)
    assert _read_sample(_isolate_metrics, "fs_first_candle_delay_seconds") == 0.0
