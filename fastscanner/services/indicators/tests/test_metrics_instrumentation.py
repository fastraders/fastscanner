from typing import Iterable
from unittest.mock import AsyncMock

import pandas as pd
import pytest
from prometheus_client import CollectorRegistry

from fastscanner.pkg.candle import Candle
from fastscanner.services.indicators.service import (
    CandleChannelHandler,
    IndicatorsService,
    SubscriptionHandler,
)
from fastscanner.services.indicators.slow_indicators_service import (
    SlowIndicatorsService,
    _SlowIndicatorCandleHandler,
)


def _read_sample(reg: CollectorRegistry, sample_name: str, **labels) -> float:
    for metric in reg.collect():
        for sample in metric.samples:
            if sample.name != sample_name:
                continue
            if all(sample.labels.get(k) == v for k, v in labels.items()):
                return sample.value
    raise AssertionError(f"sample {sample_name}{labels} not found")


class _SilentHandler(SubscriptionHandler):
    async def handle(self, symbol, new_row):
        return new_row


class _FakeIndicator:
    def __init__(self, name: str) -> None:
        self._name = name

    async def extend_realtime(self, symbol, new_row):
        return new_row

    def column_name(self) -> str:
        return self._name

    def type(self) -> str:
        return self._name


class _FakeFastIndicator(_FakeIndicator):
    pass


class _FakeSlowIndicator(_FakeIndicator):
    def __init__(self, name: str, raises: bool = False) -> None:
        super().__init__(name)
        self._raises = raises

    async def extend_realtime(self, symbol, new_row):
        if self._raises:
            raise RuntimeError("kaboom")
        return new_row


def _make_candle() -> Candle:
    ts = pd.Timestamp("2026-05-06 10:00:00", tz="America/New_York")
    return Candle(
        {"open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 100},
        timestamp=ts,
    )


@pytest.mark.asyncio
async def test_candle_channel_handler_records_extend_latency(
    _isolate_metrics: CollectorRegistry,
):
    indicators: Iterable = [_FakeFastIndicator("ATR"), _FakeFastIndicator("ADV")]
    handler = CandleChannelHandler(indicators, _SilentHandler(), "1min", AsyncMock())

    await handler._handle("AAPL", _make_candle())

    assert (
        _read_sample(
            _isolate_metrics,
            "fs_indicator_extend_latency_seconds_count",
            name="_FakeFastIndicator",
        )
        == 2
    )


@pytest.mark.asyncio
async def test_subscribe_realtime_updates_active_subscriptions_gauge(
    _isolate_metrics: CollectorRegistry,
):
    channel = AsyncMock()
    service = IndicatorsService(
        candles=AsyncMock(),
        fundamentals=AsyncMock(),
        channel=channel,
        cache=AsyncMock(),
        symbols_subscribe_channel="sub",
        symbols_unsubscribe_channel="unsub",
        cache_at_seconds=10,
        symbols_slow_indicators_subscribe_channel="slow_sub",
        symbols_slow_indicators_unsubscribe_channel="slow_unsub",
    )

    sub_id = await service.subscribe_realtime(
        "AAPL", "1min", [], _SilentHandler(), _send_events=False
    )

    assert (
        _read_sample(_isolate_metrics, "fs_active_subscriptions", kind="indicator_fanout")
        == 1
    )

    await service.unsubscribe_realtime(sub_id, _send_events=False)

    assert (
        _read_sample(_isolate_metrics, "fs_active_subscriptions", kind="indicator_fanout")
        == 0
    )


@pytest.mark.asyncio
async def test_unsubscribe_realtime_removes_from_subscription_dict(
    _isolate_metrics: CollectorRegistry,
):
    channel = AsyncMock()
    service = IndicatorsService(
        candles=AsyncMock(),
        fundamentals=AsyncMock(),
        channel=channel,
        cache=AsyncMock(),
        symbols_subscribe_channel="sub",
        symbols_unsubscribe_channel="unsub",
        cache_at_seconds=10,
        symbols_slow_indicators_subscribe_channel="slow_sub",
        symbols_slow_indicators_unsubscribe_channel="slow_unsub",
    )

    sub_id = await service.subscribe_realtime(
        "AAPL", "1min", [], _SilentHandler(), _send_events=False
    )
    assert sub_id in service._subscription_to_channel

    await service.unsubscribe_realtime(sub_id, _send_events=False)
    assert sub_id not in service._subscription_to_channel


@pytest.mark.asyncio
async def test_slow_indicator_handler_records_latency_on_success(
    _isolate_metrics: CollectorRegistry,
):
    handler = _SlowIndicatorCandleHandler(
        "AAPL",
        [_FakeSlowIndicator("NewsConfidenceIndicator")],
    )

    await handler.handle(
        "candles.min.AAPL",
        {
            "timestamp": int(
                pd.Timestamp("2026-05-06 10:00:00", tz="UTC").value / 1_000_000
            ),
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "volume": 100,
        },
    )

    assert (
        _read_sample(
            _isolate_metrics,
            "fs_indicator_extend_latency_seconds_count",
            name="_FakeSlowIndicator",
        )
        == 1
    )


@pytest.mark.asyncio
async def test_slow_indicator_handler_records_latency_on_failure(
    _isolate_metrics: CollectorRegistry,
):
    handler = _SlowIndicatorCandleHandler(
        "AAPL",
        [_FakeSlowIndicator("BoomIndicator", raises=True)],
    )

    await handler.handle(
        "candles.min.AAPL",
        {
            "timestamp": int(
                pd.Timestamp("2026-05-06 10:00:00", tz="UTC").value / 1_000_000
            ),
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "volume": 100,
        },
    )

    assert (
        _read_sample(
            _isolate_metrics,
            "fs_indicator_extend_latency_seconds_count",
            name="_FakeSlowIndicator",
        )
        == 1
    )


@pytest.mark.asyncio
async def test_slow_service_updates_bindings_gauge(
    _isolate_metrics: CollectorRegistry,
):
    channel = AsyncMock()
    service = SlowIndicatorsService(
        channel=channel,
        indicators=[_FakeSlowIndicator("NewsConfidenceIndicator")],
        subscribe_channel="slow_sub",
        unsubscribe_channel="slow_unsub",
    )

    await service._add_subscription("sub-1", "AAPL", ["NewsConfidenceIndicator"])
    assert (
        _read_sample(
            _isolate_metrics, "fs_active_subscriptions", kind="slow_indicator_fanout"
        )
        == 1
    )

    await service._add_subscription("sub-2", "MSFT", ["NewsConfidenceIndicator"])
    assert (
        _read_sample(
            _isolate_metrics, "fs_active_subscriptions", kind="slow_indicator_fanout"
        )
        == 2
    )

    await service._remove_subscription("sub-1")
    assert (
        _read_sample(
            _isolate_metrics, "fs_active_subscriptions", kind="slow_indicator_fanout"
        )
        == 1
    )
