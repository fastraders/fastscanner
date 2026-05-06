import pytest
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from fastscanner.pkg.observability import metrics
from fastscanner.pkg.observability.tests._helpers import (
    find_histogram,
    find_point as _find_point,
    snapshot as _snapshot,
)


def test_candle_received(in_memory_provider: InMemoryMetricReader):
    metrics.candle_received("Stocks")
    metrics.candle_received("Stocks")
    metrics.candle_received("ETF")
    assert _find_point(
        in_memory_provider, "fs.candles.received", symbol_class="Stocks"
    ).value == 2
    assert _find_point(
        in_memory_provider, "fs.candles.received", symbol_class="ETF"
    ).value == 1


def test_nats_publish_and_errors(in_memory_provider: InMemoryMetricReader):
    metrics.nats_publish("candles_s")
    metrics.nats_publish("candles_s")
    metrics.nats_publish_error("candles_min")
    assert _find_point(
        in_memory_provider, "fs.nats.publish", kind="candles_s"
    ).value == 2
    assert _find_point(
        in_memory_provider, "fs.nats.publish.errors", kind="candles_min"
    ).value == 1


def test_nats_reconnect(in_memory_provider: InMemoryMetricReader):
    metrics.nats_reconnect()
    metrics.nats_reconnect()
    assert _find_point(in_memory_provider, "fs.nats.reconnect").value == 2


def test_nats_flush_histogram(in_memory_provider: InMemoryMetricReader):
    metrics.nats_flush(0.0007)
    metrics.nats_flush(0.003)
    point = find_histogram(in_memory_provider, "fs.nats.flush.duration")
    assert point.count == 2


def test_nats_pending_gauge(in_memory_provider: InMemoryMetricReader):
    metrics.nats_pending(1500)
    assert _find_point(in_memory_provider, "fs.nats.pending.messages").value == 1500
    metrics.nats_pending(0)
    assert _find_point(in_memory_provider, "fs.nats.pending.messages").value == 0


def test_indicator_extend_latency(in_memory_provider: InMemoryMetricReader):
    metrics.indicator_extend_latency("ATR", 0.002)
    metrics.indicator_extend_latency("ATR", 0.05)
    metrics.indicator_extend_latency("ADV", 0.001)
    atr = find_histogram(in_memory_provider, "fs.indicator.extend.latency", name="ATR")
    adv = find_histogram(in_memory_provider, "fs.indicator.extend.latency", name="ADV")
    assert atr.count == 2
    assert adv.count == 1


def test_indicator_extend_error(in_memory_provider: InMemoryMetricReader):
    metrics.indicator_extend_error("BAD")
    metrics.indicator_extend_error("BAD")
    metrics.indicator_extend_error("OTHER")
    assert _find_point(
        in_memory_provider, "fs.indicator.extend.errors", name="BAD"
    ).value == 2
    assert _find_point(
        in_memory_provider, "fs.indicator.extend.errors", name="OTHER"
    ).value == 1


def test_indicator_cache_save(in_memory_provider: InMemoryMetricReader):
    metrics.indicator_cache_save("ATR", "ok")
    metrics.indicator_cache_save("ATR", "ok")
    metrics.indicator_cache_save("ATR", "error")
    assert _find_point(
        in_memory_provider, "fs.indicator.cache.save", name="ATR", result="ok"
    ).value == 2
    assert _find_point(
        in_memory_provider, "fs.indicator.cache.save", name="ATR", result="error"
    ).value == 1


def test_external_request(in_memory_provider: InMemoryMetricReader):
    metrics.external_request("polygon_rest", "/v2/aggs/{symbol}", 200, 0.05)
    metrics.external_request("polygon_rest", "/v2/aggs/{symbol}", 500, 0.20)
    metrics.external_request_error("polygon_rest", "/v2/aggs/{symbol}", "Timeout")

    assert _find_point(
        in_memory_provider,
        "fs.external.requests",
        source="polygon_rest",
        endpoint="/v2/aggs/{symbol}",
        code="200",
    ).value == 1
    point = find_histogram(
        in_memory_provider,
        "fs.external.request.latency",
        source="polygon_rest",
        endpoint="/v2/aggs/{symbol}",
    )
    assert point.count == 2
    assert _find_point(
        in_memory_provider,
        "fs.external.request.errors",
        source="polygon_rest",
        endpoint="/v2/aggs/{symbol}",
        kind="Timeout",
    ).value == 1


def test_ws_connected_gauge(in_memory_provider: InMemoryMetricReader):
    metrics.set_ws_connected(True)
    assert _find_point(in_memory_provider, "fs.polygon.ws.connected").value == 1
    metrics.set_ws_connected(False)
    assert _find_point(in_memory_provider, "fs.polygon.ws.connected").value == 0


def test_active_subscriptions(in_memory_provider: InMemoryMetricReader):
    metrics.set_active_subscriptions("websocket_indicator", 7)
    metrics.set_active_subscriptions("indicator_fanout", 42)
    snap = _snapshot(in_memory_provider)
    assert _find_point(
        snap, "fs.active.subscriptions", kind="websocket_indicator"
    ).value == 7
    assert _find_point(
        snap, "fs.active.subscriptions", kind="indicator_fanout"
    ).value == 42


def test_market_open_and_daily_reset(in_memory_provider: InMemoryMetricReader):
    metrics.set_market_open(True)
    metrics.set_daily_reset(False)
    snap = _snapshot(in_memory_provider)
    assert _find_point(snap, "fs.market.is.open").value == 1
    assert _find_point(snap, "fs.daily.reset.in.progress").value == 0


def test_first_candle_delay_and_spread(in_memory_provider: InMemoryMetricReader):
    metrics.set_first_candle_delay(1.4)
    metrics.set_first_candle_delay(1.6)
    metrics.set_candle_arrival_spread(2.2)
    snap = _snapshot(in_memory_provider)
    assert _find_point(snap, "fs.first.candle.delay").value == 1.6
    assert _find_point(snap, "fs.candle.arrival.spread").value == 2.2


def test_helper_called_before_init_raises():
    with pytest.raises(RuntimeError, match="setup_meter_provider"):
        metrics.candle_received("Stocks")


def test_all_metric_definitions_emit(in_memory_provider: InMemoryMetricReader):
    """Golden test: every helper produces a discoverable OTel metric.

    Catches regressions where a metric is renamed or dropped behind the wrapper API.
    """
    metrics.candle_received("Stocks")
    metrics.nats_publish("candles_s")
    metrics.nats_publish_error("candles_s")
    metrics.nats_reconnect()
    metrics.nats_flush(0.001)
    metrics.nats_pending(10)
    metrics.indicator_extend_latency("X", 0.001)
    metrics.indicator_extend_error("X")
    metrics.indicator_cache_save("X", "ok")
    metrics.external_request("polygon_rest", "/v2/aggs/{symbol}", 200, 0.01)
    metrics.external_request_error("polygon_rest", "/v2/aggs/{symbol}", "Timeout")
    metrics.set_first_candle_delay(0.5)
    metrics.set_candle_arrival_spread(0.5)
    metrics.set_ws_connected(True)
    metrics.set_active_subscriptions("indicator_fanout", 1)
    metrics.set_market_open(True)
    metrics.set_daily_reset(False)

    expected_names = {
        "fs.candles.received",
        "fs.nats.publish",
        "fs.nats.publish.errors",
        "fs.nats.reconnect",
        "fs.nats.flush.duration",
        "fs.nats.pending.messages",
        "fs.indicator.extend.latency",
        "fs.indicator.extend.errors",
        "fs.indicator.cache.save",
        "fs.external.requests",
        "fs.external.request.errors",
        "fs.external.request.latency",
        "fs.first.candle.delay",
        "fs.candle.arrival.spread",
        "fs.polygon.ws.connected",
        "fs.active.subscriptions",
        "fs.market.is.open",
        "fs.daily.reset.in.progress",
    }
    data = in_memory_provider.get_metrics_data()
    assert data is not None
    seen = {
        m.name
        for rm in data.resource_metrics
        for sm in rm.scope_metrics
        for m in sm.metrics
    }
    missing = expected_names - seen
    assert not missing, f"missing OTel metrics: {missing}"
