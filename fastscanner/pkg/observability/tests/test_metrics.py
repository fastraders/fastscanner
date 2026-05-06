from prometheus_client import CollectorRegistry, generate_latest

from fastscanner.pkg.observability import metrics


def _read_sample(reg: CollectorRegistry, sample_name: str, **labels) -> float:
    for metric in reg.collect():
        for sample in metric.samples:
            if sample.name != sample_name:
                continue
            if all(sample.labels.get(k) == v for k, v in labels.items()):
                return sample.value
    raise AssertionError(
        f"sample {sample_name}{labels} not found; got: "
        f"{[(s.name, s.labels) for m in reg.collect() for s in m.samples]}"
    )


def test_candle_received_increments(fresh_registry: CollectorRegistry):
    metrics.candle_received("Stocks")
    metrics.candle_received("Stocks")
    metrics.candle_received("ETF")
    assert _read_sample(
        fresh_registry, "fs_candles_received_total", symbol_class="Stocks"
    ) == 2
    assert _read_sample(
        fresh_registry, "fs_candles_received_total", symbol_class="ETF"
    ) == 1


def test_nats_publish_and_errors(fresh_registry: CollectorRegistry):
    metrics.nats_publish("candles_s")
    metrics.nats_publish("candles_s")
    metrics.nats_publish_error("candles_min")
    assert _read_sample(
        fresh_registry, "fs_nats_publish_total", kind="candles_s"
    ) == 2
    assert _read_sample(
        fresh_registry, "fs_nats_publish_errors_total", kind="candles_min"
    ) == 1


def test_nats_reconnect(fresh_registry: CollectorRegistry):
    metrics.nats_reconnect()
    metrics.nats_reconnect()
    assert _read_sample(fresh_registry, "fs_nats_reconnect_total") == 2


def test_nats_flush_histogram(fresh_registry: CollectorRegistry):
    metrics.nats_flush("candles_s", 0.0007)
    metrics.nats_flush("candles_s", 0.003)
    assert _read_sample(
        fresh_registry, "fs_nats_flush_duration_seconds_count", kind="candles_s"
    ) == 2
    bucket_500us = _read_sample(
        fresh_registry,
        "fs_nats_flush_duration_seconds_bucket",
        kind="candles_s",
        le="0.001",
    )
    assert bucket_500us == 1


def test_nats_pending_gauge(fresh_registry: CollectorRegistry):
    metrics.nats_pending(1500)
    assert _read_sample(fresh_registry, "fs_nats_pending_messages") == 1500
    metrics.nats_pending(0)
    assert _read_sample(fresh_registry, "fs_nats_pending_messages") == 0


def test_indicator_extend_latency(fresh_registry: CollectorRegistry):
    metrics.indicator_extend_latency("ATR", 0.002)
    metrics.indicator_extend_latency("ATR", 0.05)
    metrics.indicator_extend_latency("ADV", 0.001)
    assert _read_sample(
        fresh_registry,
        "fs_indicator_extend_latency_seconds_count",
        name="ATR",
    ) == 2
    assert _read_sample(
        fresh_registry,
        "fs_indicator_extend_latency_seconds_count",
        name="ADV",
    ) == 1


def test_indicator_cache_save(fresh_registry: CollectorRegistry):
    metrics.indicator_cache_save("ATR", "ok")
    metrics.indicator_cache_save("ATR", "ok")
    metrics.indicator_cache_save("ATR", "error")
    assert _read_sample(
        fresh_registry,
        "fs_indicator_cache_save_total",
        name="ATR",
        result="ok",
    ) == 2
    assert _read_sample(
        fresh_registry,
        "fs_indicator_cache_save_total",
        name="ATR",
        result="error",
    ) == 1


def test_http_request_histogram(fresh_registry: CollectorRegistry):
    metrics.http_request("/api/indicators", 200, 0.05)
    metrics.http_request("/api/indicators", 500, 0.20)
    assert _read_sample(
        fresh_registry,
        "fs_http_request_latency_seconds_count",
        route="/api/indicators",
        code="200",
    ) == 1
    assert _read_sample(
        fresh_registry,
        "fs_http_request_latency_seconds_count",
        route="/api/indicators",
        code="500",
    ) == 1


def test_ws_connected_gauge(fresh_registry: CollectorRegistry):
    metrics.set_ws_connected(True)
    assert _read_sample(fresh_registry, "fs_polygon_ws_connected") == 1
    metrics.set_ws_connected(False)
    assert _read_sample(fresh_registry, "fs_polygon_ws_connected") == 0


def test_active_subscriptions(fresh_registry: CollectorRegistry):
    metrics.set_active_subscriptions("websocket_indicator", 7)
    metrics.set_active_subscriptions("indicator_fanout", 42)
    assert _read_sample(
        fresh_registry, "fs_active_subscriptions", kind="websocket_indicator"
    ) == 7
    assert _read_sample(
        fresh_registry, "fs_active_subscriptions", kind="indicator_fanout"
    ) == 42


def test_market_open_and_daily_reset(fresh_registry: CollectorRegistry):
    metrics.set_market_open(True)
    metrics.set_daily_reset(False)
    assert _read_sample(fresh_registry, "fs_market_is_open") == 1
    assert _read_sample(fresh_registry, "fs_daily_reset_in_progress") == 0


def test_exposition_format_round_trip(fresh_registry: CollectorRegistry):
    metrics.candle_received("Stocks")
    metrics.set_ws_connected(True)
    output = generate_latest(fresh_registry).decode()
    assert "fs_candles_received_total" in output
    assert "fs_polygon_ws_connected 1.0" in output


def test_lazy_singleton_uses_default_registry_when_not_reset():
    metrics._instance = None
    instance = metrics._get()
    assert instance is metrics._instance
    assert metrics._get() is instance
