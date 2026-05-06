from typing import Literal

from opentelemetry.metrics import Counter, Histogram, _Gauge, get_meter

from fastscanner.pkg.observability import buckets, otel_init

CacheSaveResult = Literal["ok", "error"]
SubscriptionKind = Literal[
    "websocket_indicator",
    "scanner_wildcard",
    "indicator_fanout",
]
RequestSource = Literal["polygon_rest", "eodhd", "news", "internal"]


class _Metrics:
    def __init__(self) -> None:
        meter = get_meter("fastscanner")

        self.candles_received_total: Counter = meter.create_counter(
            name="fs.candles.received",
            description="Candles reconstructed from the realtime feed.",
        )
        self.nats_publish_total: Counter = meter.create_counter(
            name="fs.nats.publish",
            description="NATS messages published, by subject prefix kind.",
        )
        self.nats_publish_errors_total: Counter = meter.create_counter(
            name="fs.nats.publish.errors",
            description="NATS publish exceptions, by subject prefix kind.",
        )
        self.nats_reconnect_total: Counter = meter.create_counter(
            name="fs.nats.reconnect",
            description="NATS client reconnections.",
        )
        self.indicator_cache_save_total: Counter = meter.create_counter(
            name="fs.indicator.cache.save",
            description="Indicator cache save attempts.",
        )
        self.indicator_extend_errors_total: Counter = meter.create_counter(
            name="fs.indicator.extend.errors",
            description="Indicator extend_realtime exceptions, by indicator name.",
        )
        self.external_requests_total: Counter = meter.create_counter(
            name="fs.external.requests",
            description="Outbound requests by source, endpoint template, and status code.",
        )
        self.external_request_errors_total: Counter = meter.create_counter(
            name="fs.external.request.errors",
            description="Outbound request exceptions by source, endpoint template, and kind.",
        )

        self.nats_flush_duration_seconds: Histogram = meter.create_histogram(
            name="fs.nats.flush.duration",
            unit="s",
            description="Latency of NATSChannel.flush(), one observation per flush call.",
            explicit_bucket_boundaries_advisory=list(buckets.NATS_FLUSH_SECONDS[:-1]),
        )
        self.indicator_extend_latency_seconds: Histogram = meter.create_histogram(
            name="fs.indicator.extend.latency",
            unit="s",
            description="Latency of indicator.extend_realtime() per indicator name.",
            explicit_bucket_boundaries_advisory=list(
                buckets.INDICATOR_EXTEND_SECONDS[:-1]
            ),
        )
        self.external_request_latency_seconds: Histogram = meter.create_histogram(
            name="fs.external.request.latency",
            unit="s",
            description="Outbound request latency by source and endpoint template.",
            explicit_bucket_boundaries_advisory=list(buckets.HTTP_REQUEST_SECONDS[:-1]),
        )

        self.first_candle_delay_seconds: _Gauge = meter.create_gauge(
            name="fs.first.candle.delay",
            unit="s",
            description=(
                "Wall-clock seconds past the bar end when the FIRST candle for the "
                "most recently observed minute arrived from Polygon."
            ),
        )
        self.candle_arrival_spread_seconds: _Gauge = meter.create_gauge(
            name="fs.candle.arrival.spread",
            unit="s",
            description=(
                "Seconds between the first and last candle arrival for the most "
                "recently completed minute bar."
            ),
        )
        self.nats_pending_messages: _Gauge = meter.create_gauge(
            name="fs.nats.pending.messages",
            description="Depth of NATSChannel._pending_messages at flush boundary.",
        )
        self.polygon_ws_connected: _Gauge = meter.create_gauge(
            name="fs.polygon.ws.connected",
            description="1 if the Polygon WebSocket is connected; 0 otherwise.",
        )
        self.active_subscriptions: _Gauge = meter.create_gauge(
            name="fs.active.subscriptions",
            description="Active subscription count by kind.",
        )
        self.market_is_open: _Gauge = meter.create_gauge(
            name="fs.market.is.open",
            description="1 if the equity market is currently open; 0 otherwise.",
        )
        self.daily_reset_in_progress: _Gauge = meter.create_gauge(
            name="fs.daily.reset.in.progress",
            description="1 while the realtime writer is performing scheduled daily reset.",
        )

    def candle_received(self, symbol_class: str) -> None:
        self.candles_received_total.add(1, {"symbol_class": symbol_class})

    def nats_publish(self, kind: str) -> None:
        self.nats_publish_total.add(1, {"kind": kind})

    def nats_publish_error(self, kind: str) -> None:
        self.nats_publish_errors_total.add(1, {"kind": kind})

    def nats_reconnect(self) -> None:
        self.nats_reconnect_total.add(1)

    def nats_flush(self, latency_seconds: float) -> None:
        self.nats_flush_duration_seconds.record(latency_seconds)

    def nats_pending(self, depth: int) -> None:
        self.nats_pending_messages.set(depth)

    def indicator_extend_latency(self, name: str, latency_seconds: float) -> None:
        self.indicator_extend_latency_seconds.record(
            latency_seconds, {"name": name}
        )

    def indicator_extend_error(self, name: str) -> None:
        self.indicator_extend_errors_total.add(1, {"name": name})

    def indicator_cache_save(self, name: str, result: CacheSaveResult) -> None:
        self.indicator_cache_save_total.add(1, {"name": name, "result": result})

    def external_request(
        self, source: str, endpoint: str, code: int, latency_seconds: float
    ) -> None:
        attrs = {"source": source, "endpoint": endpoint, "code": str(code)}
        self.external_requests_total.add(1, attrs)
        self.external_request_latency_seconds.record(
            latency_seconds, {"source": source, "endpoint": endpoint}
        )

    def external_request_error(self, source: str, endpoint: str, kind: str) -> None:
        self.external_request_errors_total.add(
            1, {"source": source, "endpoint": endpoint, "kind": kind}
        )

    def set_first_candle_delay(self, delay_seconds: float) -> None:
        self.first_candle_delay_seconds.set(delay_seconds)

    def set_candle_arrival_spread(self, spread_seconds: float) -> None:
        self.candle_arrival_spread_seconds.set(spread_seconds)

    def set_ws_connected(self, connected: bool) -> None:
        self.polygon_ws_connected.set(1 if connected else 0)

    def set_active_subscriptions(self, kind: SubscriptionKind, count: int) -> None:
        self.active_subscriptions.set(count, {"kind": kind})

    def set_market_open(self, is_open: bool) -> None:
        self.market_is_open.set(1 if is_open else 0)

    def set_daily_reset(self, in_progress: bool) -> None:
        self.daily_reset_in_progress.set(1 if in_progress else 0)


_instance: _Metrics | None = None


def _get() -> _Metrics:
    global _instance
    if _instance is None:
        if not otel_init.is_initialized():
            raise RuntimeError(
                "metrics helper called before otel_init.setup_meter_provider(). "
                "Each long-running process must call init_metrics(role=...) at startup."
            )
        _instance = _Metrics()
    return _instance


def candle_received(symbol_class: str) -> None:
    _get().candle_received(symbol_class)


def nats_publish(kind: str) -> None:
    _get().nats_publish(kind)


def nats_publish_error(kind: str) -> None:
    _get().nats_publish_error(kind)


def nats_reconnect() -> None:
    _get().nats_reconnect()


def nats_flush(latency_seconds: float) -> None:
    _get().nats_flush(latency_seconds)


def nats_pending(depth: int) -> None:
    _get().nats_pending(depth)


def indicator_extend_latency(name: str, latency_seconds: float) -> None:
    _get().indicator_extend_latency(name, latency_seconds)


def indicator_extend_error(name: str) -> None:
    _get().indicator_extend_error(name)


def indicator_cache_save(name: str, result: CacheSaveResult) -> None:
    _get().indicator_cache_save(name, result)


def external_request(
    source: str, endpoint: str, code: int, latency_seconds: float
) -> None:
    _get().external_request(source, endpoint, code, latency_seconds)


def external_request_error(source: str, endpoint: str, kind: str) -> None:
    _get().external_request_error(source, endpoint, kind)


def set_first_candle_delay(delay_seconds: float) -> None:
    _get().set_first_candle_delay(delay_seconds)


def set_candle_arrival_spread(spread_seconds: float) -> None:
    _get().set_candle_arrival_spread(spread_seconds)


def set_ws_connected(connected: bool) -> None:
    _get().set_ws_connected(connected)


def set_active_subscriptions(kind: SubscriptionKind, count: int) -> None:
    _get().set_active_subscriptions(kind, count)


def set_market_open(is_open: bool) -> None:
    _get().set_market_open(is_open)


def set_daily_reset(in_progress: bool) -> None:
    _get().set_daily_reset(in_progress)


def _reset_for_test() -> None:
    global _instance
    _instance = None
