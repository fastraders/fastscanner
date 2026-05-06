from typing import Literal

from prometheus_client import REGISTRY, CollectorRegistry, Counter, Gauge, Histogram

from fastscanner.pkg.observability import buckets

CacheSaveResult = Literal["ok", "error", "miss"]
SubscriptionKind = Literal[
    "websocket_indicator",
    "scanner_wildcard",
    "indicator_fanout",
    "slow_indicator_fanout",
]


class _Metrics:
    def __init__(self, registry: CollectorRegistry):
        self._registry = registry
        self.candles_received_total = Counter(
            "fs_candles_received_total",
            "Candles reconstructed from the realtime feed.",
            ["symbol_class"],
            registry=registry,
        )
        self.nats_publish_total = Counter(
            "fs_nats_publish_total",
            "NATS messages published, by subject prefix kind.",
            ["kind"],
            registry=registry,
        )
        self.nats_publish_errors_total = Counter(
            "fs_nats_publish_errors_total",
            "NATS publish exceptions, by subject prefix kind.",
            ["kind"],
            registry=registry,
        )
        self.nats_reconnect_total = Counter(
            "fs_nats_reconnect_total",
            "NATS client reconnections.",
            registry=registry,
        )
        self.indicator_cache_save_total = Counter(
            "fs_indicator_cache_save_total",
            "Indicator cache save attempts.",
            ["name", "result"],
            registry=registry,
        )
        self.nats_flush_duration_seconds = Histogram(
            "fs_nats_flush_duration_seconds",
            "Latency of NATSChannel.flush(), one observation per flush call.",
            buckets=buckets.NATS_FLUSH_SECONDS,
            registry=registry,
        )
        self.indicator_extend_latency_seconds = Histogram(
            "fs_indicator_extend_latency_seconds",
            "Latency of indicator.extend_realtime() per indicator name.",
            ["name"],
            buckets=buckets.INDICATOR_EXTEND_SECONDS,
            registry=registry,
        )
        self.http_request_latency_seconds = Histogram(
            "fs_http_request_latency_seconds",
            "FastAPI request latency by route template and status code.",
            ["route", "code"],
            buckets=buckets.HTTP_REQUEST_SECONDS,
            registry=registry,
        )
        self.first_candle_delay_seconds = Histogram(
            "fs_first_candle_delay_seconds",
            "Wall-clock seconds past the bar end when the FIRST candle for a "
            "minute arrived from Polygon (one observation per bar minute).",
            buckets=buckets.CANDLE_DELAY_SECONDS,
            registry=registry,
        )
        self.last_candle_delay_seconds = Histogram(
            "fs_last_candle_delay_seconds",
            "Wall-clock seconds past the bar end when the LAST candle for a "
            "minute arrived from Polygon (one observation per bar minute).",
            buckets=buckets.CANDLE_DELAY_SECONDS,
            registry=registry,
        )
        self.nats_pending_messages = Gauge(
            "fs_nats_pending_messages",
            "Depth of NATSChannel._pending_messages at flush boundary.",
            registry=registry,
            multiprocess_mode="livesum",
        )
        self.polygon_ws_connected = Gauge(
            "fs_polygon_ws_connected",
            "1 if the Polygon WebSocket is connected; 0 otherwise.",
            registry=registry,
            multiprocess_mode="max",
        )
        self.active_subscriptions = Gauge(
            "fs_active_subscriptions",
            "Active subscription count by kind. Sums across multiprocess workers.",
            ["kind"],
            registry=registry,
            multiprocess_mode="livesum",
        )
        self.market_is_open = Gauge(
            "fs_market_is_open",
            "1 if the equity market is currently open per exchange_calendars; 0 otherwise.",
            registry=registry,
            multiprocess_mode="max",
        )
        self.daily_reset_in_progress = Gauge(
            "fs_daily_reset_in_progress",
            "1 while the realtime writer is performing its scheduled daily disconnect/reconnect.",
            registry=registry,
            multiprocess_mode="max",
        )

    def candle_received(self, symbol_class: str) -> None:
        self.candles_received_total.labels(symbol_class=symbol_class).inc()

    def nats_publish(self, kind: str) -> None:
        self.nats_publish_total.labels(kind=kind).inc()

    def nats_publish_error(self, kind: str) -> None:
        self.nats_publish_errors_total.labels(kind=kind).inc()

    def nats_reconnect(self) -> None:
        self.nats_reconnect_total.inc()

    def nats_flush(self, latency_seconds: float) -> None:
        self.nats_flush_duration_seconds.observe(latency_seconds)

    def nats_pending(self, depth: int) -> None:
        self.nats_pending_messages.set(depth)

    def indicator_extend_latency(self, name: str, latency_seconds: float) -> None:
        self.indicator_extend_latency_seconds.labels(name=name).observe(
            latency_seconds
        )

    def indicator_cache_save(self, name: str, result: CacheSaveResult) -> None:
        self.indicator_cache_save_total.labels(name=name, result=result).inc()

    def http_request(self, route: str, code: int, latency_seconds: float) -> None:
        self.http_request_latency_seconds.labels(
            route=route, code=str(code)
        ).observe(latency_seconds)

    def first_candle_delay(self, delay_seconds: float) -> None:
        self.first_candle_delay_seconds.observe(delay_seconds)

    def last_candle_delay(self, delay_seconds: float) -> None:
        self.last_candle_delay_seconds.observe(delay_seconds)

    def set_ws_connected(self, connected: bool) -> None:
        self.polygon_ws_connected.set(1 if connected else 0)

    def set_active_subscriptions(self, kind: SubscriptionKind, count: int) -> None:
        self.active_subscriptions.labels(kind=kind).set(count)

    def set_market_open(self, is_open: bool) -> None:
        self.market_is_open.set(1 if is_open else 0)

    def set_daily_reset(self, in_progress: bool) -> None:
        self.daily_reset_in_progress.set(1 if in_progress else 0)


_instance: _Metrics | None = None


def _get() -> _Metrics:
    global _instance
    if _instance is None:
        _instance = _Metrics(REGISTRY)
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


def indicator_cache_save(name: str, result: CacheSaveResult) -> None:
    _get().indicator_cache_save(name, result)


def http_request(route: str, code: int, latency_seconds: float) -> None:
    _get().http_request(route, code, latency_seconds)


def first_candle_delay(delay_seconds: float) -> None:
    _get().first_candle_delay(delay_seconds)


def last_candle_delay(delay_seconds: float) -> None:
    _get().last_candle_delay(delay_seconds)


def set_ws_connected(connected: bool) -> None:
    _get().set_ws_connected(connected)


def set_active_subscriptions(kind: SubscriptionKind, count: int) -> None:
    _get().set_active_subscriptions(kind, count)


def set_market_open(is_open: bool) -> None:
    _get().set_market_open(is_open)


def set_daily_reset(in_progress: bool) -> None:
    _get().set_daily_reset(in_progress)


def _reset_for_test(registry: CollectorRegistry | None = None) -> _Metrics:
    global _instance
    if _instance is not None:
        for attr in vars(_instance).values():
            if isinstance(attr, (Counter, Gauge, Histogram)):
                try:
                    _instance._registry.unregister(attr)
                except KeyError:
                    pass
    target = registry if registry is not None else CollectorRegistry()
    _instance = _Metrics(target)
    return _instance
